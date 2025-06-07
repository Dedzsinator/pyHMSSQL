# cython: language_level=3, boundscheck=True, wraparound=False, initializedcheck=True, cdivision=True

"""
Highly optimized B+ tree implementation using Cython.
"""

import numpy as np
import pickle
import json
import logging
from datetime import datetime
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy
import cython

# Create logger
logger = logging.getLogger("bptree_opt")

# Define structure for multidimensional keys
cdef struct MultiDimKey:
    double* dimensions  # Array of dimension values
    int num_dimensions  # Number of dimensions
    size_t value_ptr    # For leaf nodes

# Define structure for key-value pairs (backward compatibility)
cdef struct KeyValue:
    # For non-leaf nodes, value_ptr is unused (set to 0)
    double key
    size_t value_ptr

# Define structure for B+ tree nodes
ctypedef struct BPNode:
    bint is_leaf
    int num_keys
    int capacity
    KeyValue* keys              # Single-dimensional keys (legacy)
    MultiDimKey* multidim_keys  # Multi-dimensional keys
    BPNode** children
    BPNode* next               # For leaf nodes' linked list
    int dimensions             # Number of dimensions (0 for single-dim)

# Multidimensional key comparison functions
cdef int _compare_multidim_keys(double* key1, double* key2, int dimensions) nogil:
    """
    Compare two multidimensional keys lexicographically.
    Returns: -1 if key1 < key2, 0 if equal, 1 if key1 > key2
    """
    cdef int i
    cdef double epsilon = 1e-9
    cdef double diff
    
    for i in range(dimensions):
        diff = key1[i] - key2[i]
        if diff < -epsilon:
            return -1
        elif diff > epsilon:
            return 1
    return 0

cdef bint _multidim_key_in_range(double* key, double* start_key, double* end_key, int dimensions) nogil:
    """
    Check if a multidimensional key is within the given range.
    Returns True if start_key <= key <= end_key
    """
    cdef int i
    cdef double epsilon = 1e-9
    
    for i in range(dimensions):
        if key[i] < start_key[i] - epsilon or key[i] > end_key[i] + epsilon:
            return False
    return True

cdef void _free_multidim_key(MultiDimKey* key) noexcept nogil:
    """Free memory allocated for a multidimensional key"""
    if key != NULL:
        if key.dimensions != NULL:
            free(key.dimensions)
        free(key)

cdef MultiDimKey* _create_multidim_key(double* dimensions, int num_dimensions, size_t value_ptr) except? NULL:
    """Create a new multidimensional key with allocated memory"""
    cdef MultiDimKey* key
    cdef int i
    
    key = <MultiDimKey*>malloc(sizeof(MultiDimKey))
    if key == NULL:
        return NULL
    
    key.dimensions = <double*>malloc(sizeof(double) * num_dimensions)
    if key.dimensions == NULL:
        free(key)
        return NULL
    
    key.num_dimensions = num_dimensions
    key.value_ptr = value_ptr
    
    # Copy dimension values
    for i in range(num_dimensions):
        key.dimensions[i] = dimensions[i]
    
    return key

cdef class ValueHolder:
    """Lightweight class to hold values with optimized memory usage"""
    __slots__ = ('value',)
    
    cdef public object value  # Added explicit declaration for the value attribute
    
    def __init__(self, value):
        self.value = value

cdef class BPlusTreeOptimized:
    """
    Optimized B+ tree implementation using Cython.
    Optimized for numeric keys (doubles) and arbitrary value pointers.
    """
    __slots__ = (
    'root', 'order', 'max_keys', 'name', 'operation_counter', 
    'value_store', 'next_value_id'
    )
    
    cdef:
        BPNode* root
        public int order
        int max_keys
        public str name
        int operation_counter
        dict value_store  # Store actual Python values
        size_t next_value_id

    def __cinit__(self, int order=50, name=None):
        """Initialize the B+ tree with proper validation"""
        # Validate order (must be at least 2)
        if order < 2:
            logging.warning(f"Invalid B+ tree order {order}, using default 50")
            order = 50
        
        self.order = order
        self.max_keys = 2 * order - 1
        
        # Sanity check for max_keys
        if self.max_keys <= 0:
            logging.error(f"Invalid max_keys {self.max_keys}, using default 99")
            self.max_keys = 99
        
        # Set name
        self.name = name or f"tree_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Initialize other members
        self.operation_counter = 0
        self.value_store = {}
        self.next_value_id = 1
        
        # Create root node
        self.root = self._create_node(1)  # True = leaf
        
        # Verify initialization - fixed string formatting
        if self.root == NULL:
            raise ValueError(f"Failed to properly initialize B+ tree: root is NULL")
        elif self.root.capacity <= 0:
            root_capacity = self.root.capacity
            raise ValueError(f"Failed to properly initialize B+ tree: root capacity is {root_capacity}")
        
        logger.info(f"Created new optimized B+ tree '{self.name}' with order {order}")

    def __dealloc__(self):
        if self.root != NULL:
            self._free_tree(self.root)
            self.root = NULL

    cdef BPNode* _create_node(self, bint is_leaf) except? NULL:
        """Create a new B+ tree node with allocated memory"""
        cdef:
            BPNode* node
            int i
        
        # Ensure we have a valid order
        if self.order < 2:
            logging.error(f"[{self.name}] Invalid B+ tree order: {self.order}, using default 50")
            self.order = 50
            self.max_keys = 2 * self.order - 1
        
        # Allocate and initialize the node
        node = <BPNode*>malloc(sizeof(BPNode))
        if node == NULL:
            raise MemoryError("Failed to allocate memory for B+ tree node")
        
        # Initialize node fields
        node.is_leaf = is_leaf
        node.num_keys = 0
        node.capacity = self.max_keys  # Make sure this is calculated properly
        
        # Sanity check for capacity
        if node.capacity <= 0:
            logging.error(f"[{self.name}] Invalid capacity calculated: {node.capacity}, using default 99")
            node.capacity = 99  # Fallback to a reasonable default
            
        node.next = NULL
        
        # Allocate and initialize keys
        node.keys = <KeyValue*>malloc(sizeof(KeyValue) * node.capacity)
        if node.keys == NULL:
            free(node)
            raise MemoryError("Failed to allocate memory for keys")
        
        # Initialize multidimensional keys to NULL
        node.multidim_keys = NULL
        node.dimensions = 0
        
        # Initialize keys to zero
        for i in range(node.capacity):
            node.keys[i].key = 0.0
            node.keys[i].value_ptr = 0
        
        # For internal nodes, allocate child pointers
        if is_leaf:
            node.children = NULL
        else:
            node.children = <BPNode**>malloc(sizeof(BPNode*) * (node.capacity + 1))
            if node.children == NULL:
                free(node.keys)
                free(node)
                raise MemoryError("Failed to allocate memory for children pointers")
            
            # Initialize all child pointers to NULL
            for i in range(node.capacity + 1):
                node.children[i] = NULL
        
        return node

    cdef void _free_node(self, BPNode* node) nogil:
        """Free memory allocated for a node"""
        cdef int i
        
        if node != NULL:
            if node.keys != NULL:
                free(node.keys)
            if node.children != NULL:
                free(node.children)
            
            # Free multidimensional keys if present
            if node.multidim_keys != NULL:
                for i in range(node.num_keys):
                    if node.multidim_keys[i].dimensions != NULL:
                        free(node.multidim_keys[i].dimensions)
                free(node.multidim_keys)
            
            free(node)

    cdef void _free_tree(self, BPNode* node) nogil:
        """Recursively free memory for the entire tree"""
        cdef int i
        if node != NULL:
            if not node.is_leaf and node.children != NULL:
                for i in range(node.num_keys + 1):
                    if node.children[i] != NULL:
                        self._free_tree(node.children[i])
            self._free_node(node)

    @cython.cdivision(True)
    cdef int _binary_search(self, BPNode* node, double key) nogil:
        """
        Binary search to find the position of a key in a node.
        Returns the index where the key should be inserted.
        """
        cdef:
            int low = 0
            int high = node.num_keys - 1
            int mid
        
        # Handle empty node case
        if node.num_keys == 0:
            return 0
            
        # Binary search
        while low <= high:
            mid = (low + high) // 2
            if node.keys[mid].key == key:
                return mid
            elif node.keys[mid].key > key:
                high = mid - 1
            else:
                low = mid + 1
                
        return low

    @cython.cdivision(True)
    cdef int _binary_search_multidim(self, BPNode* node, double* key_dimensions, int dimensions) nogil:
        """
        Binary search for multidimensional keys in a node.
        Returns the index where the key should be inserted.
        """
        cdef:
            int low = 0
            int high = node.num_keys - 1
            int mid
            int cmp_result
        
        # Handle empty node case
        if node.num_keys == 0:
            return 0
        
        # Handle dimension mismatch
        if node.dimensions != dimensions:
            return 0
            
        # Binary search using multidimensional comparison
        while low <= high:
            mid = (low + high) // 2
            cmp_result = _compare_multidim_keys(node.multidim_keys[mid].dimensions, key_dimensions, dimensions)
            
            if cmp_result == 0:
                return mid
            elif cmp_result > 0:
                high = mid - 1
            else:
                low = mid + 1
                
        return low

    cdef void _split_child(self, BPNode* parent, int index) except * nogil:
        """Split a child node when it's full"""
        cdef:
            BPNode* y
            BPNode* z
            int mid
            int i, j
        
        # Validate parent node
        if parent == NULL:
            with gil:
                logger.error(f"[{self.name}] Error: Parent node is NULL in split operation")
            return
        
        # Validate index bounds
        if index < 0 or index > parent.num_keys:
            with gil:
                logger.error(f"[{self.name}] Error: Invalid index {index} for split operation")
            return
        
        # Validate parent has children
        if parent.children == NULL:
            with gil:
                logger.error(f"[{self.name}] Error: Parent has NULL children array in split operation")
            return
        
        # Validate the child exists
        y = parent.children[index]
        if y == NULL:
            with gil:
                logger.error(f"[{self.name}] Error: Child at index {index} is NULL in split operation")
            return
        
        # Create the new node with the correct leaf status
        with gil:
            z = self._create_node(y.is_leaf)
        
        if z == NULL:
            with gil:
                logger.error(f"[{self.name}] Error: Failed to create new node during split")
            return
            
        # Calculate split point
        mid = self.order - 1
        
        if y.is_leaf:
            # For leaf nodes: copy second half of keys to new node
            if mid < y.num_keys:
                # Copy keys to the new node
                for i in range(mid, y.num_keys):
                    if i - mid < z.capacity:
                        z.keys[i - mid] = y.keys[i]
                        z.num_keys += 1
                
                y.num_keys = mid
                
                # Link leaves for range queries
                z.next = y.next
                y.next = z
                
                # Add separator key to parent
                if z.num_keys > 0 and parent.num_keys < parent.capacity:
                    # Shift parent keys and children to make space
                    for i in range(parent.num_keys, index, -1):
                        parent.keys[i] = parent.keys[i-1]
                        
                    for i in range(parent.num_keys + 1, index + 1, -1):
                        parent.children[i] = parent.children[i-1]
                    
                    # Insert separator and new node
                    parent.keys[index] = z.keys[0]
                    parent.children[index+1] = z
                    parent.num_keys += 1
                else:
                    with gil:
                        logger.error(f"[{self.name}] Error: Cannot insert separator key in parent")
                    return
            else:
                with gil:
                    logger.warning(f"[{self.name}] Warning: Not enough keys ({y.num_keys}) to split at {mid}")
                return
        else:
            # For internal nodes: move middle key up and redistribute keys/children
            if mid < y.num_keys:
                # Make space in parent
                if parent.num_keys < parent.capacity:
                    # Shift keys and children
                    for i in range(parent.num_keys, index, -1):
                        parent.keys[i] = parent.keys[i-1]
                    
                    for i in range(parent.num_keys + 1, index + 1, -1):
                        parent.children[i] = parent.children[i-1]
                    
                    # Move middle key to parent
                    parent.keys[index] = y.keys[mid]
                    parent.children[index+1] = z
                    parent.num_keys += 1
                    
                    # Copy keys after middle to the new node
                    j = 0
                    for i in range(mid + 1, y.num_keys):
                        z.keys[j] = y.keys[i]
                        z.num_keys += 1
                        j += 1
                    
                    # Copy relevant children to the new node
                    if y.children != NULL and z.children != NULL:
                        j = 0
                        for i in range(mid + 1, y.num_keys + 1):
                            if i < y.capacity + 1:
                                z.children[j] = y.children[i]
                                y.children[i] = NULL
                                j += 1
                    
                    # Update y's key count
                    y.num_keys = mid
                else:
                    with gil:
                        logger.error(f"[{self.name}] Error: Parent node full during split")
                    return
            else:
                with gil:
                    logger.warning(f"[{self.name}] Warning: Not enough keys to split internal node")
                return

    cdef void _split_child_multidim(self, BPNode* parent, int index) except * nogil:
        """Split a multidimensional child node when it's full"""
        cdef:
            BPNode* y
            BPNode* z
            int mid
            int i, j
            int dimensions
        
        # Validate inputs
        if parent == NULL or parent.children == NULL or index < 0 or index > parent.num_keys:
            with gil:
                logger.error(f"[{self.name}] Invalid parameters for multidimensional split")
            return
        
        y = parent.children[index]
        if y == NULL:
            with gil:
                logger.error(f"[{self.name}] Child to split is NULL")
            return
        
        dimensions = y.dimensions
        if dimensions <= 0:
            with gil:
                logger.error(f"[{self.name}] Invalid dimensions for multidimensional split: {dimensions}")
            return
        
        # Create new node with same leaf status and dimensions
        with gil:
            z = self._create_multidim_node(y.is_leaf, dimensions)
        
        if z == NULL:
            with gil:
                logger.error(f"[{self.name}] Failed to create new node during multidimensional split")
            return
        
        # Calculate split point
        mid = self.order - 1
        
        if y.is_leaf:
            # For leaf nodes: copy second half of keys to new node
            if mid < y.num_keys:
                for i in range(mid, y.num_keys):
                    if i - mid < z.capacity:
                        # Copy multidimensional key
                        z.multidim_keys[i - mid].dimensions = <double*>malloc(sizeof(double) * dimensions)
                        if z.multidim_keys[i - mid].dimensions != NULL:
                            for j in range(dimensions):
                                z.multidim_keys[i - mid].dimensions[j] = y.multidim_keys[i].dimensions[j]
                            z.multidim_keys[i - mid].num_dimensions = dimensions
                            z.multidim_keys[i - mid].value_ptr = y.multidim_keys[i].value_ptr
                            z.num_keys += 1
                            
                            # Free old key
                            free(y.multidim_keys[i].dimensions)
                            y.multidim_keys[i].dimensions = NULL
                
                y.num_keys = mid
                
                # Link leaves for range queries
                z.next = y.next
                y.next = z
                
                # Add separator key to parent (use first key of new node)
                if z.num_keys > 0 and parent.num_keys < parent.capacity:
                    # Shift parent keys and children
                    for i in range(parent.num_keys, index, -1):
                        if parent.multidim_keys != NULL and parent.multidim_keys[i-1].dimensions != NULL:
                            if parent.multidim_keys[i].dimensions != NULL:
                                free(parent.multidim_keys[i].dimensions)
                            parent.multidim_keys[i].dimensions = <double*>malloc(sizeof(double) * dimensions)
                            if parent.multidim_keys[i].dimensions != NULL:
                                for j in range(dimensions):
                                    parent.multidim_keys[i].dimensions[j] = parent.multidim_keys[i-1].dimensions[j]
                                parent.multidim_keys[i].num_dimensions = dimensions
                                parent.multidim_keys[i].value_ptr = parent.multidim_keys[i-1].value_ptr
                    
                    for i in range(parent.num_keys + 1, index + 1, -1):
                        parent.children[i] = parent.children[i-1]
                    
                    # Insert separator
                    if parent.multidim_keys[index].dimensions != NULL:
                        free(parent.multidim_keys[index].dimensions)
                    parent.multidim_keys[index].dimensions = <double*>malloc(sizeof(double) * dimensions)
                    if parent.multidim_keys[index].dimensions != NULL:
                        for j in range(dimensions):
                            parent.multidim_keys[index].dimensions[j] = z.multidim_keys[0].dimensions[j]
                        parent.multidim_keys[index].num_dimensions = dimensions
                        parent.multidim_keys[index].value_ptr = 0  # Internal node
                    
                    parent.children[index + 1] = z
                    parent.num_keys += 1

    cdef void _insert_non_full(self, BPNode* node, double key, size_t value_ptr) except * nogil:
        """Insert into a non-full node with improved floating point comparison"""
        cdef:
            int i
            int pos
            size_t old_ptr
            double epsilon = 1e-9
            double diff
        
        if node == NULL:
            with gil:
                logger.error(f"[{self.name}] Error: Attempt to insert into NULL node")
            return
        
        # Check node capacity
        if node.capacity <= 0:
            with gil:
                logger.error(f"[{self.name}] Error: Node has invalid capacity: {node.capacity}")
            return
        
        if node.is_leaf:
            # Find position using binary search
            pos = self._binary_search(node, key)
            
            # Check if key already exists with floating point comparison
            if pos < node.num_keys:
                diff = node.keys[pos].key - key
                if diff < epsilon and diff > -epsilon:  # abs(diff) < epsilon
                    # Replace the value - need GIL for Python dict operations
                    with gil:
                        old_ptr = node.keys[pos].value_ptr
                        self.value_store[old_ptr] = self.value_store[value_ptr]
                        del self.value_store[value_ptr]
                        
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f"[{self.name}] UPDATE - key: {key}, old value: {self.value_store[old_ptr]}")
                    return
            
            # Key not found, continue with insertion
            # Ensure we have capacity before shifting keys
            if node.num_keys >= node.capacity:
                with gil:
                    logger.error(f"[{self.name}] Error: Leaf node capacity exceeded ({node.num_keys}/{node.capacity})")
                return
                    
            # Shift keys to make room
            for i in range(node.num_keys, pos, -1):
                if i < node.capacity and i-1 >= 0:
                    node.keys[i] = node.keys[i-1]
            
            # Insert new key-value
            if pos < node.capacity:
                # Store exact key value to avoid rounding errors
                node.keys[pos].key = key
                node.keys[pos].value_ptr = value_ptr
                node.num_keys += 1
            else:
                with gil:
                    logger.error(f"[{self.name}] Error: Invalid position for insertion: {pos}, capacity: {node.capacity}")
        else:
            # For internal nodes
            
            # Find child which will have the new key
            pos = self._binary_search(node, key)
            
            # For internal nodes, when key is greater than all keys, use the last child
            if pos >= node.num_keys:
                pos = node.num_keys
            
            # Validate position is valid
            if pos < 0 or pos > node.num_keys:
                with gil:
                    logger.error(f"[{self.name}] Error: Invalid position {pos} for node with {node.num_keys} keys")
                return
                
            # Make sure child exists
            if node.children == NULL:
                with gil:
                    logger.error(f"[{self.name}] Error: Internal node has NULL children array")
                return
                
            if node.children[pos] == NULL:
                with gil:
                    logger.error(f"[{self.name}] Error: Child at position {pos} is NULL")
                return
                    
            # If appropriate child is full, split it
            if node.children[pos].num_keys >= node.children[pos].capacity:
                self._split_child(node, pos)
                
                # After splitting, decide which child to go to
                if key > node.keys[pos].key:
                    pos += 1
                    
                # Safety check after incrementing position
                if pos > node.num_keys:
                    with gil:
                        logger.error(f"[{self.name}] Error: Position {pos} exceeds number of keys {node.num_keys} after split")
                    return
                    
                if pos < 0 or node.children[pos] == NULL:
                    with gil:
                        logger.error(f"[{self.name}] Error: Invalid position or NULL child after split: {pos}")
                    return
            
            # Recursively insert
            self._insert_non_full(node.children[pos], key, value_ptr)

    cdef void _insert_non_full_multidim(self, BPNode* node, double* key_dimensions, int dimensions, size_t value_ptr) except * nogil:
        """Insert multidimensional key into a non-full node"""
        cdef:
            int i, j
            int pos
        
        if node == NULL or key_dimensions == NULL or dimensions <= 0:
            with gil:
                logger.error(f"[{self.name}] Invalid parameters for multidimensional insertion")
            return
        
        if node.dimensions != dimensions:
            with gil:
                logger.error(f"[{self.name}] Dimension mismatch in insertion: node has {node.dimensions}, key has {dimensions}")
            return
        
        if node.is_leaf:
            # Find position using multidimensional binary search
            pos = self._binary_search_multidim(node, key_dimensions, dimensions)
            
            # Check if key already exists
            if pos < node.num_keys and _compare_multidim_keys(node.multidim_keys[pos].dimensions, key_dimensions, dimensions) == 0:
                # Update existing key
                with gil:
                    old_ptr = node.multidim_keys[pos].value_ptr
                    self.value_store[old_ptr] = self.value_store[value_ptr]
                    del self.value_store[value_ptr]
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"[{self.name}] MULTIDIM UPDATE - dimensions: {[key_dimensions[i] for i in range(dimensions)]}")
                return
            
            # Ensure capacity
            if node.num_keys >= node.capacity:
                with gil:
                    logger.error(f"[{self.name}] Leaf node capacity exceeded in multidimensional insertion")
                return
            
            # Shift keys to make room
            for i in range(node.num_keys, pos, -1):
                if i < node.capacity and i-1 >= 0:
                    if node.multidim_keys[i].dimensions != NULL:
                        free(node.multidim_keys[i].dimensions)
                    
                    node.multidim_keys[i].dimensions = <double*>malloc(sizeof(double) * dimensions)
                    if node.multidim_keys[i].dimensions != NULL:
                        for j in range(dimensions):
                            node.multidim_keys[i].dimensions[j] = node.multidim_keys[i-1].dimensions[j]
                        node.multidim_keys[i].num_dimensions = node.multidim_keys[i-1].num_dimensions
                        node.multidim_keys[i].value_ptr = node.multidim_keys[i-1].value_ptr
            
            # Insert new key
            if pos < node.capacity:
                if node.multidim_keys[pos].dimensions != NULL:
                    free(node.multidim_keys[pos].dimensions)
                
                node.multidim_keys[pos].dimensions = <double*>malloc(sizeof(double) * dimensions)
                if node.multidim_keys[pos].dimensions != NULL:
                    for j in range(dimensions):
                        node.multidim_keys[pos].dimensions[j] = key_dimensions[j]
                    node.multidim_keys[pos].num_dimensions = dimensions
                    node.multidim_keys[pos].value_ptr = value_ptr
                    node.num_keys += 1
        else:
            # Internal node: find child to insert into
            pos = self._binary_search_multidim(node, key_dimensions, dimensions)
            
            if pos >= node.num_keys:
                pos = node.num_keys
            
            # Check if child is full and split if necessary
            if node.children[pos] != NULL and node.children[pos].num_keys >= node.children[pos].capacity:
                self._split_child_multidim(node, pos)
                
                # After splitting, decide which child to go to
                if pos < node.num_keys and _compare_multidim_keys(key_dimensions, node.multidim_keys[pos].dimensions, dimensions) > 0:
                    pos += 1
            
            # Recursively insert
            if node.children[pos] != NULL:
                self._insert_non_full_multidim(node.children[pos], key_dimensions, dimensions, value_ptr)

    def search(self, key):
        """Search for a key and return its value"""
        self.operation_counter += 1
        cdef double k = float(key)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"[{self.name}][{self.operation_counter}] SEARCH - key: {key}")
        
        cdef size_t result_ptr = self._search_internal(self.root, k)
        if result_ptr == 0:
            return None
        value_holder = self.value_store.get(result_ptr)
        return value_holder.value if value_holder else None

    cdef size_t _search_internal(self, BPNode* node, double key) nogil:
        """Improved search function with better floating point comparison"""
        cdef:
            int pos
            int i
            size_t result = 0
            double epsilon = 1e-9  # Small epsilon for float comparison
            double diff
        
        if node == NULL:
            return 0
        
        if node.is_leaf:
            # Use binary search instead of linear search
            pos = self._binary_search(node, key)
            
            # Check if found at position - using epsilon comparison for floating point
            if pos < node.num_keys:
                diff = node.keys[pos].key - key
                if diff < epsilon and diff > -epsilon:  # abs(diff) < epsilon
                    return node.keys[pos].value_ptr
                    
            # Fallback to linear search to be safe
            for i in range(node.num_keys):
                diff = node.keys[i].key - key
                if diff < epsilon and diff > -epsilon:  # abs(diff) < epsilon
                    return node.keys[i].value_ptr
            return 0
        else:
            # Find the correct child
            pos = self._binary_search(node, key)
            
            # If key exactly equals a separator key, need special handling
            if pos < node.num_keys:
                diff = node.keys[pos].key - key
                if diff < epsilon and diff > -epsilon:  # abs(diff) < epsilon
                    # For exact matches on separator keys, go to right child
                    pos += 1
                    
            # Ensure position is valid
            if pos > node.num_keys:
                pos = node.num_keys
                    
            # Safety check
            if node.children == NULL or node.children[pos] == NULL:
                return 0
                    
            # Recursively search
            return self._search_internal(node.children[pos], key)

    def insert_non_unique(self, key, value):
        """Insert allowing duplicate keys"""
        cdef double k = float(key)
        
        # Check if key already exists
        existing_ptr = self._search_internal(self.root, k)
        
        if existing_ptr != 0:
            # Key exists - append to existing values
            existing_holder = self.value_store[existing_ptr]
            if isinstance(existing_holder.value, list):
                existing_holder.value.append(value)
            else:
                # Convert single value to list
                existing_holder.value = [existing_holder.value, value]
        else:
            # New key - store as single value
            self._insert_single_value(k, value)

    def _insert_single_value(self, double key, value):
        """Insert a single value (helper method)"""
        cdef size_t value_ptr = self.next_value_id
        cdef BPNode* new_root
        
        self.value_store[value_ptr] = ValueHolder(value)
        self.next_value_id += 1
        
        # Check if root is NULL
        if self.root == NULL:
            # Root is NULL, create a new one
            self.root = self._create_node(1)  # Create a new leaf root
            if self.root == NULL:
                logger.error(f"[{self.name}] Failed to create new root node in _insert_single_value")
                return
        
        # Handle root split if necessary
        if self.root.num_keys >= self.root.capacity:
            # Root is full, need to split it
            new_root = self._create_node(0)  # Create new internal root
            if new_root == NULL:
                logger.error(f"[{self.name}] Failed to create new root node during split in _insert_single_value")
                return
                
            # Make old root the first child of new root
            new_root.children[0] = self.root
            new_root.num_keys = 0  # Start with no keys in new root
            
            # Update root pointer
            self.root = new_root
            
            # Split the old root (now first child of new root)
            self._split_child(new_root, 0)
            
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"[{self.name}] Root split completed in _insert_single_value")
        
        # Insert into the (possibly new) non-full root
        self._insert_non_full(self.root, key, value_ptr)
        
        # Verify insertion for debugging
        if logger.isEnabledFor(logging.DEBUG):
            result = self.search(key)
            if result != value:
                logger.error(f"[{self.name}] Key {key} was not inserted correctly in _insert_single_value")
            else:
                logger.debug(f"[{self.name}] Successfully inserted key {key} with value {value}")

    cdef void _collect_range(self, BPNode* node, double start_key, double end_key, list result) except *:
        """Safer implementation to collect keys in range - with more error handling"""
        cdef:
            int i
            KeyValue kv
            BPNode* leaf_node
            double leaf_key
            size_t val_ptr
        
        if node == NULL:
            return
            
        # Simplified approach: find the leftmost leaf first
        leaf_node = self._find_leftmost_leaf(node)
        
        # Then traverse the linked list of leaves
        while leaf_node != NULL:
            # Check each key in the leaf
            for i in range(leaf_node.num_keys):
                if i < 0 or i >= leaf_node.capacity:
                    logger.error(f"[{self.name}] Invalid key index {i} in leaf node")
                    continue
                    
                # Get key safely
                leaf_key = leaf_node.keys[i].key
                
                # If key > end_key, we're done
                if leaf_key > end_key:
                    return
                    
                # If key is in range, add to results
                if leaf_key >= start_key:
                    val_ptr = leaf_node.keys[i].value_ptr
                    if val_ptr > 0:
                        # Fetch the value - this needs GIL but we're already in Python context
                        value = self.value_store.get(val_ptr)
                        if value is not None:
                            result.append((leaf_key, value))
            
            # Move to the next leaf
            leaf_node = leaf_node.next

    cdef BPNode* _find_leftmost_leaf(self, BPNode* node) except? NULL:
        """Helper method to find the leftmost leaf node from a given node"""
        cdef BPNode* current = node
        
        if current == NULL:
            return NULL
            
        # Traverse to leftmost leaf
        while not current.is_leaf:
            if current.children == NULL or current.children[0] == NULL:
                logger.error(f"[{self.name}] Encountered NULL child pointer in _find_leftmost_leaf")
                return NULL
            current = current.children[0]
            
        return current

    def __getitem__(self, key):
        """Get item by key, equivalent to search"""
        return self.search(key)

    def __setitem__(self, key, value):
        """Set item by key, equivalent to insert"""
        self.insert(key, value)

    def get(self, key, default=None):
        """Get value for key or return default if not found"""
        result = self.search(key)
        return result if result is not None else default
        
    def save_to_file(self, filename):
        """Save the tree to a file using pickle"""
        logging.debug(f"Saving optimized B+ tree to file: {filename}")
        
        # For saving, just collect all key-value pairs and metadata
        data = {
            'name': self.name,
            'order': self.order,
            'keys_values': self._get_all_items()  # This is simpler than trying to save the tree structure
        }
        
        with open(filename, 'wb') as f:
            pickle.dump(data, f)
            
    @classmethod
    def load_from_file(cls, file_path):
        """Load a B+ tree from a file"""
        try:
            with open(file_path, 'rb') as file:
                data = pickle.load(file)
                
                # Create a new tree
                tree = cls(order=data['order'], name=data['name'])
                
                # Insert all key-value pairs
                for key, value in data['keys_values']:
                    tree.insert(key, value)
                
                return tree
        except RuntimeError as e:
            logging.error(f"Error loading B+ tree from {file_path}: {str(e)}")
            return None
    
    def _get_all_items(self):
        """Get all key-value pairs in the tree"""
        items = []
        self._collect_all_items(self.root, items)
        return items
    
    cdef void _collect_all_items(self, BPNode* node, list items) except *:
        """Helper to collect all items in the tree"""
        cdef:
            int i, j
            KeyValue kv
            object value  # Python object for the value
            object key_tuple
        
        if node == NULL:
            return
            
        if node.is_leaf:
            # Collect items from leaf
            if node.dimensions > 0:
                # Multidimensional keys
                for i in range(node.num_keys):
                    value_holder = self.value_store.get(node.multidim_keys[i].value_ptr)
                    if value_holder is not None:
                        # Convert dimensions to tuple
                        key_tuple = tuple([node.multidim_keys[i].dimensions[j] for j in range(node.dimensions)])
                        items.append((key_tuple, value_holder.value))
            else:
                # Single-dimensional keys (legacy)
                for i in range(node.num_keys):
                    kv = node.keys[i]
                    value_holder = self.value_store.get(kv.value_ptr)
                    if value_holder is not None:
                        items.append((kv.key, value_holder.value))
                
        else:
            # Visit children in order (for sorted output)
            for i in range(node.num_keys):
                # Visit child i
                if node.children[i] != NULL:
                    self._collect_all_items(node.children[i], items)
                    
            # Visit last child
            if node.num_keys >= 0 and node.children[node.num_keys] != NULL:
                self._collect_all_items(node.children[node.num_keys], items)

    def range_query(self, start_key, end_key):
        """Get all values with keys between start_key and end_key"""
        self.operation_counter += 1
        cdef:
            double start = float(start_key)
            double end = float(end_key)
        
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"[{self.name}][{self.operation_counter}] RANGE QUERY - from {start_key} to {end_key}")
        
        # Use the optimized range query implementation
        results = self._simple_range_query(start, end)
        
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"[{self.name}] RANGE QUERY RESULT - found {len(results)} entries")
        
        return results

    def _simple_range_query(self, double start_key, double end_key):
        """Optimized range query implementation"""
        cdef:
            BPNode* current_leaf 
            int i
            double key
            size_t value_ptr
            list results = []
            double epsilon = 1e-9
        
        # Handle empty tree
        if self.root == NULL:
            return results
        
        # Find leaf that would contain start_key
        current_leaf = self._find_leaf_for_key(start_key)
        
        if current_leaf == NULL:
            return results
        
        # Loop through leaves via next pointers
        while current_leaf != NULL:
            # Process keys in this leaf
            for i in range(current_leaf.num_keys):
                if i >= current_leaf.capacity:
                    continue
                    
                key = current_leaf.keys[i].key
                
                # If we've passed end_key, we're done
                if key > end_key + epsilon:
                    return results
                    
                # If key is in range, add to results
                if key >= start_key - epsilon:
                    value_ptr = current_leaf.keys[i].value_ptr
                    if value_ptr > 0:
                        value_holder = self.value_store.get(value_ptr)
                        if value_holder is not None:
                            results.append((key, value_holder.value))
            
            # Move to next leaf
            current_leaf = current_leaf.next
        
        return results

    cdef BPNode* _find_leaf_for_key(self, double key) except? NULL:
        """Find the leaf node that should contain a key or where it would be inserted"""
        cdef:
            BPNode* node
            int pos
        
        # Check for NULL root
        if self.root == NULL:
            logger.error(f"[{self.name}] Root is NULL in _find_leaf_for_key")
            return NULL
        
        node = self.root
        
        # Traverse down to leaf
        while node != NULL and not node.is_leaf:
            # Find child index using binary search
            pos = self._binary_search(node, key)
            
            # Handle edge cases
            if pos >= node.num_keys:
                pos = node.num_keys
                    
            # Safety check
            if node.children == NULL:
                logger.error(f"[{self.name}] Node has NULL children array in _find_leaf_for_key")
                return NULL
                
            if node.children[pos] == NULL:
                logger.error(f"[{self.name}] Child at position {pos} is NULL in _find_leaf_for_key")
                return NULL
                    
            # Move to child
            node = node.children[pos]
        
        return node

    cdef BPNode* _create_multidim_node(self, bint is_leaf, int dimensions) except? NULL:
        """Create a new B+ tree node with multidimensional support"""
        cdef:
            BPNode* node
            int i
        
        # Create base node
        node = self._create_node(is_leaf)
        if node == NULL:
            return NULL
        
        # Set dimensions
        node.dimensions = dimensions
        
        if dimensions > 0:
            # Allocate multidimensional keys array
            node.multidim_keys = <MultiDimKey*>malloc(sizeof(MultiDimKey) * node.capacity)
            if node.multidim_keys == NULL:
                self._free_node(node)
                raise MemoryError("Failed to allocate memory for multidimensional keys")
            
            # Initialize multidimensional keys
            for i in range(node.capacity):
                node.multidim_keys[i].dimensions = NULL
                node.multidim_keys[i].num_dimensions = 0
                node.multidim_keys[i].value_ptr = 0
        
        return node

    def insert_multidim(self, key_dimensions, value):
        """
        Insert a multidimensional key-value pair into the B+ tree.
        
        Args:
            key_dimensions: List or tuple of dimension values (e.g., [x, y, z])
            value: The value to store
        """
        if not isinstance(key_dimensions, (list, tuple)):
            raise ValueError("key_dimensions must be a list or tuple")
        
        if len(key_dimensions) == 0:
            raise ValueError("key_dimensions cannot be empty")
        
        cdef:
            int dimensions = len(key_dimensions)
            double* key_array = <double*>malloc(sizeof(double) * dimensions)
            int i
            BPNode* new_root
            size_t value_ptr
        
        if key_array == NULL:
            raise MemoryError("Failed to allocate memory for key dimensions")
        
        try:
            # Convert and copy dimensions
            for i in range(dimensions):
                key_array[i] = float(key_dimensions[i])
            
            # Store value and get its pointer
            self.operation_counter += 1
            value_ptr = self.next_value_id
            self.value_store[value_ptr] = ValueHolder(value)
            self.next_value_id += 1
            
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"[{self.name}][{self.operation_counter}] MULTIDIM INSERT - dimensions: {key_dimensions}, value: {value}")
            
            # Check if we need to convert to multidimensional or create new tree
            if self.root == NULL or self.root.dimensions == 0:
                # Convert to multidimensional or create new multidimensional tree
                self._convert_to_multidim(dimensions)
            elif self.root.dimensions != dimensions:
                raise ValueError(f"Dimension mismatch: tree has {self.root.dimensions} dimensions, key has {dimensions}")
            
            # Insert the multidimensional key
            if self.root.num_keys >= self.root.capacity:
                # Root is full, split it
                new_root = self._create_multidim_node(0, dimensions)  # Create new internal root
                if new_root == NULL:
                    logger.error(f"[{self.name}] Failed to create new multidimensional root node")
                    return
                    
                # Make old root the first child of new root
                new_root.children[0] = self.root
                self.root = new_root
                
                # Split the old root
                self._split_child_multidim(new_root, 0)
            
            # Insert into the non-full root
            self._insert_non_full_multidim(self.root, key_array, dimensions, value_ptr)
            
        finally:
            free(key_array)

    cdef void _convert_to_multidim(self, int dimensions) except *:
        """Convert existing single-dimensional tree to multidimensional"""
        cdef:
            BPNode* old_root = self.root
            BPNode* new_root
            int i, j
            double* temp_dimensions
        
        if old_root == NULL:
            # Create new multidimensional root
            self.root = self._create_multidim_node(1, dimensions)  # Leaf root
            return
        
        if old_root.dimensions > 0:
            # Already multidimensional
            return
        
        # If we have data in single-dimensional format, we need to migrate it
        if old_root.num_keys > 0:
            logger.warning(f"[{self.name}] Converting single-dimensional tree to {dimensions}-dimensional. Existing data will be converted using first dimension only.")
            
            # Create new multidimensional root
            new_root = self._create_multidim_node(old_root.is_leaf, dimensions)
            if new_root == NULL:
                raise MemoryError("Failed to create new multidimensional root")
            
            # Migrate existing keys
            temp_dimensions = <double*>malloc(sizeof(double) * dimensions)
            if temp_dimensions == NULL:
                self._free_node(new_root)
                raise MemoryError("Failed to allocate temporary dimensions array")
            
            try:
                for i in range(old_root.num_keys):
                    # Use old key as first dimension, zeros for others
                    temp_dimensions[0] = old_root.keys[i].key
                    for j in range(1, dimensions):
                        temp_dimensions[j] = 0.0
                    
                    # Create multidimensional key - new node should have NULL dimensions initially
                    new_root.multidim_keys[i].dimensions = <double*>malloc(sizeof(double) * dimensions)
                    if new_root.multidim_keys[i].dimensions == NULL:
                        raise MemoryError("Failed to allocate dimensions for migrated key")
                    
                    for j in range(dimensions):
                        new_root.multidim_keys[i].dimensions[j] = temp_dimensions[j]
                    
                    new_root.multidim_keys[i].num_dimensions = dimensions
                    new_root.multidim_keys[i].value_ptr = old_root.keys[i].value_ptr
                
                # Set the correct number of keys after migration
                new_root.num_keys = old_root.num_keys
                
                # Update pointers and free old root
                if old_root.is_leaf and old_root.next != NULL:
                    new_root.next = old_root.next
                
                self._free_node(old_root)
                self.root = new_root
                
            finally:
                free(temp_dimensions)
        else:
            # No data to migrate, just create new multidimensional root
            self._free_node(old_root)
            self.root = self._create_multidim_node(1, dimensions)  # Leaf root

    def search_multidim(self, key_dimensions):
        """
        Search for a multidimensional key and return its value.
        
        Args:
            key_dimensions: List or tuple of dimension values
            
        Returns:
            The value associated with the key, or None if not found
        """
        if not isinstance(key_dimensions, (list, tuple)):
            raise ValueError("key_dimensions must be a list or tuple")
        
        if len(key_dimensions) == 0:
            raise ValueError("key_dimensions cannot be empty")
        
        cdef:
            int dimensions = len(key_dimensions)
            double* key_array = <double*>malloc(sizeof(double) * dimensions)
            size_t result_ptr
            int i
        
        if key_array == NULL:
            raise MemoryError("Failed to allocate memory for key dimensions")
        
        try:
            # Convert dimensions
            for i in range(dimensions):
                key_array[i] = float(key_dimensions[i])
            
            self.operation_counter += 1
            
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"[{self.name}][{self.operation_counter}] MULTIDIM SEARCH - dimensions: {key_dimensions}")
            
            # Check dimension compatibility
            if self.root == NULL or self.root.dimensions != dimensions:
                return None
            
            result_ptr = self._search_multidim_internal(self.root, key_array, dimensions)
            if result_ptr == 0:
                return None
            
            value_holder = self.value_store.get(result_ptr)
            return value_holder.value if value_holder else None
            
        finally:
            free(key_array)

    cdef size_t _search_multidim_internal(self, BPNode* node, double* key_dimensions, int dimensions) nogil:
        """Internal multidimensional search function"""
        cdef:
            int pos
            int i
            int cmp_result
        
        if node == NULL or key_dimensions == NULL:
            return 0
        
        if node.dimensions != dimensions:
            return 0
        
        if node.is_leaf:
            # Search in leaf node
            pos = self._binary_search_multidim(node, key_dimensions, dimensions)
            
            # Check if found at position
            if pos < node.num_keys:
                cmp_result = _compare_multidim_keys(node.multidim_keys[pos].dimensions, key_dimensions, dimensions)
                if cmp_result == 0:
                    return node.multidim_keys[pos].value_ptr
            
            # Fallback to linear search
            for i in range(node.num_keys):
                cmp_result = _compare_multidim_keys(node.multidim_keys[i].dimensions, key_dimensions, dimensions)
                if cmp_result == 0:
                    return node.multidim_keys[i].value_ptr
            
            return 0
        else:
            # Internal node: find correct child
            pos = self._binary_search_multidim(node, key_dimensions, dimensions)
            
            # Handle exact matches on separator keys
            if pos < node.num_keys:
                cmp_result = _compare_multidim_keys(node.multidim_keys[pos].dimensions, key_dimensions, dimensions)
                if cmp_result == 0:
                    pos += 1  # Go to right child for exact matches
            
            if pos > node.num_keys:
                pos = node.num_keys
            
            # Safety check
            if node.children == NULL or node.children[pos] == NULL:
                return 0
            
            # Recursively search
            return self._search_multidim_internal(node.children[pos], key_dimensions, dimensions)
    
    def range_query_multidim(self, start_key_dimensions, end_key_dimensions):
        """
        Perform a multidimensional range query.
        
        Args:
            start_key_dimensions: List or tuple of start dimension values
            end_key_dimensions: List or tuple of end dimension values
            
        Returns:
            List of (key_dimensions, value) tuples within the range
        """
        if not isinstance(start_key_dimensions, (list, tuple)) or not isinstance(end_key_dimensions, (list, tuple)):
            raise ValueError("Key dimensions must be lists or tuples")
        
        if len(start_key_dimensions) != len(end_key_dimensions):
            raise ValueError("Start and end key dimensions must have the same length")
        
        if len(start_key_dimensions) == 0:
            raise ValueError("Key dimensions cannot be empty")
        
        cdef:
            int dimensions = len(start_key_dimensions)
            double* start_array = <double*>malloc(sizeof(double) * dimensions)
            double* end_array = <double*>malloc(sizeof(double) * dimensions)
            list results = []
            int i
        
        if start_array == NULL or end_array == NULL:
            if start_array != NULL:
                free(start_array)
            if end_array != NULL:
                free(end_array)
            raise MemoryError("Failed to allocate memory for range query")
        
        try:
            # Convert dimensions
            for i in range(dimensions):
                start_array[i] = float(start_key_dimensions[i])
                end_array[i] = float(end_key_dimensions[i])
            
            self.operation_counter += 1
            
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"[{self.name}][{self.operation_counter}] MULTIDIM RANGE QUERY - from {start_key_dimensions} to {end_key_dimensions}")
            
            # Check dimension compatibility
            if self.root == NULL or self.root.dimensions != dimensions:
                return results
            
            # Collect results
            self._collect_range_multidim(self.root, start_array, end_array, dimensions, results)
            
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"[{self.name}] MULTIDIM RANGE QUERY RESULT - found {len(results)} entries")
            
            return results
            
        finally:
            free(start_array)
            free(end_array)

    cdef void _collect_range_multidim(self, BPNode* node, double* start_key, double* end_key, int dimensions, list results) except *:
        """Collect multidimensional keys within the specified range"""
        cdef:
            int i, j
            BPNode* leaf_node
            object key_tuple
            object value
        
        if node == NULL or dimensions <= 0:
            return
        
        if node.dimensions != dimensions:
            return
        
        # Find the leftmost leaf that might contain keys in range
        leaf_node = self._find_leftmost_leaf(node)
        
        # Traverse the linked list of leaves
        while leaf_node != NULL:
            # Check each key in the leaf
            for i in range(leaf_node.num_keys):
                if i >= leaf_node.capacity:
                    continue
                
                # Check if key is within range
                if _multidim_key_in_range(leaf_node.multidim_keys[i].dimensions, start_key, end_key, dimensions):
                    # Key is in range, add to results
                    value_ptr = leaf_node.multidim_keys[i].value_ptr
                    if value_ptr > 0:
                        value_holder = self.value_store.get(value_ptr)
                        if value_holder is not None:
                            # Convert dimensions back to tuple
                            key_tuple = tuple([leaf_node.multidim_keys[i].dimensions[j] for j in range(dimensions)])
                            results.append((key_tuple, value_holder.value))
            
            # Move to next leaf
            leaf_node = leaf_node.next

    def is_multidimensional(self):
        """Check if this tree is configured for multidimensional indexing"""
        return self.root != NULL and self.root.dimensions > 0

    def get_dimensions(self):
        """Get the number of dimensions this tree supports"""
        if self.root == NULL:
            return 0
        return self.root.dimensions

    def insert_composite(self, *args, **kwargs):
        """
        Unified insert method that handles both single and multidimensional keys.
        
        For single-dimensional: insert_composite(key, value)
        For multidimensional: insert_composite(key_dimensions, value) or insert_composite(dimensions=key_dimensions, value=value)
        """
        if 'dimensions' in kwargs:
            # Explicit multidimensional insert
            dimensions = kwargs['dimensions']
            value = kwargs.get('value', args[0] if args else None)
            if value is None:
                raise ValueError("Value must be provided")
            return self.insert_multidim(dimensions, value)
        elif len(args) == 2:
            key, value = args
            if isinstance(key, (list, tuple)):
                if len(key) > 1:
                    # Multidimensional key
                    return self.insert_multidim(key, value)
                elif len(key) == 1:
                    # Single-dimensional key in list format
                    if self.is_multidimensional():
                        # Convert to multidimensional with trailing zeros
                        dimensions = self.get_dimensions()
                        multidim_key = [float(key[0])] + [0.0] * (dimensions - 1)
                        return self.insert_multidim(multidim_key, value)
                    else:
                        return self._insert_single_value(float(key[0]), value)
                else:
                    raise ValueError("Key cannot be empty")
            else:
                # Single-dimensional key (scalar)
                if self.is_multidimensional():
                    # Convert to multidimensional with trailing zeros
                    dimensions = self.get_dimensions()
                    multidim_key = [float(key)] + [0.0] * (dimensions - 1)
                    return self.insert_multidim(multidim_key, value)
                else:
                    return self._insert_single_value(float(key), value)
        else:
            raise ValueError("Invalid arguments for composite insert")

    def search_composite(self, key=None, dimensions=None):
        """
        Unified search method that handles both single and multidimensional keys.
        
        For single-dimensional: search_composite(key)
        For multidimensional: search_composite(dimensions=key_dimensions) or search_composite(key_dimensions)
        """
        if dimensions is not None:
            return self.search_multidim(dimensions)
        elif key is not None:
            if isinstance(key, (list, tuple)):
                if len(key) > 1:
                    # Multidimensional key
                    return self.search_multidim(key)
                elif len(key) == 1:
                    # Single-dimensional key in list format
                    if self.is_multidimensional():
                        # Convert to multidimensional with trailing zeros
                        dimensions = self.get_dimensions()
                        multidim_key = [float(key[0])] + [0.0] * (dimensions - 1)
                        return self.search_multidim(multidim_key)
                    else:
                        return self.search(key[0])
                else:
                    raise ValueError("Key cannot be empty")
            else:
                # Single-dimensional key (scalar)
                if self.is_multidimensional():
                    # Convert to multidimensional with trailing zeros
                    dimensions = self.get_dimensions()
                    multidim_key = [float(key)] + [0.0] * (dimensions - 1)
                    return self.search_multidim(multidim_key)
                else:
                    return self.search(key)
        else:
            raise ValueError("Either key or dimensions must be provided")

    def range_query_composite(self, start_key=None, end_key=None, start_dimensions=None, end_dimensions=None):
        """
        Unified range query method that handles both single and multidimensional keys.
        
        For single-dimensional: range_query_composite(start_key, end_key)
        For multidimensional: range_query_composite(start_dimensions=start_dims, end_dimensions=end_dims)
        """
        if start_dimensions is not None and end_dimensions is not None:
            return self.range_query_multidim(start_dimensions, end_dimensions)
        elif start_key is not None and end_key is not None:
            if isinstance(start_key, (list, tuple)) and isinstance(end_key, (list, tuple)):
                # Multidimensional range
                return self.range_query_multidim(start_key, end_key)
            else:
                # Single-dimensional range
                return self.range_query(start_key, end_key)
        else:
            raise ValueError("Must provide either single-dimensional keys or multidimensional dimensions")
    
    def insert(self, key, value):
        """Insert a single-dimensional key-value pair (legacy compatibility)"""
        return self._insert_single_value(float(key), value)