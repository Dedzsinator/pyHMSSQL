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

# Define structure for key-value pairs
cdef struct KeyValue:
    # For non-leaf nodes, value_ptr is unused (set to 0)
    double key
    size_t value_ptr

# Define structure for B+ tree nodes
ctypedef struct BPNode:
    bint is_leaf
    int num_keys
    int capacity
    KeyValue* keys
    BPNode** children
    BPNode* next  # For leaf nodes' linked list

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
        if node != NULL:
            if node.keys != NULL:
                free(node.keys)
            if node.children != NULL:
                free(node.children)
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

    cdef bint _validate_node(self, BPNode* node, bint print_error=True) except -1 nogil:
        """Validate a node to ensure it's properly initialized"""
        if node == NULL:
            if print_error:
                with gil:
                    logger.error(f"[{self.name}] Validation error: NULL node")
            return False
        
        # Ensure capacity is valid
        if node.capacity <= 0:
            if print_error:
                with gil:
                    logger.error(f"[{self.name}] Validation error: Invalid capacity {node.capacity}")
            return False
        
        # Check num_keys is in valid range
        if node.num_keys < 0:
            if print_error:
                with gil:
                    logger.error(f"[{self.name}] Validation error: Negative num_keys {node.num_keys}")
            return False
            
        if node.num_keys > node.capacity:
            if print_error:
                with gil:
                    logger.error(f"[{self.name}] Validation error: num_keys {node.num_keys} exceeds capacity {node.capacity}")
            return False
        
        # Check keys array is valid
        if node.keys == NULL:
            if print_error:
                with gil:
                    logger.error(f"[{self.name}] Validation error: NULL keys array")
            return False
        
        # For internal nodes, check children array
        if not node.is_leaf:
            if node.children == NULL:
                if print_error:
                    with gil:
                        logger.error(f"[{self.name}] Validation error: Internal node with NULL children array")
                return False
        
        return True

    def insert(self, key, value):
        """Insert a key-value pair into the B+ tree"""
        # Convert key to double for consistent handling
        cdef double k = float(key)
        cdef BPNode* new_root
        
        # Store value and get its pointer
        self.operation_counter += 1
        cdef size_t value_ptr = self.next_value_id
        self.value_store[value_ptr] = ValueHolder(value)
        self.next_value_id += 1
        
        # Log operation
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"[{self.name}][{self.operation_counter}] INSERT - key: {key}, value: {value}")
        
        # Check if root is NULL or invalid
        if self.root == NULL or not self._validate_node(self.root):
            # Root is invalid, create a new one
            self.root = self._create_node(1)  # Create a new leaf root
        
        # Now insert
        if self.root.num_keys >= self.root.capacity:
            # Root is full, split it
            new_root = self._create_node(0)  # Create new internal root
            if new_root == NULL:
                logger.error(f"[{self.name}] Failed to create new root node")
                return
                
            # Make old root the first child of new root
            new_root.children[0] = self.root
            self.root = new_root
            self._split_child(new_root, 0)
        
        # Finally insert into non-full root
        self._insert_non_full(self.root, k, value_ptr)
        
        # Verify key was inserted
        if logger.isEnabledFor(logging.DEBUG):
            result = self.search(key)
            if result != value:
                logger.error(f"[{self.name}] Key {key} was not inserted correctly")

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
            int i
            KeyValue kv
            object value  # Python object for the value
        
        if node == NULL:
            return
            
        if node.is_leaf:
            # Collect items from leaf
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