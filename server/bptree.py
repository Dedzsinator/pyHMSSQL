import pickle
import logging
import os
import json
from datetime import datetime

# Create a dedicated B+ tree logger
def setup_bptree_logger():
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    logger = logging.getLogger('bptree')
    logger.setLevel(logging.DEBUG)
    
    # Create a file handler for the B+ tree logger
    log_file = os.path.join(log_dir, 'bptree_operations.log')
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    
    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        
    logger.addHandler(file_handler)
    logger.propagate = False  # Don't propagate to root logger
    
    return logger

bptree_logger = setup_bptree_logger()

class BPlusTreeNode:
    def __init__(self, leaf=False):
        self.keys = []
        self.leaf = leaf
        self.children = []
        self.next = None  # For leaf nodes to point to next leaf

class BPlusTree:
    def __init__(self, order=50):
        self.root = BPlusTreeNode(leaf=True)
        self.order = order  # Maximum number of keys per node
        
class BPlusTree:
    def __init__(self, order=50, name=None):
        self.root = BPlusTreeNode(leaf=True)
        self.order = order  # Maximum number of keys per node
        self.name = name or f"tree_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        self.operation_counter = 0
        bptree_logger.info(f"Created new B+ tree '{self.name}' with order {order}")
        
    def insert(self, key, value):
        self.operation_counter += 1
        bptree_logger.debug(f"[{self.name}][{self.operation_counter}] INSERT - key: {key}, value: {value}")
        
        # Log tree state before insert if debugging detail needed
        if bptree_logger.level <= logging.DEBUG:
            bptree_logger.debug(f"Tree before insert: {self._get_tree_structure_json()}")
            
        # Original insert logic
        root = self.root
        if len(root.keys) == (2 * self.order) - 1:
            bptree_logger.debug(f"[{self.name}] Root node is full, creating new root")
            new_root = BPlusTreeNode(leaf=False)
            self.root = new_root
            new_root.children.append(root)
            self._split_child(new_root, 0)
            self._insert_non_full(new_root, key, value)
        else:
            self._insert_non_full(root, key, value)
            
        # Log tree state after insert if debugging detail needed
        if bptree_logger.level <= logging.DEBUG:
            bptree_logger.debug(f"Tree after insert: {self._get_tree_structure_json()}")
    
    def _split_child(self, parent, index):
        self.operation_counter += 1
        order = self.order
        y = parent.children[index]
        z = BPlusTreeNode(leaf=y.leaf)
        
        bptree_logger.debug(f"[{self.name}][{self.operation_counter}] SPLIT - Splitting child at index: {index}")
        bptree_logger.debug(f"[{self.name}] Parent keys: {parent.keys}")
        bptree_logger.debug(f"[{self.name}] Child keys before split: {y.keys}")
        
        # Move the latter half of y's keys to z
        mid = order - 1
        
        if y.leaf:
            # For leaf nodes, we copy the middle key
            z.keys = y.keys[mid:]
            y.keys = y.keys[:mid]
            
            # Connect leaves for range queries
            z.next = y.next
            y.next = z
            
            bptree_logger.debug(f"[{self.name}] Leaf node split - mid point: {mid}")
            bptree_logger.debug(f"[{self.name}] Left node keys after split: {y.keys}")
            bptree_logger.debug(f"[{self.name}] Right node keys after split: {z.keys}")
        else:
            # For internal nodes, we move the middle key up
            middle_key = y.keys[mid]
            z.keys = y.keys[mid+1:]
            parent.keys.insert(index, middle_key)
            y.keys = y.keys[:mid]
            
            # Move the corresponding children
            z.children = y.children[mid+1:]
            y.children = y.children[:mid+1]
            
            bptree_logger.debug(f"[{self.name}] Internal node split - mid key moving up: {middle_key}")
            bptree_logger.debug(f"[{self.name}] Left node keys after split: {y.keys}")
            bptree_logger.debug(f"[{self.name}] Right node keys after split: {z.keys}")
        
        # Insert z as a child of parent
        parent.children.insert(index + 1, z)
        bptree_logger.debug(f"[{self.name}] Parent keys after split: {parent.keys}")
    
    def _insert_non_full(self, node, key, value):
        i = len(node.keys) - 1
        
        if node.leaf:
            # Find the correct position to insert the key
            while i >= 0 and key < node.keys[i][0]:
                i -= 1
            
            # Check if key already exists
            for idx, (k, _) in enumerate(node.keys):
                if k == key:
                    # Replace the value for existing key
                    old_value = node.keys[idx][1]
                    node.keys[idx] = (key, value)
                    bptree_logger.debug(f"[{self.name}] UPDATE - key: {key}, old value: {old_value}, new value: {value}")
                    return
            
            # Insert the new key-value pair
            node.keys.insert(i + 1, (key, value))
            bptree_logger.debug(f"[{self.name}] INSERT LEAF - key: {key}, value: {value}, position: {i + 1}")
        else:
            # Find the child which will have the new key
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1
            
            bptree_logger.debug(f"[{self.name}] INSERT INTERNAL - traversing to child at index {i}")
            
            if len(node.children[i].keys) == (2 * self.order) - 1:
                # If the child is full, split it
                bptree_logger.debug(f"[{self.name}] Child node at index {i} is full, needs splitting")
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1
                    bptree_logger.debug(f"[{self.name}] After split, moving to child at index {i}")
            
            self._insert_non_full(node.children[i], key, value)

    def search(self, key):
        self.operation_counter += 1
        bptree_logger.debug(f"[{self.name}][{self.operation_counter}] SEARCH - key: {key}")
        return self._search(self.root, key)
    
    def _search(self, node, key):
        i = 0
        # Find the first key greater than or equal to k
        while i < len(node.keys) and (node.leaf and key > node.keys[i][0] or not node.leaf and key > node.keys[i]):
            i += 1
        
        if node.leaf:
            # If we're at a leaf, check if we found the key
            if i < len(node.keys) and node.keys[i][0] == key:
                bptree_logger.debug(f"[{self.name}] FOUND - key: {key}, value: {node.keys[i][1]}")
                return node.keys[i][1]  # Return the value
            bptree_logger.debug(f"[{self.name}] NOT FOUND - key: {key}")
            return None  # Key not found
        
        # Recurse to the appropriate child
        if i > 0 and key <= node.keys[i-1]:
            i -= 1
        bptree_logger.debug(f"[{self.name}] SEARCH INTERNAL - traversing to child at index {i}")
        return self._search(node.children[i], key)
    
    def range_query(self, start_key, end_key):
        """Get all values with keys between start_key and end_key."""
        self.operation_counter += 1
        bptree_logger.debug(f"[{self.name}][{self.operation_counter}] RANGE QUERY - from {start_key} to {end_key}")
        result = []
        self._range_query(self.root, start_key, end_key, result)
        bptree_logger.debug(f"[{self.name}] RANGE QUERY RESULT - found {len(result)} entries")
        return result
        
    def visualize(self, visualizer=None, output_name=None):
        """
        Visualize the B+ tree structure
        
        Args:
            visualizer: Optional BPlusTreeVisualizer instance
            output_name: Optional name for the output file
        
        Returns:
            Path to the visualization file or None if visualization failed
        """
        if visualizer is None:
            # Create a new visualizer if not provided
            from bptree_visualizer import BPlusTreeVisualizer
            visualizer = BPlusTreeVisualizer()
    
    return visualizer.visualize_tree(self, output_name)
    
    def _get_tree_structure_json(self):
        """Generate a JSON representation of the tree structure for debugging"""
        def node_to_dict(node, node_id=0):
            node_data = {
                "id": node_id,
                "leaf": node.leaf,
                "keys": [k[0] if node.leaf else k for k in node.keys] if node.leaf else node.keys,
                "children": []
            }
            
            for i, child in enumerate(node.children):
                child_id = node_id * 10 + i + 1
                node_data["children"].append(node_to_dict(child, child_id))
                
            return node_data
        
        return json.dumps(node_to_dict(self.root))
        
    def _range_query(self, node, start_key, end_key, result):
        if node.leaf:
            # Find all keys in the range
            for k, v in node.keys:
                if start_key <= k <= end_key:
                    result.append((k, v))
            
            # If we haven't reached end_key yet, check the next leaf
            if node.next and node.keys and node.keys[-1][0] < end_key:
                self._range_query(node.next, start_key, end_key, result)
        else:
            # Find the first child that might contain keys in range
            i = 0
            while i < len(node.keys) and start_key > node.keys[i]:
                i += 1
            
            # Recurse to children that might contain keys in range
            self._range_query(node.children[i], start_key, end_key, result)
            
            # Continue to next children if needed
            while i < len(node.keys) and node.keys[i] <= end_key:
                self._range_query(node.children[i+1], start_key, end_key, result)
                i += 1
    
    def __getitem__(self, key):
        return self.search(key)
    
    def __setitem__(self, key, value):
        self.insert(key, value)
    
    def get(self, key, default=None):
        result = self.search(key)
        return result if result is not None else default
    
    def save_to_file(self, filename):
        logging.debug(f"Saving B+ tree to file: {filename}")
        with open(filename, 'wb') as f:
            pickle.dump(self, f)
    
    @classmethod
    def load_from_file(cls, filename):
        logging.debug(f"Loading B+ tree from file: {filename}")
        with open(filename, 'rb') as f:
            return pickle.load(f)