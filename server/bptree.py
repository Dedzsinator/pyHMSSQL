import pickle

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
        
    def insert(self, key, value):
        # Find the leaf node where this key should be inserted
        root = self.root
        if len(root.keys) == (2 * self.order) - 1:
            # Root is full, we need to split it
            new_root = BPlusTreeNode(leaf=False)
            self.root = new_root
            new_root.children.append(root)
            self._split_child(new_root, 0)
            self._insert_non_full(new_root, key, value)
        else:
            self._insert_non_full(root, key, value)
    
    def _split_child(self, parent, index):
        order = self.order
        y = parent.children[index]
        z = BPlusTreeNode(leaf=y.leaf)
        
        # Move the latter half of y's keys to z
        mid = order - 1
        
        if y.leaf:
            # For leaf nodes, we copy the middle key
            z.keys = y.keys[mid:]
            y.keys = y.keys[:mid]
            
            # Connect leaves for range queries
            z.next = y.next
            y.next = z
        else:
            # For internal nodes, we move the middle key up
            z.keys = y.keys[mid+1:]
            parent.keys.insert(index, y.keys[mid])
            y.keys = y.keys[:mid]
            
            # Move the corresponding children
            z.children = y.children[mid+1:]
            y.children = y.children[:mid+1]
        
        # Insert z as a child of parent
        parent.children.insert(index + 1, z)
    
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
                    node.keys[idx] = (key, value)
                    return
            
            # Insert the new key-value pair
            node.keys.insert(i + 1, (key, value))
        else:
            # Find the child which will have the new key
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1
            
            if len(node.children[i].keys) == (2 * self.order) - 1:
                # If the child is full, split it
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1
            
            self._insert_non_full(node.children[i], key, value)
    
    def search(self, key):
        return self._search(self.root, key)
    
    def _search(self, node, key):
        i = 0
        # Find the first key greater than or equal to k
        while i < len(node.keys) and (node.leaf and key > node.keys[i][0] or not node.leaf and key > node.keys[i]):
            i += 1
        
        if node.leaf:
            # If we're at a leaf, check if we found the key
            if i < len(node.keys) and node.keys[i][0] == key:
                return node.keys[i][1]  # Return the value
            return None  # Key not found
        
        # Recurse to the appropriate child
        if i > 0 and key <= node.keys[i-1]:
            i -= 1
        return self._search(node.children[i], key)
    
    def range_query(self, start_key, end_key):
        """Get all values with keys between start_key and end_key."""
        result = []
        self._range_query(self.root, start_key, end_key, result)
        return result
    
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
        with open(filename, 'wb') as f:
            pickle.dump(self, f)
    
    @classmethod
    def load_from_file(cls, filename):
        with open(filename, 'rb') as f:
            return pickle.load(f)