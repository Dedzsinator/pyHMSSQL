"""
B+ tree adapter for seamless integration with the DBMS
"""
from bptree_optimized import BPlusTreeOptimized

class BPlusTree:
    """
    Adapter class that provides consistent interface for B+ tree
    implementations, but uses the optimized implementation internally.
    """
    def __init__(self, order=75, name=None):
        """Initialize with the optimal order based on benchmarking"""
        self.tree = BPlusTreeOptimized(order=order, name=name)
    
    def insert(self, key, value):
        """Insert a key-value pair"""
        return self.tree.insert(key, value)
    
    def search(self, key):
        """Search for a key"""
        return self.tree.search(key)
    
    def range_query(self, start_key, end_key):
        """Perform a range query"""
        return self.tree.range_query(start_key, end_key)
    
    def __getitem__(self, key):
        """Dictionary-style access"""
        return self.tree[key]
    
    def __setitem__(self, key, value):
        """Dictionary-style assignment"""
        self.tree[key] = value
    
    def get(self, key, default=None):
        """Get with default value"""
        return self.tree.get(key, default)
    
    def save_to_file(self, filename):
        """Save tree to file"""
        return self.tree.save_to_file(filename)
    
    @classmethod
    def load_from_file(cls, filename):
        """Load tree from file"""
        tree = cls()
        tree.tree = BPlusTreeOptimized.load_from_file(filename)
        return tree