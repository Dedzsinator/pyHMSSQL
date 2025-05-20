"""
B+ tree adapter for seamless integration with the DBMS
"""
from bptree_optimized import BPlusTreeOptimized
import logging

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
        # Ensure key is a numeric value
        try:
            numeric_key = float(key) if not isinstance(key, (int, float)) else key
            return self.tree.insert(numeric_key, value)
        except (ValueError, TypeError) as e:
            logging.warning(f"Key conversion error during insert: {str(e)}. Using fallback key.")
            # Use a fallback strategy - hash the key if it's not numeric
            if not isinstance(key, (int, float)):
                hash_key = hash(str(key)) % (2**32)
                return self.tree.insert(float(hash_key), value)
            raise

    def search(self, key):
        """Search for a key"""
        try:
            numeric_key = float(key) if not isinstance(key, (int, float)) else key
            return self.tree.search(numeric_key)
        except (ValueError, TypeError) as e:
            logging.warning(f"Key conversion error during search: {str(e)}. Using fallback key.")
            # Use a fallback strategy - hash the key if it's not numeric
            if not isinstance(key, (int, float)):
                hash_key = hash(str(key)) % (2**32)
                return self.tree.search(float(hash_key))
            return None

    def range_query(self, start_key, end_key):
        """Perform a range query"""
        try:
            numeric_start = float(start_key) if not isinstance(start_key, (int, float)) else start_key
            numeric_end = float(end_key) if not isinstance(end_key, (int, float)) else end_key
            return self.tree.range_query(numeric_start, numeric_end)
        except (ValueError, TypeError) as e:
            logging.error(f"Key conversion error during range query: {str(e)}")
            # For range queries, failing gracefully with empty result is better than crashing
            return []

    def __getitem__(self, key):
        """Dictionary-style access"""
        return self.search(key)

    def __setitem__(self, key, value):
        """Dictionary-style assignment"""
        self.insert(key, value)

    def get(self, key, default=None):
        """Get with default value"""
        result = self.search(key)
        return result if result is not None else default

    def save_to_file(self, filename):
        """Save tree to file"""
        return self.tree.save_to_file(filename)

    @classmethod
    def load_from_file(cls, filename):
        """Load tree from file with error handling"""
        try:
            tree = cls()
            tree.tree = BPlusTreeOptimized.load_from_file(filename)
            if tree.tree is None:
                logging.error(f"Failed to load B+ tree from {filename}")
                return None
            return tree
        except Exception as e:
            logging.error(f"Error loading B+ tree from {filename}: {str(e)}")
            # Create a new tree as a fallback
            return cls(name=f"fallback_{filename.split('/')[-1]}")