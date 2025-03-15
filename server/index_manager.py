from bptree import BPlusTree
import os

class IndexManager:
    def __init__(self, index_dir):
        self.index_dir = index_dir
        if not os.path.exists(index_dir):
            os.makedirs(index_dir)
    
    def create_index(self, index_name, column):
        """
        Create a B+ Tree index for a column.
        """
        index_file = os.path.join(self.index_dir, f"{index_name}.idx")
        tree = BPlusTree(order=50)
        tree.save_to_file(index_file)
        return tree
    
    def get_index(self, index_name):
        """
        Retrieve an existing B+ Tree index.
        """
        index_file = os.path.join(self.index_dir, f"{index_name}.idx")
        if not os.path.exists(index_file):
            return None
        return BPlusTree.load_from_file(index_file)
    
    def delete_index(self, index_name):
        """
        Delete a B+ Tree index.
        """
        index_file = os.path.join(self.index_dir, f"{index_name}.idx")
        if os.path.exists(index_file):
            os.remove(index_file)
    
    def insert_into_index(self, index_name, key, value):
        """
        Insert a key-value pair into the B+ Tree index.
        """
        tree = self.get_index(index_name)
        if tree:
            tree[key] = value
    
    def search_index(self, index_name, key):
        """
        Search for a key in the B+ Tree index.
        """
        tree = self.get_index(index_name)
        if tree:
            return tree.get(key)
        return None