from bptree import BPlusTree
import os
import logging

class IndexManager:
    def __init__(self, index_dir):
        self.index_dir = index_dir
        if not os.path.exists(index_dir):
            os.makedirs(index_dir)
        logging.info(f"IndexManager initialized with directory: {index_dir}")
    
    def create_index(self, index_name, column):
        """
        Create a B+ Tree index for a column.
        """
        logging.debug(f"Creating index: {index_name} on column: {column}")
        index_file = os.path.join(self.index_dir, f"{index_name}.idx")
        tree = BPlusTree(order=50)
        tree.save_to_file(index_file)
        logging.info(f"Created index: {index_name} on column: {column}")
        return tree
    
    def get_index(self, index_name):
        """
        Retrieve an existing B+ Tree index.
        """
        logging.debug(f"Retrieving index: {index_name}")
        index_file = os.path.join(self.index_dir, f"{index_name}.idx")
        if not os.path.exists(index_file):
            logging.warning(f"Index file not found: {index_file}")
            return None
        logging.info(f"Retrieved index: {index_name}")
        return BPlusTree.load_from_file(index_file)
    
    def delete_index(self, index_name):
        """
        Delete a B+ Tree index.
        """
        logging.debug(f"Deleting index: {index_name}")
        index_file = os.path.join(self.index_dir, f"{index_name}.idx")
        if os.path.exists(index_file):
            os.remove(index_file)
            logging.info(f"Deleted index: {index_name}")
    
    def insert_into_index(self, index_name, key, value):
        """
        Insert a key-value pair into the B+ Tree index.
        """
        logging.debug(f"Inserting key: {key}, value: {value} into index: {index_name}")
        tree = self.get_index(index_name)
        if tree:
            tree[key] = value
            logging.info(f"Inserted key: {key}, value: {value} into index: {index_name}")
    
    def search_index(self, index_name, key):
        """
        Search for a key in the B+ Tree index.
        """
        logging.debug(f"Searching for key: {key} in index: {index_name}")
        tree = self.get_index(index_name)
        if tree:
            result = tree.get(key)
            logging.info(f"Searched key: {key} in index: {index_name}, found: {result}")
            return result
        return None