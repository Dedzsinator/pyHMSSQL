from bptree import BPlusTree, setup_bptree_logger
from bptreeGraph import BPlusTreeVisualizer
import os
import logging

class IndexManager:
    def __init__(self, index_dir):
        self.index_dir = index_dir
        if not os.path.exists(index_dir):
            os.makedirs(index_dir)
            
        # Initialize the visualizer
        viz_dir = os.path.join(index_dir, 'visualizations')
        self.visualizer = BPlusTreeVisualizer(output_dir=viz_dir)
        
        logging.info(f"IndexManager initialized with directory: {index_dir}")
    
    def create_index(self, index_name, column):
        """
        Create a B+ Tree index for a column.
        """
        logging.info(f"Creating index: {index_name} on column: {column}")
        index_file = os.path.join(self.index_dir, f"{index_name}.idx")
        
        # Create the B+ tree with a meaningful name
        tree = BPlusTree(order=50, name=index_name)
        
        # Save the tree
        tree.save_to_file(index_file)
        
        # Create an initial visualization of the empty tree
        tree.visualize(self.visualizer, f"{index_name}_initial")
        
        logging.info(f"Created index: {index_name} on column: {column}")
        return tree
    
    def get_index(self, index_name):
        """
        Retrieve an existing B+ Tree index.
        """
        logging.info(f"Retrieving index: {index_name}")
        index_file = os.path.join(self.index_dir, f"{index_name}.idx")
        
        if not os.path.exists(index_file):
            logging.warning(f"Index file not found: {index_file}")
            return None
            
        # Load the tree
        tree = BPlusTree.load_from_file(index_file)
        
        # Set the name if not already set
        if not hasattr(tree, 'name') or not tree.name:
            tree.name = index_name
            
        logging.info(f"Retrieved index: {index_name}")
        return tree
    
    def update_index(self, index_name, key, value):
        """
        Update a value in the index or insert if it doesn't exist.
        """
        logging.info(f"Updating index: {index_name} with key: {key}")
        tree = self.get_index(index_name)
        
        if tree is None:
            logging.warning(f"Cannot update non-existent index: {index_name}")
            return False
            
        # Insert/update the key-value pair
        tree.insert(key, value)
        
        # Visualize the updated tree
        tree.visualize(self.visualizer, f"{index_name}_after_update_{key}")
        
        # Save the updated tree
        index_file = os.path.join(self.index_dir, f"{index_name}.idx")
        tree.save_to_file(index_file)
        
        logging.info(f"Updated index: {index_name}")
        return True
    
    def delete_index(self, index_name):
        """
        Delete a B+ Tree index.
        """
        logging.info(f"Deleting index: {index_name}")
        index_file = os.path.join(self.index_dir, f"{index_name}.idx")
        
        if os.path.exists(index_file):
            # Visualize the tree before deletion for history
            tree = self.get_index(index_name)
            if tree:
                tree.visualize(self.visualizer, f"{index_name}_before_deletion")
                
            # Delete the file
            os.remove(index_file)
            logging.info(f"Deleted index: {index_name}")
            return True
        else:
            logging.warning(f"Index file not found: {index_file}")
            return False
    
    def visualize_all_indexes(self):
        """
        Generate visualizations for all indexes.
        """
        logging.info(f"Visualizing all indexes in {self.index_dir}")
        
        # Find all index files
        index_files = [f for f in os.listdir(self.index_dir) if f.endswith('.idx')]
        
        for idx_file in index_files:
            index_name = os.path.splitext(idx_file)[0]
            tree = self.get_index(index_name)
            
            if tree:
                tree.visualize(self.visualizer, f"{index_name}_snapshot")
                
        logging.info(f"Visualized {len(index_files)} indexes")
        return len(index_files)