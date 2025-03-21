from bptree import BPlusTree, setup_bptree_logger
import os
import logging

class IndexManager:
    def __init__(self, index_dir):
        self.index_dir = index_dir
        if not os.path.exists(index_dir):
            os.makedirs(index_dir)
            
        # Initialize the unified visualizer
        viz_dir = os.path.join(index_dir, 'visualizations')
        from bptree_visualizer import BPTreeVisualizer
        self.visualizer = BPTreeVisualizer(output_dir=viz_dir)
        
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
    
    def build_index(self, table_name, index_name, column_name, is_unique=False, db_name=None):
        """
        Build or rebuild an index from table data.
        
        Args:
            table_name: Name of the table to index
            index_name: Name of the index
            column_name: Name of the column to index
            is_unique: Whether this is a unique index
            db_name: Database name (optional)
        """
        from pymongo import MongoClient
        import os
        
        logging.info(f"Building index: {index_name} on {table_name}.{column_name}")
        
        # Create a full index name for the file
        full_index_name = f"{table_name}.{index_name}"
        
        # Create a new B+ tree
        tree = BPlusTree(order=50, name=full_index_name)
        
        # Connect to MongoDB to get the data
        client = MongoClient()
        
        if not db_name:
            # Try to get the current database from the catalog
            from catalog_manager import CatalogManager
            catalog = CatalogManager(client)
            db_name = catalog.get_current_database()
            
        if not db_name:
            logging.error("No database selected for index building")
            return None
        
        # Get the data from MongoDB
        db = client[db_name]
        collection = db[table_name]
        
        # Get all documents
        documents = collection.find({})
        record_count = 0
        
        # Build the index by adding each document's column value
        for doc in documents:
            if column_name in doc:
                key = doc[column_name]
                document_id = str(doc.get('_id', record_count))
                tree.insert(key, document_id)
                record_count += 1
        
        # Save the index to a file
        os.makedirs(self.index_dir, exist_ok=True)
        index_file = os.path.join(self.index_dir, f"{full_index_name}.idx")
        tree.save_to_file(index_file)
        
        logging.info(f"Built index {index_name} with {record_count} records")
        return tree