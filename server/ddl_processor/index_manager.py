import logging
import traceback
import os
try:
    from bptree_optimized import BPlusTreeOptimized as BPlusTree
except ImportError:
    # Fallback to adapter if optimized is not available
    from bptree_adapter import BPlusTree


class IndexManager:
    """Handles index operations and management"""

    def __init__(self, catalog_manager):
        self.catalog_manager = catalog_manager
        self.indexes = {}  # In-memory cache of loaded indexes
        self.visualizer = None  # Will be initialized if visualization is needed
        self.logger = logging.getLogger(__name__)  # Add this line

    def get_index(self, full_index_name):
        """Get an index by its full name (table.index)"""
        if full_index_name in self.indexes:
            return self.indexes[full_index_name]

        # Try to load the index
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return None

        parts = full_index_name.split(".")
        if len(parts) != 2:
            return None

        table_name, index_name = parts

        # Get index definition from catalog
        indexes = self.catalog_manager.get_indexes_for_table(table_name)
        if not indexes or index_name not in indexes:
            return None

        index_def = indexes[index_name]
        column = index_def.get("column")

        # Load or build the index
        return self.build_index(
            table_name, index_name, column, index_def.get("unique", False), db_name
        )

    def rebuild_index(self, index_id):
        """Rebuild an index from scratch"""
        try:
            # Parse the index ID to get table and index name
            parts = index_id.split(".")
            if len(parts) != 2:
                self.logger.error(f"Invalid index ID format: {index_id}")
                return False

            table_name, index_name = parts

            # Get index definition from catalog - FIX THE LOOKUP
            indexes = self.catalog_manager.get_indexes_for_table(table_name)
            if not indexes:
                self.logger.error(f"No indexes found for table {table_name}")
                return False

            # Find the specific index - FIX THE SEARCH
            index_def = None

            # Try direct lookup first
            if index_name in indexes:
                index_def = indexes[index_name]
            else:
                # Try fuzzy matching
                for idx_key, idx_info in indexes.items():
                    if idx_key == index_name or idx_key.endswith(f".{index_name}"):
                        index_def = idx_info
                        break

            if not index_def:
                self.logger.error(
                    f"Index definition not found for {index_name}. Available indexes: {list(indexes.keys())}"
                )
                return False

            # Get the column name - handle both single and multiple columns
            columns = index_def.get("columns", [])
            if not columns:
                # Fallback to single column format
                column = index_def.get("column")
                if column:
                    columns = [column]

            if not columns:
                self.logger.error(f"No columns found for index {index_name}")
                return False

            # Use first column for file naming (for backward compatibility)
            main_column = columns[0]
            is_unique = index_def.get("unique", False)

            # Remove old index from cache
            full_index_name = f"{table_name}.{index_name}"
            if full_index_name in self.indexes:
                del self.indexes[full_index_name]

            # Rebuild the index
            rebuilt_index = self.build_index(
                table_name, index_name, main_column, is_unique
            )

            if rebuilt_index:
                self.logger.info(f"Successfully rebuilt index {index_id}")
                return True
            else:
                self.logger.error(f"Failed to rebuild index {index_id}")
                return False

        except Exception as e:
            self.logger.error(f"Error rebuilding index {index_id}: {str(e)}")
            return False

    def build_index(
        self, table_name, index_name, column, is_unique=False, db_name=None
    ):
        """Build or rebuild an index"""
        if db_name is None:
            db_name = self.catalog_manager.get_current_database()
            if not db_name:
                return None

        # Check if index file exists
        index_file = os.path.join(
            self.catalog_manager.indexes_dir,
            f"{
                db_name}_{table_name}_{column}.idx",
        )

        # If file exists, load it
        if os.path.exists(index_file):
            try:
                index = BPlusTree.load_from_file(index_file)
                self.indexes[f"{table_name}.{index_name}"] = index
                return index
            except RuntimeError as e:
                self.logger.error("Error loading index: %s", str(e))
                # Fall through to rebuilding

        # Build the index from table data
        try:
            # Get table data
            records = self.catalog_manager.query_with_condition(table_name, [], ["*"])
            if not records:
                return None

            # Create a new B+ tree
            index = BPlusTree(order=50, name=f"{table_name}_{column}_index")

            # Populate the index
            for record in records:
                if column in record:
                    key = record[column]
                    if is_unique:
                        # For unique indexes, the key maps to a single record ID
                        index.insert(key, record.get("id", hash(str(record))))
                    else:
                        # For non-unique indexes, maintain a list of record IDs
                        existing = index.search(key)
                        record_id = record.get("id", hash(str(record)))

                        if existing is None:
                            index.insert(key, [record_id])
                        elif isinstance(existing, list):
                            existing.append(record_id)
                            index.insert(key, existing)
                        else:
                            index.insert(key, [existing, record_id])

            # Save the index to disk
            index.save_to_file(index_file)

            # Cache the index
            self.indexes[f"{table_name}.{index_name}"] = index
            return index

        except RuntimeError as e:
            self.logger.error("Error building index: %s", str(e))
            self.logger.error(traceback.format_exc())
            return None

    def visualize_all_indexes(self):
        """Visualize all indexes in the current database"""
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return 0

        count = 0
        # Get all tables
        tables = self.catalog_manager.list_tables(db_name)
        for table in tables:
            # Get indexes for this table
            indexes = self.catalog_manager.get_indexes_for_table(table)
            for idx_name in indexes:
                full_name = f"{table}.{idx_name}"
                index = self.get_index(full_name)
                if index:
                    try:
                        index.visualize(self.visualizer, output_name=full_name)
                        count += 1
                    except RuntimeError as e:
                        self.logger.error("Error visualizing index: %s", str(e))

        return count
