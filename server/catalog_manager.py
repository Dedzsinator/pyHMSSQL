import json
import os
import logging
from hashlib import sha256
import datetime
from bptree import BPlusTree
import pickle

class CatalogManager:
    def __init__(self, data_dir='data'):
        # Base directories
        self.data_dir = data_dir
        self.catalog_dir = os.path.join(data_dir, 'catalog')
        self.tables_dir = os.path.join(data_dir, 'tables')
        self.indexes_dir = os.path.join(data_dir, 'indexes')
        
        # Ensure directories exist
        for dir_path in [self.data_dir, self.catalog_dir, self.tables_dir, self.indexes_dir]:
            os.makedirs(dir_path, exist_ok=True)
        
        # Catalog files paths
        self.databases_file = os.path.join(self.catalog_dir, 'databases.json')
        self.tables_file = os.path.join(self.catalog_dir, 'tables.json')
        self.indexes_file = os.path.join(self.catalog_dir, 'indexes.json')
        self.preferences_file = os.path.join(self.catalog_dir, 'preferences.json')
        self.users_file = os.path.join(self.catalog_dir, 'users.json')
        self.views_file = os.path.join(self.catalog_dir, 'views.json')
        self.procedures_file = os.path.join(self.catalog_dir, 'procedures.json')
        self.functions_file = os.path.join(self.catalog_dir, 'functions.json')
        self.triggers_file = os.path.join(self.catalog_dir, 'triggers.json')
        
        # Load or initialize catalog files
        self.databases = self._load_or_init_json(self.databases_file, {})
        self.tables = self._load_or_init_json(self.tables_file, {})
        self.indexes = self._load_or_init_json(self.indexes_file, {})
        self.preferences = self._load_or_init_json(self.preferences_file, {})
        self.users = self._load_or_init_json(self.users_file, [])
        self.views = self._load_or_init_json(self.views_file, {})
        self.procedures = self._load_or_init_json(self.procedures_file, {})
        self.functions = self._load_or_init_json(self.functions_file, {})
        self.triggers = self._load_or_init_json(self.triggers_file, {})
        
        # Temporary tables are still in-memory
        self.temp_tables = {}
        
        # Set current database
        self.current_database = self.preferences.get('current_database')
        
        logging.info("CatalogManager initialized with file-based storage.")
    
    def _load_or_init_json(self, file_path, default_value):
        """Load JSON file or initialize with default value if not exists"""
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logging.error(f"Error decoding {file_path}, initializing with default value")
                return default_value
        else:
            with open(file_path, 'w') as f:
                json.dump(default_value, f, indent=2)
            return default_value
    
    def _save_json(self, file_path, data):
        """Save data to a JSON file"""
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    def register_user(self, username, password, role="user"):
        """Register a new user."""
        # Check if user exists
        for user in self.users:
            if user.get("username") == username:
                return "Username already exists."
        
        # Hash the password
        hashed_password = sha256(password.encode()).hexdigest()
        
        # Add user to the list
        self.users.append({
            "username": username, 
            "password": hashed_password, 
            "role": role
        })
        
        # Save changes
        self._save_json(self.users_file, self.users)
        return "User registered successfully."
    
    def authenticate_user(self, username, password):
        """Authenticate a user."""
        for user in self.users:
            if user.get("username") == username:
                # Verify the password
                hashed_password = sha256(password.encode()).hexdigest()
                if user.get("password") == hashed_password:
                    return user
        return None
    
    def create_database(self, db_name):
        """Create a new database."""
        logging.debug(f"Creating database: {db_name}")
        if db_name in self.databases:
            logging.warning(f"Database {db_name} already exists.")
            return f"Database {db_name} already exists."
        
        # Create database entry
        self.databases[db_name] = {
            "tables": [],
            "created_at": datetime.datetime.now().isoformat()
        }
        
        # Create directory for database tables
        db_tables_dir = os.path.join(self.tables_dir, db_name)
        os.makedirs(db_tables_dir, exist_ok=True)
        
        # Save changes
        self._save_json(self.databases_file, self.databases)
        
        logging.info(f"Database {db_name} created.")
        return f"Database {db_name} created."
    
    def drop_database(self, db_name):
        """Drop a database."""
        logging.debug(f"Dropping database: {db_name}")
        if db_name not in self.databases:
            logging.warning(f"Database {db_name} does not exist.")
            return f"Database {db_name} does not exist."
        
        # Remove database from catalog
        del self.databases[db_name]
        
        # Remove tables belonging to this database
        db_tables = []
        for table_id in list(self.tables.keys()):
            if table_id.startswith(f"{db_name}."):
                db_tables.append(table_id)
                del self.tables[table_id]
        
        # Remove indexes for this database
        for index_id in list(self.indexes.keys()):
            if index_id.startswith(f"{db_name}."):
                del self.indexes[index_id]
        
        # Save changes
        self._save_json(self.databases_file, self.databases)
        self._save_json(self.tables_file, self.tables)
        self._save_json(self.indexes_file, self.indexes)
        
        # Remove physical files
        import shutil
        db_dir = os.path.join(self.tables_dir, db_name)
        if os.path.exists(db_dir):
            shutil.rmtree(db_dir)
        
        logging.info(f"Database {db_name} dropped.")
        return f"Database {db_name} dropped."
    
    def set_current_database(self, database_name):
        """Set the current database for subsequent operations."""
        self.current_database = database_name
        
        # Save to preferences
        self.preferences["current_database"] = database_name
        self._save_json(self.preferences_file, self.preferences)
        
        logging.info(f"Current database set to: {database_name}")
        return True
    
    def get_current_database(self):
        """Get the currently selected database"""
        return self.current_database
    
    def create_table(self, table_name, columns, constraints=None):
        """Create a table in the catalog."""
        if constraints is None:
            constraints = []
        
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."
        
        table_id = f"{db_name}.{table_name}"
        
        # Check if table already exists
        if table_id in self.tables:
            return f"Table {table_name} already exists."
        
        # Create table metadata
        self.tables[table_id] = {
            "database": db_name,
            "name": table_name,
            "columns": columns,
            "constraints": constraints,
            "created_at": datetime.datetime.now().isoformat()
        }
        
        # Add to database's tables list
        if table_name not in self.databases[db_name]["tables"]:
            self.databases[db_name]["tables"].append(table_name)
        
        # Create a B+ tree for the table
        table_file = os.path.join(self.tables_dir, db_name, f"{table_name}.tbl")
        tree = BPlusTree(order=50, name=table_name)
        tree.save_to_file(table_file)
        
        # Save changes
        self._save_json(self.tables_file, self.tables)
        self._save_json(self.databases_file, self.databases)
        
        return True
        
    def get_preferences(self):
        """Get the current preferences."""
        return self.preferences

    def update_preferences(self, prefs, user_id=None):
        """Update user preferences."""
        # Update preferences
        for key, value in prefs.items():
            self.preferences[key] = value
        
        # Save to file
        self._save_json(self.preferences_file, self.preferences)
        
        logging.info(f"Preferences updated: {prefs}")
        return True
    
    def list_databases(self):
        """Get a list of all databases."""
        return list(self.databases.keys())

    def list_tables(self, db_name=None):
        """Get a list of tables in a database."""
        if db_name is None:
            db_name = self.get_current_database()
            if not db_name:
                return []
        
        if db_name not in self.databases:
            return []
        
        return self.databases[db_name].get("tables", [])

    def get_indexes_for_table(self, table_name):
        """Get all indexes for a specific table."""
        result = {}
        db_name = self.get_current_database()
        if not db_name:
            return result
        
        table_id = f"{db_name}.{table_name}"
        
        for index_id, index_info in self.indexes.items():
            parts = index_id.split('.')
            if len(parts) >= 3 and f"{parts[0]}.{parts[1]}" == table_id:
                index_name = parts[2]
                result[index_name] = index_info
        
        return result

    def query_with_condition(self, table_name, conditions=None, columns=None):
        """Query table data with conditions."""
        if conditions is None:
            conditions = []
        if columns is None:
            columns = ['*']
        
        db_name = self.get_current_database()
        if not db_name:
            return []
        
        # Case-insensitive table lookup
        actual_table_name = None
        db_tables = self.list_tables(db_name)
        
        # Direct match first
        if table_name in db_tables:
            actual_table_name = table_name
        else:
            # Try case-insensitive match
            for db_table in db_tables:
                if db_table.lower() == table_name.lower():
                    actual_table_name = db_table
                    break
        
        if not actual_table_name:
            logging.warning(f"Table '{table_name}' not found in database '{db_name}'")
            return []
        
        # Use the correct table name for the lookup
        table_id = f"{db_name}.{actual_table_name}"
        
        # Check if table exists
        if table_id not in self.tables:
            logging.warning(f"Table ID '{table_id}' not found in catalog")
            return []
        
        # Load the table file
        table_file = os.path.join(self.tables_dir, db_name, f"{actual_table_name}.tbl")
        if not os.path.exists(table_file):
            logging.warning(f"Table file not found at: {table_file}")
            return []
        
        try:
            # Load the B+ tree with better error handling
            import pickle  # Add import here to make sure it's available
            tree = None
            
            logging.debug(f"Loading B+ tree from file: {table_file}")
            with open(table_file, 'rb') as f:
                try:
                    # Check for BOM marker and skip it
                    first_bytes = f.read(3)
                    if first_bytes == b'\xef\xbb\xbf':  # UTF-8 BOM
                        logging.warning(f"Found UTF-8 BOM in {table_file}, skipping")
                    else:
                        # Reset to beginning if no BOM
                        f.seek(0)
                    
                    # Load the tree
                    tree = pickle.load(f)
                except Exception as e:
                    logging.error(f"Error loading B+ tree: {str(e)}")
                    # Create a new tree
                    tree = BPlusTree(order=50, name=actual_table_name)
                    tree.save_to_file(table_file)
            
            if tree is None:
                logging.error(f"Failed to load or create B+ tree for {table_file}")
                return []
            
            # Get all records
            all_records = tree.range_query(float('-inf'), float('inf'))
            
            logging.debug(f"Found {len(all_records)} total records in {actual_table_name}")
            
            results = []
            for key, record in all_records:
                # Check if record matches all conditions
                if conditions:
                    logging.debug(f"Checking conditions: {conditions} against record: {record}")
                    
                matches = True
                for condition in conditions:
                    col = condition.get('column')
                    op = condition.get('operator')
                    val = condition.get('value')
                    
                    # Skip conditions with missing parts
                    if not col or not op:
                        continue
                        
                    # Case-insensitive column matching
                    matching_col = None
                    for record_col in record:
                        if record_col.lower() == col.lower():
                            matching_col = record_col
                            break
                    
                    if matching_col is None:
                        matches = False
                        break
                    
                    # Get the value using the matching column name
                    record_val = record[matching_col]
                    logging.debug(f"Comparing {record_val} {op} {val}")
                    
                    # Apply operator (with type conversion as needed)
                    if op == '=':
                        if record_val != val:
                            matches = False
                            break
                    elif op == '>':
                        try:
                            # Try numeric comparison first
                            if float(record_val) <= float(val):
                                matches = False
                                break
                        except (ValueError, TypeError):
                            # Fall back to string comparison
                            if str(record_val) <= str(val):
                                matches = False
                                break
                    elif op == '<':
                        try:
                            if float(record_val) >= float(val):
                                matches = False
                                break
                        except (ValueError, TypeError):
                            if str(record_val) >= str(val):
                                matches = False
                                break
                    elif op == '>=':
                        try:
                            if float(record_val) < float(val):
                                matches = False
                                break
                        except (ValueError, TypeError):
                            if str(record_val) < str(val):
                                matches = False
                                break
                    elif op == '<=':
                        try:
                            if float(record_val) > float(val):
                                matches = False
                                break
                        except (ValueError, TypeError):
                            if str(record_val) > str(val):
                                matches = False
                                break
                    elif op == '!=':
                        if record_val == val:
                            matches = False
                            break
                        
                if matches:
                    # Project selected columns
                    if '*' in columns:
                        results.append(record)
                    else:
                        projected = {}
                        for col in columns:
                            # Case-insensitive column matching
                            for record_col in record:
                                if record_col.lower() == col.lower():
                                    projected[record_col] = record[record_col]
                                    break
                        
                        # Only add records that have at least one matching column
                        if projected:
                            results.append(projected)
            
            logging.debug(f"Returning {len(results)} records after applying conditions")
            return results
        
        except Exception as e:
            logging.error(f"Error querying table: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return []

    def insert_record(self, table_name, record):
        """Insert a record into a table."""
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."
        
        table_id = f"{db_name}.{table_name}"
        
        # Check if table exists
        if table_id not in self.tables:
            return f"Table {table_name} does not exist."
        
        # Get table schema to check constraints
        table_schema = self.tables[table_id]
        pk_column = None
        
        # Find primary key column (if any) - handle both list and dict column definitions
        columns_data = table_schema.get("columns", {})
        
        # Fix for the list issue - check the type and handle accordingly
        if isinstance(columns_data, dict):
            # Dictionary format - this is the expected case
            for col_name, col_def in columns_data.items():
                if isinstance(col_def, dict) and col_def.get("primary_key", False):
                    pk_column = col_name
                    break
        elif isinstance(columns_data, list):
            # List format - this is what's causing the error
            # No primary key in this format, so we skip the check
            logging.debug(f"Table {table_name} has columns in list format, skipping PK check")
        
        # Load the table file
        table_file = os.path.join(self.tables_dir, db_name, f"{table_name}.tbl")
        
        try:
            # Load or create B+ tree
            if os.path.exists(table_file):
                tree = BPlusTree.load_from_file(table_file)
                if tree is None:
                    # If loading failed, create a new tree
                    tree = BPlusTree(order=50, name=table_name)
            else:
                tree = BPlusTree(order=50, name=table_name)
            
            # Check for primary key violation if needed
            if pk_column and pk_column in record:
                pk_value = record[pk_column]
                
                # Check if this primary key already exists
                all_records = tree.range_query(float('-inf'), float('inf'))
                for _, existing_record in all_records:
                    if pk_column in existing_record and existing_record[pk_column] == pk_value:
                        return f"Primary key violation: {pk_column}={pk_value} already exists"
            
            # Generate a unique record ID (use primary key if available)
            if pk_column and pk_column in record:
                record_id = record[pk_column]
            else:
                record_id = int(datetime.datetime.now().timestamp() * 1000)  # Unique ID based on timestamp
            
            # Insert the record into the B+ tree
            tree.insert(record_id, record)
            
            # Save the updated tree
            tree.save_to_file(table_file)
            
            logging.info(f"Record inserted into {table_name} with ID {record_id}")
            
            # Update indexes if any
            self._update_indexes_after_insert(db_name, table_name, record_id, record)
            
            return True
                
        except Exception as e:
            logging.error(f"Error inserting record: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return f"Error inserting record: {str(e)}"

    def _update_indexes_after_insert(self, db_name, table_name, record_id, record):
        """Update indexes after a record is inserted."""
        table_id = f"{db_name}.{table_name}"
        
        # Check if we have any indexes to update
        indexes_to_update = {}
        for index_id, index_info in self.indexes.items():
            if index_info.get("table") == table_id:
                column = index_info.get("column")
                if column and column in record:
                    if index_id not in indexes_to_update:
                        indexes_to_update[index_id] = index_info
        
        # Update each relevant index
        for index_id, index_info in indexes_to_update.items():
            column = index_info.get("column")
            index_filename = f"{db_name}_{table_name}_{column}.idx"
            index_path = os.path.join(self.indexes_dir, index_filename)
            
            # Load or create the index
            if os.path.exists(index_path):
                try:
                    index_tree = BPlusTree.load_from_file(index_path)
                    if index_tree is None:
                        index_tree = BPlusTree(order=50, name=f"{table_name}_{column}_index")
                except:
                    index_tree = BPlusTree(order=50, name=f"{table_name}_{column}_index")
            else:
                index_tree = BPlusTree(order=50, name=f"{table_name}_{column}_index")
            
            # Add the record to the index
            column_value = record.get(column)
            if column_value is not None:
                # For unique indexes, check if value already exists
                if index_info.get("unique", False):
                    existing = index_tree.search(column_value)
                    if existing:
                        # Already indexed, but could be the same record (update case)
                        if existing != record_id:
                            logging.warning(f"Unique constraint violation on {column}={column_value}")
                            continue
                
                # Add or update the index entry
                index_tree.insert(column_value, record_id)
                
                # Save the index
                index_tree.save_to_file(index_path)

    def delete_records(self, table_name, conditions=None):
        """Delete records from a table based on conditions."""
        if conditions is None:
            conditions = []
            
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."
        
        table_id = f"{db_name}.{table_name}"
        
        # Check if table exists with case-insensitive matching
        actual_table_name = None
        tables = self.list_tables(db_name)
        
        if table_name in tables:
            actual_table_name = table_name
        else:
            for t in tables:
                if t.lower() == table_name.lower():
                    actual_table_name = t
                    break
        
        if not actual_table_name:
            return f"Table '{table_name}' does not exist."
        
        # Load the B+ tree
        table_file = os.path.join(self.tables_dir, db_name, f"{actual_table_name}.tbl")
        if not os.path.exists(table_file):
            return f"Table data file not found."
        
        try:
            # Load the B+ tree
            tree = BPlusTree.load_from_file(table_file)
            
            # Get all records
            all_records = tree.range_query(float('-inf'), float('inf'))
            
            # Track records to delete
            records_to_delete = []
            
            for record_key, record in all_records:
                # Check if record matches all conditions
                matches = True
                
                for condition in conditions:
                    col = condition.get('column')
                    op = condition.get('operator')
                    val = condition.get('value')
                    
                    # Case-insensitive column matching
                    matching_col = None
                    for record_col in record:
                        if record_col.lower() == col.lower():
                            matching_col = record_col
                            break
                    
                    if matching_col is None:
                        matches = False
                        break
                    
                    # Get the value using the matching column name
                    record_val = record[matching_col]
                    
                    # Apply operator
                    if op == '=':
                        if record_val != val:
                            matches = False
                            break
                    elif op == '>':
                        if record_val <= val:
                            matches = False
                            break
                    elif op == '<':
                        if record_val >= val:
                            matches = False
                            break
                    elif op == '>=':
                        if record_val < val:
                            matches = False
                            break
                    elif op == '<=':
                        if record_val > val:
                            matches = False
                            break
                    elif op == '!=':
                        if record_val == val:
                            matches = False
                            break
                
                # If record matches conditions, add it to the deletion list
                if matches:
                    records_to_delete.append(record_key)
            
            # Create a new tree without the deleted records
            new_tree = BPlusTree(order=50, name=actual_table_name)
            deleted_count = 0
            
            # Add all records except those marked for deletion
            for record_key, record in all_records:
                if record_key not in records_to_delete:
                    new_tree.insert(record_key, record)
                else:
                    deleted_count += 1
            
            # Save the updated tree
            new_tree.save_to_file(table_file)
            
            # Update indexes if needed
            if hasattr(self, '_update_indexes_after_modify'):
                remaining_records = new_tree.range_query(float('-inf'), float('inf'))
                self._update_indexes_after_modify(db_name, actual_table_name, remaining_records)
            
            return f"{deleted_count} records deleted."
        except Exception as e:
            logging.error(f"Error deleting records: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return f"Error deleting records: {str(e)}"

    def update_records(self, table_name, updates, conditions=None):
        """Update records in a table based on conditions."""
        if conditions is None:
            conditions = []
        
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."
        
        table_id = f"{db_name}.{table_name}"
        
        # Check if table exists
        if table_id not in self.tables:
            return f"Table {table_name} does not exist."
        
        # Load the table file
        table_file = os.path.join(self.tables_dir, db_name, f"{table_name}.tbl")
        if not os.path.exists(table_file):
            return "Table data file not found."
        
        try:
            # Load B+ tree
            tree = BPlusTree.load_from_file(table_file)
            
            # Get all records
            all_records = tree.range_query(float('-inf'), float('inf'))
            
            # Process records and apply updates based on conditions
            updated_count = 0
            for record_key, record in all_records:
                # Check if record matches all conditions
                matches = True
                for condition in conditions:
                    col = condition.get('column')
                    op = condition.get('operator')
                    val = condition.get('value')
                    
                    if col not in record:
                        matches = False
                        break
                    
                    # Apply operator
                    if op == '=':
                        if record[col] != val:
                            matches = False
                            break
                    # Add other operators as needed
                
                if matches or not conditions:
                    # Apply updates
                    updated_record = record.copy()
                    for field, value in updates.items():
                        updated_record[field] = value
                    
                    # Update the record
                    tree.insert(record_key, updated_record)
                    updated_count += 1
            
            # Save the updated tree
            tree.save_to_file(table_file)
            
            # Update indexes if needed
            
            return f"{updated_count} records updated."
        
        except Exception as e:
            logging.error(f"Error updating records: {e}")
            return f"Error updating records: {str(e)}"

    def create_index(self, table_name, column_name, index_type="BTREE", is_unique=False):
        """Create an index on a table column."""
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."
        
        table_id = f"{db_name}.{table_name}"
        index_id = f"{table_id}.{column_name}"
        
        # Check if table exists
        if table_id not in self.tables:
            return f"Table {table_name} does not exist."
        
        # Check if index already exists
        if index_id in self.indexes:
            return f"Index on {table_name}.{column_name} already exists."
        
        # Add to indexes catalog
        self.indexes[index_id] = {
            "table": table_id,
            "column": column_name,
            "type": index_type,
            "unique": is_unique,
            "created_at": datetime.datetime.now().isoformat()
        }
        
        # Build the index
        table_file = os.path.join(self.tables_dir, db_name, f"{table_name}.tbl")
        index_file = os.path.join(self.indexes_dir, f"{db_name}_{table_name}_{column_name}.idx")
        
        try:
            # Load table data
            if os.path.exists(table_file):
                table_tree = BPlusTree.load_from_file(table_file)
                all_records = table_tree.range_query(float('-inf'), float('inf'))
                
                # Create index tree
                index_tree = BPlusTree(order=50, name=f"{table_name}_{column_name}_index")
                
                # Populate index
                for record_key, record in all_records:
                    if column_name in record:
                        index_tree.insert(record[column_name], record_key)
                
                # Save index
                index_tree.save_to_file(index_file)
            
            # Save changes to catalog
            self._save_json(self.indexes_file, self.indexes)
            
            logging.info(f"Index created on {table_name}.{column_name}")
            return f"Index created on {table_name}.{column_name}"
        
        except Exception as e:
            logging.error(f"Error creating index: {e}")
            if index_id in self.indexes:
                del self.indexes[index_id]
            return f"Error creating index: {str(e)}"

    def drop_index(self, table_name, index_name):
        """Drop an index from a table."""
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."
        
        table_id = f"{db_name}.{table_name}"
        index_id = f"{table_id}.{index_name}"
        
        # Check if index exists
        if index_id not in self.indexes:
            return f"Index {index_name} on {table_name} does not exist."
        
        # Get the column name from the index info
        column_name = self.indexes[index_id].get("column")
        
        # Remove from indexes catalog
        del self.indexes[index_id]
        
        # Remove physical file
        index_file = os.path.join(self.indexes_dir, f"{db_name}_{table_name}_{column_name}.idx")
        if os.path.exists(index_file):
            os.remove(index_file)
        
        # Save changes
        self._save_json(self.indexes_file, self.indexes)
        
        logging.info(f"Index {index_name} dropped from {table_name}")
        return f"Index {index_name} dropped from {table_name}"

    def begin_transaction(self):
        """Begin a new transaction (stub for now)."""
        return "Transaction started (Note: transactions not fully implemented)"

    def commit_transaction(self):
        """Commit the current transaction (stub for now)."""
        return "Transaction committed (Note: transactions not fully implemented)"

    def rollback_transaction(self):
        """Rollback the current transaction (stub for now)."""
        return "Transaction rolled back (Note: transactions not fully implemented)"

    def create_view(self, view_name, query):
        """Create a view."""
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."
        
        view_id = f"{db_name}.{view_name}"
        
        # Check if view already exists
        if view_id in self.views:
            return f"View {view_name} already exists."
        
        # Add to views catalog
        self.views[view_id] = {
            "database": db_name,
            "name": view_name,
            "query": query,
            "created_at": datetime.datetime.now().isoformat()
        }
        
        # Save changes
        self._save_json(self.views_file, self.views)
        
        logging.info(f"View {view_name} created")
        return f"View {view_name} created"

    def drop_view(self, view_name):
        """Drop a view."""
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."
        
        view_id = f"{db_name}.{view_name}"
        
        # Check if view exists
        if view_id not in self.views:
            return f"View {view_name} does not exist."
        
        # Remove from views catalog
        del self.views[view_id]
        
        # Save changes
        self._save_json(self.views_file, self.views)
        
        logging.info(f"View {view_name} dropped")
        return f"View {view_name} dropped"