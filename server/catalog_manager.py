import json
import os
import re
import traceback
import shutil
import logging
import time
import datetime
from hashlib import sha256
import datetime
from bptree_wrapper import BPlusTreeFactory

class CatalogManager:
    """_summary_
    """
    def __init__(self, data_dir="data"):
        # Base directories
        self.data_dir = data_dir
        self.catalog_dir = os.path.join(data_dir, "catalog")
        self.tables_dir = os.path.join(data_dir, "tables")
        self.indexes_dir = os.path.join(data_dir, "indexes")

        # Ensure directories exist
        for dir_path in [
            self.data_dir,
            self.catalog_dir,
            self.tables_dir,
            self.indexes_dir,
        ]:
            os.makedirs(dir_path, exist_ok=True)

        # Catalog files paths
        self.databases_file = os.path.join(self.catalog_dir, "databases.json")
        self.tables_file = os.path.join(self.catalog_dir, "tables.json")
        self.indexes_file = os.path.join(self.catalog_dir, "indexes.json")
        self.preferences_file = os.path.join(
            self.catalog_dir, "preferences.json")
        self.users_file = os.path.join(self.catalog_dir, "users.json")
        self.views_file = os.path.join(self.catalog_dir, "views.json")
        self.procedures_file = os.path.join(
            self.catalog_dir, "procedures.json")
        self.functions_file = os.path.join(self.catalog_dir, "functions.json")
        self.triggers_file = os.path.join(self.catalog_dir, "triggers.json")

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
        self.in_batch_mode = False
        self.batch_operations = {}

        # Set current database
        self.current_database = self.preferences.get("current_database")

        logging.info("CatalogManager initialized with file-based storage.")

    def _load_or_init_json(self, file_path, default_value):
        """Load JSON file or initialize with default value if not exists"""
        if os.path.exists(file_path):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logging.error("Error decoding %s, initializing with default value", file_path)
                return default_value
        else:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(default_value, f, indent=2)
            return default_value

    def _save_json(self, file_path, data):
        """Save data to a JSON file"""
        with open(file_path, "w", encoding="utf-8") as f:
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
        self.users.append(
            {"username": username, "password": hashed_password, "role": role}
        )

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

    def begin_batch_operation(self):
        """Start a batch operation mode."""
        self.in_batch_mode = True
        self.batch_operations = {}
        logging.info("Started batch operation mode")

    def end_batch_operation(self, commit=True):
        """End a batch operation mode."""
        if not hasattr(self, 'in_batch_mode') or not self.in_batch_mode:
            return

        self.in_batch_mode = False

        if commit:
            logging.info("Committing batch operations")
            for table_path, tree in self.batch_operations.items():
                # Save the updated tree
                tree.save_to_file(table_path)
                logging.info(f"Saved batch tree for {table_path}")
        else:
            logging.info("Rolling back batch operations")

        # Clear batch operations
        self.batch_operations = {}

    def insert_records_batch(self, table_name, records):
        """Insert multiple records in a batch - hyperoptimized version."""
        db_name = self.get_current_database()
        if not db_name:
            return {"error": "No database selected", "status": "error"}

        table_id = f"{db_name}.{table_name}"
        if table_id not in self.tables:
            return {"error": f"Table '{table_name}' not found", "status": "error"}

        # Load the table file
        table_file = os.path.join(self.tables_dir, db_name, f"{table_name}.tbl")

        try:
            # Load existing B+ tree or create new one
            if os.path.exists(table_file):
                tree = BPlusTreeFactory.load_from_file(table_file)
            else:
                tree = BPlusTreeFactory.create(order=50, name=f"{db_name}_{table_name}")

            # Get table schema for validation
            table_schema = self.tables[table_id]
            
            # Insert each record
            inserted_count = 0
            current_time = int(time.time() * 1000000)  # Get base timestamp
            
            for i, record in enumerate(records):
                try:
                    # Generate a numeric key instead of string key
                    # Use timestamp + index to ensure uniqueness
                    record_key = float(current_time + i)
                    
                    # Validate record against schema if needed
                    # (Add validation logic here if required)
                    
                    # Insert into B+ tree with numeric key
                    tree.insert(record_key, record)
                    inserted_count += 1
                    
                except Exception as e:
                    logging.error(f"Error inserting record {i}: {str(e)}")
                    continue

            # Save the updated tree back to file
            tree.save_to_file(table_file)
            
            logging.info(f"Successfully inserted {inserted_count} records into {table_name}")
            
            return {
                "status": "success",
                "inserted_count": inserted_count,
                "message": f"Inserted {inserted_count} records"
            }
            
        except Exception as e:
            logging.error(f"Batch insert failed: {str(e)}")
            return {"error": f"Batch insert failed: {str(e)}", "status": "error"}

    def create_database(self, db_name):
        """Create a new database."""
        logging.debug("Creating database: %s", db_name)
        if db_name in self.databases:
            logging.warning("Database %s already exists.", db_name)
            return f"Database {db_name} already exists."

        # Create database entry
        self.databases[db_name] = {
            "tables": [],
            "created_at": datetime.datetime.now().isoformat(),
        }

        # Create directory for database tables
        db_tables_dir = os.path.join(self.tables_dir, db_name)
        os.makedirs(db_tables_dir, exist_ok=True)

        # Save changes
        self._save_json(self.databases_file, self.databases)

        logging.info("Database %s created.", db_name)
        return f"Database {db_name} created."

    def drop_database(self, db_name):
        """Drop a database."""
        logging.debug("Dropping database: %s", db_name)
        if db_name not in self.databases:
            logging.warning("Database %s does not exist.", db_name)
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

        db_dir = os.path.join(self.tables_dir, db_name)
        if os.path.exists(db_dir):
            shutil.rmtree(db_dir)

        logging.info("Database %s dropped.", db_name)
        return f"Database %s dropped."

    def get_all_tables(self):
        """Get a list of all tables across all databases.

        Returns:
            list: A list of tuples (database_name, table_name) for all tables
        """
        all_tables = []
        current_db = self.get_current_database()

        # Iterate through all databases
        for db_name in self.list_databases():
            # Get tables for this database
            tables = self.list_tables(db_name)
            for table_name in tables:
                all_tables.append([db_name, table_name])

        # Restore original database if needed
        if current_db:
            self.set_current_database(current_db)

        logging.info("Retrieved %s tables across all databases", len(all_tables))
        return all_tables

    def set_current_database(self, database_name):
        """Set the current database for subsequent operations."""
        self.current_database = database_name

        # Save to preferences
        self.preferences["current_database"] = database_name
        self._save_json(self.preferences_file, self.preferences)

        logging.info("Current database set to: %s", database_name)
        return True

    def get_current_database(self):
        """Get the currently selected database"""
        return self.current_database

    def create_table(self, table_name, columns, constraints=None):
        """Create a table in the catalog with compound key support."""
        table_level_constraints = []
        compound_pk_columns = []
        
        if constraints is not None:
            for constraint in constraints:
                if isinstance(constraint, dict) and constraint.get("type") == "PRIMARY_KEY":
                    compound_pk_columns = constraint.get("columns", [])
                    table_level_constraints.append(constraint)
                else:
                    table_level_constraints.append(constraint)

        db_name = self.get_current_database()
        if not db_name:
            return "No database selected"

        table_id = f"{db_name}.{table_name}"

        if table_id in self.tables:
            return f"Table {table_name} already exists"

        processed_columns = {}

        # Process column definitions
        temp_columns_list_for_processing = []
        if isinstance(columns, list):
            for item in columns:
                if isinstance(item, str):
                    item_stripped = item.strip()
                    item_upper = item_stripped.upper()
                    if item_upper.startswith("FOREIGN KEY") or \
                    item_upper.startswith("UNIQUE (") or \
                    item_upper.startswith("PRIMARY KEY (") or \
                    item_upper.startswith("CHECK ("):
                        if item_stripped not in table_level_constraints:
                            table_level_constraints.append(item_stripped)
                    else:
                        temp_columns_list_for_processing.append(item_stripped)
                elif isinstance(item, dict):
                    temp_columns_list_for_processing.append(item)
        else:
            temp_columns_list_for_processing = [columns]

        for col_def_source in temp_columns_list_for_processing:
            col_name = None
            column_attributes = {}
            
            if isinstance(col_def_source, dict):
                col_name = col_def_source.get("name")
                if not col_name:
                    logging.warning("Column definition dictionary missing name in table '%s': %s",
                                table_name, col_def_source)
                    continue
                column_attributes = {key: value for key, value in col_def_source.items() if key != "name"}

            elif isinstance(col_def_source, str):
                parts = col_def_source.split()
                if not parts:
                    logging.warning("Empty column definition string in table '%s'.", table_name)
                    continue

                col_name = parts[0]
                column_attributes["type"] = parts[1] if len(parts) > 1 else "UNKNOWN"

                col_def_upper_str = col_def_source.upper()

                if "NOT NULL" in col_def_upper_str:
                    column_attributes["not_null"] = True

                if "PRIMARY KEY" in col_def_upper_str and "PRIMARY KEY (" not in col_def_upper_str:
                    column_attributes["primary_key"] = True

                if "UNIQUE" in col_def_upper_str and "UNIQUE (" not in col_def_upper_str and not column_attributes.get("primary_key"):
                    column_attributes["unique"] = True

                if "IDENTITY" in col_def_upper_str:
                    column_attributes["identity"] = True
                    identity_match = re.search(r"IDENTITY\s*\((\d+),\s*(\d+)\)", col_def_source, re.IGNORECASE)
                    if identity_match:
                        column_attributes["identity_seed"] = int(identity_match.group(1))
                        column_attributes["identity_increment"] = int(identity_match.group(2))
                    else:
                        column_attributes["identity_seed"] = 1
                        column_attributes["identity_increment"] = 1

            # Check if this column is part of compound primary key
            if col_name in compound_pk_columns:
                column_attributes["primary_key"] = True
                column_attributes["compound_pk"] = len(compound_pk_columns) > 1
                column_attributes["pk_position"] = compound_pk_columns.index(col_name)

            if col_name:
                if col_name in processed_columns:
                    logging.warning("Duplicate column name '%s' in table '%s'. Overwriting.", col_name, table_name)
                processed_columns[col_name] = column_attributes

        self.tables[table_id] = {
            "database": db_name,
            "name": table_name,
            "columns": processed_columns,
            "constraints": table_level_constraints,
            "compound_primary_key": compound_pk_columns if len(compound_pk_columns) > 1 else None,
            "created_at": datetime.datetime.now().isoformat(),
        }

        # Create table structure
        if table_name not in self.databases[db_name]["tables"]:
            self.databases[db_name]["tables"].append(table_name)

        db_specific_tables_dir = os.path.join(self.tables_dir, db_name)
        os.makedirs(db_specific_tables_dir, exist_ok=True)
        table_file = os.path.join(db_specific_tables_dir, f"{table_name}.tbl")

        # Create a new tree using the factory
        tree = BPlusTreeFactory.create(order=50, name=f"{db_name}_{table_name}")
        tree.save_to_file(table_file)

        self._save_json(self.tables_file, self.tables)
        self._save_json(self.databases_file, self.databases)

        logging.info("Table '%s' created with compound PK: %s", table_name, compound_pk_columns)
        return True

    def _generate_compound_key(self, record, pk_columns):
        """Generate a compound key from multiple columns."""
        if not pk_columns:
            return str(time.time() * 1000000)
        
        if len(pk_columns) == 1:
            # Single primary key
            return str(record.get(pk_columns[0], ''))
        
        # Compound primary key
        key_parts = []
        for col in pk_columns:
            if col in record:
                key_parts.append(str(record[col]))
            else:
                raise ValueError(f"Primary key column '{col}' missing from record")
        
        # Create compound key with separator
        return "|".join(key_parts)

    def get_preferences(self):
        """Get the current preferences."""
        return self.preferences

    def update_preferences(self, prefs, _ =None): # user_id was not used
        """Update user preferences."""
        # Update preferences
        for key, value in prefs.items():
            self.preferences[key] = value

        # Save to file
        self._save_json(self.preferences_file, self.preferences)

        logging.info("Preferences updated: %s", prefs)
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
            parts = index_id.split(".")
            if len(parts) >= 3 and f"{parts[0]}.{parts[1]}" == table_id:
                index_name = parts[2]
                result[index_name] = index_info

        return result

    def _convert_to_numeric(self, value):
        """Convert value to numeric if possible."""
        if isinstance(value, (int, float)):
            return value

        if isinstance(value, str):
            # Remove quotes if present
            if value.startswith(("'", '"')) and value.endswith(("'", '"')):
                value = value[1:-1]

            # Try to convert to numeric
            try:
                if '.' in value:
                    return float(value)
                return int(value)
            except ValueError:
                return value
        return value

    def _record_matches_conditions(self, record, conditions):
        """Check if a record matches the provided conditions."""
        if not conditions:
            return True  # No conditions, record matches

        for condition in conditions:
            column = condition.get("column")
            operator = condition.get("operator")
            value = condition.get("value")

            if column not in record:
                return False

            record_value = record[column]

            # Convert both to numeric if possible
            value = self._convert_to_numeric(value)
            record_value = self._convert_to_numeric(record_value)

            # Handle different operators
            if operator == "=":
                if record_value != value:
                    return False
            elif operator == "!=":
                if record_value == value:
                    return False
            elif operator == ">":
                if not record_value > value:
                    return False
            elif operator == ">=":
                if not record_value >= value:
                    return False
            elif operator == "<":
                if not record_value < value:
                    return False
            elif operator == "<=":
                if not record_value <= value:
                    return False
            elif operator.upper() == "LIKE":
                if not self._matches_like_pattern(str(record_value), str(value)):
                    return False
            elif operator.upper() == "IN":
                if not self._matches_in_condition(record_value, value):
                    return False

        # All conditions passed
        return True

    def _matches_like_pattern(self, record_value, pattern):
        """Check if a value matches a LIKE pattern.

        Args:
            record_value: String value from the record
            pattern: LIKE pattern (with % wildcards)

        Returns:
            bool: True if the value matches the pattern
        """
        # Remove quotes if present
        if pattern.startswith(("'", '"')) and pattern.endswith(("'", '"')):
            pattern = pattern[1:-1]

        # Convert SQL LIKE pattern to regex
        # % in SQL LIKE is .* in regex
        # _ in SQL LIKE is . in regex
        # Escape other regex special characters
        regex_pattern = pattern.replace('%', '.*').replace('_', '.')
        regex_pattern = f"^{regex_pattern}$"  # Match whole string

        return re.match(regex_pattern, record_value, re.IGNORECASE) is not None

    def _matches_in_condition(self, record_value, value_list):
        """Check if a value is in a list for IN operator.

        Args:
            record_value: Value from the record
            value_list: List of values or string representing a list

        Returns:
            bool: True if the value is in the list
        """
        # Convert string representation to list if needed
        if isinstance(value_list, str):
            if value_list.startswith('(') and value_list.endswith(')'):
                value_list = value_list[1:-1]
            # Split by comma and strip whitespace and quotes
            items = []
            for item in value_list.split(','):
                item = item.strip()
                if item.startswith(("'", '"')) and item.endswith(("'", '"')):
                    item = item[1:-1]
                items.append(item)
            value_list = items

        return record_value in value_list

    def query_with_condition(self, table_name, conditions=None, columns=None, limit=None, order_by=None):
        """Query table data with conditions."""
        if conditions is None:
            conditions = []
        if columns is None:
            columns = ["*"]

        db_name = self.get_current_database()
        if not db_name:
            return []

        # Case-insensitive table lookup - PRESERVE ORIGINAL CASE
        actual_table_name = None
        db_tables = self.list_tables(db_name)

        # Direct match first (preserve case)
        if table_name in db_tables:
            actual_table_name = table_name
        else:
            # Case-insensitive match but preserve the case from the database
            for db_table in db_tables:
                if db_table.lower() == table_name.lower():
                    actual_table_name = db_table  # Use the case from the database
                    break

        if not actual_table_name:
            logging.error("Table '%s' not found. Available tables: %s", table_name, db_tables)
            return []

        # Use the correct table name for the lookup
        table_id = f"{db_name}.{actual_table_name}"

        # Check if table exists
        if table_id not in self.tables:
            logging.error("Table ID '%s' not found in catalog", table_id)
            return []

        # Try to use index for simple equality conditions
        index_to_use = None
        condition_value = None

        if len(conditions) == 1 and conditions[0].get("operator") == "=":
            condition_column = conditions[0].get("column")
            condition_value = conditions[0].get("value")
            
            # Look for an index on this column
            table_indexes = self.get_indexes_for_table(actual_table_name)
            for idx_name, idx_info in table_indexes.items():
                if idx_info.get("column") == condition_column:
                    index_to_use = f"{db_name}_{actual_table_name}_{condition_column}.idx"
                    break

        if index_to_use and condition_value is not None:
            # Use index for lookup
            index_file = os.path.join(self.indexes_dir, index_to_use)
            if os.path.exists(index_file):
                try:
                    index_tree = BPlusTreeFactory.load_from_file(index_file)
                    record_ids = index_tree.search(condition_value)
                    
                    if record_ids:
                        # Load the actual records
                        table_file = os.path.join(self.tables_dir, db_name, f"{actual_table_name}.tbl")
                        table_tree = BPlusTreeFactory.load_from_file(table_file)
                        
                        results = []
                        if isinstance(record_ids, list):
                            for record_id in record_ids:
                                record = table_tree.search(record_id)
                                if record:
                                    results.append(record)
                        else:
                            record = table_tree.search(record_ids)
                            if record:
                                results.append(record)
                        
                        return self._apply_column_selection_and_sorting(results, columns, order_by, limit)
                except Exception as e:
                    logging.error("Error using index: %s", str(e))

        # Fallback to table scan - use B+ tree file instead of JSON
        table_file = os.path.join(self.tables_dir, db_name, f"{actual_table_name}.tbl")
        if not os.path.exists(table_file):
            logging.error("Table file not found: %s", table_file)
            return []

        try:
            table_tree = BPlusTreeFactory.load_from_file(table_file)
            all_records = table_tree.range_query(float("-inf"), float("inf"))
            
            # Filter records based on conditions
            results = []
            for record_key, record in all_records:
                if self._record_matches_conditions(record, conditions):
                    results.append(record)
            
            return self._apply_column_selection_and_sorting(results, columns, order_by, limit)
            
        except Exception as e:
            logging.error("Error querying table %s: %s", actual_table_name, str(e))
            return []

    def _get_sortable_value(self, value):
        """Convert value to a sortable format."""
        # If value is already a number, return it as is
        if isinstance(value, (int, float)):
            return value

        # Try to convert string to number if possible
        if isinstance(value, str):
            try:
                if '.' in value:
                    return float(value)
                else:
                    return int(value)
            except (ValueError, TypeError):
                # Keep as string for lexicographic comparison
                return value

        # If all else fails, return the value as is for default comparison
        return value

    def insert_record(self, table_name, record):
        """Insert a record with compound key support."""
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected"

        table_id = f"{db_name}.{table_name}"
        if table_id not in self.tables:
            return f"Table '{table_name}' not found"

        table_schema = self.tables[table_id]
        
        # Get primary key columns (compound or single)
        compound_pk = table_schema.get("compound_primary_key")
        if compound_pk:
            pk_columns = compound_pk
        else:
            # Find single primary key column
            pk_columns = []
            for col_name, col_info in table_schema.get("columns", {}).items():
                if isinstance(col_info, dict) and col_info.get("primary_key"):
                    pk_columns.append(col_name)

        # Generate record key
        try:
            if pk_columns:
                record_key = self._generate_compound_key(record, pk_columns)
            else:
                record_key = str(time.time() * 1000000)
        except ValueError as e:
            return f"Error: {str(e)}"

        # Load table file and insert
        table_file = os.path.join(self.tables_dir, db_name, f"{table_name}.tbl")
        
        try:
            if os.path.exists(table_file):
                tree = BPlusTreeFactory.load_from_file(table_file)
            else:
                tree = BPlusTreeFactory.create(order=50, name=f"{db_name}_{table_name}")

            # Convert compound key to numeric for B+ tree
            numeric_key = hash(record_key) % (2**31)
            tree.insert(float(numeric_key), record)
            tree.save_to_file(table_file)
            
            return f"Record inserted with compound key: {record_key}"
            
        except Exception as e:
            logging.error(f"Error inserting record: {str(e)}")
            return f"Error inserting record: {str(e)}"

    def _check_fk_constraints_for_delete(self, db_name, table_name, records_to_delete):
        """
        Check if deleting records would violate any foreign key constraints.
        """
        if not records_to_delete:
            return None  # No records to delete, no constraint violations

        logging.info("Checking FK constraints for deleting from %s", table_name)

        # Get all tables in the database
        tables = self.list_tables(db_name)

        # Direct check for each table that could reference this one
        for other_table in tables:
            if other_table == table_name:
                continue  # Skip the table we're deleting from

            # First, retrieve actual data structure of the tables to inspect
            other_schema = self.get_table_schema(other_table)
            logging.info("Checking constraints in %s schema: %s",
                            other_table, other_schema)

            # Detect all possible foreign key constraints (more comprehensive)
            fk_relationships = []

            # Check if it's a dictionary with a columns key
            if isinstance(other_schema, dict) and "columns" in other_schema:
                for col in other_schema["columns"]:
                    if isinstance(col, dict) and "constraints" in col:
                        for constraint in col["constraints"]:
                            if isinstance(constraint, str) and "REFERENCES" in constraint.upper():
                                fk_relationships.append({
                                    "referencing_column": col["name"],
                                    "constraint": constraint
                                })

            # Check if it's a list of column definitions
            elif isinstance(other_schema, list):
                for col in other_schema:
                    if isinstance(col, dict) and "constraints" in col:
                        for constraint in col["constraints"]:
                            if isinstance(constraint, str) and "REFERENCES" in constraint.upper():
                                fk_relationships.append({
                                    "referencing_column": col["name"],
                                    "constraint": constraint
                                })
                    elif isinstance(col, str) and "REFERENCES" in col.upper():
                        col_parts = col.split()
                        fk_relationships.append({
                            "referencing_column": col_parts[0],
                            "constraint": col
                        })

            # Check for table-level constraints
            if isinstance(other_schema, dict) and "constraints" in other_schema:
                for constraint in other_schema["constraints"]:
                    if isinstance(constraint, str) and "FOREIGN KEY" in constraint.upper():
                        fk_relationships.append({
                            "referencing_column": None,  # Will extract from constraint
                            "constraint": constraint
                        })

            # Process all found relationships
            for relationship in fk_relationships:
                constraint_str = relationship["constraint"]
                referencing_column = relationship["referencing_column"]
                logging.info("Processing potential FK constraint: %s", constraint_str)

                # Extract the referenced table and columns with multiple patterns
                fk_column = referencing_column  # Default if not extracted from constraint
                ref_table = None
                ref_column = None

                # Try these patterns in sequence
                patterns = [
                    r"FOREIGN\s+KEY\s*\(\s*(\w+)\s*\)\s*REFERENCES\s+(\w+)\s*\(\s*(\w+)\s*\)",
                    r"REFERENCES\s+(\w+)\s*\(\s*(\w+)\s*\)",
                    r"FOREIGN\s+KEY\s*[\(\s](\w+)[\)\s]+REFERENCES\s+(\w+)\s*[\(\s](\w+)"
                ]

                for pattern in patterns:
                    match = re.search(pattern, constraint_str, re.IGNORECASE)
                    if match:
                        if len(match.groups()) == 3:
                            fk_column = match.group(1)
                            ref_table = match.group(2)
                            ref_column = match.group(3)
                        elif len(match.groups()) == 2:
                            ref_table = match.group(1)
                            ref_column = match.group(2)
                        break  # Stop after first successful match

                # If we found a reference to the table we're deleting from
                if ref_table and ref_table.lower() == table_name.lower():
                    logging.info("Found FK reference: %s.%s -> %s.%s",
                                    other_table, fk_column, table_name, ref_column)

                    # For each record we're trying to delete
                    for record in records_to_delete:
                        # Get the value of the referenced column
                        if ref_column in record:
                            ref_value = record[ref_column]
                            logging.info("Checking references to value: %s", ref_value)

                            # Check for references with type handling
                            conditions = \
                                [{"column": fk_column, "operator": "=", "value": ref_value}]

                            # Convert string values for comparison if needed
                            if isinstance(ref_value, (int, float)) and not\
                                isinstance(ref_value, bool):
                                str_value = str(ref_value)
                                conditions.append({"column": fk_column,\
                                    "operator": "=", "value": str_value})

                            referencing_records = self.query_with_condition(
                                other_table, conditions, ["*"]
                            )

                            if referencing_records:
                                logging.info("Found %s referencing records: %s",
                                                len(referencing_records), referencing_records)
                                error_msg = f"Cannot delete from {table_name} \
                                    because records in {other_table}\
                                        reference it via foreign key {fk_column}->{ref_column}"
                                return error_msg

        return None  # No constraints violated

    def _check_fk_constraints_for_insert(self, db_name, table_name, record):
        """Check if an insert would violate foreign key constraints."""
        if not isinstance(record, dict):
            return None

        table_id = f"{db_name}.{table_name}"
        if table_id not in self.tables:
            return f"Table {table_name} does not exist."

        table_info = self.tables[table_id]
        constraints = table_info.get('constraints', [])

        for constraint in constraints:
            # Parse out FOREIGN KEY constraints
            if isinstance(constraint, str) and "FOREIGN KEY" in constraint.upper():
                # Parse the FK definition using regex
                pattern = r"FOREIGN\s+KEY\s*\(\s*(\w+)\s*\)\s*REFERENCES\s+(\w+)\s*\(\s*(\w+)\s*\)"
                match = re.search(pattern, constraint, re.IGNORECASE)

                if match:
                    fk_column = match.group(1)
                    ref_table = match.group(2)
                    ref_column = match.group(3)

                    # Skip if the FK column isn't in our record or is NULL
                    if fk_column not in record or record[fk_column] is None:
                        continue

                    fk_value = record[fk_column]

                    # Check if the referenced value exists
                    ref_records = self.query_with_condition(
                        ref_table,
                        [{"column": ref_column, "operator": "=", "value": fk_value}],
                        [ref_column]
                    )

                    if not ref_records:
                        return f"Foreign key constraint violation:\
                            Value {fk_value} in {table_name}.{fk_column}\
                                does not exist in {ref_table}.{ref_column}"

        # For heuristic FK checks (columns ending with _id)
        for col_name, value in record.items():
            if value is not None and col_name.endswith('_id') and col_name != 'id':
                # Try to determine referenced table name
                ref_table_base = col_name[:-3]  # Remove '_id' suffix

                # Try these patterns
                possible_tables = [
                    f"{ref_table_base}s",      # user_id -> users
                    ref_table_base,            # dept_id -> dept
                    f"{ref_table_base}es",     # box_id -> boxes
                    f"{ref_table_base}ies" if ref_table_base.endswith('y') else None,
                    f"{ref_table_base}ment",   # depart_id -> department
                    f"{ref_table_base}ments"   # depart_id -> departments
                ]

                # Filter out None values
                possible_tables = [t for t in possible_tables if t]

                # Check each possible referenced table
                for ref_table in possible_tables:
                    if ref_table in self.list_tables(db_name):
                        logging.info("Heuristic FK check: %s.%s -> %s.id = %s",
                                    table_name, col_name, ref_table, value)

                        # Check if referenced value exists
                        ref_records = self.query_with_condition(
                            ref_table,
                            [{"column": "id", "operator": "=", "value": value}],
                            ["id"]
                        )

                        if not ref_records:
                            return f"Foreign key constraint violation:\
                                Value {value} in {table_name}.{col_name}\
                                    does not exist in {ref_table}.id"
                        break

        return None  # No violations

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
                    index_tree = BPlusTreeFactory.load_from_file(index_path)
                    if index_tree is None:
                        index_tree = BPlusTreeFactory.create(
                            order=50, name=f"{table_name}_{column}_index"
                        )
                except RuntimeError:
                    index_tree = BPlusTreeFactory.create(
                        order=50, name=f"{table_name}_{column}_index"
                    )
            else:
                index_tree = BPlusTreeFactory.create(
                    order=50, name=f"{table_name}_{column}_index")

            # Add the record to the index
            column_value = record.get(column)
            if column_value is not None:
                # For unique indexes, check if value already exists
                if index_info.get("unique", False):
                    existing = index_tree.search(column_value)
                    if existing:
                        # Already indexed, but could be the same record (update case)
                        if existing != record_id:
                            logging.warning(
                                "Unique constraint violation on %s=%s",
                                column, column_value
                            )
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

        # Handle case-insensitive table name lookup
        tables = self.list_tables(db_name)
        actual_table_name = None

        # Try exact match first
        if table_name in tables:
            actual_table_name = table_name
        else:
            # Try case-insensitive match
            for t in tables:
                if t.lower() == table_name.lower():
                    actual_table_name = t
                    break

        if not actual_table_name:
            return f"Table {table_name} does not exist."

        table_id = f"{db_name}.{actual_table_name}"

        # Check if table exists
        if table_id not in self.tables:
            return f"Table {actual_table_name} does not exist."

        # Load the table file
        table_file = os.path.join(self.tables_dir, db_name, f"{actual_table_name}.tbl")
        if not os.path.exists(table_file):
            return "Table data file not found."

        try:
            # Load the B+ tree
            tree = BPlusTreeFactory.load_from_file(table_file)
            if tree is None:
                return f"Could not load table data for {actual_table_name}."

            # Get all records
            all_records = tree.range_query(float("-inf"), float("inf"))

            if not all_records:
                return "0 records deleted."

            # Track records to delete and keep
            records_to_delete = []
            records_to_keep = []

            # Identify records to delete based on conditions
            for record_key, record in all_records:
                # Check if record matches all conditions
                should_delete = True

                # If no conditions, delete all records
                if not conditions:
                    records_to_delete.append(record_key)
                    continue

                # Otherwise, check each condition
                for condition in conditions:
                    col = condition.get("column")
                    op = condition.get("operator")
                    val = condition.get("value")

                    # Skip invalid conditions
                    if not col or not op:
                        continue

                    # Case-insensitive column matching
                    matching_col = None
                    for record_col in record:
                        if record_col.lower() == col.lower():
                            matching_col = record_col
                            break

                    if matching_col is None:
                        should_delete = False
                        break

                    # Get the column value using the correct case
                    record_val = record[matching_col]

                    # Apply operator
                    if op == "=":
                        if record_val != val:
                            should_delete = False
                            break
                    elif op == ">":
                        try:
                            if float(record_val) <= float(val):
                                should_delete = False
                                break
                        except (ValueError, TypeError):
                            # Fall back to string comparison
                            if str(record_val) <= str(val):
                                should_delete = False
                                break
                    elif op == "<":
                        try:
                            if float(record_val) >= float(val):
                                should_delete = False
                                break
                        except (ValueError, TypeError):
                            if str(record_val) >= str(val):
                                should_delete = False
                                break
                    elif op == ">=":
                        try:
                            if float(record_val) < float(val):
                                should_delete = False
                                break
                        except (ValueError, TypeError):
                            if str(record_val) < str(val):
                                should_delete = False
                                break
                    elif op == "<=":
                        try:
                            if float(record_val) > float(val):
                                should_delete = False
                                break
                        except (ValueError, TypeError):
                            if str(record_val) > str(val):
                                should_delete = False
                                break
                    elif op == "!=":
                        if record_val == val:
                            should_delete = False
                            break

                if should_delete:
                    records_to_delete.append(record_key)
                else:
                    records_to_keep.append((record_key, record))

            if not records_to_delete:
                return "0 records deleted."

            # CHECK FOREIGN KEY CONSTRAINTS BEFORE DELETE
            fk_violation = self._check_fk_constraints_for_delete(db_name,\
                actual_table_name, records_to_delete)
            if fk_violation:
                logging.error("FK constraint violation during delete from %s: %s",
                            table_name, fk_violation)
                # Return a dictionary with error information
                return {"error": fk_violation, "status": "error"}

            # Create a new tree with only the records to keep
            new_tree = BPlusTreeFactory.create(order=50, name=actual_table_name)

            # Add records to keep to the new tree
            for key, record in records_to_keep:
                new_tree.insert(key, record)

            # Save the new tree
            new_tree.save_to_file(table_file)

            # Update any indexes
            for key, record in records_to_keep:
                self._update_indexes_after_insert(
                    db_name, actual_table_name, key, record
                )

            # Invalidate cache for this table
            self._invalidate_table_cache(actual_table_name)

            return f"{len(records_to_delete)} records deleted."

        except RuntimeError as e:
            logging.error("Error deleting records: %s", str(e))

            logging.error(traceback.format_exc())
            return f"Error deleting records: {str(e)}"

    def execute_update(self, plan):
        """Execute an UPDATE operation."""
        table_name = plan.get("table")
        updates_data = plan.get("updates", {}) or plan.get("set", {})
        condition = plan.get("condition")

        # Check for valid database
        db_name = self.get_current_database()
        if not db_name:
            return {"error": "No database selected", "status": "error", "type": "error"}

        # Convert updates to dictionary format if it's a list of tuples
        updates = {}
        if isinstance(updates_data, list):
            for col, val in updates_data:
                # Handle quoted string values
                if isinstance(val, str) and val.startswith("'") and val.endswith("'"):
                    val = val[1:-1]
                updates[col] = val
        else:
            updates = updates_data  # Already a dict

        # Get records that will be updated
        parsed_conditions = self._parse_conditions(condition)
        records_to_update = self.query_with_condition(
            table_name, parsed_conditions, ["*"]
        )

        if not records_to_update:
            return {
                "status": "success",
                "message": "0 records updated - no matching records found",
                "type": "update_result",
                "rowCount": 0
            }

        # Check if any primary key columns are being updated
        table_schema = self.get_table_schema(table_name)
        pk_columns = []

        # Extract primary key columns from schema
        for column in table_schema:
            if isinstance(column, dict) and column.get("primary_key"):
                pk_columns.append(column.get("name"))
            elif isinstance(column, str) and "PRIMARY KEY" in column.upper():
                col_name = column.split()[0]
                pk_columns.append(col_name)

        # Check for FK constraints if we're updating a primary key
        for update_col in updates.keys():
            if update_col in pk_columns:
                fk_violation = self._check_fk_constraints_for_update(
                    db_name, table_name, update_col, records_to_update
                )
                if fk_violation:
                    return {
                        "error": fk_violation,
                        "status": "error",
                        "type": "error"
                    }

        # Update each matching record
        updated_count = 0
        for record in records_to_update:
            # Get record ID (typically the primary key value)
            record_id = None
            if pk_columns and pk_columns[0] in record:
                record_id = record[pk_columns[0]]
            else:
                # If no primary key, try using 'id' field
                record_id = record.get('id')

            if record_id is not None:
                # Call update_record with the correct parameters
                result = self.update_record(
                    table_name,
                    record_id,  # Pass the actual ID value, not a dictionary
                    updates
                )

                if result:
                    updated_count += 1
                    logging.info("Updated record with ID %s in table %s", record_id, table_name)
            else:
                logging.warning("Could not determine record ID for update in table %s", table_name)

        return {
            "status": "success",
            "message": f"Updated {updated_count} records",
            "type": "update_result",
            "rowCount": updated_count
        }

    def create_index(
        self,
        table_name,
        column_name,
        index_type="BTREE",
        is_unique=False,
        index_name=None,
        columns=None,  # New parameter for compound indexes
    ):
        """Create an index on a table column with compound key support."""
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."

        # Use provided index_name or generate one
        if not index_name:
            index_name = f"idx_{column_name}"

        table_id = f"{db_name}.{table_name}"
        index_id = f"{table_id}.{index_name}"

        # Check if table exists
        if table_id not in self.tables:
            return f"Table {table_name} does not exist."

        # Check if index already exists
        if index_id in self.indexes:
            return f"Index '{index_name}' on {table_name}.{column_name} already exists."

        # Add to indexes catalog
        self.indexes[index_id] = {
            "table": table_id,
            "column": column_name,
            "type": index_type,
            "unique": is_unique,
            "name": index_name,
            "columns": columns or [column_name],  # Store original columns list
            "created_at": datetime.datetime.now().isoformat(),
        }

        # Build the index
        table_file = os.path.join(self.tables_dir, db_name, f"{table_name}.tbl")
        index_file = os.path.join(self.indexes_dir, f"{db_name}_{table_name}_{column_name}.idx")

        try:
            # Load table data
            if os.path.exists(table_file):
                table_tree = BPlusTreeFactory.load_from_file(table_file)
                all_records = table_tree.range_query(float("-inf"), float("inf"))

                # Create index tree
                index_tree = BPlusTreeFactory.create(
                    order=50, name=f"{table_name}_{column_name}_index"
                )

                # Populate index with compound key support
                for record_key, record in all_records:
                    if columns and len(columns) > 1:
                        # Compound index - create hash-based numeric key
                        compound_key_parts = []
                        for col in columns:
                            if col in record:
                                compound_key_parts.append(str(record[col]))
                            else:
                                compound_key_parts.append("")
                        compound_key_str = "|".join(compound_key_parts)
                        
                        # Convert compound key to numeric hash for B+ tree
                        import hashlib
                        hash_value = int(hashlib.md5(compound_key_str.encode()).hexdigest()[:8], 16)
                        
                        # Store both hash and original key for retrieval
                        index_tree.insert(hash_value, {
                            "record_key": record_key,
                            "compound_key": compound_key_str,
                            "values": {col: record.get(col) for col in columns}
                        })
                    elif column_name in record:
                        # Single column index
                        index_tree.insert(record[column_name], record_key)

                # Save index
                index_tree.save_to_file(index_file)

            # Save changes to catalog
            self._save_json(self.indexes_file, self.indexes)

            logging.info("Index created on %s.%s", table_name, column_name)
            return True

        except RuntimeError as e:
            logging.error("Error creating index: %s", str(e))
            if index_id in self.indexes:
                del self.indexes[index_id]
            return f"Error creating index: {str(e)}"

    def _apply_column_selection_and_sorting(self, records, columns, order_by, limit=None):
        """Apply column selection and sorting to query results."""
        if not records:
            return []
        
        # Apply column selection
        if columns and columns != ["*"]:
            filtered_records = []
            for record in records:
                filtered_record = {}
                for col in columns:
                    if col in record:
                        filtered_record[col] = record[col]
                    else:
                        # Handle case where column doesn't exist
                        filtered_record[col] = None
                filtered_records.append(filtered_record)
            records = filtered_records
        
        # Apply sorting
        if order_by:
            column = order_by.get("column")
            direction = order_by.get("direction", "ASC")
            
            if column:
                try:
                    reverse_order = direction.upper() == "DESC"
                    records.sort(
                        key=lambda x: self._get_sortable_value(x.get(column)),
                        reverse=reverse_order
                    )
                except Exception as e:
                    logging.warning(f"Error sorting by {column}: {str(e)}")
        
        # Apply limit if specified
        if limit is not None:
            try:
                limit_int = int(limit)
                records = records[:limit_int]
            except (ValueError, TypeError):
                logging.warning(f"Invalid limit value: {limit}")
        
        return records

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
        index_file = os.path.join(
            self.indexes_dir, f"{db_name}_{table_name}_{column_name}.idx"
        )
        if os.path.exists(index_file):
            os.remove(index_file)

        # Save changes
        self._save_json(self.indexes_file, self.indexes)

        logging.info("Index %s dropped from %s", index_name, table_name)
        return f"Index %s dropped from %s" % (index_name, table_name)

    def drop_table(self, table_name):
        """Drop a table from the catalog."""
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."

        table_id = f"{db_name}.{table_name}"

        # Check if table exists
        if table_id not in self.tables:
            return f"Table {table_name} does not exist."

        # Remove table from tables catalog
        del self.tables[table_id]

        # Remove table from database's tables list
        if table_name in self.databases[db_name]["tables"]:
            self.databases[db_name]["tables"].remove(table_name)

        # Remove table file
        table_file = os.path.join(
            self.tables_dir, db_name, f"{table_name}.tbl")
        if os.path.exists(table_file):
            os.remove(table_file)

        # Remove associated indexes
        indexes_to_remove = []
        for index_id, index_info in self.indexes.items():
            if index_info.get("table") == table_id:
                indexes_to_remove.append(index_id)

                # Remove index file
                column_name = index_info.get("column")
                index_file = os.path.join(
                    self.indexes_dir, f"{db_name}_{table_name}_{column_name}.idx"
                )
                if os.path.exists(index_file):
                    os.remove(index_file)

        for index_id in indexes_to_remove:
            del self.indexes[index_id]

        # Save changes
        self._save_json(self.tables_file, self.tables)
        self._save_json(self.databases_file, self.databases)
        self._save_json(self.indexes_file, self.indexes)

        logging.info("Table %s dropped.", table_name)
        return f"Table {table_name} dropped."

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
            "created_at": datetime.datetime.now().isoformat(),
        }

        # Save changes
        self._save_json(self.views_file, self.views)

        logging.info("View %s created", view_name)
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

        logging.info("View %s dropped", view_name)
        return f"View %s dropped"

    def get_record_by_key(self, table_name, record_key):
        """
        Retrieve a record by its key from a table.

        Args:
            table_name: The name of the table
            record_key: The key of the record to retrieve

        Returns:
            The record if found, otherwise None
        """
        db_name = self.get_current_database()
        if not db_name:
            return None

        # Load the table file
        table_file = os.path.join(self.tables_dir, db_name, f"{table_name}.tbl")
        if not os.path.exists(table_file):
            return None

        try:
            # Load the B+ tree
            tree = BPlusTreeFactory.load_from_file(table_file)
            if tree is None:
                return None

            # Get all records and find the one with matching ID
            all_records = tree.range_query(float('-inf'), float('inf'))

            for key, record in all_records:
                if isinstance(record, dict) and 'id' in record and record['id'] == record_key:
                    return record

            # If we're looking for ID but couldn't find an exact match, try as key
            for key, record in all_records:
                if key == record_key:
                    return record

            return None

        except RuntimeError as e:
            logging.error("Error retrieving record: %s", str(e))
            return None

    def table_exists(self, table_name, db_name=None):
        """
        Check if a table exists in a database.

        Args:
            table_name: Table name
            db_name: Database name (optional, uses current database if not provided)

        Returns:
            bool: True if the table exists, False otherwise
        """
        if db_name is None:
            db_name = self.get_current_database()
            if not db_name:
                return False

        if db_name not in self.databases:
            return False

        tables = self.list_tables(db_name)
        return table_name in tables

    def get_table_schema(self, table_name):
        """Get the schema for a table."""
        db_name = self.get_current_database()
        if not db_name:
            return []

        # Look up table in memory first
        table_id = f"{db_name}.{table_name}"
        if table_id in self.tables:
            table_info = self.tables[table_id]

            # Process columns and constraints
            columns = []
            constraints = []

            columns_data = table_info.get("columns", [])
            if isinstance(columns_data, list):
                for col in columns_data:
                    if isinstance(col, str):
                        # Handle FOREIGN KEY constraints separately
                        if "FOREIGN KEY" in col.upper():
                            constraints.append({"constraint": col})
                        else:
                            # Regular column definition
                            name = col.split()[0]
                            columns.append({"name": name, "definition": col})
                    else:
                        # Dictionary format column
                        columns.append(col)
            elif isinstance(columns_data, dict):
                for name, attrs in columns_data.items():
                    col_info = {"name": name}
                    col_info.update(attrs)
                    columns.append(col_info)

            # Add any separate constraints
            if "constraints" in table_info:
                for constraint in table_info.get("constraints", []):
                    constraints.append({"constraint": constraint})

            # Return combined schema
            return columns + constraints

        # Table not found
        return []

    def update_record(self, table_name, record_id, update_data):
        """Update a record by its primary key.

        Args:
            table_name: Table name
            record_id: Primary key value
            update_data: New data to update in the record

        Returns:
            bool: True if successful, False otherwise
        """
        db_name = self.get_current_database()
        if not db_name:
            return False

        table_id = f"{db_name}.{table_name}"
        if table_id not in self.tables:
            return False

        # Get the table file path
        table_file = os.path.join(self.tables_dir, db_name, f"{table_name}.tbl")

        if not os.path.exists(table_file):
            return False

        try:
            # Load the B+ tree
            tree = BPlusTreeFactory.load_from_file(table_file)
            if tree is None:
                return False

            # Get all records
            all_records = tree.range_query(float('-inf'), float('inf'))

            # Find the record to update
            record_to_update = None
            record_key = None

            for key, record in all_records:
                # Check if this is the record we're looking for
                if isinstance(record, dict) and "id" in record and record["id"] == record_id:
                    record_to_update = record
                    record_key = key
                    break

            if not record_to_update:
                return False

            # Update the record
            for field, value in update_data.items():
                record_to_update[field] = value

            # Update in the tree
            tree.insert(record_key, record_to_update)

            # Save the updated tree
            tree.save_to_file(table_file)

            # Invalidate cache for this table
            self._invalidate_table_cache(table_name)

            return True

        except RuntimeError as e:
            logging.error("Error updating record: %s", str(e))
            return False

    def get_all_records_with_keys(self, table_name):
        """Get all records from a table along with their keys.

        Args:
            table_name: Name of the table to query

        Returns:
            list: A list of tuples (key, record) for all records in the table
        """
        db_name = self.get_current_database()
        if not db_name:
            return []

        # Handle case-insensitive table name lookup
        tables = self.list_tables(db_name)
        actual_table_name = None

        # Try exact match first
        if table_name in tables:
            actual_table_name = table_name
        else:
            # Try case-insensitive match
            for t in tables:
                if t.lower() == table_name.lower():
                    actual_table_name = t
                    break

        if not actual_table_name:
            logging.warning("Table '%s' does not exist in database '%s'", table_name, db_name)
            return []

        # Load the table file
        table_file = os.path.join(self.tables_dir, db_name, f"{actual_table_name}.tbl")

        if not os.path.exists(table_file):
            logging.warning("Table file not found: %s", table_file)
            return []

        try:
            # Load the B+ tree
            tree = BPlusTreeFactory.load_from_file(table_file)
            if tree is None:
                logging.error("Failed to load B+ tree for %s", table_file)
                return []

            # Return all records with their keys
            return tree.range_query(float("-inf"), float("inf"))
        except RuntimeError as e:
            logging.error("Error getting records with keys: %s", str(e))
            logging.error(traceback.format_exc())
            return []

    def delete_record_by_id(self, table_name, record_id):
        """Delete a record by its primary key.

        Args:
            table_name: Table name
            record_id: Primary key value

        Returns:
            bool: True if successful
        """
        return self.delete_records(table_name,
                                [{"column": "id", "operator": "=", "value": record_id}])

    def update_record_by_id(self, table_name, record_id, record_data):
        """Update a record by its primary key.

        Args:
            table_name: Table name
            record_id: Primary key value
            record_data: New record data

        Returns:
            bool: True if successful
        """
        return self.update_record(table_name, record_data,
                                [{"column": "id", "operator": "=", "value": record_id}])

    def insert_record_with_id(self, table_name, record_id, record_data):
        """Insert a record with a specific ID.

        Args:
            table_name: Table name
            record_id: Primary key value
            record_data: Record data

        Returns:
            bool: True if successful
        """
        # Ensure the record has the correct ID
        record = record_data.copy()
        record["id"] = record_id

        return self.insert_record(table_name, record)

    def get_table_size(self, table_name):
        """
        Get the approximate size (number of records) of a table.

        Args:
            table_name: Name of the table

        Returns:
            Approximate number of records in the table
        """
        db_name = self.get_current_database()
        if not db_name:
            return 0

        # Load the table file
        table_file = os.path.join(
            self.tables_dir, db_name, f"{table_name}.tbl"
        )
        if not os.path.exists(table_file):
            return 0

        try:
            # Load the B+ tree
            tree = BPlusTreeFactory.load_from_file(table_file)
            if tree is None:
                return 0

            # Count records
            all_records = tree.range_query(float("-inf"), float("inf"))
            return len(all_records) if all_records else 0
        except RuntimeError as e:
            logging.error("Error getting table size: %s", str(e))
            return 0

    def close_all_connections(self):
        """Close all open database connections."""
        logging.info("CatalogManager: Closing all connections")
        # No actual database connections to close in file-based storage
        # This method exists for compatibility with the server shutdown process
        return True

    def _invalidate_table_cache(self, table_name):
        """Helper method to invalidate cache for a table if possible."""
        try:
            # Try to find query_cache in our attributes
            if hasattr(self, 'query_cache'):
                if hasattr(self.query_cache, 'invalidate'):
                    self.query_cache.invalidate(table_name)
                    logging.info(f"Invalidated query cache for table: {table_name}")
                elif hasattr(self.query_cache, 'update_table_version'):
                    self.query_cache.update_table_version(table_name)
                    logging.info(f"Updated version for table: {table_name}")

            # If we're part of a server instance that has invalidation methods
            elif hasattr(self, '_server') and hasattr(self._server, 'invalidate_caches_for_table'):
                self._server.invalidate_caches_for_table(table_name)
                logging.info(f"Invalidated server caches for table: {table_name}")

        except Exception as e:
            # Don't let cache invalidation errors affect the main operation
            logging.error(f"Error during cache invalidation for {table_name}: {str(e)}")
