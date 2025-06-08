import datetime
import fnmatch
import hashlib
import json
import logging
import os
import re
import shutil
import time
import traceback
from bptree_wrapper import BPlusTreeFactory
from sqlglot_parser import SQLGlotParser
from sqlglot import exp, parse_one

class CatalogManager:
    """_summary_
    """
    def __init__(self, data_dir="data"):
        # Base directories
        self.data_dir = data_dir
        self.catalog_dir = os.path.join(data_dir, "catalog")
        self.tables_dir = os.path.join(data_dir, "tables")
        self.indexes_dir = os.path.join(data_dir, "indexes")

        # Initialize SQLGlot parser for column definition parsing
        self.sqlglot_parser = SQLGlotParser()

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

        # Server instance reference (will be set if this catalog manager is part of a server)
        self._server = None

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
        hashed_password = hashlib.sha256(password.encode()).hexdigest()

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
                hashed_password = hashlib.sha256(password.encode()).hexdigest()
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

    def _process_column_definition(self, col_def):
        """Process a single column definition string using SQLGlot parser."""
        return self.sqlglot_parser.parse_column_definition(col_def)

    def create_table(self, table_name, columns, constraints=None):
        """Create a table in the catalog with compound key support."""
        table_level_constraints = []
        compound_pk_columns = []

        if constraints is not None:
            if isinstance(constraints, list):
                table_level_constraints = constraints
            else:
                table_level_constraints = [constraints]

        db_name = self.get_current_database()
        if not db_name:
            raise ValueError("No database selected")

        table_id = f"{db_name}.{table_name}"

        if table_id in self.tables:
            raise ValueError(f"Table '{table_name}' already exists")

        processed_columns = {}

        # CRITICAL FIX: Process column definitions correctly
        if isinstance(columns, list):
            for col_def in columns:
                if isinstance(col_def, str):
                    # Parse the column definition string
                    column_info = self._process_column_definition(col_def)
                    column_name = column_info["name"]
                    processed_columns[column_name] = {
                        "type": column_info["type"],
                        "primary_key": column_info["primary_key"],
                        "identity": column_info["identity"],
                        "not_null": not column_info["nullable"]  # Convert nullable to not_null
                    }

                    # Add identity seed/increment if present
                    if column_info.get("identity_seed"):
                        processed_columns[column_name]["identity_seed"] = column_info["identity_seed"]
                    if column_info.get("identity_increment"):
                        processed_columns[column_name]["identity_increment"] = column_info["identity_increment"]

                    # Add default if present
                    if column_info.get("default"):
                        processed_columns[column_name]["default"] = column_info["default"]

                    # Track compound primary key columns
                    if column_info["primary_key"]:
                        compound_pk_columns.append(column_name)
                else:
                    # Handle dictionary format
                    processed_columns[col_def["name"]] = col_def
        else:
            # Handle dictionary format
            processed_columns = columns

        # Store the table
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

        logging.info("Table '%s' created with columns: %s", table_name, list(processed_columns.keys()))
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
            # Fix: Handle both formats - with and without table prefix
            parts = index_id.split(".")
            if len(parts) >= 3 and f"{parts[0]}.{parts[1]}" == table_id:
                index_name = parts[2]
                result[index_name] = index_info
            elif len(parts) == 2 and parts[0] == table_name:
                # Handle legacy format
                index_name = parts[1]
                result[index_name] = index_info
            elif index_id.startswith(f"{table_id}."):
                # Extract index name from full ID
                index_name = index_id[len(f"{table_id}."):]
                result[index_name] = index_info

        logging.info(f"Found indexes for table {table_name}: {list(result.keys())}")
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

        # Handle compound condition (AND/OR structure)
        if len(conditions) == 1 and isinstance(conditions[0], dict):
            condition = conditions[0]
            if "operator" in condition and condition["operator"] == "AND" and "operands" in condition:
                # Process AND condition
                for operand in condition["operands"]:
                    if not self._record_matches_single_condition(record, operand):
                        return False
                return True
            if "operator" in condition and condition["operator"] == "OR" and "operands" in condition:
                # Process OR condition
                for operand in condition["operands"]:
                    if self._record_matches_single_condition(record, operand):
                        return True
                return False
            else:
                # Single condition in a list
                return self._record_matches_single_condition(record, condition)

        # Standard list of conditions (implicit AND)
        for condition in conditions:
            if not self._record_matches_single_condition(record, condition):
                return False

        return True

    def _record_matches_single_condition(self, record, condition):
        """Check if a record matches a single condition."""
        column = condition.get("column")
        operator = condition.get("operator")
        value = condition.get("value")

        if not column:
            return False

        # Handle table-qualified column names (e.g., "products.price")
        actual_column = column
        if "." in column:
            # Extract just the column name part
            _, column_part = column.split(".", 1)
            actual_column = column_part

            # Also try the full qualified name in case the record has it
            if column in record:
                actual_column = column
            elif column_part in record:
                actual_column = column_part
            else:
                # Try both variations
                for key in record.keys():
                    if key in (column_part, column):
                        actual_column = key
                        break
                else:
                    return False

        if actual_column not in record:
            return False

        record_value = record[actual_column]

        # Convert both to numeric if possible
        value = self._convert_to_numeric(value)
        record_value = self._convert_to_numeric(record_value)

        # Handle different operators
        if operator == "=":
            # Handle string comparisons with quote removal
            if isinstance(value, str) and value.startswith("'") and value.endswith("'"):
                value = value[1:-1]
            return record_value == value
        if operator == "!=":
            return record_value != value
        if operator == ">":
            # Ensure numeric comparison for > operator
            try:
                return float(record_value) > float(value)
            except (ValueError, TypeError):
                return str(record_value) > str(value)
        if operator == ">=":
            try:
                return float(record_value) >= float(value)
            except (ValueError, TypeError):
                return str(record_value) >= str(value)
        if operator == "<":
            try:
                return float(record_value) < float(value)
            except (ValueError, TypeError):
                return str(record_value) < str(value)
        if operator == "<=":
            try:
                return float(record_value) <= float(value)
            except (ValueError, TypeError):
                return str(record_value) <= str(value)
        if operator.upper() == "LIKE":
            return self._matches_like_pattern(str(record_value), str(value))
        if operator.upper() == "IN":
            return self._matches_in_condition(record_value, value)

        return False

    def _matches_like_pattern(self, record_value, pattern):
        """Check if a record value matches a LIKE pattern."""
        # Convert SQL LIKE pattern to shell pattern
        shell_pattern = pattern.replace('%', '*').replace('_', '?')
        return fnmatch.fnmatch(record_value, shell_pattern)

    def _matches_in_condition(self, record_value, value_list):
        """Check if a record value is in a list of values."""
        if isinstance(value_list, list):
            return record_value in value_list
        # Handle single value wrapped in parentheses like "(1)"
        if isinstance(value_list, str) and value_list.startswith('(') and value_list.endswith(')'):
            try:
                # Parse comma-separated values
                values = [v.strip() for v in value_list[1:-1].split(',')]
                # Convert to appropriate types
                converted_values = []
                for v in values:
                    if v.startswith("'") and v.endswith("'"):
                        converted_values.append(v[1:-1])  # String value
                    else:
                        try:
                            converted_values.append(int(v))  # Integer value
                        except ValueError:
                            try:
                                converted_values.append(float(v))  # Float value
                            except ValueError:
                                converted_values.append(v)  # Keep as string
                return record_value in converted_values
            except Exception:
                return False
        return record_value == value_list

    def query_with_condition(self, table_name, conditions=None, columns=None, limit=None, order_by=None):
        """Query table data with conditions and optional column selection."""
        db_name = self.get_current_database()
        if not db_name:
            return []

        table_id = f"{db_name}.{table_name}"
        if table_id not in self.tables:
            logging.error(f"Table '{table_name}' not found")
            return []

        table_file = os.path.join(self.tables_dir, db_name, f"{table_name}.tbl")

        try:
            # Try to load the B+ tree with better error handling
            if os.path.exists(table_file):
                try:
                    tree = BPlusTreeFactory.load_from_file(table_file)
                    logging.info(f"Successfully loaded B+ tree from {table_file}")
                except Exception as load_error:
                    logging.error(f"Failed to load B+ tree from {table_file}: {str(load_error)}")
                    # Try to recreate an empty tree
                    logging.info(f"Attempting to recreate empty tree for {table_name}")
                    tree = BPlusTreeFactory.create(order=50, name=f"{db_name}_{table_name}")
                    tree.save_to_file(table_file)
                    logging.info(f"Created new empty tree for {table_name}")
                    return []  # Return empty results since we had to recreate
            else:
                logging.warning(f"Table file does not exist: {table_file}")
                tree = BPlusTreeFactory.create(order=50, name=f"{db_name}_{table_name}")
                tree.save_to_file(table_file)
                return []

            # Get all records from the tree with better error handling
            try:
                if hasattr(tree, 'range_query'):
                    # Try range query first (more reliable)
                    all_records = tree.range_query(float("-inf"), float("inf"))
                    logging.info(f"Range query returned {len(all_records)} records")
                elif hasattr(tree, '_get_all_items'):
                    # Fallback to _get_all_items
                    all_records = tree._get_all_items()
                    logging.info(f"_get_all_items returned {len(all_records)} records")
                else:
                    logging.error(f"Tree does not support data retrieval methods")
                    return []

            except Exception as query_error:
                logging.error(f"Error querying tree data: {str(query_error)}")
                # Try alternative methods
                try:
                    if hasattr(tree, 'value_store') and tree.value_store:
                        logging.info(f"Falling back to value_store with {len(tree.value_store)} items")
                        all_records = []
                        for value_id, value_holder in tree.value_store.items():
                            if hasattr(value_holder, 'value'):
                                all_records.append((value_id, value_holder.value))
                            else:
                                all_records.append((value_id, value_holder))
                    else:
                        logging.error("No fallback data retrieval method available")
                        return []
                except Exception as fallback_error:
                    logging.error(f"Fallback query method failed: {str(fallback_error)}")
                    return []

            # Convert tree results to records
            records = []
            for item in all_records:
                try:
                    if isinstance(item, tuple) and len(item) >= 2:
                        _, value = item[0], item[1]
                        # Handle different value formats
                        if hasattr(value, 'value'):
                            record = value.value
                        elif isinstance(value, dict):
                            record = value
                        else:
                            # Try to treat as record directly
                            record = value

                        if isinstance(record, dict):
                            records.append(record)
                        else:
                            logging.warning(f"Skipping non-dict record: {type(record)} - {record}")
                    else:
                        logging.warning(f"Skipping malformed tree item: {item}")
                except Exception as record_error:
                    logging.warning(f"Error processing record {item}: {str(record_error)}")
                    continue

            logging.info(f"Successfully extracted {len(records)} records from tree")

            # Apply conditions if specified
            if conditions:
                records = [record for record in records if self._record_matches_conditions(record, conditions)]

            # Apply column selection and sorting
            return self._apply_column_selection_and_sorting(records, columns, order_by, limit)

        except Exception as e:
            logging.error(f"Error querying table {table_name}: {str(e)}")
            logging.error(traceback.format_exc())
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

    def insert_records_batch(self, table_name, records):
        """Insert multiple records in a batch - hyperoptimized version with better error handling."""
        db_name = self.get_current_database()
        if not db_name:
            return {"error": "No database selected", "status": "error"}

        table_id = f"{db_name}.{table_name}"
        if table_id not in self.tables:
            return {"error": f"Table '{table_name}' not found", "status": "error"}

        # Load the table file ONCE with better error handling
        table_file = os.path.join(self.tables_dir, db_name, f"{table_name}.tbl")

        try:
            # Load existing B+ tree or create new one ONCE
            if os.path.exists(table_file):
                try:
                    tree = BPlusTreeFactory.load_from_file(table_file)
                    logging.info(f"Successfully loaded existing tree from {table_file}")
                except Exception as load_error:
                    logging.error(f"Failed to load existing tree: {str(load_error)}")
                    logging.info("Creating new tree to replace corrupted one")
                    tree = BPlusTreeFactory.create(order=50, name=f"{db_name}_{table_name}")
            else:
                tree = BPlusTreeFactory.create(order=50, name=f"{db_name}_{table_name}")
                logging.info(f"Created new tree for {table_name}")

            # Get table schema for validation
            table_schema = self.tables[table_id]
            table_columns_dict = table_schema.get("columns", {})

            # Process IDENTITY columns info
            identity_columns = {}
            for col_name, col_info in table_columns_dict.items():
                if isinstance(col_info, dict) and col_info.get("identity"):
                    identity_columns[col_name] = {
                        "seed": col_info.get("identity_seed", 1),
                        "increment": col_info.get("identity_increment", 1)
                    }

            # Find current max ID for IDENTITY columns with better error handling
            max_identity_values = {}
            if identity_columns:
                try:
                    # Try multiple methods to get existing records
                    existing_records = []
                    if hasattr(tree, 'range_query'):
                        try:
                            all_records = tree.range_query(float("-inf"), float("inf"))
                            for _, existing_record in all_records:
                                if hasattr(existing_record, 'value'):
                                    existing_records.append(existing_record.value)
                                elif isinstance(existing_record, dict):
                                    existing_records.append(existing_record)
                        except Exception as range_error:
                            logging.warning(f"Range query failed: {str(range_error)}")

                    # Process existing records to find max identity values
                    for existing_record in existing_records:
                        if isinstance(existing_record, dict):
                            for col_name in identity_columns:
                                if col_name in existing_record:
                                    try:
                                        existing_id = int(existing_record[col_name])
                                        max_identity_values[col_name] = max(
                                            max_identity_values.get(col_name, 0), existing_id
                                        )
                                    except (ValueError, TypeError):
                                        pass

                except Exception as e:
                    logging.warning(f"Could not determine existing identity values: {e}")

            # Insert each record WITHOUT saving the tree each time
            inserted_count = 0
            current_time = int(time.time() * 1000000)  # Get base timestamp

            for i, record in enumerate(records):
                fk_violation = self._check_fk_constraints_for_insert(db_name, table_name, record)
                if fk_violation:
                    return {"error": fk_violation, "status": "error", "record_index": i}
                try:
                    # Make a copy of the record to avoid modifying the original
                    record_copy = record.copy() if isinstance(record, dict) else record

                    # Generate IDENTITY values for auto-increment columns
                    for col_name, identity_info in identity_columns.items():
                        if col_name not in record_copy or record_copy[col_name] is None:
                            # Calculate next identity value
                            current_max = max_identity_values.get(col_name, identity_info["seed"] - identity_info["increment"])
                            next_id = current_max + identity_info["increment"]
                            record_copy[col_name] = next_id
                            max_identity_values[col_name] = next_id
                            logging.debug(f"Generated IDENTITY {col_name} = {next_id}")

                    # Generate a numeric key instead of string key
                    # Use timestamp + index to ensure uniqueness
                    record_key = float(current_time + i)

                    # Insert into B+ tree with numeric key
                    tree.insert(record_key, record_copy)
                    inserted_count += 1

                    # Log progress for large batches
                    if inserted_count % 1000 == 0:
                        logging.info(f"Inserted {inserted_count}/{len(records)} records")

                except Exception as e:
                    logging.error(f"Error inserting record {i}: {str(e)}")
                    continue

            # Save the updated tree back to file ONCE at the end with error handling
            try:
                tree.save_to_file(table_file)
                logging.info(f"Successfully saved tree with {inserted_count} records to {table_file}")
            except Exception as save_error:
                logging.error(f"Failed to save tree to file: {str(save_error)}")
                return {"error": f"Failed to save data: {str(save_error)}", "status": "error"}

            logging.info(f"Successfully inserted {inserted_count} records into {table_name}")

            return {
                "status": "success",
                "inserted_count": inserted_count,
                "message": f"Inserted {inserted_count} records"
            }

        except Exception as e:
            logging.error(f"Batch insert failed: {str(e)}")
            logging.error(traceback.format_exc())
            return {"error": f"Batch insert failed: {str(e)}", "status": "error"}

    def insert_record(self, table_name, record):
        """Insert a record with compound key support."""
        db_name = self.get_current_database()
        if not db_name:
            return {"error": "No database selected.", "status": "error"}

        table_id = f"{db_name}.{table_name}"
        if table_id not in self.tables:
            return {"error": f"Table '{table_name}' does not exist.", "status": "error"}

        # ADD THIS CODE: Check foreign key constraints BEFORE insertion
        fk_violation = self._check_fk_constraints_for_insert(db_name, table_name, record)
        if fk_violation:
            return {"error": fk_violation, "status": "error"}

        # Check unique index constraints BEFORE insertion
        unique_violation = self._check_unique_constraints_before_insert(db_name, table_name, record)
        if unique_violation:
            return {"error": unique_violation, "status": "error"}

        table_schema = self.tables[table_id]

        # CRITICAL: Validate that all columns in the record exist in the table schema
        table_columns = set(table_schema.get("columns", {}).keys())
        record_columns = set(record.keys())
        invalid_columns = record_columns - table_columns

        if invalid_columns:
            return {"error": f"Invalid column(s): {', '.join(invalid_columns)}. Valid columns are: {', '.join(table_columns)}", "status": "error"}

        # Get primary key columns (compound or single)
        compound_pk = table_schema.get("compound_primary_key")
        if compound_pk:
            primary_key_columns = compound_pk
        else:
            # Find single primary key column
            primary_key_columns = []
            for col_name, col_info in table_schema.get("columns", {}).items():
                if isinstance(col_info, dict) and col_info.get("primary_key"):
                    primary_key_columns.append(col_name)

        # CRITICAL FIX: Check primary key constraint BEFORE processing record
        if primary_key_columns:
            # Check if we have all required primary key values (before IDENTITY generation)
            pk_check_record = record.copy()

            # For IDENTITY columns, we need to check after generation, so skip this check for IDENTITY columns
            identity_pk_columns = []
            for col_name in primary_key_columns:
                col_info = table_schema.get("columns", {}).get(col_name, {})
                if isinstance(col_info, dict) and col_info.get("identity"):
                    identity_pk_columns.append(col_name)

            # Only check for duplicate primary keys if we have non-identity primary key values
            non_identity_pk_columns = [col for col in primary_key_columns if col not in identity_pk_columns]
            if non_identity_pk_columns:
                # Generate temporary key for checking duplicates
                try:
                    temp_record_key = self._generate_compound_key(pk_check_record, non_identity_pk_columns)

                    # Check if this primary key already exists
                    table_file = os.path.join(self.tables_dir, db_name, f"{table_name}.tbl")
                    if os.path.exists(table_file):
                        try:
                            existing_tree = BPlusTreeFactory.load_from_file(table_file)
                            all_records = existing_tree.range_query(float("-inf"), float("inf"))

                            for _, existing_record in all_records:
                                if isinstance(existing_record, dict):
                                    # Check if the non-identity primary key values match
                                    matches = True
                                    for col_name in non_identity_pk_columns:
                                        if col_name in pk_check_record and col_name in existing_record:
                                            if str(pk_check_record[col_name]) != str(existing_record[col_name]):
                                                matches = False
                                                break

                                    if matches:
                                        pk_values = [str(pk_check_record.get(col, 'NULL')) for col in non_identity_pk_columns]
                                        return {"error": f"Duplicate primary key constraint violation: Primary key ({', '.join(non_identity_pk_columns)}) = ({', '.join(pk_values)}) already exists", "status": "error"}
                        except Exception as e:
                            logging.warning(f"Could not check existing primary keys: {e}")
                except ValueError:
                    # Missing primary key values - this will be caught later
                    pass

        # CRITICAL FIX: Handle IDENTITY columns FIRST - make a copy to avoid modifying original
        record = record.copy()

        # Generate IDENTITY values for auto-increment columns
        for col_name, col_info in table_schema.get("columns", {}).items():
            # CRITICAL: Check if col_info is a dictionary before calling .get()
            if not isinstance(col_info, dict):
                continue

            if col_info.get("identity") and col_name not in record:
                # Find the next identity value
                table_file = os.path.join(self.tables_dir, db_name, f"{table_name}.tbl")
                next_id = col_info.get("identity_seed", 1)  # Default start value

                if os.path.exists(table_file):
                    try:
                        existing_tree = BPlusTreeFactory.load_from_file(table_file)
                        all_records = existing_tree.range_query(float("-inf"), float("inf"))

                        # Find the maximum existing ID from the record VALUES
                        max_id = 0
                        for record_key, existing_record in all_records:
                            if isinstance(existing_record, dict) and col_name in existing_record:
                                try:
                                    existing_id = int(existing_record[col_name])
                                    max_id = max(max_id, existing_id)
                                except (ValueError, TypeError):
                                    pass

                        # Set next ID based on increment
                        increment = col_info.get("identity_increment", 1)
                        next_id = max_id + increment

                    except Exception as e:
                        logging.warning(f"Could not determine next identity value: {e}")
                        # Use a simple counter as fallback
                        next_id = len(all_records) + 1 if 'all_records' in locals() else 1

                # CRITICAL: Add the IDENTITY value to the record
                record[col_name] = next_id
                logging.info(f"Generated IDENTITY {col_name} = {next_id} for table {table_name}")

        # Generate record key AFTER adding identity values
        try:
            if primary_key_columns:
                record_key = self._generate_compound_key(record, primary_key_columns)
            else:
                # Use a hash of the entire record if no primary key
                record_str = json.dumps(record, sort_keys=True)
                record_key = hashlib.md5(record_str.encode()).hexdigest()[:16]
        except ValueError as e:
            return {"error": f"Error generating record key: {str(e)}", "status": "error"}

        # CRITICAL: Check for duplicate primary key AFTER IDENTITY generation
        if primary_key_columns and record_key:
            table_file = os.path.join(self.tables_dir, db_name, f"{table_name}.tbl")
            if os.path.exists(table_file):
                try:
                    existing_tree = BPlusTreeFactory.load_from_file(table_file)
                    all_records = existing_tree.range_query(float("-inf"), float("inf"))

                    for _, existing_record in all_records:
                        if isinstance(existing_record, dict):
                            # Generate the primary key for the existing record
                            try:
                                existing_key = self._generate_compound_key(existing_record, primary_key_columns)
                                if str(existing_key) == str(record_key):
                                    pk_values = [str(record.get(col, 'NULL')) for col in primary_key_columns]
                                    return {"error": f"Duplicate primary key constraint violation: Primary key ({', '.join(primary_key_columns)}) = ({', '.join(pk_values)}) already exists", "status": "error"}
                            except ValueError:
                                # Skip records with incomplete primary keys
                                continue
                except Exception as e:
                    logging.warning(f"Could not check for duplicate primary keys: {e}")

        # Load table file and insert
        table_file = os.path.join(self.tables_dir, db_name, f"{table_name}.tbl")

        try:
            if os.path.exists(table_file):
                table_tree = BPlusTreeFactory.load_from_file(table_file)
            else:
                table_tree = BPlusTreeFactory.create(order=50, name=f"{db_name}_{table_name}")

            # Convert the record key to numeric for the optimized B+ tree
            if isinstance(record_key, str):
                try:
                    numeric_key = float(record_key)
                except ValueError:
                    # Hash string keys to numeric
                    hash_obj = hashlib.md5(record_key.encode())
                    numeric_key = float(int(hash_obj.hexdigest()[:8], 16))
            else:
                numeric_key = float(record_key)

            # CRITICAL: Insert the complete record (including generated ID) as the value
            table_tree.insert(numeric_key, record)
            table_tree.save_to_file(table_file)

            # Update indexes AFTER successful insertion
            self._update_indexes_after_insert(db_name, table_name, record_key, record)

            logging.info(f"Inserted record with key {record_key}: {record}")
            return {
                "status": "success",
                "message": f"Record inserted with key {record_key}",
                "record_key": record_key,
                "record": record
            }

        except Exception as e:
            logging.error(f"Error inserting record: {str(e)}")
            logging.error(traceback.format_exc())
            return {"error": f"Error inserting record: {str(e)}", "status": "error"}

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
                fk_info = self._parse_foreign_key_constraint(constraint_str)

                if fk_info:
                    fk_column = fk_info["fk_column"]
                    ref_table = fk_info["ref_table"]
                    ref_column = fk_info["ref_column"]

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
                                error_msg = f"Cannot delete from {table_name} because records in {other_table} reference it via foreign key {fk_column}->{ref_column}"
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
                # Parse the FK definition using SQLGlot
                fk_info = self._parse_foreign_key_constraint(constraint)

                if fk_info:
                    fk_column = fk_info["fk_column"]
                    ref_table = fk_info["ref_table"]
                    ref_column = fk_info["ref_column"]

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
                        return f"Foreign key constraint violation: Value {fk_value} in {table_name}.{fk_column} does not exist in {ref_table}.{ref_column}"

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
                        ref_records = self.query_with_condition(
                            ref_table,
                            [{"column": "id", "operator": "=", "value": value}],
                            ["id"]
                        )

                        if not ref_records:
                            return f"Foreign key constraint violation: Value {value} in {table_name}.{col_name} does not exist in {ref_table}.id"
                        break

        return None  # No violations

    def _check_unique_constraints_before_insert(self, db_name, table_name, record):
        """Check unique index constraints before inserting a record."""
        table_id = f"{db_name}.{table_name}"

        # Check if we have any unique indexes to validate
        indexes_to_check = {}
        for index_id, index_info in self.indexes.items():
            if index_info.get("table") == table_id and index_info.get("unique", False):
                # Handle both single column and multiple columns
                columns = index_info.get("columns", [])
                if not columns:
                    # Fallback to single column format
                    column = index_info.get("column")
                    if column:
                        columns = [column]

                # Check if any of the indexed columns exist in the record
                for column in columns:
                    if column in record:
                        if index_id not in indexes_to_check:
                            indexes_to_check[index_id] = index_info
                        break

        if not indexes_to_check:
            return None  # No unique indexes to check

        # Check each unique index
        for index_id, index_info in indexes_to_check.items():
            # Get the primary column for the index file name
            columns = index_info.get("columns", [])
            if not columns:
                column = index_info.get("column")
                if column:
                    columns = [column]

            if not columns:
                continue

            # Use the first column for file naming (backward compatibility)
            main_column = columns[0]

            index_filename = f"{db_name}_{table_name}_{main_column}.idx"
            index_path = os.path.join(self.indexes_dir, index_filename)

            # Load the index if it exists
            if os.path.exists(index_path):
                try:
                    index_tree = BPlusTreeFactory.load_from_file(index_path)
                    if index_tree is None:
                        continue
                except Exception as e:
                    logging.warning(f"Error loading index {index_path}: {e}")
                    continue
            else:
                continue  # No index file exists yet, so no duplicates possible

            # Check if the record value would violate uniqueness
            if main_column in record:
                column_value = record[main_column]
                if column_value is not None:
                    try:
                        # Convert to numeric key for B+ tree
                        if isinstance(column_value, (int, float)):
                            numeric_key = float(column_value)
                        else:
                            # Hash non-numeric values
                            hash_obj = hashlib.md5(str(column_value).encode())
                            numeric_key = float(int(hash_obj.hexdigest()[:8], 16))

                        # Check if value already exists
                        existing = index_tree.search(numeric_key)
                        if existing:
                            return f"Unique constraint violation: Value '{column_value}' already exists in unique index on column '{main_column}'"

                    except Exception as e:
                        logging.warning(f"Error checking unique constraint for {main_column}={column_value}: {e}")

        return None  # No violations found

    def _update_indexes_after_insert(self, db_name, table_name, record_key, record):
        """Update indexes after a record is inserted."""
        table_id = f"{db_name}.{table_name}"

        # Check if we have any indexes to update
        indexes_to_update = {}
        for index_id, index_info in self.indexes.items():
            if index_info.get("table") == table_id:
                # Handle both single column and multiple columns
                columns = index_info.get("columns", [])
                if not columns:
                    # Fallback to single column format
                    column = index_info.get("column")
                    if column:
                        columns = [column]

                # Check if any of the indexed columns exist in the record
                for column in columns:
                    if column in record:
                        if index_id not in indexes_to_update:
                            indexes_to_update[index_id] = index_info
                        break

        logging.info(f"Found {len(indexes_to_update)} indexes to update for table {table_name}")

        # Update each relevant index
        for index_id, index_info in indexes_to_update.items():
            # Get the primary column for the index file name
            columns = index_info.get("columns", [])
            if not columns:
                column = index_info.get("column")
                if column:
                    columns = [column]

            if not columns:
                logging.warning(f"No columns found for index {index_id}")
                continue

            # Use the first column for file naming (backward compatibility)
            main_column = columns[0]

            index_filename = f"{db_name}_{table_name}_{main_column}.idx"
            index_path = os.path.join(self.indexes_dir, index_filename)

            # Load or create the index
            if os.path.exists(index_path):
                try:
                    index_tree = BPlusTreeFactory.load_from_file(index_path)
                    if index_tree is None:
                        logging.warning(f"Failed to load index from {index_path}, creating new one")
                        index_tree = BPlusTreeFactory.create(
                            order=50, name=f"{table_name}_{main_column}_index"
                        )
                except Exception as e:
                    logging.warning(f"Error loading index {index_path}: {e}, creating new one")
                    index_tree = BPlusTreeFactory.create(
                        order=50, name=f"{table_name}_{main_column}_index"
                    )
            else:
                index_tree = BPlusTreeFactory.create(
                    order=50, name=f"{table_name}_{main_column}_index")

            # Add the record to the index if the column exists
            if main_column in record:
                column_value = record[main_column]
                if column_value is not None:
                    try:
                        # Convert to numeric key for the B+ tree
                        if isinstance(column_value, (int, float)):
                            numeric_key = float(column_value)
                        else:
                            # Hash non-numeric values
                            hash_obj = hashlib.md5(str(column_value).encode())
                            numeric_key = float(int(hash_obj.hexdigest()[:8], 16))

                        # Insert into index (unique constraint was already checked before insertion)
                        index_tree.insert(numeric_key, record_key)
                        index_tree.save_to_file(index_path)

                        logging.info(f"Updated index {index_id} with key {column_value} -> {record_key}")

                    except Exception as e:
                        logging.error(f"Error updating index {index_id}: {str(e)}")
            else:
                logging.warning(f"Column {main_column} not found in record for index {index_id}")

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
            for key, record_value in all_records:
                # Extract the actual record from the value
                if hasattr(record_value, 'value'):
                    record = record_value.value
                elif isinstance(record_value, dict):
                    record = record_value
                else:
                    # Skip if we can't extract a proper record
                    records_to_keep.append((key, record_value))
                    continue

                # Check if record matches all conditions
                should_delete = True

                # If no conditions, delete all records
                if not conditions:
                    records_to_delete.append(record)
                    continue

                # Check each condition
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

                    # Get the column value
                    record_val = record[matching_col]

                    # Apply operator (use helper method for consistent comparison)
                    if not self._compare_values(record_val, val, op):
                        should_delete = False
                        break

                if should_delete:
                    records_to_delete.append(record)
                else:
                    records_to_keep.append((key, record_value))

            if not records_to_delete:
                return "0 records deleted."

            # Check foreign key constraints
            fk_violation = self._check_fk_constraints_for_delete(db_name, actual_table_name, records_to_delete)
            if fk_violation:
                return fk_violation

            # Create a new tree with only the records to keep
            new_tree = BPlusTreeFactory.create(order=50, name=actual_table_name)

            # Add records to keep to the new tree
            for key, value in records_to_keep:
                new_tree.insert(key, value)

            # Save the new tree
            new_tree.save_to_file(table_file)

            # Invalidate cache for this table
            self._invalidate_table_cache(actual_table_name)

            return f"{len(records_to_delete)} records deleted."

        except Exception as e:
            logging.error("Error deleting records: %s", str(e))
            logging.error(traceback.format_exc())
            return f"Error deleting records: {str(e)}"

    def _compare_values(self, record_val, condition_val, operator):
        """Helper method for consistent value comparison."""
        # Special handling for equality comparisons
        if operator == "=":
            # Try string comparison
            if str(record_val).strip() == str(condition_val).strip():
                return True

            # Try numeric comparison
            try:
                if float(record_val) == float(condition_val):
                    return True
            except (ValueError, TypeError):
                pass

            return False

        # For other operators, try numeric conversion
        try:
            if isinstance(record_val, (int, float)) or (isinstance(record_val, str) and record_val.replace('.', '', 1).isdigit()):
                record_val = float(record_val)
            if isinstance(condition_val, (int, float)) or (isinstance(condition_val, str) and str(condition_val).replace('.', '', 1).isdigit()):
                condition_val = float(condition_val)
        except (ValueError, TypeError):
            pass  # Keep original values for string comparison

        # Apply the operator
        if operator == ">":
            return record_val > condition_val
        elif operator == "<":
            return record_val < condition_val
        elif operator == ">=":
            return record_val >= condition_val
        elif operator == "<=":
            return record_val <= condition_val
        elif operator == "!=":
            return record_val != condition_val
        elif operator.upper() == "LIKE":
            # Implement LIKE operator (simplified without regex)
            pattern = str(condition_val).replace("%", "*")
            return fnmatch.fnmatch(str(record_val), pattern)
        elif operator.upper() == "IN":
            # Implement IN operator
            if isinstance(condition_val, list):
                return record_val in condition_val
            return False

        return False  # Unknown operator

    def _parse_conditions(self, condition):
        """Parse conditions using SQLGlot parser."""
        if not condition:
            return []

        if isinstance(condition, list):
            return condition

        if isinstance(condition, str):
            # Use the SQLGlot parser to parse conditions
            parser = SQLGlotParser()
            try:
                # Create a dummy SELECT to parse the WHERE condition
                temp_sql = f"SELECT * FROM dummy WHERE {condition}"
                result = parser.parse(temp_sql)
                if "parsed_condition" in result:
                    return result["parsed_condition"]
                elif "where_conditions" in result:
                    return result["where_conditions"]
            except Exception:
                # Fallback to simple parsing
                return self._parse_simple_condition(condition)

        return []

    def _parse_simple_condition(self, condition_str):
        """Simple fallback condition parsing."""
        # Handle simple conditions like "column = 'value'"
        match = re.match(r"(\w+)\s*=\s*'([^']+)'", condition_str.strip())
        if match:
            return [{
                "column": match.group(1),
                "operator": "=",
                "value": match.group(2)
            }]

        # Handle numeric conditions like "column = 123"
        match = re.match(r"(\w+)\s*=\s*(\d+)", condition_str.strip())
        if match:
            return [{
                "column": match.group(1),
                "operator": "=",
                "value": int(match.group(2))
            }]

        return []

    def _check_fk_constraints_for_update(self, db_name, table_name, column_name, records):
        """Check if updating a primary key would violate foreign key constraints."""
        # This is a simplified implementation
        # In a real scenario, we would check all tables that reference this table
        
        # Suppress unused argument warnings for now (parameters kept for future implementation)
        _ = db_name, table_name, column_name, records

        # For now, we'll just return None (no violation) to maintain compatibility
        # This method would need to:
        # 1. Find all tables that have foreign keys pointing to this table's column
        # 2. Check if any of those foreign key values would become invalid
        # 3. Return an error message if violations are found

        return None

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

        for update_col in updates.keys():
            if update_col in pk_columns:
                fk_violation = self._check_fk_constraints_for_update(db_name, table_name, update_col, records_to_update)
                if fk_violation:
                    return {"error": fk_violation, "status": "error", "type": "error"}

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

    def debug_table_contents(self, table_name):
        """Debug method to check what's actually in the table."""
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected"

        table_file = os.path.join(self.tables_dir, db_name, f"{table_name}.tbl")

        if not os.path.exists(table_file):
            return f"Table file not found: {table_file}"

        try:
            table_tree = BPlusTreeFactory.load_from_file(table_file)

            # Try multiple methods to get data
            all_records = []

            try:
                all_records = table_tree.range_query(float("-inf"), float("inf"))
                logging.info(f"range_query returned {len(all_records)} records")
            except Exception as e:
                logging.warning(f"range_query failed: {e}")

            if not all_records and hasattr(table_tree, '_get_all_items'):
                try:
                    all_records = table_tree._get_all_items()
                    logging.info(f"_get_all_items returned {len(all_records)} records")
                except Exception as e:
                    logging.warning(f"_get_all_items failed: {e}")

            # Show first few records for debugging
            for i, (key, record) in enumerate(all_records[:5]):
                logging.info(f"Record {i}: key={key}, record={record}")

            return f"Table has {len(all_records)} records"

        except Exception as e:
            logging.error(f"Error reading table: {str(e)}")
            return f"Error reading table: {str(e)}"

    def create_index(
        self,
        table_name,
        column_name,
        index_type="BTREE",
        is_unique=False,
        index_name=None,
        columns=None,
    ):
        """Create an index on a table column with enhanced error handling."""
        db_name = self.get_current_database()
        if not db_name:
            return {"error": "No database selected.", "status": "error"}

        table_id = f"{db_name}.{table_name}"
        if table_id not in self.tables:
            return {"error": f"Table '{table_name}' does not exist.", "status": "error"}

        # Handle multiple columns for compound indexes
        if columns and isinstance(columns, list):
            column_names = columns
            primary_column = columns[0]  # Use first column for file naming
        elif column_name:
            column_names = [column_name]
            primary_column = column_name
        else:
            return {"error": "No columns specified for index.", "status": "error"}

        # Verify all columns exist in the table
        table_schema = self.tables[table_id]
        table_columns = table_schema.get("columns", {})

        for col in column_names:
            if col not in table_columns:
                return {"error": f"Column '{col}' does not exist in table '{table_name}'.", "status": "error"}

        # Generate index name if not provided
        if not index_name:
            if len(column_names) == 1:
                index_name = f"idx_{column_names[0]}"
            else:
                index_name = f"idx_{'_'.join(column_names)}"

        # Check if index already exists
        index_id = f"{table_id}.{index_name}"
        if index_id in self.indexes:
            return {"error": f"Index '{index_name}' already exists on table '{table_name}'.", "status": "error"}

        # Create index file
        index_filename = f"{db_name}_{table_name}_{primary_column}.idx"
        index_path = os.path.join(self.indexes_dir, index_filename)

        # Load table data
        table_file = os.path.join(self.tables_dir, db_name, f"{table_name}.tbl")
        if not os.path.exists(table_file):
            return {"error": f"Table file for '{table_name}' does not exist.", "status": "error"}

        try:
            # Load the table data
            table_tree = BPlusTreeFactory.load_from_file(table_file)
            all_records = table_tree.range_query(float("-inf"), float("inf"))

            logging.info(f"Loaded {len(all_records)} records using range_query")

            # Create the index B+ tree
            logging.info("Creating optimized B+ tree with order 50")
            index_tree = BPlusTreeFactory.create(order=50, name=f"{table_name}_{primary_column}_index")

            logging.info(f"Index creation - table: {table_name}, column: {primary_column}")

            # Check first record structure for debugging
            if all_records:
                logging.info(f"First record structure: {all_records[0]}")

            # Populate the index
            indexed_count = 0
            total_records = len(all_records)

            for record_key, record_data in all_records:
                # Skip if record_data is not a dictionary
                if not isinstance(record_data, dict):
                    continue

                # Check if the primary column exists in the record
                if primary_column not in record_data:
                    continue

                column_value = record_data[primary_column]
                if column_value is not None:
                    try:
                        # Convert to numeric key for B+ tree
                        if isinstance(column_value, (int, float)):
                            numeric_key = float(column_value)
                        else:
                            # Hash non-numeric values
                            hash_obj = hashlib.md5(str(column_value).encode())
                            numeric_key = float(int(hash_obj.hexdigest()[:8], 16))

                        # Insert into index - value is the original record key
                        index_tree.insert(numeric_key, record_key)
                        indexed_count += 1

                    except Exception as e:
                        logging.warning(f"Failed to index record with {primary_column}={column_value}: {e}")
                        continue

            logging.info(f"Successfully indexed {indexed_count} out of {total_records} records")

            # Save the index
            index_tree.save_to_file(index_path)
            logging.info(f"Index saved to: {index_path}")

            # Create new tree to verify save worked
            verification_tree = BPlusTreeFactory.create(order=50, name=f"{table_name}_{primary_column}_index")
            verification_tree = BPlusTreeFactory.load_from_file(index_path)
            verification_items = verification_tree.range_query(float("-inf"), float("inf"))

            logging.info(f"Verification: Index file contains {len(verification_items)} items")

            if indexed_count == 0:
                logging.error("No records were successfully indexed!")
                return {"error": f"No records were successfully indexed for column '{primary_column}'", "status": "error"}

            # Store index metadata
            self.indexes[index_id] = {
                "table": table_id,
                "columns": column_names,
                "column": primary_column,  # Keep for backward compatibility
                "type": index_type,
                "unique": is_unique,
                "file": index_filename,
                "created_at": datetime.datetime.now().isoformat(),
            }

            # Save indexes catalog
            self._save_json(self.indexes_file, self.indexes)

            logging.info(f"Index '{index_name}' created on table '{table_name}' with {indexed_count} entries")
            return {
                "status": "success",
                "message": f"Index '{index_name}' created successfully on table '{table_name}' with {indexed_count} entries"
            }

        except Exception as e:
            logging.error(f"Error creating index: {str(e)}")
            logging.error(traceback.format_exc())
            return {"error": f"Error creating index: {str(e)}", "status": "error"}

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
                    
                    # Try hyperoptimized sorting first
                    try:
                        from hyperopt_integration import hyperopt_list_sort
                        hyperopt_list_sort(
                            records,
                            key_func=lambda x: self._get_sortable_value(x.get(column)),
                            reverse=reverse_order
                        )
                    except Exception:
                        # Fallback to built-in sorting
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

    # Trigger management methods
    def create_trigger(self, trigger_name, timing, event, table_name, body):
        """Create a trigger in the catalog."""
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."

        trigger_id = f"{db_name}.{trigger_name}"

        # Check if trigger already exists
        if trigger_id in self.triggers:
            return f"Trigger {trigger_name} already exists."

        # Store trigger
        self.triggers[trigger_id] = {
            "name": trigger_name,
            "timing": timing.upper(),
            "event": event.upper(),
            "table": table_name,
            "body": body,
            "database": db_name,
            "created_at": datetime.datetime.now().isoformat(),
        }

        # Save changes
        self._save_json(self.triggers_file, self.triggers)
        
        logging.info("Trigger %s created on table %s", trigger_name, table_name)
        return True

    def drop_trigger(self, trigger_name):
        """Drop a trigger from the catalog."""
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."

        trigger_id = f"{db_name}.{trigger_name}"

        # Check if trigger exists
        if trigger_id not in self.triggers:
            return f"Trigger {trigger_name} does not exist."

        # Remove trigger
        del self.triggers[trigger_id]

        # Save changes
        self._save_json(self.triggers_file, self.triggers)
        
        logging.info("Trigger %s dropped", trigger_name)
        return True

    def get_trigger(self, trigger_name):
        """Get trigger definition by name."""
        db_name = self.get_current_database()
        if not db_name:
            return None

        trigger_id = f"{db_name}.{trigger_name}"
        return self.triggers.get(trigger_id)

    def list_triggers(self):
        """List all triggers in the current database."""
        db_name = self.get_current_database()
        if not db_name:
            return {}

        result = {}
        for trigger_id, trigger_info in self.triggers.items():
            if trigger_info.get("database") == db_name:
                trigger_name = trigger_info["name"]
                result[trigger_name] = trigger_info

        return result

    def get_triggers_for_table(self, table_name):
        """Get all triggers for a specific table."""
        db_name = self.get_current_database()
        if not db_name:
            return {}

        result = {}
        for trigger_id, trigger_info in self.triggers.items():
            if (trigger_info.get("database") == db_name and 
                trigger_info.get("table") == table_name):
                trigger_name = trigger_info["name"]
                result[trigger_name] = trigger_info

        return result

    # Procedure management methods
    def create_procedure(self, procedure_name, parameters, body):
        """Create a procedure in the catalog."""
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."

        procedure_id = f"{db_name}.{procedure_name}"

        # Check if procedure already exists
        if procedure_id in self.procedures:
            return f"Procedure {procedure_name} already exists."

        # Store procedure info
        self.procedures[procedure_id] = {
            "name": procedure_name,
            "parameters": parameters,
            "body": body,
            "database": db_name,
            "created_at": datetime.datetime.now().isoformat(),
        }

        # Save changes
        self._save_json(self.procedures_file, self.procedures)
        
        logging.info("Procedure %s created", procedure_name)
        return True

    def drop_procedure(self, procedure_name):
        """Drop a procedure from the catalog."""
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."

        procedure_id = f"{db_name}.{procedure_name}"

        # Check if procedure exists
        if procedure_id not in self.procedures:
            return f"Procedure {procedure_name} does not exist."

        # Remove procedure
        del self.procedures[procedure_id]

        # Save changes
        self._save_json(self.procedures_file, self.procedures)
        
        logging.info("Procedure %s dropped", procedure_name)
        return True

    def get_procedure(self, procedure_name):
        """Get procedure definition by name."""
        db_name = self.get_current_database()
        if not db_name:
            return None

        procedure_id = f"{db_name}.{procedure_name}"
        return self.procedures.get(procedure_id)

    def list_procedures(self):
        """List all procedures in the current database."""
        db_name = self.get_current_database()
        if not db_name:
            return {}

        result = {}
        for procedure_id, procedure_info in self.procedures.items():
            if procedure_info.get("database") == db_name:
                procedure_name = procedure_info["name"]
                result[procedure_name] = procedure_info

        return result

    # Function management methods
    def create_function(self, function_name, parameters, return_type, body):
        """Create a function in the catalog."""
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."

        function_id = f"{db_name}.{function_name}"

        # Check if function already exists
        if function_id in self.functions:
            return f"Function {function_name} already exists."

        # Store function info
        self.functions[function_id] = {
            "name": function_name,
            "parameters": parameters,
            "return_type": return_type,
            "body": body,
            "database": db_name,
            "created_at": datetime.datetime.now().isoformat(),
        }

        # Save changes
        self._save_json(self.functions_file, self.functions)
        
        logging.info("Function %s created", function_name)
        return True

    def drop_function(self, function_name):
        """Drop a function from the catalog."""
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."

        function_id = f"{db_name}.{function_name}"

        # Check if function exists
        if function_id not in self.functions:
            return f"Function {function_name} does not exist."

        # Remove function
        del self.functions[function_id]

        # Save changes
        self._save_json(self.functions_file, self.functions)
        
        logging.info("Function %s dropped", function_name)
        return True

    def get_function(self, function_name):
        """Get function definition by name."""
        db_name = self.get_current_database()
        if not db_name:
            return None

        function_id = f"{db_name}.{function_name}"
        return self.functions.get(function_id)

    def list_functions(self):
        """List all functions in the current database."""
        db_name = self.get_current_database()
        if not db_name:
            return {}

        result = {}
        for function_id, function_info in self.functions.items():
            if function_info.get("database") == db_name:
                function_name = function_info["name"]
                result[function_name] = function_info

        return result

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

    def _parse_foreign_key_constraint(self, constraint_str):
        """Parse foreign key constraint using SQLGlot instead of regex.

        Args:
            constraint_str: String like "FOREIGN KEY (col) REFERENCES table (col)"

        Returns:
            Dictionary with parsed FK info or None if parsing fails
        """
        try:
            # Create a temporary CREATE TABLE statement to parse the constraint
            temp_sql = f"CREATE TABLE temp (id INT, {constraint_str})"
            parsed = parse_one(temp_sql, dialect=self.sqlglot_parser.dialect)

            if isinstance(parsed, exp.Create) and isinstance(parsed.this, exp.Schema):
                if parsed.this.expressions:
                    for expr in parsed.this.expressions:
                        if isinstance(expr, exp.ForeignKey):
                            # Extract foreign key information
                            fk_columns = [col.name for col in expr.expressions] if expr.expressions else []
                            ref_table = expr.reference.this.name if expr.reference and expr.reference.this else None
                            ref_columns = [col.name for col in expr.reference.expressions] if expr.reference and expr.reference.expressions else []

                            if fk_columns and ref_table and ref_columns:
                                return {
                                    "fk_column": fk_columns[0],  # First column for compatibility
                                    "ref_table": ref_table,
                                    "ref_column": ref_columns[0]  # First column for compatibility
                                }

            # Fallback: simple string parsing for basic cases
            return self._parse_foreign_key_fallback(constraint_str)

        except Exception as e:
            logging.warning(f"SQLGlot FK parsing failed for '{constraint_str}': {e}")
            return self._parse_foreign_key_fallback(constraint_str)

    def _parse_foreign_key_fallback(self, constraint_str):
        """Fallback FK parsing without regex."""
        constraint_upper = constraint_str.upper()

        if "FOREIGN KEY" not in constraint_upper or "REFERENCES" not in constraint_upper:
            return None

        try:
            # Simple parsing for common patterns
            # FOREIGN KEY (col) REFERENCES table (col)
            if "(" in constraint_str and ")" in constraint_str:
                parts = constraint_str.split()
                fk_column = None
                ref_table = None
                ref_column = None

                for i, part in enumerate(parts):
                    if part.upper() == "KEY" and i + 1 < len(parts):
                        # Next part should contain the column
                        next_part = parts[i + 1]
                        if "(" in next_part and ")" in next_part:
                            fk_column = next_part.strip("()")
                    elif part.upper() == "REFERENCES" and i + 1 < len(parts):
                        # Next part is the table name
                        ref_table = parts[i + 1]
                        # Column after that
                        if i + 2 < len(parts):
                            col_part = parts[i + 2]
                            if "(" in col_part and ")" in col_part:
                                ref_column = col_part.strip("()")

                if fk_column and ref_table and ref_column:
                    return {
                        "fk_column": fk_column,
                        "ref_table": ref_table,
                        "ref_column": ref_column
                    }
        except Exception as e:
            logging.warning(f"FK fallback parsing failed: {e}")

        return None
