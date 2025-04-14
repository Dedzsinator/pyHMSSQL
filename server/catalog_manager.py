import json
import os
import re
import traceback
import shutil
import logging
from hashlib import sha256
import datetime
from bptree import BPlusTree


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
        return f"Database {db_name} dropped."

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

        # Process columns for IDENTITY
        processed_columns = {}
        for col_def in columns:
            if isinstance(col_def, dict):
                col_name = col_def.get("name", "")
                processed_columns[col_name] = col_def

                # Check for IDENTITY attribute in column definition
                if "identity" in col_def:
                    logging.debug("Found IDENTITY column: %s", col_name)
            else:
                # For string format, parse it to look for IDENTITY
                parts = str(col_def).split()
                if len(parts) >= 2:
                    col_name = parts[0]
                    col_type = parts[1]

                    # Create column definition
                    column_def = {"type": col_type}

                    # Check for IDENTITY
                    if "IDENTITY" in str(col_def).upper():
                        column_def["identity"] = True

                        # Extract seed and increment if specified
                        identity_match = re.search(
                            r"IDENTITY\s*\((\d+),\s*(\d+)\)",
                            str(col_def),
                            re.IGNORECASE,
                        )
                        if identity_match:
                            column_def["identity_seed"] = int(
                                identity_match.group(1))
                            column_def["identity_increment"] = int(
                                identity_match.group(2)
                            )
                        else:
                            column_def["identity_seed"] = 1
                            column_def["identity_increment"] = 1

                        logging.debug("Parsed IDENTITY column: %s", col_name)

                    # Check for PRIMARY KEY
                    if "PRIMARY KEY" in str(col_def).upper():
                        column_def["primary_key"] = True

                    processed_columns[col_name] = column_def

        # Create table metadata
        self.tables[table_id] = {
            "database": db_name,
            "name": table_name,
            "columns": processed_columns,
            "constraints": constraints,
            "created_at": datetime.datetime.now().isoformat(),
        }

        # Add to database's tables list
        if table_name not in self.databases[db_name]["tables"]:
            self.databases[db_name]["tables"].append(table_name)

        # Create a B+ tree for the table
        table_file = os.path.join(
            self.tables_dir, db_name, f"{table_name}.tbl")
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

    def query_with_condition(self, table_name, conditions=None, columns=None):
        """Query table data with conditions."""
        if conditions is None:
            conditions = []
        if columns is None:
            columns = ["*"]

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
            logging.warning("Table '%s' not found in database '%s'", table_name, db_name)
            return []

        # Use the correct table name for the lookup
        table_id = f"{db_name}.{actual_table_name}"

        # Check if table exists
        if table_id not in self.tables:
            logging.warning("Table ID '%s' not found in catalog", table_id)
            return []

        # Try to use index for simple equality conditions
        index_to_use = None
        condition_value = None

        if len(conditions) == 1 and conditions[0].get("operator") == "=":
            col = conditions[0].get("column")
            val = conditions[0].get("value")

            # Handle string values - remove quotes if present
            if isinstance(val, str):
                if (val.startswith("'") and val.endswith("'")) or \
                (val.startswith('"') and val.endswith('"')):
                    val = val[1:-1]  # Remove quotes
                    conditions[0]["value"] = val  # Update the condition value
                    logging.info("Removed quotes from condition value: %s", val)

            # Check for available indexes on this column
            indexes = self.get_indexes_for_table(actual_table_name)
            for idx_name, idx_info in indexes.items():
                if idx_info.get("column").lower() == col.lower():
                    index_to_use = idx_name
                    condition_value = val
                    logging.info("Using index %s for query on column %s with value %s",
                                idx_name, col, val
                    )
                    break

        if index_to_use and condition_value is not None:
            # Use index lookup instead of full table scan
            index_file = os.path.join(self.indexes_dir,\
                f"{db_name}_{actual_table_name}_{conditions[0].get('column')}.idx")
            if os.path.exists(index_file):
                try:
                    logging.info("Loading index file: %s", index_file)
                    index_tree = BPlusTree.load_from_file(index_file)
                    record_key = index_tree.search(condition_value)
                    if record_key:
                        logging.info("Found record key %s using index %s", record_key, index_to_use)
                        record = self.get_record_by_key(actual_table_name, record_key)
                        logging.info("Found record using index %s: %s", index_to_use, record)
                        return [record] if record else []
                    logging.info("No record found with %s=%s using index",
                                conditions[0].get('column'), condition_value)
                except RuntimeError as e:
                    logging.error("Error using index: %s", str(e))
                    # Fall through to full table scan
            else:
                logging.warning("Index file not found: %s", index_file)

        # Load the table file for full scan
        table_file = os.path.join(self.tables_dir, db_name, f"{actual_table_name}.tbl")
        if not os.path.exists(table_file):
            logging.warning("Table file not found at: %s", table_file)
            return []

        try:
            # Load B+ tree
            logging.debug("Performing full table scan on: %s", table_file)
            tree = BPlusTree.load_from_file(table_file)

            if tree is None:
                logging.error("Failed to load B+ tree for %s", table_file)
                return []

            # Get all records
            all_records = tree.range_query(float("-inf"), float("inf"))
            logging.debug("Found %s total records in %s", len(all_records), actual_table_name)

            # Filter records based on conditions
            results = []
            for key, record in all_records:
                # Check if record matches all conditions
                matches = True
                for condition in conditions:
                    col = condition.get("column")
                    op = condition.get("operator")
                    val = condition.get("value")

                    # Skip conditions with missing parts
                    if not col or not op:
                        continue

                    # Case-insensitive column matching
                    matching_col = None
                    for record_col in record:
                        # Get the column name, handling different formats
                        col_name = record_col
                        if isinstance(record_col, dict) and "name" in record_col:
                            col_name = record_col["name"]

                        # Compare column names case-insensitively
                        if isinstance(col_name, str) and isinstance(col, str)\
                            and col_name.lower() == col.lower():
                            matching_col = record_col
                            break

                    if matching_col is None:
                        matches = False
                        break

                    # Get the value using the matching column name
                    record_val = record.get(matching_col)

                    # Handle dictionary values - extract actual value if needed
                    if isinstance(record_val, dict):
                        if "value" in record_val:
                            record_val = record_val["value"]

                    # Handle string comparison - normalize for string values
                    if isinstance(val, str):
                        # Remove quotes if present in values
                        if (val.startswith("'") and val.endswith("'")) or \
                        (val.startswith('"') and val.endswith('"')):
                            val = val[1:-1]  # Remove quotes

                    # Apply operator with proper type handling
                    try:
                        if op == "=":
                            if isinstance(record_val, str) and isinstance(val, str):
                                if record_val.lower() != val.lower():
                                    matches = False
                                    break
                            elif record_val != val:
                                matches = False
                                break
                        elif op == ">":
                            try:
                                if float(record_val) <= float(val):
                                    matches = False
                                    break
                            except (ValueError, TypeError):
                                if str(record_val) <= str(val):
                                    matches = False
                                    break
                        elif op == "<":
                            try:
                                if float(record_val) >= float(val):
                                    matches = False
                                    break
                            except (ValueError, TypeError):
                                if str(record_val) >= str(val):
                                    matches = False
                                    break
                        elif op == ">=":
                            try:
                                if float(record_val) < float(val):
                                    matches = False
                                    break
                            except (ValueError, TypeError):
                                if str(record_val) < str(val):
                                    matches = False
                                    break
                        elif op == "<=":
                            try:
                                if float(record_val) > float(val):
                                    matches = False
                                    break
                            except (ValueError, TypeError):
                                if str(record_val) > str(val):
                                    matches = False
                                    break
                        elif op == "!=":
                            if record_val == val:
                                matches = False
                                break
                    except RuntimeError as e:
                        logging.error("Error comparing values: %s", str(e))
                        matches = False
                        break

                if matches:
                    # Project selected columns
                    if "*" in columns:
                        results.append(record)
                    else:
                        projected = {}
                        for col in columns:
                            # Case-insensitive column matching
                            for record_col in record:
                                # Extract column name for comparison
                                col_name = record_col
                                if isinstance(record_col, dict) and "name" in record_col:
                                    col_name = record_col["name"]

                                if isinstance(col_name, str) and\
                                    isinstance(col, str) and col_name.lower() == col.lower():
                                    projected[record_col] = record[record_col]
                                    break
                        if projected:
                            results.append(projected)

                logging.info("Found %s matching records after filtering", len(results))

            return results

        except RuntimeError as e:
            logging.error("Error querying table: %s", str(e))
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

        # Check if record is already mapped and clean up flag if present
        already_mapped = record.pop("_already_mapped", False) if isinstance(record, dict) else False

        # Only map if not already mapped by executor
        if not already_mapped:
            # Get table schema to extract column names
            table_info = self.tables[table_id]
            columns_data = table_info.get("columns", {})
            column_names = []

            # Extract column names from schema based on the format
            if isinstance(columns_data, dict):
                column_names = list(columns_data.keys())
            elif isinstance(columns_data, list):
                for col in columns_data:
                    if isinstance(col, dict) and "name" in col:
                        column_names.append(col["name"])
                    elif isinstance(col, str):
                        # Extract column name from string definition
                        col_name = col.split()[0] if " " in col else col
                        column_names.append(col_name)

            # Check if we have a record as a list that needs to be mapped to column names
            if hasattr(record, "__iter__") and not isinstance(record, dict) and column_names:
                # Convert list values to a dictionary with column names
                record_dict = {}
                for i, val in enumerate(record):
                    if i < len(column_names):
                        # Remove quotes from string values if needed
                        if isinstance(val, str) and ((val.startswith("'") and val.endswith("'")) or
                                                (val.startswith('"') and val.endswith('"'))):
                            val = val[1:-1]  # Remove quotes
                        record_dict[column_names[i]] = val
                record = record_dict
                logging.info("Mapped values to columns: %s", record)

        # Get table schema to check constraints
        table_schema = self.tables[table_id]
        pk_column = None
        is_identity = False
        identity_seed = 1
        identity_increment = 1

        # Find primary key column and check if it's an IDENTITY column
        columns_data = table_schema.get("columns", {})

        # Check the type and handle accordingly
        if isinstance(columns_data, dict):
            # Dictionary format - check for primary key
            for col_name, col_def in columns_data.items():
                if isinstance(col_def, dict):
                    if col_def.get("primary_key", False):
                        pk_column = col_name
                    if col_def.get("identity", False):
                        is_identity = True
                        identity_seed = col_def.get("identity_seed", 1)
                        identity_increment = col_def.get(
                            "identity_increment", 1)
                        pk_column = (
                            col_name  # IDENTITY columns are typically primary keys
                        )
        elif isinstance(columns_data, list):
            # List format - check constraints for PRIMARY KEY
            constraints = table_schema.get("constraints", [])
            for constraint in constraints:
                if isinstance(constraint, str):
                    # Check for PRIMARY KEY constraint
                    pk_match = re.search(
                        r"PRIMARY\s+KEY\s*\(\s*(\w+)\s*\)", constraint, re.IGNORECASE
                    )
                    if pk_match:
                        pk_column = pk_match.group(1)

                    # Check for IDENTITY specification
                    if "IDENTITY" in constraint.upper():
                        identity_match = re.search(
                            r"(\w+)\s+.*IDENTITY\s*(?:\((\d+),\s*(\d+)\))?",
                            constraint,
                            re.IGNORECASE,
                        )
                        if identity_match:
                            id_col = identity_match.group(1)
                            is_identity = True
                            pk_column = (
                                id_col  # IDENTITY columns are typically primary keys
                            )

                            # Get seed and increment if specified
                            if identity_match.group(2) and identity_match.group(3):
                                identity_seed = int(identity_match.group(2))
                                identity_increment = int(
                                    identity_match.group(3))

        logging.debug(
            "Table %s - PK: %s, Identity: %s",
            table_name, pk_column, is_identity
        )

        # Load the table file
        table_file = os.path.join(
            self.tables_dir, db_name, f"{table_name}.tbl")

        try:
            # Load or create B+ tree
            if os.path.exists(table_file):
                tree = BPlusTree.load_from_file(table_file)
                if tree is None:
                    # If loading failed, create a new tree
                    tree = BPlusTree(order=50, name=table_name)
            else:
                tree = BPlusTree(order=50, name=table_name)

            # Get all existing records to check constraints and get max identity value
            all_records = tree.range_query(float("-inf"), float("inf"))
            max_identity_value = identity_seed - identity_increment

            # Handle IDENTITY column if applicable
            if is_identity and pk_column:
                # Check if the column was explicitly provided
                if pk_column in record:
                    return f"Cannot insert explicit value for identity column '{pk_column}'"

                # Find the current max value for the identity column
                for _, existing_record in all_records:
                    if pk_column in existing_record:
                        max_identity_value = max(
                            max_identity_value, existing_record[pk_column]
                        )

                # Generate the next value
                next_value = max_identity_value + identity_increment
                record[pk_column] = next_value
                logging.debug("Generated IDENTITY value: %s for %s",
                    next_value, pk_column
                )

            # Check for primary key violation
            if pk_column and pk_column in record:
                pk_value = record[pk_column]
                logging.debug(
                    "Checking primary key constraint: %s=%s",
                    pk_column, pk_value
                )

                # Check if this primary key already exists
                for _, existing_record in all_records:
                    if (
                        pk_column in existing_record
                        and existing_record[pk_column] == pk_value
                    ):
                        logging.warning(
                            "Primary key violation: %s=%s already exists",
                            pk_column, pk_value
                        )
                        return f"Primary key violation: {pk_column}={pk_value} already exists"

            # Generate a unique record ID (use primary key if available)
            if pk_column and pk_column in record:
                record_id = record[pk_column]
            else:
                # Generate a unique ID if no primary key
                record_id = int(datetime.datetime.now().timestamp() * 1000000)

            logging.debug("Inserting record with ID %s: %s", record_id, record)

            # Insert the record into the B+ tree
            tree.insert(record_id, record)

            # Save the updated tree
            tree.save_to_file(table_file)

            logging.info("Record inserted into %s with ID %s", table_name, record_id)

            # Update indexes if any
            self._update_indexes_after_insert(
                db_name, table_name, record_id, record)

            return True

        except RuntimeError as e:
            logging.error("Error inserting record: %s", str(e))

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
                        index_tree = BPlusTree(
                            order=50, name=f"{table_name}_{column}_index"
                        )
                except RuntimeError:
                    index_tree = BPlusTree(
                        order=50, name=f"{table_name}_{column}_index"
                    )
            else:
                index_tree = BPlusTree(
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
        table_file = os.path.join(self.tables_dir, db_name, f"{
                                  actual_table_name}.tbl")
        if not os.path.exists(table_file):
            return "Table data file not found."

        try:
            # Load the B+ tree
            tree = BPlusTree.load_from_file(table_file)
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

            # Create a new tree with only the records to keep
            new_tree = BPlusTree(order=50, name=actual_table_name)

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

            return f"{len(records_to_delete)} records deleted."

        except RuntimeError as e:
            logging.error("Error deleting records: %s", str(e))

            logging.error(traceback.format_exc())
            return f"Error deleting records: {str(e)}"

    def update_record(self, table_name, record_id, update_data):
        """Update a record by its primary key."""
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
            tree = BPlusTree.load_from_file(table_file)
            if tree is None:
                return False

            # First find the record by querying with conditions to get the actual B+ tree key
            conditions = [{"column": "id", "operator": "=", "value": record_id}]
            records = self.query_with_condition(table_name, conditions)

            if not records or len(records) == 0:
                return False

            # Find out which key in the B+ tree corresponds to this record
            all_records = tree.range_query(float('-inf'), float('inf'))
            tree_key = None

            for key, record in all_records:
                if record.get("id") == record_id:
                    tree_key = key
                    break

            if tree_key is None:
                return False

            # Get the existing record and update it
            updated_record = records[0].copy()
            for field, value in update_data.items():
                updated_record[field] = value

            # Update in the tree using the correct key
            tree.insert(tree_key, updated_record)

            # Save the updated tree
            tree.save_to_file(table_file)

            return True

        except RuntimeError as e:
            logging.error("Error updating record: %s", str(e))
            return False

    def create_index(
        self,
        table_name,
        column_name,
        index_type="BTREE",
        is_unique=False,
        index_name=None,
    ):
        """Create an index on a table column."""
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."

        # Use provided index_name or generate one
        if not index_name:
            index_name = f"idx_{column_name}"

        table_id = f"{db_name}.{table_name}"
        # Use index_name instead of column_name
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
            "name": index_name,  # Store the index name
            "created_at": datetime.datetime.now().isoformat(),
        }

        # Build the index
        table_file = os.path.join(
            self.tables_dir, db_name, f"{table_name}.tbl")
        index_file = os.path.join(
            self.indexes_dir, f"{db_name}_{table_name}_{column_name}.idx"
        )

        try:
            # Load table data
            if os.path.exists(table_file):
                table_tree = BPlusTree.load_from_file(table_file)
                all_records = table_tree.range_query(
                    float("-inf"), float("inf"))

                # Create index tree
                index_tree = BPlusTree(
                    order=50, name=f"{table_name}_{column_name}_index"
                )

                # Populate index
                for record_key, record in all_records:
                    if column_name in record:
                        index_tree.insert(record[column_name], record_key)

                # Save index
                index_tree.save_to_file(index_file)

            # Save changes to catalog
            self._save_json(self.indexes_file, self.indexes)

            logging.info("Index created on %s.%s", table_name, column_name)
            return f"Index created on {table_name}.{column_name}"

        except RuntimeError as e:
            logging.error("Error creating index: %s", str(e))
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
        index_file = os.path.join(
            self.indexes_dir, f"{db_name}_{table_name}_{column_name}.idx"
        )
        if os.path.exists(index_file):
            os.remove(index_file)

        # Save changes
        self._save_json(self.indexes_file, self.indexes)

        logging.info("Index %s dropped from %s", index_name, table_name)
        return f"Index {index_name} dropped from {table_name}"

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
                    self.indexes_dir, f"{db_name}_{
                        table_name}_{column_name}.idx"
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
        return f"View {view_name} dropped"

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
        table_file = os.path.join(
            self.tables_dir, db_name, f"{table_name}.tbl")
        if not os.path.exists(table_file):
            return None

        try:
            # Load the B+ tree
            tree = BPlusTree.load_from_file(table_file)
            if tree is None:
                return None

            # Search for the record
            return tree.search(record_key)
        except RuntimeError as e:
            logging.error("Error retrieving record by key: %s", str(e))
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
            tree = BPlusTree.load_from_file(table_file)
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
