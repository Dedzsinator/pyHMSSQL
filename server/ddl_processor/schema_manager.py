import logging
import traceback
import re
import os

class SchemaManager:
    """Handles DDL operations related to database and table schemas"""

    def __init__(self, catalog_manager):
        self.catalog_manager = catalog_manager

    def execute_database_operation(self, plan):
        """Execute database-level DDL operations"""
        plan_type = plan.get("type")

        if plan_type == "CREATE_DATABASE":
            return self.execute_create_database(plan)
        elif plan_type == "DROP_DATABASE":
            return self.execute_drop_database(plan)
        elif plan_type == "USE_DATABASE":
            return self.execute_use_database(plan)
        else:
            return {"error": f"Unsupported database operation: {plan_type}", "status": "error"}

    def execute_table_operation(self, plan):
        """Execute table-level DDL operations"""
        plan_type = plan.get("type")

        if plan_type == "CREATE_TABLE":
            return self.execute_create_table(plan)
        elif plan_type == "DROP_TABLE":
            return self.execute_drop_table(plan)
        else:
            return {"error": f"Unsupported table operation: {plan_type}", "status": "error"}

    def execute_index_operation(self, plan):
        """Execute index-level DDL operations"""
        plan_type = plan.get("type")

        if plan_type == "CREATE_INDEX":
            return self.execute_create_index(plan)
        elif plan_type == "DROP_INDEX":
            return self.execute_drop_index(plan)
        else:
            return {"error": f"Unsupported index operation: {plan_type}", "status": "error"}

    def execute_show_operation(self, plan):
        """Execute SHOW commands"""
        return self.execute_show(plan)

    def execute_create_database(self, plan):
        """Execute CREATE DATABASE operation"""
        database_name = plan["database"]
        result = self.catalog_manager.create_database(database_name)
        return {"message": result}

    def execute_drop_database(self, plan):
        """Execute DROP DATABASE operation."""
        database_name = plan.get("database")

        if not database_name:
            return {
                "error": "No database name specified",
                "status": "error",
                "type": "error",
            }

        # Check if database exists (case-sensitive)
        available_dbs = self.catalog_manager.list_databases()
        if database_name not in available_dbs:
            # Try case-insensitive match
            found = False
            for db in available_dbs:
                if db.lower() == database_name.lower():
                    database_name = db  # Use the correct case
                    found = True
                    break

            if not found:
                return {
                    "error": f"Database '{database_name}' does not exist",
                    "status": "error",
                    "type": "error",
                }

        # Use catalog manager to drop database
        try:
            result = self.catalog_manager.drop_database(database_name)

            # If this was the current database, reset it
            if self.catalog_manager.get_current_database() == database_name:
                self.current_database = (
                    None  # Update the execution engine's current database
                )

                # Choose another database if available
                remaining_dbs = self.catalog_manager.list_databases()
                if remaining_dbs:
                    self.catalog_manager.set_current_database(remaining_dbs[0])
                    self.current_database = remaining_dbs[0]
                else:
                    self.catalog_manager.set_current_database(None)

            return {
                "message": f"Database '{database_name}' dropped successfully",
                "status": "success",
                "type": "drop_database_result",
            }
        except Exception as e:
            logging.error(f"Error dropping database: {str(e)}")
            import traceback

            logging.error(traceback.format_exc())
            return {
                "error": f"Error dropping database: {str(e)}",
                "status": "error",
                "type": "error",
            }

    def execute_use_database(self, plan):
        """Execute USE DATABASE operation."""
        database_name = plan.get("database")

        if not database_name:
            return {"error": "No database name specified"}

        # Check if database exists
        all_dbs = self.catalog_manager.list_databases()

        if database_name not in all_dbs:
            return {"error": f"Database '{database_name}' does not exist"}

        # Set the current database
        self.catalog_manager.set_current_database(database_name)
        self.current_database = database_name

        return {"message": f"Now using database '{database_name}'"}

    def execute_show(self, plan):
        """Execute SHOW commands."""
        object_type = plan.get("object")

        if not object_type:
            return {
                "error": "No object type specified for SHOW command",
                "status": "error",
            }

        if object_type.upper() == "DATABASES":
            # List all databases
            databases = self.catalog_manager.list_databases()
            return {
                "columns": ["Database Name"],
                "rows": [[db] for db in databases],
                "status": "success",
            }

        elif object_type.upper() == "TABLES":
            # List all tables in the current database
            db_name = self.catalog_manager.get_current_database()
            if not db_name:
                return {
                    "error": "No database selected. Use 'USE database_name' first.",
                    "status": "error",
                }

            tables = self.catalog_manager.list_tables(db_name)
            return {
                "columns": ["Table Name"],
                "rows": [[table] for table in tables],
                "status": "success",
            }

        elif object_type.upper() == "INDEXES":
            # Show indexes
            table_name = plan.get("table")
            db_name = self.catalog_manager.get_current_database()
            if not db_name:
                return {
                    "error": "No database selected. Use 'USE database_name' first.",
                    "status": "error",
                }

            if table_name:
                # Show indexes for specific table
                indexes = self.catalog_manager.get_indexes_for_table(
                    table_name)
                if not indexes:
                    return {
                        "columns": ["Table", "Column", "Index Name", "Type"],
                        "rows": [],
                        "status": "success",
                        "message": f"No indexes found for table '{table_name}'",
                    }

                rows = []
                for idx_name, idx_info in indexes.items():
                    rows.append(
                        [
                            table_name,
                            idx_info.get("column", ""),
                            idx_name,
                            idx_info.get("type", "BTREE"),
                        ]
                    )

                return {
                    "columns": ["Table", "Column", "Index Name", "Type"],
                    "rows": rows,
                    "status": "success",
                }
            else:
                # Show all indexes in the current database
                all_indexes = []
                for index_id, index_info in self.catalog_manager.indexes.items():
                    if index_id.startswith(f"{db_name}."):
                        parts = index_id.split(".")
                        if len(parts) >= 3:
                            table = parts[1]
                            column = index_info.get("column", "")
                            index_name = parts[2]
                            index_type = index_info.get("type", "BTREE")
                            all_indexes.append(
                                [table, column, index_name, index_type])

                return {
                    "columns": ["Table", "Column", "Index Name", "Type"],
                    "rows": all_indexes,
                    "status": "success",
                }

        else:
            return {
                "error": f"Unknown object type: {object_type} for SHOW command",
                "status": "error",
            }

    def execute_create_table(self, plan):
        """Execute CREATE TABLE operation."""
        table_name = plan.get("table")
        column_strings = plan.get("columns", [])

        if not table_name:
            return {
                "error": "No table name specified",
                "status": "error",
                "type": "error",
            }

        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {
                "error": "No database selected. Use 'USE database_name' first.",
                "status": "error",
                "type": "error",
            }

        # Check if table already exists
        existing_tables = self.catalog_manager.list_tables(db_name)
        if table_name in existing_tables:
            return {
                "error": f"Table '{table_name}' already exists in database '{db_name}'",
                "status": "error",
                "type": "error",
            }

        # Parse column definitions
        columns = []
        constraints = []

        for col_str in column_strings:
            if col_str.upper().startswith(
                ("PRIMARY", "FOREIGN", "UNIQUE", "CHECK", "CONSTRAINT")
            ):
                constraints.append(col_str)
                continue

            # Extract column definition parts
            parts = col_str.split()
            if len(parts) >= 2:
                col_name = parts[0]
                col_type = parts[1]

                # Create column definition
                col_def = {"name": col_name, "type": col_type}

                # Check for additional column attributes
                col_str_upper = col_str.upper()

                # Handle PRIMARY KEY
                if "PRIMARY KEY" in col_str_upper:
                    col_def["primary_key"] = True

                # Handle NOT NULL
                if "NOT NULL" in col_str_upper:
                    col_def["nullable"] = False

                # Handle IDENTITY(seed, increment)
                if "IDENTITY" in col_str_upper:
                    col_def["identity"] = True

                    # Extract seed and increment values if specified
                    identity_match = re.search(
                        r"IDENTITY\s*\((\d+),\s*(\d+)\)", col_str, re.IGNORECASE
                    )
                    if identity_match:
                        col_def["identity_seed"] = int(identity_match.group(1))
                        col_def["identity_increment"] = int(
                            identity_match.group(2))
                    else:
                        # Default seed=1, increment=1
                        col_def["identity_seed"] = 1
                        col_def["identity_increment"] = 1

                # Add the column definition
                columns.append(col_def)

        # Create the table through catalog manager
        try:
            result = self.catalog_manager.create_table(
                table_name, columns, constraints)
            if result is True:
                return {
                    "message": f"Table '{table_name}' created in database '{db_name}'",
                    "status": "success",
                    "type": "create_table_result",
                }
            else:
                return {"error": result, "status": "error", "type": "error"}
        except Exception as e:
            logging.error(f"Error creating table: {str(e)}")
            return {
                "error": f"Error creating table: {str(e)}",
                "status": "error",
                "type": "error",
            }

    def execute_drop_table(self, plan):
        """Execute DROP TABLE operation."""
        table_name = plan.get("table")

        if not table_name:
            return {
                "error": "No table name specified",
                "status": "error",
                "type": "error",
            }

        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {
                "error": "No database selected. Use 'USE database_name' first.",
                "status": "error",
                "type": "error",
            }

        # Use catalog manager to drop the table
        try:
            # Check for case-insensitive table match
            tables = self.catalog_manager.list_tables(db_name)
            actual_table_name = table_name  # Default

            # Try direct match first
            if table_name not in tables:
                # Try case-insensitive match
                for db_table in tables:
                    if db_table.lower() == table_name.lower():
                        actual_table_name = db_table
                        break

            result = self.catalog_manager.drop_table(actual_table_name)

            if isinstance(result, str) and "does not exist" in result:
                return {"error": result, "status": "error", "type": "error"}

            # Also remove any temporary data or caches related to this table
            # (cleared from memory if any exists)

            return {
                "message": f"Table '{actual_table_name}' dropped from database '{db_name}'",
                "status": "success",
                "type": "drop_table_result",
            }
        except Exception as e:
            logging.error('Error dropping table: %s', str(e))

            logging.error(traceback.format_exc())
            return {
                "error": f"Error dropping table: {str(e)}",
                "status": "error",
                "type": "error",
            }

    def execute_create_index(self, plan):
        """Execute CREATE INDEX operation."""
        # Get index_name from either field
        index_name = plan.get("index_name") or plan.get("index") 
        
        # If index_name is still None, create a default name
        if not index_name:
            index_name = f"idx_{plan.get('table')}_{plan.get('column')}"
            
        table_name = plan.get("table")
        column = plan.get("column")
        is_unique = plan.get("unique", False)

        # Log the parameters for debugging
        logging.info(f"Creating index '{index_name}' on {table_name}.{column}")

        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected. Use 'USE database_name' first.",
                    "status": "error", 
                    "type": "error"}

        # Verify table exists
        tables = self.catalog_manager.list_tables(db_name)
        if table_name not in tables:
            return {"error": f"Table '{table_name}' does not exist in database '{db_name}'",
                    "status": "error",
                    "type": "error"}

        # Create the index - PASS INDEX_NAME TO CATALOG MANAGER
        try:
            result = self.catalog_manager.create_index(
                table_name, column, "BTREE", is_unique, index_name  # Add index_name here
            )
            if isinstance(result, str) and "already exists" in result:
                return {"error": result, "status": "error", "type": "error"}

            return {
                "message": f"Index '{index_name}' created on '{table_name}.{column}'",
                "status": "success",
                "type": "create_index_result"
            }
        except Exception as e:
            logging.error(f"Error creating index: {str(e)}")
            return {"error": f"Error creating index: {str(e)}", 
                    "status": "error", 
                    "type": "error"}

    def execute_drop_index(self, plan):
        """Execute DROP INDEX operation."""
        index_name = plan["index_name"]
        table_name = plan["table"]

        if not table_name:
            return {"error": "Table name must be specified for DROP INDEX"}

        if not index_name:
            return {"error": "Index name must be specified for DROP INDEX"}

        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected. Use 'USE database_name' first."}

        # Drop the index through catalog_manager
        try:
            result = self.catalog_manager.drop_index(table_name, index_name)
            if isinstance(result, str) and "does not exist" in result:
                return {"error": result}

            return {
                "message": f"Index '{index_name}' dropped from table '{table_name}'."
            }
        except Exception as e:
            logging.error(f"Error dropping index: {str(e)}")
            return {"error": f"Error dropping index: {str(e)}"}

    def get_table_schema(self, table_name):
        """Get the schema of a table."""
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return "No database selected."

        # Check if the table exists
        if not self.catalog_manager.table_exists(db_name, table_name):
            return f"Table {table_name} does not exist."

        # Get table schema from catalog manager
        return self.catalog_manager.get_table_schema(table_name)
