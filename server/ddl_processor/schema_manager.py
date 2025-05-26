"""_summary_

Returns:
    _type_: _description_
"""
import logging
import traceback
import re
from shared.utils import get_current_database_or_error


class SchemaManager:
    """Handles DDL operations related to database and table schemas"""

    def __init__(self, catalog_manager):
        self.catalog_manager = catalog_manager
        self.current_database = catalog_manager.get_current_database()

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
            return {
                "error": f"Unsupported database operation: {plan_type}",
                "status": "error",
            }

    def execute_table_operation(self, plan):
        """Execute table-level DDL operations"""
        plan_type = plan.get("type")

        if plan_type == "CREATE_TABLE":
            return self.execute_create_table(plan)
        elif plan_type == "DROP_TABLE":
            return self.execute_drop_table(plan)
        else:
            return {
                "error": f"Unsupported table operation: {plan_type}",
                "status": "error",
                "type": "error"
            }

    def execute_index_operation(self, plan):
        """Execute index-level DDL operations"""
        plan_type = plan.get("type")

        if plan_type == "CREATE_INDEX":
            return self.execute_create_index(plan)
        elif plan_type == "DROP_INDEX":
            return self.execute_drop_index(plan)
        else:
            return {
                "error": f"Unsupported index operation: {plan_type}",
                "status": "error",
            }

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
            self.catalog_manager.drop_database(database_name)

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
        except RuntimeError as e:
            logging.error("Error dropping database: %s", str(e))

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
                    "status": "error"
                }

            tables = self.catalog_manager.list_tables(db_name)
            return {
                "columns": ["Table Name"],
                "rows": [[table] for table in tables],
                "status": "success",
            }
            
        elif object_type.upper() == "ALL_TABLES":
            # Show all tables across all databases
            logging.info("Executing SHOW ALL_TABLES command")
            all_tables = self.catalog_manager.get_all_tables()
            
            return {
                "columns": ["DATABASE_NAME", "TABLE_NAME"],
                "rows": all_tables,
                "status": "success",
                "execution_time_ms": 0.001 * 1000,
                "message": f"Retrieved {len(all_tables)} tables across all databases"
            }

        elif object_type.upper() == "INDEXES":
            # Show indexes
            table_name = plan.get("table")
            db_name = self.catalog_manager.get_current_database()
            if not db_name:
                return {
                    "error": "No database selected. Use 'USE database_name' first.",
                    "status": "error"
                }

            if table_name:
                # Show indexes for specific table
                indexes = self.catalog_manager.get_indexes_for_table(table_name)
                if not indexes:
                    return {
                        "columns": ["Table", "Column", "Index Name (use this for DROP INDEX)", "Type"],
                        "rows": [],
                        "status": "success",
                        "message": f"No indexes found for table '{table_name}'",
                    }

                rows = []
                for idx_name, idx_info in indexes.items():
                    column_name = idx_info.get("column", "")
                    rows.append([
                        table_name,
                        column_name,
                        f"{idx_name}",  # Highlight the exact name to use with DROP INDEX
                        idx_info.get("type", "BTREE"),
                    ])

                return {
                    "columns": ["Table", "Column", "Index Name (use this for DROP INDEX)", "Type"],
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
                            all_indexes.append([
                                table,
                                column,
                                f"{index_name}",  # Highlight the exact name to use
                                index_type
                            ])

                return {
                    "columns": ["Table", "Column", "Index Name (use this for DROP INDEX)", "Type"],
                    "rows": all_indexes,
                    "status": "success",
                }

        elif object_type.upper() == "COLUMNS":
            # Show columns for a table
            table_name = plan.get("table")
            if not table_name:
                return {
                    "error": "Table name required for SHOW COLUMNS",
                    "status": "error"
                }

            db_name = self.catalog_manager.get_current_database()
            if not db_name:
                return {
                    "error": "No database selected. Use 'USE database_name' first.",
                    "status": "error"
                }

            # Get table schema
            table_schema = self.catalog_manager.get_table_schema(table_name)
            if not table_schema:
                return {
                    "error": f"Table '{table_name}' not found",
                    "status": "error"
                }

            rows = []
            for col_info in table_schema:
                if isinstance(col_info, dict):
                    col_name = col_info.get('name', '')
                    col_type = col_info.get('type', '')
                    is_pk = col_info.get('primary_key', False)
                    is_nullable = not col_info.get('not_null', False)
                    
                    rows.append([
                        col_name,
                        col_type,
                        "YES" if is_nullable else "NO",
                        "PRI" if is_pk else ""
                    ])

            return {
                "columns": ["Column", "Type", "Null", "Key"],
                "rows": rows,
                "status": "success"
            }

        else:
            return {
                "error": f"Unsupported SHOW object type: {object_type}",
                "status": "error"
            }

    def execute_create_table(self, plan):
        """Execute CREATE TABLE operation."""
        table_name = plan.get("table")
        column_strings = plan.get("columns", [])
        # Directly get constraints from the plan. This should contain table-level constraints like FOREIGN KEY.
        table_level_constraints_from_plan = plan.get("constraints", [])

        logging.info(f"SchemaManager: Received plan for CREATE TABLE {table_name}. Table-level constraints from plan: {table_level_constraints_from_plan}")

        if not table_name:
            return {
                "error": "No table name specified",
                "status": "error",
                "type": "error",
            }

        db_name, error = get_current_database_or_error(self.catalog_manager)
        if error:
            return error

        existing_tables = self.catalog_manager.list_tables(db_name)
        if table_name in existing_tables:
            return {
                "error": f"Table '{table_name}' already exists in database '{db_name}'",
                "status": "error",
                "type": "error",
            }

        parsed_column_definitions = []
        # Initialize the list for catalog manager with table-level constraints from the plan
        final_constraints_for_catalog = list(table_level_constraints_from_plan) # Make a copy

        if table_level_constraints_from_plan:
            logging.info(f"SchemaManager: Initialized final_constraints_for_catalog with: {final_constraints_for_catalog}")
        else:
            logging.info(f"SchemaManager: No table-level constraints provided in the plan for {table_name}.")

        # Process column strings to define columns and extract any column-level constraints
        for col_str in column_strings:
            # Basic parsing of column definition (name, type, attributes)
            # This part needs to be robust to correctly parse column definitions
            # e.g., "id INT PRIMARY KEY", "name VARCHAR(100) NOT NULL"
            parts = col_str.split() # Simplistic split
            col_name = parts[0]
            col_type = parts[1] if len(parts) > 1 else "UNKNOWN"
            
            col_def = {"name": col_name, "type": col_type} # Store basic definition

            # Extract attributes like PRIMARY KEY, NOT NULL, UNIQUE from col_str
            # and add them to col_def
            if "PRIMARY KEY" in col_str.upper():
                col_def["primary_key"] = True
                # Optionally, decide if column-level PK should also be added to final_constraints_for_catalog
                # e.g., final_constraints_for_catalog.append(f"PRIMARY KEY ({col_name})")
            if "NOT NULL" in col_str.upper():
                col_def["not_null"] = True
            if "UNIQUE" in col_str.upper() and not col_str.upper().startswith("UNIQUE"): # Column-level UNIQUE
                 col_def["unique"] = True
                # Optionally, final_constraints_for_catalog.append(f"UNIQUE ({col_name})")

            parsed_column_definitions.append(col_def)

        # The `pending_fk_constraints` logic might be redundant if this is handled correctly.
        # For now, we rely on `final_constraints_for_catalog` being correctly populated.

        try:
            # Pass the parsed column definitions and the consolidated list of constraints
            result = self.catalog_manager.create_table(table_name, parsed_column_definitions, final_constraints_for_catalog)
            
            if result is True:
                return {
                    "message": f"Table '{table_name}' created in database '{db_name}'",
                    "status": "success",
                    "type": "create_table_result",
                }
            else:
                return {"error": str(result), "status": "error", "type": "error"}
        except RuntimeError as e:
            logging.error("Error creating table: %s", str(e))
            # ...existing code...
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
                found = False
                for db_table in tables:
                    if db_table.lower() == table_name.lower():
                        actual_table_name = db_table
                        found = True
                        break

                if not found:
                    return {
                        "error": f"Table '{table_name}' does not exist in database '{db_name}'.",
                        "status": "error",
                        "type": "error",
                    }

            result = self.catalog_manager.drop_table(actual_table_name)

            # Process result properly
            if isinstance(result, str) and "does not exist" in result:
                return {"error": result, "status": "error", "type": "error"}

            return {
                "message": f"Table '{actual_table_name}'\
                dropped successfully from database '{db_name}'",
                "status": "success",
                "type": "drop_table_result",
            }
        except RuntimeError as e:
            logging.error("Error dropping table: %s", str(e))
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
        logging.info("Creating index '%s' on %s.%s", index_name, table_name, column)

        # Get the current database
        db_name, error = get_current_database_or_error(self.catalog_manager)
        if error:
            return error

        # Verify table exists
        tables = self.catalog_manager.list_tables(db_name)
        if table_name not in tables:
            return {
                "error": f"Table '{table_name}' does not exist in database '{db_name}'",
                "status": "error",
                "type": "error",
            }

        # Create the index - PASS INDEX_NAME TO CATALOG MANAGER
        try:
            result = self.catalog_manager.create_index(
                table_name,
                column,
                "BTREE",
                is_unique,
                index_name,  # Add index_name here
            )
            if isinstance(result, str) and "already exists" in result:
                return {"error": result, "status": "error", "type": "error"}

            return {
                "message": f"Index '{index_name}' created on '{table_name}.{column}'",
                "status": "success",
                "type": "create_index_result",
            }
        except RuntimeError as e:
            logging.error("Error creating index: %s", str(e))
            return {
                "error": f"Error creating index: {str(e)}",
                "status": "error",
                "type": "error",
            }

    def execute_drop_index(self, plan):
        """Drop an index on a table column"""
        index_name = plan.get("index_name")
        table_name = plan.get("table")

        if not index_name or not table_name:
            return {
                "error": "Missing index name or table name",
                "status": "error"
            }

        # Make sure we have a current database selected
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {
                "error": "No database selected",
                "status": "error"
            }

        # Call the catalog manager to drop the index
        try:
            result = self.catalog_manager.drop_index(table_name, index_name)

            # If result is a string, it's an error message
            if isinstance(result, str) and "does not exist" in result:
                return {
                    "error": result,
                    "status": "error"
                }

            return {
                "message": f"Index {index_name} dropped successfully from {table_name}",
                "status": "success"
            }
        except RuntimeError as e:
            logging.error("Error dropping index: %s", str(e))
            return {
                "error": f"Error dropping index: {str(e)}",
                "status": "error"
            }

    def get_table_schema(self, table_name):
        """Get the schema for a table.

        Args:
            table_name: Name of the table

        Returns:
            list: List of column dictionaries
        """
        # Use catalog_manager instead of non-existent table_info
        return self.catalog_manager.get_table_schema(table_name)
