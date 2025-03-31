import logging
import traceback
import re
from bptree import BPlusTree
import os
from parsers.condition_parser import ConditionParser

class DMLExecutor:
    """Class to execute DML (Data Manipulation Language) operations."""
    
    def __init__(self, catalog_manager, index_manager):
        """
        Initialize DMLExecutor.
        
        Args:
            catalog_manager: The catalog manager instance
            index_manager: The index manager instance
        """
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager
        self.condition_parser = None  # Will be set by ExecutionEngine
        
    def execute_insert(self, plan):
        """Execute INSERT operation."""
        table_name = plan.get("table")
        columns = plan.get("columns", [])
        values_list = plan.get("values", [])

        # Validate that we have a table name
        if not table_name:
            return {
                "error": "No table name specified for INSERT",
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

        # Validate table exists
        tables = self.catalog_manager.list_tables(db_name)
        if table_name not in tables:
            return {
                "error": f"Table '{table_name}' does not exist in database '{db_name}'",
                "status": "error",
                "type": "error",
            }

        # Format values into records
        records = []
        for values in values_list:
            if len(columns) != len(values):
                return {
                    "error": f"Column count ({len(columns)}) does not match value count ({len(values)})",
                    "status": "error",
                    "type": "error",
                }

            record = {}
            for i, col in enumerate(columns):
                record[col] = values[i]

            # Insert the record
            try:
                result = self.catalog_manager.insert_record(table_name, record)
                if result is not True:
                    return {"error": str(result), "status": "error", "type": "error"}
                records.append(record)
            except Exception as e:
                return {
                    "error": f"Error inserting record: {str(e)}",
                    "status": "error",
                    "type": "error",
                }

        return {
            "message": f"Inserted {len(records)} record(s) into '{db_name}.{table_name}'",
            "status": "success",
            "type": "insert_result",
            "rows": records,
        }

    def execute_delete(self, plan):
        """Execute a DELETE query."""
        table_name = plan["table"]
        condition = plan.get("condition") or plan.get(
            "where")  # Try both fields

        # Get current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {
                "error": "No database selected. Use 'USE database_name' first.",
                "status": "error",
                "type": "error",
            }

        # Verify table exists
        tables = self.catalog_manager.list_tables(db_name)

        # Case-insensitive table lookup
        actual_table_name = table_name  # Default
        if table_name.lower() in [t.lower() for t in tables]:
            for t in tables:
                if t.lower() == table_name.lower():
                    actual_table_name = t
                    break
        elif table_name not in tables:
            return {
                "error": f"Table '{table_name}' does not exist in database '{db_name}'.",
                "status": "error",
                "type": "error",
            }

        try:
            # Parse condition into our format
            conditions = []
            if condition:
                logging.debug(f"Parsing DELETE condition: {condition}")
                conditions = ConditionParser.parse_condition_to_list(condition)
                logging.debug(f"Parsed DELETE conditions: {conditions}")

            # Delete records
            result = self.catalog_manager.delete_records(
                actual_table_name, conditions)

            # Extract count from result message
            count = 0
            if isinstance(result, str):
                try:
                    count = int(result.split()[0])
                except:
                    pass

            return {
                "message": f"Deleted {count} records from {actual_table_name}.",
                "status": "success",
                "type": "delete_result",
                "count": count,
            }
        except Exception as e:
            logging.error(f"Error in DELETE operation: {str(e)}")
            logging.error(traceback.format_exc())
            return {
                "error": f"Error in DELETE operation: {str(e)}",
                "status": "error",
                "type": "error",
            }

    def execute_update(self, plan):
        """Execute an UPDATE query."""
        table_name = plan["table"]
        condition = plan.get("condition") or plan.get(
            "where")  # Try both fields
        updates = plan.get("set", {})

        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {
                "error": "No database selected. Use 'USE database_name' first.",
                "status": "error",
            }

        # Verify table exists
        tables = self.catalog_manager.list_tables(db_name)
        if table_name not in tables:
            return {
                "error": f"Table '{table_name}' does not exist in database '{db_name}'.",
                "status": "error",
            }

        try:
            # Parse the condition
            conditions = []
            if condition:
                parsed_condition = ConditionParser.parse_condition_to_dict(condition)
                if parsed_condition:
                    conditions.append(parsed_condition)

            # Process updates
            update_dict = {}
            for key, val in updates.items():
                # Handle quoted strings
                if isinstance(val, str) and val.startswith("'") and val.endswith("'"):
                    val = val[1:-1]  # Remove quotes
                # Handle numeric conversions
                elif isinstance(val, str) and val.isdigit():
                    val = int(val)
                elif isinstance(val, str) and re.match(r"^[0-9]*\.[0-9]+$", val):
                    val = float(val)

                update_dict[key] = val

            if not update_dict:
                return {"error": "No fields to update specified", "status": "error"}

            # Update records
            result = self.catalog_manager.update_records(
                table_name, update_dict, conditions
            )

            # Extract count from result message
            count = 0
            if isinstance(result, str):
                try:
                    count = int(result.split()[0])
                except:
                    pass

            return {
                "message": f"Updated {count} records in {table_name}.",
                "status": "success",
            }
        except Exception as e:
            logging.error(f"Error in UPDATE operation: {str(e)}")
            logging.error(traceback.format_exc())
            return {"error": f"Error in UPDATE operation: {str(e)}", "status": "error"}

    def _update_indexes_after_modify(self, db_name, table_name, current_records):
        """Update all indexes for a table after records are modified."""
        table_id = f"{db_name}.{table_name}"

        # Find all indexes for this table
        table_indexes = []
        # Use index_manager instead of directly accessing self.indexes
        for index_id, index_def in self.catalog_manager.get_indexes_for_table(table_name).items():
            table_indexes.append((index_id, index_def))

        # Rebuild each index
        for index_id, index_def in table_indexes:
            column_name = index_def.get("column")
            index_file = os.path.join(
                self.catalog_manager.indexes_dir, f"{db_name}_{table_name}_{column_name}.idx"
            )

            # Create a new index tree
            index_tree = BPlusTree(order=50, name=f"{table_name}_{column_name}_index")

            # Populate the index with current records
            for record_key, record in current_records:
                if column_name in record:
                    index_tree.insert(record[column_name], record_key)

            # Save the updated index
            index_tree.save_to_file(index_file)
