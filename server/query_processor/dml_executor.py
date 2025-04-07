"""DML operations executor module.

This module implements the Data Manipulation Language (DML) operations,
including INSERT, UPDATE, and DELETE, with concurrency control.
"""
import logging
import traceback
import os
import uuid
from bptree import BPlusTree
from parsers.condition_parser import ConditionParser
from transaction.lock_manager import LockManager, LockType
from utils.sql_helpers import parse_simple_condition, check_database_selected


class DMLExecutor:
    """Class to execute DML (Data Manipulation Language) operations."""

    def __init__(self, catalog_manager, index_manager):
        """Initialize DMLExecutor.

        Args:
            catalog_manager: The catalog manager instance
            index_manager: The index manager instance
        """
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager
        self.condition_parser = None  # Will be set by ExecutionEngine
        self.lock_manager = LockManager()  # Add lock manager for concurrency control

    def execute_insert(self, plan):
        #! REMOVED CONCURRENCY FOR NOW
        """Execute an INSERT operation."""
        table_name = plan.get("table")
        columns = plan.get("columns", [])
        values = plan.get("values", [])

        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected", "status": "error"}

        # If no columns are specified, get them from the table schema
        if not columns and values:
            table_schema = self.catalog_manager.get_table_schema(table_name)
            if table_schema:
                columns = []
                for col in table_schema:
                    if isinstance(col, dict) and "name" in col:
                        columns.append(col["name"])
                    elif isinstance(col, str):
                        # Extract column name from string definition
                        col_name = col.split()[0] if " " in col else col
                        columns.append(col_name)
                logging.info(f"Using schema columns for INSERT: {columns}")

        success_count = 0
        for value_row in values:
            # Always create a properly mapped record here
            record = {}

            # Map values to columns whether columns were explicitly specified or derived from schema
            if columns:
                for i, value in enumerate(value_row):
                    if i < len(columns):
                        col_name = columns[i]
                        # Clean string values
                        if isinstance(value, str):
                            if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
                                value = value[1:-1]  # Remove quotes
                        record[col_name] = value

            # Add flag to prevent double mapping
            record["_already_mapped"] = True

            # Always pass the mapped record
            result = self.catalog_manager.insert_record(table_name, record)

            if result is True:
                success_count += 1
            else:
                return {"error": result, "status": "error"}

        return {
            "status": "success",
            "message": f"Inserted {success_count} record(s)",
            "count": success_count
        }

    def execute_delete(self, plan):
        """Execute a DELETE query with concurrency control.

        Args:
            plan: The execution plan for the DELETE operation

        Returns:
            dict: Result of the operation
        """
        table_name = plan.get("table")
        condition = plan.get("condition") or plan.get("where")  # Try both fields
        session_id = plan.get("session_id")  # Get session ID for locking

        # Handle missing session ID - create a temporary one if needed
        if not session_id:
            # Use a UUID to ensure uniqueness
            session_id = f"temp_{uuid.uuid4()}"
            logging.debug("Created temporary session ID %s for delete operation", session_id)

        error = check_database_selected(self.catalog_manager)
        if error:
            return error

        # Get the database name after confirming it exists
        db_name = self.catalog_manager.get_current_database()

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

        # Acquire exclusive lock on table
        if not self.lock_manager.acquire_lock(session_id, actual_table_name, LockType.EXCLUSIVE):
            return {
                "error": "Could not acquire lock on table, operation timed out",
                "status": "error",
                "type": "error",
            }

        try:
            # Parse condition into our format
            conditions = []
            if condition:
                logging.debug("Parsing DELETE condition: %s", condition)
                conditions = ConditionParser.parse_condition_to_list(condition)
                logging.debug("Parsed DELETE conditions: %s", conditions)

            # Delete records
            result = self.catalog_manager.delete_records(
                actual_table_name, conditions)

            # Extract count from result message
            count = 0
            if isinstance(result, str):
                try:
                    count = int(result.split()[0])
                except (ValueError, IndexError):
                    pass

            # Update indexes if any records were actually deleted
            if count > 0:
                # Get all current records to rebuild indexes
                current_records = self.catalog_manager.get_all_records_with_keys(actual_table_name)
                if current_records:
                    self._update_indexes_after_modify(db_name, actual_table_name, current_records)

            # Release lock after successful operation
            self.lock_manager.release_lock(session_id, actual_table_name)

            return {
                "message": f"Deleted {count} records from {actual_table_name}.",
                "status": "success",
                "type": "delete_result",
                "count": count,
            }
        except RuntimeError as e:
            # Ensure lock is released on any exception
            self.lock_manager.release_lock(session_id, actual_table_name)
            logging.error("Error in DELETE operation: %s", str(e))
            logging.error(traceback.format_exc())
            return {
                "error": f"Error in DELETE operation: {str(e)}",
                "status": "error",
                "type": "error",
            }

    def execute_update(self, plan):
        """Execute an UPDATE operation."""
        table_name = plan.get("table")
        updates = plan.get("updates", []) or plan.get("set", [])
        condition = plan.get("condition")

        # Create a session ID if not provided
        session_id = plan.get("session_id")
        if not session_id:
            session_id = f"temp_{uuid.uuid4()}"

        # Acquire lock for writing
        self.lock_manager.acquire_lock(session_id, table_name, "write")

        try:
            # Convert condition string to condition structure
            conditions = []
            if condition:
                conditions = parse_simple_condition(condition)

            # Get records matching the condition
            records = self.catalog_manager.query_with_condition(table_name, conditions)

            if not records:
                return {
                    "status": "error",
                    "error": f"No records found matching condition: {condition}",
                    "count": 0
                }

            # Update each matching record
            updated_count = 0
            for record in records:
                # Apply updates
                update_data = {}
                for col, val in updates:
                    # Handle quoted string values
                    if isinstance(val, str) and val.startswith("'") and val.endswith("'"):
                        val = val[1:-1]
                    update_data[col] = val

                # Update the record
                primary_key = record.get("id")
                result = self.catalog_manager.update_record(
                    table_name,
                    primary_key,
                    update_data
                )

                if result:
                    updated_count += 1

            return {
                "status": "success",
                "message": f"Updated {updated_count} records",
                "count": updated_count
            }

        except RuntimeError as e:
            return {
                "status": "error",
                "error": str(e)
            }
        finally:
            # Release lock
            self.lock_manager.release_lock(session_id, table_name)

    def _update_indexes_after_modify(self, db_name, table_name, current_records):
        """Update all indexes for a table after records are modified.

        Args:
            db_name: Database name
            table_name: Table name
            current_records: List of (key, record) tuples representing the current table data
        """
        # Remove unused variable and just use the table_name directly

        # Find all indexes for this table
        table_indexes = []
        indexes = self.catalog_manager.get_indexes_for_table(table_name)
        if not indexes:
            # No indexes to update
            return

        for index_id, index_def in indexes.items():
            table_indexes.append((index_id, index_def))

        # Rebuild each index
        for index_id, index_def in table_indexes:
            column_name = index_def.get("column")
            if not column_name:
                logging.warning("No column specified for index %s, skipping rebuild", index_id)
                continue

            try:
                index_file = os.path.join(
                    self.catalog_manager.indexes_dir,
                    f"{db_name}_{table_name}_{column_name}.idx",
                )

                # Create a new index tree
                index_tree = BPlusTree(order=50, name=f"{table_name}_{column_name}_index")

                # Populate the index with current records
                for record_key, record in current_records:
                    if column_name in record and record[column_name] is not None:
                        index_tree.insert(record[column_name], record_key)

                # Save the updated index
                index_tree.save_to_file(index_file)
                logging.debug(
                    "Successfully rebuilt index %s for %s.%s",
                    index_id, table_name, column_name
                )
            except RuntimeError as e:
                logging.error("Error rebuilding index %index_id: %s",index_id, str(e))
