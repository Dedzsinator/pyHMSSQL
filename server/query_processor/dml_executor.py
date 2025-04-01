"""DML operations executor module.

This module implements the Data Manipulation Language (DML) operations,
including INSERT, UPDATE, and DELETE, with concurrency control.
"""
import logging
import traceback
import re
import os
import uuid
from bptree import BPlusTree
from parsers.condition_parser import ConditionParser
from transaction.lock_manager import LockManager, LockType


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
        """Execute INSERT operation with concurrency control.

        Args:
            plan: The execution plan for the INSERT operation

        Returns:
            dict: Result of the operation
        """
        table_name = plan.get("table")
        columns = plan.get("columns", [])
        values_list = plan.get("values", [])
        session_id = plan.get("session_id")  # Get session ID for locking

        # Handle missing session ID - create a temporary one if needed
        if not session_id:
            # Use a UUID to ensure uniqueness
            session_id = f"temp_{uuid.uuid4()}"
            logging.debug("Created temporary session ID %s for insert operation", session_id)

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

        # Acquire exclusive lock on table
        if not self.lock_manager.acquire_lock(session_id, table_name, LockType.EXCLUSIVE):
            return {
                "error": "Could not acquire lock on table, operation timed out",
                "status": "error",
                "type": "error",
            }

        try:
            # Format values into records
            records = []
            for values in values_list:
                if len(columns) != len(values):
                    # Release lock on error
                    self.lock_manager.release_lock(session_id, table_name)
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
                        # Release lock on error
                        self.lock_manager.release_lock(session_id, table_name)
                        return {"error": str(result), "status": "error", "type": "error"}
                    records.append(record)
                except RuntimeError as e:
                    # Release lock on error
                    self.lock_manager.release_lock(session_id, table_name)
                    logging.error("Error inserting record: %s", str(e))
                    return {
                        "error": f"Error inserting record: {str(e)}",
                        "status": "error",
                        "type": "error",
                    }

            # Update indexes if any records were actually inserted
            if records:
                # Get all current records to rebuild indexes
                current_records = self.catalog_manager.get_all_records_with_keys(table_name)
                if current_records:
                    self._update_indexes_after_modify(db_name, table_name, current_records)

            # Release lock after successful operation
            self.lock_manager.release_lock(session_id, table_name)

            return {
                "message": f"Inserted {len(records)} record(s) into '{db_name}.{table_name}'",
                "status": "success",
                "type": "insert_result",
                "rows": records,
            }
        except RuntimeError as e:
            # Ensure lock is released on any exception
            self.lock_manager.release_lock(session_id, table_name)
            logging.error("Error in INSERT operation: %s", str(e))
            logging.error(traceback.format_exc())
            return {
                "error": f"Error in INSERT operation: {str(e)}",
                "status": "error",
                "type": "error",
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
        """Execute an UPDATE query with concurrency control.

        Args:
            plan: The execution plan for the UPDATE operation

        Returns:
            dict: Result of the operation
        """
        table_name = plan.get("table")
        condition = plan.get("condition") or plan.get("where")  # Try both fields
        updates = plan.get("set", {})
        session_id = plan.get("session_id")  # Get session ID for locking

        # Handle missing session ID - create a temporary one if needed
        if not session_id:
            # Use a UUID to ensure uniqueness
            session_id = f"temp_{uuid.uuid4()}"
            logging.debug("Created temporary session ID %s for update operation", session_id)

        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {
                "error": "No database selected. Use 'USE database_name' first.",
                "status": "error",
                "type": "error",
            }

        # Verify table exists
        tables = self.catalog_manager.list_tables(db_name)
        if table_name not in tables:
            return {
                "error": f"Table '{table_name}' does not exist in database '{db_name}'.",
                "status": "error",
                "type": "error",
            }

        # Acquire exclusive lock on table
        if not self.lock_manager.acquire_lock(session_id, table_name, LockType.EXCLUSIVE):
            return {
                "error": "Could not acquire lock on table, operation timed out",
                "status": "error",
                "type": "error",
            }

        try:
            # Parse the condition
            conditions = []
            if condition:
                parsed_condition = self.condition_parser.parse_condition_to_dict(condition)
                if parsed_condition:
                    conditions.append(parsed_condition)

            # Process updates - handle format in the plan
            update_dict = {}
            # Check if updates is a list of tuples (comes from parser this way)
            if isinstance(updates, list):
                for key, val in updates:
                    update_dict[key] = val
            else:
                # Handle dictionary format
                update_dict = updates

            # Process values to convert strings to appropriate types
            for key, val in list(update_dict.items()):
                # Handle quoted strings
                if isinstance(val, str) and val.startswith("'") and val.endswith("'"):
                    update_dict[key] = val[1:-1]  # Remove quotes
                # Handle numeric conversions
                elif isinstance(val, str) and val.isdigit():
                    update_dict[key] = int(val)
                elif isinstance(val, str) and re.match(r"^[0-9]*\.[0-9]+$", val):
                    update_dict[key] = float(val)

            if not update_dict:
                # Release lock if nothing to update
                self.lock_manager.release_lock(session_id, table_name)
                return {
                    "error": "No fields to update specified",
                    "status": "error",
                    "type": "error"
                }

            # Update records
            result = self.catalog_manager.update_records(
                table_name, update_dict, conditions
            )

            # Extract count from result message
            count = 0
            if isinstance(result, str):
                try:
                    count = int(result.split()[0])
                except (ValueError, IndexError):
                    pass

            # Update indexes if any records were actually updated
            if count > 0:
                # Get all current records to rebuild indexes
                current_records = self.catalog_manager.get_all_records_with_keys(table_name)
                if current_records:
                    self._update_indexes_after_modify(db_name, table_name, current_records)

            # Release lock after successful operation
            self.lock_manager.release_lock(session_id, table_name)

            return {
                "message": f"Updated {count} records in {table_name}.",
                "status": "success",
                "type": "update_result",
                "count": count,
            }
        except RuntimeError as e:
            # Ensure lock is released on any exception
            self.lock_manager.release_lock(session_id, table_name)
            logging.error("Error in UPDATE operation: %s", str(e))
            logging.error(traceback.format_exc())
            return {
                "error": f"Error in UPDATE operation: {str(e)}",
                "status": "error",
                "type": "error",
            }

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
