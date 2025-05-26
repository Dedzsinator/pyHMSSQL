"""DML operations executor module.

This module implements the Data Manipulation Language (DML) operations,
including INSERT, UPDATE, and DELETE, with concurrency control.
"""
import logging
import os
from bptree import BPlusTree
import re
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
        # Configuration flag for strict heuristic FK validation
        self.strict_heuristic_fk_validation = False  # Changed from True to False

    def execute_insert(self, plan):
        """Execute INSERT operations with batch optimization."""
        try:
            # Validate plan structure first
            if not isinstance(plan, dict):
                logging.error(f"DMLExecutor: Plan is not a dict: {type(plan)} - {plan}")
                return {"error": "Plan must be a dictionary", "status": "error"}

            table_name = plan.get("table")
            specified_columns = plan.get("columns")
            values_list = plan.get("values", [])

            if not table_name:
                return {"error": "No table specified for INSERT", "status": "error"}

            if not values_list:
                return {"error": "No values provided for INSERT", "status": "error"}

            # Get current database
            db_name = self.catalog_manager.get_current_database()
            if not db_name:
                return {"error": "No database selected", "status": "error"}

            # Get table schema to determine columns if not specified
            table_id = f"{db_name}.{table_name}"
            if table_id not in self.catalog_manager.tables:
                return {"error": f"Table '{table_name}' does not exist", "status": "error"}

            table_schema = self.catalog_manager.tables[table_id]
            table_columns_dict = table_schema.get("columns", {})
            
            if not table_columns_dict:
                return {"error": f"Table '{table_name}' has no columns defined", "status": "error"}
            
            # Get ordered list of NON-IDENTITY columns from schema
            all_table_columns = []
            identity_columns = []
            
            for col_name, col_info in table_columns_dict.items():
                if not isinstance(col_info, dict):
                    logging.warning(f"Column info for {col_name} is not a dict: {type(col_info)} - {col_info}")
                    all_table_columns.append(col_name)
                    continue
                    
                if col_info.get("identity"):
                    identity_columns.append(col_name)
                else:
                    all_table_columns.append(col_name)
            
            # If no columns specified, use non-identity columns in order
            if specified_columns is None:
                columns = all_table_columns
                logging.info(f"No columns specified, using non-identity columns: {columns}")
            else:
                columns = specified_columns

            # Validate that we have the right number of values
            if values_list and len(values_list[0]) != len(columns):
                return {
                    "error": f"Number of values ({len(values_list[0])}) does not match number of columns ({len(columns)}). Available non-identity columns: {all_table_columns}",
                    "status": "error"
                }

            # CRITICAL OPTIMIZATION: Use batch insert for multiple records
            if len(values_list) > 1:
                logging.info(f"Using batch insert for {len(values_list)} records")
                
                # Convert values to record dictionaries
                records = []
                for row_values in values_list:
                    record = {}
                    for i, column in enumerate(columns):
                        if i < len(row_values):
                            record[column] = row_values[i]
                    records.append(record)
                
                # Use batch insert method
                result = self.catalog_manager.insert_records_batch(table_name, records)
                
                if result.get("status") == "success":
                    inserted_count = result.get("inserted_count", 0)
                    return {
                        "message": f"Inserted {inserted_count} record(s) into {table_name}",
                        "status": "success",
                        "count": inserted_count
                    }
                else:
                    return result
            else:
                # Single record - use existing logic
                row_values = values_list[0]
                record = {}
                for i, column in enumerate(columns):
                    if i < len(row_values):
                        record[column] = row_values[i]

                # Insert the record
                result = self.catalog_manager.insert_record(table_name, record)
                
                if isinstance(result, dict) and result.get("status") == "success":
                    return {
                        "message": f"Inserted 1 record into {table_name}",
                        "status": "success",
                        "count": 1
                    }
                else:
                    return result if isinstance(result, dict) else {"error": str(result), "status": "error"}

        except Exception as e:
            logging.error(f"Error in execute_insert: {str(e)}")
            logging.error(traceback.format_exc())
            return {"error": f"Error in execute_insert: {str(e)}", "status": "error"}

    def _parse_conditions(self, condition_str):
        """Parse condition string into a list of conditions."""
        if not condition_str:
            return []
        
        # This is a simplified parser - you might want to use a more robust one
        conditions = []
        
        # Basic parsing for simple conditions like "column = value"
        if "=" in condition_str:
            parts = condition_str.split("=")
            if len(parts) == 2:
                column = parts[0].strip()
                value = parts[1].strip()
                # Remove quotes if present
                if value.startswith(("'", '"')) and value.endswith(("'", '"')):
                    value = value[1:-1]
                conditions.append({
                    "column": column,
                    "operator": "=",
                    "value": value
                })
        
        return conditions

    def execute_delete(self, plan):
        """Execute a DELETE statement."""
        table_name = plan.get("table")
        condition = plan.get("condition")

        # Get database name
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected", "status": "error"}

        # Parse the condition
        parsed_conditions = self._parse_conditions(condition) if condition else []

        # First query the records that would be deleted to check constraints
        records_to_delete = self.catalog_manager.query_with_condition(
            table_name, parsed_conditions, ["*"]
        )

        # Check if deleting would violate foreign key constraints
        fk_violation = self._check_fk_constraints_for_delete(db_name, table_name, records_to_delete)
        if fk_violation:
            return {"error": fk_violation, "status": "error"}

        # Execute delete
        result = self.catalog_manager.delete_records(table_name, parsed_conditions)

        # Important: Check if result is an error dictionary
        if isinstance(result, dict) and "error" in result and "status" in result:
            logging.error(f"DELETE failed: {result['error']}")
            return result  # Return error dictionary to the client

        # Success case - return formatted result
        return {
            "message": result,  # This will be like "1 records deleted."
            "status": "success",
            "type": "delete_result",
            "rowCount": int(result.split()[0]) if isinstance(result, str) and result[0].isdigit() else 0
        }

    def _check_fk_constraints_for_delete(self, db_name, table_name, records_to_delete):
        """
        Check if deleting records would violate any foreign key constraints.
        """
        if not records_to_delete:
            return None  # No records to delete, no constraint violations

        logging.info(f"Checking FK constraints for deleting from {table_name}")

        # Get all tables in the database
        tables = self.catalog_manager.list_tables(db_name)

        # Direct check for each table that could reference this one
        for other_table in tables:
            if other_table == table_name:
                continue  # Skip the table we're deleting from

            # First, retrieve actual data structure of the tables to inspect
            other_schema = self.catalog_manager.get_table_schema(other_table)
            logging.info(f"Checking constraints in {other_table} schema: {other_schema}")

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
                logging.info(f"Processing potential FK constraint: {constraint_str}")

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
                    logging.info(f"Found FK reference: {other_table}.{fk_column} -> {table_name}.{ref_column}")

                    # For each record we're trying to delete
                    for record in records_to_delete:
                        # Get the value of the referenced column
                        if ref_column in record:
                            ref_value = record[ref_column]
                            logging.info(f"Checking references to value: {ref_value}")

                            # Check for references with type handling
                            conditions = [{"column": fk_column, "operator": "=", "value": ref_value}]

                            # Convert string values for comparison if needed
                            if isinstance(ref_value, (int, float)) and not isinstance(ref_value, bool):
                                str_value = str(ref_value)
                                conditions.append({"column": fk_column, "operator": "=", "value": str_value})

                            # Check if any records reference this value
                            referencing_records = self.catalog_manager.query_with_condition(
                                other_table, conditions, ["*"]
                            )

                            if referencing_records:
                                logging.info(f"Found {len(referencing_records)} referencing records: {referencing_records}")
                                error_msg = f"Cannot delete from {table_name} because records in {other_table} reference it via foreign key {fk_column}->{ref_column}"
                                return error_msg

        return None  # No constraints violated

    def execute_update(self, plan):
        """Execute an UPDATE operation."""
        table_name = plan.get("table")
        updates_data = plan.get("updates", {}) or plan.get("set", {})
        condition = plan.get("condition")

        # Check for valid database
        db_name = self.catalog_manager.get_current_database()
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
        records_to_update = self.catalog_manager.query_with_condition(
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
        table_schema = self.catalog_manager.get_table_schema(table_name)
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
                result = self.catalog_manager.update_record(
                    table_name,
                    record_id,
                    updates
                )

                if result:
                    updated_count += 1
                    logging.info(f"Updated record with ID {record_id} in table {table_name}")
            else:
                logging.warning(f"Could not determine record ID for update in table {table_name}")

        return {
            "status": "success",
            "message": f"Updated {updated_count} records",
            "type": "update_result",
            "rowCount": updated_count
        }

    def _check_fk_constraints_for_update(self, db_name, table_name, pk_column, records_to_update):
        """
        Check if updating primary key would violate foreign key constraints.

        Args:
            db_name: The current database name
            table_name: The table being updated
            pk_column: The primary key column being updated
            records_to_update: The records that would be updated

        Returns:
            None if no constraints are violated, error message otherwise
        """
        if not records_to_update:
            return None  # No records to update, no constraint violations

        # Get all tables in the database
        tables = self.catalog_manager.list_tables(db_name)

        # For each table, check if it has foreign keys referencing our table
        for other_table in tables:
            if other_table == table_name:
                continue  # Skip our own table

            table_schema = self.catalog_manager.get_table_schema(other_table)
            constraints = []

            # Collect constraints
            for column in table_schema:
                if isinstance(column, dict) and column.get("constraints"):
                    constraints.extend(column.get("constraints", []))
                elif isinstance(column, str) and "FOREIGN KEY" in column.upper():
                    constraints.append(column)

            if isinstance(table_schema, dict) and "constraints" in table_schema:
                constraints.extend(table_schema.get("constraints", []))

            # Check each constraint
            for constraint in constraints:
                if isinstance(constraint, str) and "FOREIGN KEY" in constraint.upper():
                    fk_match = re.search(r"FOREIGN\s+KEY\s*\(\s*(\w+)\s*\)\s*REFERENCES\s+(\w+)\s*\(\s*(\w+)\s*\)",
                                        constraint, re.IGNORECASE)

                    if fk_match:
                        fk_column = fk_match.group(1)
                        ref_table = fk_match.group(2)
                        ref_column = fk_match.group(3)

                        # Check if this constraint references the column we're updating
                        if ref_table.lower() == table_name.lower() and ref_column.lower() == pk_column.lower():
                            # Check if any of our to-be-updated records are referenced
                            for record in records_to_update:
                                record_value = record.get(pk_column)

                                # Check if any records in other_table reference this value
                                referencing_records = self.catalog_manager.query_with_condition(
                                    other_table,
                                    [{
                                        "column": fk_column,
                                        "operator": "=",
                                        "value": record_value
                                    }],
                                    ["*"]
                                )

                                if referencing_records:
                                    return f"Cannot update primary key in {table_name} because records in {other_table} reference it (foreign key constraint violation)"

        return None  # No constraints violated

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
