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
        """Execute an INSERT operation."""
        table_name = plan.get("table")
        columns = plan.get("columns", [])
        values_list = plan.get("values", [])
        
        # CRITICAL FIX: Always use the original values from the current plan
        # rather than potentially cached values
        values_list_to_use = values_list
        
        # Debug log to ensure we're using the correct values
        logging.info(f"INSERT plan values: {values_list}")
        
        # If the values don't match the query string, this is likely a cached plan with stale values
        if "query" in plan:
            query = plan.get("query", "")
            # Basic check - if the plan's query mentions different values than what's in values_list
            if "VALUES" in query and str(values_list) not in query:
                logging.warning("Potential cached plan with stale values detected, extracting fresh values")
                # Use fresh values from the parsed query if available
                if "fresh_values" in plan:
                    values_list_to_use = plan.get("fresh_values")
                    logging.info(f"Using fresh values for INSERT: {values_list_to_use}")
        
        if not table_name:
            return {"error": "Table name not specified", "status": "error"}
            
        if not columns:
            return {"error": "No columns specified for INSERT", "status": "error"}
            
        if not values_list_to_use:
            return {"error": "No values specified for INSERT", "status": "error"}

        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected", "status": "error"}

        logging.info(f"--- Starting FK constraint checks for INSERT into {table_name} ---")

        # If no columns are specified, get them from the table schema
        if not columns and values_list_to_use:
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

        # Load the table schema to check for foreign key constraints
        table_schema = self.catalog_manager.get_table_schema(table_name)

        # Debug - log the entire schema
        logging.info(f"Table {table_name} schema: {table_schema}")

        success_count = 0
        for value_row in values_list_to_use:  # Use the properly selected values
            # Create the record mapping columns to values
            record = {}

            if columns:
                for i, value in enumerate(value_row):
                    if i < len(columns):
                        col_name = columns[i]
                        # Clean string values
                        if isinstance(value, str) and value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                        record[col_name] = value

            record["_already_mapped"] = True

            logging.info(f"Processing INSERT record: {record}")
            logging.info(f"--- Checking heuristic FK constraints ---")

            # Get foreign key constraints from table schema
            fk_constraints = []
            # This set will track columns that have explicit FK constraints
            explicitly_constrained_columns = set()

            # Process table-level constraints - Enhanced to handle more formats
            if isinstance(table_schema, dict):
                # Check direct constraints array
                if "constraints" in table_schema:
                    constraints = table_schema.get("constraints", [])
                    logging.info(f"Found {len(constraints)} table-level constraints")

                    for constraint in constraints:
                        if isinstance(constraint, str) and ("FOREIGN KEY" in constraint.upper() or "REFERENCES" in constraint.upper()):
                            logging.info(f"Found table-level FK constraint: {constraint}")
                            fk_constraints.append(constraint)

                # Also check if constraints exist at table root level (not in a 'constraints' array)
                for key, value in table_schema.items():
                    if key == "constraints":
                        continue  # Already processed above

                    if isinstance(key, str) and key.startswith("FOREIGN KEY"):
                        logging.info(f"Found root-level FK constraint key: {key}")
                        fk_constraints.append(key)

                    if isinstance(value, str) and "REFERENCES" in value.upper():
                        logging.info(f"Found root-level FK constraint value: {value}")
                        fk_constraints.append(value)

            # Process column-level constraints
            columns_to_check = table_schema
            if isinstance(table_schema, dict) and "columns" in table_schema:
                columns_to_check = table_schema["columns"]
                logging.info(f"Checking column-level constraints in {len(columns_to_check)} columns")

            for col in columns_to_check:
                if isinstance(col, dict) and "constraints" in col:
                    col_name = col.get("name", "unknown")
                    col_constraints = col["constraints"]
                    logging.info(f"Column {col_name} has {len(col_constraints)} constraints")
                    for constraint in col_constraints:
                        if isinstance(constraint, str) and "REFERENCES" in constraint.upper():
                            logging.info(f"Found column-level FK constraint on {col_name}: {constraint}")
                            fk_constraints.append(constraint)
                elif isinstance(col, str) and "REFERENCES" in col.upper():
                    logging.info(f"Found column-level FK constraint in string format: {col}")
                    fk_constraints.append(col)

            # Check table creation constraints that might not be in schema yet
            # This helps catch constraints specified in CREATE TABLE but not yet stored
            table_id = f"{db_name}.{table_name}"
            if hasattr(self.catalog_manager, 'pending_fk_constraints') and table_id in self.catalog_manager.pending_fk_constraints:
                pending_constraints = self.catalog_manager.pending_fk_constraints[table_id]
                logging.info(f"Found {len(pending_constraints)} pending FK constraints for {table_id}")
                fk_constraints.extend(pending_constraints)

            logging.info(f"Total FK constraints found: {len(fk_constraints)}")

            # First parse all constraints to find explicitly defined foreign keys
            for constraint in fk_constraints:
                logging.info(f"Pre-processing FK constraint: {constraint}")

                # Pattern 1: FOREIGN KEY (col) REFERENCES table(col)
                pattern1 = r"FOREIGN\s+KEY\s*\(\s*(\w+)\s*\)\s*REFERENCES\s+(?:(\w+)\.)?(\w+)\s*\(\s*(\w+)\s*\)"
                match = re.search(pattern1, constraint, re.IGNORECASE)
                if match:
                    fk_column = match.group(1)
                    explicitly_constrained_columns.add(fk_column)
                    logging.info(f"Found explicit FK constraint for column: {fk_column}")
                    continue

                # Pattern 2: col_name REFERENCES table(col)
                pattern2 = r"(\w+)\s+.*REFERENCES\s+(?:(\w+)\.)?(\w+)\s*\(\s*(\w+)\s*\)"
                match = re.search(pattern2, constraint, re.IGNORECASE)
                if match:
                    fk_column = match.group(1)
                    explicitly_constrained_columns.add(fk_column)
                    logging.info(f"Found explicit FK constraint for column: {fk_column}")

            # When we do heuristic checks, skip columns that have explicit constraints
            fk_heuristic_applied = False
            table_schema = self.catalog_manager.get_table_schema(table_name)
            for col_name, value in record.items():
                if col_name.endswith('_id') and value is not None:
                    # Try to determine referenced table
                    ref_table_base = col_name[:-3]  # Remove '_id' suffix

                    # Try common naming patterns in this order
                    possible_ref_tables = [
                        f"{ref_table_base}s",      # dept_id -> departments
                        ref_table_base,            # department_id -> department
                        f"{ref_table_base}es",     # box_id -> boxes
                        f"{ref_table_base}ies" if ref_table_base.endswith('y') else None,
                        f"{ref_table_base}ment",
                        f"{ref_table_base}ments"
                    ]

                    # Filter out None values
                    possible_ref_tables = [t for t in possible_ref_tables if t]

                    # Log which tables we're checking
                    logging.info(f"Checking possible reference tables for {col_name}: {possible_ref_tables}")

                    referenced_table = None  # Initialize the variable
                    for ref_table_name in possible_ref_tables:
                        if ref_table_name in self.catalog_manager.list_tables(db_name):
                            logging.info(f"Found candidate reference table {ref_table_name} for {col_name}")

                            # Check if the value exists in reference table
                            ref_records = self.catalog_manager.query_with_condition(
                                ref_table_name,
                                [{"column": "id", "operator": "=", "value": value}],
                                ["id"]
                            )

                            if not ref_records:
                                error_msg = f"Foreign key constraint violation: Value {value} in {table_name}.{col_name} does not exist in {ref_table_name}.id"
                                logging.error(error_msg)
                                return {"error": error_msg, "status": "error"}
                            else:
                                logging.info(f"FK constraint satisfied: {col_name}={value} exists in {ref_table_name}")
                                referenced_table = ref_table_name  # Set the referenced table name
                                break

                    # If we found a referenced table, check if the value exists in it
                    if referenced_table:
                        logging.info(f"Checking heuristic FK: {table_name}.{col_name} -> {referenced_table}.id = {value}")
                        referenced_records = self.catalog_manager.query_with_condition(
                            referenced_table,
                            [{"column": "id", "operator": "=", "value": value}],
                            ["id"]
                        )

                        logging.info(f"Referenced records found: {referenced_records}")

                        if not referenced_records:
                            error_msg = f"Foreign key constraint violation: Value {value} in {table_name}.{col_name} does not exist in {referenced_table}.id"
                            logging.error(error_msg)
                            return {"error": error_msg, "status": "error"}
                        else:
                            logging.info(f"Heuristic FK check passed: Found matching record in {referenced_table}")
                    else:
                        # No matching referenced table found for this potential FK
                        warning_msg = f"No matching referenced table found for {col_name}, skipping heuristic check"
                        logging.info(warning_msg)

                        # If strict validation is enabled, block the insert
                        if self.strict_heuristic_fk_validation:
                            error_msg = f"Strict FK validation failed: Column {col_name} appears to be a foreign key, but no referenced table ('{col_name[:-3]}' or '{col_name[:-3]}s') was found"
                            logging.error(error_msg)
                            return {"error": error_msg, "status": "error"}

            table_id = f"{db_name}.{table_name}"
            if table_id in self.catalog_manager.tables:
                table_info = self.catalog_manager.tables[table_id]
                constraints = table_info.get("constraints", [])

                for constraint in constraints:
                    if isinstance(constraint, str) and "FOREIGN KEY" in constraint.upper():
                        pattern = r"FOREIGN\s+KEY\s*\(\s*(\w+)\s*\)\s*REFERENCES\s+(\w+)\s*\(\s*(\w+)\s*\)"
                        match = re.search(pattern, constraint, re.IGNORECASE)

                        if match:
                            fk_column = match.group(1)
                            ref_table = match.group(2)
                            ref_column = match.group(3)

                            if fk_column in record and record[fk_column] is not None:
                                fk_value = record[fk_column]

                                # Check if referenced value exists
                                ref_records = self.catalog_manager.query_with_condition(
                                    ref_table,
                                    [{"column": ref_column, "operator": "=", "value": fk_value}],
                                    [ref_column]
                                )

                                if not ref_records:
                                    error_msg = f"Foreign key constraint violation: Value {fk_value} in {table_name}.{fk_column} does not exist in {ref_table}.{ref_column}"
                                    logging.error(error_msg)
                                    return {"error": error_msg, "status": "error"}
                                else:
                                    logging.info(f"FK constraint satisfied: {fk_column}={fk_value} exists in {ref_table}.{ref_column}")

            if not fk_heuristic_applied:
                logging.info("No heuristic FK constraints were applied (no *_id columns found)")

            logging.info(f"--- Checking schema-defined FK constraints ---")

            # Check each foreign key constraint
            for constraint in fk_constraints:
                logging.info(f"Processing FK constraint for insert: {constraint}")

                # Try different patterns to extract FK info
                fk_match = None
                fk_column = None
                referenced_table = None
                referenced_column = None

                # Pattern 1: FOREIGN KEY (col) REFERENCES table(col)
                pattern1 = r"FOREIGN\s+KEY\s*\(\s*(\w+)\s*\)\s*REFERENCES\s+(?:(\w+)\.)?(\w+)\s*\(\s*(\w+)\s*\)"
                match = re.search(pattern1, constraint, re.IGNORECASE)
                if match:
                    fk_column = match.group(1)
                    ref_db = match.group(2)  # This might be None if no db specified
                    referenced_table = match.group(3)
                    referenced_column = match.group(4)
                    fk_match = True
                    logging.info(f"Pattern 1 matched: {fk_column} -> {ref_db + '.' if ref_db else ''}{referenced_table}.{referenced_column}")

                # Pattern 2: col_name REFERENCES table(col)
                if not fk_match:
                    col_parts = constraint.split()
                    if len(col_parts) > 1 and "REFERENCES" in constraint.upper():
                        fk_column = col_parts[0]
                        pattern2 = r"REFERENCES\s+(?:(\w+)\.)?(\w+)\s*\(\s*(\w+)\s*\)"
                        match = re.search(pattern2, constraint, re.IGNORECASE)
                        if match:
                            ref_db = match.group(1)  # This might be None if no db specified
                            referenced_table = match.group(2)
                            referenced_column = match.group(3)
                            fk_match = True
                            logging.info(f"Pattern 2 matched: {fk_column} -> {ref_db + '.' if ref_db else ''}{referenced_table}.{referenced_column}")

                # Check if we have all the necessary information and the FK column is in our record
                if fk_match and fk_column:
                    logging.info(f"Checking if FK column {fk_column} exists in record...")
                    if fk_column in record:
                        fk_value = record[fk_column]
                        logging.info(f"FK column {fk_column} found in record with value: {fk_value}")

                        # Skip if the FK value is NULL (assuming NULLs are allowed)
                        if fk_value is None:
                            logging.info(f"Skipping FK check for NULL value in {fk_column}")
                            continue

                        # Check if the referenced value exists
                        logging.info(f"Checking if {fk_value} exists in {referenced_table}.{referenced_column}")
                        referenced_records = self.catalog_manager.query_with_condition(
                            referenced_table,
                            [{"column": referenced_column, "operator": "=", "value": fk_value}],
                            [referenced_column]
                        )

                        logging.info(f"Referenced records found: {referenced_records}")

                        if not referenced_records:
                            error_msg = f"Foreign key constraint violation: Value {fk_value} in {table_name}.{fk_column} does not exist in {referenced_table}.{referenced_column}"
                            logging.error(error_msg)
                            return {"error": error_msg, "status": "error"}
                        else:
                            logging.info(f"FK constraint check passed: Found matching record in {referenced_table}")
                    else:
                        logging.info(f"FK column {fk_column} not found in record, skipping this constraint")
                else:
                    logging.warning(f"Could not extract FK information from constraint: {constraint}")

            logging.info(f"--- All FK constraint checks passed for record: {record} ---")

            # Always pass the mapped record
            result = self.catalog_manager.insert_record(table_name, record)

            if isinstance(result, dict) and "status" in result and result["status"] == "error":
                # Return the error to the client
                logging.error(f"Insert failed: {result.get('error')}")
                return result

            if result is True:
                success_count += 1
            else:
                return {"error": result, "status": "error"}

        logging.info(f"--- FK constraint checking completed, {success_count} records inserted successfully ---")

        return {
            "status": "success",
            "message": f"Inserted {success_count} record(s)",
            "count": success_count
        }

    def _parse_conditions(self, condition_str):
        """Parse condition string into a format that catalog_manager understands."""
        if not condition_str:
            return []

        # Try using the condition parser if it was set
        if self.condition_parser:
            return self.condition_parser.parse_condition_to_list(condition_str)

        # Fall back to simple condition parsing
        return parse_simple_condition(condition_str)

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
