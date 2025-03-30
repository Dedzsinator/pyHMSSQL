import logging
import json
import re
import traceback
import os
from bptree import BPlusTree
import datetime
import shutil


class ExecutionEngine:
    """_summary_
    """
    def __init__(self, catalog_manager, index_manager):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager
        self.current_database = catalog_manager.get_current_database()
        self.transaction_stack = []
        self.preferences = self.catalog_manager.get_preferences()

    def _parse_value(self, value_str):
        """
        Parse a value string into the appropriate Python type.

        Args:
            value_str: String representation of a value

        Returns:
            The value converted to the appropriate type
        """
        # Handle quoted strings
        if value_str.startswith('"') or value_str.startswith("'"):
            # Remove quotes
            return (
                value_str[1:-1]
                if value_str.endswith('"') or value_str.endswith("'")
                else value_str[1:]
            )
        # Handle NULL keyword
        elif value_str.upper() == "NULL":
            return None
        # Handle boolean literals
        elif value_str.upper() in ("TRUE", "FALSE"):
            return value_str.upper() == "TRUE"
        else:
            # Try to convert to number if possible
            try:
                if "." in value_str:
                    return float(value_str)
                else:
                    return int(value_str)
            except ValueError:
                return value_str

    def _parse_condition(self, condition_str):
        """
        Parse a condition string into a MongoDB query filter.

        Examples:
        - "age > 30" -> {"age": {"$gt": 30}}
        - "name = 'John'" -> {"name": "John"}
        """
        if not condition_str:
            return {}

        # Basic operators mapping
        operators = {
            "=": "$eq",
            ">": "$gt",
            "<": "$lt",
            ">=": "$gte",
            "<=": "$lte",
            "!=": "$ne",
            "<>": "$ne",
        }

        # Try to match an operator
        for op in sorted(
            operators.keys(), key=len, reverse=True
        ):  # Process longer ops first
            if op in condition_str:
                column, value = condition_str.split(op, 1)
                column = column.strip()
                value = value.strip()

                # Handle quoted strings
                if value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]  # Remove quotes
                # Handle numbers
                elif value.isdigit():
                    value = int(value)
                elif re.match(r"^[0-9]*\.[0-9]+$", value):
                    value = float(value)

                # MongoDB is case-sensitive for field names
                # For consistency, use lowercase field names everywhere
                column_lower = column.lower()

                # Build MongoDB query with correct operator
                if op == "=":
                    return {column_lower: value}
                else:
                    return {column_lower: {operators[op]: value}}

        # If no operator found, return empty filter with a warning
        logging.warning(f"Could not parse condition: {condition_str}")
        return {}

    def _execute_count(self, result, actual_column, column):
        """Execute COUNT aggregate function."""
        if column == "*":
            return len(result)
        else:
            if not actual_column:
                raise ValueError(f"Column '{column}' not found")
            return sum(1 for record in result if actual_column in record and record[actual_column] is not None)

    def _execute_sum(self, result, actual_column, column):
        """Execute SUM aggregate function."""
        if column == "*":
            raise ValueError("Cannot use SUM with *")
        if not actual_column:
            raise ValueError(f"Column '{column}' not found")

        total = 0
        count = 0
        for record in result:
            if actual_column in record and record[actual_column] is not None:
                try:
                    total += float(record[actual_column])
                    count += 1
                except (ValueError, TypeError):
                    pass  # Skip non-numeric values
        return total if count > 0 else None

    def _execute_avg(self, result, actual_column, column):
        """Execute AVG aggregate function."""
        if column == "*":
            raise ValueError("Cannot use AVG with *")
        if not actual_column:
            raise ValueError(f"Column '{column}' not found")

        total = 0
        count = 0
        for record in result:
            if actual_column in record and record[actual_column] is not None:
                try:
                    total += float(record[actual_column])
                    count += 1
                except (ValueError, TypeError):
                    pass  # Skip non-numeric values
        return (total / count) if count > 0 else None

    def _execute_min(self, result, actual_column, column):
        """Execute MIN aggregate function."""
        if column == "*":
            raise ValueError("Cannot use MIN with *")
        if not actual_column:
            raise ValueError(f"Column '{column}' not found")

        values = []
        for record in result:
            if actual_column in record and record[actual_column] is not None:
                values.append(record[actual_column])
        return min(values) if values else None

    def _execute_max(self, result, actual_column, column):
        """Execute MAX aggregate function."""
        if column == "*":
            raise ValueError("Cannot use MAX with *")
        if not actual_column:
            raise ValueError(f"Column '{column}' not found")

        values = []
        for record in result:
            if actual_column in record and record[actual_column] is not None:
                values.append(record[actual_column])
        return max(values) if values else None

    def _execute_rand(self, result, param_str):
        """Execute RAND aggregate function."""
        import random
        
        logging.info(f"Executing RAND with params: '{param_str}'")
        
        # Parse parameters
        params = [p.strip() for p in param_str.split(",")]
        
        try:
            if len(params) == 1:
                n = int(params[0])
                if n <= 0:
                    raise ValueError("Number of random records must be positive")
                
                if result and len(result) > 0:
                    n = min(n, len(result))
                    logging.info(f"Selecting {n} random records from {len(result)} total")
                    selected = random.sample(range(len(result)), n)
                    return n
                else:
                    logging.warning("No records to sample from")
                    return 0
                    
            elif len(params) == 3:
                n = int(params[0])
                min_val = float(params[1])
                max_val = float(params[2])
                
                logging.info(f"Generating {n} random values between {min_val} and {max_val}")
                if n <= 0:
                    raise ValueError("Number of random values must be positive")
                
                random_values = [random.uniform(min_val, max_val) for _ in range(n)]
                avg = sum(random_values) / len(random_values)
                logging.info(f"Generated values with average: {avg}")
                return avg
                
            else:
                raise ValueError("RAND requires either 1 parameter (n) or 3 parameters (n,min,max)")
                
        except ValueError as e:
            logging.error(f"Error in RAND: {str(e)}")
            raise ValueError(f"Invalid parameters for RAND: {str(e)}")

    def _execute_gcd(self, result, actual_column, column):
        """Execute GCD aggregate function."""
        import math
        
        logging.info(f"Computing GCD for column '{column}' (actual: '{actual_column}')")
        
        if column == "*":
            raise ValueError("Cannot use GCD with *")
        if not actual_column:
            raise ValueError(f"Column '{column}' not found")
        
        # Collect integer values
        values = []
        for record in result:
            if actual_column in record and record[actual_column] is not None:
                try:
                    val = int(float(record[actual_column]))
                    if val != 0:  # Skip zeros
                        values.append(abs(val))
                        logging.info(f"Added value {val} to GCD calculation")
                except (ValueError, TypeError):
                    logging.warning(f"Skipping non-numeric value: {record[actual_column]}")
        
        if not values:
            logging.warning("No valid values found for GCD calculation")
            return None
        
        gcd_result = values[0]
        for value in values[1:]:
            gcd_result = math.gcd(gcd_result, value)
        
        logging.info(f"GCD result: {gcd_result}")
        return gcd_result

    def execute_aggregate(self, plan):
        """Execute an aggregation function (COUNT, SUM, AVG, MIN, MAX, RAND, GCD)."""
        logging.info(f"===== AGGREGATE FUNCTION CALLED =====")
        logging.info(f"Plan: {plan}")
        logging.info(f"Function: {plan.get('function')}")
        logging.info(f"Column: {plan.get('column')}")
        logging.info(f"Table: {plan.get('table')}")
        logging.info(f"Condition: {plan.get('condition')}")
        logging.info(f"Top: {plan.get('top')}")
        logging.info(f"Limit: {plan.get('limit')}")

        function = plan.get("function", "").upper()
        column = plan.get("column")
        table_name = plan.get("table")
        condition = plan.get("condition")
        
        # Handle TOP and LIMIT parameters
        top_n = plan.get("top")
        limit = plan.get("limit")
        if top_n is not None:
            try:
                top_n = int(top_n)
                logging.info(f"TOP {top_n} specified for aggregate")
            except (ValueError, TypeError):
                top_n = None
                
        if limit is not None:
            try:
                limit = int(limit)
                logging.info(f"LIMIT {limit} specified for aggregate")
            except (ValueError, TypeError):
                limit = None

        # Get raw data using catalog manager
        try:
            # Parse and apply conditions
            conditions = self._parse_condition_to_list(condition) if condition else []
            result = self.catalog_manager.query_with_condition(
                table_name, conditions, ["*"]
            )
            logging.info(f"Queried {len(result) if result else 0} records from {table_name}")
            
            # Apply TOP/LIMIT to the raw data before aggregation if appropriate
            # For most aggregates, we need to aggregate all data first, then limit the results
            # But some functions like RAND may benefit from limiting data first
            if function == "RAND" and top_n is not None and top_n > 0 and result:
                result = result[:top_n]
                logging.info(f"Applied TOP {top_n} to raw data for RAND function")
            elif function == "RAND" and limit is not None and limit > 0 and result:
                result = result[:limit]
                logging.info(f"Applied LIMIT {limit} to raw data for RAND function")
            
        except Exception as e:
            logging.error(f"Error querying data: {str(e)}")
            return {
                "error": f"Error querying data: {str(e)}",
                "status": "error",
                "type": "error",
            }

        try:
            # Find the actual column name with original case
            actual_column = None
            if column != "*" and result and len(result) > 0:
                for col in result[0]:
                    if col.lower() == column.lower():
                        actual_column = col
                        break
                
                if not actual_column and function not in ["COUNT"]:
                    logging.warning(f"Column '{column}' not found in table {table_name}, available columns: {list(result[0].keys())}")
            
            logging.info(f"Using actual column name: '{actual_column}' for function {function}({column})")

            # Dispatch to appropriate function handler
            if function == "COUNT":
                aggregate_result = self._execute_count(result, actual_column, column)
            elif function == "SUM":
                aggregate_result = self._execute_sum(result, actual_column, column)
            elif function == "AVG":
                aggregate_result = self._execute_avg(result, actual_column, column)
            elif function == "MIN":
                aggregate_result = self._execute_min(result, actual_column, column)
            elif function == "MAX":
                aggregate_result = self._execute_max(result, actual_column, column)
            elif function == "RAND":
                aggregate_result = self._execute_rand(result, column)
            elif function == "GCD":
                aggregate_result = self._execute_gcd(result, actual_column, column)
            else:
                return {
                    "error": f"Unsupported aggregation function: {function}",
                    "status": "error",
                    "type": "error",
                }

            # Format result column name and value
            result_column = f"{function}({column})"
            if aggregate_result is None:
                display_value = "NULL"
            elif isinstance(aggregate_result, float):
                display_value = f"{aggregate_result:.2f}"
            else:
                display_value = str(aggregate_result)

            logging.info(f"Aggregate result: {function}({column}) = {display_value}")

            # Return in the standard result format
            return {
                "columns": [result_column],
                "rows": [[display_value]],
                "status": "success",
                "type": "select_result",
                "rowCount": 1
            }

        except Exception as e:
            logging.error(f"Error executing aggregate: {str(e)}")
            logging.error(traceback.format_exc())
            return {
                "error": f"Error executing aggregate {function}: {str(e)}",
                "status": "error",
                "type": "error",
            }

    def execute_join(self, plan):
        """Execute a JOIN query using the appropriate join algorithm."""
        join_type = plan.get("join_type", "INNER").upper()
        join_algorithm = plan.get("join_algorithm", "HASH").upper()
        table1 = plan.get("table1", "")
        table2 = plan.get("table2", "")
        condition = plan.get("condition")
        
        logging.info(f"Executing {join_type} JOIN using {join_algorithm} algorithm")
        logging.info(f"Table 1: {table1}")
        logging.info(f"Table 2: {table2}")
        logging.info(f"Condition: {condition}")
        
        # Handle ON clause in table2 if condition not provided separately
        if condition is None and isinstance(table2, str) and " ON " in table2.upper():
            parts = table2.split(" ON ", 1)
            table2 = parts[0].strip()
            condition = parts[1].strip()
        
        # Handle table aliases
        table1_parts = table1.split() if isinstance(table1, str) else []
        table1_name = table1_parts[0] if table1_parts else ""
        table1_alias = table1_parts[1] if len(table1_parts) > 1 else table1_name
        
        table2_parts = table2.split() if isinstance(table2, str) else []
        table2_name = table2_parts[0] if table2_parts else ""
        table2_alias = table2_parts[1] if len(table2_parts) > 1 else table2_name
        
        # Get current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {
                "error": "No database selected. Use 'USE database_name' first.",
                "status": "error",
                "type": "error",
            }
        
        # Verify tables exist
        tables = self.catalog_manager.list_tables(db_name)
        if table1_name not in tables:
            return {
                "error": f"Table '{table1_name}' does not exist in database '{db_name}'",
                "status": "error",
                "type": "error",
            }
        if table2_name not in tables:
            return {
                "error": f"Table '{table2_name}' does not exist in database '{db_name}'",
                "status": "error",
                "type": "error",
            }
        
        # Parse join condition (e.g., "e.dept_id = d.id")
        if not condition and join_type != "CROSS":
            return {
                "error": "No join condition specified",
                "status": "error",
                "type": "error",
            }
        
        logging.debug(f"Join condition: {condition}")
        
        try:
            # Get data from both tables
            table1_data = self.catalog_manager.query_with_condition(
                table1_name, [], ["*"]
            )
            table2_data = self.catalog_manager.query_with_condition(
                table2_name, [], ["*"]
            )
            
            # Parse the join condition to extract column names
            join_columns = self._parse_join_condition(condition) if condition else (None, None)
            left_column, right_column = join_columns
            
            # Choose and execute join algorithm
            if join_algorithm == "HASH":
                result = self._execute_hash_join(
                    table1_data,
                    table2_data,
                    left_column,
                    right_column,
                    table1_alias,
                    table2_alias,
                    join_type
                )
            elif join_algorithm == "MERGE":
                result = self._execute_merge_join(
                    table1_data,
                    table2_data,
                    left_column,
                    right_column,
                    table1_alias,
                    table2_alias,
                    join_type
                )
            elif join_algorithm == "INDEX":
                result = self._execute_index_join(
                    table1_name,
                    table2_name,
                    left_column,
                    right_column,
                    table1_alias,
                    table2_alias,
                    join_type
                )
            elif join_algorithm == "NESTED_LOOP":
                result = self._execute_nested_loop_join(
                    table1_data,
                    table2_data,
                    left_column,
                    right_column,
                    table1_alias,
                    table2_alias,
                    join_type
                )
            else:
                # Default to hash join
                result = self._execute_hash_join(
                    table1_data,
                    table2_data,
                    left_column,
                    right_column,
                    table1_alias,
                    table2_alias,
                    join_type
                )
            
            # Format columns for result
            columns = plan.get("columns", ["*"])
            if "*" in columns:
                # Get all columns from both tables with table aliases
                if result and len(result) > 0:
                    columns = list(result[0].keys())
            
            # Format rows for result
            result_rows = []
            for record in result:
                row = []
                for col in columns:
                    if col == "*":
                        # Include all fields in order
                        row.extend(record.values())
                    else:
                        # Include specific column
                        value = record.get(col, None)
                        row.append(value)
                result_rows.append(row)
            
            return {
                "columns": columns,
                "rows": result_rows,
                "status": "success",
                "type": "join_result",
                "rowCount": len(result_rows)
            }
            
        except Exception as e:
            logging.error(f"Error executing join: {str(e)}")
            logging.error(traceback.format_exc())
            return {
                "error": f"Error executing join: {str(e)}",
                "status": "error",
                "type": "error",
            }

    def _parse_join_condition(self, condition):
        """Parse a join condition like 'table1.col1 = table2.col2' into column names."""
        if not condition:
            return None, None
        
        # Try to match the pattern table.column = table.column
        match = re.search(r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", condition)
        if match:
            left_table, left_col = match.group(1), match.group(2)
            right_table, right_col = match.group(3), match.group(4)
            return left_col, right_col
        
        # Try simpler pattern column = column
        match = re.search(r"(\w+)\s*=\s*(\w+)", condition)
        if match:
            return match.group(1), match.group(2)
        
        return None, None

    def _execute_hash_join(
        self,
        left_table_data,
        right_table_data,
        left_column,
        right_column,
        left_alias,
        right_alias,
        join_type="INNER"
    ):
        """
        Execute a hash join with support for INNER, LEFT, RIGHT and FULL joins.
        
        Args:
            left_table_data: Records from the left table
            right_table_data: Records from the right table
            left_column: Join column in left table
            right_column: Join column in right table
            left_alias: Alias for the left table
            right_alias: Alias for the right table
            join_type: Type of join (INNER, LEFT, RIGHT, FULL)
            
        Returns:
            List of joined records
        """
        logging.info(f"Executing {join_type} hash join: {left_alias}.{left_column} = {right_alias}.{right_column}")
        logging.info(f"Table sizes: {left_alias}={len(left_table_data)} records, {right_alias}={len(right_table_data)} records")
        
        # For INNER and LEFT joins, build hash table on right table
        # For RIGHT joins, build hash table on left table
        # For efficiency, always build hash table on the smaller table
        if join_type == "RIGHT" or (join_type != "LEFT" and len(left_table_data) > len(right_table_data)):
            # Swap tables to make implementation simpler
            left_table_data, right_table_data = right_table_data, left_table_data
            left_column, right_column = right_column, left_column
            left_alias, right_alias = right_alias, left_alias
            # Also swap join type if needed
            if join_type == "LEFT":
                join_type = "RIGHT"
            elif join_type == "RIGHT":
                join_type = "LEFT"
        
        # Build hash table from the smaller table (now always left_table_data)
        hash_table = {}
        for record in left_table_data:
            # Find the join value case-insensitively
            join_value = None
            for col in record:
                if col.lower() == left_column.lower():
                    join_value = record[col]
                    break
            
            if join_value is not None:
                if join_value not in hash_table:
                    hash_table[join_value] = []
                hash_table[join_value].append(record)
        
        logging.debug(f"Hash table built with {len(hash_table)} unique keys")
        
        # Probe the hash table with records from the larger table
        result = []
        matched_left_records = set()  # Track which left records were matched (for LEFT/FULL joins)
        
        for right_record in right_table_data:
            # Find the join value case-insensitively
            join_value = None
            for col in right_record:
                if col.lower() == right_column.lower():
                    join_value = right_record[col]
                    break
            
            found_match = False
            if join_value is not None and join_value in hash_table:
                # Match found - join with all matching records
                for left_record in hash_table[join_value]:
                    # Create a joined record
                    joined_record = {}
                    
                    # Add fields from left table with alias prefix
                    for field, value in left_record.items():
                        joined_record[f"{left_alias}.{field}"] = value
                    
                    # Add fields from right table with alias prefix
                    for field, value in right_record.items():
                        joined_record[f"{right_alias}.{field}"] = value
                    
                    result.append(joined_record)
                    found_match = True
                    
                    # Track this left record as matched
                    record_id = tuple(sorted(left_record.items()))
                    matched_left_records.add(record_id)
            
            # For RIGHT or FULL join, include right records without matches
            if not found_match and (join_type == "RIGHT" or join_type == "FULL"):
                joined_record = {}
                
                # Add NULL values for left table
                for left_record in left_table_data[:1]:  # Use first record to get field names
                    for field in left_record:
                        joined_record[f"{left_alias}.{field}"] = None
                
                # Add fields from right table
                for field, value in right_record.items():
                    joined_record[f"{right_alias}.{field}"] = value
                
                result.append(joined_record)
        
        # For LEFT or FULL join, include left records that weren't matched
        if join_type == "LEFT" or join_type == "FULL":
            for left_record in left_table_data:
                record_id = tuple(sorted(left_record.items()))
                if record_id not in matched_left_records:
                    joined_record = {}
                    
                    # Add fields from left table
                    for field, value in left_record.items():
                        joined_record[f"{left_alias}.{field}"] = value
                    
                    # Add NULL values for right table
                    for right_record in right_table_data[:1]:  # Use first record to get field names
                        for field in right_record:
                            joined_record[f"{right_alias}.{field}"] = None
                    
                    result.append(joined_record)
        
        logging.info(f"Hash join produced {len(result)} results")
        return result

    def _execute_nested_loop_join(
        self,
        left_table_data,
        right_table_data,
        left_column,
        right_column,
        left_alias,
        right_alias,
        join_type="INNER"
    ):
        """
        Execute a nested loop join with support for all join types.
        
        This is the simplest join algorithm but can be inefficient for large tables.
        For each record in the outer table, it scans all records in the inner table.
        """
        logging.info(f"Executing {join_type} nested loop join")
        
        result = []
        
        # For cross joins, we don't need column matching
        is_cross_join = join_type == "CROSS" or (left_column is None and right_column is None)
        
        # Track matched records for LEFT/RIGHT/FULL joins
        matched_left = set()
        matched_right = set()
        
        # Outer loop - iterate through left table
        for left_record in left_table_data:
            left_matched = False
            
            # Get join value from left record
            left_value = None
            if not is_cross_join:
                for col in left_record:
                    if col.lower() == left_column.lower():
                        left_value = left_record[col]
                        break
            
            # Inner loop - iterate through right table
            for right_record in right_table_data:
                # For cross join, match all records
                if is_cross_join:
                    joined_record = self._create_joined_record(
                        left_record, right_record, left_alias, right_alias)
                    result.append(joined_record)
                    left_matched = True
                    matched_right.add(id(right_record))
                    continue
                
                # Get join value from right record
                right_value = None
                for col in right_record:
                    if col.lower() == right_column.lower():
                        right_value = right_record[col]
                        break
                
                # Check if values match for join
                if left_value == right_value:
                    joined_record = self._create_joined_record(
                        left_record, right_record, left_alias, right_alias)
                    result.append(joined_record)
                    left_matched = True
                    matched_right.add(id(right_record))
            
            # LEFT or FULL join: include left records without matches
            if not left_matched and (join_type == "LEFT" or join_type == "FULL"):
                joined_record = {}
                
                # Add fields from left table
                for field, value in left_record.items():
                    joined_record[f"{left_alias}.{field}"] = value
                
                # Add NULL values for right table
                if right_table_data:
                    for field in right_table_data[0]:
                        joined_record[f"{right_alias}.{field}"] = None
                
                result.append(joined_record)
            
            if left_matched:
                matched_left.add(id(left_record))
        
        # RIGHT or FULL join: include right records without matches
        if join_type == "RIGHT" or join_type == "FULL":
            for right_record in right_table_data:
                if id(right_record) not in matched_right:
                    joined_record = {}
                    
                    # Add NULL values for left table
                    if left_table_data:
                        for field in left_table_data[0]:
                            joined_record[f"{left_alias}.{field}"] = None
                    
                    # Add fields from right table
                    for field, value in right_record.items():
                        joined_record[f"{right_alias}.{field}"] = value
                    
                    result.append(joined_record)
        
        logging.info(f"Nested loop join produced {len(result)} results")
        return result

    def _create_joined_record(self, left_record, right_record, left_alias, right_alias):
        """Helper method to create a joined record with proper field prefixes."""
        joined_record = {}
        
        # Add fields from left record with table alias
        for field, value in left_record.items():
            joined_record[f"{left_alias}.{field}"] = value
        
        # Add fields from right record with table alias
        for field, value in right_record.items():
            joined_record[f"{right_alias}.{field}"] = value
        
        return joined_record

    def _execute_merge_join(
        self,
        left_table_data,
        right_table_data,
        left_column,
        right_column,
        left_alias,
        right_alias,
        join_type="INNER"
    ):
        """
        Execute a sort-merge join with support for all join types.
        
        This algorithm works well when data is already sorted or when the dataset is too
        large to fit in memory for a hash join.
        """
        logging.info(f"Executing {join_type} merge join")
        
        if not left_column or not right_column:
            if join_type == "CROSS":
                # For cross joins, fall back to nested loop
                return self._execute_nested_loop_join(
                    left_table_data, right_table_data, 
                    left_column, right_column,
                    left_alias, right_alias,
                    join_type
                )
            else:
                raise ValueError("Sort-merge join requires join columns")
        
        # Helper function to get join key from a record
        def get_key(record, column_name):
            for col in record:
                if col.lower() == column_name.lower():
                    return record[col]
            return None
        
        # Sort both tables on join columns
        sorted_left = sorted(
            left_table_data, 
            key=lambda r: (get_key(r, left_column) is None, get_key(r, left_column))
        )
        sorted_right = sorted(
            right_table_data, 
            key=lambda r: (get_key(r, right_column) is None, get_key(r, right_column))
        )
        
        result = []
        i = j = 0
        
        # Track matched records for outer joins
        matched_left = [False] * len(sorted_left)
        matched_right = [False] * len(sorted_right)
        
        # Merge sorted tables
        while i < len(sorted_left) and j < len(sorted_right):
            left_val = get_key(sorted_left[i], left_column)
            right_val = get_key(sorted_right[j], right_column)
            
            # Skip null values in inner join
            if left_val is None and join_type == "INNER":
                i += 1
                continue
            if right_val is None and join_type == "INNER":
                j += 1
                continue
            
            if left_val == right_val:
                # Values match - find all matching records on both sides
                
                # First, collect all matching records from left side
                left_matches = []
                k = i
                while k < len(sorted_left) and get_key(sorted_left[k], left_column) == left_val:
                    left_matches.append(k)
                    k += 1
                
                # Then, collect all matching records from right side
                right_matches = []
                l = j
                while l < len(sorted_right) and get_key(sorted_right[l], right_column) == right_val:
                    right_matches.append(l)
                    l += 1
                
                # Join all matching pairs
                for left_idx in left_matches:
                    matched_left[left_idx] = True
                    for right_idx in right_matches:
                        matched_right[right_idx] = True
                        joined_record = self._create_joined_record(
                            sorted_left[left_idx], 
                            sorted_right[right_idx],
                            left_alias, right_alias
                        )
                        result.append(joined_record)
                
                # Move pointers past all processed records
                i = k
                j = l
            elif left_val < right_val:
                i += 1
            else:
                j += 1
        
        # Handle outer joins - add unmatched records
        if join_type in ["LEFT", "FULL"]:
            for idx, record in enumerate(sorted_left):
                if not matched_left[idx]:
                    joined_record = {}
                    
                    # Add fields from left record
                    for field, value in record.items():
                        joined_record[f"{left_alias}.{field}"] = value
                    
                    # Add NULL values for right table
                    if right_table_data:
                        for field in right_table_data[0]:
                            joined_record[f"{right_alias}.{field}"] = None
                    
                    result.append(joined_record)
        
        if join_type in ["RIGHT", "FULL"]:
            for idx, record in enumerate(sorted_right):
                if not matched_right[idx]:
                    joined_record = {}
                    
                    # Add NULL values for left table
                    if left_table_data:
                        for field in left_table_data[0]:
                            joined_record[f"{left_alias}.{field}"] = None
                    
                    # Add fields from right record
                    for field, value in record.items():
                        joined_record[f"{right_alias}.{field}"] = value
                    
                    result.append(joined_record)
        
        logging.info(f"Merge join produced {len(result)} results")
        return result

    def _execute_index_join(
        self,
        left_table_name,
        right_table_name,
        left_column,
        right_column,
        left_alias,
        right_alias,
        join_type="INNER"
    ):
        """
        Execute an index join between two tables using an existing index if available.
        Supports all join types.
        """
        logging.info(f"Executing {join_type} index join")
        
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            logging.error("No database selected for index join")
            return []
        
        # Check for indexes on left and right tables
        left_index = self.index_manager.get_index(f"{left_table_name}.{left_column}")
        right_index = self.index_manager.get_index(f"{right_table_name}.{right_column}")
        
        # Determine which table has an index (prefer right table index for probe-build pattern)
        if right_index:
            # Use right table's index (optimal case)
            indexed_table_name = right_table_name
            indexed_column = right_column
            indexed_alias = right_alias
            probe_table_name = left_table_name
            probe_column = left_column
            probe_alias = left_alias
            index = right_index
            is_right_indexed = True
        elif left_index:
            # Use left table's index
            indexed_table_name = left_table_name
            indexed_column = left_column
            indexed_alias = left_alias
            probe_table_name = right_table_name
            probe_column = right_column
            probe_alias = right_alias
            index = left_index
            is_right_indexed = False
        else:
            # No index available, fall back to hash join
            logging.warning(f"No index found for join columns, falling back to hash join")
            return self._execute_hash_join(
                self.catalog_manager.query_with_condition(left_table_name, [], ["*"]),
                self.catalog_manager.query_with_condition(right_table_name, [], ["*"]),
                left_column, right_column, left_alias, right_alias, join_type
            )
        
        # Get the probe table data (full scan of non-indexed table)
        probe_data = self.catalog_manager.query_with_condition(probe_table_name, [], ["*"])
        
        # Initialize indexed_data with empty list for inner joins
        indexed_data = []
        
        # For outer joins, we may need all data from the indexed table
        if join_type in ["LEFT", "RIGHT", "FULL"]:
            # Adjust join type based on which table is indexed
            if (join_type == "LEFT" and is_right_indexed) or (join_type == "RIGHT" and not is_right_indexed):
                # We need all data from indexed table
                indexed_data = self.catalog_manager.query_with_condition(indexed_table_name, [], ["*"])
            elif join_type == "FULL":
                # For FULL joins, we always need all data from indexed table
                indexed_data = self.catalog_manager.query_with_condition(indexed_table_name, [], ["*"])
        
        result = []
        matched_probe_records = set()  # Track which probe records were matched
        matched_indexed_values = set()  # Track which indexed values were matched
        
        # Probe the index with each record from the probe table
        for probe_record in probe_data:
            # Find the join value in the probe record
            probe_value = None
            for col in probe_record:
                if col.lower() == probe_column.lower():
                    probe_value = probe_record[col]
                    break
            
            found_match = False
            
            if probe_value is not None:
                # Use the index to look up matching indexed records
                try:
                    matching_keys = index.search(probe_value)
                    
                    if matching_keys:
                        found_match = True
                        
                        # Handle different return types from index search
                        if not isinstance(matching_keys, list):
                            matching_keys = [matching_keys]
                        
                        # Retrieve each matching indexed record
                        for key in matching_keys:
                            if key is None:
                                continue
                                
                            # We need to fetch the actual record using the key
                            indexed_record = self.catalog_manager.get_record_by_key(indexed_table_name, key)
                            
                            if indexed_record:
                                # Create joined record
                                if is_right_indexed:
                                    joined_record = self._create_joined_record(probe_record, indexed_record, 
                                                                            probe_alias, indexed_alias)
                                else:
                                    joined_record = self._create_joined_record(indexed_record, probe_record,
                                                                            indexed_alias, probe_alias)
                                
                                result.append(joined_record)
                                matched_indexed_values.add(probe_value)
                except Exception as e:
                    logging.error(f"Error searching index for value {probe_value}: {str(e)}")
                    logging.error(traceback.format_exc())
            
            # For LEFT or FULL outer join, include probe records without matches
            if not found_match and ((not is_right_indexed and (join_type == "LEFT" or join_type == "FULL")) or 
                                (is_right_indexed and (join_type == "RIGHT" or join_type == "FULL"))):
                joined_record = {}
                
                # Add fields from probe record
                for field, value in probe_record.items():
                    joined_record[f"{probe_alias}.{field}"] = value
                
                # Add NULL values for indexed table fields
                # We need a sample record to get the field names
                sample_indexed = next(iter(indexed_data), {})  # Safe now since indexed_data is always initialized
                for field in sample_indexed:
                    joined_record[f"{indexed_alias}.{field}"] = None
                
                result.append(joined_record)
            
            if found_match:
                matched_probe_records.add(id(probe_record))
        
        # For RIGHT or FULL join, include indexed records without matches
        if join_type in ["RIGHT", "FULL"] and indexed_data:  # Safe check now
            for indexed_record in indexed_data:
                # Get the join value from this indexed record
                indexed_value = None
                for col in indexed_record:
                    if col.lower() == indexed_column.lower():
                        indexed_value = indexed_record[col]
                        break
                
                # Skip if this value was already matched
                if indexed_value is not None and indexed_value not in matched_indexed_values:
                    joined_record = {}
                    
                    # Add fields from indexed record
                    for field, value in indexed_record.items():
                        joined_record[f"{indexed_alias}.{field}"] = value
                    
                    # Add NULL values for probe table fields
                    if probe_data:
                        for field in probe_data[0]:
                            joined_record[f"{probe_alias}.{field}"] = None
                    
                    result.append(joined_record)
        
        logging.info(f"Index join produced {len(result)} results")
        return result

    def execute_set_preference(self, plan):
        """
        Update user preferences.
        """
        preference = plan["preference"]
        value = plan["value"]
        user_id = plan.get("user_id")

        # Update preferences
        self.preferences[preference] = value
        self.catalog_manager.update_preferences({preference: value}, user_id)

        return f"Preference '{preference}' set to '{value}'."

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

    def _parse_condition_to_list(self, condition_str):
        """
        Parse a SQL condition string into a list of condition dictionaries for B+ tree querying.

        Handles basic conditions like:
        - age > 30
        - name = 'John'
        - price <= 100

        Args:
            condition_str: SQL condition string (e.g., "age > 30")

        Returns:
            List of condition dictionaries for query processing
        """
        if not condition_str:
            return []

        # Basic parsing for simpler conditions (focus on getting this working first)
        conditions = []

        # Check if this is a simple condition with a comparison operator
        for op in [">=", "<=", "!=", "<>", "=", ">", "<"]:
            if op in condition_str:
                parts = condition_str.split(op, 1)
                if len(parts) == 2:
                    column = parts[0].strip()
                    value_str = parts[1].strip()
                    value = self._parse_value(value_str)

                    # Map <> to !=
                    operator = op if op != "<>" else "!="

                    conditions.append(
                        {"column": column, "operator": operator, "value": value}
                    )
                    break

        logging.debug(f"Parsed condition '{condition_str}' to: {conditions}")
        return conditions

    def parse_expression(self, tokens, start=0):
        """_summary_

        Args:
            tokens (_type_): _description_
            start (int, optional): _description_. Defaults to 0.

        Returns:
            _type_: _description_
        """
        conditions = []
        i = start
        current_condition = None
        current_operator = "AND"  # Default operator

        while i < len(tokens):
            token = tokens[i].upper()

            # Handle parenthesized expressions
            if token == "(":
                sub_conditions, end_idx = self.parse_expression(tokens, i + 1)

                if current_condition is None:
                    current_condition = {
                        "type": "group",
                        "operator": current_operator,
                        "conditions": sub_conditions,
                    }
                else:
                    # Join with current condition using current_operator
                    conditions.append(current_condition)
                    current_condition = {
                        "type": "group",
                        "operator": current_operator,
                        "conditions": sub_conditions,
                    }

                i = end_idx + 1  # Skip past the closing parenthesis
                continue
            elif token == ")":
                # End of parenthesized expression
                if current_condition is not None:
                    conditions.append(current_condition)
                return conditions, i

            # Handle logical operators
            elif token in ("AND", "OR"):
                current_operator = token
                i += 1
                continue

            # Handle NOT operator
            elif token == "NOT":
                # Look ahead to handle NOT IN, NOT BETWEEN, NOT LIKE
                if i + 1 < len(tokens) and tokens[i + 1].upper() in (
                    "IN",
                    "BETWEEN",
                    "LIKE",
                ):
                    token = f"NOT {tokens[i + 1].upper()}"
                    i += 1  # Skip the next token as we've combined it
                else:
                    # Standalone NOT - needs to be applied to the next condition
                    next_cond, end_idx = self.parse_expression(tokens, i + 1)
                    current_condition = {
                        "type": "NOT",
                        "condition": next_cond[0] if next_cond else {},
                    }
                    i = end_idx
                    continue

            # Process condition based on operator
            if i + 2 < len(
                tokens
            ):  # Ensure we have at least 3 tokens (column, operator, value)
                column = tokens[i]
                operator = tokens[i + 1].upper()

                # Handle special operators
                if operator in ("IN", "NOT IN"):
                    # Parse IN list: column IN (val1, val2, ...)
                    values = []
                    if i + 3 < len(tokens) and tokens[i + 2] == "(":
                        j = i + 3
                        while j < len(tokens) and tokens[j] != ")":
                            if tokens[j] != ",":
                                values.append(self._parse_value(tokens[j]))
                            j += 1

                        current_condition = {
                            "column": column,
                            "operator": operator,
                            "value": values,
                        }
                        i = j + 1  # Skip to after the closing parenthesis
                        continue

                elif operator in ("BETWEEN", "NOT BETWEEN"):
                    # Parse BETWEEN: column BETWEEN value1 AND value2
                    if i + 4 < len(tokens) and tokens[i + 3].upper() == "AND":
                        value1 = self._parse_value(tokens[i + 2])
                        value2 = self._parse_value(tokens[i + 4])

                        current_condition = {
                            "column": column,
                            "operator": operator,
                            "value": [value1, value2],
                        }
                        i += 5  # Skip all processed tokens
                        continue

                elif operator in ("LIKE", "NOT LIKE"):
                    # Handle LIKE operator
                    pattern = self._parse_value(tokens[i + 2])

                    current_condition = {
                        "column": column,
                        "operator": operator,
                        "value": pattern,
                    }
                    i += 3
                    continue

                elif operator in ("IS"):
                    # Handle IS NULL or IS NOT NULL
                    if i + 2 < len(tokens) and tokens[i + 2].upper() == "NULL":
                        current_condition = {
                            "column": column,
                            "operator": "IS NULL",
                            "value": None,
                        }
                        i += 3
                    elif (
                        i + 3 < len(tokens)
                        and tokens[i + 2].upper() == "NOT"
                        and tokens[i + 3].upper() == "NULL"
                    ):
                        current_condition = {
                            "column": column,
                            "operator": "IS NOT NULL",
                            "value": None,
                        }
                        i += 4
                    continue

                # Standard comparison operators
                elif operator in ("=", ">", "<", ">=", "<=", "!=", "<>"):
                    value = self._parse_value(tokens[i + 2])

                    # Map <> to !=
                    if operator == "<>":
                        operator = "!="

                    current_condition = {
                        "column": column,
                        "operator": operator,
                        "value": value,
                    }
                    i += 3
                    continue

            # If nothing matched, just advance
            i += 1

        # Add the last condition if it exists
        if current_condition is not None:
            conditions.append(current_condition)

        return conditions, len(tokens) - 1

    def _flatten_conditions(self, conditions):
        """
        Flatten nested condition structures into a list of simple conditions.
        Used by query engine when not optimizing with complex expressions.

        Args:
            conditions: Nested structure of conditions

        Returns:
            List of simplified conditions for basic B+ tree queries
        """
        result = []

        # For single conditions
        if isinstance(conditions, dict):
            conditions = [conditions]

        for condition in conditions:
            if condition.get("type") == "group":
                # Recursively process sub-conditions
                sub_results = self._flatten_conditions(
                    condition.get("conditions", []))
                result.extend(sub_results)
            elif condition.get("type") == "NOT":
                # Negate the condition and add it (if simple enough to negate)
                sub_condition = condition.get("condition", {})
                if "operator" in sub_condition and "column" in sub_condition:
                    negated = self._negate_condition(sub_condition)
                    if negated:
                        result.append(negated)
            elif "column" in condition and "operator" in condition:
                # Standard condition
                result.append(condition)

        return result

    def _negate_condition(self, condition):
        """
        Negate a simple condition for the NOT operator.

        Args:
            condition: Original condition dictionary

        Returns:
            Negated condition dictionary
        """
        op_map = {
            "=": "!=",
            "!=": "=",
            "<>": "=",
            ">": "<=",
            "<": ">=",
            ">=": "<",
            "<=": ">",
            "LIKE": "NOT LIKE",
            "NOT LIKE": "LIKE",
            "IN": "NOT IN",
            "NOT IN": "IN",
            "BETWEEN": "NOT BETWEEN",
            "NOT BETWEEN": "BETWEEN",
            "IS NULL": "IS NOT NULL",
            "IS NOT NULL": "IS NULL",
        }

        if condition["operator"] in op_map:
            return {
                "column": condition["column"],
                "operator": op_map[condition["operator"]],
                "value": condition["value"],
            }

        # If operator can't be simply negated
        return None

    def execute_select(self, plan):
        """Execute a SELECT query and return results."""
        # Check for aggregate functions in column names
        columns = plan.get("columns", [])
        for _, col in enumerate(columns):
            if isinstance(col, str) and col != "*":
                # Check for standard aggregate functions
                match = re.search(r"(\w+)\s*\(\s*([^)]*)\s*\)", col)
                if match:
                    func_name = match.group(1).upper()
                    col_name = match.group(2).strip()

                    # Update this line to include RAND and GCD
                    if func_name in ("COUNT", "SUM", "AVG", "MIN", "MAX", "RAND", "GCD"):
                        logging.error(f"Detected aggregate function in execute_select: {func_name}({col_name})")

                        # Create a temporary aggregate plan
                        agg_plan = {
                            "type": "AGGREGATE",
                            "function": func_name,
                            "column": col_name,
                            "table": plan.get("table") or (plan.get("tables", [""])[0] if plan.get("tables") else ""),
                            "condition": plan.get("condition"),  # Pass the condition
                            "top": plan.get("top"),  # Pass TOP parameter 
                            "limit": plan.get("limit")  # Pass LIMIT parameter
                        }

                        # Execute the aggregate plan instead
                        return self.execute_aggregate(agg_plan)

        # Get table name
        table_name = plan.get("table")
        if not table_name and "tables" in plan and plan["tables"]:
            table_name = plan["tables"][0]

        if not table_name:
            return {"error": "No table specified", "status": "error", "type": "error"}

        # Get column list - preserve original case
        columns = plan.get("columns", ["*"])

        # Handle TOP N clause
        top_n = plan.get("top")
        if top_n is not None:
            try:
                top_n = int(top_n)
                logging.debug(f"TOP {top_n} specified")
            except (ValueError, TypeError):
                top_n = None

        # Get current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {
                "error": "No database selected. Use 'USE database_name' first.",
                "status": "error",
                "type": "error",
            }

        try:
            # Verify table exists - CASE INSENSITIVE COMPARISON
            tables = self.catalog_manager.list_tables(db_name)

            # Create case-insensitive lookup for table names
            tables_lower = {table.lower(): table for table in tables}

            # Try to find the table case-insensitively
            actual_table_name = table_name  # Default
            if table_name.lower() in tables_lower:
                # Use the correct case from the tables list
                actual_table_name = tables_lower[table_name.lower()]
                logging.debug(
                    f"Using case-corrected table name: {
                        actual_table_name} instead of {table_name}"
                )
            elif table_name not in tables:
                return {
                    "error": f"Table '{table_name}' does not exist in database '{db_name}'",
                    "status": "error",
                    "type": "error",
                }

            # Build condition for query
            conditions = []
            condition = plan.get("condition")
            if condition:
                logging.debug(f"Parsing condition: {condition}")
                conditions = self._parse_condition_to_list(condition)
                logging.debug(f"Parsed conditions: {conditions}")

            # Query the data using catalog manager - always get all columns first
            results = self.catalog_manager.query_with_condition(
                actual_table_name, conditions, ["*"]
            )

            logging.debug(f"Query returned {
                          len(results) if results else 0} results")

            # Format results for client display
            if not results:
                return {
                    "columns": columns if "*" not in columns else [],
                    "rows": [],
                    "status": "success",
                    "type": "select_result",
                }

            # Create a mapping of lowercase column names to original case
            column_case_map = {}
            if results:
                first_record = results[0]
                for col_name in first_record:
                    column_case_map[col_name.lower()] = col_name

            # Apply ORDER BY if specified
            order_by = plan.get("order_by")
            if order_by and results:
                logging.debug(f"Applying ORDER BY: {order_by}")

                # Parse the ORDER BY clause
                order_columns = []
                reverse_flags = []

                # Split by comma for multiple columns
                for order_part in order_by.split(','):
                    order_part = order_part.strip()
                    if ' DESC' in order_part.upper():
                        col_name = order_part.upper().replace(' DESC', '').strip()
                        reverse = True
                    else:
                        col_name = order_part.upper().replace(' ASC', '').strip()
                        reverse = False

                    # Find actual column name with correct case
                    actual_col = None
                    for record_col in results[0]:
                        if record_col.lower() == col_name.lower():
                            actual_col = record_col
                            break

                    if actual_col:
                        order_columns.append(actual_col)
                        reverse_flags.append(reverse)

                # Sort the results using the specified columns
                if order_columns:
                    for i, (col, reverse) in reversed(list(enumerate(zip(order_columns, reverse_flags)))):
                        # Use a lambda for sorting that handles None values properly
                        results = sorted(
                            results,
                            key=lambda x: (x.get(col) is None, x.get(col)),
                            reverse=reverse
                        )

                    logging.debug(f"Results sorted by {order_columns}")

            # Apply TOP N
            if top_n is not None and top_n > 0 and results:
                results = results[:top_n]
                logging.debug(f"Applied TOP {top_n}, now {
                              len(results)} results")

            # Apply LIMIT if specified
            limit = plan.get("limit")
            if limit is not None and isinstance(limit, int) and results and limit > 0:
                results = results[:limit]
                logging.debug(f"Applied LIMIT {limit}, now {
                              len(results)} results")

            # Project columns (select specific columns or all)
            result_columns = []
            result_rows = []

            # Figure out which columns to include in the result, with original case
            if "*" in columns:
                # Make sure we have results and they have keys
                if results and isinstance(results[0], dict) and results[0]:
                    # Use all columns from first record (original case)
                    result_columns = list(results[0].keys())

                    # Log column names that will be returned
                    logging.error(f"SELECT * will return these columns: {result_columns}")

                    # Create rows with ALL columns from each record IN ORDER
                    for record in results:
                        row = []
                        for col in result_columns:
                            row.append(record.get(col))
                        result_rows.append(row)
                else:
                    # Handle empty results or malformed data
                    logging.error("No valid results or empty dictionary in results")
                    if results:
                        # Try to recover column names if possible
                        for record in results:
                            if isinstance(record, dict) and record:
                                result_columns = list(record.keys())
                                break
            else:
                # For specific columns, find the original case from data
                result_columns = []
                for col in columns:
                    # Find matching column with original case
                    original_case_col = None
                    for actual_col in results[0].keys():
                        if actual_col.lower() == col.lower():
                            original_case_col = actual_col
                            break

                    # Use original case if found, otherwise use as provided
                    result_columns.append(original_case_col or col)
                
                # Create rows with the selected columns
                for record in results:
                    row = []
                    for column_name in result_columns:
                        # Find the matching column case-insensitively
                        value = None
                        for record_col in record:
                            if record_col.lower() == column_name.lower():
                                value = record[record_col]
                                break
                        row.append(value)
                    result_rows.append(row)

            logging.error(f"Final result: {len(result_rows)} rows with columns: {result_columns}")
            # Debug print first row to verify data
            if result_rows:
                logging.error(f"First row data: {result_rows[0]}")

            return {
                "columns": result_columns,  # Original case preserved
                "rows": result_rows,
                "status": "success",
                "type": "select_result",
            }

        except Exception as e:
            import traceback

            logging.error(f"Error executing SELECT: {str(e)}")
            logging.error(traceback.format_exc())
            return {
                "error": f"Error executing SELECT: {str(e)}",
                "status": "error",
                "type": "error",
            }

    def _parse_condition_to_list(self, condition_str):
        """
        Parse a SQL condition string into a list of condition dictionaries for B+ tree querying.
        """
        if not condition_str:
            logging.debug("No condition string provided to parse")
            return []

        logging.debug(f"Parsing condition string: '{condition_str}'")
        # Basic parsing for simpler conditions (focus on getting this working first)
        conditions = []

        # Check if this is a simple condition with a comparison operator
        for op in [">=", "<=", "!=", "<>", "=", ">", "<"]:
            if op in condition_str:
                parts = condition_str.split(op, 1)
                if len(parts) == 2:
                    column = parts[0].strip()
                    value_str = parts[1].strip()
                    value = self._parse_value(value_str)

                    # Map <> to !=
                    operator = op if op != "<>" else "!="

                    conditions.append(
                        {"column": column, "operator": operator, "value": value}
                    )
                    logging.debug(f"Parsed condition: column='{column}', operator='{operator}', value='{value}'")
                    break

        logging.debug(f"Final parsed conditions: {conditions}")
        return conditions

    def execute_distinct(self, plan):
        """
        Execute a DISTINCT query.
        """
        table_name = plan["table"]
        column = plan["column"]

        # Get current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {
                "error": "No database selected. Use 'USE database_name' first.",
                "status": "error",
            }

        # Verify the table exists
        if not self.catalog_manager.list_tables(
            db_name
        ) or table_name not in self.catalog_manager.list_tables(db_name):
            return {"error": f"Table '{table_name}' does not exist", "status": "error"}

        # Use catalog manager to get data
        results = self.catalog_manager.query_with_condition(
            table_name, [], [column])

        # Extract distinct values
        distinct_values = set()
        for record in results:
            if column in record and record[column] is not None:
                distinct_values.add(record[column])

        return {
            "columns": [column],
            "rows": [[value] for value in distinct_values],
            "status": "success",
        }

    def execute_create_view(self, plan):
        """
        Execute CREATE VIEW queries.
        """
        return self.catalog_manager.create_view(plan["view_name"], plan["query"])

    def execute_drop_view(self, plan):
        """
        Execute DROP VIEW queries.
        """
        return self.catalog_manager.drop_view(plan["view_name"])

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
        index_name = plan.get("index_name")
        table_name = plan.get("table")
        column = plan.get("column")
        is_unique = plan.get("unique", False)

        if not table_name:
            return {"error": "Table name must be specified for CREATE INDEX"}

        if not column:
            return {"error": "Column name must be specified for CREATE INDEX"}

        logging.info(f"Creating index '{index_name}' on {table_name}.{column}")

        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected. Use 'USE database_name' first."}

        # Verify table exists
        tables = self.catalog_manager.list_tables(db_name)
        if table_name not in tables:
            return {
                "error": f"Table '{table_name}' does not exist in database '{db_name}'"
            }

        # Create the index
        try:
            result = self.catalog_manager.create_index(
                table_name, column, "BTREE", is_unique
            )
            if isinstance(result, str) and "already exists" in result:
                return {"error": result}

            return {
                "message": f"Index '{index_name}' created on '{table_name}.{column}'"
            }
        except Exception as e:
            logging.error(f"Error creating index: {str(e)}")
            return {"error": f"Error creating index: {str(e)}"}

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

    def execute_begin_transaction(self):
        """Begin a new transaction."""
        result = self.catalog_manager.begin_transaction()
        return {"message": result}

    def execute_commit_transaction(self):
        """Commit the current transaction."""
        result = self.catalog_manager.commit_transaction()
        return {"message": result}

    def execute_rollback_transaction(self):
        """Rollback the current transaction."""
        result = self.catalog_manager.rollback_transaction()
        return {"message": result}

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
                conditions = self._parse_condition_to_list(condition)
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
                parsed_condition = self._parse_condition_to_dict(condition)
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
        
    def _parse_condition_to_dict(self, condition_str):
        """
        Parse a SQL condition string into a dictionary format.
        This is used for UPDATE operations.
        
        Args:
            condition_str: SQL condition string (e.g., "age > 30")
            
        Returns:
            Dictionary with condition information
        """
        if not condition_str:
            return {}
            
        # Use the existing condition parsing logic
        conditions = self._parse_condition_to_list(condition_str)
        if conditions and len(conditions) > 0:
            return conditions[0]  # Return first condition
        
        return {}

    def execute_visualize(self, plan):
        """
        Execute a VISUALIZE command.
        """
        # Get visualization object type
        object_type = plan.get("object")

        if object_type == "BPTREE":
            index_name = plan.get("index_name")
            table_name = plan.get("table")

            # Get current database
            db_name = self.catalog_manager.get_current_database()
            if not db_name:
                return {
                    "error": "No database selected. Use 'USE database_name' first.",
                    "status": "error",
                }

            # Get index manager to access B+ trees
            index_manager = self.index_manager

            # If both table and index are specified, visualize that specific index
            if table_name and index_name and index_name.upper() != "ON":
                # Try to get the index object
                full_index_name = f"{table_name}.{index_name}"
                index_obj = index_manager.get_index(full_index_name)

                if not index_obj:
                    # Get indexes from catalog to see if it exists
                    indexes = self.catalog_manager.get_indexes_for_table(
                        table_name)
                    if not indexes or index_name not in indexes:
                        return {
                            "error": f"Index '{index_name}' not found for table '{table_name}'",
                            "status": "error",
                        }
                    else:
                        # Index exists in catalog but file not found - rebuild it
                        try:
                            logging.info(
                                f"Attempting to rebuild index {
                                    index_name} on {table_name}..."
                            )
                            column = indexes[index_name].get("column")
                            is_unique = indexes[index_name].get(
                                "unique", False)
                            index_obj = index_manager.build_index(
                                table_name, index_name, column, is_unique, db_name
                            )
                        except Exception as e:
                            return {
                                "error": f"Error rebuilding index: {str(e)}",
                                "status": "error",
                            }

                # Now visualize the index
                if index_obj:
                    try:
                        # Try NetworkX visualization first
                        try:
                            from bptree_networkx import BPlusTreeNetworkXVisualizer

                            visualizer = BPlusTreeNetworkXVisualizer(
                                output_dir="visualizations"
                            )
                            viz_path = visualizer.visualize_tree(
                                index_obj, output_name=full_index_name
                            )

                            if viz_path:
                                # Convert to absolute path
                                abs_path = os.path.abspath(viz_path)
                                logging.info(
                                    f"Generated visualization at {abs_path}")

                                # Generate text representation
                                text_repr = self._get_tree_text(index_obj)

                                return {
                                    "message": f"B+ Tree visualization for '{index_name}' on '{table_name}'",
                                    "status": "success",
                                    "visualization_path": abs_path,
                                    "text_representation": text_repr,
                                }
                        except ImportError:
                            logging.warning(
                                "NetworkX not available, falling back to Graphviz"
                            )

                        # Fall back to Graphviz
                        viz_path = index_obj.visualize(
                            index_manager.visualizer, f"{
                                full_index_name}_visualization"
                        )

                        if viz_path:
                            # Convert to absolute path
                            abs_path = os.path.abspath(viz_path)
                            logging.info(
                                f"Generated visualization at {abs_path}")

                            # Generate text representation
                            text_repr = self._get_tree_text(index_obj)

                            return {
                                "message": f"B+ Tree visualization for '{index_name}' on '{table_name}'",
                                "status": "success",
                                "visualization_path": abs_path,
                                "text_representation": text_repr,
                            }
                        else:
                            return {
                                "message": f"Text representation for '{index_name}' on '{table_name}'",
                                "status": "success",
                                "text_representation": self._get_tree_text(index_obj),
                            }

                    except Exception as e:
                        logging.error(f"Error visualizing B+ tree: {str(e)}")
                        return {
                            "error": f"Error visualizing B+ tree: {str(e)}",
                            "status": "error",
                        }
                else:
                    return {
                        "error": f"Could not find or rebuild index '{index_name}'",
                        "status": "error",
                    }

    def _get_tree_text(self, tree):
        """Generate a text representation of the tree"""
        lines = [f"B+ Tree '{tree.name}' (Order: {tree.order})"]

        def print_node(node, level=0, prefix="Root: "):
            indent = "  " * level

            # Print the current node
            if hasattr(node, "leaf") and node.leaf:
                key_values = []
                for item in node.keys:
                    if isinstance(item, tuple) and len(item) >= 2:
                        key_values.append(f"{item[0]}:{item[1]}")
                    else:
                        key_values.append(str(item))
                lines.append(
                    f"{indent}{prefix}LEAF {{{', '.join(key_values)}}}")
            else:
                lines.append(
                    f"{indent}{
                        prefix}NODE {{{', '.join(map(str, node.keys))}}}"
                )

            # Print children recursively
            if hasattr(node, "children"):
                for i, child in enumerate(node.children):
                    child_prefix = f"Child {i}: "
                    print_node(child, level + 1, child_prefix)

        print_node(tree.root)
        return "\n".join(lines)

    def _count_nodes(self, node):
        """Count total nodes in the tree"""
        if node is None:
            return 0
        count = 1  # Count this node
        if hasattr(node, "children"):
            for child in node.children:
                count += self._count_nodes(child)
        return count

    def _count_leaves(self, node):
        """Count leaf nodes in the tree"""
        if node is None:
            return 0
        if hasattr(node, "leaf") and node.leaf:
            return 1
        count = 0
        if hasattr(node, "children"):
            for child in node.children:
                count += self._count_leaves(child)
        return count

    def _tree_height(self, node, level=0):
        """Calculate the height of the tree"""
        if node is None:
            return level
        if hasattr(node, "leaf") and node.leaf:
            return level + 1
        if hasattr(node, "children") and node.children:
            return self._tree_height(node.children[0], level + 1)
        return level + 1

    def execute_visualize_index(self, plan):
        """
        Execute a VISUALIZE INDEX command.
        """
        index_name = plan.get("index_name")
        table_name = plan.get("table")

        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {
                "error": "No database selected. Use 'USE database_name' first.",
                "status": "error",
            }

        # Determine the index name format based on parameters
        if table_name and index_name:
            full_index_name = f"{table_name}.{index_name}"
        elif table_name:
            # Visualize all indexes for the table
            indexes = self.catalog_manager.get_indexes_for_table(table_name)
            if not indexes:
                return {
                    "error": f"No indexes found for table '{table_name}'",
                    "status": "error",
                }

            results = []
            for idx_name in indexes:
                full_index_name = f"{table_name}.{idx_name}"
                result = self.visualize_index(full_index_name)
                if result:
                    results.append(result)

            return {
                "message": f"Visualized {len(results)} indexes for table '{table_name}'",
                "visualizations": results,
                "status": "success",
            }
        else:
            # Visualize all indexes
            count = self.visualize_all_indexes()
            return {"message": f"Visualized {count} indexes", "status": "success"}

        # Visualize specific index
        result = self.visualize_index(full_index_name)

        if result:
            return {
                "message": f"Index '{full_index_name}' visualized successfully",
                "visualization": result,
                "status": "success",
            }
        else:
            return {
                "error": f"Failed to visualize index '{full_index_name}'",
                "status": "error",
            }

    def visualize_index(self, index_name):
        """
        Visualize a single index.
        """
        # Get the index from the index manager
        index = self.index_manager.get_index(index_name)
        if not index:
            return None

        # Generate a visualization
        visualization_path = f"visualizations/{index_name}_visualization.png"
        try:
            # Check if we have the BPlusTreeVisualizer
            from bptree import BPlusTreeVisualizer

            visualizer = BPlusTreeVisualizer()
            actual_path = index.visualize(visualizer, output_name=index_name)

            return {
                "index_name": index_name,
                "visualization_path": actual_path or visualization_path,
            }
        except ImportError:
            logging.warning(
                "BPlusTreeVisualizer not available. Install graphviz for visualizations."
            )
            return {
                "index_name": index_name,
                "error": "Visualization libraries not available",
            }

    def visualize_all_indexes(self):
        """
        Visualize all indexes in the current database.
        """
        try:
            # Check if we're running the visualization code
            count = self.index_manager.visualize_all_indexes()
            return count
        except Exception as e:
            logging.error(f"Error visualizing all indexes: {str(e)}")
            return 0

    def execute(self, plan):
        """
        Execute the query plan.
        """
        # Keep the type field for internal routing only
        plan_type = plan.get("type")

        if not plan_type:
            return {"error": "No operation type specified in plan", "status": "error"}

        # Process based on query type
        result = None
        logging.debug(f"Executing plan of type {plan_type}: {plan}")
        try:
            if plan_type == "SELECT":
                result = self.execute_select(plan)
            elif plan_type == "AGGREGATE":
                result = self.execute_aggregate(plan)
            elif plan_type == "INSERT":
                result = self.execute_insert(plan)
            elif plan_type == "UPDATE":
                result = self.execute_update(plan)
            elif plan_type == "DELETE":
                result = self.execute_delete(plan)
            elif plan_type == "CREATE_TABLE":
                result = self.execute_create_table(plan)
            elif plan_type == "DROP_TABLE":
                result = self.execute_drop_table(plan)
            elif plan_type == "CREATE_DATABASE":
                result = self.execute_create_database(plan)
            elif plan_type == "DROP_DATABASE":
                result = self.execute_drop_database(plan)
            elif plan_type == "USE_DATABASE":
                result = self.execute_use_database(plan)
            elif plan_type == "SHOW":
                result = self.execute_show(plan)
            elif plan_type == "CREATE_INDEX":
                result = self.execute_create_index(plan)
            elif plan_type == "DROP_INDEX":
                result = self.execute_drop_index(plan)
            elif plan_type == "VISUALIZE":
                result = self.execute_visualize(plan)
            elif plan_type == "BEGIN_TRANSACTION":
                result = self.execute_begin_transaction()
            elif plan_type == "COMMIT":
                result = self.execute_commit_transaction()
            elif plan_type == "ROLLBACK":
                result = self.execute_rollback_transaction()
            elif plan_type == "SET":
                result = self.execute_set_preference(plan)
            elif plan_type == "CREATE_VIEW":
                result = self.execute_create_view(plan)
            elif plan_type == "DROP_VIEW":
                result = self.execute_drop_view(plan)
            else:
                return {
                    "error": f"Unsupported operation type: {plan_type}",
                    "status": "error",
                }

            if isinstance(result, dict) and "status" not in result:
                result["status"] = "success"

            # Ensure type field is present
            if isinstance(result, dict) and "type" not in result:
                result["type"] = f"{plan_type.lower()}_result"

            return result

        except Exception as e:
            import traceback

            logging.error(f"Error executing {plan_type}: {str(e)}")
            logging.error(traceback.format_exc())
            return {
                "error": f"Error executing {plan_type}: {str(e)}",
                "status": "error",
                "type": "error",
            }
