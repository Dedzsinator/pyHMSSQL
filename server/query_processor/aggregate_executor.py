"""Aggregate Executor for handling aggregate functions like COUNT, SUM, AVG, etc."""

import logging
import random
import math
import traceback
from collections import defaultdict


class AggregateExecutor:
    """
    Class to execute aggregate functions.
    """

    def __init__(self, catalog_manager):
        """
        Initialize AggregateExecutor.

        Args:
            catalog_manager: The catalog manager instance
        """
        self.catalog_manager = catalog_manager

    def execute_aggregate(self, plan):
        """Execute an aggregate function and return results."""
        logging.debug("Executing aggregate function with plan: %s", plan)

        function = plan.get("function", "").upper()
        column = plan.get("column", "")
        table_name = plan.get("table", "")
        condition = plan.get("condition")
        top_n = plan.get("top")
        limit_n = plan.get("limit")

        # Get current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {
                "error": "No database selected. Use 'USE database_name' first.",
                "status": "error",
                "type": "error",
            }

        try:
            # Parse conditions if provided
            conditions = []
            if condition:
                try:
                    # Use SQLGlot-based condition parsing
                    from utils.sql_helpers import parse_condition_with_sqlglot

                    conditions = parse_condition_with_sqlglot(condition)
                except ImportError:
                    logging.warning(
                        "SQLGlot condition parsing not available, using fallback"
                    )
                    from utils.sql_helpers import parse_simple_condition

                    conditions = parse_simple_condition(condition)

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
                    "Using case-corrected table name: %s instead of %s",
                    actual_table_name,
                    table_name,
                )
            elif table_name not in tables:
                return {
                    "error": f"Table '{table_name}' does not exist in database '{db_name}'",
                    "status": "error",
                    "type": "error",
                }

            # Special handling for RAND() function
            if function == "RAND":
                result_value = random.random()
                logging.info(f"RAND() returned: {result_value}")

                return {
                    "columns": ["RAND()"],
                    "rows": [[result_value]],
                    "status": "success",
                    "type": "aggregate_result",
                }

            # Special handling for GCD function
            if function == "GCD":
                logging.info(f"Executing GCD function on column: {column}")

                # Get all records with the specified condition
                if condition:
                    records = self.catalog_manager.query_with_condition(
                        actual_table_name, conditions, [column]
                    )
                else:
                    records = self.catalog_manager.query_with_condition(
                        actual_table_name, [], [column]
                    )

                if not records:
                    return {
                        "columns": [f"GCD({column})"],
                        "rows": [[None]],
                        "status": "success",
                        "type": "aggregate_result",
                    }

                # Extract values and calculate GCD
                values = []
                for record in records:
                    for col_name in record:
                        if col_name.lower() == column.lower():
                            val = record[col_name]
                            if val is not None:
                                try:
                                    values.append(int(val))
                                except (ValueError, TypeError):
                                    logging.warning(
                                        f"Skipping non-integer value for GCD: {val}"
                                    )

                if not values:
                    gcd_result = None
                elif len(values) == 1:
                    gcd_result = values[0]
                else:
                    gcd_result = values[0]
                    for val in values[1:]:
                        gcd_result = math.gcd(gcd_result, val)

                logging.info(f"GCD calculation result: {gcd_result}")

                return {
                    "columns": [f"GCD({column})"],
                    "rows": [[gcd_result]],
                    "status": "success",
                    "type": "aggregate_result",
                }

            # Get all records for the aggregate calculation
            if condition:
                records = self.catalog_manager.query_with_condition(
                    actual_table_name, conditions, ["*"]
                )
                logging.info(
                    f"Found {len(records)} records matching condition: {condition}"
                )
            else:
                records = self.catalog_manager.query_with_condition(
                    actual_table_name, [], ["*"]
                )
                logging.info(f"Found {len(records)} total records")

            # Calculate the aggregate
            if function == "COUNT":
                if column == "*":
                    result_value = len(records)
                else:
                    # Count non-NULL values in the specified column
                    count = 0
                    for record in records:
                        for col_name in record:
                            if (
                                col_name.lower() == column.lower()
                                and record[col_name] is not None
                            ):
                                count += 1
                                break
                    result_value = count

            elif function in ["SUM", "AVG", "MIN", "MAX"]:
                values = []

                # Extract values from the specified column
                for record in records:
                    for col_name in record:
                        if col_name.lower() == column.lower():
                            val = record[col_name]
                            if val is not None:
                                try:
                                    # Convert to numeric
                                    if isinstance(val, str):
                                        if "." in val:
                                            values.append(float(val))
                                        else:
                                            values.append(int(val))
                                    else:
                                        values.append(val)
                                except (ValueError, TypeError):
                                    logging.warning(
                                        f"Skipping non-numeric value: {val}"
                                    )
                            break

                if not values:
                    result_value = None
                elif function == "SUM":
                    result_value = sum(values)
                elif function == "AVG":
                    result_value = sum(values) / len(values)
                elif function == "MIN":
                    result_value = min(values)
                elif function == "MAX":
                    result_value = max(values)

            else:
                return {
                    "error": f"Unsupported aggregate function: {function}",
                    "status": "error",
                    "type": "error",
                }

            logging.info(f"{function}({column}) = {result_value}")

            # Apply TOP N if specified
            result_rows = [[result_value]]
            if top_n is not None:
                try:
                    top_n = int(top_n)
                    if top_n > 0:
                        result_rows = result_rows[:top_n]
                except (ValueError, TypeError):
                    pass

            # Apply LIMIT if specified
            if limit_n is not None:
                try:
                    limit_n = int(limit_n)
                    if limit_n >= 0:
                        result_rows = result_rows[:limit_n]
                        logging.info(f"Applied LIMIT {limit_n} to aggregate result")
                except (ValueError, TypeError):
                    logging.error(f"Invalid LIMIT value: {limit_n}")

            return {
                "columns": [f"{function}({column})"],
                "rows": result_rows,
                "status": "success",
                "type": "aggregate_result",
            }

        except Exception as e:
            logging.error("Error executing aggregate function: %s", str(e))
            logging.error(traceback.format_exc())
            return {
                "error": f"Error executing aggregate function: {str(e)}",
                "status": "error",
                "type": "error",
            }

    def execute_group_by(self, plan):
        """Execute a GROUP BY query with aggregate functions."""
        logging.debug("Executing GROUP BY with plan: %s", plan)

        table_name = plan.get("table", "")
        columns = plan.get("columns", [])
        group_by_columns = plan.get("group_by", [])
        condition = plan.get("condition")
        order_by = plan.get("order_by")
        limit_n = plan.get("limit")

        # Get current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {
                "error": "No database selected. Use 'USE database_name' first.",
                "status": "error",
                "type": "error",
            }

        try:
            # Parse conditions if provided
            conditions = []
            if condition:
                try:
                    from utils.sql_helpers import parse_condition_with_sqlglot

                    conditions = parse_condition_with_sqlglot(condition)
                except ImportError:
                    from utils.sql_helpers import parse_simple_condition

                    conditions = parse_simple_condition(condition)

            # Verify table exists
            tables = self.catalog_manager.list_tables(db_name)
            tables_lower = {table.lower(): table for table in tables}

            actual_table_name = table_name
            if table_name.lower() in tables_lower:
                actual_table_name = tables_lower[table_name.lower()]
            elif table_name not in tables:
                return {
                    "error": f"Table '{table_name}' does not exist in database '{db_name}'",
                    "status": "error",
                    "type": "error",
                }

            # Get all records
            if condition:
                records = self.catalog_manager.query_with_condition(
                    actual_table_name, conditions, ["*"]
                )
            else:
                records = self.catalog_manager.query_with_condition(
                    actual_table_name, [], ["*"]
                )

            if not records:
                return {
                    "columns": columns,
                    "rows": [],
                    "status": "success",
                    "type": "group_by_result",
                }

            # Group records by the specified columns
            groups = defaultdict(list)

            for record in records:
                # Create group key
                group_key = []
                for group_col in group_by_columns:
                    for col_name in record:
                        if col_name.lower() == group_col.lower():
                            group_key.append(str(record[col_name]))
                            break

                groups[tuple(group_key)].append(record)

            # Calculate aggregates for each group
            result_rows = []
            result_columns = []

            # Determine result columns
            for col in columns:
                if col in group_by_columns:
                    result_columns.append(col)
                else:
                    # Assume aggregate function
                    result_columns.append(col)

            for group_key, group_records in groups.items():
                row = []

                col_index = 0
                for col in columns:
                    if col in group_by_columns:
                        # Use group key value
                        group_col_index = group_by_columns.index(col)
                        row.append(group_key[group_col_index])
                    else:
                        # Calculate aggregate for this column
                        # This is a simplified implementation
                        # In a real implementation, you'd parse the aggregate function
                        row.append(len(group_records))  # Default to count

                    col_index += 1

                result_rows.append(row)

            # Apply ORDER BY if specified
            if order_by and result_rows:
                order_column = (
                    order_by.get("column") if isinstance(order_by, dict) else order_by
                )
                direction = (
                    order_by.get("direction", "ASC")
                    if isinstance(order_by, dict)
                    else "ASC"
                )

                if order_column in result_columns:
                    col_index = result_columns.index(order_column)
                    reverse = direction.upper() == "DESC"
                    result_rows.sort(key=lambda x: x[col_index], reverse=reverse)

            # Apply LIMIT if specified
            if limit_n is not None:
                try:
                    limit_n = int(limit_n)
                    if limit_n >= 0:
                        result_rows = result_rows[:limit_n]
                except (ValueError, TypeError):
                    pass

            return {
                "columns": result_columns,
                "rows": result_rows,
                "status": "success",
                "type": "group_by_result",
            }

        except Exception as e:
            logging.error("Error executing GROUP BY: %s", str(e))
            logging.error(traceback.format_exc())
            return {
                "error": f"Error executing GROUP BY: {str(e)}",
                "status": "error",
                "type": "error",
            }
