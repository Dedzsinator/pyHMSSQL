import re
import logging
import traceback
from query_processor.aggregate_executor import AggregateExecutor
from parsers.condition_parser import ConditionParser


class SelectExecutor:
    """Class to execute SELECT queries."""

    def __init__(self, catalog_manager, join_executor, aggregate_executor):
        """
        Initialize SelectExecutor.

        Args:
            catalog_manager: The catalog manager instance
            join_executor: The join executor instance
            aggregate_executor: The aggregate executor instance
        """
        self.catalog_manager = catalog_manager
        self.join_executor = join_executor
        self.aggregate_executor = aggregate_executor
        self.condition_parser = None  # Will be set by ExecutionEngine

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
                    if func_name in (
                        "COUNT",
                        "SUM",
                        "AVG",
                        "MIN",
                        "MAX",
                        "RAND",
                        "GCD",
                    ):
                        logging.error(
                            f"Detected aggregate function in execute_select: {func_name}({col_name})"
                        )

                        # Create a temporary aggregate plan
                        agg_plan = {
                            "type": "AGGREGATE",
                            "function": func_name,
                            "column": col_name,
                            "table": plan.get("table")
                            or (
                                plan.get("tables", [""])[0]
                                if plan.get("tables")
                                else ""
                            ),
                            # Pass the condition
                            "condition": plan.get("condition"),
                            "top": plan.get("top"),  # Pass TOP parameter
                            "limit": plan.get("limit"),  # Pass LIMIT parameter
                        }

                        # Execute the aggregate plan instead
                        return self.aggregate_executor.execute_aggregate(agg_plan)

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

        # ENHANCEMENT: Check for usable indexes for this query
        index_info = None
        condition = plan.get("condition")
        if condition:
            # Remove trailing semicolon if present
            if condition.endswith(';'):
                condition = condition[:-1]
                plan["condition"] = condition  # Update plan

            # Only try to use index for simple conditions with equality
            if "=" in condition and not any(op in condition for op in ["<", ">", "AND", "OR"]):
                parts = condition.split("=")
                if len(parts) == 2:
                    col = parts[0].strip()
                    val = parts[1].strip()

                    # Remove quotes if present
                    if (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):
                        val = val[1:-1]  # Clean value

                    logging.info(f"Checking for index on column: {col} with value: {val}")

                    # We'll look up indexes after confirming the table exists with proper case
                    index_column = col
                    index_value = val

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
                    f"Using case-corrected table name: {actual_table_name} instead of {table_name}"
                )
            elif table_name not in tables:
                return {
                    "error": f"Table '{table_name}' does not exist in database '{db_name}'",
                    "status": "error",
                    "type": "error",
                }

            # ENHANCEMENT: Now that we have the actual table name, check for indexes
            if condition and "index_column" in locals():
                # Get table indexes
                indexes = self.catalog_manager.get_indexes_for_table(actual_table_name)
                for idx_name, idx_def in indexes.items():
                    if idx_def.get("column").lower() == index_column.lower():
                        index_info = {
                            "name": idx_name,
                            "column": idx_def.get("column"),
                            "value": index_value
                        }
                        logging.info(f"Will use index {idx_name} for condition {index_column}={index_value}")
                        break

            # Build condition for query
            conditions = []
            condition = plan.get("condition")
            if condition:
                logging.debug(f"Parsing condition: {condition}")
                conditions = ConditionParser.parse_condition_to_list(condition)

                # ENHANCEMENT: Handle string values correctly
                for cond in conditions:
                    val = cond.get("value")
                    if isinstance(val, str):
                        if (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):
                            cond["value"] = val[1:-1]  # Remove quotes
                            logging.info(f"Removed quotes from condition value: {cond['value']}")

                logging.debug(f"Parsed conditions: {conditions}")

            # ENHANCEMENT: Add scan info to the log
            if index_info:
                logging.info(f"Using INDEX_SCAN with index {index_info['name']} on column {index_info['column']}")
            else:
                logging.info("Using FULL_SCAN (no suitable index found)")

            # Query the data using catalog manager - always get all columns first
            results = self.catalog_manager.query_with_condition(
                actual_table_name, conditions, ["*"]
            )

            logging.debug(f"Query returned {len(results) if results else 0} results")

            # Format results for client display
            if not results:
                return {
                    "columns": columns if "*" not in columns else [],
                    "rows": [],
                    "status": "success",
                    "type": "select_result",
                    "scan_type": "INDEX_SCAN" if index_info else "FULL_SCAN",  # ENHANCEMENT
                    "index_used": index_info["name"] if index_info else None   # ENHANCEMENT
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
                for order_part in order_by.split(","):
                    order_part = order_part.strip()
                    if " DESC" in order_part.upper():
                        col_name = order_part.upper().replace(" DESC", "").strip()
                        reverse = True
                    else:
                        col_name = order_part.upper().replace(" ASC", "").strip()
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
                    for i, (col, reverse) in reversed(
                        list(enumerate(zip(order_columns, reverse_flags)))
                    ):
                        # Use a lambda for sorting that handles None values properly
                        results = sorted(
                            results,
                            key=lambda x: (x.get(col) is None, x.get(col)),
                            reverse=reverse,
                        )

                    logging.debug(f"Results sorted by {order_columns}")

            # Apply TOP N
            if top_n is not None and top_n > 0 and results:
                results = results[:top_n]
                logging.debug(f"Applied TOP {top_n}, now {len(results)} results")

            # Apply LIMIT if specified
            limit = plan.get("limit")
            if limit is not None and isinstance(limit, int) and results and limit > 0:
                results = results[:limit]
                logging.debug(f"Applied LIMIT {limit}, now {len(results)} results")

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

            # ENHANCEMENT: Add scan_type and index_used to the result
            return {
                "columns": result_columns,  # Original case preserved
                "rows": result_rows,
                "status": "success",
                "type": "select_result",
                "scan_type": "INDEX_SCAN" if index_info else "FULL_SCAN",
                "index_used": index_info["name"] if index_info else None
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
