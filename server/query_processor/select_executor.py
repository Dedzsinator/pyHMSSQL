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
                            "Detected aggregate function in execute_select: %s(%s)",
                            func_name, col_name
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
                logging.debug("TOP %s specified", top_n)
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
                    if (val.startswith("'") and val.endswith("'")) or\
                        (val.startswith('"') and val.endswith('"')):
                        val = val[1:-1]  # Clean value

                    logging.info("Checking for index on column: %s with value: %s", col, val)

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
                    "Using case-corrected table name: %s instead of %s",
                    actual_table_name,
                    table_name
                )
            elif table_name not in tables:
                return {
                    "error": f"Table '{table_name}' does not exist in database '{db_name}'",
                    "status": "error",
                    "type": "error",
                }

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
                        logging.info("Will use index %s for condition %s=%s",
                                        idx_name, index_column, index_value
                                    )
                        break

            # Build condition for query
            conditions = []
            condition = plan.get("condition")
            if condition:
                logging.debug("Parsing condition: %s", condition)
                conditions = ConditionParser.parse_condition_to_list(condition)

                # ENHANCEMENT: Handle string values correctly
                for cond in conditions:
                    val = cond.get("value")
                    if isinstance(val, str):
                        if (val.startswith("'") and val.endswith("'")) or\
                            (val.startswith('"') and val.endswith('"')):
                            cond["value"] = val[1:-1]  # Remove quotes
                            logging.info("Removed quotes from condition value: %s", cond['value'])

                logging.debug("Parsed conditions: %s", conditions)

            if index_info:
                logging.info("Using INDEX_SCAN with index %s on column %s",
                                index_info['name'], index_info['column']
                            )
            else:
                logging.info("Using FULL_SCAN (no suitable index found)")

            # Query the data using catalog manager - always get all columns first
            results = self.catalog_manager.query_with_condition(
                actual_table_name, conditions, ["*"]
            )

            logging.debug("Query returned %s results", len(results) if results else 0)

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
                logging.debug("Applying ORDER BY: %s", order_by)

                # Get the column name from the order_by plan
                order_column = order_by.get("column")
                direction = order_by.get("direction", "ASC")

                # Define a key function that properly handles dictionary values
                def get_sort_key(record):
                    # Find the correct column with case-insensitive matching
                    for record_col in record:
                        col_name = record_col
                        # Handle dictionary column name format
                        if isinstance(record_col, dict) and "name" in record_col:
                            col_name = record_col["name"]

                        if isinstance(col_name, str) and isinstance(order_column, str) and col_name.lower() == order_column.lower():
                            # Get the value, handling dictionary format if needed
                            value = record.get(record_col)
                            if isinstance(value, dict) and "value" in value:
                                return value["value"]
                            return value
                    return None

                # Sort the results using our key function
                reverse = direction.upper() == "DESC"
                results.sort(key=get_sort_key, reverse=reverse)
                logging.debug("Results sorted by %s %s",order_column, direction)

            # Apply TOP N
            if top_n is not None and top_n > 0 and results:
                results = results[:top_n]
                logging.debug("Applied TOP %s, now %s results", top_n, len(results))

            # Apply LIMIT if specified
            limit = plan.get("limit")
            if limit is not None and isinstance(limit, int) and results and limit > 0:
                results = results[:limit]
                logging.debug("Applied LIMIT %s, now %s results", limit, len(results))

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
                    logging.error("SELECT * will return these columns: %s", result_columns)

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

            logging.error("Final result: %s rows with columns: %s",
                            len(result_rows), result_columns
                        )
            # Debug print first row to verify data
            if result_rows:
                logging.error("First row data: %s", result_rows[0])

            # ENHANCEMENT: Add scan_type and index_used to the result
            return {
                "columns": result_columns,  # Original case preserved
                "rows": result_rows,
                "status": "success",
                "type": "select_result",
                "scan_type": "INDEX_SCAN" if index_info else "FULL_SCAN",
                "index_used": index_info["name"] if index_info else None
            }

        except RuntimeError as e:
            import traceback

            logging.error("Error executing SELECT: %s", str(e))
            logging.error(traceback.format_exc())
            return {
                "error": f"Error executing SELECT: {str(e)}",
                "status": "error",
                "type": "error",
            }
