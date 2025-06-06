import re
import logging
import traceback
from query_processor.aggregate_executor import AggregateExecutor


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
        self.condition_parser = None  # Deprecated - use SQLGlot instead
        self.execution_engine = None  # Will be set by ExecutionEngine

    def execute_select(self, plan):
        """Execute a SELECT query and return results."""
        # DISABLE CACHING: Force plan to bypass cache
        plan["no_cache"] = True
        # Initial logging
        logging.debug("Executing SELECT with plan: %s", {k: v for k, v in plan.items() if k != 'query'})

        limit_value = plan.get("limit")
        order_by = plan.get("order_by")

        if limit_value is not None:
            logging.info(f"Executing SELECT with LIMIT: {limit_value}")
        else:
            logging.info("Executing SELECT without LIMIT")

        if order_by:
            logging.info(f"Executing SELECT with ORDER BY: {order_by}")
        else:
            logging.info("Executing SELECT without ORDER BY")

        # Process parsed condition and handle subqueries
        if "parsed_condition" in plan and hasattr(self, "execution_engine"):
            # Check if this condition has subqueries
            if self.execution_engine._has_subquery(plan["parsed_condition"]):
                logging.info("Found subquery in condition, resolving...")
                # Resolve subqueries (execute them and replace with actual values)
                self.execution_engine._resolve_subqueries(plan["parsed_condition"])
                logging.info(f"Resolved subquery condition: {plan['parsed_condition']}")

        # Check for aggregate functions in column names
        columns = plan.get("columns", [])
        for _, col in enumerate(columns):
            if isinstance(col, str) and col != "*":
                # Check for standard aggregate functions
                match = re.search(r"(\w+)\s*\(\s*([^)]*)\s*\)", col)
                if match:
                    func_name = match.group(1).upper()
                    col_name = match.group(2).strip()

                    if func_name in ("COUNT", "SUM", "AVG", "MIN", "MAX", "RAND", "GCD"):
                        logging.info("Detected aggregate function: %s(%s)", func_name, col_name)

                        # Create a temporary aggregate plan
                        agg_plan = {
                            "type": "AGGREGATE",
                            "function": func_name,
                            "column": col_name,
                            "table": plan.get("table") or (
                                plan.get("tables", [""])[0] if plan.get("tables") else ""
                            ),
                            "condition": plan.get("condition"),
                            "top": plan.get("top"),
                            "limit": plan.get("limit"),
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

        # Check for index usage
        index_info = None
        condition = plan.get("condition")
        index_column = None
        index_value = None
        
        if condition and isinstance(condition, str):
            # Remove trailing semicolon if present
            if condition.endswith(';'):
                condition = condition[:-1]
                plan["condition"] = condition  # Update plan

            # Check for simple equality conditions
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
                    index_column = col
                    index_value = val
            
            # Check for range conditions (e.g., "age >= 25 AND age <= 30")
            elif any(op in condition for op in [">=", "<=", "<", ">"]) and "AND" in condition:
                # Parse range condition to extract column name
                # Look for patterns like "age >= 25 AND age <= 30"
                range_match = re.search(r'(\w+)\s*[><=]+\s*\d+\s*AND\s*\1\s*[><=]+\s*\d+', condition)
                if range_match:
                    index_column = range_match.group(1)
                    logging.info("Detected range condition on column: %s", index_column)

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
                logging.debug("Using case-corrected table name: %s instead of %s",
                            actual_table_name, table_name)
            elif table_name not in tables:
                return {
                    "error": f"Table '{table_name}' does not exist in database '{db_name}'",
                    "status": "error",
                    "type": "error",
                }

            # Check for index on specified column if condition is present
            if condition and index_column:
                # Get table indexes
                indexes = self.catalog_manager.get_indexes_for_table(actual_table_name)
                logging.info("DEBUG: condition=%s, index_column=%s, indexes=%s", condition, index_column, indexes)
                for idx_name, idx_def in indexes.items():
                    if idx_def.get("column").lower() == index_column.lower():
                        index_info = {
                            "name": idx_name,
                            "column": idx_def.get("column"),
                            "value": index_value
                        }
                        # Determine scan type based on condition
                        if "=" in condition and not any(op in condition for op in ["<", ">", "AND", "OR"]):
                            plan["scan_type"] = "INDEX_SCAN"
                        elif any(op in condition for op in [">=", "<=", "<", ">"]) and "AND" in condition:
                            plan["scan_type"] = "INDEX_RANGE_SCAN"
                        else:
                            plan["scan_type"] = "INDEX_SCAN"
                        
                        logging.info("Will use index %s for condition %s with scan type: %s",
                                    idx_name, index_column, plan.get("scan_type"))
                        break
                else:
                    # No index found for this condition, this will be a full table scan
                    logging.info("DEBUG: Setting scan_type to FULL_SCAN (no index found)")
                    plan["scan_type"] = "FULL_SCAN"
            else:
                # No condition or no indexable column, this will be a full table scan
                logging.info("DEBUG: Setting scan_type to FULL_SCAN (no condition or index_column)")
                plan["scan_type"] = "FULL_SCAN"

            # Check for B+ tree index in buffer pool before creating a new one
            # This step should be done by the execution engine when loading the table data
            # For now, we'll just log that we should be checking here
            logging.debug(f"Should check for cached B+ tree index for table: {actual_table_name}")

            # Fetch and filter the data
            results = []

            if "parsed_condition" in plan and hasattr(self, "execution_engine"):
                logging.info("Using parsed condition for advanced filtering")

                # First get all records (we'll filter them with the complex condition)
                all_records = self.catalog_manager.query_with_condition(
                    actual_table_name, [], ["*"]
                )

                # Apply the parsed condition using execution engine's evaluator
                if all_records:
                    for record in all_records:
                        if self.execution_engine._evaluate_condition(record, plan["parsed_condition"]):
                            results.append(record)

                    logging.info(f"Found {len(results)} matching records after filtering")
            else:
                # Use regular condition parsing for simple conditions
                conditions = []
                if condition:
                    logging.debug("Parsing condition: %s", condition)
                    # Use SQLGlot-based condition parsing
                    try:
                        from utils.sql_helpers import parse_condition_with_sqlglot
                        conditions = parse_condition_with_sqlglot(condition)
                    except ImportError:
                        logging.warning("SQLGlot condition parsing not available, using fallback")
                        from utils.sql_helpers import parse_simple_condition
                        conditions = parse_simple_condition(condition)

                    # Handle string values correctly
                    for cond in conditions:
                        val = cond.get("value")
                        if isinstance(val, str):
                            if (val.startswith("'") and val.endswith("'")) or\
                                (val.startswith('"') and val.endswith('"')):
                                cond["value"] = val[1:-1]  # Remove quotes
                                logging.info("Removed quotes from condition value: %s", cond['value'])

                    logging.debug("Parsed conditions: %s", conditions)

                # Execute the query with simple conditions
                results = self.catalog_manager.query_with_condition(
                    actual_table_name, conditions, ["*"]
                )

            logging.info("Query returned %s results", len(results) if results else 0)

            # Format results for client display
            if not results:
                empty_columns = []
                if "*" not in columns:
                    empty_columns = columns
                return {
                    "columns": empty_columns,
                    "rows": [],
                    "status": "success",
                    "type": "select_result",
                    "scan_type": plan.get("scan_type", "INDEX_SCAN" if index_info else "FULL_SCAN"),
                    "index_used": index_info["name"] if index_info else None
                }

            # Create a mapping of lowercase column names to original case
            column_case_map = {}
            if results:
                first_record = results[0]
                for col_name in first_record:
                    column_case_map[col_name.lower()] = col_name

            # Apply ORDER BY if specified
            if order_by and results:
                logging.info(f"Applying ORDER BY: {order_by}")

                # Handle both string and dict formats for order_by
                if isinstance(order_by, dict):
                    order_column = order_by.get("column")
                    direction = order_by.get("direction", "ASC").upper()
                else:
                    # Handle string format (for backward compatibility)
                    parts = order_by.strip().split()
                    order_column = parts[0]
                    direction = "DESC" if len(parts) > 1 and parts[1].upper() == "DESC" else "ASC"

                if order_column:
                    # Fix for case-insensitive column matching
                    def get_sort_key(record):
                        for col_name in record:
                            if col_name.lower() == order_column.lower():
                                val = record[col_name]
                                # Handle different data types for consistent sorting
                                if val is None:
                                    return ""
                                # Try to convert to numeric if possible for correct sorting
                                if isinstance(val, str):
                                    try:
                                        if '.' in val:
                                            return float(val)
                                        else:
                                            return int(val)
                                    except ValueError:
                                        pass
                                return val
                        return None

                    # Sort with proper direction
                    reverse = direction == "DESC"
                    logging.info(f"Sorting by {order_column} {direction}, reverse={reverse}")
                    results.sort(key=get_sort_key, reverse=reverse)
                    logging.info(f"Results sorted by {order_column} {direction}")

            # Apply LIMIT if specified - ensure it happens after ORDER BY
            if limit_value is not None and results:
                try:
                    limit_int = int(limit_value)
                    if limit_int >= 0:
                        logging.info(f"Applying LIMIT {limit_int} to results")
                        results = results[:limit_int]
                        logging.info(f"Results limited to {len(results)} rows")
                    else:
                        logging.warning(f"Ignoring negative LIMIT value: {limit_value}")
                except (ValueError, TypeError):
                    logging.error(f"Invalid LIMIT value: {limit_value}")
            else:
                logging.info("No LIMIT clause to apply")

            # Apply TOP N
            if top_n is not None and top_n > 0 and results:
                results = results[:top_n]
                logging.debug("Applied TOP %s, now %s results", top_n, len(results))

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
                    logging.info("SELECT * will return these columns: %s", result_columns)

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

            logging.info("Final result: %s rows with columns: %s", len(result_rows), result_columns)
            # Debug print first row to verify data
            if result_rows:
                logging.debug("First row data: %s", result_rows[0])

            # Return the formatted results
            return {
                "columns": result_columns,  # Original case preserved
                "rows": result_rows,
                "status": "success",
                "type": "select_result",
                "scan_type": plan.get("scan_type", "INDEX_SCAN" if index_info else "FULL_SCAN"),
                "index_used": index_info["name"] if index_info else None
            }

        except RuntimeError as e:
            logging.error("Error executing SELECT: %s", str(e))
            logging.error(traceback.format_exc())
            return {
                "error": f"Error executing SELECT: {str(e)}",
                "status": "error",
                "type": "error",
            }