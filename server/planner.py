"""_summary_

Raises:
    ValueError: _description_
    ValueError: _description_
    ValueError: _description_

Returns:
    _type_: _description_
"""

import logging
import re
import copy


class Planner:
    """_summary_"""

    def __init__(self, catalog_manager, index_manager):
        """
        Initialize the query planner.
        """
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager
        self.plan_cache = {}

    def log_execution_plan(self, plan):
        """
        Log the execution plan in logical order.
        """
        if not plan:
            return plan

        plan_type = plan.get("type")
        query_steps = []

        # Create a meaningful representation of the plan
        if plan_type == "CREATE_TABLE":
            table = plan.get("table", "")
            columns = plan.get("columns", [])

            query_steps.append(f"CREATE TABLE: {table}")
            if columns:
                if isinstance(columns[0], dict):
                    cols_str = ", ".join(
                        [f"{col.get('name')} {col.get(
                            'type')}" for col in columns]
                    )
                else:
                    cols_str = ", ".join(columns)
                query_steps.append(f"COLUMNS: {cols_str}")

        elif plan_type == "CREATE_INDEX":
            index_name = plan.get("index_name", "")
            table_name = plan.get("table", "")
            column_name = plan.get("column", "")
            is_unique = plan.get("unique", False)

            query_steps.append(
                f"CREATE {'UNIQUE ' if is_unique else ''}INDEX: {index_name}"
            )
            query_steps.append(f"ON: {table_name}({column_name})")

        elif plan_type == "DROP_TABLE":
            table = plan.get("table", "")
            query_steps.append(f"DROP TABLE: {table}")

        elif plan_type == "DROP_INDEX":
            index_name = plan.get("index_name", "")
            table_name = plan.get("table", "")
            query_steps.append(f"DROP INDEX: {index_name} ON {table_name}")

        elif plan_type == "CREATE_DATABASE":
            db_name = plan.get("database", "")
            query_steps.append(f"CREATE DATABASE: {db_name}")

        elif plan_type == "DROP_DATABASE":
            db_name = plan.get("database", "")
            query_steps.append(f"DROP DATABASE: {db_name}")

        elif plan_type == "USE_DATABASE":
            db_name = plan.get("database", "")
            query_steps.append(f"USE DATABASE: {db_name}")

        elif plan_type == "INSERT":
            table = plan.get("table", "")
            columns = plan.get("columns", [])
            values = plan.get("values", [])

            query_steps.append(f"INSERT INTO: {table}")
            if columns:
                cols_str = ", ".join(columns)
                query_steps.append(f"COLUMNS: {cols_str}")

            if values:
                vals_str = str(values[0]) if values else ""
                query_steps.append(f"VALUES: {vals_str}")

        # Log the execution plan steps with a specific logger name
        logging.info("=============================================")
        logging.info("EXECUTION PLAN - %s", plan_type)
        logging.info("=============================================")
        for step in query_steps:
            logging.info(step)
        logging.info("=============================================")

        # Log the plan objects for debugging
        logging.debug("Initial plan: %s", plan)

        # Apply optimizations (placeholder for now)
        optimized_plan = plan
        logging.debug("Optimized plan: %s", optimized_plan)

        return optimized_plan

    def plan_join_query(self, parsed_query):
        """Plan a JOIN query."""
        # Extract join info from the parsed query
        join_info = parsed_query.get("join_info", {})

        table1 = join_info.get("table1", "")
        table2 = join_info.get("table2", "")
        join_type = join_info.get("type", "INNER")
        condition = join_info.get("condition", "")
        join_algorithm = join_info.get("join_algorithm", "HASH")

        # Extract columns to select
        columns = parsed_query.get("columns", ["*"])

        # Create plan
        plan = {
            "type": "JOIN",
            "join_type": join_type,
            "join_algorithm": join_algorithm,  # Use the algorithm from the hint
            "table1": table1,
            "table2": table2,
            "condition": condition,
            "columns": columns
        }

        return plan


    def plan_query(self, parsed_query):
        """
        Plan a query based on its parsed structure.
        """
        # Generate a cache key for common queries
        query_type = parsed_query.get("type")

        # Special handling for DISTINCT queries
        if query_type == "DISTINCT":
            logging.info("Planning DISTINCT query")
            table = parsed_query.get("tables", [""])[0] if parsed_query.get("tables") else ""
            column = parsed_query.get("column", "")

            return {
                "type": "DISTINCT",
                "table": table,
                "column": column
            }

        # Simple key for common query types
        if query_type in ("SELECT", "SHOW"):
            key_parts = [query_type]

            # Add table information for SELECT
            if query_type == "SELECT" and "tables" in parsed_query:
                key_parts.append(",".join(sorted(str(t) for t in parsed_query["tables"])))

                # Add important variations to the cache key
                if "limit" in parsed_query:
                    key_parts.append(f"limit:{parsed_query['limit']}")

                if "order_by" in parsed_query:
                    if isinstance(parsed_query["order_by"], dict):
                        key_parts.append(f"orderby:{parsed_query['order_by'].get('column')}:{parsed_query['order_by'].get('direction', 'ASC')}")
                    else:
                        key_parts.append(f"orderby:{parsed_query['order_by']}")

                if "condition" in parsed_query:
                    # Use a hash to keep the key size reasonable
                    cond_str = str(parsed_query["condition"])
                    key_parts.append(f"cond:{hash(cond_str) & 0xffffffff}")

            # Add object for SHOW
            if query_type == "SHOW" and "object" in parsed_query:
                key_parts.append(str(parsed_query["object"]))

            cache_key = "_".join(key_parts)

            # Check if we have a cached plan template
            if cache_key in self.plan_cache:
                logging.info("Using cached optimized plan for key: %s", cache_key)
                plan = copy.deepcopy(self.plan_cache[cache_key])
                return self.log_execution_plan(plan)

        # DISABLED CACHING FOR NOW
        logging.info("Creating a new optimized plan (caching disabled)")
        plan = None

        # Add diagnostic logging to see what parsed_query contains
        logging.error("Planner received query: %s", parsed_query)

        # Special handling for SELECT queries with aggregate functions
        if parsed_query["type"] == "SELECT":
            columns = parsed_query.get("columns", [])
            if columns:
                col_str = str(columns[0])
                logging.error(
                    "Analyzing first column for aggregation: '%s'", col_str)

                is_aggregate, func_name, col_name = self.detect_aggregate_function(
                    col_str
                )
                if is_aggregate:
                    logging.error(
                        "DETECTED AGGREGATE FUNCTION: %s(%s)", func_name, col_name
                    )

                    # Create an aggregate plan directly
                    table = (
                        parsed_query.get("tables", [""])[0]
                        if parsed_query.get("tables")
                        else ""
                    )
                    condition = parsed_query.get("condition")

                    aggregate_plan = {
                        "type": "AGGREGATE",
                        "function": func_name,
                        "column": col_name,
                        "table": table,
                        "condition": condition,
                    }

                    # Return the plan immediately without further processing
                    return self.log_execution_plan(aggregate_plan)
        elif parsed_query["type"] in ["UNION", "INTERSECT", "EXCEPT"]:
            # Recursively plan both sides of the set operation
            left_plan = self.plan_query(parsed_query.get("left", {}))
            right_plan = self.plan_query(parsed_query.get("right", {}))

            # Create a set operation plan
            plan = {
                "type": parsed_query["type"],  # Preserve the set operation type
                "left": left_plan,
                "right": right_plan,
                "query": parsed_query.get("query", "")
            }
            return self.log_execution_plan(plan)

        try:
            if parsed_query["type"] == "SCRIPT":
                plan = self.plan_script(parsed_query)
            elif parsed_query["type"] == "CREATE_DATABASE":
                return self.plan_create_database(parsed_query)
            elif parsed_query["type"] == "DROP_DATABASE":
                return self.plan_drop_database(parsed_query)
            elif parsed_query["type"] == "DROP_TABLE":
                return self.plan_drop_table(parsed_query)
            elif parsed_query["type"] == "DROP_INDEX":
                plan = self.plan_drop_index(parsed_query)
            elif parsed_query["type"] == "USE_DATABASE":
                return self.plan_use_database(parsed_query)
            # Add direct handlers for CREATE_TABLE and CREATE_INDEX
            elif parsed_query["type"] == "CREATE_TABLE":
                plan = self.plan_create_table(parsed_query)
            elif parsed_query["type"] == "CREATE_INDEX":
                plan = self.plan_create_index(parsed_query)
            elif parsed_query["type"] == "SELECT":
                plan = self.plan_select(parsed_query)
            elif parsed_query["type"] == "INSERT":
                plan = self.plan_insert(parsed_query)
            elif parsed_query["type"] == "UPDATE":
                plan = self.plan_update(parsed_query)
            elif parsed_query["type"] == "DELETE":
                plan = self.plan_delete(parsed_query)
            elif parsed_query["type"] == "CREATE":
                # Existing handling for CREATE with subtypes
                create_type = parsed_query.get("create_type")
                if create_type == "TABLE":
                    plan = self.plan_create_table(parsed_query)
                elif create_type == "DATABASE":
                    plan = self.plan_create_database(parsed_query)
                elif create_type == "INDEX":
                    plan = self.plan_create_index(parsed_query)
            elif parsed_query["type"] == "DROP":
                # Check what type of DROP statement this is
                drop_type = parsed_query.get("drop_type")
                if drop_type == "TABLE":
                    plan = self.plan_drop_table(parsed_query)
                elif drop_type == "DATABASE":
                    plan = self.plan_drop_database(parsed_query)
                elif drop_type == "INDEX":
                    plan = self.plan_drop_index(parsed_query)
                else:
                    raise ValueError("Unsupported DROP statement type.")
            elif parsed_query["type"] == "JOIN":
                plan = self.plan_join(parsed_query)
            elif parsed_query["type"] == "CREATE_VIEW":
                plan = self.plan_create_view(parsed_query)
            elif parsed_query["type"] == "DROP_VIEW":
                plan = self.plan_drop_view(parsed_query)
            elif parsed_query["type"] == "SHOW":
                plan = self.plan_show(parsed_query)
            elif parsed_query["type"] == "USE":
                plan = self.plan_use_database(parsed_query)
            elif parsed_query["type"] == "VISUALIZE":
                return self.plan_visualize(parsed_query)
            elif parsed_query["type"] == "BEGIN_TRANSACTION":
                return self.plan_transaction_operation(parsed_query)
            elif parsed_query["type"] == "COMMIT":
                return self.plan_transaction_operation(parsed_query)
            elif parsed_query["type"] == "ROLLBACK":
                return self.plan_transaction_operation(parsed_query)
            else:
                return {"error": f"Unsupported query type: {parsed_query['type']}"}
        except ValueError as e:
            logging.error("Error planning query: %s", e)
            return {"error": str(e)}

        # Log the execution plan
        return self.log_execution_plan(plan)

    def plan_visualize(self, parsed_query):
        """Plan a visualization query."""
        plan = {"type": "VISUALIZE"}

        object_type = parsed_query.get("object")
        if object_type == "BPTREE":
            plan["object"] = "BPTREE"
            plan["index_name"] = parsed_query.get("index_name")
            plan["table"] = parsed_query.get("table")
        elif object_type == "INDEX":
            plan["object"] = "INDEX"
            plan["index_name"] = parsed_query.get("index_name")
            plan["table"] = parsed_query.get("table")
        else:
            plan["error"] = f"Unknown visualization object: {object_type}"

        return plan
    
    def plan_script(self, parsed_query):
        """Plan for SCRIPT execution."""
        logging.debug("Planning SCRIPT query: %s", parsed_query)
        return {
            "type": "SCRIPT",
            "filename": parsed_query.get("filename")
        }

    def plan_transaction_operation(self, parsed_query):
        """
        Plan for transaction control operations (BEGIN, COMMIT, ROLLBACK).
        """
        logging.debug("Planning transaction operation: %s", parsed_query)

        # Simply pass through the transaction type
        return {
            "type": parsed_query["type"],
            "transaction_id": parsed_query.get("transaction_id")
        }

    def plan_use_database(self, parsed_query):
        """
        Plan for USE DATABASE queries.
        Example: USE database_name
        """
        logging.debug("Planning USE DATABASE query: %s", parsed_query)
        return {"type": "USE_DATABASE", "database": parsed_query["database"]}

    def plan_create_database(self, parsed_query):
        """
        Plan for CREATE DATABASE queries.
        Example: CREATE DATABASE dbname
        """
        logging.debug("Planning CREATE DATABASE query: %s", parsed_query)
        return {"type": "CREATE_DATABASE", "database": parsed_query["database"]}

    def plan_drop_database(self, parsed_query):
        """
        Plan for DROP DATABASE queries.
        Example: DROP DATABASE dbname
        """
        logging.debug("Planning DROP DATABASE query: %sz", parsed_query)
        return {"type": "DROP_DATABASE", "database": parsed_query["database"]}

    def plan_create_index(self, parsed_query):
        """
        Plan for CREATE INDEX queries.
        Example: CREATE INDEX idx_name ON table_name (column_name)
        """
        logging.debug("Planning CREATE INDEX query: %s", parsed_query)

        index_name = parsed_query.get("index_name")
        table_name = parsed_query.get("table")
        
        # Handle both old format (single column) and new format (multiple columns)
        columns = parsed_query.get("columns", [])
        if not columns:
            # Fallback to old single column format
            single_column = parsed_query.get("column")
            if single_column:
                columns = [single_column]
        
        is_unique = parsed_query.get("unique", False)

        return {
            "type": "CREATE_INDEX",
            "index_name": index_name,
            "table": table_name,
            "columns": columns,  # Use the new columns format
            "unique": is_unique,
        }

    def plan_drop_index(self, parsed_query):
        """
        Plan for DROP INDEX queries.
        Example: DROP INDEX idx_name ON table_name
        """
        logging.debug("Planning DROP INDEX query: %s", parsed_query)

        # Extract data from the parsed query
        index_name = parsed_query.get("index")
        table_name = parsed_query.get("table")

        return {"type": "DROP_INDEX", "index_name": index_name, "table": table_name}

    def plan_show(self, parsed_query):
        """
        Plan for SHOW queries.
        Examples:
        - SHOW DATABASES
        - SHOW TABLES
        - SHOW INDEXES FOR table_name
        """
        logging.debug("Planning SHOW query: %s", parsed_query)
        return {
            "type": "SHOW",
            "object": parsed_query["object"],
            "table": parsed_query.get("table"),
        }

    def detect_aggregate_function(self, column_str):
        """
        Helper method to detect if a column contains an aggregate function.
        Returns tuple of (is_aggregate, function_name, column_name) if found.
        """
        if not column_str or column_str == "*":
            return (False, None, None)

        # Add debug logging
        logging.info("Checking for aggregate function in: '%s'", column_str)

        # Directly check for common aggregate function syntax
        for func_name in ["COUNT", "SUM", "AVG", "MIN", "MAX", "RAND", "GCD"]:
            if column_str.upper().startswith(f"{func_name}("):
                # Extract column name between parentheses
                col_name = column_str[len(func_name) + 1:].rstrip(")")
                logging.info("DIRECTLY MATCHED %s(%s)", func_name, col_name)
                return (True, func_name, col_name)

        # No direct match found
        logging.info("No aggregate function detected in: '%s'", column_str)
        return (False, None, None)

    def plan_aggregate(self, parsed_query):
        """
        Plan for aggregate queries (COUNT, SUM, AVG, MIN, MAX)
        """
        logging.error("Creating dedicated AGGREGATE plan")

        # Get the first column which should contain the aggregate function
        columns = parsed_query.get("columns", [])
        if not columns:
            raise ValueError("No columns found for aggregate query")

        col_str = str(columns[0])

        # Check for common aggregate functions with improved regex
        for func_name in ["COUNT", "SUM", "AVG", "MIN", "MAX", "RAND", "GCD"]:
            # Improved regex to properly extract just the column name
            pattern = rf"{func_name}\s*\(\s*([^)]+)\s*\)"
            match = re.search(pattern, col_str, re.IGNORECASE)
            if match:
                column_part = match.group(1).strip()
                
                # Handle special case for COUNT(*)
                if column_part == "*":
                    column_part = "*"
                
                logging.error(f"Direct match! Creating AGGREGATE plan for {func_name}({column_part})")
                
                # Get table from parsed query
                tables = parsed_query.get("tables", [])
                table = tables[0] if tables else None
                
                return {
                    "type": "AGGREGATE",
                    "function": func_name,
                    "column": column_part,  # Use just the column name, not the full expression
                    "table": table,
                    "condition": parsed_query.get("condition"),
                    "top": parsed_query.get("top"),
                    "limit": parsed_query.get("limit"),
                    "no_cache": True,
                }

        # If we get here, there's no valid aggregate function
        return self.plan_select(parsed_query)  # Fallback to regular select

    def plan_select(self, parsed_query):
        """Plan for SELECT queries."""
        tables = parsed_query.get("tables", [])

        # If there are multiple tables or a join condition, this is a JOIN query
        if len(tables) > 1 or parsed_query.get("join_condition"):
            return self.plan_join(parsed_query)

        table = tables[0] if tables else None

        # Extract condition (WHERE clause)
        condition = parsed_query.get("condition")

        group_by = parsed_query.get("group_by")
    
        # Check if this is a GROUP BY query with aggregates
        if group_by:
            return {
                "type": "AGGREGATE_GROUP",
                "table": table,
                "columns": columns,
                "group_by": group_by,
                "condition": condition,
                "order_by": order_by,
                "limit": limit,
                "tables": tables,
                "operation": "AGGREGATE_GROUP"
            }

        # Extract and fix ORDER BY clause
        order_by = parsed_query.get("order_by")

        # Ensure order_by has a consistent structure
        if order_by:
            # Make sure it's a dictionary with column and direction
            if isinstance(order_by, str):
                # Convert string to proper structure
                parts = order_by.strip().split()
                column_name = parts[0]
                direction = "DESC" if len(parts) > 1 and parts[1].upper() == "DESC" else "ASC"
                order_by = {"column": column_name, "direction": direction}
            elif isinstance(order_by, dict) and "column" not in order_by:
                # Handle missing column key
                if "name" in order_by:
                    order_by["column"] = order_by["name"]
                elif "field" in order_by:
                    order_by["column"] = order_by["field"]

            # Log the standardized ORDER BY structure
            logging.info(f"SELECT plan will include ORDER BY: {order_by}")

        # Extract LIMIT clause
        limit = parsed_query.get("limit")
        if limit is not None:
            try:
                # Ensure limit is an integer
                limit = int(limit)
                logging.info(f"SELECT plan will include LIMIT: {limit}")
            except (ValueError, TypeError):
                logging.error(f"Invalid LIMIT value: {limit}")
                limit = None

        # Extract OFFSET clause
        offset = parsed_query.get("offset")

        # Check for aggregation functions in columns
        columns = parsed_query.get("columns", [])

        # Log to ensure LIMIT and ORDER BY are included in the plan
        if limit is not None:
            logging.info(f"SELECT plan will include LIMIT: {limit}")
        if order_by:
            logging.info(f"SELECT plan will include ORDER BY: {order_by}")
        logging.error("Planner checking columns for aggregates: %s", columns)

        for col in columns:
            col_str = str(col) if col is not None else ""
            logging.error("Checking column: %r", col_str)

            # Skip the wildcard character
            if col_str == "*":
                continue

            # Direct string check for common aggregate functions
            for func_name in ["COUNT", "SUM", "AVG", "MIN", "MAX", "RAND", "GCD"]:
                if col_str.upper().startswith(f"{func_name}("):
                    # Extract the column name between parentheses
                    col_name = col_str[len(func_name) + 1:].rstrip(")")
                    logging.error(
                        "Direct match! Creating AGGREGATE plan for %s(%s)",
                        func_name,
                        col_name,
                    )
                    return {
                        "type": "AGGREGATE",
                        "function": func_name,
                        "column": col_name,
                        "table": table,
                        "condition": condition,
                        "top": parsed_query.get("top"),  # Add TOP n support
                        # Add LIMIT support
                        "limit": parsed_query.get("limit"),
                    }

            if "(" in col_str and ")" in col_str:
                logging.error(
                    "Potential aggregate function found: %s", col_str)

                # Try all possible patterns for maximum compatibility
                patterns = [
                    r"(\w+)\(([^)]*)\)",  # Basic: COUNT(*)
                    r"(\w+)\s*\(\s*(.*?)\s*\)",  # With spaces: COUNT( * )
                    # Mixed: COUNT(column_name)
                    r"(\w+)\s*\(\s*([^\s)]+)\s*\)",
                ]

                for pattern in patterns:
                    func_match = re.search(pattern, col_str, re.IGNORECASE)
                    if func_match:
                        func_name = func_match.group(1).upper()
                        logging.error("Extracted function: %s", func_name)

                        if func_name in (
                            "COUNT",
                            "SUM",
                            "AVG",
                            "MIN",
                            "MAX",
                            "RAND",
                            "GCD",
                            "MEDIAN",
                            "STDEV",
                        ):
                            # This is an aggregation query
                            col_name = func_match.group(2).strip()
                            if col_name == "*" and func_name != "COUNT":
                                logging.error(
                                    "Invalid: %s(*) is not allowed", func_name
                                )
                                continue  # Skip this invalid combination

                            logging.error(
                                "Creating AGGREGATE plan for %s(%s)",
                                func_name,
                                col_name,
                            )
                            return {
                                "type": "AGGREGATE",
                                "function": func_name,
                                "column": col_name,
                                "table": table,
                                "condition": condition,
                            }

        # No aggregation found - it's a regular SELECT
        logging.error(
            "No valid aggregate functions found, proceeding with regular SELECT"
        )
        return {
            "type": "SELECT",
            "table": table,
            "columns": columns,
            "condition": condition,
            "order_by": order_by,
            "limit": limit,
            "offset": offset,
            "tables": tables,  # Include tables list for compatibility
            "operation": "SELECT"  # Added to maintain consistency with other code
        }

    def plan_join(self, parsed_query):
        """Plan for JOIN queries with different join types and algorithms."""
        logging.debug("Planning JOIN query: %s", parsed_query)

        # Extract basic join information
        tables = parsed_query.get("tables", [])
        columns = parsed_query.get("columns", [])
        condition = parsed_query.get("condition")

        # Get join-specific information if available
        join_info = parsed_query.get("join_info", {})
        join_type = join_info.get("type", "INNER").upper()
        join_condition = join_info.get("condition") or parsed_query.get("join_condition")

        # IMPORTANT FIX: Extract join_algorithm directly from join_info
        join_algorithm = join_info.get("join_algorithm")

        # Only if no algorithm was specified in join_info, try other methods
        if not join_algorithm:
            # Try to extract from hint
            if "WITH" in parsed_query.get("query", "").upper():
                hint_match = re.search(
                    r"WITH\s*\(\s*JOIN_TYPE\s*=\s*'(\w+)'\s*\)",
                    parsed_query.get("query", ""),
                    re.IGNORECASE,
                )
                if hint_match:
                    join_algorithm = hint_match.group(1).upper()

        # If still no algorithm, set a default
        if not join_algorithm:
            join_algorithm = "HASH"  # Default algorithm

        # Build the join plan
        join_plan = {
            "type": "JOIN",
            "join_type": join_type,
            "join_algorithm": join_algorithm,
            "table1": join_info.get("table1", "") or tables[0] if tables else "",
            "table2": join_info.get("table2", "") or (tables[1] if len(tables) > 1 else ""),
            "condition": join_condition,
            "columns": columns,
            "where_condition": condition,
        }

        return join_plan

    def _choose_join_algorithm(self, tables, join_condition):
        """Choose the best join algorithm based on table statistics and available indexes."""
        # Get current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return "HASH"  # Default

        # If no tables or we can't determine, use hash join
        if not tables or len(tables) < 2:
            return "HASH"

        # Remove alias
        table1 = tables[0].split()[0] if " " in tables[0] else tables[0]
        table2 = tables[1].split()[0] if " " in tables[1] else tables[1]

        # Check if tables exist
        tables_in_db = self.catalog_manager.list_tables(db_name)
        if table1 not in tables_in_db or table2 not in tables_in_db:
            return "HASH"

        # Check if join condition exists
        if not join_condition:
            return "NESTED_LOOP"  # For cross joins, use nested loop

        # Extract column names from join condition (assuming format: col1 = col2)
        column_match = re.search(
            r"(\w+\.\w+)\s*=\s*(\w+\.\w+)", join_condition)
        if not column_match:
            return "HASH"

        left_col = column_match.group(1).split(
            ".")[-1]  # Get column name without table
        right_col = column_match.group(2).split(".")[-1]

        # Check for indexes on join columns
        table1_indexes = self.catalog_manager.get_indexes_for_table(table1)
        table2_indexes = self.catalog_manager.get_indexes_for_table(table2)

        has_index1 = any(
            idx_info.get("column", "").lower() == left_col.lower()
            for idx_info in table1_indexes.values()
        )
        has_index2 = any(
            idx_info.get("column", "").lower() == right_col.lower()
            for idx_info in table2_indexes.values()
        )

        # Use INDEX JOIN if we have an index on either join column
        if has_index1 or has_index2:
            return "INDEX"

        # If tables are small, nested loop might be efficient
        # In a real system, you would use statistics to make this decision
        try:
            table1_data = self.catalog_manager.query_with_condition(table1, [], [
                                                                    "*"])
            table2_data = self.catalog_manager.query_with_condition(table2, [], [
                                                                    "*"])

            table1_size = len(table1_data) if table1_data else 0
            table2_size = len(table2_data) if table2_data else 0

            if table1_size < 100 and table2_size < 100:
                return "NESTED_LOOP"
            elif table1_size < 1000 and table2_size < 1000:
                return "MERGE"
            else:
                return "HASH"
        except RuntimeError:
            # If we can't determine sizes, use hash join as a safe default
            return "HASH"

    def plan_insert(self, parsed_query):
        """
        Plan for INSERT queries with support for multiple rows.
        """
        logging.debug("Planning INSERT query: %s", parsed_query)

        table_name = parsed_query["table"]
        columns = parsed_query.get("columns", [])
        values = parsed_query.get("values", [])

        # Ensure values is a list of lists (for multiple rows)
        if values and not isinstance(values[0], list):
            values = [values]  # Single row, wrap in list

        return {
            "type": "INSERT",
            "table": table_name,
            "columns": columns,
            "values": values,  # This will now contain all rows
        }

    def plan_drop_table(self, parsed_query):
        """
        Plan for DROP TABLE queries.
        Example: DROP TABLE table1
        """
        logging.debug("Planning DROP TABLE query: %s", parsed_query)
        return {"type": "DROP_TABLE", "table": parsed_query["table"]}

    def plan_update(self, parsed_query):
        """
        Plan for UPDATE queries.
        Example: UPDATE table1 SET name = 'Bob' WHERE id = 1
        """
        logging.debug("Planning UPDATE query: %s", parsed_query)
        where_clause = parsed_query.get(
            "condition") or parsed_query.get("where")

        return {
            "type": "UPDATE",
            "table": parsed_query["table"],
            "set": parsed_query["set"],
            "updates": list(
                parsed_query["set"].items()
            ),  # Convert set pairs to update list
            "condition": where_clause,  # Use consistent key name
        }

    def plan_delete(self, parsed_query):
        """
        Plan for DELETE queries.
        Example: DELETE FROM table1 WHERE id = 1
        """
        logging.debug("Planning DELETE query: %s", parsed_query)
        return {
            "type": "DELETE",
            "table": parsed_query["table"],
            "condition": parsed_query.get("where"),
        }

    def plan_create_table(self, parsed_query):
        """
        Plan for CREATE TABLE queries.
        Example: CREATE TABLE table1 (id INT, name VARCHAR)
        """
        logging.debug("Planning CREATE TABLE query: %s", parsed_query)
        return {
            "type": "CREATE_TABLE",
            "table": parsed_query.get("table"),
            "columns": parsed_query.get("columns"),
            "constraints": parsed_query.get("constraints", [])
        }

    def plan_create_view(self, parsed_query):
        """
        Plan for CREATE VIEW queries.
        Example: CREATE VIEW view_name AS SELECT * FROM table
        """
        return {
            "type": "CREATE_VIEW",
            "view_name": parsed_query["view_name"],
            "query": parsed_query["query"],
        }

    def plan_drop_view(self, parsed_query):
        """
        Plan for DROP VIEW queries.
        Example: DROP VIEW view_name
        """
        return {"type": "DROP_VIEW", "view_name": parsed_query["view_name"]}
