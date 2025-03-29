import logging
import re
import os


class Planner:
    def __init__(self, catalog_manager, index_manager):
        """
        Initialize the query planner.
        """
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager

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
        logging.info(f"EXECUTION PLAN - {plan_type}")
        logging.info("=============================================")
        for step in query_steps:
            logging.info(step)
        logging.info("=============================================")

        # Log the plan objects for debugging
        logging.debug(f"Initial plan: {plan}")

        # Apply optimizations (placeholder for now)
        optimized_plan = plan
        logging.debug(f"Optimized plan: {optimized_plan}")

        return optimized_plan

    def plan_query(self, parsed_query):
        """
        Generate an execution plan from the parsed query.
        """
        plan = None

        if parsed_query["type"] == "SELECT":
            plan = self.plan_select(parsed_query)
        elif parsed_query["type"] == "INSERT":
            plan = self.plan_insert(parsed_query)
        elif parsed_query["type"] == "UPDATE":
            plan = self.plan_update(parsed_query)
        elif parsed_query["type"] == "DELETE":
            plan = self.plan_delete(parsed_query)
        elif parsed_query["type"] == "CREATE":
            # Check what type of CREATE statement this is
            create_type = parsed_query.get("create_type")
            if create_type == "TABLE":
                plan = self.plan_create_table(parsed_query)
            elif create_type == "DATABASE":
                plan = self.plan_create_database(parsed_query)
            elif create_type == "INDEX":
                plan = self.plan_create_index(parsed_query)
            else:
                raise ValueError("Unsupported CREATE statement type.")
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
        else:
            raise ValueError("Unsupported query type.")

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

    def plan_use_database(self, parsed_query):
        """
        Plan for USE DATABASE queries.
        Example: USE database_name
        """
        logging.debug(f"Planning USE DATABASE query: {parsed_query}")
        return {"type": "USE_DATABASE", "database": parsed_query["database"]}

    def plan_create_database(self, parsed_query):
        """
        Plan for CREATE DATABASE queries.
        Example: CREATE DATABASE dbname
        """
        logging.debug(f"Planning CREATE DATABASE query: {parsed_query}")
        return {"type": "CREATE_DATABASE", "database": parsed_query["database"]}

    def plan_drop_database(self, parsed_query):
        """
        Plan for DROP DATABASE queries.
        Example: DROP DATABASE dbname
        """
        logging.debug(f"Planning DROP DATABASE query: {parsed_query}")
        return {"type": "DROP_DATABASE", "database": parsed_query["database"]}

    def plan_create_index(self, parsed_query):
        """
        Plan for CREATE INDEX queries.
        Example: CREATE INDEX idx_name ON table_name (column_name)
        """
        logging.debug(f"Planning CREATE INDEX query: {parsed_query}")

        # Extract data from the parsed query
        index_name = parsed_query.get("index")
        table_name = parsed_query.get("table")
        column_name = parsed_query.get("column")
        is_unique = parsed_query.get("unique", False)

        return {
            "type": "CREATE_INDEX",
            "index_name": index_name,
            "table": table_name,
            "column": column_name,
            "unique": is_unique,
        }

    def plan_drop_index(self, parsed_query):
        """
        Plan for DROP INDEX queries.
        Example: DROP INDEX idx_name ON table_name
        """
        logging.debug(f"Planning DROP INDEX query: {parsed_query}")

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
        logging.debug(f"Planning SHOW query: {parsed_query}")
        return {
            "type": "SHOW",
            "object": parsed_query["object"],
            "table": parsed_query.get("table"),
        }

    def plan_select(self, parsed_query):
        """
        Plan for SELECT queries.
        """
        tables = parsed_query.get("tables", [])

        # If there are multiple tables or a join condition, this is a JOIN query
        if len(tables) > 1 or parsed_query.get("join_condition"):
            return self.plan_join(parsed_query)

        table = tables[0] if tables else None

        # Extract condition (WHERE clause)
        condition = parsed_query.get("condition")

        # Extract ORDER BY clause
        order_by = parsed_query.get("order_by")

        # Extract LIMIT clause
        limit = parsed_query.get("limit")

        # Check for aggregation functions in columns
        columns = parsed_query.get("columns", [])
        for col in columns:
            # Check if this is an aggregate function
            if isinstance(col, str) and "(" in col and ")" in col:
                func_match = re.match(r"(\w+)\(([^)]*)\)", col)
                if func_match:
                    func_name = func_match.group(1).upper()
                    if func_name in ("COUNT", "SUM", "AVG", "MIN", "MAX"):
                        # This is an aggregation query
                        col_name = func_match.group(2).strip()
                        if col_name == "*" and func_name != "COUNT":
                            col_name = None  # Only COUNT(*) is valid

                        return {
                            "type": "AGGREGATE",
                            "function": func_name,
                            "column": col_name,
                            "table": table,
                            "condition": condition,
                        }

        # No aggregation, regular SELECT
        return {
            "type": "SELECT",
            "table": table,
            "columns": columns,
            "condition": condition,
            "order_by": order_by,
            "limit": limit,
        }

    def plan_join(self, parsed_query):
        """
        Plan for JOIN queries.
        """
        tables = parsed_query.get("tables", [])
        join_type = parsed_query.get("join_type", "INNER JOIN")
        join_condition = parsed_query.get("join_condition")
        columns = parsed_query.get("columns", [])
        condition = parsed_query.get("condition")

        # Default to hash join strategy
        join_strategy = "HASH_JOIN"
        preferences = self.catalog_manager.get_preferences()
        if preferences and isinstance(preferences, dict):
            if preferences.get("join_strategy") == "sort_merge":
                join_strategy = "SORT_MERGE_JOIN"
            elif preferences.get("join_strategy") == "index":
                join_strategy = "INDEX_JOIN"
            elif preferences.get("join_strategy") == "nested_loop":
                join_strategy = "NESTED_LOOP_JOIN"

        # Determine which tables are being joined
        table1 = tables[0]
        table2 = tables[1] if len(tables) > 1 else None

        # Build the join plan
        join_plan = {
            "type": join_strategy,
            "table1": table1,
            "table2": table2,
            "condition": join_condition,
            "columns": columns,
            "where_condition": condition,
        }

        return join_plan

    def plan_insert(self, parsed_query):
        """
        Plan for INSERT queries.
        """
        logging.debug(f"Planning INSERT query: {parsed_query}")

        table_name = parsed_query["table"]
        columns = parsed_query.get("columns", [])
        values = parsed_query.get("values", [])

        # Build a record from columns and values
        record = {}
        if columns and values and len(values) > 0:
            for i, column in enumerate(columns):
                if i < len(values[0]):
                    value = values[0][i]
                    record[column] = value

        return {
            "type": "INSERT",
            "table": table_name,
            "record": record,
            "columns": columns,
            "values": values,
        }

    def plan_drop_table(self, parsed_query):
        """
        Plan for DROP TABLE queries.
        Example: DROP TABLE table1
        """
        logging.debug(f"Planning DROP TABLE query: {parsed_query}")
        return {"type": "DROP_TABLE", "table": parsed_query["table"]}

    def plan_update(self, parsed_query):
        """
        Plan for UPDATE queries.
        Example: UPDATE table1 SET name = 'Bob' WHERE id = 1
        """
        logging.debug(f"Planning UPDATE query: {parsed_query}")
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
        logging.debug(f"Planning DELETE query: {parsed_query}")
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
        logging.debug(f"Planning CREATE TABLE query: {parsed_query}")
        return {
            "type": "CREATE_TABLE",
            "table": parsed_query["table"],
            "columns": parsed_query["columns"],
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

