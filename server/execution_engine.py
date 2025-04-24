"""_summary_

Returns:
    _type_: _description_
"""
import logging
from query_processor.join_executor import JoinExecutor
from query_processor.aggregate_executor import AggregateExecutor
from query_processor.select_executor import SelectExecutor
from query_processor.dml_executor import DMLExecutor
from ddl_processor.schema_manager import SchemaManager
from ddl_processor.view_manager import ViewManager
from transaction.transaction_manager import TransactionManager
from parsers.condition_parser import ConditionParser
from utils.visualizer import Visualizer
from utils.sql_helpers import parse_simple_condition, check_database_selected

class ExecutionEngine:
    """Main execution engine that coordinates between different modules"""

    def __init__(self, catalog_manager, index_manager):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager
        self.current_database = catalog_manager.get_current_database()
        self.preferences = self.catalog_manager.get_preferences()

        # Initialize all sub-modules
        self.join_executor = JoinExecutor(catalog_manager, index_manager)
        self.aggregate_executor = AggregateExecutor(catalog_manager)
        self.select_executor = SelectExecutor(
            catalog_manager, self.join_executor, self.aggregate_executor
        )

        self.select_executor.execution_engine = self

        self.select_executor.execution_engine = self
        self.dml_executor = DMLExecutor(catalog_manager, index_manager)
        self.schema_manager = SchemaManager(catalog_manager)
        self.view_manager = ViewManager(catalog_manager)
        self.transaction_manager = TransactionManager(catalog_manager)
        self.visualizer = Visualizer(catalog_manager, index_manager)

        # Share the condition parser with all modules that need it
        self.condition_parser = ConditionParser()

        # Set the condition parser for each executor that needs it
        self.select_executor.condition_parser = self.condition_parser
        self.dml_executor.condition_parser = self.condition_parser
        self.join_executor.condition_parser = self.condition_parser

    def execute_distinct(self, plan):
        """
        Execute a DISTINCT query.
        """
        table_name = plan["table"]
        column = plan["column"]

        error = check_database_selected(self.catalog_manager)
        if error:
            return error

        # Get the database name after confirming it exists
        db_name = self.catalog_manager.get_current_database()

        # Verify the table exists
        if not self.catalog_manager.list_tables(db_name) or \
            table_name not in self.catalog_manager.list_tables(db_name):
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

    def execute_create_index(self, plan):
        """Create an index on a table column"""
        # Forward to schema_manager with the correct parameters
        return self.schema_manager.execute_create_index(plan)

    def execute_drop_index(self, plan):
        """Drop an index"""
        # Forward to schema_manager with the correct parameters
        return self.schema_manager.execute_drop_index(plan)

    def execute_visualize_index(self, plan):
        """Visualize index structure"""
        index_name = plan.get("index_name")
        table_name = plan.get("table")

        # Use the index_manager directly instead of through schema_manager
        index_manager = self.index_manager

        if not index_manager:
            return {"error": "Index manager not available", "status": "error"}

        # If specific index is requested
        if index_name and table_name:
            full_index_name = f"{table_name}.{index_name}"
            index = index_manager.get_index(full_index_name)

            if not index:
                return {
                    "error": f"Index '{full_index_name}' not found",
                    "status": "error",
                }

            # Visualize the index
            try:
                index.visualize(self.visualizer, output_name=full_index_name)
                return {
                    "message": f"Visualized index '{index_name}' on table '{table_name}'",
                    "status": "success",
                }
            except RuntimeError as e:
                return {
                    "error": f"Error visualizing index: {str(e)}",
                    "status": "error",
                }
        else:
            # Visualize all indexes
            try:
                count = index_manager.visualize_all_indexes()
                return {"message": f"Visualized {count} indexes", "status": "success"}
            except RuntimeError as e:
                return {
                    "error": f"Error visualizing indexes: {str(e)}",
                    "status": "error",
                }

    def execute_set_operation(self, plan):
        """Execute a set operation (UNION, INTERSECT, EXCEPT)."""
        operation = plan.get("type")
        left_plan = plan.get("left")
        right_plan = plan.get("right")

        logging.info(f"Executing {operation} operation")
        logging.info(f"Left plan: {left_plan}")
        logging.info(f"Right plan: {right_plan}")

        # Execute both sides
        left_result = self.execute(left_plan)
        right_result = self.execute(right_plan)

        # Check for errors
        if left_result.get("status") == "error":
            return left_result
        if right_result.get("status") == "error":
            return right_result

        logging.info(f"Left result: {left_result}")
        logging.info(f"Right result: {right_result}")

        # Get column names from the left result
        columns = left_result.get("columns", [])

        # Get rows data - handle different result formats
        left_rows = []
        right_rows = []

        # Extract rows from data or rows format
        if "data" in left_result:
            left_data = left_result["data"]
            left_rows = [list(row.values()) for row in left_data]
        elif "rows" in left_result:
            left_rows = left_result["rows"]

        if "data" in right_result:
            right_data = right_result["data"]
            right_rows = [list(row.values()) for row in right_data]
        elif "rows" in right_result:
            right_rows = right_result["rows"]

        # Perform the set operation
        result_rows = []

        if operation == "UNION":
            # Use a set to eliminate duplicates (convert rows to tuples for hashing)
            seen = set()
            for row in left_rows + right_rows:
                row_tuple = tuple(row)
                if row_tuple not in seen:
                    seen.add(row_tuple)
                    result_rows.append(row)

        elif operation == "INTERSECT":
            # Find rows that exist in both sets
            left_tuples = [tuple(row) for row in left_rows]
            right_tuples = [tuple(row) for row in right_rows]
            common_tuples = set(left_tuples).intersection(set(right_tuples))

            # Convert back to rows
            for tuple_row in common_tuples:
                result_rows.append(list(tuple_row))

        elif operation == "EXCEPT":
            # Find rows in left that aren't in right
            left_tuples = [tuple(row) for row in left_rows]
            right_tuples = [tuple(row) for row in right_rows]
            diff_tuples = set(left_tuples).difference(set(right_tuples))

            # Convert back to rows
            for tuple_row in diff_tuples:
                result_rows.append(list(tuple_row))

        # Return in the expected format for SELECT results
        return {
            "columns": columns,
            "rows": result_rows,
            "status": "success",
            "type": f"{operation.lower()}_result",
            "rowCount": len(result_rows)
        }

    def _has_subquery(self, condition):
        """Check if a condition contains a subquery."""
        if isinstance(condition, dict):
            if "subquery" in condition:
                return True
            for value in condition.values():
                if isinstance(value, dict) and self._has_subquery(value):
                    return True
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict) and self._has_subquery(item):
                            return True
        return False

    def _resolve_subqueries(self, condition):
        """Resolve any subqueries in the condition and replace with results."""
        if not isinstance(condition, dict):
            return

        if "subquery" in condition:
            # Execute the subquery
            subquery_plan = condition["subquery"]

            # IMPORTANT: Fix table case in subquery plan
            if "tables" in subquery_plan and subquery_plan["tables"]:
                db_name = self.catalog_manager.get_current_database()
                tables = self.catalog_manager.list_tables(db_name)
                tables_lower = {table.lower(): table for table in tables}

                # Fix table case sensitivity issues
                for i, table in enumerate(subquery_plan["tables"]):
                    if table.lower() in tables_lower:
                        subquery_plan["tables"][i] = tables_lower[table.lower()]
                        if "table" in subquery_plan and subquery_plan["table"].lower() == table.lower():
                            subquery_plan["table"] = tables_lower[table.lower()]

            # Log subquery execution
            logging.info(f"Executing subquery: {subquery_plan}")

            # Execute the subquery to get results
            subquery_result = self.execute(subquery_plan)

            # Log the result for debugging
            logging.info(f"Subquery result: {subquery_result}")

            # Extract values from the result
            if subquery_result.get("status") == "success":
                # For IN conditions, we need a list of values
                if condition.get("operator") == "IN":
                    values = []

                    # Handle different result formats
                    if "rows" in subquery_result and subquery_result["rows"]:
                        # Rows format (list of lists)
                        for row in subquery_result["rows"]:
                            if row and len(row) > 0:
                                values.append(row[0])  # Take first column

                    # Check for columns/data format
                    elif "data" in subquery_result and subquery_result["data"]:
                        # Extract column name from first record
                        if len(subquery_result["data"]) > 0:
                            first_record = subquery_result["data"][0]
                            if first_record and len(first_record) > 0:
                                # Get the first column name
                                first_col = list(first_record.keys())[0]
                                # Extract values
                                values = [record[first_col] for record in subquery_result["data"]]

                    # Alternative format for select_result
                    elif "columns" in subquery_result and "rows" in subquery_result:
                        for row in subquery_result["rows"]:
                            if row and len(row) > 0:
                                values.append(row[0])  # Take first column

                    # Log extracted values
                    logging.info(f"Extracted subquery values: {values}")

                    # Replace subquery with values
                    condition["values"] = values
                    condition.pop("subquery")  # Remove the subquery

        # Recursively process nested conditions
        for key, value in condition.items():
            if isinstance(value, dict):
                self._resolve_subqueries(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        self._resolve_subqueries(item)

    def _evaluate_condition(self, record, condition):
        """Evaluate a complex condition against a record."""
        if not condition:
            return True

        logging.debug(f"Evaluating condition: {condition}")
        logging.debug(f"Against record: {record}")

        if "operator" in condition:
            op = condition["operator"].upper()

            # Logical operators
            if op == "AND":
                operands = condition.get("operands", [])
                return all(self._evaluate_condition(record, operand) for operand in operands)

            elif op == "OR":
                operands = condition.get("operands", [])
                return any(self._evaluate_condition(record, operand) for operand in operands)

            elif op == "NOT":
                operand = condition.get("operand")
                return not self._evaluate_condition(record, operand)

            # Comparison operators
            elif op in ["=", "<>", "!=", ">", "<", ">=", "<="]:
                column = condition.get("column")
                value = condition.get("value")

                # Case-insensitive column lookup
                record_value = None
                column_found = False

                # Try exact match first
                if column in record:
                    record_value = record[column]
                    column_found = True
                else:
                    # Try case-insensitive match
                    for record_col in record:
                        if isinstance(column, str) and isinstance(record_col, str) and column.lower() == record_col.lower():
                            record_value = record[record_col]
                            column_found = True
                            break

                if column_found:
                    # Handle string values - remove quotes
                    if isinstance(value, str) and (value.startswith("'") and value.endswith("'")) or \
                    (value.startswith('"') and value.endswith('"')):
                        value = value[1:-1]  # Remove quotes

                    # Convert types for comparison
                    try:
                        if isinstance(record_value, (int, float)) and isinstance(value, str):
                            value = float(value) if '.' in value else int(value)
                    except ValueError:
                        pass  # Keep original value if conversion fails

                    # Compare based on operator
                    if op == "=":
                        return record_value == value
                    elif op in ["<>", "!="]:
                        return record_value != value
                    elif op == ">":
                        return record_value > value
                    elif op == "<":
                        return record_value < value
                    elif op == ">=":
                        return record_value >= value
                    elif op == "<=":
                        return record_value <= value

            # IN operator with subquery or value list
            elif op == "IN":
                column = condition.get("column")
                values = condition.get("values", [])

                # Case-insensitive column lookup
                record_value = None
                column_found = False

                # Try exact match first
                if column in record:
                    record_value = record[column]
                    column_found = True
                else:
                    # Try case-insensitive match
                    for record_col in record:
                        if isinstance(column, str) and isinstance(record_col, str) and column.lower() == record_col.lower():
                            record_value = record[record_col]
                            column_found = True
                            break

                if column_found:
                    # For numeric comparisons, ensure types match
                    if isinstance(record_value, (int, float)):
                        # Convert string values to numeric if needed
                        numeric_values = []
                        for val in values:
                            if isinstance(val, str) and val.isdigit():
                                numeric_values.append(int(val))
                            elif isinstance(val, str) and '.' in val:
                                try:
                                    numeric_values.append(float(val))
                                except ValueError:
                                    numeric_values.append(val)
                            else:
                                numeric_values.append(val)

                        logging.debug(f"Checking if {record_value} IN {numeric_values}")
                        return record_value in numeric_values
                    else:
                        logging.debug(f"Checking if {record_value} IN {values}")
                        return record_value in values

        # If we can't evaluate the condition, default to True
        logging.debug("Couldn't evaluate condition, defaulting to True")
        return True

    def execute(self, plan):
        """Execute the query plan by dispatching to appropriate module."""
        plan_type = plan.get("type")
        transaction_id = plan.get("transaction_id")

        if "parsed_condition" in plan and self._has_subquery(plan["parsed_condition"]):
            self._resolve_subqueries(plan["parsed_condition"])

        if plan_type in ["UNION", "INTERSECT", "EXCEPT"]:
            return self.execute_set_operation(plan)

        # Handle transaction control operations first
        if plan_type == "BEGIN_TRANSACTION":
            return self.transaction_manager.execute_transaction_operation("BEGIN_TRANSACTION")
        elif plan_type == "COMMIT":
            return self.transaction_manager.execute_transaction_operation("COMMIT", transaction_id)
        elif plan_type == "ROLLBACK":
            return self.transaction_manager.execute_transaction_operation("ROLLBACK", transaction_id)

        try:
            result = None

            # Consolidated SQL query operations - each operation executed only ONCE
            if plan_type == "SELECT":
                result = self.select_executor.execute_select(plan)
            elif plan_type == "AGGREGATE":
                result = self.aggregate_executor.execute_aggregate(plan)
            elif plan_type == "JOIN":
                result = self.join_executor.execute_join(plan)
            # DML operations
            elif plan_type == "INSERT":
                result = self.dml_executor.execute_insert(plan)

                # Consolidated transaction handling for INSERT
                if transaction_id:
                    # Extract record information from the plan or result
                    record = plan.get("record", {})
                    if not record and plan.get("columns") and plan.get("values"):
                        record = {
                            col: val for col, val in zip(plan.get("columns", []),
                                                    plan.get("values", [[]])[0] if plan.get("values") else [])
                        }

                    # Get record ID from various possible sources
                    record_id = None
                    if "id" in record:
                        record_id = record["id"]
                    elif plan.get("values") and plan.get("values")[0] and \
                        len(plan.get("values")[0]) > 0:
                        record_id = plan.get("values")[0][0]

                    # Record the operation
                    self.transaction_manager.record_operation(transaction_id, {
                        "type": "INSERT",
                        "table": plan.get("table"),
                        "record": record,
                        "record_id": record_id
                    })
            elif plan_type == "UPDATE":
                # Get existing record for rollback if in a transaction
                existing_records = []  # Initialize to empty list
                if transaction_id:
                    table = plan.get("table")
                    condition = plan.get("condition")
                    if condition and table:
                        # Parse condition and get record before update
                        conditions = parse_simple_condition(condition)
                        existing_records = self.catalog_manager.query_with_condition(table, conditions)

                # Execute update
                result = self.dml_executor.execute_update(plan)

                # Record operation if successful and in a transaction
                if transaction_id and result["status"] == "success" and existing_records and \
                    len(existing_records) > 0:
                    for record in existing_records:
                        self.transaction_manager.record_operation(transaction_id, {
                            "type": "UPDATE",
                            "table": plan.get("table"),
                            "record_id": record.get("id"),
                            "old_values": record,
                            "updates": plan.get("set", {})
                        })

            elif plan_type == "DELETE":
                # Get existing record for rollback if in a transaction
                existing_records = []  # Initialize to empty list
                if transaction_id:
                    table = plan.get("table")
                    condition = plan.get("condition")
                    if condition and table:
                        # Use helper function instead of duplicating parsing code
                        conditions = parse_simple_condition(condition)
                        existing_records = self.catalog_manager.query_with_condition(table, conditions)

                # Execute delete
                result = self.dml_executor.execute_delete(plan)

                # Record operation if successful and in a transaction
                if transaction_id and result["status"] == "success" and existing_records and \
                    len(existing_records) > 0:
                    for record in existing_records:
                        self.transaction_manager.record_operation(transaction_id, {
                            "type": "DELETE",
                            "table": plan.get("table"),
                            "record": record
                        })

            # DDL operations
            elif plan_type in ["CREATE_TABLE", "DROP_TABLE"]:
                result = self.schema_manager.execute_table_operation(plan)
            elif plan_type in ["CREATE_DATABASE", "DROP_DATABASE", "USE_DATABASE"]:
                result = self.schema_manager.execute_database_operation(plan)
            elif plan_type == "CREATE_INDEX":
                result = self.execute_create_index(plan)
            elif plan_type == "DROP_INDEX":
                result = self.execute_drop_index(plan)
            elif plan_type in ["CREATE_VIEW", "DROP_VIEW"]:
                result = self.view_manager.execute_view_operation(plan)

            # Utility operations
            elif plan_type == "SHOW":
                result = self.schema_manager.execute_show_operation(plan)
            elif plan_type == "SET":
                result = self.execute_set_preference(plan)
            elif plan_type == "VISUALIZE":
                result = self.execute_visualize_index(plan)
            elif plan_type == "JOIN":
                result = self.join_executor.execute_join(plan)

            else:
                result = {
                    "error": f"Unsupported operation type: {plan_type}",
                    "status": "error",
                }

            if isinstance(result, dict) and "type" not in result:
                result["type"] = f"{plan_type.lower()}_result"

            return result

        except RuntimeError as e:
            logging.error("Error executing %s: %s", plan_type, str(e))
            return {"status": "error", "error": f"Error executing {plan_type}: {str(e)}"}

    def execute_set_preference(self, plan):
        """Update user preferences."""
        preference = plan["preference"]
        value = plan["value"]
        user_id = plan.get("user_id")

        # Update preferences
        self.preferences[preference] = value
        self.catalog_manager.update_preferences({preference: value}, user_id)

        return {
            "message": f"Preference '{preference}' set to '{value}'.",
            "status": "success",
        }
