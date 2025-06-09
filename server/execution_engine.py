"""_summary_

Returns:
    _type_: _description_
"""

from query_processor.join_executor import JoinExecutor
from query_processor.aggregate_executor import AggregateExecutor
from query_processor.select_executor import SelectExecutor
from query_processor.dml_executor import DMLExecutor
from query_processor.group_by_executor import GroupByExecutor
from ddl_processor.schema_manager import SchemaManager
from ddl_processor.view_manager import ViewManager
from ddl_processor.procedure_manager import ProcedureManager
from ddl_processor.function_manager import FunctionManager
from ddl_processor.trigger_manager import TriggerManager
from ddl_processor.temp_table_manager import TemporaryTableManager
from transaction.transaction_manager import TransactionManager
from parsers.condition_parser import ConditionParser
from utils.visualizer import Visualizer
from utils.sql_helpers import parse_simple_condition, check_database_selected
import logging
import traceback


class ExecutionEngine:
    """Main execution engine that coordinates between different modules"""

    def __init__(self, catalog_manager, index_manager, planner):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager
        self.planner = planner
        self.current_database = catalog_manager.get_current_database()
        self.preferences = self.catalog_manager.get_preferences()

        # Initialize all sub-modules
        self.join_executor = JoinExecutor(catalog_manager, index_manager)
        self.aggregate_executor = AggregateExecutor(catalog_manager)
        self.select_executor = SelectExecutor(
            catalog_manager, self.join_executor, self.aggregate_executor
        )
        self.group_by_executor = GroupByExecutor(catalog_manager)

        # Set execution_engine reference only once (removing duplicate)
        self.select_executor.execution_engine = self

        self.dml_executor = DMLExecutor(catalog_manager, index_manager)
        self.schema_manager = SchemaManager(catalog_manager)
        self.view_manager = ViewManager(catalog_manager)
        self.procedure_manager = ProcedureManager(catalog_manager)
        self.function_manager = FunctionManager(catalog_manager)
        self.trigger_manager = TriggerManager(catalog_manager)
        self.temp_table_manager = TemporaryTableManager(catalog_manager)
        self.transaction_manager = TransactionManager(catalog_manager)
        self.visualizer = Visualizer(catalog_manager, index_manager)

        # Share the condition parser with all modules that need it
        self.condition_parser = ConditionParser()

        # Set the condition parser for each executor that needs it
        self.select_executor.condition_parser = self.condition_parser
        self.dml_executor.condition_parser = self.condition_parser
        self.join_executor.condition_parser = self.condition_parser
        self.group_by_executor.condition_parser = self.condition_parser

        # Set the transaction manager for DML executor to enable transaction recording
        self.dml_executor.transaction_manager = self.transaction_manager

        # Set execution_engine reference for managers that need it
        self.procedure_manager.execution_engine = self
        self.trigger_manager.execution_engine = self

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

        # Handle case sensitivity for table names - PRESERVE ORIGINAL CASE
        tables = self.catalog_manager.list_tables(db_name)
        case_corrected_table = None

        # Try direct match first
        if table_name in tables:
            case_corrected_table = table_name
        else:
            # Try case-insensitive match
            for table in tables:
                if table.lower() == table_name.lower():
                    case_corrected_table = table
                    break

        # Verify the table exists
        if not case_corrected_table:
            return {"error": f"Table '{table_name}' does not exist", "status": "error"}

        # Use the case-corrected table name for the query
        actual_table_name = case_corrected_table

        # Use catalog manager to get data
        results = self.catalog_manager.query_with_condition(
            actual_table_name, [], [column]
        )

        # Extract distinct values
        distinct_values = set()
        for record in results:
            distinct_values.add(record.get(column))

        # Sort the distinct values for consistent output
        sorted_values = sorted(distinct_values, key=lambda x: (x is None, x))

        return {
            "columns": [column],
            "rows": [[value] for value in sorted_values],
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
            return self.visualizer.visualize_index(table_name, index_name)
        else:
            return self.visualizer.visualize_all_indexes()

    def execute_visualize_bptree(self, plan):
        """Execute a VISUALIZE BPTREE command."""
        index_name = plan.get("index_name")
        table_name = plan.get("table")

        # Get current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected", "status": "error"}

        # Validate that table and index are specified
        if not table_name or not index_name:
            return {
                "error": "Table name and index name required for VISUALIZE BPTREE",
                "status": "error",
            }

        try:
            return self.visualizer.visualize_bptree(table_name, index_name)

        except Exception as e:
            return {
                "error": f"Failed to visualize B+ tree: {str(e)}",
                "status": "error",
            }

    def execute_batch_insert(self, plan):
        """Execute a batch insert operation with optimizations."""
        table_name = plan.get("table")
        records = plan.get("records", [])

        if not records:
            return {"error": "No records to insert", "status": "error"}

        # Check for valid database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected", "status": "error"}

        # Get optimal batch size (increase default for better performance)
        batch_size = plan.get("batch_size", 5000)  # Increased from 1000 to 5000

        logging.info(
            f"Starting batch insert of {len(records)} records into {table_name} with batch size {batch_size}"
        )

        try:
            return self.dml_executor.execute_batch_insert(
                table_name, records, batch_size
            )

        except Exception as e:
            return {"error": f"Batch insert failed: {str(e)}", "status": "error"}

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
            left_rows = left_result["data"]
        elif "rows" in left_result:
            left_rows = left_result["rows"]

        if "data" in right_result:
            right_rows = right_result["data"]
        elif "rows" in right_result:
            right_rows = right_result["rows"]

        # Perform the set operation
        result_rows = []

        if operation == "UNION":
            # Combine and remove duplicates
            all_rows = left_rows + right_rows
            seen = set()
            for row in all_rows:
                row_tuple = tuple(row) if isinstance(row, list) else tuple(row.values())
                if row_tuple not in seen:
                    seen.add(row_tuple)
                    result_rows.append(row)

        elif operation == "INTERSECT":
            # Find common rows
            left_set = {
                tuple(row) if isinstance(row, list) else tuple(row.values())
                for row in left_rows
            }
            for row in right_rows:
                row_tuple = tuple(row) if isinstance(row, list) else tuple(row.values())
                if row_tuple in left_set:
                    result_rows.append(row)

        elif operation == "EXCEPT":
            # Find rows in left but not in right
            right_set = {
                tuple(row) if isinstance(row, list) else tuple(row.values())
                for row in right_rows
            }
            for row in left_rows:
                row_tuple = tuple(row) if isinstance(row, list) else tuple(row.values())
                if row_tuple not in right_set:
                    result_rows.append(row)

        # Return in the expected format for SELECT results
        return {
            "columns": columns,
            "rows": result_rows,
            "status": "success",
            "type": f"{operation.lower()}_result",
            "rowCount": len(result_rows),
        }

    def _has_subquery(self, condition):
        """Check if a condition contains a subquery."""
        if isinstance(condition, dict):
            return "subquery" in condition
        return False

    def _resolve_subqueries(self, condition):
        """Resolve any subqueries in the condition and replace with results."""
        if not isinstance(condition, dict):
            return condition

        if "subquery" in condition:
            # Execute subquery and replace with results
            subquery_result = self.execute(condition["subquery"])
            if subquery_result.get("status") == "success":
                # Extract values from subquery result
                values = [row[0] for row in subquery_result.get("rows", [])]
                condition["value"] = values
                del condition["subquery"]  # Remove subquery, replace with values

        # Recursively process nested conditions
        for key, value in condition.items():
            if isinstance(value, dict):
                condition[key] = self._resolve_subqueries(value)
            elif isinstance(value, list):
                condition[key] = [
                    self._resolve_subqueries(item) if isinstance(item, dict) else item
                    for item in value
                ]

    def _evaluate_condition(self, record, condition):
        """
        Evaluate a condition against a record.

        Args:
            record: The record to check
            condition: Condition dictionary with operator, column, and value

        Returns:
            bool: True if the record matches the condition
        """
        if not condition:
            return True

        if condition.get("operator") == "AND":
            return all(
                self._evaluate_condition(record, cond)
                for cond in condition.get("operands", [])
            )

        if condition.get("operator") == "OR":
            return any(
                self._evaluate_condition(record, cond)
                for cond in condition.get("operands", [])
            )

        column = condition.get("column")
        operator = condition.get("operator")
        value = condition.get("value")

        if not column or not operator or column not in record:
            return False

        record_value = record[column]

        # Convert string values to numeric for comparison if needed
        try:
            if isinstance(value, str) and value.isdigit():
                value = int(value)
            elif isinstance(record_value, str) and record_value.isdigit():
                record_value = int(record_value)
        except (ValueError, TypeError):
            pass

        # Apply operator
        if operator == "=":
            return record_value == value
        elif operator == "!=":
            return record_value != value
        elif operator == ">":
            return record_value > value
        elif operator == ">=":
            return record_value >= value
        elif operator == "<":
            return record_value < value
        elif operator == "<=":
            return record_value <= value
        elif operator.upper() == "LIKE":
            return str(value).replace("%", ".*") in str(record_value)
        elif operator.upper() == "IN":
            return record_value in value

        return False

    def execute_aggregate_with_groupby(self, plan):
        """Execute an aggregate query with GROUP BY clause."""
        table_name = plan.get("table")
        group_by_columns = plan.get("group_by", [])
        return self.aggregate_executor.execute_group_by(plan)

    def _calculate_group_aggregate(self, agg_expression, records):
        return self.aggregate_executor.calculate_aggregate(agg_expression, records)

    def execute(self, plan):
        """
        Execute a query plan and return results.
        """
        if not plan or "type" not in plan:
            return {"error": "Invalid query plan", "status": "error"}

        plan_type = plan["type"]

        # Log the plan execution
        logging.info("🔍 Executing %s plan", plan_type)

        try:
            # Resolve temporary table names for operations that work with tables
            if plan_type in [
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "JOIN",
                "DISTINCT",
            ]:
                plan = self._resolve_plan_table_names(plan)

            if plan_type == "SELECT":
                return self.select_executor.execute_select(plan)
            elif plan_type == "DISTINCT":
                return self.execute_distinct(plan)
            elif plan_type == "INSERT":
                result = self.dml_executor.execute_insert(plan)
                # Fire INSERT triggers if successful
                if result.get("status") == "success":
                    self._fire_triggers("INSERT", plan.get("table"), plan)
                return result
            elif plan_type == "UPDATE":
                result = self.dml_executor.execute_update(plan)
                # Fire UPDATE triggers if successful
                if result.get("status") == "success":
                    self._fire_triggers("UPDATE", plan.get("table"), plan)
                return result
            elif plan_type == "DELETE":
                result = self.dml_executor.execute_delete(plan)
                # Fire DELETE triggers if successful
                if result.get("status") == "success":
                    self._fire_triggers("DELETE", plan.get("table"), plan)
                return result
            elif plan_type == "CREATE_TABLE":
                return self.schema_manager.execute_create_table(plan)
            elif plan_type == "CREATE_DATABASE":
                return self.schema_manager.execute_create_database(plan)
            elif plan_type == "CREATE_INDEX":
                return self.execute_create_index(plan)
            elif plan_type == "DROP_TABLE":
                return self.schema_manager.execute_drop_table(plan)
            elif plan_type == "DROP_DATABASE":
                return self.schema_manager.execute_drop_database(plan)
            elif plan_type == "DROP_INDEX":
                return self.execute_drop_index(plan)
            elif plan_type == "CREATE_VIEW":
                return self.view_manager.create_view(plan)
            elif plan_type == "DROP_VIEW":
                return self.view_manager.drop_view(plan)
            elif plan_type == "CREATE_PROCEDURE":
                return self.create_procedure(plan)
            elif plan_type == "DROP_PROCEDURE":
                return self.drop_procedure(plan)
            elif plan_type == "CALL_PROCEDURE":
                return self.call_procedure(plan)
            elif plan_type == "CREATE_FUNCTION":
                return self.create_function(plan)
            elif plan_type == "DROP_FUNCTION":
                return self.drop_function(plan)
            elif plan_type == "CREATE_TRIGGER":
                return self.create_trigger(plan)
            elif plan_type == "DROP_TRIGGER":
                return self.drop_trigger(plan)
            elif plan_type == "CREATE_TEMP_TABLE":
                return self.create_temp_table(plan)
            elif plan_type == "DROP_TEMP_TABLE":
                return self.drop_temp_table(plan)
            elif plan_type == "SHOW":
                return self.execute_show(plan)
            elif plan_type == "USE_DATABASE":
                return self.schema_manager.execute_use_database(plan)
            elif plan_type == "JOIN":
                # Fix: Properly extract and pass WHERE conditions for JOIN queries
                where_conditions = plan.get("where_conditions") or plan.get(
                    "parsed_condition"
                )
                join_info = plan.get("join_info", {})

                logging.info(
                    "🔍 Executing JOIN plan with WHERE conditions: %s", where_conditions
                )

                # Extract and clean table names from join_info
                table1 = join_info.get("table1", "") or plan.get("table1", "")
                table2 = join_info.get("table2", "") or plan.get("table2", "")
                condition = join_info.get("condition") or plan.get("condition")

                # Use planner to choose the best join algorithm
                join_algorithm = "HASH"  # Default
                if self.planner and table1 and table2 and condition:
                    try:
                        join_algorithm = self.planner._choose_join_algorithm(
                            [table1, table2], condition
                        )
                    except Exception as e:
                        logging.warning(
                            f"Failed to determine join algorithm, using HASH: {e}"
                        )
                        join_algorithm = "HASH"

                # Pass all necessary information to the join executor including WHERE conditions
                join_plan = {
                    "join_type": plan.get("join_type", "INNER"),
                    "join_algorithm": join_algorithm,
                    "table1": table1,
                    "table2": table2,
                    "condition": condition,
                    "where_conditions": where_conditions,  # Pass WHERE conditions properly
                    "join_info": join_info,
                    "columns": plan.get(
                        "columns", ["*"]
                    ),  # Also pass requested columns
                }

                # Update the original plan with the join algorithm used
                plan["join_algorithm"] = join_plan["join_algorithm"]

                # Execute the join and get the raw results
                join_results = self.join_executor.execute_join(join_plan)

                # CRITICAL FIX: Format JOIN results to match expected dictionary format
                if isinstance(join_results, list):
                    if not join_results:
                        # Empty result set
                        return {
                            "columns": [],
                            "rows": [],
                            "status": "success",
                            "type": "join_result",
                        }

                    # Get all unique columns from all records
                    all_columns = set()
                    for record in join_results:
                        if isinstance(record, dict):
                            all_columns.update(record.keys())

                    # Sort columns for consistent display
                    columns = sorted(list(all_columns))

                    # Convert list of dictionaries to rows format
                    rows = []
                    for record in join_results:
                        if isinstance(record, dict):
                            row = []
                            for col in columns:
                                row.append(record.get(col))
                            rows.append(row)

                    return {
                        "columns": columns,
                        "rows": rows,
                        "status": "success",
                        "type": "join_result",
                    }
                else:
                    # If join_results is already in the correct format, return it
                    return join_results
            elif plan_type == "VISUALIZE":
                object_type = plan.get("object")
                if object_type == "BPTREE":
                    return self.execute_visualize_bptree(plan)
                else:
                    return self.execute_visualize_index(plan)
            elif plan_type == "BATCH_INSERT":
                return self.execute_batch_insert(plan)
            elif plan_type in ["UNION", "INTERSECT", "EXCEPT"]:
                return self.execute_set_operation(plan)
            elif plan_type == "BEGIN_TRANSACTION":
                return self.transaction_manager.execute_transaction_operation(
                    "BEGIN_TRANSACTION"
                )
            elif plan_type == "COMMIT":
                return self.transaction_manager.execute_transaction_operation(
                    "COMMIT", plan.get("transaction_id")
                )
            elif plan_type == "ROLLBACK":
                return self.transaction_manager.execute_transaction_operation(
                    "ROLLBACK", plan.get("transaction_id")
                )
            elif plan_type == "SET_PREFERENCE":
                return self.execute_set_preference(plan)
            elif plan_type == "SCRIPT":
                return self.execute_script(plan)
            elif plan_type == "AGGREGATE":
                return self.aggregate_executor.execute_aggregate(plan)
            elif plan_type == "GROUP_BY":
                result = self.group_by_executor.execute_group_by(plan)
                return result
            else:
                return {
                    "error": f"Unsupported plan type: {plan_type}",
                    "status": "error",
                }

        except Exception as e:
            logging.error("Error executing plan: %s", str(e))
            logging.error(traceback.format_exc())
            return {"error": f"Execution error: {str(e)}", "status": "error"}

    def execute_set_preference(self, plan):
        return self.catalog_manager.set_preference(plan.get("key"), plan.get("value"))

    def execute_script(self, plan):
        return {"error": "Script execution not implemented", "status": "error"}

    def execute_show(self, plan):
        """Execute enhanced SHOW commands with support for new objects."""
        object_type = plan.get("object", "").upper()

        # Route to appropriate manager based on object type
        if object_type in ["DATABASES", "TABLES", "ALL_TABLES", "COLUMNS", "INDEXES"]:
            return self.schema_manager.execute_show(plan)
        elif object_type == "VIEWS":
            return self.view_manager.list_views()
        elif object_type == "PROCEDURES":
            return self.procedure_manager.list_procedures()
        elif object_type == "FUNCTIONS":
            return self.function_manager.list_functions()
        elif object_type == "TRIGGERS":
            return self.trigger_manager.list_triggers()
        elif object_type == "TEMP_TABLES":
            session_id = plan.get("session_id", "default")
            return self.temp_table_manager.list_temp_tables(session_id)
        else:
            # Fallback to schema manager for unknown types
            return self.schema_manager.execute_show(plan)

    def _fire_triggers(self, event_type, table_name, plan):
        """Fire triggers for DML operations."""
        if not table_name:
            return

        try:
            # Fire BEFORE triggers
            self.trigger_manager.fire_triggers(event_type, table_name, "BEFORE")

            # Fire AFTER triggers
            self.trigger_manager.fire_triggers(event_type, table_name, "AFTER")

        except Exception as e:
            logging.error(
                f"Error firing triggers for {event_type} on {table_name}: {str(e)}"
            )
            # Don't fail the main operation if trigger execution fails

    def create_procedure(self, plan):
        """Create a procedure via ProcedureManager."""
        return self.procedure_manager.create_procedure(plan)

    def drop_procedure(self, plan):
        """Drop a procedure via ProcedureManager."""
        return self.procedure_manager.drop_procedure(plan)

    def call_procedure(self, plan):
        """Call a procedure via ProcedureManager."""
        return self.procedure_manager.call_procedure(plan)

    def create_function(self, plan):
        """Create a function via FunctionManager."""
        return self.function_manager.create_function(plan)

    def drop_function(self, plan):
        """Drop a function via FunctionManager."""
        return self.function_manager.drop_function(plan)

    def create_trigger(self, plan):
        """Create a trigger via TriggerManager."""
        return self.trigger_manager.create_trigger(plan)

    def drop_trigger(self, plan):
        """Drop a trigger via TriggerManager."""
        return self.trigger_manager.drop_trigger(plan)

    def create_temp_table(self, plan):
        """Create a temporary table via TemporaryTableManager."""
        return self.temp_table_manager.create_temp_table(plan)

    def drop_temp_table(self, plan):
        """Drop a temporary table via TemporaryTableManager."""
        return self.temp_table_manager.drop_temp_table(plan)

    def evaluate_function_call(self, function_name, arguments):
        """Evaluate a function call and return the result."""
        try:
            return self.function_manager.call_function(function_name, arguments)
        except Exception as e:
            logging.error(f"Error evaluating function {function_name}: {str(e)}")
            return {"error": f"Function evaluation failed: {str(e)}", "status": "error"}

    def cleanup_session_resources(self, session_id):
        """Clean up temporary tables and other session resources."""
        try:
            # Clean up temporary tables for this session
            self.temp_table_manager.cleanup_session_temp_tables(session_id)
            logging.info(f"Cleaned up resources for session: {session_id}")
        except Exception as e:
            logging.error(f"Error cleaning up session {session_id}: {str(e)}")

    def _resolve_temp_table_name(self, table_name, session_id=None):
        """
        Resolve a table name, checking for temporary tables first.
        If a temporary table exists with the given name and session_id,
        return the internal temp table name, otherwise return the original name.
        """
        if not session_id or not table_name:
            return table_name

        return self.temp_table_manager.resolve_table_name(table_name, session_id)

    def _resolve_plan_table_names(self, plan):
        """
        Resolve table names in a plan to handle temporary tables.
        This modifies the plan in-place to replace logical temp table names
        with internal names when a session_id is present.
        """
        session_id = plan.get("session_id")
        if not session_id:
            return plan

        # Resolve main table name
        if "table" in plan:
            plan["table"] = self._resolve_temp_table_name(plan["table"], session_id)

        # Resolve table names in JOIN operations
        if "table1" in plan:
            plan["table1"] = self._resolve_temp_table_name(plan["table1"], session_id)
        if "table2" in plan:
            plan["table2"] = self._resolve_temp_table_name(plan["table2"], session_id)

        # Resolve table names in join_info
        if "join_info" in plan and isinstance(plan["join_info"], dict):
            join_info = plan["join_info"]
            if "table1" in join_info:
                join_info["table1"] = self._resolve_temp_table_name(
                    join_info["table1"], session_id
                )
            if "table2" in join_info:
                join_info["table2"] = self._resolve_temp_table_name(
                    join_info["table2"], session_id
                )

        return plan
