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
from transaction.transaction_manager import TransactionManager
from parsers.condition_parser import ConditionParser
from utils.visualizer import Visualizer
from utils.sql_helpers import parse_simple_condition, check_database_selected
import logging
import traceback

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
        self.group_by_executor = GroupByExecutor(catalog_manager)

        # Set execution_engine reference only once (removing duplicate)
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
        self.group_by_executor.condition_parser = self.condition_parser

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
            actual_table_name, [], [column])

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
            return {"error": "Table name and index name required for VISUALIZE BPTREE", "status": "error"}

        try:
            return self.visualizer.visualize_bptree(table_name, index_name)
                
        except Exception as e:
            return {"error": f"Failed to visualize B+ tree: {str(e)}", "status": "error"}

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

        logging.info(f"Starting batch insert of {len(records)} records into {table_name} with batch size {batch_size}")

        try:
            return self.dml_executor.execute_batch_insert(table_name, records, batch_size)

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
            left_set = {tuple(row) if isinstance(row, list) else tuple(row.values()) for row in left_rows}
            for row in right_rows:
                row_tuple = tuple(row) if isinstance(row, list) else tuple(row.values())
                if row_tuple in left_set:
                    result_rows.append(row)

        elif operation == "EXCEPT":
            # Find rows in left but not in right
            right_set = {tuple(row) if isinstance(row, list) else tuple(row.values()) for row in right_rows}
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
            "rowCount": len(result_rows)
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
                condition[key] = [self._resolve_subqueries(item) if isinstance(item, dict) else item for item in value]

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
            return all(self._evaluate_condition(record, cond) for cond in condition.get("operands", []))

        if condition.get("operator") == "OR":
            return any(self._evaluate_condition(record, cond) for cond in condition.get("operands", []))

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
            return str(value).replace('%', '.*') in str(record_value)
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
            if plan_type == "SELECT":
                return self.select_executor.execute_select(plan)
            elif plan_type == "DISTINCT":
                return self.execute_distinct(plan)
            elif plan_type == "INSERT":
                return self.dml_executor.execute_insert(plan)
            elif plan_type == "UPDATE":
                return self.dml_executor.execute_update(plan)
            elif plan_type == "DELETE":
                return self.dml_executor.execute_delete(plan)
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
            elif plan_type == "SHOW":
                return self.schema_manager.execute_show(plan)
            elif plan_type == "USE_DATABASE":
                return self.schema_manager.execute_use_database(plan)
            elif plan_type == "JOIN":
                # Fix: Properly extract and pass WHERE conditions for JOIN queries
                where_conditions = plan.get("where_conditions") or plan.get("parsed_condition")
                join_info = plan.get("join_info", {})
                
                logging.info("🔍 Executing JOIN plan with WHERE conditions: %s", where_conditions)
                
                # Extract and clean table names from join_info
                table1 = join_info.get("table1", "") or plan.get("table1", "")
                table2 = join_info.get("table2", "") or plan.get("table2", "")
                
                # Pass all necessary information to the join executor including WHERE conditions
                join_plan = {
                    "join_type": join_info.get("type", "INNER") or plan.get("join_type", "INNER"),
                    "join_algorithm": join_info.get("join_algorithm", "HASH") or plan.get("join_algorithm", "HASH"),
                    "table1": table1,
                    "table2": table2,
                    "condition": join_info.get("condition") or plan.get("condition"),
                    "where_conditions": where_conditions,  # Pass WHERE conditions properly
                    "join_info": join_info,
                    "columns": plan.get("columns", ["*"])  # Also pass requested columns
                }
                
                return self.join_executor.execute_join(join_plan)
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
                return self.transaction_manager.begin()
            elif plan_type == "COMMIT":
                return self.transaction_manager.commit()
            elif plan_type == "ROLLBACK":
                return self.transaction_manager.rollback()
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
                return {"error": f"Unsupported plan type: {plan_type}", "status": "error"}

        except Exception as e:
            logging.error("Error executing plan: %s", str(e))
            logging.error(traceback.format_exc())
            return {"error": f"Execution error: {str(e)}", "status": "error"}

    def execute_set_preference(self, plan):
        return self.catalog_manager.set_preference(plan.get("key"), plan.get("value"))

    def execute_script(self, plan):
        return {"error": "Script execution not implemented", "status": "error"}