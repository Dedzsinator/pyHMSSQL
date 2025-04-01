import logging
import traceback
from query_processor.join_executor import JoinExecutor
from query_processor.aggregate_executor import AggregateExecutor
from query_processor.select_executor import SelectExecutor
from query_processor.dml_executor import DMLExecutor
from ddl_processor.schema_manager import SchemaManager
from ddl_processor.view_manager import ViewManager
from transaction.transaction_manager import TransactionManager
from parsers.condition_parser import ConditionParser
from utils.visualizer import Visualizer


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

    def execute(self, plan):
        """
        Execute the query plan by dispatching to appropriate module.
        """
        plan_type = plan.get("type")

        if not plan_type:
            return {"error": "No operation type specified in plan", "status": "error"}

        logging.debug("Executing plan of type %s: %s", plan_type, plan)

        session_id = plan.get("session_id")

        try:
            result = None

            # SQL queries
            if plan_type == "SELECT":
                result = self.select_executor.execute_select(plan)
            elif plan_type == "AGGREGATE":
                result = self.aggregate_executor.execute_aggregate(plan)
            elif plan_type == "JOIN":
                result = self.join_executor.execute_join(plan)

            # DML operations
            elif plan_type == "INSERT":
                result = self.dml_executor.execute_insert(plan)
            elif plan_type == "UPDATE":
                result = self.dml_executor.execute_update(plan)
            elif plan_type == "DELETE":
                result = self.dml_executor.execute_delete(plan)

            # DDL operations
            elif plan_type in ["CREATE_TABLE", "DROP_TABLE"]:
                result = self.schema_manager.execute_table_operation(plan)
            elif plan_type in ["CREATE_DATABASE", "DROP_DATABASE", "USE_DATABASE"]:
                result = self.schema_manager.execute_database_operation(plan)
            # Replace these lines for index operations
            elif plan_type == "CREATE_INDEX":
                result = self.execute_create_index(plan)
            elif plan_type == "DROP_INDEX":
                result = self.execute_drop_index(plan)
            elif plan_type in ["CREATE_VIEW", "DROP_VIEW"]:
                result = self.view_manager.execute_view_operation(plan)

            # Transaction operations
            elif plan_type in ["BEGIN_TRANSACTION", "COMMIT", "ROLLBACK"]:
                result = self.transaction_manager.execute_transaction_operation(
                    plan_type
                )

            # Utility operations
            elif plan_type == "SHOW":
                result = self.schema_manager.execute_show_operation(plan)
            elif plan_type == "SET":
                result = self.execute_set_preference(plan)
            elif plan_type == "VISUALIZE":
                result = self.execute_visualize_index(plan)

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
            logging.error(traceback.format_exc())
            return {
                "error": f"Error executing {plan_type}: {str(e)}",
                "status": "error",
                "type": "error",
            }

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
