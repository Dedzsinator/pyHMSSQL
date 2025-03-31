import logging
import traceback
from query_processor.join_executor import JoinExecutor
from query_processor.aggregate_executor import AggregateExecutor
from query_processor.select_executor import SelectExecutor
from query_processor.dml_executor import DMLExecutor
from ddl_processor.schema_manager import SchemaManager
from ddl_processor.index_manager import IndexManager
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
        self.select_executor = SelectExecutor(catalog_manager, self.join_executor, self.aggregate_executor)
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

    def execute(self, plan):
        """
        Execute the query plan by dispatching to appropriate module.
        """
        plan_type = plan.get("type")

        if not plan_type:
            return {"error": "No operation type specified in plan", "status": "error"}

        logging.debug("Executing plan of type %s: %s", plan_type, plan)

        try:
            # SQL queries
            if plan_type == "SELECT":
                return self.select_executor.execute_select(plan)
            elif plan_type == "AGGREGATE":
                return self.aggregate_executor.execute_aggregate(plan)
            elif plan_type == "JOIN":
                return self.join_executor.execute_join(plan)

            # DML operations
            elif plan_type == "INSERT":
                return self.dml_executor.execute_insert(plan)
            elif plan_type == "UPDATE":
                return self.dml_executor.execute_update(plan)
            elif plan_type == "DELETE":
                return self.dml_executor.execute_delete(plan)

            # DDL operations
            elif plan_type in ["CREATE_TABLE", "DROP_TABLE"]:
                return self.schema_manager.execute_table_operation(plan)
            elif plan_type in ["CREATE_DATABASE", "DROP_DATABASE", "USE_DATABASE"]:
                return self.schema_manager.execute_database_operation(plan)
            elif plan_type in ["CREATE_INDEX", "DROP_INDEX"]:
                return self.schema_manager.execute_index_operation(plan)
            elif plan_type in ["CREATE_VIEW", "DROP_VIEW"]:
                return self.view_manager.execute_view_operation(plan)

            # Transaction operations
            elif plan_type in ["BEGIN_TRANSACTION", "COMMIT", "ROLLBACK"]:
                return self.transaction_manager.execute_transaction_operation(plan_type)

            # Utility operations
            elif plan_type == "SHOW":
                return self.schema_manager.execute_show_operation(plan)
            elif plan_type == "SET":
                return self.execute_set_preference(plan)
            elif plan_type == "VISUALIZE":
                return self.visualizer.execute_visualize(plan)

            else:
                return {
                    "error": f"Unsupported operation type: {plan_type}",
                    "status": "error"
                }

        except Exception as e:
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
            "status": "success"
        }
