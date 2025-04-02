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
        if not self.catalog_manager.list_tables(db_name) or table_name not in self.catalog_manager.list_tables(db_name):
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
        """Execute the query plan by dispatching to appropriate module."""
        plan_type = plan.get("type")
        transaction_id = plan.get("transaction_id")

        # Handle transaction control operations first
        if plan_type == "BEGIN_TRANSACTION":
            return self.transaction_manager.execute_transaction_operation("BEGIN_TRANSACTION")
        elif plan_type == "COMMIT":
            return self.transaction_manager.execute_transaction_operation("COMMIT", transaction_id)
        elif plan_type == "ROLLBACK":
            return self.transaction_manager.execute_transaction_operation("ROLLBACK", transaction_id)

        try:
            result = None

            # Handle different operation types
            if plan_type == "SELECT":
                result = self.select_executor.execute_select(plan)
            elif plan_type == "INSERT":
                result = self.dml_executor.execute_insert(plan)

                # Record the operation if within a transaction
                if transaction_id and result and result.get("status") == "success":
                    record = {}

                    # Extract record data from different possible formats
                    if plan.get("record"):
                        record = plan.get("record")
                    elif plan.get("columns") and plan.get("values"):
                        record = dict(zip(plan.get("columns"), plan.get("values")[0] if plan.get("values") else []))

                    record_id = record.get("id")
                    if record_id:
                        # Record this operation for potential rollback
                        self.transaction_manager.record_operation(transaction_id, {
                            "type": "INSERT",
                            "table": plan.get("table"),
                            "record_id": record_id,
                            "record": record
                        })

            # SQL queries
            if plan_type == "SELECT":
                result = self.select_executor.execute_select(plan)
            elif plan_type == "AGGREGATE":
                result = self.aggregate_executor.execute_aggregate(plan)
            elif plan_type == "JOIN":
                result = self.join_executor.execute_join(plan)

            # DML operations - record for transaction if within a transaction
            elif plan_type == "INSERT":
                result = self.dml_executor.execute_insert(plan)

                # Make sure to record transaction operation regardless of result status
                if transaction_id:
                    # Extract record information from the plan or result
                    record = plan.get("record", {})
                    if not record and plan.get("columns") and plan.get("values"):
                        record = {
                            col: val for col, val in zip(plan.get("columns", []),
                                                    plan.get("values", [[]])[0])
                        }

                    # Get record ID from various possible sources
                    record_id = None
                    if "id" in record:
                        record_id = record["id"]
                    elif plan.get("values") and plan.get("values")[0] and len(plan.get("values")[0]) > 0:
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

                        existing_records = self.catalog_manager.query_with_condition(table, conditions)

                # Execute update
                result = self.dml_executor.execute_update(plan)

                # Record operation if successful and in a transaction
                if transaction_id and result["status"] == "success" and existing_records and len(existing_records) > 0:
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
                        # Similar condition parsing as for UPDATE
                        conditions = []
                        parts = condition.split('=')
                        if len(parts) == 2:
                            col = parts[0].strip()
                            val = parts[1].strip()
                            try:
                                val = int(val)
                            except ValueError:
                                if val.startswith("'") and val.endswith("'"):
                                    val = val[1:-1]
                            conditions.append({
                                "column": col,
                                "operator": "=",
                                "value": val
                            })

                        existing_records = self.catalog_manager.query_with_condition(table, conditions)

                # Execute delete
                result = self.dml_executor.execute_delete(plan)

                # Record operation if successful and in a transaction
                if transaction_id and result["status"] == "success" and existing_records and len(existing_records) > 0:
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

            else:
                result = {
                    "error": f"Unsupported operation type: {plan_type}",
                    "status": "error",
                }

            if isinstance(result, dict) and "type" not in result:
                result["type"] = f"{plan_type.lower()}_result"

            return result

        except Exception as e:
            logging.error(f"Error executing {plan_type}: {str(e)}")
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
