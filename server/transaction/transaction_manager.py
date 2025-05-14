"""
Transaction manager for handling database transactions.
"""

import logging
import uuid
import time
import os
import datetime
import traceback
from transaction.lock_manager import LockManager
from bptree import BPlusTree


class TransactionManager:
    """Handles database transactions."""

    def __init__(self, catalog_manager):
        self.catalog_manager = catalog_manager
        self.transactions = {}  # Unified transaction storage
        self.lock_manager = LockManager()
        self.logger = logging.getLogger(__name__)


    def record_operation(self, transaction_id, operation):
        """Record an operation in the transaction log."""
        if transaction_id not in self.transactions:
            # Create the transaction if it doesn't exist
            self.logger.warning(f"Transaction {transaction_id} not found, creating it")
            self.transactions[transaction_id] = {
                "status": "active",
                "operations": [],
                "start_time": time.time()
            }

        self.transactions[transaction_id]["operations"].append(operation)
        return True

    def execute_transaction_operation(self, operation_type, transaction_id=None):
        """Execute a transaction operation (BEGIN, COMMIT, ROLLBACK)."""

        if operation_type == "BEGIN_TRANSACTION":
            # Generate a transaction ID and create a new transaction
            transaction_id = str(uuid.uuid4())
            self.transactions[transaction_id] = {
                "status": "active",
                "operations": [],
                "start_time": time.time()
            }
            self.logger.info(f"Started transaction {transaction_id}")
            return {
                "status": "success",
                "message": "Transaction started",
                "transaction_id": transaction_id
            }

        elif operation_type == "ROLLBACK":
            # Check if transaction exists
            if transaction_id not in self.transactions:
                return {"status": "error", "error": f"Transaction {transaction_id} not found"}

            # Get operations in reverse order
            operations = list(reversed(self.transactions[transaction_id].get("operations", [])))
            rollback_success = True

            # Process each operation in reverse order
            for op in operations:
                if op["type"] == "INSERT":
                    # Remove inserted record
                    table = op["table"]
                    record_id = op.get("record_id")

                    if record_id is not None:
                        # Delete the inserted record directly from the B+ tree
                        db_name = self.catalog_manager.get_current_database()
                        table_file = os.path.join(self.catalog_manager.tables_dir, db_name, f"{table}.tbl")

                        if os.path.exists(table_file):
                            tree = BPlusTree.load_from_file(table_file)
                            if tree:
                                # Find the exact key for this record
                                all_records = tree.range_query(float('-inf'), float('inf'))
                                for key, record in all_records:
                                    if record.get("id") == record_id:
                                        tree.delete(key)
                                        tree.save_to_file(table_file)
                                        self.logger.info(f"Rollback: Deleted record {record_id} from {table}")
                                        break
                        else:
                            rollback_success = False

            # Mark transaction as rolled back
            self.transactions[transaction_id]["status"] = "rolled_back"

            if rollback_success:
                return {"status": "success", "message": "Transaction rolled back successfully"}
            else:
                return {"status": "error", "message": "Transaction rollback failed"}

        # Handle COMMIT
        elif operation_type == "COMMIT":
            # Simply mark as committed
            if transaction_id in self.transactions:
                self.transactions[transaction_id]["status"] = "committed"
                return {"status": "success", "message": "Transaction committed"}
            else:
                return {"status": "error", "error": f"Transaction {transaction_id} not found"}

        else:
            return {"status": "error", "error": f"Unknown transaction operation: {operation_type}"}

    def _undo_operation(self, operation):
        """Undo a single operation in a transaction."""
        if operation["type"] == "INSERT":
            table = operation["table"]
            record_id = operation.get("record_id")

            if not record_id:
                self.logger.warning("Cannot rollback INSERT without record ID")
                return False

            # Remove the inserted record
            try:
                # Use the actual key in the B+ tree
                db_name = self.catalog_manager.get_current_database()
                table_file = os.path.join(self.catalog_manager.tables_dir, db_name, f"{table}.tbl")

                if os.path.exists(table_file):
                    tree = BPlusTree.load_from_file(table_file)
                    if tree:
                        # Find records matching the ID
                        all_records = tree.range_query(float('-inf'), float('inf'))
                        for key, record in all_records:
                            if record.get("id") == record_id:
                                # Delete using the correct tree key
                                tree.delete(key)
                                tree.save_to_file(table_file)
                                self.logger.info(f"Rolled back INSERT in {table} for record {record_id}")
                                return True

                # Fallback to using conditions
                conditions = [{"column": "id", "operator": "=", "value": record_id}]
                result = self.catalog_manager.delete_records(table, conditions)
                if result:
                    self.logger.info(f"Rolled back INSERT in {table} for record {record_id}")
                    return True
                return False

            except RuntimeError as e:
                self.logger.error(f"Error rolling back INSERT: {str(e)}")
                return False

        elif operation["type"] == "UPDATE":
            # For UPDATE, restore the old values
            table = operation["table"]
            record_id = operation["record_id"]
            old_values = operation["old_values"]

            try:
                # Use update_record to restore original values
                result = self.catalog_manager.update_record(table, record_id, old_values)
                if result:
                    self.logger.info(f"Rolled back UPDATE in {table} for record {record_id}")
                return result
            except RuntimeError as e:
                self.logger.error(f"Error rolling back UPDATE: {str(e)}")
                return False

        elif operation["type"] == "DELETE":
            # For DELETE, re-insert the record
            table = operation["table"]
            record = operation["record"]

            try:
                # Use insert_record to restore the deleted record
                result = self.catalog_manager.insert_record(table, record)
                if result:
                    self.logger.info(f"Rolled back DELETE in {table} for record {record.get('id')}")
                return bool(result)
            except RuntimeError as e:
                self.logger.error(f"Error rolling back DELETE: {str(e)}")
                return False

        return False  # Unknown operation type
    
    def handle_query(self, data):
        """
        Handle query requests.
        """
        session_id = data.get("session_id")
        if session_id not in self.sessions:
            logging.warning(
                "Unauthorized query attempt with invalid session ID: %s", session_id
            )
            return {"error": "Unauthorized. Please log in.", "status": "error"}

        user = self.sessions[session_id]
        query = data.get("query", "")

        logging.info("Query from %s: %s", user.get(
            "username", "Unknown"), query)

        # Log the query for audit
        self._log_query(user.get("username"), query)

        # Print current database for debugging
        current_db = self.catalog_manager.get_current_database()
        logging.info("Current database: %s", current_db)

        # Parse and execute with planner and optimizer integration
        try:
            # 1. Parse SQL query
            parsed = self.sql_parser.parse_sql(query)
            if "error" in parsed:
                logging.error("SQL parsing error: %s", parsed["error"])
                return {"error": parsed["error"], "status": "error"}

            # Handle transaction ID propagation
            if parsed and parsed.get("type") == "BEGIN_TRANSACTION":
                # Generate execution plan
                execution_plan = self.planner.plan_query(parsed)
                if "error" in execution_plan:
                    logging.error("Error generating plan: %s", execution_plan["error"])
                    return {"error": execution_plan["error"], "status": "error"}
                    
                # Execute the plan
                result = self.execution_engine.execute(execution_plan)
                
                # Store transaction ID in session if created successfully
                if result and result.get("status") == "success" and "transaction_id" in result:
                    if not hasattr(self, "active_transactions"):
                        self.active_transactions = {}
                    self.active_transactions[session_id] = result["transaction_id"]
                    
                return result
            elif parsed and parsed.get("type") in ["COMMIT", "ROLLBACK"]:
                # Add transaction ID from session to the parsed query
                if hasattr(self, "active_transactions") and session_id in self.active_transactions:
                    transaction_id = self.active_transactions[session_id]
                    parsed["transaction_id"] = transaction_id
                    
                    # Generate execution plan
                    execution_plan = self.planner.plan_query(parsed)
                    if "error" in execution_plan:
                        logging.error("Error generating plan: %s", execution_plan["error"])
                        return {"error": execution_plan["error"], "status": "error"}
                    
                    # Execute the plan
                    result = self.execution_engine.execute(execution_plan)
                    
                    # Clean up the session's transaction tracking
                    if result and result.get("status") == "success":
                        del self.active_transactions[session_id]
                        
                    return result
                else:
                    return {"error": "No active transaction for this session", "status": "error"}
            else:
                # For DML queries in a transaction, attach the transaction ID
                if hasattr(self, "active_transactions") and session_id in self.active_transactions:
                    # Add transaction ID to the parsed query for INSERT, UPDATE, DELETE operations
                    if parsed.get("type") in ["INSERT", "UPDATE", "DELETE"]:
                        parsed["transaction_id"] = self.active_transactions[session_id]

            # Log the parsed query structure
            logging.info("▶️ Parsed query: %s", parsed)

            # 2. Generate execution plan using planner
            execution_plan = self.planner.plan_query(parsed)
            if "error" in execution_plan:
                logging.error("Error generating plan: %s",
                            execution_plan["error"])
                return {"error": execution_plan["error"], "status": "error"}

            logging.info("🔄 Initial execution plan:")
            logging.info("---------------------------------------------")
            self._log_plan_details(execution_plan)
            logging.info("---------------------------------------------")

            # 3. Optimize the execution plan
            optimized_plan = self.optimizer.optimize(execution_plan)
            if "error" in optimized_plan:
                logging.error("Error optimizing plan: %s",
                            optimized_plan["error"])
                return {"error": optimized_plan["error"], "status": "error"}

            logging.info("✅ Optimized execution plan:")
            logging.info("---------------------------------------------")
            self._log_plan_details(optimized_plan)
            logging.info("---------------------------------------------")

            # 4. Execute the optimized plan
            start_time = datetime.datetime.now()
            result = self.execution_engine.execute(optimized_plan)
            execution_time = (datetime.datetime.now() -
                            start_time).total_seconds()

            # Add execution time to result
            if isinstance(result, dict):
                result["execution_time_ms"] = round(execution_time * 1000, 2)

            logging.info("⏱️ Query executed in %s ms",
                        round(execution_time * 1000, 2))
            return result
        except (
            ValueError,
            SyntaxError,
            TypeError,
            AttributeError,
            KeyError,
            RuntimeError,
        ) as e:
            logging.error("Error executing query: %s", str(e))
            logging.error(traceback.format_exc())
            return {"error": str(e), "status": "error"}