"""
Transaction manager for handling database transactions.
"""

import logging
import uuid
import time
import os
import datetime
import traceback
from collections import OrderedDict, defaultdict
from transaction.lock_manager import LockManager
from bptree_wrapper import BPlusTreeFactory


class TransactionManager:
    """Handles database transactions."""

    def __init__(self, catalog_manager):
        self.catalog_manager = catalog_manager
        # Use OrderedDict for transactions to maintain operation order
        self.transactions = OrderedDict()  # {transaction_id: {status, operations, start_time}}
        # Table snapshots for efficient rollback - stores original data before modification
        self.table_snapshots = defaultdict(dict)  # {transaction_id: {table_name: {record_id: original_record}}}
        self.lock_manager = LockManager()
        self.logger = logging.getLogger(__name__)

    def record_operation(self, transaction_id, operation):
        """Record an operation in the transaction log.
        
        Args:
            transaction_id: The ID of the transaction
            operation: Dictionary with operation details (type, table, etc.)
            
        Returns:
            bool: True if operation was recorded
        """
        if transaction_id not in self.transactions:
            # Create the transaction if it doesn't exist
            self.logger.warning(f"Transaction {transaction_id} not found, creating it")
            self.transactions[transaction_id] = {
                "status": "active",
                "operations": [],
                "start_time": time.time()
            }

        # For INSERT, UPDATE, DELETE operations, take snapshots for efficient rollback
        table_name = operation.get("table")
        if table_name:
            # For DELETE and UPDATE, store original data
            if operation["type"] in ["DELETE", "UPDATE"]:
                # Take a snapshot of the original record for rollback
                record = operation.get("record") or operation.get("old_values")
                if record and "id" in record:
                    record_id = record["id"]
                    # Only store the first snapshot (original state) for each record
                    if transaction_id not in self.table_snapshots or \
                       table_name not in self.table_snapshots[transaction_id] or \
                       record_id not in self.table_snapshots[transaction_id].get(table_name, {}):
                        if transaction_id not in self.table_snapshots:
                            self.table_snapshots[transaction_id] = {}
                        if table_name not in self.table_snapshots[transaction_id]:
                            self.table_snapshots[transaction_id][table_name] = {}
                        
                        # Store a copy of the original record
                        self.table_snapshots[transaction_id][table_name][record_id] = record.copy()
                        self.logger.info(f"Stored snapshot for {table_name}.{record_id} in transaction {transaction_id}")
            
        # Add the operation to the transaction log
        self.transactions[transaction_id]["operations"].append(operation)
        return True

    def execute_transaction_operation(self, operation_type, transaction_id=None):
        """Execute a transaction operation (BEGIN, COMMIT, ROLLBACK).
        
        Args:
            operation_type: Type of operation (BEGIN_TRANSACTION, COMMIT, ROLLBACK)
            transaction_id: ID of existing transaction (for COMMIT/ROLLBACK)
            
        Returns:
            dict: Result of operation
        """
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
            # Add comprehensive logging for debugging
            self.logger.info(f"Attempting to rollback transaction: {transaction_id}")
            
            # Check if transaction exists
            if not transaction_id:
                self.logger.error("No transaction ID provided for rollback")
                return {"status": "error", "error": "No transaction ID provided"}
                
            if transaction_id not in self.transactions:
                self.logger.error(f"Transaction {transaction_id} not found. Available transactions: {list(self.transactions.keys())}")
                return {"status": "error", "error": f"Transaction {transaction_id} not found"}

            # Get operations in reverse order
            operations = list(reversed(self.transactions[transaction_id].get("operations", [])))
            rollback_success = True
            self.logger.info(f"Rolling back {len(operations)} operations")

            # First use the more efficient snapshot-based rollback when possible
            if transaction_id in self.table_snapshots:
                self.logger.info(f"Found {len(self.table_snapshots[transaction_id])} table snapshots for rollback")
                for table_name, records in self.table_snapshots[transaction_id].items():
                    self.logger.info(f"Restoring {len(records)} records in table {table_name}")
                    for record_id, original_record in records.items():
                        # Check if the record was deleted and needs to be restored
                        current_record = self.catalog_manager.get_record_by_key(table_name, record_id)
                        if current_record is None:
                            # Record was deleted, restore it
                            self.logger.info(f"Restoring deleted record: {table_name}.{record_id}")
                            result = self.catalog_manager.insert_record_with_id(
                                table_name, record_id, original_record
                            )
                            if not result:
                                rollback_success = False
                                self.logger.error(f"Failed to restore deleted record: {table_name}.{record_id}")
                        else:
                            # Record exists but may have been modified, restore original values
                            self.logger.info(f"Restoring original values for record: {table_name}.{record_id}")
                            result = self.catalog_manager.update_record(
                                table_name, record_id, original_record
                            )
                            if not result:
                                rollback_success = False
                                self.logger.error(f"Failed to restore original values: {table_name}.{record_id}")
                    
                    # Clear the snapshots for this transaction
                    del self.table_snapshots[transaction_id]

            # Process each operation in reverse order (for INSERT operations not covered by snapshots)
            for op in operations:
                if op["type"] == "INSERT":
                    # Remove inserted record
                    table = op["table"]
                    record_id = op.get("record_id")

                    if record_id is not None:
                        # Delete the inserted record directly using catalog manager
                        self.logger.info(f"Rolling back inserted record: {table}.{record_id}")
                        result = self.catalog_manager.delete_record_by_id(table, record_id)
                        if result:
                            self.logger.info(f"Rollback: Deleted record {record_id} from {table}")
                        else:
                            rollback_success = False
                            self.logger.error(f"Failed to delete record {record_id} from {table} during rollback")

            # Mark transaction as rolled back
            self.transactions[transaction_id]["status"] = "rolled_back"
            
            # Clean up transaction data
            if rollback_success:
                # Keep transaction in history but clean up
                self.logger.info(f"Transaction {transaction_id} rolled back successfully")
                return {"status": "success", "message": "Transaction rolled back successfully"}
            else:
                self.logger.error(f"Transaction {transaction_id} rollback encountered errors")
                return {"status": "error", "message": "Transaction rollback failed"}

        # Handle COMMIT
        elif operation_type == "COMMIT":
            # Simply mark as committed
            self.logger.info(f"Committing transaction: {transaction_id}")
            if not transaction_id:
                self.logger.error("No transaction ID provided for commit")
                return {"status": "error", "error": "No transaction ID provided"}
                
            if transaction_id in self.transactions:
                self.transactions[transaction_id]["status"] = "committed"
                
                # Clean up snapshots when committing
                if transaction_id in self.table_snapshots:
                    del self.table_snapshots[transaction_id]
                
                self.logger.info(f"Transaction {transaction_id} committed successfully")
                return {"status": "success", "message": "Transaction committed"}
            else:
                self.logger.error(f"Transaction {transaction_id} not found for commit")
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
                result = self.catalog_manager.delete_record_by_id(table, record_id)
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
    
    def get_transaction_status(self, transaction_id):
        """Get the status of a transaction."""
        if transaction_id in self.transactions:
            return self.transactions[transaction_id]["status"]
        return None
    
    def cleanup_old_transactions(self, max_age_hours=24):
        """Clean up old transactions to prevent memory leaks."""
        current_time = time.time()
        to_remove = []
        
        for tx_id, tx_data in self.transactions.items():
            # Keep active transactions
            if tx_data["status"] == "active":
                continue
                
            # Remove old completed transactions
            if current_time - tx_data["start_time"] > max_age_hours * 3600:
                to_remove.append(tx_id)
                
        # Remove old transactions
        for tx_id in to_remove:
            del self.transactions[tx_id]
            if tx_id in self.table_snapshots:
                del self.table_snapshots[tx_id]