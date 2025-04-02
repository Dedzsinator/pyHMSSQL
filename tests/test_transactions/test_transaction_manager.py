"""
Tests for transaction management.
"""
import pytest
import threading
import time
import sys
import os

# Add correct path for imports
project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), '..'))
server_dir = os.path.join(project_root, 'server')
if server_dir not in sys.path:
    sys.path.insert(0, server_dir)

# Import required modules
from execution_engine import ExecutionEngine
from ddl_processor.index_manager import IndexManager

class TestTransactionOperations:
    """Test transaction operations."""
    
    def test_basic_transaction(self, transaction_manager, catalog_manager, execution_engine, test_table):
        """Test basic transaction with commit."""
        # Start transaction
        begin_plan = {"type": "BEGIN_TRANSACTION"}
        begin_result = transaction_manager.execute_transaction_operation("BEGIN_TRANSACTION")
        transaction_id = begin_result["transaction_id"]
        
        assert begin_result["status"] == "success"
        assert transaction_id is not None
        
        # Execute INSERT within transaction
        insert_plan = {
            "type": "INSERT",
            "table": test_table,
            "columns": ["id", "name", "email", "age"],
            "values": [[100, "Transaction Test", "trans@example.com", 45]],
            "transaction_id": transaction_id
        }
        
        insert_result = execution_engine.execute(insert_plan)
        assert insert_result["status"] == "success"
        
        # Commit transaction
        commit_result = transaction_manager.execute_transaction_operation("COMMIT", transaction_id)
        assert commit_result["status"] == "success"
        
        # Verify data is visible after commit
        records = catalog_manager.query_with_condition(
            test_table, [{"column": "id", "operator": "=", "value": 100}]
        )
        assert len(records) == 1
        assert records[0]["name"] == "Transaction Test"
    
    def test_transaction_rollback(self, transaction_manager, catalog_manager, execution_engine, test_table):
        """Test transaction rollback."""
        # Start transaction
        begin_result = transaction_manager.execute_transaction_operation("BEGIN_TRANSACTION")
        transaction_id = begin_result["transaction_id"]
        
        # Execute INSERT within transaction
        insert_plan = {
            "type": "INSERT",
            "table": test_table,
            "columns": ["id", "name", "email", "age"],
            "values": [[101, "Rollback Test", "rollback@example.com", 50]],
            "transaction_id": transaction_id
        }
        
        insert_result = execution_engine.execute(insert_plan)
        assert insert_result["status"] == "success"
        
        # Rollback transaction
        rollback_result = transaction_manager.execute_transaction_operation("ROLLBACK", transaction_id)
        assert rollback_result["status"] == "success"
        
        # Verify data is not visible after rollback
        records = catalog_manager.query_with_condition(
            test_table, [{"column": "id", "operator": "=", "value": 101}]
        )
        assert len(records) == 0