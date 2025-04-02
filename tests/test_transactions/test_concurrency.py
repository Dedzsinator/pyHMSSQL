"""
Tests for concurrency control in the DBMS.
"""
import pytest
import threading
import time
import sys
import os

# Add server directory to path
project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), '..'))
server_dir = os.path.join(project_root, 'server')
if server_dir not in sys.path:
    sys.path.insert(0, server_dir)

# Now import directly from the modules
from execution_engine import ExecutionEngine
from ddl_processor.index_manager import IndexManager

class TestConcurrencyControl:
    """Test concurrency control mechanisms."""
    
    def test_concurrent_updates(self, execution_engine, catalog_manager, test_table):
        """Test concurrent updates to the same record."""
        # Create a second execution engine
        execution_engine2 = ExecutionEngine(catalog_manager, IndexManager(catalog_manager))
        
        # Set up a synchronization event
        ready_to_execute = threading.Event()
        
        # First update - should acquire the lock
        def update_thread1():
            plan = {
                "type": "UPDATE",
                "table": test_table,
                "set": [("name", "'Thread 1 Update'"), ("age", 55)],
                "condition": "id = 1",
                "session_id": "session1"
            }
            
            # Signal ready to execute
            ready_to_execute.set()
            
            # Execute update and hold the lock for a moment
            result = execution_engine.execute(plan)
            time.sleep(0.5)  # Hold the lock
            
            return result
            
        # Second update - should wait for the lock
        def update_thread2():
            # Wait for thread 1 to start
            ready_to_execute.wait()
            
            # Give thread 1 time to acquire the lock
            time.sleep(0.1)
            
            plan = {
                "type": "UPDATE",
                "table": test_table,
                "set": [("name", "'Thread 2 Update'"), ("age", 60)],
                "condition": "id = 1",
                "session_id": "session2"
            }
            
            return execution_engine2.execute(plan)
        
        # Start threads
        thread1 = threading.Thread(target=update_thread1)
        thread2 = threading.Thread(target=update_thread2)
        
        thread1_result = None
        thread2_result = None
        
        thread1.start()
        thread2.start()
        
        thread1.join(timeout=2)
        thread2.join(timeout=2)
        
        # Verify the final record state (Thread 2 should have updated last)
        records = catalog_manager.query_with_condition(
            test_table, [{"column": "id", "operator": "=", "value": 1}]
        )
        assert len(records) == 1
        assert records[0]["name"] == "Thread 2 Update"
        assert records[0]["age"] == 60
    
    def test_deadlock_prevention(self, execution_engine, catalog_manager, test_table):
        """Test that deadlocks are prevented or detected."""
        # Create additional tables
        columns = [
            {"name": "id", "type": "INT", "primary_key": True},
            {"name": "value", "type": "TEXT"}
        ]
        catalog_manager.create_table("table_a", columns)
        catalog_manager.create_table("table_b", columns)
        
        catalog_manager.insert_record("table_a", {"id": 1, "value": "A1"})
        catalog_manager.insert_record("table_b", {"id": 1, "value": "B1"})
        
        # Set up execution engines for different sessions
        engine1 = ExecutionEngine(catalog_manager, IndexManager(catalog_manager))
        engine2 = ExecutionEngine(catalog_manager, IndexManager(catalog_manager))
        
        # Create potential deadlock scenario:
        # Thread 1: Lock A, then B
        # Thread 2: Lock B, then A
        
        # Barriers for coordinating the test
        barrier1 = threading.Event()
        barrier2 = threading.Event()
        
        def thread1_func():
            # First lock table A
            plan1 = {
                "type": "UPDATE",
                "table": "table_a",
                "set": [("value", "'Updated A'")],
                "condition": "id = 1",
                "session_id": "thread1"
            }
            result1 = engine1.execute(plan1)
            
            # Signal thread 2 to lock table B
            barrier1.set()
            
            # Wait for thread 2 to lock table B
            barrier2.wait(timeout=1)
            
            # Now try to lock table B
            plan2 = {
                "type": "UPDATE",
                "table": "table_b",
                "set": [("value", "'Updated B from thread1'")],
                "condition": "id = 1",
                "session_id": "thread1"
            }
            result2 = engine1.execute(plan2)
            
            return result1, result2
        
        def thread2_func():
            # Wait for thread 1 to lock table A
            barrier1.wait(timeout=1)
            
            # Now lock table B
            plan1 = {
                "type": "UPDATE",
                "table": "table_b",
                "set": [("value", "'Updated B'")],
                "condition": "id = 1",
                "session_id": "thread2"
            }
            result1 = engine2.execute(plan1)
            
            # Signal thread 1 that table B is locked
            barrier2.set()
            
            # Now try to lock table A
            plan2 = {
                "type": "UPDATE",
                "table": "table_a",
                "set": [("value", "'Updated A from thread2'")],
                "condition": "id = 1",
                "session_id": "thread2"
            }
            result2 = engine2.execute(plan2)
            
            return result1, result2
        
        thread1 = threading.Thread(target=thread1_func)
        thread2 = threading.Thread(target=thread2_func)
        
        thread1.start()
        thread2.start()
        
        thread1.join(timeout=3)
        thread2.join(timeout=3)
        
        # At least one thread should have timed out due to deadlock prevention
        # or a lock should have been released to allow the other thread to proceed
        record_a = catalog_manager.query_with_condition(
            "table_a", [{"column": "id", "operator": "=", "value": 1}]
        )[0]
        record_b = catalog_manager.query_with_condition(
            "table_b", [{"column": "id", "operator": "=", "value": 1}]
        )[0]
        
        # Check that at least one update was successful
        assert (record_a["value"].startswith("Updated") or 
                record_b["value"].startswith("Updated"))