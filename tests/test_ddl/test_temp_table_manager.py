import unittest
import tempfile
import os
import json
from unittest.mock import MagicMock, patch

from ddl_processor.temp_table_manager import TemporaryTableManager
from catalog_manager import CatalogManager


class TestTemporaryTableManager(unittest.TestCase):
    def setUp(self):
        """Set up test environment with temporary directory"""
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test.db")
        
        # Create mock dependencies
        self.mock_catalog = MagicMock(spec=CatalogManager)
        
        # Initialize TemporaryTableManager
        self.temp_table_manager = TemporaryTableManager(self.mock_catalog)

    def tearDown(self):
        """Clean up test environment"""
        import shutil
        shutil.rmtree(self.test_dir)

    def test_execute_create_temporary_table_success(self):
        """Test successful temporary table creation"""
        plan = {
            "type": "CREATE_TEMPORARY_TABLE",
            "table": "temp_users",
            "columns": [
                {"name": "id", "type": "INTEGER", "constraints": ["PRIMARY KEY"]},
                {"name": "name", "type": "VARCHAR(50)", "constraints": []}
            ],
            "constraints": []
        }
        
        session_id = "session_123"
        
        # Setup mocks
        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.create_table.return_value = True
        
        result = self.temp_table_manager.execute_create_temporary_table(plan, session_id)
        
        self.assertEqual(result["status"], "success")
        self.assertIn("created successfully", result["message"])
        self.assertIn("temp_table_name", result)
        
        # Check that temporary table was created with session prefix
        expected_temp_name = f"_temp_{session_id}_temp_users"
        self.assertEqual(result["temp_table_name"], expected_temp_name)
        self.mock_catalog.create_table.assert_called_once_with(
            expected_temp_name, plan["columns"], plan["constraints"]
        )

    def test_execute_create_temporary_table_missing_name(self):
        """Test temporary table creation with missing name"""
        plan = {
            "type": "CREATE_TEMPORARY_TABLE",
            "columns": [
                {"name": "id", "type": "INTEGER", "constraints": ["PRIMARY KEY"]}
            ],
            "constraints": []
        }
        
        session_id = "session_123"
        
        result = self.temp_table_manager.execute_create_temporary_table(plan, session_id)
        
        self.assertEqual(result["status"], "error")
        self.assertIn("No table name specified", result["error"])

    def test_execute_create_temporary_table_default_session(self):
        """Test temporary table creation with default session"""
        plan = {
            "type": "CREATE_TEMPORARY_TABLE",
            "table": "temp_users",
            "columns": [
                {"name": "id", "type": "INTEGER", "constraints": ["PRIMARY KEY"]}
            ],
            "constraints": []
        }
        
        # Setup mocks
        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.create_table.return_value = True
        
        # Don't pass session_id, should use default
        result = self.temp_table_manager.execute_create_temporary_table(plan)
        
        self.assertEqual(result["status"], "success")
        
        # Check that default session was used
        expected_temp_name = f"_temp_default_session_temp_users"
        self.assertEqual(result["temp_table_name"], expected_temp_name)

    def test_execute_create_temporary_table_already_exists(self):
        """Test creating temporary table that already exists in session"""
        plan = {
            "type": "CREATE_TEMPORARY_TABLE",
            "table": "temp_users",
            "columns": [
                {"name": "id", "type": "INTEGER", "constraints": ["PRIMARY KEY"]}
            ],
            "constraints": []
        }
        
        session_id = "session_123"
        
        # Setup mocks
        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.create_table.return_value = True
        
        # Create the table first time
        result1 = self.temp_table_manager.execute_create_temporary_table(plan, session_id)
        self.assertEqual(result1["status"], "success")
        
        # Try to create it again - should fail
        result2 = self.temp_table_manager.execute_create_temporary_table(plan, session_id)
        
        self.assertEqual(result2["status"], "error")
        self.assertIn("already exists in current session", result2["error"])

    def test_execute_create_temporary_table_catalog_failure(self):
        """Test temporary table creation with catalog failure"""
        plan = {
            "type": "CREATE_TEMPORARY_TABLE",
            "table": "temp_users",
            "columns": [
                {"name": "id", "type": "INTEGER", "constraints": ["PRIMARY KEY"]}
            ],
            "constraints": []
        }
        
        session_id = "session_123"
        
        # Setup mocks
        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.create_table.return_value = "Table creation failed"
        
        result = self.temp_table_manager.execute_create_temporary_table(plan, session_id)
        
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error"], "Table creation failed")

    def test_execute_drop_temporary_table_success(self):
        """Test successful temporary table drop"""
        # First create a temporary table
        create_plan = {
            "type": "CREATE_TEMPORARY_TABLE",
            "table": "temp_users",
            "columns": [
                {"name": "id", "type": "INTEGER", "constraints": ["PRIMARY KEY"]}
            ],
            "constraints": []
        }
        
        session_id = "session_123"
        
        # Setup mocks for creation
        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.create_table.return_value = True
        
        # Create the table
        create_result = self.temp_table_manager.execute_create_temporary_table(create_plan, session_id)
        self.assertEqual(create_result["status"], "success")
        
        # Now drop it
        drop_plan = {
            "type": "DROP_TEMPORARY_TABLE",
            "table": "temp_users"
        }
        
        # Setup mocks for drop
        self.mock_catalog.drop_table.return_value = True
        
        drop_result = self.temp_table_manager.execute_drop_temporary_table(drop_plan, session_id)
        
        self.assertEqual(drop_result["status"], "success")
        self.assertIn("dropped successfully", drop_result["message"])
        
        # Check that correct temporary table name was used
        expected_temp_name = f"_temp_{session_id}_temp_users"
        self.mock_catalog.drop_table.assert_called_once_with(expected_temp_name)

    def test_execute_drop_temporary_table_not_exists(self):
        """Test dropping temporary table that doesn't exist in session"""
        drop_plan = {
            "type": "DROP_TEMPORARY_TABLE",
            "table": "nonexistent_table"
        }
        
        session_id = "session_123"
        
        # Setup mocks
        self.mock_catalog.get_current_database.return_value = "test_db"
        
        result = self.temp_table_manager.execute_drop_temporary_table(drop_plan, session_id)
        
        self.assertEqual(result["status"], "error")
        self.assertIn("does not exist in current session", result["error"])

    def test_execute_drop_temporary_table_missing_name(self):
        """Test dropping temporary table with missing name"""
        drop_plan = {
            "type": "DROP_TEMPORARY_TABLE"
        }
        
        session_id = "session_123"
        
        result = self.temp_table_manager.execute_drop_temporary_table(drop_plan, session_id)
        
        self.assertEqual(result["status"], "error")
        self.assertIn("No table name specified", result["error"])

    def test_cleanup_session_tables_success(self):
        """Test successful session cleanup"""
        session_id = "session_123"
        
        # Create multiple temporary tables
        for table_name in ["temp_users", "temp_products", "temp_orders"]:
            plan = {
                "type": "CREATE_TEMPORARY_TABLE",
                "table": table_name,
                "columns": [{"name": "id", "type": "INTEGER", "constraints": ["PRIMARY KEY"]}],
                "constraints": []
            }
            
            # Setup mocks
            self.mock_catalog.get_current_database.return_value = "test_db"
            self.mock_catalog.create_table.return_value = True
            
            result = self.temp_table_manager.execute_create_temporary_table(plan, session_id)
            self.assertEqual(result["status"], "success")
        
        # Setup mock for cleanup
        self.mock_catalog.drop_table.return_value = True
        
        # Cleanup session
        self.temp_table_manager.cleanup_session_temp_tables(session_id)
        
        # Check that all 3 tables were dropped
        self.assertEqual(self.mock_catalog.drop_table.call_count, 3)
        
        # Check that session was removed from tracking
        self.assertNotIn(session_id, self.temp_table_manager._session_temp_tables)

    def test_cleanup_session_tables_partial_failure(self):
        """Test session cleanup with some table drop failures"""
        session_id = "session_123"
        
        # Create two temporary tables
        for table_name in ["temp_users", "temp_products"]:
            plan = {
                "type": "CREATE_TEMPORARY_TABLE",
                "table": table_name,
                "columns": [{"name": "id", "type": "INTEGER", "constraints": ["PRIMARY KEY"]}],
                "constraints": []
            }
            
            # Setup mocks
            self.mock_catalog.get_current_database.return_value = "test_db"
            self.mock_catalog.create_table.return_value = True
            
            result = self.temp_table_manager.execute_create_temporary_table(plan, session_id)
            self.assertEqual(result["status"], "success")
        
        # Setup mock for cleanup - first drop fails, second succeeds
        self.mock_catalog.drop_table.side_effect = [False, True]
        
        # Cleanup session
        self.temp_table_manager.cleanup_session_temp_tables(session_id)
        
        # Check that both drops were attempted
        self.assertEqual(self.mock_catalog.drop_table.call_count, 2)
        
        # Session should still be removed from tracking
        self.assertNotIn(session_id, self.temp_table_manager._session_temp_tables)

    def test_cleanup_session_tables_nonexistent_session(self):
        """Test cleanup of non-existent session"""
        # Cleanup non-existent session - should not raise exception
        self.temp_table_manager.cleanup_session_temp_tables("nonexistent_session")
        
        # Should not call drop_table
        self.mock_catalog.drop_table.assert_not_called()

    def test_list_session_temp_tables_success(self):
        """Test listing temporary tables for a session"""
        session_id = "session_123"
        
        # Create multiple temporary tables
        table_names = ["temp_users", "temp_products"]
        for table_name in table_names:
            plan = {
                "type": "CREATE_TEMPORARY_TABLE",
                "table": table_name,
                "columns": [{"name": "id", "type": "INTEGER", "constraints": ["PRIMARY KEY"]}],
                "constraints": []
            }
            
            # Setup mocks
            self.mock_catalog.get_current_database.return_value = "test_db"
            self.mock_catalog.create_table.return_value = True
            
            result = self.temp_table_manager.execute_create_temporary_table(plan, session_id)
            self.assertEqual(result["status"], "success")
        
        # List session tables
        result = self.temp_table_manager.list_session_temp_tables(session_id)
        
        self.assertEqual(result["status"], "success")
        self.assertIn("temp_tables", result)
        
        # Check that both tables are listed
        listed_tables = result["temp_tables"]
        expected_tables = table_names  # User-friendly names, not internal names
        
        for expected_table in expected_tables:
            self.assertIn(expected_table, listed_tables)

    def test_list_session_temp_tables_empty_session(self):
        """Test listing temporary tables for empty session"""
        result = self.temp_table_manager.list_session_temp_tables("empty_session")
        
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["temp_tables"], [])

    def test_session_isolation(self):
        """Test that temporary tables are isolated between sessions"""
        session1 = "session_123"
        session2 = "session_456"
        table_name = "temp_users"
        
        plan = {
            "type": "CREATE_TEMPORARY_TABLE",
            "table": table_name,
            "columns": [{"name": "id", "type": "INTEGER", "constraints": ["PRIMARY KEY"]}],
            "constraints": []
        }
        
        # Setup mocks
        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.create_table.return_value = True
        
        # Create table in both sessions
        result1 = self.temp_table_manager.execute_create_temporary_table(plan, session1)
        result2 = self.temp_table_manager.execute_create_temporary_table(plan, session2)
        
        self.assertEqual(result1["status"], "success")
        self.assertEqual(result2["status"], "success")
        
        # Check that different physical table names were used
        self.assertNotEqual(result1["temp_table_name"], result2["temp_table_name"])
        self.assertEqual(result1["temp_table_name"], f"_temp_{session1}_{table_name}")
        self.assertEqual(result2["temp_table_name"], f"_temp_{session2}_{table_name}")
        
        # List tables for each session
        list1 = self.temp_table_manager.list_session_temp_tables(session1)
        list2 = self.temp_table_manager.list_session_temp_tables(session2)
        
        self.assertEqual(len(list1["temp_tables"]), 1)
        self.assertEqual(len(list2["temp_tables"]), 1)
        # Both should contain the same user-friendly name since it's the same table name
        self.assertEqual(list1["temp_tables"][0], table_name)
        self.assertEqual(list2["temp_tables"][0], table_name)

    def test_execute_temporary_table_operation_create(self):
        """Test temporary table operation routing for CREATE"""
        plan = {
            "type": "CREATE_TEMPORARY_TABLE",
            "table": "temp_users",
            "columns": [{"name": "id", "type": "INTEGER", "constraints": ["PRIMARY KEY"]}],
            "constraints": []
        }
        
        session_id = "session_123"
        
        # Setup mocks
        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.create_table.return_value = True
        
        result = self.temp_table_manager.execute_temporary_table_operation(plan, session_id)
        
        self.assertEqual(result["status"], "success")

    def test_execute_temporary_table_operation_drop(self):
        """Test temporary table operation routing for DROP"""
        # First create a table
        create_plan = {
            "type": "CREATE_TEMPORARY_TABLE",
            "table": "temp_users",
            "columns": [{"name": "id", "type": "INTEGER", "constraints": ["PRIMARY KEY"]}],
            "constraints": []
        }
        
        session_id = "session_123"
        
        # Setup mocks
        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.create_table.return_value = True
        
        create_result = self.temp_table_manager.execute_temporary_table_operation(create_plan, session_id)
        self.assertEqual(create_result["status"], "success")
        
        # Now drop it
        drop_plan = {
            "type": "DROP_TEMPORARY_TABLE",
            "table": "temp_users"
        }
        
        self.mock_catalog.drop_table.return_value = True
        
        drop_result = self.temp_table_manager.execute_temporary_table_operation(drop_plan, session_id)
        
        self.assertEqual(drop_result["status"], "success")

    def test_execute_temporary_table_operation_unsupported(self):
        """Test temporary table operation routing for unsupported operation"""
        plan = {
            "type": "UNSUPPORTED_OPERATION"
        }
        
        session_id = "session_123"
        
        result = self.temp_table_manager.execute_temporary_table_operation(plan, session_id)
        
        self.assertEqual(result["status"], "error")
        self.assertIn("Unsupported temporary table operation", result["error"])


if __name__ == '__main__':
    unittest.main()
