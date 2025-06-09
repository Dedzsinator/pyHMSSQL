import unittest
import tempfile
import os
import json
from unittest.mock import MagicMock, patch

from ddl_processor.trigger_manager import TriggerManager
from catalog_manager import CatalogManager


class TestTriggerManager(unittest.TestCase):
    def setUp(self):
        """Set up test environment with temporary directory"""
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test.db")

        # Create mock dependencies
        self.mock_catalog = MagicMock(spec=CatalogManager)
        self.mock_execution_engine = MagicMock()

        # Initialize TriggerManager
        self.trigger_manager = TriggerManager(
            self.mock_catalog, self.mock_execution_engine
        )

    def tearDown(self):
        """Clean up test environment"""
        import shutil

        shutil.rmtree(self.test_dir)

    def test_execute_create_trigger_success(self):
        """Test successful trigger creation via execute operation"""
        plan = {
            "type": "CREATE_TRIGGER",
            "trigger_name": "test_trigger",
            "timing": "BEFORE",
            "event": "INSERT",
            "table": "users",
            "body": "UPDATE audit_log SET count = count + 1",
        }

        # Setup mocks
        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.list_tables.return_value = ["users", "audit_log"]
        self.mock_catalog.create_trigger.return_value = True

        result = self.trigger_manager.execute_create_trigger(plan)

        self.assertEqual(result["status"], "success")
        self.assertIn("created successfully", result["message"])
        self.mock_catalog.create_trigger.assert_called_once_with(
            "test_trigger",
            "BEFORE",
            "INSERT",
            "users",
            "UPDATE audit_log SET count = count + 1",
        )

    def test_execute_create_trigger_missing_name(self):
        """Test trigger creation with missing name"""
        plan = {
            "type": "CREATE_TRIGGER",
            "timing": "BEFORE",
            "event": "INSERT",
            "table": "users",
            "body": "UPDATE audit_log SET count = count + 1",
        }

        result = self.trigger_manager.execute_create_trigger(plan)

        self.assertEqual(result["status"], "error")
        self.assertIn("No trigger name specified", result["error"])

    def test_execute_create_trigger_missing_table(self):
        """Test trigger creation with missing table"""
        plan = {
            "type": "CREATE_TRIGGER",
            "trigger_name": "test_trigger",
            "timing": "BEFORE",
            "event": "INSERT",
            "body": "UPDATE audit_log SET count = count + 1",
        }

        result = self.trigger_manager.execute_create_trigger(plan)

        self.assertEqual(result["status"], "error")
        self.assertIn("No table specified", result["error"])

    def test_execute_create_trigger_invalid_timing(self):
        """Test trigger creation with invalid timing"""
        plan = {
            "type": "CREATE_TRIGGER",
            "trigger_name": "test_trigger",
            "timing": "DURING",  # Invalid timing
            "event": "INSERT",
            "table": "users",
            "body": "UPDATE audit_log SET count = count + 1",
        }

        # Setup mocks
        self.mock_catalog.get_current_database.return_value = "test_db"

        result = self.trigger_manager.execute_create_trigger(plan)

        self.assertEqual(result["status"], "error")
        self.assertIn("timing must be BEFORE or AFTER", result["error"])

    def test_execute_create_trigger_invalid_event(self):
        """Test trigger creation with invalid event"""
        plan = {
            "type": "CREATE_TRIGGER",
            "trigger_name": "test_trigger",
            "timing": "BEFORE",
            "event": "SELECT",  # Invalid event
            "table": "users",
            "body": "UPDATE audit_log SET count = count + 1",
        }

        # Setup mocks
        self.mock_catalog.get_current_database.return_value = "test_db"

        result = self.trigger_manager.execute_create_trigger(plan)

        self.assertEqual(result["status"], "error")
        self.assertIn("event must be INSERT, UPDATE, or DELETE", result["error"])

    def test_execute_create_trigger_table_not_exists(self):
        """Test trigger creation with non-existent table"""
        plan = {
            "type": "CREATE_TRIGGER",
            "trigger_name": "test_trigger",
            "timing": "BEFORE",
            "event": "INSERT",
            "table": "nonexistent_table",
            "body": "UPDATE audit_log SET count = count + 1",
        }

        # Setup mocks
        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.list_tables.return_value = ["users", "audit_log"]

        result = self.trigger_manager.execute_create_trigger(plan)

        self.assertEqual(result["status"], "error")
        self.assertIn("does not exist", result["error"])

    def test_execute_create_trigger_empty_body(self):
        """Test trigger creation with empty body"""
        plan = {
            "type": "CREATE_TRIGGER",
            "trigger_name": "test_trigger",
            "timing": "BEFORE",
            "event": "INSERT",
            "table": "users",
            "body": "   ",  # Empty/whitespace body
        }

        # Setup mocks
        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.list_tables.return_value = ["users", "audit_log"]

        result = self.trigger_manager.execute_create_trigger(plan)

        self.assertEqual(result["status"], "error")
        self.assertIn("cannot be empty", result["error"])

    def test_execute_drop_trigger_success(self):
        """Test successful trigger drop"""
        plan = {"type": "DROP_TRIGGER", "trigger_name": "test_trigger"}

        # Setup mocks
        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.drop_trigger.return_value = True

        result = self.trigger_manager.execute_drop_trigger(plan)

        self.assertEqual(result["status"], "success")
        self.assertIn("dropped successfully", result["message"])
        self.mock_catalog.drop_trigger.assert_called_once_with("test_trigger")

    def test_execute_drop_trigger_missing_name(self):
        """Test trigger drop with missing name"""
        plan = {"type": "DROP_TRIGGER"}

        result = self.trigger_manager.execute_drop_trigger(plan)

        self.assertEqual(result["status"], "error")
        self.assertIn("No trigger name specified", result["error"])

    def test_execute_drop_trigger_not_exists(self):
        """Test dropping non-existent trigger"""
        plan = {"type": "DROP_TRIGGER", "trigger_name": "nonexistent_trigger"}

        # Setup mocks
        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.drop_trigger.return_value = "Trigger not found"

        result = self.trigger_manager.execute_drop_trigger(plan)

        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error"], "Trigger not found")

    def test_fire_triggers_success(self):
        """Test successful trigger firing"""
        # Setup mock triggers
        triggers = {
            "before_insert_trigger": {
                "event": "INSERT",
                "timing": "BEFORE",
                "body": "UPDATE counters SET count = count + 1",
            },
            "after_insert_trigger": {
                "event": "INSERT",
                "timing": "AFTER",
                "body": "INSERT INTO audit_log (action) VALUES ('INSERT')",
            },
        }

        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.get_triggers_for_table.return_value = triggers

        # Mock the execution engine execute_plan method
        self.mock_execution_engine.execute_plan = MagicMock()

        # Fire BEFORE INSERT triggers
        self.trigger_manager.fire_triggers("INSERT", "users", "BEFORE")

        # Should have attempted to parse and execute the trigger
        self.mock_catalog.get_triggers_for_table.assert_called_with("users")

    def test_fire_triggers_no_matching_triggers(self):
        """Test firing triggers when no matching triggers exist"""
        triggers = {
            "before_update_trigger": {
                "event": "UPDATE",
                "timing": "BEFORE",
                "body": "UPDATE counters SET count = count + 1",
            }
        }

        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.get_triggers_for_table.return_value = triggers

        # Fire INSERT triggers (should not match UPDATE trigger)
        self.trigger_manager.fire_triggers("INSERT", "users", "BEFORE")

        # Should have queried for triggers but not executed any
        self.mock_catalog.get_triggers_for_table.assert_called_with("users")

    def test_fire_triggers_with_old_new_data(self):
        """Test firing triggers with OLD and NEW data replacement"""
        triggers = {
            "update_trigger": {
                "event": "UPDATE",
                "timing": "BEFORE",
                "body": "INSERT INTO history (old_name, new_name) VALUES (OLD.name, NEW.name)",
            }
        }

        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.get_triggers_for_table.return_value = triggers

        old_data = {"name": "John", "age": 25}
        new_data = {"name": "Jane", "age": 26}

        # Fire UPDATE triggers with data
        self.trigger_manager.fire_triggers(
            "UPDATE", "users", "BEFORE", old_data, new_data
        )

        # Should have queried for triggers
        self.mock_catalog.get_triggers_for_table.assert_called_with("users")

    def test_list_triggers_all(self):
        """Test listing all triggers"""
        mock_triggers = {
            "trigger1": {"event": "INSERT", "timing": "BEFORE", "table": "users"},
            "trigger2": {"event": "UPDATE", "timing": "AFTER", "table": "products"},
        }

        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.list_triggers.return_value = mock_triggers

        result = self.trigger_manager.list_triggers()

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["triggers"], mock_triggers)
        self.mock_catalog.list_triggers.assert_called_once()

    def test_list_triggers_for_table(self):
        """Test listing triggers for specific table"""
        mock_triggers = {
            "trigger1": {"event": "INSERT", "timing": "BEFORE", "table": "users"}
        }

        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.get_triggers_for_table.return_value = mock_triggers

        result = self.trigger_manager.list_triggers("users")

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["triggers"], mock_triggers)
        self.mock_catalog.get_triggers_for_table.assert_called_once_with("users")

    def test_get_trigger_definition_success(self):
        """Test getting trigger definition"""
        mock_trigger = {
            "event": "INSERT",
            "timing": "BEFORE",
            "table": "users",
            "body": "UPDATE counters SET count = count + 1",
        }

        self.mock_catalog.get_trigger.return_value = mock_trigger

        result = self.trigger_manager.get_trigger_definition("test_trigger")

        self.assertEqual(result, mock_trigger)
        self.mock_catalog.get_trigger.assert_called_once_with("test_trigger")

    def test_get_trigger_definition_not_found(self):
        """Test getting non-existent trigger definition"""
        self.mock_catalog.get_trigger.return_value = None

        result = self.trigger_manager.get_trigger_definition("nonexistent_trigger")

        self.assertIsNone(result)

    def test_execute_trigger_operation_create(self):
        """Test trigger operation routing for CREATE"""
        plan = {
            "type": "CREATE_TRIGGER",
            "trigger_name": "test_trigger",
            "timing": "BEFORE",
            "event": "INSERT",
            "table": "users",
            "body": "UPDATE audit_log SET count = count + 1",
        }

        # Setup mocks
        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.list_tables.return_value = ["users", "audit_log"]
        self.mock_catalog.create_trigger.return_value = True

        result = self.trigger_manager.execute_trigger_operation(plan)

        self.assertEqual(result["status"], "success")

    def test_execute_trigger_operation_drop(self):
        """Test trigger operation routing for DROP"""
        plan = {"type": "DROP_TRIGGER", "trigger_name": "test_trigger"}

        # Setup mocks
        self.mock_catalog.get_current_database.return_value = "test_db"
        self.mock_catalog.drop_trigger.return_value = True

        result = self.trigger_manager.execute_trigger_operation(plan)

        self.assertEqual(result["status"], "success")

    def test_execute_trigger_operation_unsupported(self):
        """Test trigger operation routing for unsupported operation"""
        plan = {"type": "UNSUPPORTED_OPERATION"}

        result = self.trigger_manager.execute_trigger_operation(plan)

        self.assertEqual(result["status"], "error")
        self.assertIn("Unsupported trigger operation", result["error"])


if __name__ == "__main__":
    unittest.main()
