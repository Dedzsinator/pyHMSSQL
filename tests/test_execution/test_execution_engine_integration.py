"""
Tests for the ExecutionEngine integration with new DDL operations.
"""

import pytest
import tempfile
import shutil
from execution_engine import ExecutionEngine
from catalog_manager import CatalogManager
from index_manager import IndexManager
from planner import Planner


class TestExecutionEngineIntegration:
    """Test ExecutionEngine integration with procedures, functions, triggers, and temp tables."""

    @pytest.fixture
    def setup_test_environment(self):
        """Set up test environment with temporary data directory."""
        temp_dir = tempfile.mkdtemp()
        catalog_manager = CatalogManager(data_dir=temp_dir)
        index_manager = IndexManager(buffer_pool=None)
        planner = Planner(catalog_manager, index_manager)
        execution_engine = ExecutionEngine(catalog_manager, index_manager, planner)

        # Create test database and table
        execution_engine.execute({"type": "CREATE_DATABASE", "database": "test_db"})
        execution_engine.execute({"type": "USE_DATABASE", "database": "test_db"})
        execution_engine.execute(
            {
                "type": "CREATE_TABLE",
                "table": "users",
                "columns": ["id INT PRIMARY KEY", "name VARCHAR(50)", "age INT"],
            }
        )

        yield execution_engine, catalog_manager

        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_procedure_operations(self, setup_test_environment):
        """Test procedure creation, execution, and dropping."""
        engine, catalog = setup_test_environment

        # Create procedure
        result = engine.execute(
            {
                "type": "CREATE_PROCEDURE",
                "procedure_name": "test_proc",
                "parameters": [{"name": "user_id", "type": "INT"}],
                "body": "INSERT INTO users (id, name, age) VALUES (@user_id, 'Test User', 25);",
            }
        )
        assert result["status"] == "success"

        # Call procedure
        result = engine.execute(
            {"type": "CALL_PROCEDURE", "procedure_name": "test_proc", "arguments": [1]}
        )
        assert result["status"] == "success"

        # Verify data was inserted
        select_result = engine.execute(
            {"type": "SELECT", "table": "users", "columns": ["*"]}
        )
        assert len(select_result["rows"]) == 1
        assert select_result["rows"][0][0] == 1  # id

        # Drop procedure
        result = engine.execute(
            {"type": "DROP_PROCEDURE", "procedure_name": "test_proc"}
        )
        assert result["status"] == "success"

    def test_function_operations(self, setup_test_environment):
        """Test function creation, evaluation, and dropping."""
        engine, catalog = setup_test_environment

        # Create function
        result = engine.execute(
            {
                "type": "CREATE_FUNCTION",
                "function_name": "get_user_name",
                "parameters": [{"name": "user_id", "type": "INT"}],
                "return_type": "VARCHAR(50)",
                "body": "SELECT name FROM users WHERE id = @user_id;",
            }
        )
        assert result["status"] == "success"

        # Insert test data
        engine.execute(
            {"type": "INSERT", "table": "users", "values": [[1, "John Doe", 30]]}
        )

        # Evaluate function
        result = engine.evaluate_function_call("get_user_name", [1])
        assert result["status"] == "success"

        # Drop function
        result = engine.execute(
            {"type": "DROP_FUNCTION", "function_name": "get_user_name"}
        )
        assert result["status"] == "success"

    def test_trigger_operations(self, setup_test_environment):
        """Test trigger creation and automatic firing."""
        engine, catalog = setup_test_environment

        # Create audit table
        engine.execute(
            {
                "type": "CREATE_TABLE",
                "table": "audit_log",
                "columns": ["id INT PRIMARY KEY", "action VARCHAR(10)", "user_id INT"],
            }
        )

        # Create trigger
        result = engine.execute(
            {
                "type": "CREATE_TRIGGER",
                "trigger_name": "audit_insert",
                "timing": "AFTER",
                "event": "INSERT",
                "table": "users",
                "body": "INSERT INTO audit_log (id, action, user_id) VALUES (1, 'INSERT', NEW.id);",
            }
        )
        assert result["status"] == "success"

        # Insert data (should fire trigger)
        result = engine.execute(
            {"type": "INSERT", "table": "users", "values": [[1, "Jane Doe", 25]]}
        )
        assert result["status"] == "success"

        # Drop trigger
        result = engine.execute(
            {"type": "DROP_TRIGGER", "trigger_name": "audit_insert"}
        )
        assert result["status"] == "success"

    def test_temp_table_operations(self, setup_test_environment):
        """Test temporary table creation and session management."""
        engine, catalog = setup_test_environment

        # Create temporary table
        result = engine.execute(
            {
                "type": "CREATE_TEMP_TABLE",
                "table": "temp_users",
                "columns": ["id INT", "name VARCHAR(50)"],
                "session_id": "test_session",
            }
        )
        assert result["status"] == "success"

        # Insert into temp table
        result = engine.execute(
            {
                "type": "INSERT",
                "table": "temp_users",
                "values": [[1, "Temp User"]],
                "session_id": "test_session",
            }
        )
        assert result["status"] == "success"

        # Query temp table
        result = engine.execute(
            {
                "type": "SELECT",
                "table": "temp_users",
                "columns": ["*"],
                "session_id": "test_session",
            }
        )
        assert result["status"] == "success"
        assert len(result["rows"]) == 1

        # Clean up session
        engine.cleanup_session_resources("test_session")

        # Verify temp table is gone
        result = engine.execute(
            {
                "type": "SELECT",
                "table": "temp_users",
                "columns": ["*"],
                "session_id": "test_session",
            }
        )
        assert result["status"] == "error"

    def test_show_operations(self, setup_test_environment):
        """Test enhanced SHOW commands."""
        engine, catalog = setup_test_environment

        # Create test objects
        engine.execute(
            {
                "type": "CREATE_PROCEDURE",
                "procedure_name": "test_proc",
                "parameters": [],
                "body": "SELECT 1;",
            }
        )

        engine.execute(
            {
                "type": "CREATE_FUNCTION",
                "function_name": "test_func",
                "parameters": [],
                "return_type": "INT",
                "body": "SELECT 1;",
            }
        )

        # Test SHOW PROCEDURES
        result = engine.execute({"type": "SHOW", "object": "PROCEDURES"})
        assert result["status"] == "success"
        assert "procedures" in result

        # Test SHOW FUNCTIONS
        result = engine.execute({"type": "SHOW", "object": "FUNCTIONS"})
        assert result["status"] == "success"
        assert "functions" in result

        # Test SHOW TRIGGERS
        result = engine.execute({"type": "SHOW", "object": "TRIGGERS"})
        assert result["status"] == "success"
        assert "triggers" in result

    def test_dml_trigger_integration(self, setup_test_environment):
        """Test that DML operations automatically fire triggers."""
        engine, catalog = setup_test_environment

        # Create log table
        engine.execute(
            {
                "type": "CREATE_TABLE",
                "table": "change_log",
                "columns": [
                    "id INT PRIMARY KEY",
                    "table_name VARCHAR(50)",
                    "operation VARCHAR(10)",
                ],
            }
        )

        # Create triggers for all DML operations
        for operation in ["INSERT", "UPDATE", "DELETE"]:
            engine.execute(
                {
                    "type": "CREATE_TRIGGER",
                    "trigger_name": f"log_{operation.lower()}",
                    "timing": "AFTER",
                    "event": operation,
                    "table": "users",
                    "body": f"INSERT INTO change_log (id, table_name, operation) VALUES ({hash(operation) % 1000}, 'users', '{operation}');",
                }
            )

        # Test INSERT trigger
        engine.execute(
            {"type": "INSERT", "table": "users", "values": [[1, "Test User", 30]]}
        )

        # Test UPDATE trigger
        engine.execute(
            {
                "type": "UPDATE",
                "table": "users",
                "set": [("name", "'Updated User'")],
                "condition": "id = 1",
            }
        )

        # Test DELETE trigger
        engine.execute({"type": "DELETE", "table": "users", "condition": "id = 1"})

        # Verify triggers fired by checking log
        result = engine.execute(
            {"type": "SELECT", "table": "change_log", "columns": ["operation"]}
        )
        assert result["status"] == "success"
        operations = [row[0] for row in result["rows"]]
        assert "INSERT" in operations
        assert "UPDATE" in operations
        assert "DELETE" in operations
