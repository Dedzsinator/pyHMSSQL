"""
Tests for ProcedureManager operations.
"""
import pytest
import tempfile
import shutil
from catalog_manager import CatalogManager
from ddl_processor.procedure_manager import ProcedureManager
from execution_engine import ExecutionEngine
from index_manager import IndexManager
from planner import Planner


class TestProcedureManager:
    """Test ProcedureManager functionality."""

    @pytest.fixture
    def setup_test_environment(self):
        """Set up test environment with temporary data directory."""
        temp_dir = tempfile.mkdtemp()
        catalog_manager = CatalogManager(data_dir=temp_dir)
        index_manager = IndexManager(buffer_pool=None)
        planner = Planner(catalog_manager, index_manager)
        execution_engine = ExecutionEngine(catalog_manager, index_manager, planner)
        procedure_manager = ProcedureManager(catalog_manager)
        procedure_manager.execution_engine = execution_engine
        
        # Create test database and table
        catalog_manager.create_database("test_db")
        catalog_manager.set_current_database("test_db")
        catalog_manager.create_table("users", [
            {"name": "id", "type": "INT", "primary_key": True},
            {"name": "name", "type": "VARCHAR(50)"},
            {"name": "age", "type": "INT"}
        ], [])
        
        yield procedure_manager, catalog_manager, execution_engine
        
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_create_procedure(self, setup_test_environment):
        """Test creating a stored procedure."""
        procedure_manager, catalog_manager, execution_engine = setup_test_environment
        
        plan = {
            "procedure_name": "add_user",
            "parameters": [
                {"name": "user_id", "type": "INT"},
                {"name": "user_name", "type": "VARCHAR(50)"},
                {"name": "user_age", "type": "INT"}
            ],
            "body": "INSERT INTO users (id, name, age) VALUES (@user_id, @user_name, @user_age);"
        }
        
        result = procedure_manager.create_procedure(plan)
        assert result["status"] == "success"
        assert "add_user" in result["message"]

    def test_create_procedure_validation(self, setup_test_environment):
        """Test procedure creation validation."""
        procedure_manager, catalog_manager, execution_engine = setup_test_environment
        
        # Test missing procedure name
        plan = {
            "parameters": [],
            "body": "SELECT 1;"
        }
        result = procedure_manager.create_procedure(plan)
        assert result["status"] == "error"
        assert "procedure name" in result["error"].lower()
        
        # Test empty body
        plan = {
            "procedure_name": "empty_proc",
            "parameters": [],
            "body": ""
        }
        result = procedure_manager.create_procedure(plan)
        assert result["status"] == "error"
        assert "body cannot be empty" in result["error"].lower()

    def test_call_procedure(self, setup_test_environment):
        """Test calling a stored procedure."""
        procedure_manager, catalog_manager, execution_engine = setup_test_environment
        
        # Create procedure
        create_plan = {
            "procedure_name": "add_user",
            "parameters": [
                {"name": "user_id", "type": "INT"},
                {"name": "user_name", "type": "VARCHAR(50)"}
            ],
            "body": "INSERT INTO users (id, name, age) VALUES (@user_id, @user_name, 25);"
        }
        procedure_manager.create_procedure(create_plan)
        
        # Call procedure
        call_plan = {
            "procedure_name": "add_user",
            "arguments": [1, "John Doe"]
        }
        result = procedure_manager.call_procedure(call_plan)
        assert result["status"] == "success"
        
        # Verify data was inserted
        users = catalog_manager.query_with_condition("users", [], ["*"])
        assert len(users) == 1
        assert users[0]["id"] == 1
        assert users[0]["name"] == "John Doe"

    def test_call_procedure_parameter_mismatch(self, setup_test_environment):
        """Test calling procedure with wrong number of parameters."""
        procedure_manager, catalog_manager, execution_engine = setup_test_environment
        
        # Create procedure with 2 parameters
        create_plan = {
            "procedure_name": "test_proc",
            "parameters": [
                {"name": "param1", "type": "INT"},
                {"name": "param2", "type": "VARCHAR(50)"}
            ],
            "body": "SELECT @param1, @param2;"
        }
        procedure_manager.create_procedure(create_plan)
        
        # Call with wrong number of arguments
        call_plan = {
            "procedure_name": "test_proc",
            "arguments": [1]  # Only 1 argument, need 2
        }
        result = procedure_manager.call_procedure(call_plan)
        assert result["status"] == "error"
        assert "expects 2 arguments, got 1" in result["error"]

    def test_drop_procedure(self, setup_test_environment):
        """Test dropping a stored procedure."""
        procedure_manager, catalog_manager, execution_engine = setup_test_environment
        
        # Create procedure
        create_plan = {
            "procedure_name": "test_proc",
            "parameters": [],
            "body": "SELECT 1;"
        }
        procedure_manager.create_procedure(create_plan)
        
        # Drop procedure
        drop_plan = {"procedure_name": "test_proc"}
        result = procedure_manager.drop_procedure(drop_plan)
        assert result["status"] == "success"
        assert "test_proc" in result["message"]
        
        # Verify procedure is gone
        procedures = procedure_manager.list_procedures()
        assert "test_proc" not in procedures.get("procedures", {})

    def test_list_procedures(self, setup_test_environment):
        """Test listing procedures."""
        procedure_manager, catalog_manager, execution_engine = setup_test_environment
        
        # Create multiple procedures
        for i in range(3):
            plan = {
                "procedure_name": f"proc_{i}",
                "parameters": [],
                "body": f"SELECT {i};"
            }
            procedure_manager.create_procedure(plan)
        
        # List procedures
        result = procedure_manager.list_procedures()
        assert result["status"] == "success"
        procedures = result["procedures"]
        assert len(procedures) == 3
        assert "proc_0" in procedures
        assert "proc_1" in procedures
        assert "proc_2" in procedures

    def test_procedure_with_complex_body(self, setup_test_environment):
        """Test procedure with multiple SQL statements."""
        procedure_manager, catalog_manager, execution_engine = setup_test_environment
        
        # Create procedure with multiple statements
        create_plan = {
            "procedure_name": "complex_proc",
            "parameters": [{"name": "base_id", "type": "INT"}],
            "body": """
                INSERT INTO users (id, name, age) VALUES (@base_id, 'User1', 20);
                INSERT INTO users (id, name, age) VALUES (@base_id + 1, 'User2', 21);
                SELECT COUNT(*) FROM users;
            """
        }
        result = procedure_manager.create_procedure(create_plan)
        assert result["status"] == "success"
        
        # Call the complex procedure
        call_plan = {
            "procedure_name": "complex_proc",
            "arguments": [10]
        }
        result = procedure_manager.call_procedure(call_plan)
        assert result["status"] == "success"

    def test_procedure_parameter_substitution(self, setup_test_environment):
        """Test parameter substitution in procedure bodies."""
        procedure_manager, catalog_manager, execution_engine = setup_test_environment
        
        # Create procedure with parameter substitution
        create_plan = {
            "procedure_name": "param_test",
            "parameters": [
                {"name": "id_val", "type": "INT"},
                {"name": "name_val", "type": "VARCHAR(50)"},
                {"name": "age_val", "type": "INT"}
            ],
            "body": "INSERT INTO users (id, name, age) VALUES (@id_val, @name_val, @age_val);"
        }
        procedure_manager.create_procedure(create_plan)
        
        # Call with specific values
        call_plan = {
            "procedure_name": "param_test",
            "arguments": [42, "Test User", 30]
        }
        result = procedure_manager.call_procedure(call_plan)
        assert result["status"] == "success"
        
        # Verify correct substitution
        users = catalog_manager.query_with_condition("users", [], ["*"])
        assert len(users) == 1
        assert users[0]["id"] == 42
        assert users[0]["name"] == "Test User"
        assert users[0]["age"] == 30
