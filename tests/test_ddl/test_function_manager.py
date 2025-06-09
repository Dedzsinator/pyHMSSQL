"""
Tests for FunctionManager operations.
"""

import pytest
import tempfile
import shutil
from catalog_manager import CatalogManager
from ddl_processor.function_manager import FunctionManager
from execution_engine import ExecutionEngine
from index_manager import IndexManager
from planner import Planner


class TestFunctionManager:
    """Test FunctionManager functionality."""

    @pytest.fixture
    def setup_test_environment(self):
        """Set up test environment with temporary data directory."""
        temp_dir = tempfile.mkdtemp()
        catalog_manager = CatalogManager(data_dir=temp_dir)
        index_manager = IndexManager(buffer_pool=None)
        planner = Planner(catalog_manager, index_manager)
        execution_engine = ExecutionEngine(catalog_manager, index_manager, planner)
        function_manager = FunctionManager(catalog_manager)
        function_manager.execution_engine = execution_engine

        # Create test database and table
        catalog_manager.create_database("test_db")
        catalog_manager.set_current_database("test_db")
        catalog_manager.create_table(
            "employees",
            [
                {"name": "id", "type": "INT", "primary_key": True},
                {"name": "name", "type": "VARCHAR(50)"},
                {"name": "salary", "type": "DECIMAL(10,2)"},
                {"name": "department", "type": "VARCHAR(30)"},
            ],
            [],
        )

        # Insert test data
        catalog_manager.insert_record(
            "employees", {"id": 1, "name": "John", "salary": 50000, "department": "IT"}
        )
        catalog_manager.insert_record(
            "employees", {"id": 2, "name": "Jane", "salary": 60000, "department": "HR"}
        )
        catalog_manager.insert_record(
            "employees", {"id": 3, "name": "Bob", "salary": 55000, "department": "IT"}
        )

        yield function_manager, catalog_manager, execution_engine

        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_create_function(self, setup_test_environment):
        """Test creating a user-defined function."""
        function_manager, catalog_manager, execution_engine = setup_test_environment

        plan = {
            "function_name": "get_employee_name",
            "parameters": [{"name": "emp_id", "type": "INT"}],
            "return_type": "VARCHAR(50)",
            "body": "SELECT name FROM employees WHERE id = @emp_id;",
        }

        result = function_manager.create_function(plan)
        assert result["status"] == "success"
        assert "get_employee_name" in result["message"]

    def test_create_function_validation(self, setup_test_environment):
        """Test function creation validation."""
        function_manager, catalog_manager, execution_engine = setup_test_environment

        # Test missing function name
        plan = {"parameters": [], "return_type": "INT", "body": "SELECT 1;"}
        result = function_manager.create_function(plan)
        assert result["status"] == "error"
        assert "function name" in result["error"].lower()

        # Test missing return type
        plan = {"function_name": "test_func", "parameters": [], "body": "SELECT 1;"}
        result = function_manager.create_function(plan)
        assert result["status"] == "error"
        assert "return type" in result["error"].lower()

        # Test empty body
        plan = {
            "function_name": "empty_func",
            "parameters": [],
            "return_type": "INT",
            "body": "",
        }
        result = function_manager.create_function(plan)
        assert result["status"] == "error"
        assert "body cannot be empty" in result["error"].lower()

    def test_call_function(self, setup_test_environment):
        """Test calling a user-defined function."""
        function_manager, catalog_manager, execution_engine = setup_test_environment

        # Create function
        create_plan = {
            "function_name": "get_employee_name",
            "parameters": [{"name": "emp_id", "type": "INT"}],
            "return_type": "VARCHAR(50)",
            "body": "SELECT name FROM employees WHERE id = @emp_id;",
        }
        function_manager.create_function(create_plan)

        # Call function
        result = function_manager.call_function("get_employee_name", [1])
        assert result["status"] == "success"
        assert "John" in str(result.get("result", ""))

    def test_call_function_parameter_validation(self, setup_test_environment):
        """Test function call parameter validation."""
        function_manager, catalog_manager, execution_engine = setup_test_environment

        # Create function with specific parameter count
        create_plan = {
            "function_name": "test_func",
            "parameters": [
                {"name": "param1", "type": "INT"},
                {"name": "param2", "type": "VARCHAR(50)"},
            ],
            "return_type": "INT",
            "body": "SELECT 1;",
        }
        function_manager.create_function(create_plan)

        # Call with wrong number of arguments
        result = function_manager.call_function("test_func", [1])  # Need 2 args, gave 1
        assert result["status"] == "error"
        assert "parameter count mismatch" in result["error"].lower()

    def test_drop_function(self, setup_test_environment):
        """Test dropping a user-defined function."""
        function_manager, catalog_manager, execution_engine = setup_test_environment

        # Create function
        create_plan = {
            "function_name": "test_func",
            "parameters": [],
            "return_type": "INT",
            "body": "SELECT 42;",
        }
        function_manager.create_function(create_plan)

        # Drop function
        drop_plan = {"function_name": "test_func"}
        result = function_manager.drop_function(drop_plan)
        assert result["status"] == "success"
        assert "test_func" in result["message"]

        # Verify function is gone
        functions = function_manager.list_functions()
        assert "test_func" not in functions.get("functions", {})

    def test_list_functions(self, setup_test_environment):
        """Test listing functions."""
        function_manager, catalog_manager, execution_engine = setup_test_environment

        # Create multiple functions
        for i in range(3):
            plan = {
                "function_name": f"func_{i}",
                "parameters": [],
                "return_type": "INT",
                "body": f"SELECT {i};",
            }
            function_manager.create_function(plan)

        # List functions
        result = function_manager.list_functions()
        assert result["status"] == "success"
        functions = result["functions"]
        assert len(functions) == 3
        assert "func_0" in functions
        assert "func_1" in functions
        assert "func_2" in functions

    def test_function_with_multiple_parameters(self, setup_test_environment):
        """Test function with multiple parameters."""
        function_manager, catalog_manager, execution_engine = setup_test_environment

        # Create function that calculates salary bonus
        create_plan = {
            "function_name": "calculate_bonus",
            "parameters": [
                {"name": "base_salary", "type": "DECIMAL(10,2)"},
                {"name": "bonus_percent", "type": "DECIMAL(5,2)"},
            ],
            "return_type": "DECIMAL(10,2)",
            "body": "SELECT @base_salary * (@bonus_percent / 100);",
        }
        result = function_manager.create_function(create_plan)
        assert result["status"] == "success"

        # Call function with specific values
        result = function_manager.call_function("calculate_bonus", [50000, 10])
        assert result["status"] == "success"

    def test_function_with_table_query(self, setup_test_environment):
        """Test function that queries table data."""
        function_manager, catalog_manager, execution_engine = setup_test_environment

        # Create function that gets department salary average
        create_plan = {
            "function_name": "avg_department_salary",
            "parameters": [{"name": "dept", "type": "VARCHAR(30)"}],
            "return_type": "DECIMAL(10,2)",
            "body": "SELECT AVG(salary) FROM employees WHERE department = @dept;",
        }
        result = function_manager.create_function(create_plan)
        assert result["status"] == "success"

        # Call function
        result = function_manager.call_function("avg_department_salary", ["IT"])
        assert result["status"] == "success"

    def test_function_return_type_validation(self, setup_test_environment):
        """Test function return type validation."""
        function_manager, catalog_manager, execution_engine = setup_test_environment

        # Create function with specific return type
        create_plan = {
            "function_name": "return_string",
            "parameters": [],
            "return_type": "VARCHAR(20)",
            "body": "SELECT 'Hello World';",
        }
        result = function_manager.create_function(create_plan)
        assert result["status"] == "success"

        # Call function and verify return
        result = function_manager.call_function("return_string", [])
        assert result["status"] == "success"

    def test_function_parameter_substitution(self, setup_test_environment):
        """Test parameter substitution in function bodies."""
        function_manager, catalog_manager, execution_engine = setup_test_environment

        # Create function with parameter substitution
        create_plan = {
            "function_name": "get_employee_info",
            "parameters": [{"name": "emp_id", "type": "INT"}],
            "return_type": "VARCHAR(100)",
            "body": "SELECT CONCAT(name, ' - ', department) FROM employees WHERE id = @emp_id;",
        }
        function_manager.create_function(create_plan)

        # Call with specific ID
        result = function_manager.call_function("get_employee_info", [2])
        assert result["status"] == "success"
