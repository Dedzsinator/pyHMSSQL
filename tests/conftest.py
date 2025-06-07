"""
Shared test fixtures and configuration for the DBMS test suite.
"""
import os
import sys
import tempfile
import shutil
import logging
import time
import types
import pytest

# Get the absolute path to the server directory
project_root = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
server_dir = os.path.join(project_root, 'server')

# Add server directory to path so that imports from within server work correctly
if server_dir not in sys.path:
    sys.path.insert(0, server_dir)

# Add project root to path as well
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from catalog_manager import CatalogManager
from execution_engine import ExecutionEngine
from parser import SQLParser
from planner import Planner
from optimizer import Optimizer
from ddl_processor.index_manager import IndexManager
from ddl_processor.schema_manager import SchemaManager
from transaction.transaction_manager import TransactionManager
from query_processor.join_executor import JoinExecutor

# Create a mock Visualizer class for tests
class MockVisualizer:
    """Mock implementation of Visualizer for testing."""

    def __init__(self, catalog_manager=None, index_manager=None):
        """Initialize mock visualizer."""
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager

    def execute_visualize(self, plan):
        """Mock visualization execution."""
        return {"status": "success", "message": "Mock visualization"}

    def visualize_index(self, index_name):
        """Mock index visualization."""
        return {
            "status": "success",
            "index_name": index_name,
            "visualization": "Mock visualization"
            }

    def visualize_database(self, db_name=None):
        """Mock database visualization."""
        return {
            "status": "success",
            "database": db_name or "current",
            "visualization": "Mock visualization"
            }

utils_module = types.ModuleType('utils')

# Add visualizer module
visualizer_module = types.ModuleType('utils.visualizer')
visualizer_module.Visualizer = MockVisualizer
utils_module.visualizer = visualizer_module

# Add sql_helpers module with required functions
sql_helpers_module = types.ModuleType('utils.sql_helpers')

def parse_simple_condition(condition):
    """Mock implementation of parse_simple_condition."""
    conditions = []
    if condition:
        parts = condition.split('=')
        if len(parts) == 2:
            col = parts[0].strip()
            val = parts[1].strip()
            try:
                val = int(val)
            except ValueError:
                if (val.startswith("'") and val.endswith("'")) or \
                    (val.startswith('"') and val.endswith('"')):
                    val = val[1:-1]

            conditions.append({
                "column": col,
                "operator": "=",
                "value": val
            })
    return conditions

def check_database_selected(catalog_manager):
    """Mock implementation of check_database_selected."""
    db_name = catalog_manager.get_current_database()
    if not db_name:
        return {
            "error": "No database selected. Use 'USE database_name' first.",
            "status": "error",
        }
    return None

# Add functions to the module
sql_helpers_module.parse_simple_condition = parse_simple_condition
sql_helpers_module.check_database_selected = check_database_selected
utils_module.sql_helpers = sql_helpers_module

# Register all modules
sys.modules['utils'] = utils_module
sys.modules['utils.visualizer'] = visualizer_module
sys.modules['utils.sql_helpers'] = sql_helpers_module

# Configure logging for tests
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

@pytest.fixture
def temp_data_dir():
    """Create a temporary directory for test data."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Clean up after test
    shutil.rmtree(temp_dir)

@pytest.fixture
def catalog_manager(temp_data_dir):
    """Create a catalog manager with a temporary data directory."""
    cm = CatalogManager(data_dir=temp_data_dir)
    # Create a test database
    cm.create_database("test_db")
    cm.set_current_database("test_db")
    yield cm

@pytest.fixture
def index_manager(catalog_manager):
    """Create an index manager for testing."""
    return IndexManager(catalog_manager)

@pytest.fixture
def parser():
    """Create an SQL parser for testing."""
    return SQLParser()

@pytest.fixture
def planner(catalog_manager, index_manager):
    """Create a query planner for testing."""
    return Planner(catalog_manager, index_manager)

@pytest.fixture
def optimizer(catalog_manager, index_manager):
    """Create a query optimizer for testing."""
    return Optimizer(catalog_manager, index_manager)

@pytest.fixture
def execution_engine(catalog_manager, index_manager, planner, transaction_manager):
    """Create an execution engine for testing."""
    engine = ExecutionEngine(catalog_manager, index_manager, planner)
    # Use the same transaction manager instance for consistency in tests
    engine.transaction_manager = transaction_manager
    engine.dml_executor.transaction_manager = transaction_manager
    return engine

@pytest.fixture
def schema_manager(catalog_manager):
    """Create a schema manager for testing."""
    return SchemaManager(catalog_manager)

@pytest.fixture
def transaction_manager(catalog_manager):
    """Create a transaction manager for testing."""
    return TransactionManager(catalog_manager)

@pytest.fixture
def test_table(catalog_manager):
    """Create a test table with sample data."""
    columns = [
        {"name": "id", "type": "INT", "primary_key": True, "nullable": False},
        {"name": "name", "type": "TEXT", "nullable": False},
        {"name": "email", "type": "TEXT", "nullable": True},
        {"name": "age", "type": "INT", "nullable": True},
    ]
    catalog_manager.create_table("customers", columns)

    # Insert sample data
    catalog_manager.insert_record("customers", {
        "id": 1,
        "name": "John Doe",
        "email": "john@example.com",
        "age": 30
    })
    catalog_manager.insert_record("customers", {
        "id": 2,
        "name": "Jane Smith",
        "email": "jane@example.com",
        "age": 25
    })
    catalog_manager.insert_record("customers", {
        "id": 3,
        "name": "Bob Johnson",
        "email": "bob@example.com",
        "age": 40
    })

    yield "customers"

@pytest.fixture
def join_tables(schema_manager, catalog_manager):
    """Create customers and orders tables with related data for join testing."""
    # Create customers table if it doesn't exist
    if "customers" not in catalog_manager.list_tables("test_db"):
        schema_manager.execute_create_table({
            "type": "CREATE_TABLE",
            "table": "customers",
            "columns": [
                "id INT NOT NULL PRIMARY KEY",
                "name TEXT NOT NULL",
                "email TEXT"
            ]
        })

    # Create orders table
    schema_manager.execute_create_table({
        "type": "CREATE_TABLE",
        "table": "orders",
        "columns": [
            "id INT NOT NULL PRIMARY KEY",
            "customer_id INT NOT NULL",
            "amount DECIMAL NOT NULL",
            "order_date TEXT",
            "FOREIGN KEY (customer_id) REFERENCES customers(id)"
        ]
    })

    # Insert customers if needed
    existing_customers = catalog_manager.query_with_condition("customers", [])
    if len(existing_customers) < 50:
        for i in range(1, 51):
            catalog_manager.insert_record(
                "customers",
                {"id": i, "name": f"Customer {i}", "email": f"customer{i}@example.com"}
            )

    # Insert orders data
    for i in range(1, 151):
        customer_id = ((i - 1) % 50) + 1  # Maps to customer IDs 1-50
        catalog_manager.insert_record(
            "orders",
            {
                "id": i,
                "customer_id": customer_id,
                "amount": i * 10.5,
                "order_date": f"2023-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}"
            }
        )

    yield ["customers", "orders"]

    # Clean up will be done by test_db teardown

@pytest.fixture
def index_on_orders(schema_manager, join_tables):
    """Create an index on the customer_id column in orders table."""
    schema_manager.execute_create_index({
        "type": "CREATE_INDEX",
        "index_name": "idx_customer_id",
        "table": "orders",
        "column": "customer_id"
    })

    yield "idx_customer_id"

    # Clean up the index
    schema_manager.execute_drop_index({
        "type": "DROP_INDEX",
        "index_name": "idx_customer_id"
    })

@pytest.fixture
def indexed_users_table(schema_manager, catalog_manager):
    """Create a users table with various indexes for testing index scans."""
    # Create users table
    schema_manager.execute_create_table({
        "type": "CREATE_TABLE",
        "table": "users",
        "columns": [
            "id INT NOT NULL PRIMARY KEY",
            "username TEXT NOT NULL",
            "age INT",
            "email TEXT",
            "status TEXT"
        ]
    })

    # Insert sample data - create enough records to make indexes meaningful
    for i in range(1, 201):
        catalog_manager.insert_record(
            "users",
            {
                "id": i,
                "username": f"user{i}",
                "age": 20 + (i % 60),
                "email": f"user{i}@example.com",
                "status": ["active", "inactive", "suspended"][i % 3]
            }
        )

    # Create indexes on different columns
    schema_manager.execute_create_index({
        "type": "CREATE_INDEX",
        "index_name": "idx_users_age",
        "table": "users",
        "column": "age"
    })

    schema_manager.execute_create_index({
        "type": "CREATE_INDEX",
        "index_name": "idx_users_status",
        "table": "users",
        "column": "status"
    })

    schema_manager.execute_create_index({
        "type": "CREATE_INDEX",
        "index_name": "idx_users_email",
        "table": "users",
        "column": "email",
        "unique": True
    })

    yield "users"

    # Clean up will be done by test_db teardown

@pytest.fixture
def join_executor(catalog_manager, index_manager):
    """Create a join executor for direct join testing."""
    return JoinExecutor(catalog_manager, index_manager)

# Performance measurement utilities
@pytest.fixture
def measure_performance():
    """Fixture to measure and compare performance of operations."""
    def _measure(func, *args, **kwargs):
        """Execute function and measure time."""
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        return result, end_time - start_time

    return _measure
