"""
Shared test fixtures and configuration for the DBMS test suite.
"""
import os
import sys
import pytest
import tempfile
import shutil
import logging
import pathlib

# Get the absolute path to the server directory
project_root = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
server_dir = os.path.join(project_root, 'server')

# Add server directory to path so that imports from within server work correctly
if server_dir not in sys.path:
    sys.path.insert(0, server_dir)
    
# Add project root to path as well
if project_root not in sys.path:
    sys.path.insert(0, project_root)

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
        return {"status": "success", "index_name": index_name, "visualization": "Mock visualization"}

    def visualize_database(self, db_name=None):
        """Mock database visualization."""
        return {"status": "success", "database": db_name or "current", "visualization": "Mock visualization"}

# Create a utils module with visualizer submodule
import types
utils_module = types.ModuleType('utils')
visualizer_module = types.ModuleType('utils.visualizer')
visualizer_module.Visualizer = MockVisualizer
utils_module.visualizer = visualizer_module
sys.modules['utils'] = utils_module
sys.modules['utils.visualizer'] = visualizer_module

# Now import DBMS components directly from the server directory
# Import using direct imports from server directory since it's now in sys.path
from catalog_manager import CatalogManager
from execution_engine import ExecutionEngine
from parser import SQLParser
from planner import Planner
from optimizer import Optimizer
from ddl_processor.index_manager import IndexManager
from ddl_processor.schema_manager import SchemaManager
from transaction.transaction_manager import TransactionManager

# Rest of the code remains the same

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
def execution_engine(catalog_manager, index_manager):
    """Create an execution engine for testing."""
    return ExecutionEngine(catalog_manager, index_manager)

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
    catalog_manager.insert_record("customers", {"id": 1, "name": "John Doe", "email": "john@example.com", "age": 30})
    catalog_manager.insert_record("customers", {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "age": 25})
    catalog_manager.insert_record("customers", {"id": 3, "name": "Bob Johnson", "email": "bob@example.com", "age": 40})
    
    yield "customers"