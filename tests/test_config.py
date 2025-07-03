"""
Configuration for test execution modes.

This module provides configuration options for controlling test behavior,
particularly for disabling profiling overhead during development testing.
Enhanced with comprehensive mock implementations and testing utilities.
"""

import os
import logging
import tempfile
import threading
import time
from pathlib import Path
from typing import Dict, Any, List, Optional, Union, Callable
from contextlib import contextmanager

# Configuration flags
DISABLE_PROFILING = os.environ.get("DISABLE_PROFILING", "0").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
QUICK_TESTS_ONLY = os.environ.get("QUICK_TESTS_ONLY", "0").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
VERBOSE_OUTPUT = os.environ.get("VERBOSE_TESTS", "0").lower() in (
    "1",
    "true",
    "yes",
    "on",
)

# Test data sizes for different modes
if QUICK_TESTS_ONLY:
    # Smaller datasets for quick testing
    TEST_SIZES = {
        "small": 50,
        "medium": 200,
        "large": 500,
        "grid_size": 10,
        "stress_size": 100,
    }
else:
    # Full test datasets
    TEST_SIZES = {
        "small": 100,
        "medium": 500,
        "large": 1000,
        "grid_size": 20,
        "stress_size": 1000,
    }

# B+ tree orders to test
BPTREE_ORDERS = [3, 5, 10] if QUICK_TESTS_ONLY else [3, 5, 10, 20]

# Multidimensional test dimensions
TEST_DIMENSIONS = [2, 3] if QUICK_TESTS_ONLY else [2, 3, 5]

# Performance test timeouts
PERFORMANCE_TIMEOUT = 2.0 if QUICK_TESTS_ONLY else 10.0


def log_test_config():
    """Log the current test configuration."""
    if VERBOSE_OUTPUT:
        logging.info(f"Test Configuration:")
        logging.info(f"  DISABLE_PROFILING: {DISABLE_PROFILING}")
        logging.info(f"  QUICK_TESTS_ONLY: {QUICK_TESTS_ONLY}")
        logging.info(f"  VERBOSE_OUTPUT: {VERBOSE_OUTPUT}")
        logging.info(f"  TEST_SIZES: {TEST_SIZES}")
        logging.info(f"  BPTREE_ORDERS: {BPTREE_ORDERS}")
        logging.info(f"  TEST_DIMENSIONS: {TEST_DIMENSIONS}")


class MockProfiler:
    """
    Enhanced mock profiler for comprehensive testing.
    
    Provides realistic profiling interface with controlled data generation
    for predictable test behavior.
    """

    def __init__(self, *args, **kwargs):
        self.profiling_active = False
        self.operations = []
        self.current_operation = None
        self.start_time = None
        self.metrics = {}
        self.lock = threading.RLock()
        self._operation_counter = 0

    def start_profiling(self):
        """Start mock profiling session"""
        with self.lock:
            self.profiling_active = True
            self.start_time = time.time()
            self.operations.clear()
            self.metrics.clear()
            self._operation_counter = 0
            if VERBOSE_OUTPUT:
                logging.debug("Mock profiler started")

    def stop_profiling(self):
        """Stop mock profiling session"""
        with self.lock:
            self.profiling_active = False
            if self.current_operation:
                self.end_operation()
            if VERBOSE_OUTPUT:
                logging.debug(f"Mock profiler stopped after {len(self.operations)} operations")

    def start_operation(self, operation_name: str, **kwargs):
        """Start profiling an operation"""
        with self.lock:
            if not self.profiling_active:
                return
                
            self._operation_counter += 1
            operation = {
                'id': self._operation_counter,
                'name': operation_name,
                'start_time': time.time(),
                'details': kwargs,
                'metrics': {
                    'cpu_percent': 10.5 + (self._operation_counter % 10),  # Varying CPU usage
                    'memory_mb': 156.3 + (self._operation_counter * 0.5),   # Growing memory usage
                    'io_read_bytes': 1024 * (50 + self._operation_counter),  # Varying I/O
                    'io_write_bytes': 1024 * (25 + self._operation_counter // 2),
                }
            }
            self.current_operation = operation
            if VERBOSE_OUTPUT:
                logging.debug(f"Mock profiler started operation: {operation_name}")

    def end_operation(self):
        """End current operation"""
        with self.lock:
            if not self.current_operation or not self.profiling_active:
                return
            
            operation = self.current_operation
            operation['end_time'] = time.time()
            operation['duration'] = operation['end_time'] - operation['start_time']
            
            # Add realistic variation to metrics
            variation = 1.0 + (operation['id'] % 5) * 0.1  # 0-50% variation
            operation['metrics']['actual_cpu'] = operation['metrics']['cpu_percent'] * variation
            operation['metrics']['peak_memory_mb'] = operation['metrics']['memory_mb'] * (1.1 + variation * 0.1)
            
            self.operations.append(operation)
            self.current_operation = None
            if VERBOSE_OUTPUT:
                logging.debug(f"Mock profiler ended operation: {operation['name']} (duration: {operation['duration']:.3f}s)")

    def get_metrics(self) -> Dict[str, Any]:
        """Get comprehensive metrics"""
        with self.lock:
            if not self.operations:
                return {}
            
            total_operations = len(self.operations)
            total_duration = sum(op['duration'] for op in self.operations)
            avg_cpu = sum(op['metrics']['cpu_percent'] for op in self.operations) / total_operations
            peak_memory = max(op['metrics']['peak_memory_mb'] for op in self.operations)
            
            return {
                'total_operations': total_operations,
                'total_duration_seconds': total_duration,
                'average_duration_seconds': total_duration / total_operations,
                'average_cpu_percent': avg_cpu,
                'peak_memory_mb': peak_memory,
                'operations_per_second': total_operations / total_duration if total_duration > 0 else 0,
                'operations': self.operations.copy(),
                'profiling_active': self.profiling_active,
                'session_duration': time.time() - self.start_time if self.start_time else 0
            }

    def get_operation_summary(self) -> Dict[str, Any]:
        """Get operation summary statistics"""
        metrics = self.get_metrics()
        if not metrics.get('operations'):
            return {
                'total_operations': 0,
                'operation_types': [],
                'total_execution_time': 0,
                'performance_statistics': {},
                'resource_usage_summary': {},
                'operations_by_type': {}
            }
        
        operations = metrics['operations']
        operation_types = list(set(op['name'] for op in operations))
        
        # Group operations by type
        ops_by_type = {}
        for op in operations:
            name = op['name']
            if name not in ops_by_type:
                ops_by_type[name] = []
            ops_by_type[name].append(op)
        
        # Calculate statistics by type
        operations_by_type = {}
        for op_type, ops in ops_by_type.items():
            operations_by_type[op_type] = {
                'count': len(ops),
                'total_time': sum(op['duration'] for op in ops),
                'average_time': sum(op['duration'] for op in ops) / len(ops),
                'max_time': max(op['duration'] for op in ops),
                'min_time': min(op['duration'] for op in ops)
            }
        
        return {
            'total_operations': metrics['total_operations'],
            'operation_types': operation_types,
            'total_execution_time': metrics['total_duration_seconds'],
            'performance_statistics': {
                'average_execution_time': metrics['average_duration_seconds'],
                'max_execution_time': max(op['duration'] for op in operations),
                'min_execution_time': min(op['duration'] for op in operations),
                'operations_per_second': metrics['operations_per_second']
            },
            'resource_usage_summary': {
                'total_memory_mb': metrics['peak_memory_mb'],
                'average_cpu_percent': metrics['average_cpu_percent'],
                'peak_cpu_percent': max(op['metrics']['cpu_percent'] for op in operations)
            },
            'operations_by_type': operations_by_type
        }

    def export_report(self, filename: str):
        """Export profiling report to file"""
        metrics = self.get_metrics()
        summary = self.get_operation_summary()
        
        report = {
            'test_mode': 'mock_profiling',
            'timestamp': time.time(),
            'configuration': {
                'disable_profiling': DISABLE_PROFILING,
                'quick_tests_only': QUICK_TESTS_ONLY,
                'verbose_output': VERBOSE_OUTPUT
            },
            'metrics': metrics,
            'summary': summary
        }
        
        try:
            import json
            with open(filename, 'w') as f:
                json.dump(report, f, indent=2)
            if VERBOSE_OUTPUT:
                logging.info(f"Mock profiling report exported to {filename}")
        except Exception as e:
            logging.warning(f"Failed to export mock profiling report: {e}")


class MockDatabaseConnection:
    """Mock database connection for testing database operations"""
    
    def __init__(self, connection_string: str = "mock://localhost:5432/testdb"):
        self.connection_string = connection_string
        self.connected = False
        self.transactions = []
        self.current_transaction = None
        self.query_count = 0
    
    def connect(self):
        """Mock connection"""
        self.connected = True
        if VERBOSE_OUTPUT:
            logging.debug(f"Mock database connected to {self.connection_string}")
    
    def disconnect(self):
        """Mock disconnection"""
        self.connected = False
        if VERBOSE_OUTPUT:
            logging.debug("Mock database disconnected")
    
    def execute_query(self, query: str) -> Dict[str, Any]:
        """Mock query execution"""
        if not self.connected:
            return {'status': 'error', 'error': 'Not connected to database'}
        
        self.query_count += 1
        
        # Simple query pattern matching for mock responses
        query_lower = query.lower().strip()
        
        if query_lower.startswith('select'):
            return {
                'status': 'success',
                'rows': [{'id': 1, 'name': 'Test Record'}],
                'row_count': 1,
                'execution_time_ms': 5.5
            }
        elif query_lower.startswith('insert'):
            return {
                'status': 'success',
                'rows_affected': 1,
                'execution_time_ms': 3.2
            }
        elif query_lower.startswith('update'):
            return {
                'status': 'success',
                'rows_affected': 1,
                'execution_time_ms': 4.1
            }
        elif query_lower.startswith('delete'):
            return {
                'status': 'success',
                'rows_affected': 1,
                'execution_time_ms': 3.8
            }
        else:
            return {
                'status': 'success',
                'message': 'Query executed successfully',
                'execution_time_ms': 2.0
            }
    
    def begin_transaction(self):
        """Begin mock transaction"""
        transaction_id = len(self.transactions) + 1
        self.current_transaction = {
            'id': transaction_id,
            'start_time': time.time(),
            'queries': []
        }
        return transaction_id
    
    def commit_transaction(self):
        """Commit mock transaction"""
        if self.current_transaction:
            self.current_transaction['end_time'] = time.time()
            self.current_transaction['status'] = 'committed'
            self.transactions.append(self.current_transaction)
            self.current_transaction = None
            return True
        return False
    
    def rollback_transaction(self):
        """Rollback mock transaction"""
        if self.current_transaction:
            self.current_transaction['end_time'] = time.time()
            self.current_transaction['status'] = 'rolled_back'
            self.transactions.append(self.current_transaction)
            self.current_transaction = None
            return True
        return False


class TestEnvironmentManager:
    """
    Enhanced test environment manager for comprehensive test resource management.
    """
    
    def __init__(self):
        self.temp_directories = []
        self.mock_objects = {}
        self.cleanup_callbacks = []
        self.test_databases = {}
        
    @contextmanager
    def temporary_directory(self, prefix: str = "pyhmssql_test_"):
        """Context manager for temporary directories"""
        temp_dir = tempfile.mkdtemp(prefix=prefix)
        self.temp_directories.append(temp_dir)
        try:
            yield temp_dir
        finally:
            self._cleanup_directory(temp_dir)
    
    def _cleanup_directory(self, directory: str):
        """Clean up a temporary directory"""
        try:
            import shutil
            shutil.rmtree(directory, ignore_errors=True)
            if directory in self.temp_directories:
                self.temp_directories.remove(directory)
        except Exception as e:
            if VERBOSE_OUTPUT:
                logging.warning(f"Failed to cleanup directory {directory}: {e}")
    
    def create_mock_database(self, db_name: str = "test_db") -> MockDatabaseConnection:
        """Create a mock database for testing"""
        mock_db = MockDatabaseConnection(f"mock://localhost:5432/{db_name}")
        mock_db.connect()
        self.test_databases[db_name] = mock_db
        return mock_db
    
    def get_mock_database(self, db_name: str) -> Optional[MockDatabaseConnection]:
        """Get a mock database by name"""
        return self.test_databases.get(db_name)
    
    def register_mock(self, name: str, mock_object: Any):
        """Register a mock object for later cleanup"""
        self.mock_objects[name] = mock_object
    
    def get_mock(self, name: str) -> Any:
        """Get a registered mock object"""
        return self.mock_objects.get(name)
    
    def register_cleanup_callback(self, callback: Callable):
        """Register a cleanup callback"""
        self.cleanup_callbacks.append(callback)
    
    def cleanup_all(self):
        """Clean up all test resources"""
        # Run cleanup callbacks
        for callback in self.cleanup_callbacks:
            try:
                callback()
            except Exception as e:
                if VERBOSE_OUTPUT:
                    logging.warning(f"Cleanup callback failed: {e}")
        
        # Clean up test databases
        for db_name, db_conn in self.test_databases.items():
            try:
                db_conn.disconnect()
            except Exception as e:
                if VERBOSE_OUTPUT:
                    logging.warning(f"Failed to disconnect test database {db_name}: {e}")
        
        # Clean up temporary directories
        for temp_dir in self.temp_directories.copy():
            self._cleanup_directory(temp_dir)
        
        # Clear all resources
        self.mock_objects.clear()
        self.cleanup_callbacks.clear()
        self.test_databases.clear()


# Global test environment manager
_test_env_manager = TestEnvironmentManager()


def get_profiler(*args, **kwargs):
    """Get profiler instance based on configuration."""
    if DISABLE_PROFILING:
        profiler = MockProfiler(*args, **kwargs)
        _test_env_manager.register_mock("profiler", profiler)
        return profiler
    else:
        try:
            from profiler import SystemProfiler
            return SystemProfiler(*args, **kwargs)
        except ImportError:
            if VERBOSE_OUTPUT:
                logging.warning("System profiler not available, using mock profiler")
            profiler = MockProfiler(*args, **kwargs)
            _test_env_manager.register_mock("profiler", profiler)
            return profiler


def get_test_environment_manager() -> TestEnvironmentManager:
    """Get the global test environment manager"""
    return _test_env_manager


def should_skip_profiling_tests():
    """Check if profiling tests should be skipped."""
    return DISABLE_PROFILING


def get_test_timeout(base_timeout=5.0):
    """Get test timeout based on configuration."""
    if QUICK_TESTS_ONLY:
        return min(base_timeout, PERFORMANCE_TIMEOUT)
    return base_timeout


def setup_test_logging(level: int = logging.WARNING):
    """Setup logging for tests with appropriate level"""
    if VERBOSE_OUTPUT:
        level = logging.DEBUG
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )


def cleanup_test_environment():
    """Global cleanup function"""
    _test_env_manager.cleanup_all()


# Initialize test configuration on import
log_test_config()

# Register cleanup for module shutdown
import atexit
atexit.register(cleanup_test_environment)
