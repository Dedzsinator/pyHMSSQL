"""
Test configuration for CyCore unit tests.

This module provides test fixtures and configuration for testing the CyCore
high-performance modules including Swiss hashmaps and HLC implementations.
"""

import pytest
import os
import sys

# Add project paths
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Try to import CyCore components
try:
    from cycore import SwissMap, RangeMap, TombstoneMap, SWISS_IMPLEMENTATION
    CYCORE_AVAILABLE = True
except ImportError:
    CYCORE_AVAILABLE = False


def pytest_configure(config):
    """Configure pytest with custom markers for CyCore tests"""
    config.addinivalue_line("markers", "cycore: mark test as CyCore component test")
    config.addinivalue_line("markers", "swiss_map: mark test as Swiss map test")
    config.addinivalue_line("markers", "performance: mark test as performance test")
    config.addinivalue_line("markers", "threading: mark test as thread safety test")
    config.addinivalue_line("markers", "integration: mark test as integration test")


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers automatically"""
    for item in items:
        # Add cycore marker to all tests in this directory
        item.add_marker(pytest.mark.cycore)
        
        # Add specific markers based on test names
        if "swiss_map" in item.name.lower():
            item.add_marker(pytest.mark.swiss_map)
        
        if "performance" in item.name.lower() or "benchmark" in item.name.lower():
            item.add_marker(pytest.mark.performance)
        
        if "thread" in item.name.lower() or "concurrent" in item.name.lower():
            item.add_marker(pytest.mark.threading)


@pytest.fixture(scope="session")
def cycore_availability():
    """Check if CyCore components are available"""
    return CYCORE_AVAILABLE


@pytest.fixture(scope="session")
def swiss_implementation():
    """Get the Swiss map implementation type"""
    if CYCORE_AVAILABLE:
        return SWISS_IMPLEMENTATION
    return "none"


@pytest.fixture
def sample_swiss_map():
    """Create a sample Swiss map with test data"""
    if not CYCORE_AVAILABLE:
        pytest.skip("CyCore not available")
    
    smap = SwissMap()
    test_data = {
        "key1": "value1",
        "key2": "value2", 
        "key3": "value3",
        "numeric_key": "42",
        "unicode_key": "value_ñáéíóú"
    }
    
    for k, v in test_data.items():
        smap[k] = v
    
    return smap


@pytest.fixture
def large_swiss_map():
    """Create a large Swiss map for performance testing"""
    if not CYCORE_AVAILABLE:
        pytest.skip("CyCore not available")
    
    smap = SwissMap()
    for i in range(10000):
        smap[f"large_key_{i}"] = f"large_value_{i}"
    
    return smap


@pytest.fixture
def empty_swiss_map():
    """Create an empty Swiss map"""
    if not CYCORE_AVAILABLE:
        pytest.skip("CyCore not available")
    
    return SwissMap()


@pytest.fixture
def sample_range_map():
    """Create a sample RangeMap with test data"""
    if not CYCORE_AVAILABLE:
        pytest.skip("CyCore not available")
    
    rmap = RangeMap()
    test_data = {
        "range_start": "0",
        "range_end": "100",
        "range_step": "10"
    }
    
    for k, v in test_data.items():
        rmap[k] = v
    
    return rmap


@pytest.fixture
def sample_tombstone_map():
    """Create a sample TombstoneMap with test data"""
    if not CYCORE_AVAILABLE:
        pytest.skip("CyCore not available")
    
    tmap = TombstoneMap()
    test_data = {
        "active_key": "active_value",
        "deleted_key": "deleted_value"
    }
    
    for k, v in test_data.items():
        tmap[k] = v
    
    return tmap


@pytest.fixture
def performance_test_config():
    """Configuration for performance tests"""
    return {
        "small_dataset_size": 1000,
        "medium_dataset_size": 10000,
        "large_dataset_size": 100000,
        "max_insert_time": 5.0,  # seconds
        "max_lookup_time": 2.0,  # seconds
        "min_ops_per_second": 50000,
    }


@pytest.fixture(autouse=True)
def cleanup_test_resources():
    """Automatically cleanup test resources after each test"""
    yield
    # Cleanup logic could go here if needed
    # For Swiss maps, they should clean up automatically
