"""
Configuration for test execution modes.

This module provides configuration options for controlling test behavior,
particularly for disabling profiling overhead during development testing.
"""

import os
import logging

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
    """Mock profiler for when profiling is disabled."""

    def __init__(self, *args, **kwargs):
        pass

    def start_profiling(self):
        pass

    def stop_profiling(self):
        pass

    def start_operation(self, operation_name, **kwargs):
        pass

    def end_operation(self):
        pass

    def get_metrics(self):
        return {}

    def export_report(self, filename):
        pass


def get_profiler(*args, **kwargs):
    """Get profiler instance based on configuration."""
    if DISABLE_PROFILING:
        return MockProfiler(*args, **kwargs)
    else:
        try:
            from profiler import SystemProfiler

            return SystemProfiler(*args, **kwargs)
        except ImportError:
            logging.warning("System profiler not available, using mock profiler")
            return MockProfiler(*args, **kwargs)


def should_skip_profiling_tests():
    """Check if profiling tests should be skipped."""
    return DISABLE_PROFILING


def get_test_timeout(base_timeout=5.0):
    """Get test timeout based on configuration."""
    if QUICK_TESTS_ONLY:
        return min(base_timeout, PERFORMANCE_TIMEOUT)
    return base_timeout
