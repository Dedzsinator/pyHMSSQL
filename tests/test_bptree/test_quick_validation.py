"""
Simple B+ tree validation tests without profiling overhead.

This module provides quick validation tests that can be run frequently
during development without the overhead of comprehensive profiling.
"""

import pytest
import os
import sys

# Add server directory to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
server_dir = os.path.join(project_root, "server")
if server_dir not in sys.path:
    sys.path.insert(0, server_dir)

# Import the B+ tree implementation
try:
    from bptree_optimized import BPlusTreeOptimized as BPTree
except ImportError:
    try:
        from bptree import BPlusTree as BPTree
    except ImportError:
        raise ImportError("Could not import B+ tree implementation")


def test_basic_functionality():
    """Quick test of basic B+ tree functionality."""
    tree = BPTree(order=3)

    # Basic insertion and search
    tree.insert(1, "one")
    tree.insert(2, "two")
    tree.insert(3, "three")

    assert tree.search(1) == "one"
    assert tree.search(2) == "two"
    assert tree.search(3) == "three"
    assert tree.search(4) is None


def test_multidimensional_basic():
    """Quick test of multidimensional functionality."""
    tree = BPTree(order=3)

    # 2D operations
    tree.insert_multidim([1.0, 2.0], "point1")
    tree.insert_multidim([3.0, 4.0], "point2")

    assert tree.search_multidim([1.0, 2.0]) == "point1"
    assert tree.search_multidim([3.0, 4.0]) == "point2"
    assert tree.search_multidim([5.0, 6.0]) is None


def test_composite_operations():
    """Quick test of composite operations."""
    tree = BPTree(order=3)

    # Test composite insert and search if available
    if hasattr(tree, "insert_composite") and hasattr(tree, "search_composite"):
        tree.insert_composite(1, "single")
        tree.insert_composite([2.0, 3.0], "multi")

        assert tree.search_composite(1) == "single"
        assert tree.search_composite([2.0, 3.0]) == "multi"


def test_range_queries():
    """Quick test of range queries."""
    tree = BPTree(order=4)

    # Insert test data
    for i in range(10):
        tree.insert(i, f"value_{i}")

    # Test single-dimensional range query
    if hasattr(tree, "range_query"):
        results = tree.range_query(3, 7)
        assert len(results) == 5  # Should include 3, 4, 5, 6, 7

    # Test multidimensional range query
    tree_multi = BPTree(order=4)
    for x in range(3):
        for y in range(3):
            tree_multi.insert_multidim([float(x), float(y)], f"grid_{x}_{y}")

    if hasattr(tree_multi, "range_query_multidim"):
        results = tree_multi.range_query_multidim([0.0, 0.0], [2.0, 2.0])
        assert len(results) == 9  # 3x3 grid


def test_tree_properties():
    """Quick test of tree property methods."""
    tree = BPTree(order=3)

    # Initial state
    if hasattr(tree, "is_multidimensional"):
        assert not tree.is_multidimensional()

    if hasattr(tree, "get_dimensions"):
        # Some implementations return 0 for uninitialized trees, others return 1
        initial_dims = tree.get_dimensions()
        assert initial_dims in [0, 1], f"Expected 0 or 1, got {initial_dims}"

    # After multidimensional insertion
    tree.insert_multidim([1.0, 2.0], "point")

    if hasattr(tree, "is_multidimensional"):
        assert tree.is_multidimensional()

    if hasattr(tree, "get_dimensions"):
        assert tree.get_dimensions() == 2


def test_error_handling():
    """Quick test of error handling."""
    tree = BPTree(order=3)

    # Test invalid inputs
    with pytest.raises(TypeError):
        tree.insert(None, "value")

    # Test empty multidimensional key
    with pytest.raises(ValueError):
        tree.insert_multidim([], "value")


def test_performance_quick():
    """Quick performance validation (no detailed profiling)."""
    tree = BPTree(order=10)

    # Insert moderate amount of data
    for i in range(100):
        tree.insert(i, f"value_{i}")

    # Verify all can be found
    for i in range(100):
        assert tree.search(i) == f"value_{i}"

    # Test multidimensional performance
    tree_multi = BPTree(order=10)
    for i in range(50):
        tree_multi.insert_multidim([float(i), float(i * 2)], f"multi_{i}")

    # Verify multidimensional searches
    for i in range(50):
        assert tree_multi.search_multidim([float(i), float(i * 2)]) == f"multi_{i}"


if __name__ == "__main__":
    # Run tests directly for quick validation
    test_functions = [
        test_basic_functionality,
        test_multidimensional_basic,
        test_composite_operations,
        test_range_queries,
        test_tree_properties,
        test_error_handling,
        test_performance_quick,
    ]

    print("Running quick B+ tree validation tests...")

    for test_func in test_functions:
        try:
            test_func()
            print(f"✓ {test_func.__name__}")
        except Exception as e:
            print(f"✗ {test_func.__name__}: {e}")

    print("Quick validation tests completed.")
