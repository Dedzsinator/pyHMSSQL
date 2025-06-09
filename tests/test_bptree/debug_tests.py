#!/usr/bin/env python3
"""
Debug script to test B+ tree functionality with detailed error reporting.
"""

import os
import sys
import traceback

# Add server directory to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
server_dir = os.path.join(project_root, "server")
if server_dir not in sys.path:
    sys.path.insert(0, server_dir)

# Import the B+ tree implementation
try:
    from bptree_optimized import BPlusTreeOptimized as BPTree

    print("✅ Successfully imported BPlusTreeOptimized")
except ImportError as e:
    print(f"❌ Failed to import BPlusTreeOptimized: {e}")
    try:
        from bptree import BPlusTree as BPTree

        print("✅ Successfully imported BPlusTree (fallback)")
    except ImportError as e2:
        print(f"❌ Failed to import BPlusTree: {e2}")
        sys.exit(1)


def test_basic_functionality():
    """Test basic B+ tree functionality with detailed error reporting."""
    print("\n🔍 Testing basic functionality...")

    try:
        tree = BPTree(order=3)
        print("  ✓ Tree created successfully")

        # Basic insertion
        result1 = tree.insert(1, "one")
        print(f"  ✓ Inserted (1, 'one'): {result1}")

        result2 = tree.insert(2, "two")
        print(f"  ✓ Inserted (2, 'two'): {result2}")

        result3 = tree.insert(3, "three")
        print(f"  ✓ Inserted (3, 'three'): {result3}")

        # Basic search
        search1 = tree.search(1)
        print(f"  ✓ Search(1): {search1}")
        assert search1 == "one", f"Expected 'one', got {search1}"

        search2 = tree.search(2)
        print(f"  ✓ Search(2): {search2}")
        assert search2 == "two", f"Expected 'two', got {search2}"

        search3 = tree.search(3)
        print(f"  ✓ Search(3): {search3}")
        assert search3 == "three", f"Expected 'three', got {search3}"

        search4 = tree.search(4)
        print(f"  ✓ Search(4): {search4}")
        assert search4 is None, f"Expected None, got {search4}"

        print("  ✅ Basic functionality test PASSED")
        return True

    except Exception as e:
        print(f"  ❌ Basic functionality test FAILED: {e}")
        traceback.print_exc()
        return False


def test_multidimensional_basic():
    """Test multidimensional B+ tree functionality."""
    print("\n🔍 Testing multidimensional functionality...")

    try:
        tree = BPTree(order=4)
        print("  ✓ Tree created for multidimensional testing")

        # Insert multidimensional data
        result1 = tree.insert_multidim([1.0, 2.0], "point_1_2")
        print(f"  ✓ Inserted multidim ([1.0, 2.0], 'point_1_2'): {result1}")

        result2 = tree.insert_multidim([3.0, 4.0], "point_3_4")
        print(f"  ✓ Inserted multidim ([3.0, 4.0], 'point_3_4'): {result2}")

        # Search multidimensional data
        search1 = tree.search_multidim([1.0, 2.0])
        print(f"  ✓ Search multidim([1.0, 2.0]): {search1}")
        assert search1 == "point_1_2", f"Expected 'point_1_2', got {search1}"

        search2 = tree.search_multidim([3.0, 4.0])
        print(f"  ✓ Search multidim([3.0, 4.0]): {search2}")
        assert search2 == "point_3_4", f"Expected 'point_3_4', got {search2}"

        search3 = tree.search_multidim([5.0, 6.0])
        print(f"  ✓ Search multidim([5.0, 6.0]): {search3}")
        assert search3 is None, f"Expected None, got {search3}"

        # Check tree properties
        is_multidim = tree.is_multidimensional()
        print(f"  ✓ Is multidimensional: {is_multidim}")
        assert is_multidim is True, f"Expected True, got {is_multidim}"

        dimensions = tree.get_dimensions()
        print(f"  ✓ Dimensions: {dimensions}")
        assert dimensions == 2, f"Expected 2, got {dimensions}"

        print("  ✅ Multidimensional functionality test PASSED")
        return True

    except Exception as e:
        print(f"  ❌ Multidimensional functionality test FAILED: {e}")
        traceback.print_exc()
        return False


def test_composite_operations():
    """Test composite operations (mixed single/multidimensional)."""
    print("\n🔍 Testing composite operations...")

    try:
        tree = BPTree(order=4)
        print("  ✓ Tree created for composite operations testing")

        # Start with single-dimensional
        result1 = tree.insert_composite(1, "single_1")
        print(f"  ✓ Composite insert single (1, 'single_1'): {result1}")

        search1 = tree.search_composite(1)
        print(f"  ✓ Composite search single (1): {search1}")
        assert search1 == "single_1", f"Expected 'single_1', got {search1}"

        # Add multidimensional data (this should convert the tree)
        result2 = tree.insert_multidim([2.0, 3.0], "multi_2_3")
        print(f"  ✓ Multidim insert after single ([2.0, 3.0], 'multi_2_3'): {result2}")

        # Check if tree is now multidimensional
        is_multidim = tree.is_multidimensional()
        print(f"  ✓ Tree is now multidimensional: {is_multidim}")

        # Now test composite operation on multidimensional tree
        result3 = tree.insert_composite(4, "single_4")
        print(
            f"  ✓ Composite insert single into multidim tree (4, 'single_4'): {result3}"
        )

        search3 = tree.search_composite(4)
        print(f"  ✓ Composite search single from multidim tree (4): {search3}")
        assert search3 == "single_4", f"Expected 'single_4', got {search3}"

        # Verify original data is still there
        search_orig = tree.search_composite(1)
        print(f"  ✓ Original composite data still accessible (1): {search_orig}")
        assert search_orig == "single_1", f"Expected 'single_1', got {search_orig}"

        search_multi = tree.search_multidim([2.0, 3.0])
        print(f"  ✓ Multidim data still accessible ([2.0, 3.0]): {search_multi}")
        assert search_multi == "multi_2_3", f"Expected 'multi_2_3', got {search_multi}"

        print("  ✅ Composite operations test PASSED")
        return True

    except Exception as e:
        print(f"  ❌ Composite operations test FAILED: {e}")
        traceback.print_exc()
        return False


def main():
    """Run all debug tests."""
    print("🚀 Starting B+ tree debug tests...")

    tests = [
        test_basic_functionality,
        test_multidimensional_basic,
        test_composite_operations,
    ]

    passed = 0
    total = len(tests)

    for test_func in tests:
        if test_func():
            passed += 1

    print(f"\n📊 Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests PASSED!")
    else:
        print("❌ Some tests FAILED!")

    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
