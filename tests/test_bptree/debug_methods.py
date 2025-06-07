#!/usr/bin/env python3
"""
Debug script to check B+ tree implementation methods and functionality.
"""

import os
import sys

# Add server directory to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
server_dir = os.path.join(project_root, 'server')
if server_dir not in sys.path:
    sys.path.insert(0, server_dir)

# Import the B+ tree implementation
try:
    from bptree_optimized import BPlusTreeOptimized as BPTree
    print("✓ Successfully imported optimized B+ tree")
    implementation = "optimized"
except ImportError as e:
    print(f"✗ Could not import optimized B+ tree: {e}")
    try:
        from bptree import BPlusTree as BPTree
        print("✓ Successfully imported standard B+ tree")
        implementation = "standard"
    except ImportError as e:
        print(f"✗ Could not import standard B+ tree: {e}")
        sys.exit(1)

print(f"\nUsing {implementation} B+ tree implementation")
print("=" * 50)

# Create a tree instance
try:
    tree = BPTree(order=3)
    print("✓ Successfully created B+ tree instance")
except Exception as e:
    print(f"✗ Could not create B+ tree instance: {e}")
    sys.exit(1)

# Check available methods
print("\nAvailable methods:")
methods = [method for method in dir(tree) if not method.startswith('_')]
for method in sorted(methods):
    print(f"  - {method}")

print("\nTesting basic functionality:")

# Test 1: Basic insertion and search
print("\n1. Testing basic insertion and search:")
try:
    result = tree.insert(1, "value1")
    print(f"   Insert result: {result}")
    
    search_result = tree.search(1)
    print(f"   Search result: {search_result}")
    
    if search_result == "value1":
        print("   ✓ Basic functionality works")
    else:
        print(f"   ✗ Basic functionality failed: expected 'value1', got {search_result}")
except Exception as e:
    print(f"   ✗ Basic functionality failed: {e}")

# Test 2: Multidimensional functionality
print("\n2. Testing multidimensional functionality:")
if hasattr(tree, 'insert_multidim'):
    try:
        result = tree.insert_multidim([1.0, 2.0], "point1")
        print(f"   Multidim insert result: {result}")
        
        search_result = tree.search_multidim([1.0, 2.0])
        print(f"   Multidim search result: {search_result}")
        
        if search_result == "point1":
            print("   ✓ Multidimensional functionality works")
        else:
            print(f"   ✗ Multidimensional functionality failed: expected 'point1', got {search_result}")
    except Exception as e:
        print(f"   ✗ Multidimensional functionality failed: {e}")
else:
    print("   - insert_multidim method not available")

# Test 3: Composite operations
print("\n3. Testing composite operations:")
if hasattr(tree, 'insert_composite'):
    try:
        result1 = tree.insert_composite(5, "single")
        print(f"   Composite single insert result: {result1}")
        
        result2 = tree.insert_composite([3.0, 4.0], "multi")
        print(f"   Composite multi insert result: {result2}")
        
        search1 = tree.search_composite(5)
        search2 = tree.search_composite([3.0, 4.0])
        print(f"   Composite search results: {search1}, {search2}")
        
        if search1 == "single" and search2 == "multi":
            print("   ✓ Composite functionality works")
        else:
            print(f"   ✗ Composite functionality failed")
    except Exception as e:
        print(f"   ✗ Composite functionality failed: {e}")
else:
    print("   - insert_composite method not available")

# Test 4: Tree introspection
print("\n4. Testing tree introspection:")
if hasattr(tree, 'is_multidimensional'):
    try:
        is_multi = tree.is_multidimensional()
        print(f"   is_multidimensional: {is_multi}")
    except Exception as e:
        print(f"   ✗ is_multidimensional failed: {e}")
else:
    print("   - is_multidimensional method not available")

if hasattr(tree, 'get_dimensions'):
    try:
        dimensions = tree.get_dimensions()
        print(f"   get_dimensions: {dimensions}")
    except Exception as e:
        print(f"   ✗ get_dimensions failed: {e}")
else:
    print("   - get_dimensions method not available")

# Test 5: Range queries
print("\n5. Testing range queries:")
if hasattr(tree, 'range_query'):
    try:
        # Insert some data first
        for i in range(10):
            tree.insert(i + 10, f"range_value_{i}")
        
        results = tree.range_query(12, 16)
        print(f"   Range query results: {results}")
        print(f"   Number of results: {len(results) if results else 0}")
        
        if results and len(results) > 0:
            print("   ✓ Range query functionality works")
        else:
            print("   ✗ Range query functionality failed or returned no results")
    except Exception as e:
        print(f"   ✗ Range query failed: {e}")
else:
    print("   - range_query method not available")

if hasattr(tree, 'range_query_multidim'):
    try:
        # Insert some multidimensional data if possible
        if hasattr(tree, 'insert_multidim'):
            for x in range(3):
                for y in range(3):
                    tree.insert_multidim([float(x), float(y)], f"grid_{x}_{y}")
            
            results = tree.range_query_multidim([0.0, 0.0], [2.0, 2.0])
            print(f"   Multidim range query results: {len(results) if results else 0} items")
            
            if results and len(results) > 0:
                print("   ✓ Multidimensional range query functionality works")
            else:
                print("   ✗ Multidimensional range query failed or returned no results")
    except Exception as e:
        print(f"   ✗ Multidimensional range query failed: {e}")
else:
    print("   - range_query_multidim method not available")

print("\nDiagnostic complete!")
