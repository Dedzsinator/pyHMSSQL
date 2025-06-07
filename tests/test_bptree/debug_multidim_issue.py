#!/usr/bin/env python3
"""Debug the multidimensional search issue."""

import sys
import os

# Add server directory to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
server_dir = os.path.join(project_root, 'server')
if server_dir not in sys.path:
    sys.path.insert(0, server_dir)

from bptree_optimized import BPlusTreeOptimized as BPTree

def debug_multidim_search():
    """Debug the specific case that's failing."""
    tree = BPTree(order=10)
    
    # Test with a smaller scale first to see the pattern
    test_size = 100
    
    # Insert points using the same pattern as the failing test
    points = []
    for i in range(test_size):
        x = float(i % 100)  # Use larger modulo to avoid duplicates
        y = float(i // 100)
        point = [x, y]
        value = f"point_{i}"
        points.append((point, value))
        tree.insert_multidim(point, value)
    
    # Test all points to see the pattern
    print(f"Testing all {test_size} points:")
    working = []
    failing = []
    
    for i, (point, expected_value) in enumerate(points):
        found_value = tree.search_multidim(point)
        if found_value == expected_value:
            working.append(i)
        else:
            failing.append(i)
    
    print(f"Working: {len(working)} points")
    print(f"Failing: {len(failing)} points")
    
    if failing:
        print(f"First 10 failing indices: {failing[:10]}")
        print(f"First 10 working indices: {working[:10]}")
        
        # Check if there's a pattern in the y-coordinates
        failing_coords = [points[i][0] for i in failing[:10]]
        working_coords = [points[i][0] for i in working[:10]]
        
        print("Failing coordinates:")
        for i, coord in enumerate(failing_coords):
            print(f"  {failing[i]}: {coord}")
        
        print("Working coordinates:")
        for i, coord in enumerate(working_coords):
            print(f"  {working[i]}: {coord}")
    
    # Try a different approach - use completely different coordinates
    print("\n--- Testing different coordinate strategy ---")
    tree2 = BPTree(order=10)
    
    # Use a different coordinate generation strategy
    test_points_2 = []
    for i in range(20):
        # Use prime numbers to avoid patterns
        x = float(i * 7 % 97)  # Different pattern
        y = float(i * 11 % 89)
        point = [x, y]
        value = f"alt_{i}"
        test_points_2.append((point, value))
        tree2.insert_multidim(point, value)
    
    failures_2 = 0
    for i, (point, expected_value) in enumerate(test_points_2):
        found_value = tree2.search_multidim(point)
        status = "✓" if found_value == expected_value else "✗"
        print(f"{status} {point} -> {expected_value}")
        if found_value != expected_value:
            failures_2 += 1
    
    print(f"Alternative strategy failures: {failures_2}/{len(test_points_2)}")

if __name__ == "__main__":
    debug_multidim_search()
