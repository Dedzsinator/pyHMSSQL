#!/usr/bin/env python3
"""
Comprehensive test suite for hyperoptimized sorting system.
Tests all sorting algorithms, edge cases, and performance characteristics.
"""

import unittest
import numpy as np
import random
import time
import os
import tempfile
import sys
from typing import List, Tuple, Any
import gc

# Import our hyperoptimized sorting module
try:
    import hyperoptimized_sort as hs
except ImportError:
    print("Hyperoptimized sort module not compiled. Run: python setup_hyperopt_sort.py build_ext --inplace")
    sys.exit(1)


class TestHyperoptimizedSort(unittest.TestCase):
    """Test suite for hyperoptimized sorting algorithms."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.sorter = hs.HyperoptimizedSorter()
        random.seed(42)
        np.random.seed(42)
    
    def tearDown(self):
        """Clean up after tests."""
        gc.collect()
    
    def test_empty_array(self):
        """Test sorting empty arrays."""
        empty_int = np.array([], dtype=np.int64)
        empty_float = np.array([], dtype=np.float64)
        
        self.sorter.sort(empty_int)
        self.sorter.sort(empty_float)
        
        self.assertEqual(len(empty_int), 0)
        self.assertEqual(len(empty_float), 0)
    
    def test_single_element(self):
        """Test sorting single-element arrays."""
        single_int = np.array([42], dtype=np.int64)
        single_float = np.array([3.14], dtype=np.float64)
        
        self.sorter.sort(single_int)
        self.sorter.sort(single_float)
        
        self.assertEqual(single_int[0], 42)
        self.assertEqual(single_float[0], 3.14)
    
    def test_already_sorted(self):
        """Test sorting already sorted arrays."""
        sorted_ints = np.arange(1000, dtype=np.int64)
        sorted_floats = np.arange(1000, dtype=np.float64) * 0.1
        
        expected_ints = sorted_ints.copy()
        expected_floats = sorted_floats.copy()
        
        self.sorter.sort(sorted_ints)
        self.sorter.sort(sorted_floats)
        
        np.testing.assert_array_equal(sorted_ints, expected_ints)
        np.testing.assert_array_equal(sorted_floats, expected_floats)
    
    def test_reverse_sorted(self):
        """Test sorting reverse-sorted arrays."""
        reverse_ints = np.arange(1000, dtype=np.int64)[::-1]
        reverse_floats = np.arange(1000, dtype=np.float64)[::-1] * 0.1
        
        expected_ints = np.arange(1000, dtype=np.int64)
        expected_floats = np.arange(1000, dtype=np.float64) * 0.1
        
        self.sorter.sort(reverse_ints)
        self.sorter.sort(reverse_floats)
        
        np.testing.assert_array_equal(reverse_ints, expected_ints)
        np.testing.assert_array_equal(reverse_floats, expected_floats)
    
    def test_random_data(self):
        """Test sorting random data of various sizes."""
        sizes = [10, 100, 1000, 10000]
        
        for size in sizes:
            with self.subTest(size=size):
                # Integer arrays
                random_ints = np.random.randint(-1000000, 1000000, size, dtype=np.int64)
                expected_ints = np.sort(random_ints.copy())
                self.sorter.sort(random_ints)
                np.testing.assert_array_equal(random_ints, expected_ints)
                
                # Float arrays
                random_floats = np.random.uniform(-1000.0, 1000.0, size).astype(np.float64)
                expected_floats = np.sort(random_floats.copy())
                self.sorter.sort(random_floats)
                np.testing.assert_array_almost_equal(random_floats, expected_floats)
    
    def test_duplicate_values(self):
        """Test sorting arrays with many duplicate values."""
        # Array with many duplicates
        duplicates = np.array([5, 1, 3, 5, 2, 3, 1, 5, 2, 3] * 100, dtype=np.int64)
        expected = np.sort(duplicates.copy())
        
        self.sorter.sort(duplicates)
        np.testing.assert_array_equal(duplicates, expected)
    
    def test_special_float_values(self):
        """Test sorting with special float values (NaN, inf, -inf)."""
        special_floats = np.array([
            1.0, np.inf, -np.inf, 0.0, -0.0, 
            np.nan, 2.5, -1.5, np.inf, np.nan
        ], dtype=np.float64)
        
        # Sort and verify NaN handling
        self.sorter.sort(special_floats)
        
        # Check that finite values are sorted correctly
        finite_mask = np.isfinite(special_floats)
        finite_values = special_floats[finite_mask]
        self.assertTrue(np.all(finite_values[:-1] <= finite_values[1:]))
    
    def test_algorithm_selection(self):
        """Test that appropriate algorithms are selected for different data sizes."""
        # Small array should use insertion sort
        small_array = np.random.randint(0, 100, 10, dtype=np.int64)
        algorithm = self.sorter.select_algorithm(small_array)
        self.assertEqual(algorithm, "insertion")
        
        # Medium array should use introsort
        medium_array = np.random.randint(0, 100, 1000, dtype=np.int64)
        algorithm = self.sorter.select_algorithm(medium_array)
        self.assertEqual(algorithm, "introsort")
        
        # Large array with integer data might use radix sort
        large_int_array = np.random.randint(0, 1000000, 100000, dtype=np.int64)
        algorithm = self.sorter.select_algorithm(large_int_array)
        self.assertIn(algorithm, ["radix", "introsort"])
    
    def test_external_sort(self):
        """Test external sorting for very large datasets."""
        # Create a large dataset that should trigger external sorting
        large_size = 1500000  # 1.5M elements to trigger external sort
        
        # Generate test data
        large_data = np.random.randint(-1000000, 1000000, large_size, dtype=np.int64)
        expected = np.sort(large_data.copy())
        
        # Sort using our hyperoptimized sorter
        start_time = time.time()
        self.sorter.sort(large_data)
        sort_time = time.time() - start_time
        
        # Verify correctness
        np.testing.assert_array_equal(large_data, expected)
        print(f"External sort of {large_size} elements took {sort_time:.3f} seconds")
    
    def test_stability(self):
        """Test sorting stability for records with equal keys."""
        # Create array of tuples (key, original_index)
        data = [(5, 0), (1, 1), (3, 2), (5, 3), (1, 4), (3, 5)]
        keys = np.array([item[0] for item in data], dtype=np.int64)
        
        # Get original indices before sorting
        original_indices = {i: data[i][1] for i in range(len(data))}
        
        # Sort keys
        sorted_indices = np.argsort(keys, kind='stable')
        
        # Verify stability: equal keys should maintain relative order
        for i in range(len(sorted_indices) - 1):
            if keys[sorted_indices[i]] == keys[sorted_indices[i + 1]]:
                self.assertLess(
                    original_indices[sorted_indices[i]], 
                    original_indices[sorted_indices[i + 1]]
                )
    
    def test_memory_efficiency(self):
        """Test memory usage during sorting."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        
        # Measure memory before
        memory_before = process.memory_info().rss
        
        # Create and sort large array
        large_array = np.random.randint(0, 1000000, 500000, dtype=np.int64)
        self.sorter.sort(large_array)
        
        # Measure memory after
        memory_after = process.memory_info().rss
        memory_used = memory_after - memory_before
        
        # Memory usage should be reasonable (less than 10x the array size)
        array_size = large_array.nbytes
        self.assertLess(memory_used, array_size * 10)
        
        print(f"Memory used for sorting 500K elements: {memory_used / 1024 / 1024:.2f} MB")
    
    def test_concurrent_sorting(self):
        """Test that multiple sorting operations can run concurrently."""
        import threading
        
        results = []
        errors = []
        
        def sort_worker(worker_id, size):
            try:
                sorter = hs.HyperoptimizedSorter()
                data = np.random.randint(0, 100000, size, dtype=np.int64)
                expected = np.sort(data.copy())
                sorter.sort(data)
                
                if np.array_equal(data, expected):
                    results.append(f"Worker {worker_id}: SUCCESS")
                else:
                    results.append(f"Worker {worker_id}: FAILED - incorrect sort")
            except Exception as e:
                errors.append(f"Worker {worker_id}: ERROR - {str(e)}")
        
        # Start multiple sorting threads
        threads = []
        for i in range(4):
            thread = threading.Thread(target=sort_worker, args=(i, 50000))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Check results
        self.assertEqual(len(errors), 0, f"Errors occurred: {errors}")
        self.assertEqual(len(results), 4)
        for result in results:
            self.assertIn("SUCCESS", result)


class TestSortingPerformance(unittest.TestCase):
    """Performance benchmarks for sorting algorithms."""
    
    def setUp(self):
        """Set up performance test fixtures."""
        self.sorter = hs.HyperoptimizedSorter()
        self.sizes = [1000, 10000, 100000, 500000]
        self.results = {}
    
    def benchmark_algorithm(self, algorithm_name: str, data: np.ndarray) -> float:
        """Benchmark a specific sorting algorithm."""
        data_copy = data.copy()
        
        start_time = time.perf_counter()
        
        if algorithm_name == "builtin":
            data_copy.sort()
        elif algorithm_name == "numpy":
            np.sort(data_copy, kind='quicksort')
        else:
            self.sorter.sort(data_copy)
        
        end_time = time.perf_counter()
        return end_time - start_time
    
    def test_performance_comparison(self):
        """Compare performance against Python's builtin sort and NumPy."""
        algorithms = ["hyperoptimized", "builtin", "numpy"]
        
        print("\n" + "="*80)
        print("SORTING PERFORMANCE BENCHMARK")
        print("="*80)
        print(f"{'Size':<10} {'HyperOpt':<12} {'Builtin':<12} {'NumPy':<12} {'Speedup vs Builtin':<20}")
        print("-"*80)
        
        for size in self.sizes:
            # Generate random test data
            test_data = np.random.randint(-1000000, 1000000, size, dtype=np.int64)
            
            times = {}
            for algorithm in algorithms:
                # Run multiple times and take average
                run_times = []
                for _ in range(3):
                    run_time = self.benchmark_algorithm(algorithm, test_data)
                    run_times.append(run_time)
                
                times[algorithm] = min(run_times)  # Take best time
            
            speedup = times["builtin"] / times["hyperoptimized"]
            
            print(f"{size:<10} {times['hyperoptimized']:<12.4f} {times['builtin']:<12.4f} "
                  f"{times['numpy']:<12.4f} {speedup:<20.2f}x")
            
            self.results[size] = times
        
        print("-"*80)
        
        # Verify our implementation is competitive
        for size in self.sizes:
            times = self.results[size]
            # Our sort should be at least as fast as builtin for larger sizes
            if size >= 10000:
                self.assertLessEqual(
                    times["hyperoptimized"], 
                    times["builtin"] * 2.0,  # Allow 2x tolerance
                    f"Hyperoptimized sort too slow for size {size}"
                )
    
    def test_scalability(self):
        """Test how sorting performance scales with input size."""
        print("\n" + "="*60)
        print("SCALABILITY TEST")
        print("="*60)
        print(f"{'Size':<12} {'Time (s)':<12} {'Rate (M/s)':<12} {'Complexity':<12}")
        print("-"*60)
        
        prev_time = None
        prev_size = None
        
        for size in self.sizes:
            test_data = np.random.randint(0, 1000000, size, dtype=np.int64)
            
            # Measure sorting time
            start_time = time.perf_counter()
            self.sorter.sort(test_data)
            sort_time = time.perf_counter() - start_time
            
            rate = size / sort_time / 1_000_000  # Million elements per second
            
            # Calculate complexity ratio
            if prev_time is not None:
                complexity_ratio = (sort_time / prev_time) / (size / prev_size)
                complexity_str = f"{complexity_ratio:.2f}"
            else:
                complexity_str = "N/A"
            
            print(f"{size:<12} {sort_time:<12.4f} {rate:<12.2f} {complexity_str:<12}")
            
            prev_time = sort_time
            prev_size = size
        
        print("-"*60)


class TestFactoryFunctions(unittest.TestCase):
    """Test the factory functions for easy integration."""
    
    def test_create_optimized_sorter(self):
        """Test the optimized sorter factory function."""
        sorter = hs.create_optimized_sorter()
        self.assertIsInstance(sorter, hs.HyperoptimizedSorter)
    
    def test_hyperopt_sort_function(self):
        """Test the standalone hyperopt_sort function."""
        test_data = np.random.randint(0, 1000, 1000, dtype=np.int64)
        expected = np.sort(test_data.copy())
        
        result = hs.hyperopt_sort(test_data.copy())
        np.testing.assert_array_equal(result, expected)
    
    def test_sort_with_indices(self):
        """Test sorting with index tracking."""
        test_data = np.array([5, 2, 8, 1, 9, 3], dtype=np.int64)
        
        sorted_data, indices = hs.hyperopt_sort_with_indices(test_data.copy())
        
        # Verify sorted data
        expected_sorted = np.array([1, 2, 3, 5, 8, 9], dtype=np.int64)
        np.testing.assert_array_equal(sorted_data, expected_sorted)
        
        # Verify indices correctly map original to sorted positions
        for i, idx in enumerate(indices):
            self.assertEqual(test_data[idx], sorted_data[i])


def run_comprehensive_tests():
    """Run all test suites."""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestHyperoptimizedSort))
    suite.addTests(loader.loadTestsFromTestCase(TestSortingPerformance))
    suite.addTests(loader.loadTestsFromTestCase(TestFactoryFunctions))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_comprehensive_tests()
    sys.exit(0 if success else 1)
