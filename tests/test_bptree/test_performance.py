"""
Performance benchmarking for B+ tree without profiling overhead.

This module provides performance benchmarks for the B+ tree implementation
without the overhead of detailed system profiling, allowing for quick
performance validation during development.
"""

import time
import statistics
import random
import os
import sys

# Add server directory to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
server_dir = os.path.join(project_root, "server")
if server_dir not in sys.path:
    sys.path.insert(0, server_dir)

# Import the B+ tree implementation
try:
    from bptree import BPlusTreeOptimized as BPTree
except ImportError:
    try:
        from bptree import BPlusTree as BPTree
    except ImportError:
        raise ImportError("Could not import B+ tree implementation")


class BPTreeBenchmark:
    """Lightweight benchmarking class for B+ tree operations."""

    def __init__(self):
        self.results = {
            "single_dim": {},
            "multi_dim": {},
            "composite": {},
            "range_queries": {},
        }

    def time_operation(self, operation, *args, **kwargs):
        """Time a single operation and return the duration."""
        start_time = time.perf_counter()
        result = operation(*args, **kwargs)
        end_time = time.perf_counter()
        return end_time - start_time, result

    def benchmark_single_dim_insertions(
        self, sizes=[100, 500, 1000], orders=[3, 5, 10]
    ):
        """Benchmark single-dimensional insertions."""
        print("Benchmarking single-dimensional insertions...")

        for order in orders:
            for size in sizes:
                tree = BPTree(order=order)

                # Time insertions
                start_time = time.perf_counter()
                for i in range(size):
                    tree.insert(i, f"value_{i}")
                insertion_time = time.perf_counter() - start_time

                # Time searches
                start_time = time.perf_counter()
                for i in range(size):
                    tree.search(i)
                search_time = time.perf_counter() - start_time

                key = f"order_{order}_size_{size}"
                self.results["single_dim"][key] = {
                    "insertion_time": insertion_time,
                    "search_time": search_time,
                    "insertion_rate": (
                        size / insertion_time if insertion_time > 0 else float("inf")
                    ),
                    "search_rate": (
                        size / search_time if search_time > 0 else float("inf")
                    ),
                }

                print(
                    f"  Order {order}, Size {size}: Insert {insertion_time:.4f}s ({size/insertion_time:.0f} ops/s), "
                    f"Search {search_time:.4f}s ({size/search_time:.0f} ops/s)"
                )

    def benchmark_multi_dim_insertions(
        self, sizes=[100, 500], orders=[3, 5, 10], dimensions=[2, 3]
    ):
        """Benchmark multidimensional insertions."""
        print("Benchmarking multidimensional insertions...")

        for order in orders:
            for dim in dimensions:
                for size in sizes:
                    tree = BPTree(order=order)

                    # Generate multidimensional points
                    points = []
                    for i in range(size):
                        point = [float(i + j) for j in range(dim)]
                        points.append((point, f"value_{i}"))

                    # Time insertions
                    start_time = time.perf_counter()
                    for point, value in points:
                        tree.insert_multidim(point, value)
                    insertion_time = time.perf_counter() - start_time

                    # Time searches
                    start_time = time.perf_counter()
                    for point, value in points:
                        tree.search_multidim(point)
                    search_time = time.perf_counter() - start_time

                    key = f"order_{order}_dim_{dim}_size_{size}"
                    self.results["multi_dim"][key] = {
                        "insertion_time": insertion_time,
                        "search_time": search_time,
                        "insertion_rate": (
                            size / insertion_time
                            if insertion_time > 0
                            else float("inf")
                        ),
                        "search_rate": (
                            size / search_time if search_time > 0 else float("inf")
                        ),
                    }

                    print(
                        f"  Order {order}, {dim}D, Size {size}: Insert {insertion_time:.4f}s ({size/insertion_time:.0f} ops/s), "
                        f"Search {search_time:.4f}s ({size/search_time:.0f} ops/s)"
                    )

    def benchmark_composite_operations(self, size=500, order=5):
        """Benchmark composite operations."""
        print("Benchmarking composite operations...")

        if not hasattr(BPTree(order=3), "insert_composite"):
            print("  Composite operations not available")
            return

        tree = BPTree(order=order)

        # Mixed data types
        single_data = [(i, f"single_{i}") for i in range(size // 2)]
        multi_data = [
            ([float(i), float(i * 2)], f"multi_{i}") for i in range(size // 2)
        ]

        # Time composite insertions
        start_time = time.perf_counter()
        for key, value in single_data:
            tree.insert_composite(key, value)
        for key, value in multi_data:
            tree.insert_composite(key, value)
        insertion_time = time.perf_counter() - start_time

        # Time composite searches
        start_time = time.perf_counter()
        for key, value in single_data:
            tree.search_composite(key)
        for key, value in multi_data:
            tree.search_composite(key)
        search_time = time.perf_counter() - start_time

        self.results["composite"][f"order_{order}_size_{size}"] = {
            "insertion_time": insertion_time,
            "search_time": search_time,
            "insertion_rate": (
                size / insertion_time if insertion_time > 0 else float("inf")
            ),
            "search_rate": size / search_time if search_time > 0 else float("inf"),
        }

        print(
            f"  Order {order}, Size {size}: Insert {insertion_time:.4f}s ({size/insertion_time:.0f} ops/s), "
            f"Search {search_time:.4f}s ({size/search_time:.0f} ops/s)"
        )

    def benchmark_range_queries(self, grid_size=20, order=10):
        """Benchmark range query performance."""
        print("Benchmarking range queries...")

        # Single-dimensional range queries
        tree_1d = BPTree(order=order)
        for i in range(grid_size * grid_size):
            tree_1d.insert(i, f"value_{i}")

        if hasattr(tree_1d, "range_query"):
            # Time single-dimensional range queries
            start_time = time.perf_counter()
            for i in range(0, grid_size * grid_size, grid_size):
                tree_1d.range_query(i, i + grid_size // 2)
            range_1d_time = time.perf_counter() - start_time

            self.results["range_queries"]["single_dim"] = {
                "time": range_1d_time,
                "queries": grid_size,
                "rate": (
                    grid_size / range_1d_time if range_1d_time > 0 else float("inf")
                ),
            }

            print(
                f"  1D Range queries: {range_1d_time:.4f}s ({grid_size/range_1d_time:.0f} queries/s)"
            )

        # Multidimensional range queries
        tree_2d = BPTree(order=order)
        for x in range(grid_size):
            for y in range(grid_size):
                tree_2d.insert_multidim([float(x), float(y)], f"grid_{x}_{y}")

        if hasattr(tree_2d, "range_query_multidim"):
            # Time multidimensional range queries
            start_time = time.perf_counter()
            query_size = 5
            num_queries = 0
            for x in range(0, grid_size - query_size, query_size):
                for y in range(0, grid_size - query_size, query_size):
                    tree_2d.range_query_multidim(
                        [float(x), float(y)],
                        [float(x + query_size), float(y + query_size)],
                    )
                    num_queries += 1
            range_2d_time = time.perf_counter() - start_time

            self.results["range_queries"]["multi_dim"] = {
                "time": range_2d_time,
                "queries": num_queries,
                "rate": (
                    num_queries / range_2d_time if range_2d_time > 0 else float("inf")
                ),
            }

            print(
                f"  2D Range queries: {range_2d_time:.4f}s ({num_queries/range_2d_time:.0f} queries/s)"
            )

    def benchmark_memory_efficiency(self, sizes=[1000, 5000, 10000]):
        """Benchmark memory efficiency with different tree orders."""
        print("Benchmarking memory efficiency (order comparison)...")

        orders = [3, 5, 10, 20]

        for size in sizes:
            print(f"  Size {size}:")
            for order in orders:
                tree = BPTree(order=order)

                # Time insertion for this order
                start_time = time.perf_counter()
                for i in range(size):
                    tree.insert(i, f"value_{i}")
                insertion_time = time.perf_counter() - start_time

                # Quick verification
                sample_checks = min(100, size)
                correct = 0
                for i in range(0, size, size // sample_checks):
                    if tree.search(i) == f"value_{i}":
                        correct += 1

                accuracy = correct / sample_checks

                print(
                    f"    Order {order}: {insertion_time:.4f}s, Accuracy: {accuracy:.2%}"
                )

    def run_all_benchmarks(self):
        """Run all benchmark suites."""
        print("Starting B+ Tree Performance Benchmarks")
        print("=" * 50)

        self.benchmark_single_dim_insertions()
        print()

        self.benchmark_multi_dim_insertions()
        print()

        self.benchmark_composite_operations()
        print()

        self.benchmark_range_queries()
        print()

        self.benchmark_memory_efficiency()
        print()

        self.print_summary()

    def print_summary(self):
        """Print a summary of benchmark results."""
        print("Benchmark Summary")
        print("=" * 50)

        # Single-dimensional summary
        if self.results["single_dim"]:
            print("Single-dimensional Operations:")
            best_insert_rate = 0
            best_search_rate = 0
            for key, data in self.results["single_dim"].items():
                if data["insertion_rate"] > best_insert_rate:
                    best_insert_rate = data["insertion_rate"]
                if data["search_rate"] > best_search_rate:
                    best_search_rate = data["search_rate"]
            print(f"  Best insertion rate: {best_insert_rate:.0f} ops/s")
            print(f"  Best search rate: {best_search_rate:.0f} ops/s")

        # Multidimensional summary
        if self.results["multi_dim"]:
            print("Multidimensional Operations:")
            best_insert_rate = 0
            best_search_rate = 0
            for key, data in self.results["multi_dim"].items():
                if data["insertion_rate"] > best_insert_rate:
                    best_insert_rate = data["insertion_rate"]
                if data["search_rate"] > best_search_rate:
                    best_search_rate = data["search_rate"]
            print(f"  Best insertion rate: {best_insert_rate:.0f} ops/s")
            print(f"  Best search rate: {best_search_rate:.0f} ops/s")

        # Range query summary
        if self.results["range_queries"]:
            print("Range Query Operations:")
            for query_type, data in self.results["range_queries"].items():
                print(f"  {query_type}: {data['rate']:.0f} queries/s")


def test_benchmark_suite():
    """Test function for pytest compatibility."""
    benchmark = BPTreeBenchmark()

    # Run smaller benchmarks for testing
    benchmark.benchmark_single_dim_insertions(sizes=[100], orders=[5])
    benchmark.benchmark_multi_dim_insertions(sizes=[50], orders=[5], dimensions=[2])
    benchmark.benchmark_composite_operations(size=100, order=5)
    benchmark.benchmark_range_queries(grid_size=10, order=5)

    # Verify we have some results
    assert len(benchmark.results["single_dim"]) > 0
    assert len(benchmark.results["multi_dim"]) > 0


if __name__ == "__main__":
    # Run full benchmark suite
    benchmark = BPTreeBenchmark()
    benchmark.run_all_benchmarks()
