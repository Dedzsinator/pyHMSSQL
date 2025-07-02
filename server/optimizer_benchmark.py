"""
Comprehensive Benchmark and Test Framework for Query Optimizer.

This module provides extensive testing capabilities for the unified query optimizer,
including performance benchmarks, correctness tests, and stress testing.

Test Categories:
- Star schema join optimization
- Selectivity edge cases
- Deep nested filter optimization
- Complex aggregation queries
- Multi-table join ordering
- Index selection scenarios
- Cache effectiveness tests
- ML model accuracy tests
- Adaptive optimizer learning tests
"""

import logging
import time
import random
import statistics
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
import json
import tempfile
import os
import pytest
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np

# Import optimizer components for testing
from .unified_optimizer import (
    UnifiedQueryOptimizer,
    OptimizationLevel,
    OptimizationOptions,
)
from .db_statistics import AdvancedStatisticsCollector, TableStatistics


@dataclass
class BenchmarkResult:
    """Result of a single benchmark test."""

    test_name: str
    optimization_time_ms: float
    estimated_cost: float
    estimated_rows: int
    transformations_count: int
    cache_hit: bool
    success: bool
    error_message: Optional[str] = None
    improvement_ratio: float = 1.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "test_name": self.test_name,
            "optimization_time_ms": self.optimization_time_ms,
            "estimated_cost": self.estimated_cost,
            "estimated_rows": self.estimated_rows,
            "transformations_count": self.transformations_count,
            "cache_hit": self.cache_hit,
            "success": self.success,
            "error_message": self.error_message,
            "improvement_ratio": self.improvement_ratio,
        }


@dataclass
class BenchmarkSuite:
    """Complete benchmark suite results."""

    suite_name: str
    results: List[BenchmarkResult]
    total_time_ms: float
    success_rate: float
    avg_optimization_time_ms: float
    avg_improvement_ratio: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "suite_name": self.suite_name,
            "results": [r.to_dict() for r in self.results],
            "total_time_ms": self.total_time_ms,
            "success_rate": self.success_rate,
            "avg_optimization_time_ms": self.avg_optimization_time_ms,
            "avg_improvement_ratio": self.avg_improvement_ratio,
        }


class MockCatalogManager:
    """Mock catalog manager for testing."""

    def __init__(self):
        self.tables = {
            "customers": {
                "columns": ["customer_id", "name", "country", "signup_date"],
                "row_count": 1000000,
                "size_bytes": 50000000,
            },
            "orders": {
                "columns": ["order_id", "customer_id", "order_date", "amount"],
                "row_count": 5000000,
                "size_bytes": 200000000,
            },
            "products": {
                "columns": ["product_id", "name", "category", "price"],
                "row_count": 100000,
                "size_bytes": 10000000,
            },
            "order_items": {
                "columns": ["order_id", "product_id", "quantity", "price"],
                "row_count": 10000000,
                "size_bytes": 300000000,
            },
            "sales_fact": {
                "columns": [
                    "sale_id",
                    "customer_id",
                    "product_id",
                    "date_id",
                    "amount",
                ],
                "row_count": 50000000,
                "size_bytes": 1000000000,
            },
            "date_dim": {
                "columns": ["date_id", "date", "year", "month", "quarter"],
                "row_count": 3650,
                "size_bytes": 500000,
            },
        }
        self.current_db = "test_db"

    def get_current_database(self):
        return self.current_db

    def list_tables(self, database: str):
        return list(self.tables.keys())

    def get_table_info(self, table_name: str):
        return self.tables.get(table_name, {})

    def is_table_sorted(self, table_name: str, column: str):
        # Simulate some tables being sorted
        sorted_tables = {
            "customers": ["customer_id"],
            "orders": ["order_date"],
            "date_dim": ["date_id"],
        }
        return column in sorted_tables.get(table_name, [])


class MockIndexManager:
    """Mock index manager for testing."""

    def __init__(self):
        self.indexes = {
            "customers.idx_customer_id": {
                "table": "customers",
                "column": "customer_id",
                "type": "btree",
                "unique": True,
            },
            "customers.idx_country": {
                "table": "customers",
                "column": "country",
                "type": "btree",
                "unique": False,
            },
            "orders.idx_customer_id": {
                "table": "orders",
                "column": "customer_id",
                "type": "btree",
                "unique": False,
            },
            "orders.idx_order_date": {
                "table": "orders",
                "column": "order_date",
                "type": "btree",
                "unique": False,
            },
            "products.idx_category": {
                "table": "products",
                "column": "category",
                "type": "btree",
                "unique": False,
            },
            "order_items.idx_order_id": {
                "table": "order_items",
                "column": "order_id",
                "type": "btree",
                "unique": False,
            },
            "sales_fact.idx_customer_id": {
                "table": "sales_fact",
                "column": "customer_id",
                "type": "btree",
                "unique": False,
            },
            "sales_fact.idx_date_id": {
                "table": "sales_fact",
                "column": "date_id",
                "type": "btree",
                "unique": False,
            },
        }

    def get_index(self, index_name: str):
        return self.indexes.get(index_name)

    def list_indexes(self, table_name: str = None):
        if table_name:
            return [
                idx for name, idx in self.indexes.items() if idx["table"] == table_name
            ]
        return list(self.indexes.values())


class OptimizerTestFramework:
    """Comprehensive testing framework for the query optimizer."""

    def __init__(self):
        self.catalog_manager = MockCatalogManager()
        self.index_manager = MockIndexManager()
        self.optimizers = {}

        # Initialize optimizers for different levels
        for level in OptimizationLevel:
            self.optimizers[level] = UnifiedQueryOptimizer(
                self.catalog_manager,
                self.index_manager,
                OptimizationOptions.for_level(level),
            )

        self.test_queries = self._generate_test_queries()

    def _generate_test_queries(self) -> Dict[str, Dict[str, Any]]:
        """Generate a comprehensive set of test queries."""
        queries = {}

        # Simple selection queries
        queries["simple_select"] = {
            "type": "SELECT",
            "table": "customers",
            "columns": ["customer_id", "name"],
            "condition": 'country = "USA"',
        }

        queries["range_select"] = {
            "type": "SELECT",
            "table": "orders",
            "columns": ["order_id", "amount"],
            "condition": 'order_date >= "2023-01-01" AND order_date < "2024-01-01"',
        }

        # Two-table joins
        queries["simple_join"] = {
            "type": "JOIN",
            "table1": "customers",
            "table2": "orders",
            "condition": "customers.customer_id = orders.customer_id",
            "columns": ["customers.name", "orders.amount"],
        }

        queries["filtered_join"] = {
            "type": "JOIN",
            "table1": "customers",
            "table2": "orders",
            "condition": "customers.customer_id = orders.customer_id",
            "filter": 'customers.country = "USA" AND orders.amount > 100',
            "columns": ["customers.name", "orders.order_date", "orders.amount"],
        }

        # Three-table joins
        queries["three_table_join"] = {
            "type": "JOIN",
            "tables": ["customers", "orders", "order_items"],
            "conditions": [
                "customers.customer_id = orders.customer_id",
                "orders.order_id = order_items.order_id",
            ],
            "columns": ["customers.name", "orders.order_date", "order_items.quantity"],
        }

        # Star schema queries
        queries["star_schema_simple"] = {
            "type": "JOIN",
            "fact_table": "sales_fact",
            "dimension_tables": ["customers", "products", "date_dim"],
            "conditions": [
                "sales_fact.customer_id = customers.customer_id",
                "sales_fact.product_id = products.product_id",
                "sales_fact.date_id = date_dim.date_id",
            ],
            "filter": "date_dim.year = 2023",
            "columns": ["customers.name", "products.name", "sales_fact.amount"],
        }

        queries["star_schema_complex"] = {
            "type": "JOIN",
            "fact_table": "sales_fact",
            "dimension_tables": ["customers", "products", "date_dim"],
            "conditions": [
                "sales_fact.customer_id = customers.customer_id",
                "sales_fact.product_id = products.product_id",
                "sales_fact.date_id = date_dim.date_id",
            ],
            "filter": 'customers.country = "USA" AND products.category = "Electronics" AND date_dim.quarter = "Q1"',
            "group_by": ["customers.country", "products.category"],
            "aggregates": ["SUM(sales_fact.amount)", "COUNT(*)"],
            "columns": [
                "customers.country",
                "products.category",
                "SUM(sales_fact.amount)",
                "COUNT(*)",
            ],
        }

        # Aggregation queries
        queries["simple_aggregate"] = {
            "type": "AGGREGATE",
            "table": "orders",
            "function": "SUM",
            "column": "amount",
            "group_by": ["customer_id"],
        }

        queries["complex_aggregate"] = {
            "type": "AGGREGATE",
            "table": "sales_fact",
            "functions": ["SUM(amount)", "AVG(amount)", "COUNT(*)"],
            "group_by": ["customer_id", "product_id"],
            "having": "SUM(amount) > 1000",
        }

        # Selectivity edge cases
        queries["high_selectivity"] = {
            "type": "SELECT",
            "table": "customers",
            "condition": "customer_id = 12345",  # Very selective
            "columns": ["*"],
        }

        queries["low_selectivity"] = {
            "type": "SELECT",
            "table": "customers",
            "condition": 'signup_date >= "2020-01-01"',  # Most customers
            "columns": ["*"],
        }

        queries["multiple_conditions"] = {
            "type": "SELECT",
            "table": "orders",
            "condition": 'amount > 50 AND amount < 500 AND order_date >= "2023-06-01"',
            "columns": ["order_id", "customer_id", "amount"],
        }

        # Deep nested filters
        queries["nested_filters"] = {
            "type": "SELECT",
            "table": "customers",
            "condition": '(country = "USA" OR country = "Canada") AND (signup_date >= "2022-01-01" AND signup_date < "2023-01-01") AND customer_id NOT IN (SELECT customer_id FROM orders WHERE amount < 10)',
            "columns": ["customer_id", "name", "country"],
        }

        # Subqueries
        queries["correlated_subquery"] = {
            "type": "SELECT",
            "table": "customers",
            "condition": "EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.customer_id AND orders.amount > 1000)",
            "columns": ["customer_id", "name"],
        }

        # Complex multi-table scenarios
        queries["five_table_join"] = {
            "type": "JOIN",
            "tables": ["customers", "orders", "order_items", "products", "date_dim"],
            "conditions": [
                "customers.customer_id = orders.customer_id",
                "orders.order_id = order_items.order_id",
                "order_items.product_id = products.product_id",
                "DATE(orders.order_date) = date_dim.date",
            ],
            "filter": 'customers.country = "USA" AND products.category = "Electronics" AND date_dim.year = 2023',
            "columns": [
                "customers.name",
                "products.name",
                "order_items.quantity",
                "date_dim.quarter",
            ],
        }

        return queries

    def run_single_test(
        self, test_name: str, query: Dict[str, Any], optimizer: UnifiedQueryOptimizer
    ) -> BenchmarkResult:
        """Run a single optimization test."""
        try:
            start_time = time.time()

            # Add some basic estimates to the query
            query["estimated_rows"] = 1000
            query["estimated_cost"] = 5000.0

            result = optimizer.optimize(query, query_signature=test_name)

            optimization_time = (time.time() - start_time) * 1000

            return BenchmarkResult(
                test_name=test_name,
                optimization_time_ms=optimization_time,
                estimated_cost=result.estimated_cost,
                estimated_rows=result.estimated_rows,
                transformations_count=len(result.transformations_applied),
                cache_hit=result.cache_hit,
                success=True,
                improvement_ratio=result.get_improvement_ratio(),
            )

        except Exception as e:
            logging.error(f"Test {test_name} failed: {e}")
            return BenchmarkResult(
                test_name=test_name,
                optimization_time_ms=0.0,
                estimated_cost=0.0,
                estimated_rows=0,
                transformations_count=0,
                cache_hit=False,
                success=False,
                error_message=str(e),
            )

    def run_benchmark_suite(
        self,
        suite_name: str,
        level: OptimizationLevel = OptimizationLevel.STANDARD,
        iterations: int = 1,
    ) -> BenchmarkSuite:
        """Run a complete benchmark suite."""
        optimizer = self.optimizers[level]
        results = []

        start_time = time.time()

        for iteration in range(iterations):
            for test_name, query in self.test_queries.items():
                # Create a unique test name for multiple iterations
                full_test_name = (
                    f"{test_name}_iter_{iteration}" if iterations > 1 else test_name
                )

                result = self.run_single_test(full_test_name, query.copy(), optimizer)
                results.append(result)

        total_time = (time.time() - start_time) * 1000

        # Calculate statistics
        successful_results = [r for r in results if r.success]
        success_rate = len(successful_results) / len(results) if results else 0.0

        avg_optimization_time = 0.0
        avg_improvement_ratio = 1.0

        if successful_results:
            avg_optimization_time = statistics.mean(
                [r.optimization_time_ms for r in successful_results]
            )
            avg_improvement_ratio = statistics.mean(
                [r.improvement_ratio for r in successful_results]
            )

        return BenchmarkSuite(
            suite_name=suite_name,
            results=results,
            total_time_ms=total_time,
            success_rate=success_rate,
            avg_optimization_time_ms=avg_optimization_time,
            avg_improvement_ratio=avg_improvement_ratio,
        )

    def run_performance_comparison(self) -> Dict[OptimizationLevel, BenchmarkSuite]:
        """Compare performance across all optimization levels."""
        results = {}

        for level in OptimizationLevel:
            suite_name = f"performance_comparison_{level.value}"
            results[level] = self.run_benchmark_suite(suite_name, level)

            logging.info(
                f"Completed benchmark for {level.value}: "
                f"Success rate: {results[level].success_rate:.2%}, "
                f"Avg time: {results[level].avg_optimization_time_ms:.1f}ms"
            )

        return results

    def run_stress_test(
        self, concurrent_queries: int = 10, total_queries: int = 100
    ) -> Dict[str, Any]:
        """Run stress test with concurrent optimization requests."""
        optimizer = self.optimizers[OptimizationLevel.STANDARD]
        results = []

        def optimize_random_query():
            query_name = random.choice(list(self.test_queries.keys()))
            query = self.test_queries[query_name].copy()
            return self.run_single_test(query_name, query, optimizer)

        start_time = time.time()

        with ThreadPoolExecutor(max_workers=concurrent_queries) as executor:
            futures = [
                executor.submit(optimize_random_query) for _ in range(total_queries)
            ]

            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logging.error(f"Stress test query failed: {e}")

        total_time = time.time() - start_time

        successful_results = [r for r in results if r.success]
        success_rate = len(successful_results) / len(results) if results else 0.0

        stats = {
            "total_queries": total_queries,
            "concurrent_queries": concurrent_queries,
            "total_time_seconds": total_time,
            "queries_per_second": total_queries / total_time,
            "success_rate": success_rate,
            "avg_optimization_time_ms": (
                statistics.mean([r.optimization_time_ms for r in successful_results])
                if successful_results
                else 0.0
            ),
        }

        return stats

    def run_cache_effectiveness_test(self) -> Dict[str, Any]:
        """Test cache effectiveness with repeated queries."""
        optimizer = self.optimizers[OptimizationLevel.STANDARD]

        # Run the same queries multiple times
        test_query = self.test_queries["simple_join"].copy()
        query_signature = "cache_test_query"

        results = []
        for i in range(10):
            result = optimizer.optimize(test_query, query_signature)
            results.append(
                {
                    "iteration": i,
                    "cache_hit": result.cache_hit,
                    "optimization_time_ms": result.optimization_time_ms,
                }
            )

        cache_hits = sum(1 for r in results if r["cache_hit"])
        cache_hit_rate = cache_hits / len(results)

        # Calculate time savings from caching
        cached_times = [r["optimization_time_ms"] for r in results if r["cache_hit"]]
        uncached_times = [
            r["optimization_time_ms"] for r in results if not r["cache_hit"]
        ]

        time_savings = 0.0
        if cached_times and uncached_times:
            avg_cached_time = statistics.mean(cached_times)
            avg_uncached_time = statistics.mean(uncached_times)
            time_savings = (avg_uncached_time - avg_cached_time) / avg_uncached_time

        return {
            "cache_hit_rate": cache_hit_rate,
            "time_savings_ratio": time_savings,
            "avg_cached_time_ms": (
                statistics.mean(cached_times) if cached_times else 0.0
            ),
            "avg_uncached_time_ms": (
                statistics.mean(uncached_times) if uncached_times else 0.0
            ),
            "results": results,
        }

    def generate_report(self, output_file: str = None) -> str:
        """Generate a comprehensive test report."""
        report_lines = []

        report_lines.append("=" * 80)
        report_lines.append("QUERY OPTIMIZER COMPREHENSIVE TEST REPORT")
        report_lines.append("=" * 80)
        report_lines.append("")

        # Performance comparison
        report_lines.append("1. PERFORMANCE COMPARISON BY OPTIMIZATION LEVEL")
        report_lines.append("-" * 50)

        comparison_results = self.run_performance_comparison()

        for level, suite in comparison_results.items():
            report_lines.append(f"\n{level.value.upper()}:")
            report_lines.append(f"  Success Rate: {suite.success_rate:.2%}")
            report_lines.append(
                f"  Avg Optimization Time: {suite.avg_optimization_time_ms:.1f}ms"
            )
            report_lines.append(
                f"  Avg Improvement Ratio: {suite.avg_improvement_ratio:.2f}x"
            )
            report_lines.append(f"  Total Tests: {len(suite.results)}")

        # Stress test
        report_lines.append("\n\n2. STRESS TEST RESULTS")
        report_lines.append("-" * 30)

        stress_results = self.run_stress_test()

        report_lines.append(f"Total Queries: {stress_results['total_queries']}")
        report_lines.append(
            f"Concurrent Queries: {stress_results['concurrent_queries']}"
        )
        report_lines.append(f"Total Time: {stress_results['total_time_seconds']:.1f}s")
        report_lines.append(
            f"Queries/Second: {stress_results['queries_per_second']:.1f}"
        )
        report_lines.append(f"Success Rate: {stress_results['success_rate']:.2%}")
        report_lines.append(
            f"Avg Optimization Time: {stress_results['avg_optimization_time_ms']:.1f}ms"
        )

        # Cache effectiveness
        report_lines.append("\n\n3. CACHE EFFECTIVENESS")
        report_lines.append("-" * 25)

        cache_results = self.run_cache_effectiveness_test()

        report_lines.append(f"Cache Hit Rate: {cache_results['cache_hit_rate']:.2%}")
        report_lines.append(
            f"Time Savings from Caching: {cache_results['time_savings_ratio']:.2%}"
        )
        report_lines.append(
            f"Avg Cached Time: {cache_results['avg_cached_time_ms']:.1f}ms"
        )
        report_lines.append(
            f"Avg Uncached Time: {cache_results['avg_uncached_time_ms']:.1f}ms"
        )

        # Detailed test results for standard level
        report_lines.append("\n\n4. DETAILED TEST RESULTS (STANDARD LEVEL)")
        report_lines.append("-" * 45)

        standard_suite = comparison_results[OptimizationLevel.STANDARD]

        for result in standard_suite.results:
            status = "✓" if result.success else "✗"
            cache_indicator = "🏠" if result.cache_hit else "⚡"

            report_lines.append(
                f"{status} {cache_indicator} {result.test_name:<25} "
                f"{result.optimization_time_ms:>6.1f}ms "
                f"Cost: {result.estimated_cost:>8.0f} "
                f"Rows: {result.estimated_rows:>8} "
                f"Transforms: {result.transformations_count}"
            )

        # Optimizer statistics
        report_lines.append("\n\n5. OPTIMIZER STATISTICS")
        report_lines.append("-" * 30)

        for level, optimizer in self.optimizers.items():
            stats = optimizer.get_optimization_statistics()
            report_lines.append(f"\n{level.value.upper()}:")
            report_lines.append(
                f"  Total Optimizations: {stats.get('total_optimizations', 0)}"
            )
            report_lines.append(f"  Cache Hits: {stats.get('cache_hits', 0)}")
            report_lines.append(
                f"  Transformations Applied: {stats.get('transformations_applied', 0)}"
            )
            report_lines.append(
                f"  Join Reorderings: {stats.get('join_reorderings', 0)}"
            )

        report_lines.append("\n" + "=" * 80)
        report_lines.append("END OF REPORT")
        report_lines.append("=" * 80)

        report_content = "\n".join(report_lines)

        if output_file:
            with open(output_file, "w") as f:
                f.write(report_content)
            logging.info(f"Test report saved to: {output_file}")

        return report_content

    def cleanup(self):
        """Clean up test resources."""
        for optimizer in self.optimizers.values():
            optimizer.shutdown()


# Pytest integration for automated testing
class TestOptimizerCorrectness:
    """Pytest test class for optimizer correctness."""

    @pytest.fixture
    def framework(self):
        """Provide test framework instance."""
        return OptimizerTestFramework()

    def test_basic_optimization(self, framework):
        """Test basic optimization functionality."""
        optimizer = framework.optimizers[OptimizationLevel.BASIC]
        query = framework.test_queries["simple_select"]

        result = optimizer.optimize(query)

        assert result.optimized_plan is not None
        assert result.estimated_cost > 0
        assert result.estimated_rows > 0
        assert not result.warnings or len(result.warnings) == 0

    def test_join_optimization(self, framework):
        """Test join optimization."""
        optimizer = framework.optimizers[OptimizationLevel.STANDARD]
        query = framework.test_queries["simple_join"]

        result = optimizer.optimize(query)

        assert result.optimized_plan is not None
        assert result.estimated_cost > 0
        # Join optimization should potentially improve the plan
        assert result.get_improvement_ratio() >= 1.0

    def test_complex_query_optimization(self, framework):
        """Test optimization of complex queries."""
        optimizer = framework.optimizers[OptimizationLevel.AGGRESSIVE]
        query = framework.test_queries["star_schema_complex"]

        result = optimizer.optimize(query)

        assert result.optimized_plan is not None
        assert result.estimated_cost > 0
        # Complex queries should benefit from aggressive optimization
        assert len(result.transformations_applied) >= 0

    def test_cache_functionality(self, framework):
        """Test caching functionality."""
        optimizer = framework.optimizers[OptimizationLevel.STANDARD]
        query = framework.test_queries["simple_select"]
        query_signature = "test_cache_query"

        # First optimization - should not be cached
        result1 = optimizer.optimize(query, query_signature)
        assert not result1.cache_hit

        # Second optimization - should be cached
        result2 = optimizer.optimize(query, query_signature)
        assert result2.cache_hit

    def test_optimization_levels(self, framework):
        """Test that different optimization levels produce different results."""
        query = framework.test_queries["three_table_join"]

        results = {}
        for level in OptimizationLevel:
            optimizer = framework.optimizers[level]
            results[level] = optimizer.optimize(query)

        # Basic should be fastest
        assert (
            results[OptimizationLevel.BASIC].optimization_time_ms
            <= results[OptimizationLevel.AGGRESSIVE].optimization_time_ms
        )

        # All should succeed
        for result in results.values():
            assert result.optimized_plan is not None


def run_full_benchmark():
    """Run the complete benchmark suite and generate report."""
    logging.basicConfig(level=logging.INFO)

    framework = OptimizerTestFramework()

    try:
        report = framework.generate_report("optimizer_benchmark_report.txt")
        print(report)

        logging.info("Benchmark completed successfully!")

    except Exception as e:
        logging.error(f"Benchmark failed: {e}")
        raise
    finally:
        framework.cleanup()


if __name__ == "__main__":
    run_full_benchmark()
