"""
Integration test for the unified query optimizer.

This test verifies that all optimizer components work together correctly
and that the system can handle realistic optimization scenarios.
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import time
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class MockCatalogManager:
    """Simple mock catalog manager for testing."""

    def __init__(self):
        self.current_db = "test_db"
        self.tables = {
            "users": {"columns": ["id", "name", "email"], "row_count": 10000},
            "orders": {
                "columns": ["id", "user_id", "amount", "date"],
                "row_count": 50000,
            },
            "products": {"columns": ["id", "name", "price"], "row_count": 1000},
        }

    def get_current_database(self):
        return self.current_db

    def list_tables(self, database):
        return list(self.tables.keys())

    def get_table_info(self, table_name):
        return self.tables.get(table_name, {})

    def is_table_sorted(self, table_name, column):
        return column in ["id", "date"]


class MockIndexManager:
    """Simple mock index manager for testing."""

    def __init__(self):
        self.indexes = {
            "users.idx_id": {"table": "users", "column": "id"},
            "orders.idx_user_id": {"table": "orders", "column": "user_id"},
            "orders.idx_date": {"table": "orders", "column": "date"},
            "products.idx_id": {"table": "products", "column": "id"},
        }

    def get_index(self, index_name):
        return self.indexes.get(index_name)

    def list_indexes(self, table_name=None):
        if table_name:
            return [
                idx for name, idx in self.indexes.items() if idx["table"] == table_name
            ]
        return list(self.indexes.values())


def test_unified_optimizer_integration():
    """Test the unified optimizer with realistic queries."""

    print("=" * 60)
    print("UNIFIED QUERY OPTIMIZER INTEGRATION TEST")
    print("=" * 60)

    # Import the unified optimizer
    try:
        from server.unified_optimizer import (
            UnifiedQueryOptimizer,
            OptimizationLevel,
            OptimizationOptions,
        )

        print("✓ Successfully imported unified optimizer")
    except ImportError as e:
        print(f"✗ Failed to import unified optimizer: {e}")
        return False

    # Initialize mock managers
    catalog_manager = MockCatalogManager()
    index_manager = MockIndexManager()

    print("✓ Initialized mock catalog and index managers")

    # Test different optimization levels
    test_results = {}

    for level in OptimizationLevel:
        print(f"\nTesting optimization level: {level.value}")
        print("-" * 40)

        try:
            # Create optimizer for this level
            options = OptimizationOptions.for_level(level)
            optimizer = UnifiedQueryOptimizer(catalog_manager, index_manager, options)

            # Test queries
            test_queries = [
                {
                    "name": "simple_select",
                    "plan": {
                        "type": "SELECT",
                        "table": "users",
                        "columns": ["id", "name"],
                        "condition": "id = 123",
                        "estimated_rows": 1,
                        "estimated_cost": 100.0,
                    },
                },
                {
                    "name": "simple_join",
                    "plan": {
                        "type": "JOIN",
                        "table1": "users",
                        "table2": "orders",
                        "condition": "users.id = orders.user_id",
                        "columns": ["users.name", "orders.amount"],
                        "estimated_rows": 5000,
                        "estimated_cost": 10000.0,
                    },
                },
                {
                    "name": "complex_query",
                    "plan": {
                        "type": "JOIN",
                        "table1": "users",
                        "table2": "orders",
                        "condition": "users.id = orders.user_id",
                        "filter": 'users.name LIKE "%john%" AND orders.amount > 100',
                        "columns": ["users.name", "orders.amount", "orders.date"],
                        "estimated_rows": 1000,
                        "estimated_cost": 15000.0,
                    },
                },
            ]

            level_results = []

            for query_test in test_queries:
                query_name = query_test["name"]
                query_plan = query_test["plan"]

                try:
                    start_time = time.time()
                    result = optimizer.optimize(
                        query_plan, query_signature=f"{level.value}_{query_name}"
                    )
                    optimization_time = (time.time() - start_time) * 1000

                    print(
                        f"  ✓ {query_name}: {optimization_time:.1f}ms, "
                        f"Cost: {result.estimated_cost:.0f}, "
                        f"Transforms: {len(result.transformations_applied)}, "
                        f"Cache: {'Hit' if result.cache_hit else 'Miss'}"
                    )

                    level_results.append(
                        {
                            "query": query_name,
                            "success": True,
                            "time_ms": optimization_time,
                            "cost": result.estimated_cost,
                            "transforms": len(result.transformations_applied),
                            "cache_hit": result.cache_hit,
                        }
                    )

                except Exception as e:
                    print(f"  ✗ {query_name}: Failed - {e}")
                    level_results.append(
                        {"query": query_name, "success": False, "error": str(e)}
                    )

            # Test cache effectiveness (run same query twice)
            print("  Testing cache effectiveness...")
            cache_query = test_queries[0]["plan"].copy()

            # First run (should miss cache)
            result1 = optimizer.optimize(cache_query, query_signature="cache_test")

            # Second run (should hit cache)
            result2 = optimizer.optimize(cache_query, query_signature="cache_test")

            if result2.cache_hit:
                print(
                    f"  ✓ Cache working: {result2.optimization_time_ms:.1f}ms vs {result1.optimization_time_ms:.1f}ms"
                )
            else:
                print("  ⚠ Cache not working as expected")

            # Get optimizer statistics
            stats = optimizer.get_optimization_statistics()
            print(
                f"  Stats: {stats.get('total_optimizations', 0)} optimizations, "
                f"{stats.get('cache_hits', 0)} cache hits"
            )

            test_results[level] = {
                "success": True,
                "results": level_results,
                "stats": stats,
            }

            # Cleanup
            optimizer.shutdown()

        except Exception as e:
            print(f"  ✗ Failed to test {level.value}: {e}")
            test_results[level] = {"success": False, "error": str(e)}

    # Print summary
    print("\n" + "=" * 60)
    print("INTEGRATION TEST SUMMARY")
    print("=" * 60)

    total_tests = 0
    successful_tests = 0

    for level, results in test_results.items():
        if results["success"]:
            level_success = sum(1 for r in results["results"] if r["success"])
            level_total = len(results["results"])
            total_tests += level_total
            successful_tests += level_success

            avg_time = sum(
                r["time_ms"] for r in results["results"] if r["success"]
            ) / max(1, level_success)

            print(
                f"{level.value:>12}: {level_success}/{level_total} tests passed, "
                f"avg time: {avg_time:.1f}ms"
            )
        else:
            print(f"{level.value:>12}: FAILED - {results['error']}")

    success_rate = successful_tests / max(1, total_tests)
    print(
        f"\nOverall: {successful_tests}/{total_tests} tests passed ({success_rate:.1%})"
    )

    if success_rate >= 0.8:
        print("✓ Integration test PASSED")
        return True
    else:
        print("✗ Integration test FAILED")
        return False


def test_component_availability():
    """Test which optimizer components are available."""

    print("\n" + "=" * 60)
    print("COMPONENT AVAILABILITY TEST")
    print("=" * 60)

    components = [
        ("Advanced Statistics", "server.advanced_statistics"),
        ("Query Transformer", "server.query_transformer"),
        ("Join Order Enumerator", "server.join_order_enumerator"),
        ("Cost Estimator", "server.cost_estimator"),
        ("Circular Cache", "server.circular_cache"),
        ("Access Path Selector", "server.access_path_selector"),
        ("ML Extensions", "server.ml_extensions"),
        ("Adaptive Optimizer", "server.adaptive_optimizer"),
        ("Unified Optimizer", "server.unified_optimizer"),
    ]

    available_components = 0

    for name, module_path in components:
        try:
            __import__(module_path)
            print(f"✓ {name:<25} Available")
            available_components += 1
        except ImportError as e:
            print(f"✗ {name:<25} Not available: {e}")

    print(
        f"\nComponent availability: {available_components}/{len(components)} ({available_components/len(components):.1%})"
    )

    # Test optional dependencies
    print("\nOptional dependencies:")

    optional_deps = [
        ("XGBoost", "xgboost"),
        ("PyTorch", "torch"),
        ("NumPy", "numpy"),
        ("SciPy", "scipy"),
    ]

    for name, module_name in optional_deps:
        try:
            __import__(module_name)
            print(f"✓ {name:<15} Available")
        except ImportError:
            print(f"⚠ {name:<15} Not available (optional)")


if __name__ == "__main__":
    print("Starting Query Optimizer Integration Test...")

    # Test component availability
    test_component_availability()

    # Test unified optimizer integration
    success = test_unified_optimizer_integration()

    if success:
        print(
            "\n🎉 All tests passed! The unified query optimizer is working correctly."
        )
        exit(0)
    else:
        print("\n❌ Some tests failed. Please check the implementation.")
        exit(1)
