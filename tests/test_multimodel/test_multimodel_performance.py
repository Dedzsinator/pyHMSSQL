#!/usr/bin/env python3
"""
Performance Tests for pyHMSSQL Multimodel Extension

This script provides comprehensive performance testing and benchmarking
for multimodel operations including document store, graph database,
and object-relational features.
"""

import sys
import os
import time
import json
import statistics
import threading
import concurrent.futures
import tempfile
import shutil
from datetime import datetime, timedelta
import pytest

# Add the server directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../server"))

import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class MultimodelPerformanceTester:
    """Performance testing suite for multimodel operations"""

    def __init__(self):
        self.results = {}
        self.execution_engine = None
        self.catalog_manager = None
        self.test_data_dir = None

    def setup(self):
        """Setup test environment"""
        try:
            # Ensure we're using the correct import path
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../server"))
            
            from catalog_manager import CatalogManager
            from index_manager import IndexManager
            from execution_engine import ExecutionEngine
            from planner import Planner

            # Create unique test data directory to avoid conflicts
            import uuid
            test_id = str(uuid.uuid4())[:8]
            self.test_data_dir = os.path.join(tempfile.gettempdir(), f"test_perf_data_{test_id}")
            
            self.catalog_manager = CatalogManager(data_dir=self.test_data_dir)
            index_manager = IndexManager(self.catalog_manager)
            planner = Planner(self.catalog_manager, index_manager)
            self.execution_engine = ExecutionEngine(
                self.catalog_manager, index_manager, planner
            )

            # Create test database
            self.execution_engine.execute(
                {"type": "CREATE_DATABASE", "database": "perf_test_db"}
            )

            self.execution_engine.execute(
                {"type": "USE_DATABASE", "database": "perf_test_db"}
            )

            return True

        except Exception as e:
            print(f"Setup failed: {e}")
            return False

    def benchmark_document_operations(self, num_documents=1000):
        """Benchmark document store operations"""
        print(f"\n🔥 Benchmarking document operations ({num_documents} documents)...")

        # Create collection
        start_time = time.time()
        result = self.execution_engine.execute(
            {
                "type": "CREATE_COLLECTION",
                "collection": "perf_docs",
                "schema": {
                    "properties": {
                        "title": {"type": "string"},
                        "content": {"type": "string"},
                        "metadata": {"type": "object"},
                        "tags": {"type": "array"},
                    }
                },
            }
        )
        collection_creation_time = time.time() - start_time

        if result.get("status") != "success":
            print(f"❌ Collection creation failed: {result}")
            return {}

        # Benchmark document inserts
        insert_times = []
        for i in range(num_documents):
            document = {
                "title": f"Document {i}",
                "content": f"This is the content of document number {i}. " * 10,
                "metadata": {
                    "author": f"Author_{i % 50}",
                    "version": i % 5 + 1,
                    "priority": i % 3,
                    "created": datetime.now().isoformat(),
                },
                "tags": [f"tag_{i % 10}", f"category_{i % 5}", f"type_{i % 3}"],
            }

            start_time = time.time()
            result = self.execution_engine.execute(
                {
                    "type": "INSERT_DOCUMENT",
                    "collection": "perf_docs",
                    "document": document,
                }
            )
            insert_time = time.time() - start_time
            insert_times.append(insert_time)

            if result.get("status") != "success":
                print(f"❌ Document insert {i} failed: {result}")
                break

        # Benchmark document queries
        query_times = []
        for i in range(min(100, num_documents // 10)):
            start_time = time.time()
            result = self.execution_engine.execute(
                {
                    "type": "QUERY_COLLECTION",
                    "collection": "perf_docs",
                    "query": {"metadata.author": f"Author_{i % 50}"},
                }
            )
            query_time = time.time() - start_time
            query_times.append(query_time)

            if result.get("status") != "success":
                print(f"❌ Query {i} failed: {result}")
                break

        # Benchmark JSONPath queries
        jsonpath_times = []
        for i in range(min(50, num_documents // 20)):
            start_time = time.time()
            result = self.execution_engine.execute(
                {
                    "type": "QUERY_COLLECTION",
                    "collection": "perf_docs",
                    "columns": ["$.title", "$.metadata.priority", "$.tags[0]"],
                    "where_conditions": {"$.metadata.version": 1},
                    "query_type": "jsonpath",
                }
            )
            jsonpath_time = time.time() - start_time
            jsonpath_times.append(jsonpath_time)

            if result.get("status") != "success":
                print(f"❌ JSONPath query {i} failed: {result}")
                break

        # Calculate statistics
        results = {
            "collection_creation_time": collection_creation_time,
            "insert_stats": {
                "total_inserts": len(insert_times),
                "avg_insert_time": statistics.mean(insert_times) if insert_times else 0,
                "min_insert_time": min(insert_times) if insert_times else 0,
                "max_insert_time": max(insert_times) if insert_times else 0,
                "median_insert_time": (
                    statistics.median(insert_times) if insert_times else 0
                ),
                "inserts_per_second": (
                    len(insert_times) / sum(insert_times) if insert_times else 0
                ),
            },
            "query_stats": {
                "total_queries": len(query_times),
                "avg_query_time": statistics.mean(query_times) if query_times else 0,
                "min_query_time": min(query_times) if query_times else 0,
                "max_query_time": max(query_times) if query_times else 0,
                "queries_per_second": (
                    len(query_times) / sum(query_times) if query_times else 0
                ),
            },
            "jsonpath_stats": {
                "total_queries": len(jsonpath_times),
                "avg_query_time": (
                    statistics.mean(jsonpath_times) if jsonpath_times else 0
                ),
                "min_query_time": min(jsonpath_times) if jsonpath_times else 0,
                "max_query_time": max(jsonpath_times) if jsonpath_times else 0,
                "queries_per_second": (
                    len(jsonpath_times) / sum(jsonpath_times) if jsonpath_times else 0
                ),
            },
        }

        self.results["document_operations"] = results
        return results

    def benchmark_graph_operations(self, num_vertices=500, num_edges=1000):
        """Benchmark graph database operations"""
        print(
            f"\n🔥 Benchmarking graph operations ({num_vertices} vertices, {num_edges} edges)..."
        )

        # Create graph schema
        start_time = time.time()
        result = self.execution_engine.execute(
            {
                "type": "CREATE_GRAPH_SCHEMA",
                "graph": "perf_graph",
                "vertex_types": [
                    {
                        "name": "Person",
                        "properties": {"name": "string", "age": "integer"},
                    },
                    {
                        "name": "Company",
                        "properties": {"name": "string", "industry": "string"},
                    },
                    {
                        "name": "Product",
                        "properties": {"name": "string", "price": "float"},
                    },
                ],
                "edge_types": [
                    {
                        "name": "WORKS_FOR",
                        "properties": {"since": "string", "position": "string"},
                    },
                    {
                        "name": "KNOWS",
                        "properties": {"since": "string", "relationship": "string"},
                    },
                    {
                        "name": "PURCHASES",
                        "properties": {"date": "string", "quantity": "integer"},
                    },
                ],
            }
        )
        schema_creation_time = time.time() - start_time

        if result.get("status") != "success":
            print(f"❌ Graph schema creation failed: {result}")
            return {}

        # Benchmark vertex creation
        vertex_times = []
        vertex_types = ["Person", "Company", "Product"]

        for i in range(num_vertices):
            vertex_type = vertex_types[i % len(vertex_types)]

            if vertex_type == "Person":
                vertex = {
                    "id": f"person_{i}",
                    "label": "Person",
                    "properties": {"name": f"Person_{i}", "age": 20 + (i % 50)},
                }
            elif vertex_type == "Company":
                vertex = {
                    "id": f"company_{i}",
                    "label": "Company",
                    "properties": {
                        "name": f"Company_{i}",
                        "industry": ["Tech", "Finance", "Healthcare", "Retail"][i % 4],
                    },
                }
            else:  # Product
                vertex = {
                    "id": f"product_{i}",
                    "label": "Product",
                    "properties": {
                        "name": f"Product_{i}",
                        "price": round(10.0 + (i % 1000) / 10.0, 2),
                    },
                }

            start_time = time.time()
            result = self.execution_engine.execute(
                {"type": "CREATE_VERTEX", "graph": "perf_graph", "vertex": vertex}
            )
            vertex_time = time.time() - start_time
            vertex_times.append(vertex_time)

            if result.get("status") != "success":
                print(f"❌ Vertex creation {i} failed: {result}")
                break

        # Benchmark edge creation
        edge_times = []
        edge_types = ["WORKS_FOR", "KNOWS", "PURCHASES"]

        for i in range(num_edges):
            edge_type = edge_types[i % len(edge_types)]

            if edge_type == "WORKS_FOR":
                from_vertex = f"person_{i % (num_vertices // 3)}"
                to_vertex = f"company_{i % (num_vertices // 3)}"
            elif edge_type == "KNOWS":
                from_vertex = f"person_{i % (num_vertices // 3)}"
                to_vertex = f"person_{(i + 1) % (num_vertices // 3)}"
            else:  # PURCHASES
                from_vertex = f"person_{i % (num_vertices // 3)}"
                to_vertex = f"product_{i % (num_vertices // 3)}"

            edge = {
                "id": f"edge_{i}",
                "from_vertex": from_vertex,
                "to_vertex": to_vertex,
                "label": edge_type,
                "properties": {"created": datetime.now().isoformat(), "weight": i % 10},
            }

            start_time = time.time()
            result = self.execution_engine.execute(
                {"type": "CREATE_EDGE", "graph": "perf_graph", "edge": edge}
            )
            edge_time = time.time() - start_time
            edge_times.append(edge_time)

            if result.get("status") != "success":
                print(f"❌ Edge creation {i} failed: {result}")
                break

        # Benchmark graph traversals
        traversal_times = []
        for i in range(min(50, num_vertices // 10)):
            start_vertex = f"person_{i % (num_vertices // 3)}"

            start_time = time.time()
            result = self.execution_engine.execute(
                {
                    "type": "GRAPH_TRAVERSAL",
                    "graph": "perf_graph",
                    "start_vertex": start_vertex,
                    "max_depth": 3,
                    "edge_filters": ["KNOWS", "WORKS_FOR"],
                }
            )
            traversal_time = time.time() - start_time
            traversal_times.append(traversal_time)

            if result.get("status") != "success":
                print(f"❌ Traversal {i} failed: {result}")
                break

        # Calculate statistics
        results = {
            "schema_creation_time": schema_creation_time,
            "vertex_stats": {
                "total_vertices": len(vertex_times),
                "avg_creation_time": (
                    statistics.mean(vertex_times) if vertex_times else 0
                ),
                "min_creation_time": min(vertex_times) if vertex_times else 0,
                "max_creation_time": max(vertex_times) if vertex_times else 0,
                "vertices_per_second": (
                    len(vertex_times) / sum(vertex_times) if vertex_times else 0
                ),
            },
            "edge_stats": {
                "total_edges": len(edge_times),
                "avg_creation_time": statistics.mean(edge_times) if edge_times else 0,
                "min_creation_time": min(edge_times) if edge_times else 0,
                "max_creation_time": max(edge_times) if edge_times else 0,
                "edges_per_second": (
                    len(edge_times) / sum(edge_times) if edge_times else 0
                ),
            },
            "traversal_stats": {
                "total_traversals": len(traversal_times),
                "avg_traversal_time": (
                    statistics.mean(traversal_times) if traversal_times else 0
                ),
                "min_traversal_time": min(traversal_times) if traversal_times else 0,
                "max_traversal_time": max(traversal_times) if traversal_times else 0,
                "traversals_per_second": (
                    len(traversal_times) / sum(traversal_times)
                    if traversal_times
                    else 0
                ),
            },
        }

        self.results["graph_operations"] = results
        return results

    def benchmark_type_operations(self, num_types=50, num_instances=200):
        """Benchmark object-relational type operations"""
        print(
            f"\n🔥 Benchmarking type operations ({num_types} types, {num_instances} instances)..."
        )

        # Benchmark type creation
        type_creation_times = []
        for i in range(num_types):
            start_time = time.time()
            result = self.execution_engine.execute(
                {
                    "type": "CREATE_TYPE",
                    "type_name": f"CustomType_{i}",
                    "attributes": [
                        {"name": "id", "type": "integer"},
                        {"name": "name", "type": "string"},
                        {"name": f"field_{i}", "type": "string"},
                        {"name": "value", "type": "float"},
                    ],
                }
            )
            type_time = time.time() - start_time
            type_creation_times.append(type_time)

            if result.get("status") != "success":
                print(f"❌ Type creation {i} failed: {result}")
                break

        # Benchmark instance creation
        instance_times = []
        for i in range(num_instances):
            type_index = i % num_types

            start_time = time.time()
            result = self.execution_engine.execute(
                {
                    "type": "CREATE_INSTANCE",
                    "type_name": f"CustomType_{type_index}",
                    "values": {
                        "id": i,
                        "name": f"Instance_{i}",
                        f"field_{type_index}": f"Value_{i}",
                        "value": round(i * 1.5, 2),
                    },
                }
            )
            instance_time = time.time() - start_time
            instance_times.append(instance_time)

            if result.get("status") != "success":
                print(f"❌ Instance creation {i} failed: {result}")
                break

        # Benchmark type queries
        query_times = []
        for i in range(min(50, num_types)):
            start_time = time.time()
            result = self.execution_engine.execute(
                {
                    "type": "QUERY_TYPE",
                    "type_name": f"CustomType_{i}",
                    "conditions": {"value": f"> {i * 10}"},
                }
            )
            query_time = time.time() - start_time
            query_times.append(query_time)

            if result.get("status") != "success":
                print(f"❌ Type query {i} failed: {result}")
                break

        # Calculate statistics
        results = {
            "type_creation_stats": {
                "total_types": len(type_creation_times),
                "avg_creation_time": (
                    statistics.mean(type_creation_times) if type_creation_times else 0
                ),
                "min_creation_time": (
                    min(type_creation_times) if type_creation_times else 0
                ),
                "max_creation_time": (
                    max(type_creation_times) if type_creation_times else 0
                ),
                "types_per_second": (
                    len(type_creation_times) / sum(type_creation_times)
                    if type_creation_times
                    else 0
                ),
            },
            "instance_stats": {
                "total_instances": len(instance_times),
                "avg_creation_time": (
                    statistics.mean(instance_times) if instance_times else 0
                ),
                "min_creation_time": min(instance_times) if instance_times else 0,
                "max_creation_time": max(instance_times) if instance_times else 0,
                "instances_per_second": (
                    len(instance_times) / sum(instance_times) if instance_times else 0
                ),
            },
            "query_stats": {
                "total_queries": len(query_times),
                "avg_query_time": statistics.mean(query_times) if query_times else 0,
                "min_query_time": min(query_times) if query_times else 0,
                "max_query_time": max(query_times) if query_times else 0,
                "queries_per_second": (
                    len(query_times) / sum(query_times) if query_times else 0
                ),
            },
        }

        self.results["type_operations"] = results
        return results

    def benchmark_concurrent_operations(self, num_threads=5, operations_per_thread=100):
        """Benchmark concurrent operations"""
        print(
            f"\n🔥 Benchmarking concurrent operations ({num_threads} threads, {operations_per_thread} ops/thread)..."
        )

        def concurrent_document_inserts(thread_id, num_ops):
            """Perform concurrent document inserts"""
            times = []
            for i in range(num_ops):
                document = {
                    "thread_id": thread_id,
                    "doc_id": i,
                    "content": f"Thread {thread_id} document {i}",
                    "timestamp": datetime.now().isoformat(),
                }

                start_time = time.time()
                result = self.execution_engine.execute(
                    {
                        "type": "INSERT_DOCUMENT",
                        "collection": "perf_docs",
                        "document": document,
                    }
                )
                operation_time = time.time() - start_time
                times.append(operation_time)

                if result.get("status") != "success":
                    break

            return times

        # Run concurrent operations
        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(concurrent_document_inserts, i, operations_per_thread)
                for i in range(num_threads)
            ]

            all_times = []
            for future in concurrent.futures.as_completed(futures):
                thread_times = future.result()
                all_times.extend(thread_times)

        total_time = time.time() - start_time

        # Calculate statistics
        results = {
            "total_operations": len(all_times),
            "total_time": total_time,
            "avg_operation_time": statistics.mean(all_times) if all_times else 0,
            "min_operation_time": min(all_times) if all_times else 0,
            "max_operation_time": max(all_times) if all_times else 0,
            "operations_per_second": (
                len(all_times) / total_time if total_time > 0 else 0
            ),
            "concurrent_throughput": (
                len(all_times) / total_time if total_time > 0 else 0
            ),
        }

        self.results["concurrent_operations"] = results
        return results

    def run_full_benchmark(self):
        """Run complete performance benchmark suite"""
        print("🚀 Starting pyHMSSQL Multimodel Performance Benchmark Suite")
        print("=" * 60)

        if not self.setup():
            print("❌ Performance test setup failed!")
            return False

        start_time = time.time()

        # Run all benchmarks
        try:
            self.benchmark_document_operations(num_documents=500)
            self.benchmark_graph_operations(num_vertices=200, num_edges=400)
            self.benchmark_type_operations(num_types=20, num_instances=100)
            self.benchmark_concurrent_operations(
                num_threads=3, operations_per_thread=50
            )

        except Exception as e:
            print(f"❌ Benchmark failed: {e}")
            return False

        total_time = time.time() - start_time

        # Print summary
        print("\n" + "=" * 60)
        print("📊 PERFORMANCE BENCHMARK SUMMARY")
        print("=" * 60)

        for operation_type, stats in self.results.items():
            print(f"\n{operation_type.upper()}:")
            if isinstance(stats, dict):
                for key, value in stats.items():
                    if isinstance(value, dict):
                        print(f"  {key}:")
                        for subkey, subvalue in value.items():
                            if isinstance(subvalue, float):
                                print(f"    {subkey}: {subvalue:.6f}")
                            else:
                                print(f"    {subkey}: {subvalue}")
                    else:
                        if isinstance(value, float):
                            print(f"  {key}: {value:.6f}")
                        else:
                            print(f"  {key}: {value}")

        print(f"\n⏱️ Total benchmark time: {total_time:.2f} seconds")
        print("✅ Performance benchmark completed successfully!")

        return True

    def cleanup(self):
        """Clean up test environment"""
        try:
            if self.catalog_manager and hasattr(self.catalog_manager, "close"):
                self.catalog_manager.close()

            # Clean up test files
            import shutil
            
            # Clean up the specific test data directory
            if self.test_data_dir and os.path.exists(self.test_data_dir):
                shutil.rmtree(self.test_data_dir)

            # Also clean up any legacy test files
            if os.path.exists("test_perf_db"):
                shutil.rmtree("test_perf_db")
                
            # Clean up any generic test directories
            test_data_dir = os.path.join(tempfile.gettempdir(), "test_perf_data")
            if os.path.exists(test_data_dir):
                shutil.rmtree(test_data_dir)

        except Exception as e:
            print(f"Cleanup warning: {e}")


# Pytest Integration
@pytest.fixture(scope="module")
def performance_tester():
    """Pytest fixture for performance tester"""
    tester = MultimodelPerformanceTester()
    yield tester
    tester.cleanup()


@pytest.mark.performance
@pytest.mark.slow
def test_document_operations_performance(performance_tester):
    """Test document operations performance"""
    assert performance_tester.setup()
    results = performance_tester.benchmark_document_operations(num_documents=100)

    assert results is not None
    assert "insert_stats" in results
    assert "query_stats" in results
    assert results["insert_stats"]["total_inserts"] > 0
    assert results["insert_stats"]["avg_insert_time"] < 1.0  # Should be fast


@pytest.mark.performance
@pytest.mark.slow
def test_graph_operations_performance(performance_tester):
    """Test graph operations performance"""
    assert performance_tester.setup()
    results = performance_tester.benchmark_graph_operations(
        num_vertices=50, num_edges=100
    )

    assert results is not None
    assert "vertex_stats" in results
    assert "edge_stats" in results
    assert results["vertex_stats"]["total_vertices"] > 0
    assert results["edge_stats"]["total_edges"] > 0


@pytest.mark.performance
@pytest.mark.slow
def test_type_operations_performance(performance_tester):
    """Test object-relational type operations performance"""
    assert performance_tester.setup()
    results = performance_tester.benchmark_type_operations(
        num_types=10, num_instances=50
    )

    assert results is not None
    assert "type_creation_stats" in results
    assert "instance_stats" in results
    assert results["type_creation_stats"]["total_types"] > 0


@pytest.mark.performance
@pytest.mark.slow
def test_concurrent_operations_performance(performance_tester):
    """Test concurrent operations performance"""
    assert performance_tester.setup()
    results = performance_tester.benchmark_concurrent_operations(
        num_threads=2, operations_per_thread=25
    )

    assert results is not None
    assert "total_operations" in results
    assert results["total_operations"] > 0
    assert results["operations_per_second"] > 0


@pytest.mark.performance
@pytest.mark.slow
def test_full_benchmark_suite(performance_tester):
    """Test the complete performance benchmark suite"""
    success = performance_tester.run_full_benchmark()
    assert success is True
    assert len(performance_tester.results) > 0


if __name__ == "__main__":
    # Run as standalone script
    tester = MultimodelPerformanceTester()
    try:
        success = tester.run_full_benchmark()
        exit(0 if success else 1)
    finally:
        tester.cleanup()
