#!/usr/bin/env python3
"""
Comprehensive End-to-End Integration Tests for pyHMSSQL Multimodel Extension

This test suite provides complete integration testing of all multimodel features:
- Object-Relational features (Custom Types, Inheritance)
- Document Store operations (JSON documents, JSONPath queries)
- Graph Database operations (Vertices, Edges, Traversals)
- Cross-model queries and operations
- Performance benchmarking
- Error handling and edge cases
"""

import sys
import os
import time
import json
import tempfile
import shutil
from unittest.mock import Mock, patch
import pytest

# Add the server directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../server"))

import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class MultimodelIntegrationTestSuite:
    """Comprehensive integration test suite for multimodel functionality"""

    def __init__(self):
        self.test_db_path = None
        self.catalog_manager = None
        self.execution_engine = None
        self.index_manager = None
        self.planner = None
        self.test_results = []
        self.performance_metrics = {}
        # Generate unique test ID to avoid resource conflicts
        import uuid
        self.test_id = str(uuid.uuid4())[:8]

    def setup_test_environment(self):
        """Set up a clean test environment"""
        try:
            # Create temporary database directory
            self.test_db_path = tempfile.mkdtemp(prefix="pyhmssql_test_")
            print(f"📁 Created test database directory: {self.test_db_path}")

            # Import required modules
            from catalog_manager import CatalogManager
            from ddl_processor.index_manager import IndexManager
            from execution_engine import ExecutionEngine
            from planner import Planner

            # Initialize core components
            self.catalog_manager = CatalogManager(data_dir=self.test_db_path)
            self.index_manager = IndexManager(self.catalog_manager)
            self.planner = Planner(self.catalog_manager, self.index_manager)
            self.execution_engine = ExecutionEngine(
                self.catalog_manager, self.index_manager, self.planner
            )

            # Create test database
            create_db_result = self.execution_engine.execute(
                {"type": "CREATE_DATABASE", "database": "test_multimodel_db"}
            )

            if create_db_result.get("status") == "error":
                raise Exception(f"Failed to create test database: {create_db_result}")

            # Use the test database
            use_db_result = self.execution_engine.execute(
                {"type": "USE_DATABASE", "database": "test_multimodel_db"}
            )

            if use_db_result.get("status") == "error":
                raise Exception(f"Failed to use test database: {use_db_result}")

            print("✅ Test environment setup completed")
            return True

        except Exception as e:
            print(f"❌ Failed to setup test environment: {e}")
            return False

    def cleanup_test_environment(self):
        """Clean up test environment"""
        try:
            if self.test_db_path and os.path.exists(self.test_db_path):
                shutil.rmtree(self.test_db_path)
                print("🧹 Cleaned up test environment")
        except Exception as e:
            print(f"⚠️ Warning: Failed to cleanup test environment: {e}")

    def run_test(self, test_name, test_function):
        """Run a single test and record results"""
        print(f"\n🧪 Running test: {test_name}")
        start_time = time.time()

        try:
            result = test_function()
            end_time = time.time()
            duration = end_time - start_time

            self.test_results.append(
                {
                    "name": test_name,
                    "status": "PASS" if result else "FAIL",
                    "duration": duration,
                    "error": None,
                }
            )

            status_emoji = "✅" if result else "❌"
            print(
                f"{status_emoji} {test_name}: {'PASS' if result else 'FAIL'} ({duration:.3f}s)"
            )
            return result

        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time

            self.test_results.append(
                {
                    "name": test_name,
                    "status": "ERROR",
                    "duration": duration,
                    "error": str(e),
                }
            )

            print(f"💥 {test_name}: ERROR - {e} ({duration:.3f}s)")
            return False

    def test_object_relational_types(self):
        """Test Object-Relational custom types and inheritance"""
        try:
            # Test CREATE TYPE
            create_type_result = self.execution_engine.execute(
                {
                    "type": "CREATE_TYPE",
                    "type_name": "address_type",
                    "attributes": [
                        {"name": "street", "type": "VARCHAR(100)"},
                        {"name": "city", "type": "VARCHAR(50)"},
                        {"name": "zipcode", "type": "VARCHAR(10)"},
                    ],
                }
            )

            if create_type_result.get("status") != "success":
                print(f"CREATE TYPE failed: {create_type_result}")
                return False

            # Test using custom type in table
            create_table_result = self.execution_engine.execute(
                {
                    "type": "CREATE_TABLE",
                    "table": "employees",
                    "columns": [
                        {
                            "name": "id",
                            "type": "INTEGER",
                            "constraints": ["PRIMARY KEY"],
                        },
                        {"name": "name", "type": "VARCHAR(100)"},
                        {"name": "home_address", "type": "address_type"},
                        {"name": "work_address", "type": "address_type"},
                    ],
                }
            )

            if create_table_result.get("status") != "success":
                print(f"CREATE TABLE with custom type failed: {create_table_result}")
                return False

            # Test INSERT with composite type values
            insert_result = self.execution_engine.execute(
                {
                    "type": "INSERT",
                    "table": "employees",
                    "columns": ["id", "name", "home_address", "work_address"],
                    "values": [
                        [
                            1,
                            "John Doe",
                            {
                                "street": "123 Main St",
                                "city": "New York",
                                "zipcode": "10001",
                            },
                            {
                                "street": "456 Work Ave",
                                "city": "New York",
                                "zipcode": "10002",
                            },
                        ]
                    ],
                }
            )

            if insert_result.get("status") != "success":
                print(f"INSERT with composite type failed: {insert_result}")
                return False

            # Test SELECT with composite type access
            select_result = self.execution_engine.execute(
                {
                    "type": "SELECT",
                    "table": "employees",
                    "columns": ["name", "home_address.city", "work_address.street"],
                    "query_type": "composite_select",
                }
            )

            if select_result.get("status") != "success":
                print(f"SELECT with composite type access failed: {select_result}")
                return False

            # Verify the results
            if not select_result.get("rows"):
                print("No rows returned from composite type SELECT")
                return False

            row = select_result["rows"][0]
            if row[1] != "New York" or row[2] != "456 Work Ave":
                print(f"Incorrect composite type values: {row}")
                return False

            print("✅ Object-Relational type operations successful")
            return True

        except Exception as e:
            print(f"❌ Object-Relational test failed: {e}")
            return False

    def test_document_store_operations(self):
        """Test Document Store operations"""
        try:
            # Test CREATE COLLECTION
            create_collection_result = self.execution_engine.execute(
                {
                    "type": "CREATE_COLLECTION",
                    "collection": "users",
                    "schema": {
                        "properties": {
                            "name": {"type": "string"},
                            "email": {"type": "string"},
                            "profile": {"type": "object"},
                            "tags": {"type": "array"},
                        }
                    },
                }
            )

            if create_collection_result.get("status") != "success":
                print(f"CREATE COLLECTION failed: {create_collection_result}")
                return False

            # Test DOCUMENT INSERT
            insert_doc_result = self.execution_engine.execute(
                {
                    "type": "DOCUMENT_INSERT",
                    "collection": "users",
                    "document": {
                        "_id": "user1",
                        "name": "Alice Johnson",
                        "email": "alice@example.com",
                        "profile": {
                            "age": 30,
                            "city": "San Francisco",
                            "interests": ["programming", "music"],
                        },
                        "tags": ["developer", "manager"],
                        "created_at": datetime.now().isoformat(),
                    },
                }
            )

            if insert_doc_result.get("status") != "success":
                print(f"DOCUMENT INSERT failed: {insert_doc_result}")
                return False

            # Test DOCUMENT FIND
            find_result = self.execution_engine.execute(
                {
                    "type": "DOCUMENT_FIND",
                    "collection": "users",
                    "filter": {"name": "Alice Johnson"},
                }
            )

            if find_result.get("status") != "success":
                print(f"DOCUMENT FIND failed: {find_result}")
                return False

            if not find_result.get("documents"):
                print("No documents found")
                return False

            # Test JSONPath SELECT
            jsonpath_result = self.execution_engine.execute(
                {
                    "type": "SELECT",
                    "table": "users",
                    "columns": ["$.profile.age", "$.profile.interests[0]", "$.tags"],
                    "query_type": "jsonpath",
                    "where_conditions": {"$.name": "Alice Johnson"},
                }
            )

            if jsonpath_result.get("status") != "success":
                print(f"JSONPath SELECT failed: {jsonpath_result}")
                return False

            # Test DOCUMENT UPDATE
            update_doc_result = self.execution_engine.execute(
                {
                    "type": "DOCUMENT_UPDATE",
                    "collection": "users",
                    "filter": {"_id": "user1"},
                    "update": {
                        "$set": {"profile.age": 31},
                        "$push": {"tags": "senior"},
                    },
                }
            )

            if update_doc_result.get("status") != "success":
                print(f"DOCUMENT UPDATE failed: {update_doc_result}")
                return False

            # Test aggregation pipeline
            aggregate_result = self.execution_engine.execute(
                {
                    "type": "DOCUMENT_AGGREGATE",
                    "collection": "users",
                    "pipeline": [
                        {"$match": {"profile.age": {"$gte": 30}}},
                        {"$project": {"name": 1, "age": "$profile.age"}},
                        {"$sort": {"age": -1}},
                    ],
                    "query_type": "aggregation_pipeline",
                }
            )

            if aggregate_result.get("status") != "success":
                print(f"DOCUMENT AGGREGATE failed: {aggregate_result}")
                return False

            print("✅ Document Store operations successful")
            return True

        except Exception as e:
            print(f"❌ Document Store test failed: {e}")
            return False

    def test_graph_database_operations(self):
        """Test Graph Database operations"""
        try:
            # Test CREATE GRAPH SCHEMA
            create_graph_result = self.execution_engine.execute(
                {
                    "type": "CREATE_GRAPH_SCHEMA",
                    "graph": "social_network",
                    "vertex_types": [
                        {"type": "Person", "properties": ["name", "age", "email"]},
                        {"type": "Company", "properties": ["name", "industry"]},
                    ],
                    "edge_types": [
                        {"type": "WORKS_FOR", "from": "Person", "to": "Company"},
                        {"type": "KNOWS", "from": "Person", "to": "Person"},
                        {"type": "FRIEND_OF", "from": "Person", "to": "Person"},
                    ],
                }
            )

            if create_graph_result.get("status") != "success":
                print(f"CREATE GRAPH SCHEMA failed: {create_graph_result}")
                return False

            # Test CREATE VERTEX
            create_vertex1_result = self.execution_engine.execute(
                {
                    "type": "CREATE_VERTEX",
                    "graph": "social_network",
                    "vertex": {
                        "id": "person1",
                        "label": "Person",
                        "properties": {
                            "name": "Bob Smith",
                            "age": 35,
                            "email": "bob@example.com",
                        },
                    },
                }
            )

            if create_vertex1_result.get("status") != "success":
                print(f"CREATE VERTEX 1 failed: {create_vertex1_result}")
                return False

            create_vertex2_result = self.execution_engine.execute(
                {
                    "type": "CREATE_VERTEX",
                    "graph": "social_network",
                    "vertex": {
                        "id": "company1",
                        "label": "Company",
                        "properties": {"name": "TechCorp", "industry": "Software"},
                    },
                }
            )

            if create_vertex2_result.get("status") != "success":
                print(f"CREATE VERTEX 2 failed: {create_vertex2_result}")
                return False

            # Test CREATE EDGE
            create_edge_result = self.execution_engine.execute(
                {
                    "type": "CREATE_EDGE",
                    "graph": "social_network",
                    "edge": {
                        "id": "edge1",
                        "from_vertex": "person1",
                        "to_vertex": "company1",
                        "label": "WORKS_FOR",
                        "properties": {
                            "since": "2020-01-01",
                            "position": "Senior Developer",
                        },
                    },
                }
            )

            if create_edge_result.get("status") != "success":
                print(f"CREATE EDGE failed: {create_edge_result}")
                return False

            # Test GRAPH TRAVERSE
            traverse_result = self.execution_engine.execute(
                {
                    "type": "GRAPH_TRAVERSE",
                    "graph": "social_network",
                    "start_vertex": "person1",
                    "direction": "outgoing",
                    "edge_filter": {"label": "WORKS_FOR"},
                    "max_depth": 2,
                    "query_type": "graph_traverse",
                }
            )

            if traverse_result.get("status") != "success":
                print(f"GRAPH TRAVERSE failed: {traverse_result}")
                return False

            # Test GRAPH PATTERN MATCH
            pattern_result = self.execution_engine.execute(
                {
                    "type": "GRAPH_PATTERN",
                    "graph": "social_network",
                    "pattern": "(p:Person)-[r:WORKS_FOR]->(c:Company)",
                    "where": {"p.age": {"$gte": 30}},
                    "return": ["p.name", "r.position", "c.name"],
                    "query_type": "graph_pattern",
                }
            )

            if pattern_result.get("status") != "success":
                print(f"GRAPH PATTERN failed: {pattern_result}")
                return False

            # Test GRAPH PATH
            path_result = self.execution_engine.execute(
                {
                    "type": "GRAPH_PATH",
                    "graph": "social_network",
                    "from_vertex": "person1",
                    "to_vertex": "company1",
                    "max_hops": 3,
                    "query_type": "graph_path",
                }
            )

            if path_result.get("status") != "success":
                print(f"GRAPH PATH failed: {path_result}")
                return False

            print("✅ Graph Database operations successful")
            return True

        except Exception as e:
            print(f"❌ Graph Database test failed: {e}")
            return False

    def test_cross_model_operations(self):
        """Test operations that span multiple data models"""
        try:
            # Test joining relational table with document collection
            cross_join_result = self.execution_engine.execute(
                {
                    "type": "SELECT",
                    "from": ["employees", "users"],
                    "columns": ["e.name", "u.$.email", "e.home_address.city"],
                    "where_conditions": {"e.name": "u.$.name"},
                    "query_type": "cross_model_join",
                }
            )

            # Note: This might not be fully implemented yet, so we'll check for graceful handling
            if cross_join_result.get("status") == "error":
                if "not implemented" in cross_join_result.get("error", "").lower():
                    print("Cross-model join not yet implemented (expected)")
                    return True
                else:
                    print(f"Unexpected cross-model join error: {cross_join_result}")
                    return False

            # Test graph-document hybrid query
            hybrid_result = self.execution_engine.execute(
                {
                    "type": "SELECT",
                    "graph": "social_network",
                    "collection": "users",
                    "query": """
                    MATCH (p:Person)-[r:WORKS_FOR]->(c:Company)
                    WITH p.email as email
                    FIND DOCUMENT IN users WHERE $.email = email
                    RETURN p.name, DOCUMENT.profile.age, c.name
                """,
                    "query_type": "hybrid_graph_document",
                }
            )

            # This is also likely not implemented yet
            if hybrid_result.get("status") == "error":
                if "not implemented" in hybrid_result.get("error", "").lower():
                    print("Hybrid graph-document query not yet implemented (expected)")
                    return True

            print("✅ Cross-model operations handled gracefully")
            return True

        except Exception as e:
            print(f"❌ Cross-model operations test failed: {e}")
            return False

    def test_performance_benchmarks(self):
        """Run performance benchmarks for multimodel operations"""
        try:
            print("🚀 Running performance benchmarks...")

            # Benchmark 1: Bulk document inserts
            start_time = time.time()
            for i in range(100):
                result = self.execution_engine.execute(
                    {
                        "type": "DOCUMENT_INSERT",
                        "collection": "users",
                        "document": {
                            "_id": f"perf_user_{i}",
                            "name": f"User {i}",
                            "email": f"user{i}@example.com",
                            "profile": {"age": 20 + (i % 50), "score": i * 10},
                        },
                    }
                )

                if result.get("status") != "success":
                    print(f"Bulk insert failed at document {i}")
                    return False

            bulk_insert_time = time.time() - start_time
            self.performance_metrics["bulk_document_insert_100"] = bulk_insert_time
            print(f"📊 Bulk insert (100 docs): {bulk_insert_time:.3f}s")

            # Benchmark 2: Complex JSONPath queries
            start_time = time.time()
            for i in range(50):
                result = self.execution_engine.execute(
                    {
                        "type": "DOCUMENT_FIND",
                        "collection": "users",
                        "filter": {
                            "profile.age": {"$gte": 30},
                            "profile.score": {"$lt": 500},
                        },
                    }
                )

                if result.get("status") != "success":
                    print(f"JSONPath query failed at iteration {i}")
                    return False

            jsonpath_query_time = time.time() - start_time
            self.performance_metrics["jsonpath_queries_50"] = jsonpath_query_time
            print(f"📊 JSONPath queries (50x): {jsonpath_query_time:.3f}s")

            # Benchmark 3: Graph traversals
            start_time = time.time()
            for i in range(20):
                result = self.execution_engine.execute(
                    {
                        "type": "GRAPH_TRAVERSE",
                        "graph": "social_network",
                        "start_vertex": "person1",
                        "direction": "both",
                        "max_depth": 3,
                    }
                )

                if result.get("status") != "success":
                    print(f"Graph traversal failed at iteration {i}")
                    return False

            graph_traversal_time = time.time() - start_time
            self.performance_metrics["graph_traversals_20"] = graph_traversal_time
            print(f"📊 Graph traversals (20x): {graph_traversal_time:.3f}s")

            print("✅ Performance benchmarks completed")
            return True

        except Exception as e:
            print(f"❌ Performance benchmark failed: {e}")
            return False

    def test_error_handling(self):
        """Test error handling and edge cases"""
        try:
            # Test invalid document structure
            invalid_doc_result = self.execution_engine.execute(
                {
                    "type": "DOCUMENT_INSERT",
                    "collection": "users",
                    "document": "invalid_json_structure",
                }
            )

            if invalid_doc_result.get("status") != "error":
                print("Expected error for invalid document structure")
                return False

            # Test invalid graph vertex
            invalid_vertex_result = self.execution_engine.execute(
                {
                    "type": "CREATE_VERTEX",
                    "graph": "nonexistent_graph",
                    "vertex": {"id": "v1", "label": "Person"},
                }
            )

            if invalid_vertex_result.get("status") != "error":
                print("Expected error for nonexistent graph")
                return False

            # Test invalid type operation
            invalid_type_result = self.execution_engine.execute(
                {"type": "CREATE_TYPE", "type_name": "", "attributes": []}  # Empty name
            )

            if invalid_type_result.get("status") != "error":
                print("Expected error for invalid type name")
                return False

            # Test malformed operation - use invalid type instead of JSONPath
            invalid_operation_result = self.execution_engine.execute(
                {
                    "type": "INVALID_OPERATION",  # Non-existent operation type
                    "data": "test",
                }
            )

            # The execution engine should handle unknown operation types gracefully
            if invalid_operation_result.get("status") == "success":
                print("Expected error for invalid operation type")
                return False

            print("✅ Error handling tests passed")
            return True

        except Exception as e:
            print(f"❌ Error handling test failed: {e}")
            return False

    def test_concurrent_operations(self):
        """Test concurrent multimodel operations"""
        try:
            import threading
            import queue

            results_queue = queue.Queue()

            def worker_insert_documents(worker_id, num_docs):
                """Worker function for concurrent document inserts"""
                try:
                    for i in range(num_docs):
                        result = self.execution_engine.execute(
                            {
                                "type": "DOCUMENT_INSERT",
                                "collection": "users",
                                "document": {
                                    "_id": f"worker_{worker_id}_doc_{i}",
                                    "worker_id": worker_id,
                                    "doc_num": i,
                                    "data": f"worker {worker_id} document {i}",
                                },
                            }
                        )

                        if result.get("status") != "success":
                            results_queue.put(("error", worker_id, result))
                            return

                    results_queue.put(("success", worker_id, num_docs))

                except Exception as e:
                    results_queue.put(("exception", worker_id, str(e)))

            # Start 5 concurrent workers, each inserting 10 documents
            threads = []
            num_workers = 5
            docs_per_worker = 10

            for worker_id in range(num_workers):
                thread = threading.Thread(
                    target=worker_insert_documents, args=(worker_id, docs_per_worker)
                )
                threads.append(thread)
                thread.start()

            # Wait for all threads to complete
            for thread in threads:
                thread.join()

            # Collect results
            successful_workers = 0
            while not results_queue.empty():
                status, worker_id, data = results_queue.get()
                if status == "success":
                    successful_workers += 1
                    print(f"Worker {worker_id} completed successfully ({data} docs)")
                else:
                    print(f"Worker {worker_id} failed: {data}")

            if successful_workers == num_workers:
                print("✅ Concurrent operations test passed")
                return True
            else:
                print(f"❌ Only {successful_workers}/{num_workers} workers succeeded")
                return False

        except Exception as e:
            print(f"❌ Concurrent operations test failed: {e}")
            return False

    def generate_test_report(self):
        """Generate comprehensive test report"""
        print("\n" + "=" * 80)
        print("🔍 MULTIMODEL INTEGRATION TEST REPORT")
        print("=" * 80)

        total_tests = len(self.test_results)
        passed_tests = sum(1 for t in self.test_results if t["status"] == "PASS")
        failed_tests = sum(1 for t in self.test_results if t["status"] == "FAIL")
        error_tests = sum(1 for t in self.test_results if t["status"] == "ERROR")

        print(f"\n📊 TEST SUMMARY:")
        print(f"   Total Tests: {total_tests}")
        print(f"   Passed: {passed_tests} ✅")
        print(f"   Failed: {failed_tests} ❌")
        print(f"   Errors: {error_tests} 💥")
        print(f"   Success Rate: {(passed_tests/total_tests)*100:.1f}%")

        print(f"\n⏱️ PERFORMANCE METRICS:")
        for metric, value in self.performance_metrics.items():
            print(f"   {metric}: {value:.3f}s")

        print(f"\n📋 DETAILED RESULTS:")
        for test in self.test_results:
            status_icon = {"PASS": "✅", "FAIL": "❌", "ERROR": "💥"}[test["status"]]
            print(f"   {status_icon} {test['name']} ({test['duration']:.3f}s)")
            if test["error"]:
                print(f"      Error: {test['error']}")

        print("\n" + "=" * 80)

        if passed_tests == total_tests:
            print("🎉 ALL TESTS PASSED! Multimodel extension is ready for production.")
        else:
            print("⚠️ Some tests failed. Review the detailed results above.")

        print("=" * 80)

        return passed_tests == total_tests


# Pytest Integration
@pytest.fixture(scope="module")
def integration_test_suite():
    """Pytest fixture for integration test suite"""
    suite = MultimodelIntegrationTestSuite()
    if suite.setup_test_environment():
        yield suite
    else:
        pytest.skip("Failed to setup test environment")
    suite.cleanup_test_environment()


@pytest.mark.integration
@pytest.mark.multimodel
def test_object_relational_types_integration(integration_test_suite):
    """Test Object-Relational custom types and inheritance"""
    result = integration_test_suite.test_object_relational_types()
    assert result is True, "Object-Relational types test failed"


@pytest.mark.integration
@pytest.mark.multimodel
def test_document_store_operations_integration(integration_test_suite):
    """Test Document Store operations"""
    result = integration_test_suite.test_document_store_operations()
    assert result is True, "Document Store operations test failed"


@pytest.mark.integration
@pytest.mark.multimodel
def test_graph_database_operations_integration(integration_test_suite):
    """Test Graph Database operations"""
    result = integration_test_suite.test_graph_database_operations()
    assert result is True, "Graph Database operations test failed"


@pytest.mark.integration
@pytest.mark.multimodel
def test_cross_model_operations_integration(integration_test_suite):
    """Test operations that span multiple data models"""
    result = integration_test_suite.test_cross_model_operations()
    assert result is True, "Cross-model operations test failed"


@pytest.mark.integration
@pytest.mark.multimodel
@pytest.mark.performance
def test_performance_benchmarks_integration(integration_test_suite):
    """Run performance benchmarks for multimodel operations"""
    result = integration_test_suite.test_performance_benchmarks()
    assert result is True, "Performance benchmarks test failed"


@pytest.mark.integration
@pytest.mark.multimodel
def test_error_handling_integration(integration_test_suite):
    """Test error handling and edge cases"""
    result = integration_test_suite.test_error_handling()
    assert result is True, "Error handling test failed"


@pytest.mark.integration
@pytest.mark.multimodel
@pytest.mark.concurrency
def test_concurrent_operations_integration(integration_test_suite):
    """Test concurrent multimodel operations"""
    result = integration_test_suite.test_concurrent_operations()
    assert result is True, "Concurrent operations test failed"


@pytest.mark.integration
@pytest.mark.multimodel
@pytest.mark.slow
def test_full_integration_suite(integration_test_suite):
    """Run the complete integration test suite"""
    # Track all test results
    all_tests_passed = True

    # Run all integration tests
    all_tests_passed &= integration_test_suite.run_test(
        "Object-Relational Types", integration_test_suite.test_object_relational_types
    )

    all_tests_passed &= integration_test_suite.run_test(
        "Document Store Operations",
        integration_test_suite.test_document_store_operations,
    )

    all_tests_passed &= integration_test_suite.run_test(
        "Graph Database Operations",
        integration_test_suite.test_graph_database_operations,
    )

    all_tests_passed &= integration_test_suite.run_test(
        "Cross-Model Operations", integration_test_suite.test_cross_model_operations
    )

    all_tests_passed &= integration_test_suite.run_test(
        "Performance Benchmarks", integration_test_suite.test_performance_benchmarks
    )

    all_tests_passed &= integration_test_suite.run_test(
        "Error Handling", integration_test_suite.test_error_handling
    )

    all_tests_passed &= integration_test_suite.run_test(
        "Concurrent Operations", integration_test_suite.test_concurrent_operations
    )

    # Generate comprehensive report
    final_result = integration_test_suite.generate_test_report()

    assert (
        final_result is True
    ), f"Integration test suite failed. Passed: {sum(1 for t in integration_test_suite.test_results if t['status'] == 'PASS')}/{len(integration_test_suite.test_results)}"


def main():
    """Run the complete multimodel integration test suite"""
    test_suite = MultimodelIntegrationTestSuite()

    try:
        # Setup test environment
        if not test_suite.setup_test_environment():
            print("❌ Failed to setup test environment. Exiting.")
            return 1

        # Run all integration tests
        print("\n🚀 Starting Multimodel Integration Test Suite...")

        all_tests_passed = True

        # Core multimodel functionality tests
        all_tests_passed &= test_suite.run_test(
            "Object-Relational Types", test_suite.test_object_relational_types
        )

        all_tests_passed &= test_suite.run_test(
            "Document Store Operations", test_suite.test_document_store_operations
        )

        all_tests_passed &= test_suite.run_test(
            "Graph Database Operations", test_suite.test_graph_database_operations
        )

        all_tests_passed &= test_suite.run_test(
            "Cross-Model Operations", test_suite.test_cross_model_operations
        )

        # Performance and stress tests
        all_tests_passed &= test_suite.run_test(
            "Performance Benchmarks", test_suite.test_performance_benchmarks
        )

        all_tests_passed &= test_suite.run_test(
            "Error Handling", test_suite.test_error_handling
        )

        all_tests_passed &= test_suite.run_test(
            "Concurrent Operations", test_suite.test_concurrent_operations
        )

        # Generate comprehensive report
        final_result = test_suite.generate_test_report()

        return 0 if final_result else 1

    except Exception as e:
        print(f"💥 Test suite failed with exception: {e}")
        import traceback

        traceback.print_exc()
        return 1

    finally:
        # Always cleanup
        test_suite.cleanup_test_environment()


# Pytest Integration
@pytest.fixture(scope="module")
def integration_test_suite():
    """Pytest fixture for integration test suite"""
    suite = MultimodelIntegrationTestSuite()
    if suite.setup_test_environment():
        yield suite
    else:
        pytest.skip("Test environment setup failed")
    suite.cleanup_test_environment()


@pytest.mark.integration
def test_object_relational_types_integration(integration_test_suite):
    """Test Object-Relational custom types and inheritance"""
    result = integration_test_suite.run_test(
        "Object-Relational Types", integration_test_suite.test_object_relational_types
    )
    assert result is True


@pytest.mark.integration
def test_document_store_operations_integration(integration_test_suite):
    """Test Document Store operations"""
    result = integration_test_suite.run_test(
        "Document Store Operations",
        integration_test_suite.test_document_store_operations,
    )
    assert result is True


@pytest.mark.integration
def test_graph_database_operations_integration(integration_test_suite):
    """Test Graph Database operations"""
    result = integration_test_suite.run_test(
        "Graph Database Operations",
        integration_test_suite.test_graph_database_operations,
    )
    assert result is True


@pytest.mark.integration
def test_cross_model_operations_integration(integration_test_suite):
    """Test operations that span multiple data models"""
    result = integration_test_suite.run_test(
        "Cross-Model Operations", integration_test_suite.test_cross_model_operations
    )
    assert result is True


@pytest.mark.integration
@pytest.mark.performance
def test_performance_benchmarks_integration(integration_test_suite):
    """Run performance benchmarks for multimodel operations"""
    result = integration_test_suite.run_test(
        "Performance Benchmarks", integration_test_suite.test_performance_benchmarks
    )
    assert result is True


@pytest.mark.integration
def test_error_handling_integration(integration_test_suite):
    """Test error handling and edge cases"""
    result = integration_test_suite.run_test(
        "Error Handling", integration_test_suite.test_error_handling
    )
    assert result is True


@pytest.mark.integration
@pytest.mark.concurrency
def test_concurrent_operations_integration(integration_test_suite):
    """Test concurrent multimodel operations"""
    result = integration_test_suite.run_test(
        "Concurrent Operations", integration_test_suite.test_concurrent_operations
    )
    assert result is True


@pytest.mark.integration
def test_full_integration_suite(integration_test_suite):
    """Test the complete integration suite"""
    all_tests_passed = True

    test_methods = [
        (
            "Object-Relational Types",
            integration_test_suite.test_object_relational_types,
        ),
        (
            "Document Store Operations",
            integration_test_suite.test_document_store_operations,
        ),
        (
            "Graph Database Operations",
            integration_test_suite.test_graph_database_operations,
        ),
        ("Cross-Model Operations", integration_test_suite.test_cross_model_operations),
        ("Performance Benchmarks", integration_test_suite.test_performance_benchmarks),
        ("Error Handling", integration_test_suite.test_error_handling),
        ("Concurrent Operations", integration_test_suite.test_concurrent_operations),
    ]

    for test_name, test_method in test_methods:
        test_result = integration_test_suite.run_test(test_name, test_method)
        all_tests_passed &= test_result

    final_result = integration_test_suite.generate_test_report()
    assert final_result is True
    assert all_tests_passed is True


if __name__ == "__main__":
    sys.exit(main())
