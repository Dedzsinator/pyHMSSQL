"""
Comprehensive test suite for the Document Store Adapter.

This module tests all aspects of the Document Store functionality including:
- Document CRUD operations
- JSONPath queries and indexing
- Collection management
- Error handling and edge cases
- Performance characteristics
- Integration with unified record layout
"""

import pytest
import json
import uuid
import time
import tempfile
import threading
from typing import Dict, List, Any, Optional
from unittest.mock import Mock, patch, MagicMock

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from server.multimodel.document_store.doc_adapter import (
    DocumentStoreAdapter,
    DocumentQuery,
    DocumentCollection,
    JSONPathParser
)
from catalog_manager import CatalogManager
from transaction.transaction_manager import TransactionManager


class TestJSONPathParser:
    """Test the JSONPath parser functionality."""
    
    def test_extract_paths_simple_document(self):
        """Test path extraction from simple documents."""
        doc = {
            "name": "John",
            "age": 30,
            "active": True
        }
        
        paths = JSONPathParser.extract_paths(doc)
        expected_paths = [
            ("name", "John"),
            ("age", 30),
            ("active", True)
        ]
        
        assert len(paths) == 3
        for path, value in expected_paths:
            assert (path, value) in paths
    
    def test_extract_paths_nested_document(self):
        """Test path extraction from nested documents."""
        doc = {
            "user": {
                "profile": {
                    "name": "Alice",
                    "age": 25
                },
                "settings": {
                    "theme": "dark"
                }
            },
            "metadata": {
                "created": "2023-01-01"
            }
        }
        
        paths = JSONPathParser.extract_paths(doc)
        
        # Check for nested paths
        assert ("user.profile.name", "Alice") in paths
        assert ("user.profile.age", 25) in paths
        assert ("user.settings.theme", "dark") in paths
        assert ("metadata.created", "2023-01-01") in paths
        
        # Check for intermediate paths
        assert ("user.profile", {"name": "Alice", "age": 25}) in paths
        assert ("user.settings", {"theme": "dark"}) in paths
    
    def test_extract_paths_with_arrays(self):
        """Test path extraction with array elements."""
        doc = {
            "tags": ["python", "database", "nosql"],
            "scores": [85, 90, 78],
            "items": [
                {"name": "item1", "value": 10},
                {"name": "item2", "value": 20}
            ]
        }
        
        paths = JSONPathParser.extract_paths(doc)
        
        # Check array element paths
        assert ("tags", ["python", "database", "nosql"]) in paths
        assert ("tags.0", "python") in paths
        assert ("tags.1", "database") in paths
        assert ("tags.2", "nosql") in paths
        
        assert ("scores.0", 85) in paths
        assert ("scores.1", 90) in paths
        assert ("scores.2", 78) in paths
        
        assert ("items.0.name", "item1") in paths
        assert ("items.0.value", 10) in paths
        assert ("items.1.name", "item2") in paths
        assert ("items.1.value", 20) in paths
    
    def test_get_value_by_path(self):
        """Test retrieving values by path."""
        doc = {
            "user": {
                "profile": {
                    "name": "Bob",
                    "contacts": {
                        "email": "bob@example.com"
                    }
                }
            },
            "tags": ["admin", "user"]
        }
        
        assert JSONPathParser.get_value_by_path(doc, "user.profile.name") == "Bob"
        assert JSONPathParser.get_value_by_path(doc, "user.profile.contacts.email") == "bob@example.com"
        assert JSONPathParser.get_value_by_path(doc, "tags.0") == "admin"
        assert JSONPathParser.get_value_by_path(doc, "tags.1") == "user"
        assert JSONPathParser.get_value_by_path(doc, "nonexistent.path") is None
    
    def test_evaluate_filter_simple_conditions(self):
        """Test filter evaluation with simple conditions."""
        doc = {
            "name": "Alice",
            "age": 30,
            "active": True,
            "score": 85.5
        }
        
        assert JSONPathParser.evaluate_filter(doc, {"name": "Alice"}) == True
        assert JSONPathParser.evaluate_filter(doc, {"name": "Bob"}) == False
        assert JSONPathParser.evaluate_filter(doc, {"age": 30}) == True
        assert JSONPathParser.evaluate_filter(doc, {"age": 25}) == False
        assert JSONPathParser.evaluate_filter(doc, {"active": True}) == True
        assert JSONPathParser.evaluate_filter(doc, {"active": False}) == False
    
    def test_evaluate_filter_nested_conditions(self):
        """Test filter evaluation with nested conditions."""
        doc = {
            "user": {
                "profile": {
                    "name": "Charlie",
                    "age": 28
                }
            }
        }
        
        assert JSONPathParser.evaluate_filter(doc, {"user.profile.name": "Charlie"}) == True
        assert JSONPathParser.evaluate_filter(doc, {"user.profile.name": "Dave"}) == False
        assert JSONPathParser.evaluate_filter(doc, {"user.profile.age": 28}) == True
    
    def test_evaluate_filter_comparison_operators(self):
        """Test filter evaluation with comparison operators."""
        doc = {
            "score": 85,
            "rating": 4.5,
            "count": 10
        }
        
        # Greater than
        assert JSONPathParser.evaluate_filter(doc, {"score": {"$gt": 80}}) == True
        assert JSONPathParser.evaluate_filter(doc, {"score": {"$gt": 90}}) == False
        
        # Greater than or equal
        assert JSONPathParser.evaluate_filter(doc, {"score": {"$gte": 85}}) == True
        assert JSONPathParser.evaluate_filter(doc, {"score": {"$gte": 90}}) == False
        
        # Less than
        assert JSONPathParser.evaluate_filter(doc, {"score": {"$lt": 90}}) == True
        assert JSONPathParser.evaluate_filter(doc, {"score": {"$lt": 80}}) == False
        
        # Less than or equal
        assert JSONPathParser.evaluate_filter(doc, {"score": {"$lte": 85}}) == True
        assert JSONPathParser.evaluate_filter(doc, {"score": {"$lte": 80}}) == False
        
        # Not equal
        assert JSONPathParser.evaluate_filter(doc, {"score": {"$ne": 90}}) == True
        assert JSONPathParser.evaluate_filter(doc, {"score": {"$ne": 85}}) == False


class TestDocumentStoreAdapter:
    """Test the main Document Store Adapter functionality."""
    
    @pytest.fixture
    def mock_catalog_manager(self):
        """Create a mock catalog manager."""
        catalog = Mock()  # Remove spec to allow any attribute access
        catalog.get_table_schema.return_value = []
        catalog.table_exists.return_value = False
        catalog.create_table.return_value = True
        catalog.drop_table.return_value = True
        catalog.insert_record.return_value = {"id": 1}
        catalog.query_with_condition.return_value = []
        catalog.get_current_database.return_value = "test_db"
        catalog.list_tables.return_value = []
        return catalog
    
    @pytest.fixture
    def mock_transaction_manager(self):
        """Create a mock transaction manager."""
        transaction = Mock()  # Remove spec to allow any attribute access
        transaction.begin_transaction.return_value = "txn_123"
        transaction.commit_transaction.return_value = {"status": "success"}
        transaction.rollback_transaction.return_value = {"status": "success"}
        return transaction
        transaction.commit_transaction.return_value = True
        transaction.rollback_transaction.return_value = True
        return transaction
    
    @pytest.fixture
    def adapter(self, mock_catalog_manager, mock_transaction_manager):
        """Create a document store adapter instance."""
        return DocumentStoreAdapter(mock_catalog_manager, mock_transaction_manager)
    
    def test_create_collection(self, adapter):
        """Test collection creation."""
        result = adapter.create_collection("users", "test_schema")
        assert result == True
        
        # Check that collection exists
        assert "test_schema.users" in adapter.collections
        collection = adapter.collections["test_schema.users"]
        assert collection.name == "users"
        assert collection.schema_name == "test_schema"
        
        # Check that primary index was created
        assert "test_schema.users._id" in adapter.path_indexes
    
    def test_create_duplicate_collection(self, adapter):
        """Test creating a collection that already exists."""
        adapter.create_collection("users", "test_schema")
        
        with pytest.raises(ValueError, match="Collection .* already exists"):
            adapter.create_collection("users", "test_schema")
    
    def test_drop_collection(self, adapter):
        """Test collection deletion."""
        adapter.create_collection("users", "test_schema")
        
        result = adapter.drop_collection("users", "test_schema")
        assert result == True
        
        # Check that collection was removed
        assert "test_schema.users" not in adapter.collections
        
        # Check that indexes were cleaned up
        assert "test_schema.users._id" not in adapter.path_indexes
    
    def test_drop_nonexistent_collection(self, adapter):
        """Test dropping a collection that doesn't exist."""
        result = adapter.drop_collection("nonexistent", "test_schema")
        assert result == False
    
    def test_insert_document(self, adapter):
        """Test basic document insertion."""
        adapter.create_collection("users", "test_schema")
        
        doc = {
            "name": "John Doe",
            "email": "john@example.com",
            "age": 30
        }
        
        doc_id = adapter.insert_document("users", doc, "test_schema")
        assert doc_id is not None
        assert isinstance(doc_id, str)
        
        # Verify document was stored
        retrieved_doc = adapter.find_document_by_id("users", doc_id, "test_schema")
        assert retrieved_doc is not None
        assert retrieved_doc["name"] == "John Doe"
        assert retrieved_doc["email"] == "john@example.com"
        assert retrieved_doc["age"] == 30
        assert retrieved_doc["_id"] == doc_id
    
    def test_insert_document_with_custom_id(self, adapter):
        """Test inserting document with custom ID."""
        adapter.create_collection("users", "test_schema")
        
        doc = {
            "name": "Jane Doe",
            "email": "jane@example.com"
        }
        
        custom_id = "user_123"
        doc_id = adapter.insert_document("users", doc, "test_schema", custom_id)
        assert doc_id == custom_id
        
        retrieved_doc = adapter.find_document_by_id("users", custom_id, "test_schema")
        assert retrieved_doc["_id"] == custom_id
    
    def test_insert_document_nonexistent_collection(self, adapter):
        """Test inserting into nonexistent collection."""
        doc = {"name": "Test"}
        
        with pytest.raises(ValueError, match="Collection .* does not exist"):
            adapter.insert_document("nonexistent", doc, "test_schema")
    
    def test_update_document(self, adapter):
        """Test document updates."""
        adapter.create_collection("users", "test_schema")
        
        # Insert initial document
        doc = {
            "name": "John Doe",
            "email": "john@example.com",
            "age": 30
        }
        doc_id = adapter.insert_document("users", doc, "test_schema")
        
        # Update document
        updates = {
            "age": 31,
            "city": "New York"
        }
        result = adapter.update_document("users", doc_id, updates, "test_schema")
        assert result == True
        
        # Verify updates
        updated_doc = adapter.find_document_by_id("users", doc_id, "test_schema")
        assert updated_doc["age"] == 31
        assert updated_doc["city"] == "New York"
        assert updated_doc["name"] == "John Doe"  # Unchanged field
        assert updated_doc["email"] == "john@example.com"  # Unchanged field
    
    def test_update_nonexistent_document(self, adapter):
        """Test updating a document that doesn't exist."""
        adapter.create_collection("users", "test_schema")
        
        updates = {"age": 31}
        result = adapter.update_document("users", "nonexistent_id", updates, "test_schema")
        assert result == False
    
    def test_delete_document(self, adapter):
        """Test document deletion."""
        adapter.create_collection("users", "test_schema")
        
        # Insert document
        doc = {"name": "Test User"}
        doc_id = adapter.insert_document("users", doc, "test_schema")
        
        # Verify document exists
        assert adapter.find_document_by_id("users", doc_id, "test_schema") is not None
        
        # Delete document
        result = adapter.delete_document("users", doc_id, "test_schema")
        assert result == True
        
        # Verify document was deleted
        assert adapter.find_document_by_id("users", doc_id, "test_schema") is None
    
    def test_delete_nonexistent_document(self, adapter):
        """Test deleting a document that doesn't exist."""
        adapter.create_collection("users", "test_schema")
        
        result = adapter.delete_document("users", "nonexistent_id", "test_schema")
        assert result == False
    
    def test_find_documents_simple_filter(self, adapter):
        """Test finding documents with simple filters."""
        adapter.create_collection("users", "test_schema")
        
        # Insert test documents
        docs = [
            {"name": "John", "age": 25, "city": "NYC"},
            {"name": "Jane", "age": 30, "city": "LA"},
            {"name": "Bob", "age": 25, "city": "Chicago"},
            {"name": "Alice", "age": 35, "city": "NYC"}
        ]
        
        for doc in docs:
            adapter.insert_document("users", doc, "test_schema")
        
        # Test various filters
        query = DocumentQuery(collection="users", filter_conditions={"age": 25})
        results = adapter.find_documents(query)
        assert len(results) == 2
        names = [doc["name"] for doc in results]
        assert "John" in names
        assert "Bob" in names
        
        query = DocumentQuery(collection="users", filter_conditions={"city": "NYC"})
        results = adapter.find_documents(query)
        assert len(results) == 2
        names = [doc["name"] for doc in results]
        assert "John" in names
        assert "Alice" in names
    
    def test_find_documents_with_projection(self, adapter):
        """Test finding documents with field projection."""
        adapter.create_collection("users", "test_schema")
        
        doc = {
            "name": "John",
            "email": "john@example.com",
            "age": 30,
            "password": "secret"
        }
        adapter.insert_document("users", doc, "test_schema")
        
        # Test projection to specific fields
        query = DocumentQuery(
            collection="users",
            filter_conditions={"name": "John"},
            projection=["name", "email"]
        )
        results = adapter.find_documents(query)
        
        assert len(results) == 1
        result_doc = results[0]
        assert "name" in result_doc
        assert "email" in result_doc
        assert "password" not in result_doc
        assert "age" not in result_doc
        assert "_id" in result_doc  # ID is always included
    
    def test_find_documents_with_limit(self, adapter):
        """Test finding documents with result limits."""
        adapter.create_collection("users", "test_schema")
        
        # Insert multiple documents
        for i in range(10):
            doc = {"name": f"User{i}", "index": i}
            adapter.insert_document("users", doc, "test_schema")
        
        # Test limit
        query = DocumentQuery(collection="users", limit=5)
        results = adapter.find_documents(query)
        assert len(results) == 5
        
        # Test limit with skip
        query = DocumentQuery(collection="users", limit=3, skip=2)
        results = adapter.find_documents(query)
        assert len(results) == 3
    
    def test_create_index(self, adapter):
        """Test creating indexes on document paths."""
        adapter.create_collection("users", "test_schema")
        
        # Insert test documents
        docs = [
            {"name": "John", "profile": {"age": 25}},
            {"name": "Jane", "profile": {"age": 30}},
        ]
        for doc in docs:
            adapter.insert_document("users", doc, "test_schema")
        
        # Create index on nested path
        index_name = adapter.create_index("users", "profile.age", "test_schema")
        assert index_name is not None
        
        # Verify index was created
        index_path = "test_schema.users.profile.age"
        assert index_path in adapter.path_indexes
        
        # Test duplicate index creation
        with pytest.raises(ValueError, match="Index on path .* already exists"):
            adapter.create_index("users", "profile.age", "test_schema")
    
    def test_complex_nested_queries(self, adapter):
        """Test queries on deeply nested documents."""
        adapter.create_collection("products", "test_schema")
        
        docs = [
            {
                "name": "Laptop",
                "specs": {
                    "cpu": {
                        "brand": "Intel",
                        "model": "i7",
                        "cores": 8
                    },
                    "memory": {"size": 16, "type": "DDR4"}
                },
                "price": 1200
            },
            {
                "name": "Desktop",
                "specs": {
                    "cpu": {
                        "brand": "AMD",
                        "model": "Ryzen",
                        "cores": 12
                    },
                    "memory": {"size": 32, "type": "DDR4"}
                },
                "price": 1500
            }
        ]
        
        for doc in docs:
            adapter.insert_document("products", doc, "test_schema")
        
        # Query by nested CPU brand
        query = DocumentQuery(
            collection="products",
            filter_conditions={"specs.cpu.brand": "Intel"}
        )
        results = adapter.find_documents(query)
        assert len(results) == 1
        assert results[0]["name"] == "Laptop"
        
        # Query by nested memory size with comparison
        query = DocumentQuery(
            collection="products",
            filter_conditions={"specs.memory.size": {"$gte": 20}}
        )
        results = adapter.find_documents(query)
        assert len(results) == 1
        assert results[0]["name"] == "Desktop"


class TestDocumentStorePerformance:
    """Test performance characteristics of the document store."""
    
    @pytest.fixture
    def adapter(self):
        """Create adapter for performance testing."""
        catalog = Mock(spec=CatalogManager)
        catalog.get_table_info.return_value = None
        transaction = Mock(spec=TransactionManager)
        return DocumentStoreAdapter(catalog, transaction)
    
    def test_bulk_insert_performance(self, adapter):
        """Test performance of bulk document insertions."""
        adapter.create_collection("bulk_test", "perf_schema")
        
        # Generate test documents
        docs = []
        for i in range(1000):
            doc = {
                "id": i,
                "name": f"User{i}",
                "email": f"user{i}@example.com",
                "profile": {
                    "age": 20 + (i % 50),
                    "department": f"Dept{i % 10}",
                    "skills": [f"skill{j}" for j in range(i % 5 + 1)]
                }
            }
            docs.append(doc)
        
        # Measure insertion time
        start_time = time.time()
        doc_ids = []
        for doc in docs:
            doc_id = adapter.insert_document("bulk_test", doc, "perf_schema")
            doc_ids.append(doc_id)
        end_time = time.time()
        
        insertion_time = end_time - start_time
        docs_per_second = len(docs) / insertion_time
        
        print(f"Inserted {len(docs)} documents in {insertion_time:.2f}s")
        print(f"Rate: {docs_per_second:.2f} docs/sec")
        
        # Verify all documents were inserted
        assert len(doc_ids) == len(docs)
        assert all(doc_id is not None for doc_id in doc_ids)
        
        # Performance should be reasonable (adjust threshold as needed)
        assert docs_per_second > 100  # At least 100 docs/sec
    
    def test_query_performance_with_indexes(self, adapter):
        """Test query performance with and without indexes."""
        adapter.create_collection("perf_test", "perf_schema")
        
        # Insert test data
        for i in range(500):
            doc = {
                "user_id": i,
                "name": f"User{i}",
                "score": i % 100,
                "category": f"cat_{i % 10}"
            }
            adapter.insert_document("perf_test", doc, "perf_schema")
        
        # Test query performance without index
        start_time = time.time()
        query = DocumentQuery(
            collection="perf_test",
            filter_conditions={"score": {"$gte": 50}}
        )
        results_no_index = adapter.find_documents(query)
        time_no_index = time.time() - start_time
        
        # Create index on score field
        adapter.create_index("perf_test", "score", "perf_schema")
        
        # Test query performance with index
        start_time = time.time()
        results_with_index = adapter.find_documents(query)
        time_with_index = time.time() - start_time
        
        # Verify same results
        assert len(results_no_index) == len(results_with_index)
        
        print(f"Query time without index: {time_no_index:.4f}s")
        print(f"Query time with index: {time_with_index:.4f}s")
        
        # With index should be faster for larger datasets
        # (May not always be true for small datasets due to overhead)
        if len(results_no_index) > 100:
            assert time_with_index <= time_no_index
    
    def test_concurrent_operations(self, adapter):
        """Test concurrent document operations."""
        adapter.create_collection("concurrent_test", "perf_schema")
        
        def worker_insert(worker_id, num_docs):
            """Worker function for concurrent insertions."""
            doc_ids = []
            for i in range(num_docs):
                doc = {
                    "worker": worker_id,
                    "index": i,
                    "data": f"data_{worker_id}_{i}"
                }
                try:
                    doc_id = adapter.insert_document("concurrent_test", doc, "perf_schema")
                    doc_ids.append(doc_id)
                except Exception as e:
                    print(f"Worker {worker_id} error: {e}")
            return doc_ids
        
        # Run concurrent insertions
        num_workers = 5
        docs_per_worker = 50
        threads = []
        results = {}
        
        start_time = time.time()
        for worker_id in range(num_workers):
            thread = threading.Thread(
                target=lambda wid=worker_id: results.update({wid: worker_insert(wid, docs_per_worker)})
            )
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        end_time = time.time()
        
        # Verify results
        total_docs = sum(len(doc_ids) for doc_ids in results.values())
        expected_total = num_workers * docs_per_worker
        
        print(f"Concurrent insertion: {total_docs}/{expected_total} docs in {end_time - start_time:.2f}s")
        
        # Should have inserted most documents successfully
        assert total_docs >= expected_total * 0.8  # Allow for some concurrency issues


class TestDocumentStoreEdgeCases:
    """Test edge cases and error conditions."""
    
    @pytest.fixture
    def adapter(self):
        """Create adapter for edge case testing."""
        catalog = Mock(spec=CatalogManager)
        catalog.get_table_info.return_value = None
        transaction = Mock(spec=TransactionManager)
        return DocumentStoreAdapter(catalog, transaction)
    
    def test_large_document_handling(self, adapter):
        """Test handling of large documents."""
        adapter.create_collection("large_docs", "test_schema")
        
        # Create a large document
        large_doc = {
            "id": "large_doc_1",
            "data": "x" * 100000,  # 100KB string
            "array": list(range(1000)),  # Large array
            "nested": {
                "level1": {
                    "level2": {
                        "level3": {
                            "data": ["item" + str(i) for i in range(100)]
                        }
                    }
                }
            }
        }
        
        # Should handle large documents
        doc_id = adapter.insert_document("large_docs", large_doc, "test_schema")
        assert doc_id is not None
        
        # Should be able to retrieve large documents
        retrieved = adapter.find_document_by_id("large_docs", doc_id, "test_schema")
        assert retrieved is not None
        assert len(retrieved["data"]) == 100000
        assert len(retrieved["array"]) == 1000
    
    def test_unicode_and_special_characters(self, adapter):
        """Test handling of Unicode and special characters."""
        adapter.create_collection("unicode_test", "test_schema")
        
        doc = {
            "english": "Hello World",
            "chinese": "你好世界",
            "arabic": "مرحبا بالعالم",
            "emoji": "😀🌟💖",
            "special_chars": "!@#$%^&*()[]{}|\\:;\"'<>?,./-_=+",
            "unicode_escape": "\u0041\u0042\u0043",  # ABC
            "null_char": "test\x00test",
            "newlines": "line1\nline2\rline3\r\nline4"
        }
        
        doc_id = adapter.insert_document("unicode_test", doc, "test_schema")
        assert doc_id is not None
        
        retrieved = adapter.find_document_by_id("unicode_test", doc_id, "test_schema")
        assert retrieved["chinese"] == "你好世界"
        assert retrieved["arabic"] == "مرحبا بالعالم"
        assert retrieved["emoji"] == "😀🌟💖"
        assert retrieved["unicode_escape"] == "ABC"
    
    def test_null_and_undefined_values(self, adapter):
        """Test handling of null and undefined values."""
        adapter.create_collection("null_test", "test_schema")
        
        doc = {
            "null_field": None,
            "empty_string": "",
            "empty_array": [],
            "empty_object": {},
            "zero": 0,
            "false": False,
            "undefined": None  # JSON doesn't have undefined, maps to null
        }
        
        doc_id = adapter.insert_document("null_test", doc, "test_schema")
        assert doc_id is not None
        
        retrieved = adapter.find_document_by_id("null_test", doc_id, "test_schema")
        assert retrieved["null_field"] is None
        assert retrieved["empty_string"] == ""
        assert retrieved["empty_array"] == []
        assert retrieved["empty_object"] == {}
        assert retrieved["zero"] == 0
        assert retrieved["false"] == False
    
    def test_duplicate_id_handling(self, adapter):
        """Test handling of duplicate document IDs."""
        adapter.create_collection("dup_test", "test_schema")
        
        doc1 = {"name": "First Doc"}
        doc2 = {"name": "Second Doc"}
        
        # Insert with custom ID
        custom_id = "custom_123"
        doc_id1 = adapter.insert_document("dup_test", doc1, "test_schema", custom_id)
        assert doc_id1 == custom_id
        
        # Try to insert with same ID - should generate new ID or raise error
        doc_id2 = adapter.insert_document("dup_test", doc2, "test_schema", custom_id)
        
        # Implementation should either:
        # 1. Generate a new unique ID, or
        # 2. Raise an error
        # Either behavior is acceptable, test for consistency
        if doc_id2 == custom_id:
            # If same ID returned, it should have updated the document
            retrieved = adapter.find_document_by_id("dup_test", custom_id, "test_schema")
            assert retrieved["name"] == "Second Doc"
        else:
            # If different ID returned, both documents should exist
            assert doc_id2 != custom_id
            doc1_retrieved = adapter.find_document_by_id("dup_test", custom_id, "test_schema")
            doc2_retrieved = adapter.find_document_by_id("dup_test", doc_id2, "test_schema")
            assert doc1_retrieved["name"] == "First Doc"
            assert doc2_retrieved["name"] == "Second Doc"
    
    def test_malformed_queries(self, adapter):
        """Test handling of malformed queries."""
        adapter.create_collection("query_test", "test_schema")
        
        # Insert test document
        doc = {"name": "Test", "age": 25}
        adapter.insert_document("query_test", doc, "test_schema")
        
        # Test with invalid filter conditions
        invalid_queries = [
            {"invalid.operator": {"$invalid": "value"}},
            {"field": {"$gt": None}},
            {"": "empty_field_name"},
        ]
        
        for invalid_filter in invalid_queries:
            query = DocumentQuery(
                collection="query_test",
                filter_conditions=invalid_filter
            )
            
            # Should handle gracefully without crashing
            try:
                results = adapter.find_documents(query)
                # If no error, results should be empty or contain valid documents
                assert isinstance(results, list)
            except Exception as e:
                # If error occurs, it should be handled gracefully
                assert isinstance(e, (ValueError, TypeError, KeyError))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
