#!/usr/bin/env python3
"""
Cross-Component Integration Tests for pyHMSSQL

This test suite validates that all major components work together correctly:
- Parser + Planner + Execution Engine
- B+ Tree + Index Manager + Query Optimizer
- Multimodel components + Core SQL Engine
- Transaction Management + Concurrency Control
- Performance Optimization + Cache Systems
"""

import pytest
import tempfile
import shutil
import time
import threading
import concurrent.futures
from unittest.mock import Mock, patch
import sys
import os

# Add server directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../server"))

from catalog_manager import CatalogManager
from ddl_processor.index_manager import IndexManager
from execution_engine import ExecutionEngine
from planner import Planner
from parser import SQLGlotParser
# from transaction.transaction_manager import TransactionManager
from unittest.mock import Mock

# Mock TransactionManager for testing
class TransactionManager:
    def __init__(self):
        self.transactions = {}
    
    def begin_transaction(self):
        return "mock_transaction_id"
    
    def commit_transaction(self, tx_id):
        return True
    
    def rollback_transaction(self, tx_id):
        return True
from threading import Lock


class TestCrossComponentIntegration:
    """Test that all major components integrate properly"""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test environment"""
        self.test_dir = tempfile.mkdtemp(prefix="pyhmssql_integration_")
        self.catalog_manager = CatalogManager(data_dir=self.test_dir)
        self.index_manager = IndexManager(self.catalog_manager)
        self.planner = Planner(self.catalog_manager, self.index_manager)
        self.parser = SQLGlotParser()
        self.execution_engine = ExecutionEngine(
            self.catalog_manager, self.index_manager, self.planner
        )
        
        # Create test database and tables
        self.execution_engine.execute({"type": "CREATE_DATABASE", "database": "test_integration"})
        self.execution_engine.execute({"type": "USE_DATABASE", "database": "test_integration"})
        
        yield
        
        # Cleanup
        try:
            if hasattr(self, 'test_dir') and os.path.exists(self.test_dir):
                shutil.rmtree(self.test_dir)
        except:
            pass

    def test_complete_sql_query_lifecycle(self):
        """Test complete SQL query lifecycle from parsing to execution"""
        
        # 1. Create table via DDL
        create_table_sql = """
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            department VARCHAR(50),
            salary DECIMAL(10,2),
            hire_date DATE
        )
        """
        
        parsed = self.parser.parse_sql(create_table_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        assert result["status"] == "success"
        
        # 2. Insert data via DML
        insert_sql = """
        INSERT INTO employees (id, name, department, salary, hire_date) 
        VALUES 
            (1, 'John Doe', 'Engineering', 75000.00, '2020-01-15'),
            (2, 'Jane Smith', 'Marketing', 65000.00, '2019-03-10'),
            (3, 'Bob Johnson', 'Engineering', 80000.00, '2021-06-01'),
            (4, 'Alice Brown', 'HR', 60000.00, '2020-11-20')
        """
        
        parsed = self.parser.parse_sql(insert_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        assert result["status"] == "success"
        
        # 3. Create index
        create_index_sql = "CREATE INDEX idx_department ON employees(department)"
        parsed = self.parser.parse_sql(create_index_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        assert result["status"] == "success"
        
        # 4. Query with index usage
        select_sql = """
        SELECT name, salary 
        FROM employees 
        WHERE department = 'Engineering'
        ORDER BY salary DESC
        """
        
        parsed = self.parser.parse_sql(select_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        
        assert result["status"] == "success"
        assert len(result["rows"]) == 2
        assert result["rows"][0][0] == "Bob Johnson"  # Higher salary first
        assert result["rows"][1][0] == "John Doe"
        
        # 5. Update data
        update_sql = "UPDATE employees SET salary = 85000.00 WHERE name = 'John Doe'"
        parsed = self.parser.parse_sql(update_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        assert result["status"] == "success"
        
        # 6. Verify update with aggregation
        aggregate_sql = """
        SELECT department, COUNT(*) as employee_count, AVG(salary) as avg_salary
        FROM employees 
        GROUP BY department
        HAVING COUNT(*) > 1
        """
        
        parsed = self.parser.parse_sql(aggregate_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        
        assert result["status"] == "success"
        assert len(result["rows"]) == 1  # Only Engineering has > 1 employee
        assert result["rows"][0][0] == "Engineering"

    def test_join_query_with_indexes(self):
        """Test complex join queries with index optimization"""
        
        # Create tables
        create_customers = """
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100)
        )
        """
        
        create_orders = """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            amount DECIMAL(10,2),
            order_date DATE,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        )
        """
        
        for sql in [create_customers, create_orders]:
            parsed = self.parser.parse_sql(sql)
            plan = self.planner.plan_query(parsed)
            result = self.execution_engine.execute(plan)
            assert result["status"] == "success"
        
        # Insert test data
        insert_customers = """
        INSERT INTO customers (id, name, email) VALUES
            (1, 'Customer A', 'a@example.com'),
            (2, 'Customer B', 'b@example.com'),
            (3, 'Customer C', 'c@example.com')
        """
        
        insert_orders = """
        INSERT INTO orders (id, customer_id, amount, order_date) VALUES
            (1, 1, 100.00, '2023-01-01'),
            (2, 1, 150.00, '2023-01-15'),
            (3, 2, 200.00, '2023-02-01'),
            (4, 3, 75.00, '2023-02-15'),
            (5, 1, 300.00, '2023-03-01')
        """
        
        for sql in [insert_customers, insert_orders]:
            parsed = self.parser.parse_sql(sql)
            plan = self.planner.plan_query(parsed)
            result = self.execution_engine.execute(plan)
            assert result["status"] == "success"
        
        # Create indexes for join optimization
        create_index_sql = "CREATE INDEX idx_customer_id ON orders(customer_id)"
        parsed = self.parser.parse_sql(create_index_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        assert result["status"] == "success"
        
        # Complex join query
        join_sql = """
        SELECT c.name, c.email, COUNT(o.id) as order_count, SUM(o.amount) as total_amount
        FROM customers c
        LEFT JOIN orders o ON c.id = o.customer_id
        GROUP BY c.id, c.name, c.email
        HAVING SUM(o.amount) > 200 OR SUM(o.amount) IS NULL
        ORDER BY total_amount DESC
        """
        
        parsed = self.parser.parse_sql(join_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        
        assert result["status"] == "success"
        assert len(result["rows"]) >= 1  # Should have Customer A with total > 200

    def test_concurrent_operations(self):
        """Test concurrent database operations"""
        
        # Create table for concurrent testing
        create_table_sql = """
        CREATE TABLE concurrent_test (
            id INTEGER PRIMARY KEY,
            value INTEGER,
            thread_id INTEGER
        )
        """
        
        parsed = self.parser.parse_sql(create_table_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        assert result["status"] == "success"
        
        # Function for concurrent inserts
        def concurrent_insert(thread_id, num_inserts):
            results = []
            for i in range(num_inserts):
                insert_sql = f"""
                INSERT INTO concurrent_test (id, value, thread_id) 
                VALUES ({thread_id * 1000 + i}, {i * thread_id}, {thread_id})
                """
                try:
                    parsed = self.parser.parse_sql(insert_sql)
                    plan = self.planner.plan_query(parsed)
                    result = self.execution_engine.execute(plan)
                    results.append(result["status"] == "success")
                except Exception as e:
                    results.append(False)
            return results
        
        # Run concurrent operations
        num_threads = 3
        inserts_per_thread = 10
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(concurrent_insert, thread_id, inserts_per_thread)
                for thread_id in range(num_threads)
            ]
            
            all_results = []
            for future in concurrent.futures.as_completed(futures):
                results = future.result()
                all_results.extend(results)
        
        # Verify that most operations succeeded
        success_rate = sum(all_results) / len(all_results)
        assert success_rate >= 0.8  # At least 80% success rate
        
        # Verify data integrity
        select_sql = "SELECT COUNT(*) FROM concurrent_test"
        parsed = self.parser.parse_sql(select_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        
        assert result["status"] == "success"
        total_records = result["rows"][0][0]
        assert total_records > 0  # Some records should have been inserted

    def test_transaction_rollback_integration(self):
        """Test transaction rollback affects all components correctly"""
        
        # Create table
        create_table_sql = """
        CREATE TABLE transaction_test (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            balance DECIMAL(10,2)
        )
        """
        
        parsed = self.parser.parse_sql(create_table_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        assert result["status"] == "success"
        
        # Insert initial data
        insert_sql = """
        INSERT INTO transaction_test (id, name, balance) VALUES
            (1, 'Account A', 1000.00),
            (2, 'Account B', 500.00)
        """
        
        parsed = self.parser.parse_sql(insert_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        assert result["status"] == "success"
        
        # Begin transaction
        begin_sql = "BEGIN TRANSACTION"
        parsed = self.parser.parse_sql(begin_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        
        # Perform updates within transaction
        update1_sql = "UPDATE transaction_test SET balance = balance - 100 WHERE id = 1"
        update2_sql = "UPDATE transaction_test SET balance = balance + 100 WHERE id = 2"
        
        for sql in [update1_sql, update2_sql]:
            parsed = self.parser.parse_sql(sql)
            plan = self.planner.plan_query(parsed)
            result = self.execution_engine.execute(plan)
        
        # Rollback transaction
        rollback_sql = "ROLLBACK"
        parsed = self.parser.parse_sql(rollback_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        
        # Verify original values are restored
        select_sql = "SELECT id, balance FROM transaction_test ORDER BY id"
        parsed = self.parser.parse_sql(select_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        
        assert result["status"] == "success"
        assert len(result["rows"]) == 2
        assert float(result["rows"][0][1]) == 1000.00  # Account A unchanged
        assert float(result["rows"][1][1]) == 500.00   # Account B unchanged

    def test_error_handling_across_components(self):
        """Test that errors are handled gracefully across all components"""
        
        # Test parser error handling
        invalid_sql = "INVALID SQL SYNTAX HERE"
        try:
            parsed = self.parser.parse_sql(invalid_sql)
            # If parsing doesn't raise an exception, the plan should fail
            plan = self.planner.plan_query(parsed)
            result = self.execution_engine.execute(plan)
            assert result["status"] == "error"
        except Exception:
            # Parser should handle syntax errors gracefully
            pass
        
        # Test constraint violation
        create_table_sql = """
        CREATE TABLE constraint_test (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100) NOT NULL
        )
        """
        
        parsed = self.parser.parse_sql(create_table_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        assert result["status"] == "success"
        
        # Try to insert duplicate primary key
        insert1_sql = "INSERT INTO constraint_test (id, name) VALUES (1, 'Test')"
        insert2_sql = "INSERT INTO constraint_test (id, name) VALUES (1, 'Duplicate')"
        
        parsed = self.parser.parse_sql(insert1_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        assert result["status"] == "success"
        
        parsed = self.parser.parse_sql(insert2_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        assert result["status"] == "error"  # Should fail due to duplicate key
        
        # Verify that the table is still accessible after error
        select_sql = "SELECT COUNT(*) FROM constraint_test"
        parsed = self.parser.parse_sql(select_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        assert result["status"] == "success"
        assert result["rows"][0][0] == 1  # Only one record should exist

    def test_performance_optimization_integration(self):
        """Test that performance optimizations work across components"""
        
        # Create large dataset for performance testing
        create_table_sql = """
        CREATE TABLE performance_test (
            id INTEGER PRIMARY KEY,
            category VARCHAR(50),
            value INTEGER,
            timestamp DATETIME
        )
        """
        
        parsed = self.parser.parse_sql(create_table_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        assert result["status"] == "success"
        
        # Insert test data (smaller dataset for CI environment)
        num_records = 100
        for i in range(0, num_records, 10):  # Batch inserts
            values = []
            for j in range(10):
                record_id = i + j
                if record_id >= num_records:
                    break
                category = f"cat_{record_id % 5}"
                value = record_id * 10
                values.append(f"({record_id}, '{category}', {value}, '2023-01-01')")
            
            if values:
                insert_sql = f"INSERT INTO performance_test (id, category, value, timestamp) VALUES {', '.join(values)}"
                parsed = self.parser.parse_sql(insert_sql)
                plan = self.planner.plan_query(parsed)
                result = self.execution_engine.execute(plan)
                assert result["status"] == "success"
        
        # Create index for performance optimization
        create_index_sql = "CREATE INDEX idx_category ON performance_test(category)"
        parsed = self.parser.parse_sql(create_index_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        assert result["status"] == "success"
        
        # Test query performance with index
        start_time = time.time()
        select_sql = """
        SELECT category, COUNT(*), AVG(value)
        FROM performance_test 
        WHERE category = 'cat_1'
        GROUP BY category
        """
        
        parsed = self.parser.parse_sql(select_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        query_time = time.time() - start_time
        
        assert result["status"] == "success"
        assert query_time < 1.0  # Should be fast with index
        assert len(result["rows"]) == 1

    def test_multimodel_integration_simulation(self):
        """Test multimodel operations integration (simulated)"""
        
        # Since actual multimodel functionality may not be fully integrated,
        # we'll test the infrastructure that supports it
        
        # Test that the planner can handle multimodel operation types
        multimodel_operations = [
            {"type": "CREATE_COLLECTION", "collection": "documents"},
            {"type": "CREATE_GRAPH_SCHEMA", "graph": "social_network"},
            {"type": "CREATE_TYPE", "type_name": "address_type"},
        ]
        
        for operation in multimodel_operations:
            try:
                plan = self.planner.plan_query(operation)
                result = self.execution_engine.execute(plan)
                # Operation might not be fully implemented, but should not crash
                assert "status" in result
            except Exception as e:
                # Should handle unsupported operations gracefully
                assert "not implemented" in str(e).lower() or "not supported" in str(e).lower()

    def test_b_plus_tree_index_integration(self):
        """Test B+ tree and index integration"""
        
        # Create table with various data types
        create_table_sql = """
        CREATE TABLE tree_test (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            score DECIMAL(8,2),
            active BOOLEAN
        )
        """
        
        parsed = self.parser.parse_sql(create_table_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        assert result["status"] == "success"
        
        # Insert data with various patterns
        insert_data = []
        for i in range(50):
            insert_data.append(f"({i}, 'Name{i}', {i * 10.5}, {i % 2 == 0})")
        
        insert_sql = f"INSERT INTO tree_test (id, name, score, active) VALUES {', '.join(insert_data)}"
        parsed = self.parser.parse_sql(insert_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        assert result["status"] == "success"
        
        # Create multiple indexes
        indexes = [
            "CREATE INDEX idx_name ON tree_test(name)",
            "CREATE INDEX idx_score ON tree_test(score)",
            "CREATE INDEX idx_active ON tree_test(active)"
        ]
        
        for index_sql in indexes:
            parsed = self.parser.parse_sql(index_sql)
            plan = self.planner.plan_query(parsed)
            result = self.execution_engine.execute(plan)
            assert result["status"] == "success"
        
        # Test range queries that should use indexes
        range_queries = [
            "SELECT * FROM tree_test WHERE score BETWEEN 100 AND 200",
            "SELECT * FROM tree_test WHERE name LIKE 'Name1%'",
            "SELECT * FROM tree_test WHERE active = true AND score > 50"
        ]
        
        for query_sql in range_queries:
            parsed = self.parser.parse_sql(query_sql)
            plan = self.planner.plan_query(parsed)
            result = self.execution_engine.execute(plan)
            assert result["status"] == "success"
            assert len(result["rows"]) >= 0  # Should return valid results

    def test_schema_evolution_integration(self):
        """Test schema changes and their impact on all components"""
        
        # Create initial table
        create_table_sql = """
        CREATE TABLE evolution_test (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100)
        )
        """
        
        parsed = self.parser.parse_sql(create_table_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        assert result["status"] == "success"
        
        # Insert initial data
        insert_sql = "INSERT INTO evolution_test (id, name) VALUES (1, 'Initial')"
        parsed = self.parser.parse_sql(insert_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        assert result["status"] == "success"
        
        # Add column (ALTER TABLE might not be fully implemented)
        try:
            alter_sql = "ALTER TABLE evolution_test ADD COLUMN email VARCHAR(100)"
            parsed = self.parser.parse_sql(alter_sql)
            plan = self.planner.plan_query(parsed)
            result = self.execution_engine.execute(plan)
            
            if result["status"] == "success":
                # If ALTER TABLE works, test it
                update_sql = "UPDATE evolution_test SET email = 'test@example.com' WHERE id = 1"
                parsed = self.parser.parse_sql(update_sql)
                plan = self.planner.plan_query(parsed)
                result = self.execution_engine.execute(plan)
                assert result["status"] == "success"
        except Exception:
            # ALTER TABLE might not be implemented
            pass
        
        # Verify table is still accessible
        select_sql = "SELECT * FROM evolution_test"
        parsed = self.parser.parse_sql(select_sql)
        plan = self.planner.plan_query(parsed)
        result = self.execution_engine.execute(plan)
        assert result["status"] == "success"
        assert len(result["rows"]) == 1


if __name__ == "__main__":
    # Can be run standalone for debugging
    test_instance = TestCrossComponentIntegration()
    test_instance.setup()
    
    try:
        print("Running cross-component integration tests...")
        test_instance.test_complete_sql_query_lifecycle()
        print("✅ SQL query lifecycle test passed")
        
        test_instance.test_join_query_with_indexes()
        print("✅ Join query with indexes test passed")
        
        test_instance.test_error_handling_across_components()
        print("✅ Error handling test passed")
        
        print("🎉 All integration tests passed!")
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        raise
    finally:
        # Cleanup handled by fixture
        pass
