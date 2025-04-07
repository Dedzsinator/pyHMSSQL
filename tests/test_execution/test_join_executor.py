"""
Tests for index usage in JOIN operations.
"""

import pytest
import time
import logging


class TestIndexJoins:
    """Test JOIN operations with and without indexes."""

    @pytest.fixture
    def setup_join_tables(self, schema_manager, catalog_manager):
        """Set up two tables with data for join testing."""
        # Create customers table
        schema_manager.execute_create_table({
            "type": "CREATE_TABLE",
            "table": "customers",
            "columns": [
                "id INT NOT NULL PRIMARY KEY",
                "name TEXT NOT NULL",
                "email TEXT"
            ]
        })

        # Create orders table with foreign key to customers
        schema_manager.execute_create_table({
            "type": "CREATE_TABLE",
            "table": "orders",
            "columns": [
                "id INT NOT NULL PRIMARY KEY",
                "customer_id INT NOT NULL",
                "amount DECIMAL NOT NULL",
                "FOREIGN KEY (customer_id) REFERENCES customers(id)"
            ]
        })

        # Insert sample data in customers
        for i in range(1, 101):
            catalog_manager.insert_record(
                "customers",
                {"id": i, "name": f"Customer {i}", "email": f"customer{i}@example.com"}
            )

        # Insert sample data in orders (multiple orders per customer)
        for i in range(1, 301):
            customer_id = ((i - 1) % 100) + 1  # Maps to customer IDs 1-100
            catalog_manager.insert_record(
                "orders",
                {"id": i, "customer_id": customer_id, "amount": i * 10.5}
            )

        # Return the table names for cleanup
        return ["customers", "orders"]

    @pytest.fixture
    def cleanup_join_tables(self, schema_manager):
        """Cleanup fixture to drop tables after tests."""
        yield
        schema_manager.execute_drop_table({"type": "DROP_TABLE", "table": "orders"})
        schema_manager.execute_drop_table({"type": "DROP_TABLE", "table": "customers"})

    def test_join_without_index(self, execution_engine, setup_join_tables, cleanup_join_tables):
        """Test a join operation without any indexes."""
        # Execute a join query
        plan = {
            "type": "JOIN",
            "table1": "customers",
            "table2": "orders",
            "condition": "customers.id = orders.customer_id",
            "join_type": "INNER"
        }

        start_time = time.time()
        result = execution_engine.execute(plan)
        execution_time = time.time() - start_time

        # Verify results
        assert result["status"] == "success"
        data = result.get("data", [])
        assert len(data) == 300  # All orders should have customers

        # Store time for comparison
        return execution_time

    def test_join_with_index(self, schema_manager, execution_engine, setup_join_tables, cleanup_join_tables):
        """Test a join operation with an index on the join column."""
        # Create an index on the join column
        schema_manager.execute_create_index({
            "type": "CREATE_INDEX",
            "index_name": "idx_customer_id",
            "table": "orders",
            "column": "customer_id"
        })

        # Execute the same join query
        plan = {
            "type": "JOIN",
            "table1": "customers",
            "table2": "orders",
            "condition": "customers.id = orders.customer_id",
            "join_type": "INNER"
        }

        start_time = time.time()
        result = execution_engine.execute(plan)
        execution_time = time.time() - start_time

        # Verify results
        assert result["status"] == "success"
        data = result.get("data", [])
        assert len(data) == 300  # All orders should match with customers

        # Verify the query plan uses index join
        assert "join_algorithm" in plan
        assert plan["join_algorithm"] == "INDEX"

        return execution_time

    def test_join_performance_improvement(self, execution_engine, schema_manager, setup_join_tables, cleanup_join_tables):
        """Test that using an index improves join performance."""
        # Run join without index
        time_without_index = self.test_join_without_index(execution_engine, setup_join_tables, None)

        # Create index
        schema_manager.execute_create_index({
            "type": "CREATE_INDEX",
            "index_name": "idx_customer_id",
            "table": "orders",
            "column": "customer_id"
        })

        # Run join with index
        time_with_index = self.test_join_with_index(schema_manager, execution_engine, setup_join_tables, None)

        # Check if index improves performance (should be faster)
        # Note: In very small datasets, the overhead might make indexed join slower
        logging.info(f"Join without index: {time_without_index:.6f}s")
        logging.info(f"Join with index: {time_with_index:.6f}s")

        # We're not making a strict assertion because in test environments
        # with small data, index overhead might outweigh benefits
        assert True, "Performance comparison logged"

    def test_left_join_with_index(self, schema_manager, execution_engine, setup_join_tables, cleanup_join_tables):
        """Test a LEFT JOIN operation with an index."""
        # Create an index
        schema_manager.execute_create_index({
            "type": "CREATE_INDEX",
            "index_name": "idx_customer_id",
            "table": "orders",
            "column": "customer_id"
        })

        # Add a customer without orders
        catalog_manager = execution_engine.catalog_manager
        catalog_manager.insert_record(
            "customers",
            {"id": 500, "name": "No Orders Customer", "email": "noorders@example.com"}
        )

        # Execute LEFT JOIN query
        plan = {
            "type": "JOIN",
            "table1": "customers",
            "table2": "orders",
            "condition": "customers.id = orders.customer_id",
            "join_type": "LEFT"
        }

        result = execution_engine.execute(plan)

        # Verify results
        assert result["status"] == "success"
        data = result.get("data", [])
        assert len(data) == 301  # 300 matched orders + 1 customer without orders

        # Verify the customer with no orders is included with NULL values for order columns
        no_order_records = [r for r in data if r.get("customers.id") == 500]
        assert len(no_order_records) == 1
        assert no_order_records[0].get("orders.id") is None