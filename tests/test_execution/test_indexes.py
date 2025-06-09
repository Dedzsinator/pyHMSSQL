"""
Tests for index usage in query operations (filtering, sorting).
"""

import pytest
import time


class TestIndexScan:
    """Test index usage in SELECT queries."""

    @pytest.fixture
    def setup_index_table(self, schema_manager, catalog_manager):
        """Set up a table with data for index testing."""
        # Create users table
        schema_manager.execute_create_table(
            {
                "type": "CREATE_TABLE",
                "table": "users",
                "columns": [
                    "id INT NOT NULL PRIMARY KEY",
                    "username TEXT NOT NULL",
                    "age INT",
                    "email TEXT",
                ],
            }
        )

        # Insert sample data
        for i in range(1, 501):
            catalog_manager.insert_record(
                "users",
                {
                    "id": i,
                    "username": f"user{i}",
                    "age": 20 + (i % 60),
                    "email": f"user{i}@example.com",
                },
            )

        return "users"

    @pytest.fixture
    def cleanup_index_table(self, schema_manager):
        """Cleanup fixture to drop tables after tests."""
        yield
        schema_manager.execute_drop_table({"type": "DROP_TABLE", "table": "users"})

    def test_select_with_index(
        self, schema_manager, execution_engine, setup_index_table, cleanup_index_table
    ):  # pylint: disable=unused-argument
        """Test that a SELECT query uses an index when available."""
        # Create an index on the age column
        schema_manager.execute_create_index(
            {
                "type": "CREATE_INDEX",
                "index_name": "idx_users_age",
                "table": "users",
                "column": "age",
            }
        )

        # Execute a query that should use the index
        plan = {
            "type": "SELECT",
            "table": "users",
            "columns": ["id", "username", "age"],
            "condition": "age = 30",
        }

        start_time = time.time()
        result = execution_engine.execute(plan)
        index_time = time.time() - start_time

        # Verify results
        assert result["status"] == "success"
        rows = result.get("rows", [])
        columns = result.get("columns", [])
        # Check that all returned rows have age=30
        age_index = columns.index("age")
        assert all(row[age_index] == 30 for row in rows)

        # Check that the optimizer used an index scan
        assert "scan_type" in plan
        assert plan["scan_type"] == "INDEX_SCAN"

        # Assert that query execution was fast (index should help performance)
        # Just ensure it completes within reasonable time
        assert index_time < 1.0, f"Index scan took too long: {index_time:.4f}s"

    def test_select_without_index(
        self, execution_engine, setup_index_table, cleanup_index_table
    ):  # pylint: disable=unused-argument
        """Test SELECT performance without index."""
        # Execute a query without an index
        plan = {
            "type": "SELECT",
            "table": "users",
            "columns": ["id", "username", "email"],
            "condition": "username = 'user100'",
        }

        start_time = time.time()
        result = execution_engine.execute(plan)
        no_index_time = time.time() - start_time

        # Verify results
        assert result["status"] == "success"
        rows = result.get("rows", [])
        columns = result.get("columns", [])
        assert len(rows) == 1
        username_index = columns.index("username")
        assert rows[0][username_index] == "user100"

        # Check that this was a full table scan
        assert "scan_type" in plan
        assert plan["scan_type"] == "FULL_SCAN"

        # Assert that query execution completes (no specific performance requirement for full scan)
        assert no_index_time >= 0, "Query execution time should be non-negative"

    def test_index_range_scan(
        self, schema_manager, execution_engine, setup_index_table, cleanup_index_table
    ):  # pylint: disable=unused-argument
        """Test index usage for range queries."""
        # Create an index on the age column
        schema_manager.execute_create_index(
            {
                "type": "CREATE_INDEX",
                "index_name": "idx_users_age",
                "table": "users",
                "column": "age",
            }
        )

        # Execute a range query
        plan = {
            "type": "SELECT",
            "table": "users",
            "columns": ["id", "username", "age"],
            "condition": "age >= 25 AND age <= 30",
        }

        result = execution_engine.execute(plan)

        # Verify results
        assert result["status"] == "success"
        data = result.get("data", [])
        # Check that all returned rows are in the age range
        assert all(25 <= row["age"] <= 30 for row in data)

        # Verify index was used for range scan
        assert "scan_type" in plan
        assert plan["scan_type"] == "INDEX_RANGE_SCAN"

    def test_unique_index_constraint(
        self, schema_manager, catalog_manager, setup_index_table, cleanup_index_table
    ):
        """Test that a UNIQUE index enforces uniqueness."""
        # Create a unique index on the email
        schema_manager.execute_create_index(
            {
                "type": "CREATE_INDEX",
                "index_name": "idx_users_email_unique",
                "table": "users",
                "column": "email",
                "unique": True,
            }
        )

        # Try to insert a duplicate email
        result = catalog_manager.insert_record(
            "users",
            {
                "id": 1001,
                "username": "newuser",
                "age": 25,
                "email": "user1@example.com",  # This email already exists
            },
        )

        # Verify the insert was rejected due to unique constraint
        assert "error" in result
        assert (
            "unique constraint" in result["error"].lower()
            or "duplicate" in result["error"].lower()
        )
