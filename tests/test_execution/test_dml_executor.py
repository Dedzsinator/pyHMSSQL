"""
Tests for DML execution operations (INSERT, UPDATE, DELETE).
"""
import pytest

class TestInsertExecution:
    """Test INSERT statement execution."""

    def test_insert_basic_record(self, execution_engine, catalog_manager, test_table):
        """Test inserting a basic record."""
        plan = {
            "type": "INSERT",
            "table": test_table,
            "columns": ["id", "name", "email", "age"],
            "values": [[4, "Alex Wilson", "alex@example.com", 35]],
            "session_id": "test_session"
        }

        result = execution_engine.execute(plan)

        assert result["status"] == "success"

        # Verify record was inserted
        records = catalog_manager.query_with_condition(
            test_table, [{"column": "id", "operator": "=", "value": 4}]
        )
        assert len(records) == 1
        assert records[0]["name"] == "Alex Wilson"

    def test_insert_duplicate_primary_key(self, execution_engine, test_table):
        """Test inserting a record with a duplicate primary key."""
        plan = {
            "type": "INSERT",
            "table": test_table,
            "columns": ["id", "name", "email", "age"],
            "values": [[1, "Duplicate", "duplicate@example.com", 50]],
            "session_id": "test_session"
        }

        result = execution_engine.execute(plan)

        assert result["status"] == "error"
        assert "duplicate" in result["error"].lower() or "primary key" in result["error"].lower()

    def test_insert_invalid_column(self, execution_engine, test_table):
        """Test inserting with an invalid column."""
        plan = {
            "type": "INSERT",
            "table": test_table,
            "columns": ["id", "name", "invalid_column"],
            "values": [[5, "Test User", "test"]],
            "session_id": "test_session"
        }

        result = execution_engine.execute(plan)
        assert result["status"] == "error"

class TestUpdateExecution:
    """Test UPDATE statement execution."""

    def test_update_basic(self, execution_engine, catalog_manager, test_table):
        """Test basic update operation."""
        plan = {
            "type": "UPDATE",
            "table": test_table,
            "set": [("name", "'Updated Name'"), ("age", 45)],
            "condition": "id = 1",
            "session_id": "test_session"
        }

        result = execution_engine.execute(plan)

        assert result["status"] == "success"

        # Verify record was updated
        records = catalog_manager.query_with_condition(
            test_table, [{"column": "id", "operator": "=", "value": 1}]
        )
        assert len(records) == 1
        assert records[0]["name"] == "Updated Name"
        assert records[0]["age"] == 45

    def test_update_multiple_records(self, execution_engine, catalog_manager, test_table):
        """Test updating multiple records."""
        plan = {
            "type": "UPDATE",
            "table": test_table,
            "set": [("age", 50)],
            "condition": "age < 40",
            "session_id": "test_session"
        }

        result = execution_engine.execute(plan)

        assert result["status"] == "success"
        assert "count" in result
        assert result["count"] > 0

        # Verify records were updated
        records = catalog_manager.query_with_condition(
            test_table, [{"column": "age", "operator": "=", "value": 50}]
        )
        assert len(records) > 0

class TestDeleteExecution:
    """Test DELETE statement execution."""

    def test_delete_single_record(self, execution_engine, catalog_manager, test_table):
        """Test deleting a single record."""
        plan = {
            "type": "DELETE",
            "table": test_table,
            "condition": "id = 2",
            "session_id": "test_session"
        }

        result = execution_engine.execute(plan)

        assert result["status"] == "success"
        assert result["count"] == 1

        # Verify record was deleted
        records = catalog_manager.query_with_condition(
            test_table, [{"column": "id", "operator": "=", "value": 2}]
        )
        assert len(records) == 0

    def test_delete_multiple_records(self, execution_engine, catalog_manager, test_table):
        """Test deleting multiple records."""
        plan = {
            "type": "DELETE",
            "table": test_table,
            "condition": "age >= 30",
            "session_id": "test_session"
        }

        result = execution_engine.execute(plan)

        assert result["status"] == "success"

        # Verify records were deleted
        records = catalog_manager.query_with_condition(
            test_table, [{"column": "age", "operator": ">=", "value": 30}]
        )
        assert len(records) == 0

    def test_delete_all_records(self, execution_engine, catalog_manager, test_table):
        """Test deleting all records."""
        plan = {
            "type": "DELETE",
            "table": test_table,
            "session_id": "test_session"
        }

        result = execution_engine.execute(plan)

        assert result["status"] == "success"

        # Verify all records were deleted
        records = catalog_manager.query_with_condition(test_table, [])
        assert len(records) == 0