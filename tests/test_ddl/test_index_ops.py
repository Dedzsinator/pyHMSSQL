"""
Tests for index operations (CREATE INDEX, DROP INDEX).
"""

import pytest
import os


class TestCreateIndex:
    """Test CREATE INDEX operations."""

    def test_create_basic_index(self, schema_manager, catalog_manager, test_table):
        """Test creating a basic index."""
        plan = {
            "type": "CREATE_INDEX",
            "index_name": "idx_customer_name",
            "table": test_table,
            "column": "name",
        }

        result = schema_manager.execute_create_index(plan)

        assert result["status"] == "success"

        # Verify index was created in catalog
        indexes = catalog_manager.get_indexes_for_table(test_table)
        assert "idx_customer_name" in indexes

        # Verify index file was created
        db_name = catalog_manager.get_current_database()
        index_file = os.path.join(
            catalog_manager.indexes_dir, f"{db_name}_{test_table}_name.idx"
        )
        assert os.path.exists(index_file)

    def test_create_unique_index(self, schema_manager, catalog_manager, test_table):
        """Test creating a unique index."""
        plan = {
            "type": "CREATE_INDEX",
            "index_name": "idx_customer_email",
            "table": test_table,
            "column": "email",
            "unique": True,
        }

        result = schema_manager.execute_create_index(plan)

        assert result["status"] == "success"

        # Verify index was created as unique
        indexes = catalog_manager.get_indexes_for_table(test_table)
        assert indexes["idx_customer_email"]["unique"] is True

    def test_create_duplicate_index(self, schema_manager, catalog_manager, test_table):
        """Test creating a duplicate index."""
        # First create the index
        plan1 = {
            "type": "CREATE_INDEX",
            "index_name": "idx_customer_age",
            "table": test_table,
            "column": "age",
        }

        schema_manager.execute_create_index(plan1)

        # Try to create it again
        plan2 = {
            "type": "CREATE_INDEX",
            "index_name": "idx_customer_age",
            "table": test_table,
            "column": "age",
        }

        result = schema_manager.execute_create_index(plan2)

        assert result["status"] == "error"
        assert "already exists" in result["error"].lower()


class TestDropIndex:
    """Test DROP INDEX operations."""

    def test_drop_existing_index(self, schema_manager, catalog_manager, test_table):
        """Test dropping an existing index."""
        # First create the index
        create_plan = {
            "type": "CREATE_INDEX",
            "index_name": "idx_to_drop",
            "table": test_table,
            "column": "id",
        }

        schema_manager.execute_create_index(create_plan)

        # Then drop it
        drop_plan = {
            "type": "DROP_INDEX",
            "index_name": "idx_to_drop",
            "table": test_table,
        }

        result = schema_manager.execute_drop_index(drop_plan)

        assert result["status"] == "success"

        # Verify index was removed
        indexes = catalog_manager.get_indexes_for_table(test_table)
        assert "idx_to_drop" not in indexes

    def test_drop_nonexistent_index(self, schema_manager, test_table):
        """Test dropping a non-existent index."""
        plan = {
            "type": "DROP_INDEX",
            "index_name": "idx_not_exists",
            "table": test_table,
        }

        result = schema_manager.execute_drop_index(plan)

        assert result["status"] == "error"
        assert (
            "not found" in result["error"].lower()
            or "does not exist" in result["error"].lower()
        )
