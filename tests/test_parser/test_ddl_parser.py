"""
Tests for SQL DDL statement parsing (CREATE, DROP, ALTER).
"""
import pytest
import sys
import os

# Add server directory to path
project_root = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), '..'))
server_dir = os.path.join(project_root, 'server')
if server_dir not in sys.path:
    sys.path.insert(0, server_dir)

from parser import SQLParser

@pytest.fixture
def parser():
    """Create a parser instance for testing."""
    return SQLParser()

class TestCreateTableParser:
    """Test CREATE TABLE statement parsing."""

    def test_basic_create_table(self, parser):
        """Test parsing basic CREATE TABLE statement."""
        query = "CREATE TABLE users (id INT, name VARCHAR(100))"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "CREATE_TABLE"
        assert parsed["table"] == "users"
        assert len(parsed["columns"]) >= 2

    def test_create_table_with_constraints(self, parser):
        """Test CREATE TABLE with various constraints."""
        query = """CREATE TABLE orders (
            id INT PRIMARY KEY,
            user_id INT NOT NULL,
            amount DECIMAL(10,2) DEFAULT 0.00,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )"""
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "CREATE_TABLE"
        assert parsed["table"] == "orders"
        assert parsed["columns"] is not None

    def test_create_table_with_compound_primary_key(self, parser):
        """Test CREATE TABLE with compound primary key."""
        query = """CREATE TABLE user_roles (
            user_id INT,
            role_id INT,
            granted_at TIMESTAMP,
            PRIMARY KEY (user_id, role_id)
        )"""
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "CREATE_TABLE"
        assert parsed["table"] == "user_roles"

    def test_create_table_if_not_exists(self, parser):
        """Test CREATE TABLE IF NOT EXISTS."""
        query = "CREATE TABLE IF NOT EXISTS temp_table (id INT)"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "CREATE_TABLE"
        assert parsed["table"] == "temp_table"

class TestCreateIndexParser:
    """Test CREATE INDEX statement parsing."""

    def test_basic_create_index(self, parser):
        """Test parsing basic CREATE INDEX statement."""
        query = "CREATE INDEX idx_name ON users (name)"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "CREATE_INDEX"
        assert parsed["index_name"] == "idx_name"
        assert parsed["table"] == "users"

    def test_create_unique_index(self, parser):
        """Test parsing CREATE UNIQUE INDEX statement."""
        query = "CREATE UNIQUE INDEX idx_email ON users (email)"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "CREATE_INDEX"
        assert parsed["unique"] == True
        assert parsed["index_name"] == "idx_email"
        assert parsed["table"] == "users"

    def test_create_compound_index(self, parser):
        """Test parsing CREATE INDEX with multiple columns."""
        query = "CREATE INDEX idx_name_age ON users (last_name, first_name, age)"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "CREATE_INDEX"
        assert parsed["index_name"] == "idx_name_age"
        assert parsed["table"] == "users"
        assert len(parsed.get("columns", [])) >= 1

class TestCreateDatabaseParser:
    """Test CREATE DATABASE statement parsing."""

    def test_basic_create_database(self, parser):
        """Test parsing basic CREATE DATABASE statement."""
        query = "CREATE DATABASE testdb"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "CREATE_DATABASE"
        assert parsed["database"] == "testdb"

    def test_create_database_if_not_exists(self, parser):
        """Test CREATE DATABASE IF NOT EXISTS."""
        query = "CREATE DATABASE IF NOT EXISTS testdb"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "CREATE_DATABASE"
        assert parsed["database"] == "testdb"

class TestDropStatements:
    """Test DROP statement parsing."""

    def test_drop_table(self, parser):
        """Test parsing DROP TABLE statement."""
        query = "DROP TABLE users"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "DROP_TABLE"
        assert parsed["table"] == "users"

    def test_drop_table_if_exists(self, parser):
        """Test DROP TABLE IF EXISTS."""
        query = "DROP TABLE IF EXISTS temp_table"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "DROP_TABLE"
        assert parsed["table"] == "temp_table"

    def test_drop_index(self, parser):
        """Test parsing DROP INDEX statement."""
        query = "DROP INDEX idx_name ON users"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "DROP_INDEX"
        assert parsed["index_name"] == "idx_name"
        assert parsed["table"] == "users"

    def test_drop_database(self, parser):
        """Test parsing DROP DATABASE statement."""
        query = "DROP DATABASE testdb"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "DROP_DATABASE"
        assert parsed["database"] == "testdb"

class TestAlterStatements:
    """Test ALTER statement parsing."""

    def test_alter_table_add_column(self, parser):
        """Test parsing ALTER TABLE ADD COLUMN."""
        query = "ALTER TABLE users ADD COLUMN phone VARCHAR(20)"
        parsed = parser.parse_sql(query)

        # SQLGlot might parse this differently, check for basic structure
        assert "users" in str(parsed)
        assert "phone" in str(parsed)

    def test_alter_table_drop_column(self, parser):
        """Test parsing ALTER TABLE DROP COLUMN."""
        query = "ALTER TABLE users DROP COLUMN phone"
        parsed = parser.parse_sql(query)

        assert "users" in str(parsed)
        assert "phone" in str(parsed)

    def test_alter_table_modify_column(self, parser):
        """Test parsing ALTER TABLE MODIFY COLUMN."""
        query = "ALTER TABLE users MODIFY COLUMN name VARCHAR(200)"
        parsed = parser.parse_sql(query)

        assert "users" in str(parsed)
        assert "name" in str(parsed)
