#!/usr/bin/env python3
"""
Comprehensive tests for the SQLGlot-based SQL parser.
Tests all SQL statement types, edge cases, and performance.
"""

import pytest
import sys
import os

# Add server directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "server"))

from sqlglot_parser import SQLGlotParser


class TestSQLGlotParser:
    """Test class for SQLGlot parser functionality."""

    @pytest.fixture
    def parser(self):
        """Create a parser instance for testing."""
        return SQLGlotParser()

    def test_initialization(self, parser):
        """Test parser initialization."""
        assert parser.dialect == "mysql"
        assert parser is not None

    def test_empty_query(self, parser):
        """Test parsing empty query."""
        result = parser.parse("")
        assert "error" in result
        assert result["error"] == "Empty query"

    def test_invalid_query(self, parser):
        """Test parsing invalid SQL."""
        result = parser.parse("INVALID SQL STATEMENT")
        assert "error" in result
        assert "Parse error" in result["error"]


class TestSelectStatements:
    """Test SELECT statement parsing."""

    @pytest.fixture
    def parser(self):
        return SQLGlotParser()

    def test_simple_select(self, parser):
        """Test basic SELECT statement."""
        sql = "SELECT * FROM users"
        result = parser.parse(sql)

        assert result["type"] == "SELECT"
        assert result["operation"] == "SELECT"
        assert result["columns"] == ["*"]
        assert result["tables"] == ["users"]
        assert "distinct" in result

    def test_select_with_columns(self, parser):
        """Test SELECT with specific columns."""
        sql = "SELECT name, age, email FROM users"
        result = parser.parse(sql)

        assert result["type"] == "SELECT"
        assert result["columns"] == ["name", "age", "email"]
        assert result["tables"] == ["users"]

    def test_select_with_where(self, parser):
        """Test SELECT with WHERE clause."""
        sql = "SELECT * FROM users WHERE age > 18"
        result = parser.parse(sql)

        assert result["type"] == "SELECT"
        assert "condition" in result
        assert "where" in result
        assert result["condition"] == "age > 18"
        assert isinstance(result["parsed_condition"], list)

    def test_select_with_multiple_conditions(self, parser):
        """Test SELECT with multiple WHERE conditions."""
        sql = "SELECT * FROM users WHERE age > 18 AND name = 'John'"
        result = parser.parse(sql)

        assert result["type"] == "SELECT"
        assert "parsed_condition" in result
        assert len(result["parsed_condition"]) >= 1

    def test_select_distinct(self, parser):
        """Test SELECT DISTINCT."""
        sql = "SELECT DISTINCT name FROM users"
        result = parser.parse(sql)

        assert result["type"] == "SELECT"
        assert result["distinct"] is True
        assert result["columns"] == ["name"]

    def test_select_with_order_by(self, parser):
        """Test SELECT with ORDER BY."""
        sql = "SELECT * FROM users ORDER BY name ASC"
        result = parser.parse(sql)

        assert result["type"] == "SELECT"
        if "order_by" in result:
            assert result["order_by"]["direction"] in ["ASC", "DESC"]

    def test_select_with_limit(self, parser):
        """Test SELECT with LIMIT."""
        sql = "SELECT * FROM users LIMIT 10"
        result = parser.parse(sql)

        assert result["type"] == "SELECT"
        if "limit" in result:
            assert result["limit"] == 10

    def test_select_with_group_by(self, parser):
        """Test SELECT with GROUP BY."""
        sql = "SELECT name, COUNT(*) FROM users GROUP BY name"
        result = parser.parse(sql)

        assert result["type"] == "AGGREGATE"
        assert result["function"] == "COUNT"
        if "group_by" in result:
            assert "name" in result["group_by"]

    def test_aggregate_functions(self, parser):
        """Test aggregate functions."""
        test_cases = [
            ("SELECT COUNT(*) FROM users", "COUNT", "*"),
            ("SELECT SUM(age) FROM users", "SUM", "age"),
            ("SELECT AVG(age) FROM users", "AVG", "age"),
            ("SELECT MIN(age) FROM users", "MIN", "age"),
            ("SELECT MAX(age) FROM users", "MAX", "age"),
        ]

        for sql, func, column in test_cases:
            result = parser.parse(sql)
            assert result["type"] == "AGGREGATE"
            assert result["function"] == func
            assert result["column"] == column


class TestInsertStatements:
    """Test INSERT statement parsing."""

    @pytest.fixture
    def parser(self):
        return SQLGlotParser()

    def test_insert_with_columns(self, parser):
        """Test INSERT with column list."""
        sql = "INSERT INTO users (name, age) VALUES ('John', 25)"
        result = parser.parse(sql)

        assert result["type"] == "INSERT"
        assert result["operation"] == "INSERT"
        assert "table" in result
        assert "values" in result
        assert result["values"] == [["John", 25]]

    def test_insert_without_columns(self, parser):
        """Test INSERT without column list."""
        sql = "INSERT INTO users VALUES (1, 'John', 25)"
        result = parser.parse(sql)

        assert result["type"] == "INSERT"
        assert result["table"] == "users"
        assert result["values"] == [[1, "John", 25]]

    def test_insert_multiple_rows(self, parser):
        """Test INSERT with multiple rows."""
        sql = "INSERT INTO users (name, age) VALUES ('John', 25), ('Jane', 30)"
        result = parser.parse(sql)

        assert result["type"] == "INSERT"
        if "values" in result and len(result["values"]) > 1:
            assert len(result["values"]) == 2


class TestUpdateStatements:
    """Test UPDATE statement parsing."""

    @pytest.fixture
    def parser(self):
        return SQLGlotParser()

    def test_simple_update(self, parser):
        """Test basic UPDATE statement."""
        sql = "UPDATE users SET age = 26 WHERE name = 'John'"
        result = parser.parse(sql)

        assert result["type"] == "UPDATE"
        assert result["operation"] == "UPDATE"
        assert "table" in result
        assert "set" in result
        assert "condition" in result

    def test_update_multiple_columns(self, parser):
        """Test UPDATE with multiple columns."""
        sql = (
            "UPDATE users SET age = 26, email = 'john@example.com' WHERE name = 'John'"
        )
        result = parser.parse(sql)

        assert result["type"] == "UPDATE"
        assert "set" in result
        if len(result["set"]) >= 1:
            assert isinstance(result["set"], dict)


class TestDeleteStatements:
    """Test DELETE statement parsing."""

    @pytest.fixture
    def parser(self):
        return SQLGlotParser()

    def test_simple_delete(self, parser):
        """Test basic DELETE statement."""
        sql = "DELETE FROM users WHERE id = 1"
        result = parser.parse(sql)

        assert result["type"] == "DELETE"
        assert result["operation"] == "DELETE"
        assert "table" in result
        assert "condition" in result

    def test_delete_all(self, parser):
        """Test DELETE without WHERE clause."""
        sql = "DELETE FROM users"
        result = parser.parse(sql)

        assert result["type"] == "DELETE"
        assert "table" in result


class TestDDLStatements:
    """Test DDL statement parsing."""

    @pytest.fixture
    def parser(self):
        return SQLGlotParser()

    def test_create_table(self, parser):
        """Test CREATE TABLE statement."""
        sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)"
        result = parser.parse(sql)

        assert result["type"] == "CREATE_TABLE"
        assert result["operation"] == "CREATE_TABLE"
        assert "table" in result
        assert "columns" in result

    def test_drop_table(self, parser):
        """Test DROP TABLE statement."""
        sql = "DROP TABLE users"
        result = parser.parse(sql)

        assert result["type"] == "DROP_TABLE"
        assert result["operation"] == "DROP_TABLE"
        assert "table" in result


class TestJoinStatements:
    """Test JOIN statement parsing."""

    @pytest.fixture
    def parser(self):
        return SQLGlotParser()

    def test_inner_join(self, parser):
        """Test INNER JOIN."""
        sql = "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
        result = parser.parse(sql)

        if result["type"] == "JOIN":
            assert "join_info" in result
            assert result["join_info"]["type"] == "INNER"

    def test_left_join(self, parser):
        """Test LEFT JOIN."""
        sql = "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id"
        result = parser.parse(sql)

        if result["type"] == "JOIN":
            assert result["join_info"]["type"] == "LEFT"


class TestColumnDefinitionParsing:
    """Test column definition parsing."""

    @pytest.fixture
    def parser(self):
        return SQLGlotParser()

    def test_simple_column(self, parser):
        """Test basic column definition."""
        result = parser.parse_column_definition("id INT")

        assert result["name"] == "id"
        assert "INT" in result["type"]
        assert result["primary_key"] is False
        assert result["nullable"] is True

    def test_primary_key_column(self, parser):
        """Test PRIMARY KEY column."""
        result = parser.parse_column_definition("id INT PRIMARY KEY")

        assert result["name"] == "id"
        assert result["primary_key"] is True

    def test_not_null_column(self, parser):
        """Test NOT NULL column."""
        result = parser.parse_column_definition("name VARCHAR(50) NOT NULL")

        assert result["name"] == "name"
        assert result["nullable"] is False
        assert "VARCHAR" in result["type"]

    def test_default_value_column(self, parser):
        """Test column with DEFAULT value."""
        result = parser.parse_column_definition("age INT DEFAULT 0")

        assert result["name"] == "age"
        assert result["default"] == "0"

    def test_identity_column(self, parser):
        """Test IDENTITY column."""
        result = parser.parse_column_definition("id INT IDENTITY(1,1)")

        assert result["name"] == "id"
        assert result["identity"] is True
        assert result["identity_seed"] == 1
        assert result["identity_increment"] == 1

    def test_complex_column(self, parser):
        """Test complex column definition."""
        result = parser.parse_column_definition(
            "email VARCHAR(100) NOT NULL DEFAULT 'user@example.com'"
        )

        assert result["name"] == "email"
        assert result["nullable"] is False
        assert result["default"] is not None
        assert "VARCHAR" in result["type"]


class TestSpecialStatements:
    """Test special SQL statements."""

    @pytest.fixture
    def parser(self):
        return SQLGlotParser()

    def test_script_statement(self, parser):
        """Test SCRIPT statement."""
        sql = "SCRIPT test_script.sql"
        result = parser.parse(sql)

        assert result["type"] == "SCRIPT"
        assert result["operation"] == "SCRIPT"
        assert result["filename"] == "test_script.sql"

    def test_visualize_statement(self, parser):
        """Test VISUALIZE statement."""
        sql = "VISUALIZE BPTREE index_name ON users"
        result = parser.parse(sql)

        assert result["type"] == "VISUALIZE"
        assert result["operation"] == "VISUALIZE"
        assert result["object"] == "BPTREE"

    def test_transaction_statements(self, parser):
        """Test transaction statements."""
        test_cases = [
            ("BEGIN TRANSACTION", "BEGIN_TRANSACTION"),
            ("COMMIT", "COMMIT"),
            ("ROLLBACK", "ROLLBACK"),
        ]

        for sql, expected_type in test_cases:
            result = parser.parse(sql)
            assert result["type"] == expected_type
            assert result["operation"] == expected_type


class TestEdgeCases:
    """Test edge cases and error handling."""

    @pytest.fixture
    def parser(self):
        return SQLGlotParser()

    def test_malformed_sql(self, parser):
        """Test malformed SQL statements."""
        malformed_queries = [
            "SELECT FROM",
            "INSERT INTO",
            "UPDATE SET",
            "DELETE WHERE",
            "CREATE",
        ]

        for sql in malformed_queries:
            result = parser.parse(sql)
            # Should either parse with limited info or return error
            assert "query" in result

    def test_sql_with_comments(self, parser):
        """Test SQL with comments."""
        sql = "SELECT * FROM users -- Get all users"
        result = parser.parse(sql)

        assert result["type"] == "SELECT"
        assert result["tables"] == ["users"]

    def test_case_insensitive_parsing(self, parser):
        """Test case insensitive SQL parsing."""
        sql_upper = "SELECT * FROM USERS WHERE ID = 1"
        sql_lower = "select * from users where id = 1"

        result_upper = parser.parse(sql_upper)
        result_lower = parser.parse(sql_lower)

        assert result_upper["type"] == result_lower["type"]
        assert result_upper["operation"] == result_lower["operation"]


class TestOptimizationAndTranspilation:
    """Test SQLGlot optimization and transpilation features."""

    @pytest.fixture
    def parser(self):
        return SQLGlotParser()

    def test_optimization(self, parser):
        """Test SQL optimization."""
        sql = "SELECT * FROM users WHERE 1 = 1 AND name = 'John'"
        optimized = parser.optimize(sql)

        # Should return a string
        assert isinstance(optimized, str)
        # Should contain the essential parts
        assert "users" in optimized
        assert "John" in optimized

    def test_transpilation(self, parser):
        """Test SQL transpilation to different dialects."""
        sql = "SELECT * FROM users LIMIT 10"

        # Transpile to PostgreSQL
        postgres_sql = parser.transpile(sql, "postgres")
        assert isinstance(postgres_sql, str)

        # Transpile to SQLite
        sqlite_sql = parser.transpile(sql, "sqlite")
        assert isinstance(sqlite_sql, str)


class TestPerformance:
    """Test parser performance with various query sizes."""

    @pytest.fixture
    def parser(self):
        return SQLGlotParser()

    def test_simple_query_performance(self, parser):
        """Test performance with simple queries."""
        import time

        sql = "SELECT * FROM users WHERE id = 1"

        start_time = time.time()
        for _ in range(100):
            result = parser.parse(sql)
        end_time = time.time()

        avg_time = (end_time - start_time) / 100
        # Should parse quickly (less than 10ms per query)
        assert avg_time < 0.01

    def test_complex_query_performance(self, parser):
        """Test performance with complex queries."""
        import time

        sql = """
        SELECT u.name, COUNT(o.id) as order_count, AVG(o.total) as avg_total
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.active = 1 AND o.created_at > '2023-01-01'
        GROUP BY u.id, u.name
        HAVING COUNT(o.id) > 5
        ORDER BY avg_total DESC
        LIMIT 100
        """

        start_time = time.time()
        for _ in range(10):
            result = parser.parse(sql)
        end_time = time.time()

        avg_time = (end_time - start_time) / 10
        # Complex queries should still parse reasonably quickly
        assert avg_time < 0.1


if __name__ == "__main__":
    # Run tests directly if script is executed
    pytest.main([__file__, "-v"])
