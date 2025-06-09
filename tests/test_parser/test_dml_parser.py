"""
Tests for SQL DML statement parsing (SELECT, INSERT, UPDATE, DELETE).
"""

import pytest
import sys
import os

# Add server directory to path
project_root = os.path.abspath(
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "..")
)
server_dir = os.path.join(project_root, "server")
if server_dir not in sys.path:
    sys.path.insert(0, server_dir)

# Now import directly from the modules
from parser import SQLParser  # Direct import instead of from server.parser


@pytest.fixture
def parser():
    """Create a parser instance for testing."""
    return SQLParser()


class TestSelectParser:
    """Test SELECT statement parsing."""

    def test_basic_select(self, parser):
        """Test parsing of basic SELECT statements."""
        query = "SELECT id, name FROM customers"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert parsed["columns"] == ["id", "name"]
        # Fix: accept uppercase table names in parser output
        assert parsed["tables"][0].lower() == "customers"

    def test_select_all_columns(self, parser):
        """Test SELECT * parsing."""
        query = "SELECT * FROM products"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert parsed["columns"] == ["*"]
        assert "products" in [t.lower() for t in parsed["tables"]]

    def test_select_with_table_alias(self, parser):
        """Test SELECT with table alias."""
        query = "SELECT u.id, u.name FROM users u"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert "users" in [t.lower() for t in parsed["tables"]]

    def test_select_with_column_alias(self, parser):
        """Test SELECT with column aliases."""
        query = "SELECT id AS user_id, name AS full_name FROM users"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert len(parsed["columns"]) == 2

    def test_select_with_where(self, parser):
        """Test parsing SELECT with WHERE clause."""
        query = "SELECT id, name FROM customers WHERE age > 30"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert parsed["columns"] == ["id", "name"]
        assert parsed["tables"] == ["customers"]
        assert "age > 30" in parsed["condition"]

    def test_select_with_complex_where(self, parser):
        """Test SELECT with complex WHERE conditions."""
        query = "SELECT * FROM users WHERE age > 18 AND (status = 'active' OR premium = true)"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert "condition" in parsed

    def test_select_with_in_clause(self, parser):
        """Test SELECT with IN clause."""
        query = "SELECT * FROM users WHERE id IN (1, 2, 3, 4, 5)"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert "IN" in str(parsed["condition"])

    def test_select_with_between(self, parser):
        """Test SELECT with BETWEEN clause."""
        query = "SELECT * FROM orders WHERE amount BETWEEN 100 AND 500"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert "BETWEEN" in str(parsed["condition"])

    def test_select_with_like(self, parser):
        """Test SELECT with LIKE clause."""
        query = "SELECT * FROM users WHERE name LIKE 'John%'"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert "LIKE" in str(parsed["condition"])

    def test_select_with_is_null(self, parser):
        """Test SELECT with IS NULL."""
        query = "SELECT * FROM users WHERE email IS NULL"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert "NULL" in str(parsed["condition"])

    def test_select_with_order_by(self, parser):
        """Test parsing SELECT with ORDER BY clause."""
        query = "SELECT * FROM customers ORDER BY age DESC"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert parsed["columns"] == ["*"]
        assert parsed["tables"] == ["customers"]
        assert parsed["order_by"]["column"] == "age"
        assert parsed["order_by"]["direction"] == "DESC"

    def test_select_with_order_by_multiple(self, parser):
        """Test SELECT with multiple ORDER BY columns."""
        query = "SELECT * FROM users ORDER BY last_name ASC, first_name ASC, age DESC"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        # Should contain order by information
        assert "order_by" in parsed

    def test_select_with_limit(self, parser):
        """Test parsing SELECT with LIMIT clause."""
        query = "SELECT * FROM customers LIMIT 10"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert parsed["columns"] == ["*"]
        assert parsed["tables"] == ["customers"]
        assert parsed["limit"] == 10

    def test_select_with_limit_offset(self, parser):
        """Test SELECT with LIMIT and OFFSET."""
        query = "SELECT * FROM users LIMIT 20 OFFSET 10"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert parsed["limit"] == 20
        assert "offset" in parsed

    def test_select_distinct(self, parser):
        """Test SELECT DISTINCT parsing."""
        query = "SELECT DISTINCT country FROM users"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "DISTINCT" or parsed["distinct"] == True
        if parsed["type"] == "DISTINCT":
            assert parsed["column"] == "country"

    def test_select_with_aggregate(self, parser):
        """Test parsing SELECT with aggregate function."""
        query = "SELECT COUNT(*) FROM customers"
        parsed = parser.parse_sql(query)

        # This should be detected as an aggregate query
        assert parsed["type"] == "AGGREGATE" or "COUNT" in str(parsed)

    def test_select_with_multiple_aggregates(self, parser):
        """Test SELECT with multiple aggregate functions."""
        query = "SELECT COUNT(*), AVG(age), MAX(salary), MIN(salary) FROM employees"
        parsed = parser.parse_sql(query)

        # Should contain aggregate functions
        query_str = str(parsed)
        assert any(func in query_str for func in ["COUNT", "AVG", "MAX", "MIN"])

    def test_select_with_group_by(self, parser):
        """Test SELECT with GROUP BY."""
        query = "SELECT department, COUNT(*) FROM employees GROUP BY department"
        parsed = parser.parse_sql(query)

        assert (
            parsed["type"] == "AGGREGATE"
        )  # SQLGlot correctly identifies this as an aggregate operation
        assert "group_by" in parsed or "GROUP BY" in str(parsed)

    def test_select_with_having(self, parser):
        """Test SELECT with HAVING clause."""
        query = "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5"
        parsed = parser.parse_sql(query)

        assert (
            parsed["type"] == "AGGREGATE"
        )  # SQLGlot correctly identifies this as an aggregate operation
        assert "HAVING" in str(parsed)


class TestInsertParser:
    """Test INSERT statement parsing."""

    def test_basic_insert(self, parser):
        """Test parsing of basic INSERT statement."""
        query = "INSERT INTO customers (id, name, email) VALUES (4, 'Alice', 'alice@example.com')"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "INSERT"
        assert parsed["table"] == "customers"
        assert parsed["columns"] == ["id", "name", "email"]
        assert len(parsed["values"]) == 1
        assert len(parsed["values"][0]) == 3

    def test_insert_without_columns(self, parser):
        """Test INSERT without specifying columns."""
        query = "INSERT INTO users VALUES (1, 'John', 'john@example.com', 25)"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "INSERT"
        assert parsed["table"] == "users"
        assert len(parsed["values"]) == 1

    def test_multi_row_insert(self, parser):
        """Test parsing INSERT with multiple rows."""
        query = "INSERT INTO customers (id, name) VALUES (5, 'Dave'), (6, 'Eve')"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "INSERT"
        assert parsed["table"] == "customers"
        assert parsed["columns"] == ["id", "name"]
        assert len(parsed["values"]) == 2

    def test_insert_with_null_values(self, parser):
        """Test INSERT with NULL values."""
        query = "INSERT INTO users (id, name, email) VALUES (1, 'John', NULL)"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "INSERT"
        assert parsed["table"] == "users"

    def test_insert_with_default_values(self, parser):
        """Test INSERT with DEFAULT values."""
        query = "INSERT INTO users (id, name, created_at) VALUES (1, 'John', DEFAULT)"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "INSERT"
        assert parsed["table"] == "users"

    def test_insert_select(self, parser):
        """Test INSERT with SELECT subquery."""
        query = "INSERT INTO archive_users SELECT * FROM users WHERE created_at < '2020-01-01'"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "INSERT"
        assert parsed["table"] == "archive_users"


class TestUpdateParser:
    """Test UPDATE statement parsing."""

    def test_basic_update(self, parser):
        """Test parsing of basic UPDATE statement."""
        query = "UPDATE customers SET email = 'updated@example.com' WHERE id = 1"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "UPDATE"
        assert parsed["table"] == "customers"
        # Check if set is a list of tuples or a dict
        if isinstance(parsed["set"], dict):
            assert parsed["set"]["email"] == "updated@example.com"
        else:
            assert ("email", "updated@example.com") in parsed["set"]

    def test_multi_column_update(self, parser):
        """Test parsing UPDATE with multiple columns."""
        query = "UPDATE customers SET name = 'Updated Name', age = 31 WHERE id = 1"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "UPDATE"
        assert parsed["table"] == "customers"
        # Check if set is a list of tuples or a dict
        if isinstance(parsed["set"], list):
            set_dict = dict(parsed["set"])
            assert "name" in set_dict
            assert "age" in set_dict
        else:
            assert "name" in parsed["set"]
            assert "age" in parsed["set"]

    def test_update_with_expressions(self, parser):
        """Test UPDATE with expressions in SET clause."""
        query = "UPDATE products SET price = price * 1.1, updated_at = NOW()"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "UPDATE"
        assert parsed["table"] == "products"

    def test_update_with_complex_where(self, parser):
        """Test UPDATE with complex WHERE clause."""
        query = "UPDATE orders SET status = 'shipped' WHERE user_id IN (1, 2, 3) AND amount > 100"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "UPDATE"
        assert parsed["table"] == "orders"
        assert "condition" in parsed

    def test_update_with_join(self, parser):
        """Test UPDATE with JOIN (MySQL style)."""
        query = """
        UPDATE orders o
        JOIN users u ON o.user_id = u.id
        SET o.discount = 0.1
        WHERE u.premium = true
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "UPDATE"

    def test_update_without_where(self, parser):
        """Test UPDATE without WHERE clause (updates all rows)."""
        query = "UPDATE products SET category = 'general'"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "UPDATE"
        assert parsed["table"] == "products"
        assert "condition" not in parsed or parsed["condition"] is None


class TestDeleteParser:
    """Test DELETE statement parsing."""

    def test_basic_delete(self, parser):
        """Test parsing of basic DELETE statement."""
        query = "DELETE FROM customers WHERE id = 3"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "DELETE"
        assert parsed["table"] == "customers"
        # SQLGlot returns structured condition format, not literal string
        condition = parsed.get("condition", [])
        assert isinstance(condition, list) and len(condition) > 0
        assert (
            condition[0]["column"] == "id"
            and condition[0]["operator"] == "="
            and condition[0]["value"] == 3
        )

    def test_delete_with_complex_where(self, parser):
        """Test DELETE with complex WHERE clause."""
        query = "DELETE FROM orders WHERE status = 'cancelled' AND created_at < '2023-01-01'"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "DELETE"
        assert parsed["table"] == "orders"
        assert "condition" in parsed

    def test_delete_with_in_clause(self, parser):
        """Test DELETE with IN clause."""
        query = "DELETE FROM users WHERE id IN (1, 2, 3, 4, 5)"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "DELETE"
        assert parsed["table"] == "users"
        assert "IN" in str(parsed["condition"])

    def test_delete_with_subquery(self, parser):
        """Test DELETE with subquery in WHERE clause."""
        query = """
        DELETE FROM orders
        WHERE user_id IN (SELECT id FROM users WHERE status = 'inactive')
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "DELETE"
        assert parsed["table"] == "orders"

    def test_delete_all(self, parser):
        """Test parsing DELETE without WHERE clause."""
        query = "DELETE FROM customers"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "DELETE"
        assert parsed["table"] == "customers"
        assert "condition" not in parsed or parsed["condition"] is None

    def test_delete_with_limit(self, parser):
        """Test DELETE with LIMIT (MySQL style)."""
        query = "DELETE FROM logs WHERE created_at < '2023-01-01' LIMIT 1000"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "DELETE"
        assert parsed["table"] == "logs"

    def test_delete_with_order_by_limit(self, parser):
        """Test DELETE with ORDER BY and LIMIT."""
        query = (
            "DELETE FROM logs WHERE level = 'debug' ORDER BY created_at ASC LIMIT 100"
        )
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "DELETE"
        assert parsed["table"] == "logs"


class TestAdvancedDMLFeatures:
    """Test advanced DML features."""

    def test_upsert_mysql(self, parser):
        """Test MySQL UPSERT (INSERT ... ON DUPLICATE KEY UPDATE)."""
        query = """
        INSERT INTO users (id, name, email)
        VALUES (1, 'John', 'john@example.com')
        ON DUPLICATE KEY UPDATE name = VALUES(name), email = VALUES(email)
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "INSERT"
        assert "DUPLICATE" in str(parsed) or "UPSERT" in str(parsed)

    def test_upsert_postgres(self, parser):
        """Test PostgreSQL UPSERT (INSERT ... ON CONFLICT)."""
        query = """
        INSERT INTO users (id, name, email)
        VALUES (1, 'John', 'john@example.com')
        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "INSERT"

    def test_merge_statement(self, parser):
        """Test MERGE statement (SQL Server/Oracle style)."""
        query = """
        MERGE users AS target
        USING new_users AS source ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET name = source.name
        WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name)
        """
        parsed = parser.parse_sql(query)

        # Should parse as some type of statement
        assert "type" in parsed

    def test_replace_statement(self, parser):
        """Test REPLACE statement (MySQL)."""
        query = "REPLACE INTO users (id, name, email) VALUES (1, 'John', 'john@example.com')"
        parsed = parser.parse_sql(query)

        # Should parse as some form of insert/replace
        assert "type" in parsed

    def test_truncate_table(self, parser):
        """Test TRUNCATE TABLE statement."""
        query = "TRUNCATE TABLE logs"
        parsed = parser.parse_sql(query)

        # Should parse as some type of statement
        assert "type" in parsed


class TestDataTypeHandling:
    """Test handling of various data types in DML."""

    def test_insert_with_various_types(self, parser):
        """Test INSERT with various data types."""
        query = """
        INSERT INTO mixed_types (
            int_col, float_col, string_col, bool_col, date_col, timestamp_col, json_col
        ) VALUES (
            42, 3.14, 'hello world', true, '2023-01-01', '2023-01-01 12:00:00', '{"key": "value"}'
        )
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "INSERT"
        assert parsed["table"] == "mixed_types"
        assert len(parsed["values"]) == 1

    def test_update_with_functions(self, parser):
        """Test UPDATE with SQL functions."""
        query = """
        UPDATE users SET
            updated_at = NOW(),
            age = YEAR(CURDATE()) - YEAR(birth_date),
            full_name = CONCAT(first_name, ' ', last_name)
        WHERE id = 1
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "UPDATE"
        assert parsed["table"] == "users"
