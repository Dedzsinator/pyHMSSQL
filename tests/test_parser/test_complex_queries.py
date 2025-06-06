"""
Tests for complex and obscure SQL query parsing.
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

class TestJoinQueries:
    """Test complex JOIN queries."""

    def test_inner_join(self, parser):
        """Test parsing INNER JOIN."""
        query = "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "JOIN"
        assert parsed["join_info"]["type"] == "INNER"
        assert "users" in parsed["tables"]
        assert "orders" in parsed["tables"]

    def test_left_join(self, parser):
        """Test parsing LEFT JOIN."""
        query = "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "JOIN"
        assert parsed["join_info"]["type"] == "LEFT"

    def test_right_join(self, parser):
        """Test parsing RIGHT JOIN."""
        query = "SELECT u.name, o.amount FROM users u RIGHT JOIN orders o ON u.id = o.user_id"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "JOIN"
        assert parsed["join_info"]["type"] == "RIGHT"

    def test_full_outer_join(self, parser):
        """Test parsing FULL OUTER JOIN."""
        query = "SELECT u.name, o.amount FROM users u FULL OUTER JOIN orders o ON u.id = o.user_id"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "JOIN"

    def test_cross_join(self, parser):
        """Test parsing CROSS JOIN."""
        query = "SELECT u.name, p.name FROM users u CROSS JOIN products p"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "JOIN"

    def test_multiple_joins(self, parser):
        """Test parsing multiple JOINs."""
        query = """
        SELECT u.name, o.amount, p.name 
        FROM users u 
        INNER JOIN orders o ON u.id = o.user_id 
        INNER JOIN order_items oi ON o.id = oi.order_id
        INNER JOIN products p ON oi.product_id = p.id
        """
        parsed = parser.parse_sql(query)

        # Should parse as some form of join
        assert "JOIN" in str(parsed).upper()

    def test_join_with_where(self, parser):
        """Test JOIN with WHERE clause."""
        query = """
        SELECT u.name, o.amount 
        FROM users u 
        INNER JOIN orders o ON u.id = o.user_id 
        WHERE o.amount > 100 AND u.status = 'active'
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "JOIN"
        assert "where_conditions" in parsed or "condition" in parsed

class TestSubqueries:
    """Test subquery parsing."""

    def test_subquery_in_where(self, parser):
        """Test subquery in WHERE clause."""
        query = """
        SELECT name FROM users 
        WHERE id IN (SELECT user_id FROM orders WHERE amount > 1000)
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert "IN" in str(parsed["condition"]).upper()

    def test_correlated_subquery(self, parser):
        """Test correlated subquery."""
        query = """
        SELECT name FROM users u1
        WHERE EXISTS (
            SELECT 1 FROM orders o 
            WHERE o.user_id = u1.id AND o.amount > 500
        )
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert "EXISTS" in str(parsed).upper()

    def test_subquery_in_from(self, parser):
        """Test subquery in FROM clause."""
        query = """
        SELECT avg_amount FROM (
            SELECT AVG(amount) as avg_amount FROM orders GROUP BY user_id
        ) as user_averages
        """
        parsed = parser.parse_sql(query)

        # Should parse as a SELECT with subquery
        assert parsed["type"] == "SELECT"

class TestAggregateQueries:
    """Test aggregate function parsing."""

    def test_count_all(self, parser):
        """Test COUNT(*) parsing."""
        query = "SELECT COUNT(*) FROM users"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "AGGREGATE" or "COUNT" in str(parsed)

    def test_count_distinct(self, parser):
        """Test COUNT(DISTINCT column) parsing."""
        query = "SELECT COUNT(DISTINCT email) FROM users"
        parsed = parser.parse_sql(query)

        assert "COUNT" in str(parsed)
        assert "DISTINCT" in str(parsed)

    def test_multiple_aggregates(self, parser):
        """Test multiple aggregate functions."""
        query = "SELECT COUNT(*), AVG(amount), MAX(amount), MIN(amount) FROM orders"
        parsed = parser.parse_sql(query)

        # Should contain aggregate functions
        result_str = str(parsed)
        assert "COUNT" in result_str or "AVG" in result_str

    def test_group_by_having(self, parser):
        """Test GROUP BY with HAVING clause."""
        query = """
        SELECT user_id, COUNT(*), AVG(amount) 
        FROM orders 
        GROUP BY user_id 
        HAVING COUNT(*) > 5 AND AVG(amount) > 100
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "AGGREGATE"  # SQLGlot correctly identifies this as an aggregate operation
        assert "group_by" in parsed or "GROUP BY" in str(parsed)

class TestSetOperations:
    """Test set operations (UNION, INTERSECT, EXCEPT)."""

    def test_union(self, parser):
        """Test UNION parsing."""
        query = """
        SELECT name FROM users WHERE age > 30
        UNION
        SELECT name FROM customers WHERE status = 'premium'
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "UNION"
        assert "left" in parsed
        assert "right" in parsed

    def test_union_all(self, parser):
        """Test UNION ALL parsing."""
        query = """
        SELECT name FROM users
        UNION ALL
        SELECT name FROM customers
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "UNION"

    def test_intersect(self, parser):
        """Test INTERSECT parsing."""
        query = """
        SELECT email FROM users
        INTERSECT
        SELECT email FROM customers
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "INTERSECT"

    def test_except(self, parser):
        """Test EXCEPT parsing."""
        query = """
        SELECT email FROM users
        EXCEPT
        SELECT email FROM blacklist
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "EXCEPT"

class TestWindowFunctions:
    """Test window function parsing."""

    def test_row_number(self, parser):
        """Test ROW_NUMBER() window function."""
        query = """
        SELECT name, ROW_NUMBER() OVER (ORDER BY created_at) as rn
        FROM users
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert "ROW_NUMBER" in str(parsed)

    def test_rank_functions(self, parser):
        """Test RANK and DENSE_RANK functions."""
        query = """
        SELECT name, amount,
               RANK() OVER (ORDER BY amount DESC) as rank,
               DENSE_RANK() OVER (ORDER BY amount DESC) as dense_rank
        FROM orders
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"

    def test_partition_by(self, parser):
        """Test PARTITION BY in window functions."""
        query = """
        SELECT user_id, amount,
               AVG(amount) OVER (PARTITION BY user_id) as user_avg
        FROM orders
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"

class TestObscureQueries:
    """Test obscure and edge case queries."""

    def test_recursive_cte(self, parser):
        """Test recursive Common Table Expression."""
        query = """
        WITH RECURSIVE employee_hierarchy AS (
            SELECT employee_id, name, manager_id, 1 as level
            FROM employees
            WHERE manager_id IS NULL
            
            UNION ALL
            
            SELECT e.employee_id, e.name, e.manager_id, eh.level + 1
            FROM employees e
            INNER JOIN employee_hierarchy eh ON e.manager_id = eh.employee_id
        )
        SELECT * FROM employee_hierarchy
        """
        parsed = parser.parse_sql(query)

        # Should parse as some form of query
        assert "type" in parsed

    def test_case_when(self, parser):
        """Test CASE WHEN expressions."""
        query = """
        SELECT name,
               CASE 
                   WHEN age < 18 THEN 'Minor'
                   WHEN age < 65 THEN 'Adult'
                   ELSE 'Senior'
               END as age_group
        FROM users
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert "CASE" in str(parsed)

    def test_lateral_join(self, parser):
        """Test LATERAL JOIN."""
        query = """
        SELECT u.name, recent_orders.amount
        FROM users u
        CROSS JOIN LATERAL (
            SELECT amount FROM orders o 
            WHERE o.user_id = u.id 
            ORDER BY created_at DESC 
            LIMIT 1
        ) as recent_orders
        """
        parsed = parser.parse_sql(query)

        # Should parse as some form of join
        assert "type" in parsed

    def test_values_clause(self, parser):
        """Test VALUES clause."""
        query = """
        SELECT * FROM (VALUES 
            (1, 'Alice', 25),
            (2, 'Bob', 30),
            (3, 'Charlie', 35)
        ) AS users(id, name, age)
        """
        parsed = parser.parse_sql(query)

        assert "type" in parsed

    def test_pivot_like_query(self, parser):
        """Test pivot-like query with conditional aggregation."""
        query = """
        SELECT 
            user_id,
            SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) as completed_total,
            SUM(CASE WHEN status = 'pending' THEN amount ELSE 0 END) as pending_total,
            SUM(CASE WHEN status = 'cancelled' THEN amount ELSE 0 END) as cancelled_total
        FROM orders
        GROUP BY user_id
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "AGGREGATE"  # SQLGlot correctly identifies this as an aggregate operation

    def test_json_operations(self, parser):
        """Test JSON operations (MySQL/PostgreSQL style)."""
        query = "SELECT data->>'$.name' as extracted_name FROM json_table"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"

    def test_array_operations(self, parser):
        """Test array operations (PostgreSQL style)."""
        query = "SELECT tags[1], array_length(tags, 1) FROM articles WHERE 'tech' = ANY(tags)"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"

class TestSpecialStatements:
    """Test special statement types."""

    def test_show_statements(self, parser):
        """Test various SHOW statements."""
        queries = [
            "SHOW DATABASES",
            "SHOW TABLES",
            "SHOW COLUMNS FROM users",
            "SHOW INDEXES FROM users",
            "SHOW CREATE TABLE users"
        ]
        
        for query in queries:
            parsed = parser.parse_sql(query)
            assert parsed["type"] == "SHOW" or "SHOW" in str(parsed)

    def test_use_database(self, parser):
        """Test USE DATABASE statement."""
        query = "USE testdb"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "USE_DATABASE"
        assert parsed["database"] == "testdb"

    def test_explain_query(self, parser):
        """Test EXPLAIN statement."""
        query = "EXPLAIN SELECT * FROM users WHERE age > 30"
        parsed = parser.parse_sql(query)

        # Should parse the inner SELECT or handle EXPLAIN
        assert "type" in parsed

    def test_transaction_statements(self, parser):
        """Test transaction control statements."""
        queries = [
            "BEGIN TRANSACTION",
            "COMMIT",
            "ROLLBACK",
            "START TRANSACTION",
            "COMMIT TRANSACTION",
            "ROLLBACK TRANSACTION"
        ]
        
        for query in queries:
            parsed = parser.parse_sql(query)
            assert "type" in parsed

    def test_script_statement(self, parser):
        """Test custom SCRIPT statement."""
        query = "SCRIPT 'test_script.sql'"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SCRIPT"
        assert parsed["filename"] == "test_script.sql"

    def test_visualize_statement(self, parser):
        """Test custom VISUALIZE statement."""
        queries = [
            "VISUALIZE BPTREE ON users",
            "VISUALIZE BPTREE idx_name ON users"
        ]
        
        for query in queries:
            parsed = parser.parse_sql(query)
            assert parsed["type"] == "VISUALIZE"
            assert parsed["object"] == "BPTREE"

class TestErrorHandling:
    """Test parser error handling."""

    def test_empty_query(self, parser):
        """Test parsing empty query."""
        parsed = parser.parse_sql("")
        assert "error" in parsed

    def test_invalid_syntax(self, parser):
        """Test parsing invalid SQL syntax."""
        query = "SELECTT * FROMM users WHEREE"
        parsed = parser.parse_sql(query)
        assert "error" in parsed

    def test_incomplete_query(self, parser):
        """Test parsing incomplete query."""
        query = "SELECT * FROM"
        parsed = parser.parse_sql(query)
        assert "error" in parsed

    def test_malformed_join(self, parser):
        """Test parsing malformed JOIN."""
        query = "SELECT * FROM users JOIN ON orders"
        parsed = parser.parse_sql(query)
        assert "error" in parsed

class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_very_long_query(self, parser):
        """Test parsing very long query."""
        columns = ", ".join([f"col{i}" for i in range(100)])
        query = f"SELECT {columns} FROM huge_table"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert len(parsed["columns"]) == 100

    def test_deeply_nested_subqueries(self, parser):
        """Test deeply nested subqueries."""
        query = """
        SELECT * FROM (
            SELECT * FROM (
                SELECT * FROM (
                    SELECT id FROM users WHERE age > 20
                ) AS level1 WHERE id > 100
            ) AS level2 WHERE id < 1000
        ) AS level3
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"

    def test_unicode_content(self, parser):
        """Test parsing queries with Unicode content."""
        query = "SELECT * FROM users WHERE name = 'José García'"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert "José García" in str(parsed)

    def test_special_characters_in_identifiers(self, parser):
        """Test parsing with special characters in identifiers."""
        query = 'SELECT `weird-column`, `table with spaces`.`another-column` FROM `table with spaces`'
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"

    def test_comments_in_query(self, parser):
        """Test parsing queries with comments."""
        query = """
        SELECT id, name -- This is a comment
        FROM users /* Another comment */
        WHERE age > 30 -- Final comment
        """
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert parsed["tables"] == ["users"]

    def test_case_insensitive_keywords(self, parser):
        """Test case insensitive SQL keywords."""
        query = "select ID, Name from USERS where AGE > 30 order by NAME asc"
        parsed = parser.parse_sql(query)

        assert parsed["type"] == "SELECT"
        assert "users" in [t.lower() for t in parsed["tables"]]
