"""
Shared configuration for parser tests.
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

@pytest.fixture(scope="session")
def parser():
    """Create a parser instance for testing."""
    return SQLParser()

@pytest.fixture(scope="session")
def sample_queries():
    """Provide sample queries for testing."""
    return {
        "simple_select": "SELECT * FROM users",
        "complex_join": """
            SELECT u.name, o.amount, p.name as product_name
            FROM users u
            INNER JOIN orders o ON u.id = o.user_id
            INNER JOIN order_items oi ON o.id = oi.order_id
            INNER JOIN products p ON oi.product_id = p.id
            WHERE u.status = 'active' AND o.amount > 100
            ORDER BY o.created_at DESC
            LIMIT 50
        """,
        "recursive_cte": """
            WITH RECURSIVE employee_hierarchy AS (
                SELECT employee_id, name, manager_id, 1 as level
                FROM employees
                WHERE manager_id IS NULL
                UNION ALL
                SELECT e.employee_id, e.name, e.manager_id, eh.level + 1
                FROM employees e
                INNER JOIN employee_hierarchy eh ON e.manager_id = eh.employee_id
                WHERE eh.level < 10
            )
            SELECT * FROM employee_hierarchy ORDER BY level, name
        """,
        "window_functions": """
            SELECT
                name,
                salary,
                department,
                ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
                AVG(salary) OVER (PARTITION BY department) as dept_avg_salary,
                LAG(salary) OVER (ORDER BY hire_date) as prev_salary
            FROM employees
            WHERE hire_date >= '2020-01-01'
        """,
        "pivot_query": """
            SELECT
                user_id,
                SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 1 THEN amount ELSE 0 END) as jan_total,
                SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 2 THEN amount ELSE 0 END) as feb_total,
                SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 3 THEN amount ELSE 0 END) as mar_total,
                COUNT(CASE WHEN EXTRACT(MONTH FROM order_date) = 1 THEN 1 END) as jan_orders,
                COUNT(CASE WHEN EXTRACT(MONTH FROM order_date) = 2 THEN 1 END) as feb_orders,
                COUNT(CASE WHEN EXTRACT(MONTH FROM order_date) = 3 THEN 1 END) as mar_orders
            FROM orders
            WHERE EXTRACT(YEAR FROM order_date) = 2023
            GROUP BY user_id
            HAVING SUM(amount) > 1000
            ORDER BY SUM(amount) DESC
        """
    }

# Add markers for different test categories
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line("markers", "basic: mark test as basic functionality")
    config.addinivalue_line("markers", "complex: mark test as complex query")
    config.addinivalue_line("markers", "edge_case: mark test as edge case")
    config.addinivalue_line("markers", "performance: mark test as performance test")
    config.addinivalue_line("markers", "ddl: mark test as DDL statement")
    config.addinivalue_line("markers", "dml: mark test as DML statement")
    config.addinivalue_line("markers", "join: mark test as JOIN query")
    config.addinivalue_line("markers", "aggregate: mark test as aggregate query")
    config.addinivalue_line("markers", "subquery: mark test as subquery test")
