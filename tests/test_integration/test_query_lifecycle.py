"""
End-to-end integration tests for query processing.
"""
import pytest

class TestQueryLifecycle:
    """Test end-to-end query processing from parse to execute."""

    def test_select_query_lifecycle(self, parser, planner, optimizer, execution_engine, catalog_manager, test_table):
        """Test complete lifecycle of a SELECT query."""
        # 1. Parse the query
        sql = f"SELECT id, name FROM {test_table} WHERE age > 25"
        parsed = parser.parse_sql(sql)

        assert parsed["type"] == "SELECT"

        # 2. Create execution plan
        execution_plan = planner.plan_query(parsed)

        assert execution_plan["type"] in ["SELECT", "SCAN"]  # Depending on implementation

        # 3. Optimize the plan
        optimized_plan = optimizer.optimize(execution_plan)

        # 4. Execute the plan
        result = execution_engine.execute(optimized_plan)

        assert result["status"] == "success"
        assert "rows" in result

        # Verify the data matches expectations
        customer_ids = [row[0] for row in result["rows"]]
        assert 1 in customer_ids  # John Doe (30)
        assert 3 in customer_ids  # Bob Johnson (40)

    def test_insert_update_delete_lifecycle(self, parser, planner, optimizer, execution_engine):
        """Test a full lifecycle with INSERT, UPDATE, and DELETE."""
        # Parse and execute INSERT
        sql_insert = "INSERT INTO customers (id, name, email, age) VALUES (10, 'Test User', 'test@example.com', 28)"
        parsed_insert = parser.parse_sql(sql_insert)
        plan_insert = planner.plan_query(parsed_insert)
        optimized_insert = optimizer.optimize(plan_insert)
        result_insert = execution_engine.execute(optimized_insert)

        assert result_insert["status"] == "success"

        # Parse and execute UPDATE
        sql_update = "UPDATE customers SET age = 29 WHERE id = 10"
        parsed_update = parser.parse_sql(sql_update)
        plan_update = planner.plan_query(parsed_update)
        optimized_update = optimizer.optimize(plan_update)
        result_update = execution_engine.execute(optimized_update)

        assert result_update["status"] == "success"

        # Parse and execute SELECT to verify the update
        sql_select = "SELECT * FROM customers WHERE id = 10"
        parsed_select = parser.parse_sql(sql_select)
        plan_select = planner.plan_query(parsed_select)
        optimized_select = optimizer.optimize(plan_select)
        result_select = execution_engine.execute(optimized_select)

        assert result_select["status"] == "success"
        assert len(result_select["rows"]) == 1

        # Get the age column index
        age_index = result_select["columns"].index("age")
        assert result_select["rows"][0][age_index] == 29

        # Parse and execute DELETE
        sql_delete = "DELETE FROM customers WHERE id = 10"
        parsed_delete = parser.parse_sql(sql_delete)
        plan_delete = planner.plan_query(parsed_delete)
        optimized_delete = optimizer.optimize(plan_delete)
        result_delete = execution_engine.execute(optimized_delete)

        assert result_delete["status"] == "success"

        # Verify deletion
        sql_verify = "SELECT * FROM customers WHERE id = 10"
        parsed_verify = parser.parse_sql(sql_verify)
        plan_verify = planner.plan_query(parsed_verify)
        optimized_verify = optimizer.optimize(plan_verify)
        result_verify = execution_engine.execute(optimized_verify)

        assert result_verify["status"] == "success"
        assert len(result_verify["rows"]) == 0