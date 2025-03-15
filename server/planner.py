import logging

class Planner:
    def __init__(self, catalog_manager, index_manager):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager
        logging.basicConfig(level=logging.DEBUG)
    
    def plan_query(self, query):
        """
        Parse the query and generate an execution plan.
        """
        logging.debug(f"Planning query: {query}")
        if query.startswith("SELECT"):
            plan = self.plan_select(query)
        elif query.startswith("INSERT"):
            plan = self.plan_insert(query)
        elif query.startswith("UPDATE"):
            plan = self.plan_update(query)
        elif query.startswith("DELETE"):
            plan = self.plan_delete(query)
        else:
            raise ValueError("Unsupported query type.")
        logging.debug(f"Generated plan: {plan}")
        return plan
    
    def plan_select(self, query):
        """
        Plan for SELECT queries.
        Example: SELECT * FROM table1 INNER JOIN table2 ON table1.id = table2.id
        """
        logging.debug(f"Planning SELECT query: {query}")
        if "INNER JOIN" in query:
            plan = self.plan_join(query)
        else:
            parts = query.split()
            table_name = parts[3]  # Extract table name
            condition = None
            
            # Extract condition if it exists
            if "WHERE" in query:
                condition = query.split("WHERE")[1].strip()
                if "SELECT" in condition:  # Handle subquery
                    subquery = condition.split("SELECT")[1].strip()
                    subquery_plan = self.plan_select(f"SELECT {subquery}")
                    plan = {"type": "SELECT", "table": table_name, "condition": condition, "subquery": subquery_plan}
                else:
                    plan = {"type": "SELECT", "table": table_name, "condition": condition}
            else:
                plan = {"type": "SELECT", "table": table_name, "condition": condition}
        logging.debug(f"Generated SELECT plan: {plan}")
        return plan
    
    def plan_join(self, query):
        """
        Plan for INNER JOIN queries.
        """
        logging.debug(f"Planning JOIN query: {query}")
        parts = query.split()
        table1 = parts[3]  # First table
        table2 = parts[5]  # Second table
        join_condition = query.split("ON")[1].strip()  # Join condition
        
        plan = {"type": "JOIN", "table1": table1, "table2": table2, "condition": join_condition}
        logging.debug(f"Generated JOIN plan: {plan}")
        return plan
    
    def plan_insert(self, query):
        """
        Plan for INSERT queries.
        Example: INSERT INTO table1 (id, name) VALUES (1, 'Alice')
        """
        logging.debug(f"Planning INSERT query: {query}")
        parts = query.split()
        table_name = parts[2]  # Extract table name
        
        # Extract column names and values
        columns_part = query.split("(")[1].split(")")[0].strip()
        values_part = query.split("VALUES")[1].strip().strip("()")
        
        columns = [col.strip() for col in columns_part.split(",")]
        values = [val.strip().strip("'") for val in values_part.split(",")]
        
        # Combine columns and values into a dictionary
        record = dict(zip(columns, values))
        
        plan = {"type": "INSERT", "table": table_name, "record": record}
        logging.debug(f"Generated INSERT plan: {plan}")
        return plan
    
    def plan_update(self, query):
        """
        Plan for UPDATE queries.
        Example: UPDATE table1 SET name = 'Bob' WHERE id = 1
        """
        logging.debug(f"Planning UPDATE query: {query}")
        parts = query.split()
        table_name = parts[1]  # Extract table name
        
        # Extract updates (e.g., "name = 'Bob'")
        updates_part = query.split("SET")[1].split("WHERE")[0].strip()
        updates = [update.strip() for update in updates_part.split(",")]
        
        # Extract condition (e.g., "id = 1")
        condition = query.split("WHERE")[1].strip()
        
        plan = {"type": "UPDATE", "table": table_name, "condition": condition, "updates": updates}
        logging.debug(f"Generated UPDATE plan: {plan}")
        return plan
    
    def plan_delete(self, query):
        """
        Plan for DELETE queries.
        Example: DELETE FROM table1 WHERE id = 1
        """
        logging.debug(f"Planning DELETE query: {query}")
        parts = query.split()
        table_name = parts[2]  # Extract table name
        condition = None
        
        # Extract condition if it exists
        if "WHERE" in query:
            condition = query.split("WHERE")[1].strip()
        
        plan = {"type": "DELETE", "table": table_name, "condition": condition}
        logging.debug(f"Generated DELETE plan: {plan}")
        return plan