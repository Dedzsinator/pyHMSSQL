class Planner:
    def __init__(self, catalog_manager, index_manager):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager
    
    def plan_query(self, query):
        """
        Parse the query and generate an execution plan.
        """
        if query.startswith("SELECT"):
            return self.plan_select(query)
        elif query.startswith("INSERT"):
            return self.plan_insert(query)
        elif query.startswith("DELETE"):
            return self.plan_delete(query)
        else:
            raise ValueError("Unsupported query type.")
    
    def plan_select(self, query):
        """
        Plan for SELECT queries.
        Example: SELECT * FROM table1 WHERE id = 1
        """
        parts = query.split()
        table_name = parts[3]  # Extract table name
        condition = None
        
        # Extract condition if it exists
        if "WHERE" in query:
            condition = query.split("WHERE")[1].strip()
        
        return {"type": "SELECT", "table": table_name, "condition": condition}
    
    def plan_insert(self, query):
        """
        Plan for INSERT queries.
        Example: INSERT INTO table1 (id, name) VALUES (1, 'Alice')
        """
        parts = query.split()
        table_name = parts[2]  # Extract table name
        
        # Extract column names and values
        columns_part = query.split("(")[1].split(")")[0].strip()
        values_part = query.split("VALUES")[1].strip().strip("()")
        
        columns = [col.strip() for col in columns_part.split(",")]
        values = [val.strip().strip("'") for val in values_part.split(",")]
        
        # Combine columns and values into a dictionary
        record = dict(zip(columns, values))
        
        return {"type": "INSERT", "table": table_name, "record": record}
    
    def plan_delete(self, query):
        """
        Plan for DELETE queries.
        Example: DELETE FROM table1 WHERE id = 1
        """
        parts = query.split()
        table_name = parts[2]  # Extract table name
        condition = None
        
        # Extract condition if it exists
        if "WHERE" in query:
            condition = query.split("WHERE")[1].strip()
        
        return {"type": "DELETE", "table": table_name, "condition": condition}