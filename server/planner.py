import logging

class Planner:
    def __init__(self, catalog_manager, index_manager):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager
        logging.basicConfig(level=logging.DEBUG)
    
    def plan_query(self, parsed_query):
        """
        Generate an execution plan from the parsed query.
        """
        if parsed_query['type'] == "SELECT":
            return self.plan_select(parsed_query)
        elif parsed_query['type'] == "INSERT":
            return self.plan_insert(parsed_query)
        elif parsed_query['type'] == "UPDATE":
            return self.plan_update(parsed_query)
        elif parsed_query['type'] == "DELETE":
            return self.plan_delete(parsed_query)
        elif parsed_query['type'] == "CREATE_TABLE":
            return self.plan_create_table(parsed_query)
        elif parsed_query['type'] == "DROP_TABLE":
            return self.plan_drop_table(parsed_query)
        elif parsed_query['type'] == "JOIN":
            return self.plan_join(parsed_query)
        elif parsed_query['type'] == "CREATE_VIEW":
            return self.plan_create_view(parsed_query)
        elif parsed_query['type'] == "DROP_VIEW":
            return self.plan_drop_view(parsed_query)
        else:
            raise ValueError("Unsupported query type.")
    
    def plan_select(self, parsed_query):
        """
        Plan for SELECT queries.
        """
        logging.debug(f"Planning SELECT query: {parsed_query}")
        return {
            'type': 'SELECT',
            'table': parsed_query['from']['table'],
            'columns': parsed_query['columns'],
            'condition': parsed_query.get('where')
        }
    
    def plan_insert(self, parsed_query):
        """
        Plan for INSERT queries.
        """
        logging.debug(f"Planning INSERT query: {parsed_query}")
        return {
            'type': 'INSERT',
            'table': parsed_query['table'],
            'values': parsed_query['values']
        }
    
    def plan_join(self, parsed_query):
        """
        Plan for JOIN queries.
        Example: SELECT * FROM table1 INNER JOIN table2 ON table1.id = table2.id
        """
        logging.debug(f"Planning JOIN query: {parsed_query}")
        return {
            'type': 'JOIN',
            'table1': parsed_query['from']['table'],
            'table2': parsed_query['join']['table'],
            'condition': parsed_query['join']['condition']
        }
    
    def plan_drop_table(self, parsed_query):
        """
        Plan for DROP TABLE queries.
        Example: DROP TABLE table1
        """
        logging.debug(f"Planning DROP TABLE query: {parsed_query}")
        return {
            'type': 'DROP_TABLE',
            'table': parsed_query['table']
        }
    
    def plan_update(self, parsed_query):
        """
        Plan for UPDATE queries.
        Example: UPDATE table1 SET name = 'Bob' WHERE id = 1
        """
        logging.debug(f"Planning UPDATE query: {parsed_query}")
        return {
            'type': 'UPDATE',
            'table': parsed_query['table'],
            'set': parsed_query['set'],
            'condition': parsed_query.get('where')
        }
    
    def plan_delete(self, parsed_query):
        """
        Plan for DELETE queries.
        Example: DELETE FROM table1 WHERE id = 1
        """
        logging.debug(f"Planning DELETE query: {parsed_query}")
        return {
            'type': 'DELETE',
            'table': parsed_query['table'],
            'condition': parsed_query.get('where')
        }

    def plan_create_table(self, parsed_query):
        """
        Plan for CREATE TABLE queries.
        Example: CREATE TABLE table1 (id INT, name VARCHAR)
        """
        logging.debug(f"Planning CREATE TABLE query: {parsed_query}")
        return {
            'type': 'CREATE_TABLE',
            'table': parsed_query['table'],
            'columns': parsed_query['columns']
        }
    
    def plan_create_view(self, parsed_query):
        """
        Plan for CREATE VIEW queries.
        Example: CREATE VIEW view_name AS SELECT * FROM table
        """
        return {
            'type': 'CREATE_VIEW',
            'view_name': parsed_query['view_name'],
            'query': parsed_query['query']
        }
    
    def plan_drop_view(self, parsed_query):
        """
        Plan for DROP VIEW queries.
        Example: DROP VIEW view_name
        """
        return {
            'type': 'DROP_VIEW',
            'view_name': parsed_query['view_name']
        }