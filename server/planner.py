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
        elif parsed_query['type'] == "CREATE":
            # Check if this is a CREATE DATABASE, CREATE TABLE, or CREATE INDEX
            if "table" in parsed_query and parsed_query["table"]:
                return self.plan_create_table(parsed_query)
            elif "database" in parsed_query and parsed_query["database"]:
                return self.plan_create_database(parsed_query)
            elif "index" in parsed_query and parsed_query["index"]:
                return self.plan_create_index(parsed_query)
            else:
                raise ValueError("Unsupported CREATE statement type.")
        elif parsed_query['type'] == "DROP":
            # Check if this is a DROP DATABASE, DROP TABLE, or DROP INDEX
            if "table" in parsed_query and parsed_query["table"]:
                return self.plan_drop_table(parsed_query)
            elif "database" in parsed_query and parsed_query["database"]:
                return self.plan_drop_database(parsed_query)
            elif "index" in parsed_query and parsed_query["index"]:
                return self.plan_drop_index(parsed_query)
            else:
                raise ValueError("Unsupported DROP statement type.")
        elif parsed_query['type'] == "JOIN":
            return self.plan_join(parsed_query)
        elif parsed_query['type'] == "CREATE_VIEW":
            return self.plan_create_view(parsed_query)
        elif parsed_query['type'] == "DROP_VIEW":
            return self.plan_drop_view(parsed_query)
        elif parsed_query['type'] == "SHOW":
            return self.plan_show(parsed_query)
        else:
            raise ValueError("Unsupported query type.")
    
    def plan_create_database(self, parsed_query):
        """
        Plan for CREATE DATABASE queries.
        Example: CREATE DATABASE dbname
        """
        logging.debug(f"Planning CREATE DATABASE query: {parsed_query}")
        return {
            'type': 'CREATE_DATABASE',
            'database': parsed_query['database']
        }
    
    def plan_drop_database(self, parsed_query):
        """
        Plan for DROP DATABASE queries.
        Example: DROP DATABASE dbname
        """
        logging.debug(f"Planning DROP DATABASE query: {parsed_query}")
        return {
            'type': 'DROP_DATABASE',
            'database': parsed_query['database']
        }
    
    def plan_create_index(self, parsed_query):
        """
        Plan for CREATE INDEX queries.
        Example: CREATE INDEX idx_name ON table_name (column_name)
        """
        logging.debug(f"Planning CREATE INDEX query: {parsed_query}")
        return {
            'type': 'CREATE_INDEX',
            'index_name': parsed_query['index'],
            'table': parsed_query['table'],
            'column': parsed_query['column'],
            'unique': parsed_query.get('unique', False)
        }
    
    def plan_drop_index(self, parsed_query):
        """
        Plan for DROP INDEX queries.
        Example: DROP INDEX idx_name ON table_name
        """
        logging.debug(f"Planning DROP INDEX query: {parsed_query}")
        return {
            'type': 'DROP_INDEX',
            'index_name': parsed_query['index'],
            'table': parsed_query['table']
        }
    
    def plan_show(self, parsed_query):
        """
        Plan for SHOW queries.
        Examples: 
        - SHOW DATABASES
        - SHOW TABLES
        - SHOW INDEXES FOR table_name
        """
        logging.debug(f"Planning SHOW query: {parsed_query}")
        return {
            'type': 'SHOW',
            'object': parsed_query['object'],
            'table': parsed_query.get('table')
        }
    
    def plan_select(self, parsed_query):
        """
        Plan for SELECT queries.
        """
        logging.debug(f"Planning SELECT query: {parsed_query}")
        
        # Determine if we can use indexes for this query
        table = parsed_query['tables'][0] if parsed_query.get('tables') else None
        where_clause = parsed_query.get('where')
        use_index = False
        index_column = None
        
        if table and where_clause:
            # Check if the condition could use an index
            # Simple implementation - assumes condition is of form "column = value"
            if '=' in where_clause and not (' OR ' in where_clause.upper()):
                parts = where_clause.split('=')
                if len(parts) == 2:
                    column = parts[0].strip()
                    # Check if this column has an index
                    if self.catalog_manager.has_index(table, column):
                        use_index = True
                        index_column = column
        
        return {
            'type': 'SELECT',
            'table': table,
            'columns': parsed_query['columns'],
            'condition': where_clause,
            'use_index': use_index,
            'index_column': index_column,
            'limit': parsed_query.get('limit')
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