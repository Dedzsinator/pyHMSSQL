import logging
import re

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
        tables = parsed_query.get('tables', [])
        
        # If there are multiple tables or a join condition, this is a JOIN query
        if len(tables) > 1 or parsed_query.get('join_condition'):
            return self.plan_join(parsed_query)
        
        table = tables[0] if tables else None
        
        # Extract condition (WHERE clause)
        condition = parsed_query.get('condition')
        
        # Extract ORDER BY clause
        order_by = parsed_query.get('order_by')
        
        # Extract LIMIT clause
        limit = parsed_query.get('limit')
        
        # Check for aggregation functions in columns
        columns = parsed_query.get('columns', [])
        aggregate_function = None
        aggregate_column = None
        
        for col in columns:
            if '(' in col and ')' in col:
                # This might be an aggregate function
                func_match = re.match(r'(\w+)\((\w+|\*)\)', col)
                if func_match:
                    func_name = func_match.group(1).upper()
                    if func_name in ('COUNT', 'SUM', 'AVG', 'MIN', 'MAX'):
                        aggregate_function = func_name
                        aggregate_column = func_match.group(2)
        
        if aggregate_function:
            return {
                'type': 'AGGREGATE',
                'function': aggregate_function,
                'column': aggregate_column if aggregate_column != '*' else None,
                'table': table,
                'condition': condition
            }
        
        return {
            'type': 'SELECT',
            'table': table,
            'columns': columns,
            'condition': condition,
            'order_by': order_by,
            'limit': limit,
            'use_index': False,
            'index_column': None
        }

    def plan_join(self, parsed_query):
        """
        Plan for JOIN queries.
        """
        tables = parsed_query.get('tables', [])
        join_type = parsed_query.get('join_type', 'INNER JOIN')
        join_condition = parsed_query.get('join_condition')
        columns = parsed_query.get('columns', [])
        condition = parsed_query.get('condition')
        
        # Default to hash join strategy
        join_strategy = 'HASH_JOIN'
        preferences = self.catalog_manager.get_preferences()
        if preferences.get('join_strategy') == 'sort_merge':
            join_strategy = 'SORT_MERGE_JOIN'
        elif preferences.get('join_strategy') == 'index':
            join_strategy = 'INDEX_JOIN'
        elif preferences.get('join_strategy') == 'nested_loop':
            join_strategy = 'NESTED_LOOP_JOIN'
        
        # Determine which tables are being joined
        table1 = tables[0]
        table2 = tables[1] if len(tables) > 1 else None
        
        # Build the join plan
        join_plan = {
            'type': join_strategy,
            'table1': table1,
            'table2': table2,
            'condition': join_condition,
            'columns': columns,
            'where_condition': condition
        }
        
        return join_plan
    
    def plan_insert(self, parsed_query):
        """
        Plan for INSERT queries.
        """
        logging.debug(f"Planning INSERT query: {parsed_query}")
        
        table_name = parsed_query['table']
        columns = parsed_query.get('columns', [])
        values = parsed_query.get('values', [])
        
        # Build a record from columns and values
        record = {}
        if columns and values and len(values) > 0:
            for i, column in enumerate(columns):
                if i < len(values[0]):
                    value = values[0][i]
                    record[column] = value
        
        return {
            'type': 'INSERT',
            'table': table_name,
            'record': record,
            'columns': columns,
            'values': values
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
        where_clause = parsed_query.get('condition') or parsed_query.get('where')
    
        return {
            'type': 'UPDATE',
            'table': parsed_query['table'],
            'set': parsed_query['set'],
            'updates': list(parsed_query['set'].items()),  # Convert set pairs to update list
            'condition': where_clause  # Use consistent key name
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