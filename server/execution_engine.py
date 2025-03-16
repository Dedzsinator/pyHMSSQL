from pymongo import MongoClient
from bson import ObjectId
import logging
import json
import re

class ExecutionEngine:
    def __init__(self, catalog_manager, index_manager):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager
        self.client = MongoClient('localhost', 27017)
        self.transaction_stack = []
        self.preferences = self.catalog_manager.get_preferences()
    
    def _parse_condition(self, condition_str):
        """
        Parse a condition string into a MongoDB query filter.
        
        Examples:
        - "age > 30" -> {"age": {"$gt": 30}}
        - "name = 'John'" -> {"name": "John"}
        """
        if not condition_str:
            return {}
        
        # Basic operators mapping
        operators = {
            '=': '$eq',
            '>': '$gt',
            '<': '$lt',
            '>=': '$gte',
            '<=': '$lte',
            '!=': '$ne',
            '<>': '$ne'
        }
        
        # Try to match an operator
        for op in sorted(operators.keys(), key=len, reverse=True):  # Process longer ops first
            if op in condition_str:
                column, value = condition_str.split(op, 1)
                column = column.strip()
                value = value.strip()
                
                # Handle quoted strings
                if value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]
                # Handle numbers
                elif value.isdigit():
                    value = int(value)
                elif re.match(r'^[0-9]*\.[0-9]+$', value):
                    value = float(value)
                
                # Build MongoDB query
                if op == '=':
                    return {column: value}
                else:
                    return {column: {operators[op]: value}}
        
        # If no operator found, return an empty filter
        return {}
    
    def execute_aggregate(self, plan):
        """
        Execute an aggregation function (COUNT, SUM, AVG, MIN, MAX).
        """
        function = plan['function']
        column = plan['column']
        table = plan['table']
        condition = plan.get('condition')
        
        db = self.client['dbms_project']
        collection = db[table]
        
        # Apply condition if present
        query_filter = {}
        if condition:
            query_filter = self._parse_condition(condition)
        
        # Execute the aggregation
        result = None
        documents = list(collection.find(query_filter))
        
        if function == 'COUNT':
            if column:
                # Count non-null values in specified column
                result = sum(1 for doc in documents if column in doc and doc[column] is not None)
            else:
                # Count all documents
                result = len(documents)
        
        elif function == 'SUM':
            # Sum values in specified column
            result = sum(doc.get(column, 0) for doc in documents if column in doc)
        
        elif function == 'AVG':
            # Calculate average of values in specified column
            values = [doc.get(column) for doc in documents if column in doc]
            if values:
                result = sum(values) / len(values)
            else:
                result = None
        
        elif function == 'MIN':
            # Find minimum value in specified column
            values = [doc.get(column) for doc in documents if column in doc]
            if values:
                result = min(values)
            else:
                result = None
        
        elif function == 'MAX':
            # Find maximum value in specified column
            values = [doc.get(column) for doc in documents if column in doc]
            if values:
                result = max(values)
            else:
                result = None
        
        return {
            "columns": [f"{function}({column or '*'})"],
            "rows": [[result]]
        }
    
    def execute_join(self, plan):
        """
        Execute a JOIN query.
        """
        join_type = plan.get('type', 'HASH_JOIN')
        
        # Log the plan for debugging
        logging.debug(f"Executing join with plan: {plan}")
        
        if join_type == "HASH_JOIN":
            result = self.execute_hash_join(plan)
        elif join_type == "SORT_MERGE_JOIN":
            result = self.execute_sort_merge_join(plan)
        elif join_type == "INDEX_JOIN":
            result = self.execute_index_join(plan)
        else:
            # Default to nested loop join
            result = self.execute_nested_loop_join(plan)
            
        # Process any WHERE conditions on the joined result
        if 'where_condition' in plan and plan['where_condition']:
            filtered_result = []
            condition = plan['where_condition']
            query_filter = self._parse_condition(condition)
            
            for doc in result:
                # Check if the document matches the condition
                matches = True
                for field, value in query_filter.items():
                    if field not in doc or doc[field] != value:
                        matches = False
                        break
                
                if matches:
                    filtered_result.append(doc)
            
            result = filtered_result
        
        # Format the result to match the expected output format
        columns = plan.get('columns', ['*'])
        if columns and '*' not in columns:
            # Only include specified columns in the result
            result_docs = []
            for doc in result:
                result_doc = {}
                for col in columns:
                    if col in doc:
                        result_doc[col] = doc[col]
                result_docs.append(result_doc)
            result = result_docs
        
        return {
            "columns": columns,
            "rows": result
        }

    def execute_nested_loop_join(self, plan):
        """
        Execute a nested loop join.
        """
        table1 = plan['table1']
        table2 = plan['table2']
        condition = plan['condition']
        
        db = self.client['dbms_project']
        collection1 = db[table1]
        collection2 = db[table2]
        
        # Parse the join condition (e.g., "table1.id = table2.id")
        col1, col2 = condition.split('=')
        col1 = col1.strip().split('.')[1]  # Extract column name
        col2 = col2.strip().split('.')[1]  # Extract column name
        
        # Perform the nested loop join
        result = []
        for doc1 in collection1.find():
            for doc2 in collection2.find():
                if doc1[col1] == doc2[col2]:
                    combined = {**doc1, **doc2}
                    result.append(combined)
        
        return result
    
    def execute_hash_join(self, plan):
        """
        Execute a hash join.
        """
        table1 = plan['table1']
        table2 = plan['table2']
        condition = plan['condition']
        
        db = self.client['dbms_project']
        collection1 = db[table1]
        collection2 = db[table2]
        
        # Parse the join condition (e.g., "table1.id = table2.id")
        col1, col2 = condition.split('=')
        col1 = col1.strip().split('.')[1]  # Extract column name
        col2 = col2.strip().split('.')[1]  # Extract column name
        
        # Build hash table for the smaller table
        hash_table = {}
        for doc in collection1.find():
            key = doc[col1]
            if key not in hash_table:
                hash_table[key] = []
            hash_table[key].append(doc)
        
        # Perform the join
        result = []
        for doc in collection2.find():
            key = doc[col2]
            if key in hash_table:
                for match in hash_table[key]:
                    combined = {**match, **doc}
                    result.append(combined)
        
        return result
    
    def execute_sort_merge_join(self, plan):
        """
        Execute a sort-merge join.
        """
        table1 = plan['table1']
        table2 = plan['table2']
        condition = plan['condition']
        
        db = self.client['dbms_project']
        collection1 = db[table1]
        collection2 = db[table2]
        
        # Parse the join condition (e.g., "table1.id = table2.id")
        col1, col2 = condition.split('=')
        col1 = col1.strip().split('.')[1]  # Extract column name
        col2 = col2.strip().split('.')[1]  # Extract column name
        
        # Sort both collections
        sorted1 = list(collection1.find().sort(col1, 1))
        sorted2 = list(collection2.find().sort(col2, 1))
        
        # Perform the merge
        result = []
        i, j = 0, 0
        while i < len(sorted1) and j < len(sorted2):
            if sorted1[i][col1] == sorted2[j][col2]:
                combined = {**sorted1[i], **sorted2[j]}
                result.append(combined)
                i += 1
                j += 1
            elif sorted1[i][col1] < sorted2[j][col2]:
                i += 1
            else:
                j += 1
        
        return result
    
    def execute_index_join(self, plan):
        """
        Execute an index join.
        """
        table1 = plan['table1']
        table2 = plan['table2']
        condition = plan['condition']
        index = plan['index']
        
        db = self.client['dbms_project']
        collection1 = db[table1]
        collection2 = db[table2]
        
        # Parse the join condition (e.g., "table1.id = table2.id")
        col1, col2 = condition.split('=')
        col1 = col1.strip().split('.')[1]  # Extract column name
        col2 = col2.strip().split('.')[1]  # Extract column name
        
        # Perform the join using the index
        result = []
        for doc in collection1.find():
            key = doc[col1]
            index_result = index.search(key)
            if index_result:
                for match in collection2.find({col2: key}):
                    combined = {**doc, **match}
                    result.append(combined)
        
        return result
    
    def execute_begin_transaction(self):
        """
        Begin a new transaction.
        """
        self.transaction_stack.append({})
        return "Transaction started."
    
    def execute_commit_transaction(self):
        """
        Commit the current transaction.
        """
        if not self.transaction_stack:
            return "No transaction to commit."
        self.transaction_stack.pop()
        return "Transaction committed."
    
    def execute_rollback_transaction(self):
        """
        Rollback the current transaction.
        """
        if not self.transaction_stack:
            return "No transaction to rollback."
        self.transaction_stack.pop()
        return "Transaction rolled back."
    
    def execute_select(self, plan):
        """
        Execute SELECT query.
        """
        table_name = plan.get('table')
        if not table_name:
            return {"message": "No table specified"}
            
        columns = plan.get('columns', [])
        condition = plan.get('condition')
        order_by = plan.get('order_by')
        limit = plan.get('limit')
        
        db = self.client['dbms_project']
        collection = db[table_name]
        
        # Parse and apply condition
        query_filter = {}
        if condition:
            query_filter = self._parse_condition(condition)
        
        # Execute query
        cursor = collection.find(query_filter)
        
        # Apply ORDER BY if specified
        if order_by:
            # Parse order_by string to determine field and direction
            # Example: "age DESC" -> sort by age descending
            parts = order_by.split()
            field = parts[0]
            direction = -1 if len(parts) > 1 and parts[1].upper() == 'DESC' else 1
            cursor = cursor.sort(field, direction)
        
        # Apply LIMIT if specified
        if limit and isinstance(limit, int) and limit > 0:
            cursor = cursor.limit(limit)
        
        # Convert cursor to list
        documents = list(cursor)
        
        # Project columns if specified
        if columns and '*' not in columns:
            # Only include specified columns in the result
            result_docs = []
            for doc in documents:
                result_doc = {}
                for col in columns:
                    if col in doc:
                        result_doc[col] = doc[col]
                result_docs.append(result_doc)
            documents = result_docs
        
        return {
            "columns": columns if columns else ["*"],
            "rows": documents
        }

    def execute_set_preference(self, plan):
            """
            Update user preferences.
            """
            preference = plan['preference']
            value = plan['value']
            user_id = plan.get('user_id')
            
            # Update preferences
            self.preferences[preference] = value
            self.catalog_manager.update_preferences({preference: value}, user_id)
            
            return f"Preference '{preference}' set to '{value}'."
    
    def execute_insert(self, plan):
        """
        Execute an INSERT query.
        """
        table_name = plan['table']
        record = plan.get('record', {})
        
        # Handle the case where we have raw values but no record dictionary yet
        if not record and 'columns' in plan and 'values' in plan:
            cols = plan['columns']
            vals = plan['values'][0] if plan['values'] else []
            record = dict(zip(cols, vals))
        
        db = self.client['dbms_project']
        collection = db[table_name]
        
        # Insert into MongoDB
        result = collection.insert_one(record)
        
        # Update B+ Tree index if applicable
        for column, index in self.catalog_manager.get_indexes(table_name).items():
            if column in record:
                index_name = f"{table_name}.idx_{column}"
                self.index_manager.insert_into_index(index_name, record[column], str(result.inserted_id))
        
        return {"message": f"Inserted record into {table_name}.", "id": str(result.inserted_id)}
    
    def execute_update(self, plan):
        """
        Execute an UPDATE query.
        """
        table_name = plan['table']
        condition = plan.get('condition')
        updates = plan.get('updates')
        
        db = self.client['dbms_project']
        collection = db[table_name]
        
        if condition:
            # Parse the condition (e.g., "id = 1")
            column, value = condition.split('=')
            column = column.strip()
            value = value.strip().strip("'")  # Remove quotes from strings
            
            # Parse updates (e.g., "name = 'Bob'")
            update_dict = {}
            for update in updates:
                key, val = update.split('=')
                update_dict[key.strip()] = val.strip().strip("'")
            
            # Update MongoDB
            result = collection.update_many({column: value}, {"$set": update_dict})
            return f"Updated {result.modified_count} records in {table_name}."
        else:
            return "No condition provided for UPDATE query."
    
    def execute_delete(self, plan):
        """
        Execute a DELETE query.
        """
        table_name = plan['table']
        condition = plan.get('condition')
        
        db = self.client['dbms_project']
        collection = db[table_name]
        
        if condition:
            # Parse the condition (e.g., "id = 1")
            column, value = condition.split('=')
            column = column.strip()
            value = value.strip().strip("'")  # Remove quotes from strings
            
            # Delete from MongoDB
            result = collection.delete_many({column: value})
            return f"Deleted {result.deleted_count} records from {table_name}."
        else:
            # Delete all records
            result = collection.delete_many({})
            return f"Deleted {result.deleted_count} records from {table_name}."
    
    def execute_distinct(self, plan):
        """
        Execute a DISTINCT query.
        """
        table_name = plan['table']
        column = plan['column']
        
        db = self.client['dbms_project']
        collection = db[table_name]
        
        # Use index if available
        index = self.index_manager.get_index(f"{table_name}.idx_{column}")
        if index:
            distinct_values = set()
            for key in index.keys():
                distinct_values.add(key)
            return list(distinct_values)
        else:
            return list(collection.distinct(column))

    def execute_set_operation(self, plan):
        """
        Execute set operations.
        """
        if plan['type'] == "UNION":
            return self.execute_union(plan)
        elif plan['type'] == "INTERSECT":
            return self.execute_intersect(plan)
        elif plan['type'] == "EXCEPT":
            return self.execute_except(plan)
        else:
            raise ValueError("Unsupported set operation.")
    
    def execute_union(self, plan):
        """
        Execute UNION operation.
        """
        result1 = self.execute(plan['left'])
        result2 = self.execute(plan['right'])
        return list(set(result1 + result2))
    
    def execute_intersect(self, plan):
        """
        Execute INTERSECT operation.
        """
        result1 = self.execute(plan['left'])
        result2 = self.execute(plan['right'])
        return list(set(result1) & set(result2))
    
    def execute_except(self, plan):
        """
        Execute EXCEPT operation.
        """
        result1 = self.execute(plan['left'])
        result2 = self.execute(plan['right'])
        return list(set(result1) - set(result2))

    def execute_logical_operation(self, plan):
        """
        Execute logical operations.
        """
        if plan['type'] == "AND":
            return self.execute_and(plan)
        elif plan['type'] == "OR":
            return self.execute_or(plan)
        elif plan['type'] == "NOT":
            return self.execute_not(plan)
        else:
            raise ValueError("Unsupported logical operation.")
    
    def execute_and(self, plan):
        """
        Execute AND operation.
        """
        result1 = self.execute(plan['left'])
        result2 = self.execute(plan['right'])
        return [doc for doc in result1 if doc in result2]
    
    def execute_or(self, plan):
        """
        Execute OR operation.
        """
        result1 = self.execute(plan['left'])
        result2 = self.execute(plan['right'])
        return list(set(result1 + result2))
    
    def execute_not(self, plan):
        """
        Execute NOT operation.
        """
        result = self.execute(plan['child'])
        all_docs = list(self.client['dbms_project'][plan['table']].find())
        return [doc for doc in all_docs if doc not in result]
    
    def execute_create_view(self, plan):
        """
        Execute CREATE VIEW queries.
        """
        return self.catalog_manager.create_view(plan['view_name'], plan['query'])
    
    def execute_drop_view(self, plan):
        """
        Execute DROP VIEW queries.
        """
        return self.catalog_manager.drop_view(plan['view_name'])

    def execute_create_database(self, plan):
        """Execute CREATE DATABASE operation"""
        database_name = plan['database']
        result = self.catalog_manager.create_database(database_name)
        return {"message": result}

    def execute_drop_database(self, plan):
        """Execute DROP DATABASE operation"""
        database_name = plan['database']
        result = self.catalog_manager.drop_database(database_name)
        return {"message": result}
    
    def execute_create_index(self, plan):
        """
        Execute CREATE INDEX operation.
        """
        index_name = plan['index_name']
        table_name = plan['table']
        column = plan['column']
        is_unique = plan.get('unique', False)
        
        # First, check if table exists
        db = self.client['dbms_project']
        if table_name not in db.list_collection_names():
            return {"message": f"Table '{table_name}' does not exist."}
        
        # Register the index in the catalog
        self.catalog_manager.create_index(table_name, column, index_name, is_unique)
        
        # Create the actual B+ tree index
        collection = db[table_name]
        index = self.index_manager.create_index(f"{table_name}.{index_name}", is_unique)
        
        # Populate the index with existing data
        for doc in collection.find():
            if column in doc:
                index.insert(doc[column], str(doc['_id']))
        
        return {"message": f"Index '{index_name}' created on '{table_name}.{column}'"}

    def execute_drop_index(self, plan):
        """
        Execute DROP INDEX operation.
        """
        index_name = plan['index_name']
        table_name = plan['table']
        
        # Remove the index from the catalog
        result = self.catalog_manager.drop_index(table_name, index_name)
        
        # Remove the actual B+ tree index
        if result:
            self.index_manager.drop_index(f"{table_name}.{index_name}")
            return {"message": f"Index '{index_name}' dropped from '{table_name}'"}
        else:
            return {"message": f"Index '{index_name}' does not exist on '{table_name}'"}

    def execute_show(self, plan):
        """
        Execute SHOW commands.
        """
        object_type = plan['object']
        
        if object_type == "DATABASES":
            # List all databases
            databases = self.catalog_manager.get_databases()
            return {
                "columns": ["Database Name"],
                "rows": [[db] for db in databases]
            }
        
        elif object_type == "TABLES":
            # List all tables
            tables = self.catalog_manager.get_tables()
            return {
                "columns": ["Table Name"],
                "rows": [[table] for table in tables]
            }
        
        elif object_type == "INDEXES":
            # List indexes for a specific table or all indexes
            table_name = plan.get('table')
            if table_name:
                indexes = self.catalog_manager.get_indexes_for_table(table_name)
                return {
                    "columns": ["Index Name", "Column", "Unique"],
                    "rows": [[idx, info['column'], "Yes" if info['unique'] else "No"] 
                            for idx, info in indexes.items()]
                }
            else:
                all_indexes = self.catalog_manager.get_all_indexes()
                return {
                    "columns": ["Table", "Index Name", "Column", "Unique"],
                    "rows": [[table, idx, info['column'], "Yes" if info['unique'] else "No"] 
                            for table, indexes in all_indexes.items() 
                            for idx, info in indexes.items()]
                }
        
        elif object_type == "VIEWS":
            # List all views
            views = self.catalog_manager.get_views()
            return {
                "columns": ["View Name", "Query"],
                "rows": [[view, query] for view, query in views.items()]
            }
        
        elif object_type == "COLUMNS":
            # Show columns for a specific table
            table_name = plan.get('table')
            if not table_name:
                return {"error": "Table name must be specified for SHOW COLUMNS"}
                
            columns = self.catalog_manager.get_columns(table_name)
            return {
                "columns": ["Column Name", "Data Type", "Constraints"],
                "rows": [[col["name"], col["type"], col.get("constraints", "")] 
                        for col in columns]
            }
        
        elif object_type == "CREATE":
            # Show CREATE TABLE statement
            table_name = plan.get('table')
            if not table_name:
                return {"error": "Table name must be specified for SHOW CREATE"}
                
            create_stmt = self.catalog_manager.get_create_statement(table_name)
            return {
                "columns": ["Table", "Create Statement"],
                "rows": [[table_name, create_stmt]]
            }
        
        else:
            return {"message": f"Unknown object type: {object_type}"}
    
    def execute_create_table(self, plan):
        """
        Execute CREATE TABLE operation.
        """
        table_name = plan['table']
        columns = plan['columns']
        constraints = plan.get('constraints', [])
        
        # Check if the table already exists
        db = self.client['dbms_project']
        if table_name in db.list_collection_names():
            return {"message": f"Table '{table_name}' already exists."}
        
        # Create the table (collection) in MongoDB
        collection = db[table_name]
        
        # Register the table and its schema in the catalog
        self.catalog_manager.create_table(table_name, columns, constraints)
        
        return {"message": f"Table '{table_name}' created successfully."}
    
    def execute_drop_table(self, plan):
        """
        Execute DROP TABLE operation.
        """
        table_name = plan['table']
        
        # Check if the table exists
        db = self.client['dbms_project']
        if table_name not in db.list_collection_names():
            return {"message": f"Table '{table_name}' does not exist."}
        
        # Drop the table (collection) in MongoDB
        db.drop_collection(table_name)
        
        # Remove the table from the catalog
        self.catalog_manager.drop_table(table_name)
        
        # Drop any associated indexes
        indexes = self.catalog_manager.get_indexes_for_table(table_name)
        for index_name in indexes:
            self.index_manager.drop_index(f"{table_name}.{index_name}")
        
        return {"message": f"Table '{table_name}' dropped successfully."}
    
    def execute(self, plan):
        """
        Execute a query plan.
        """
        plan_type = plan.get('type')
        
        if plan_type == 'SELECT':
            return self.execute_select(plan)
        elif plan_type == 'INSERT':
            return self.execute_insert(plan)
        elif plan_type == 'UPDATE':
            return self.execute_update(plan)
        elif plan_type == 'DELETE':
            return self.execute_delete(plan)
        elif plan_type == 'CREATE_TABLE':
            return self.execute_create_table(plan)
        elif plan_type == 'DROP_TABLE':
            return self.execute_drop_table(plan)
        elif plan_type == 'CREATE_INDEX':
            return self.execute_create_index(plan)
        elif plan_type == 'DROP_INDEX':
            return self.execute_drop_index(plan)
        elif plan_type == 'SHOW':
            return self.execute_show(plan)
        elif plan_type == 'AGGREGATE':
            return self.execute_aggregate(plan)
        elif plan_type in ['JOIN', 'HASH_JOIN', 'SORT_MERGE_JOIN', 'INDEX_JOIN', 'NESTED_LOOP_JOIN']:
            return self.execute_join(plan)
        elif plan_type == 'CREATE_DATABASE':
            return self.execute_create_database(plan)
        elif plan_type == 'DROP_DATABASE':
            return self.execute_drop_database(plan)
        elif plan_type == 'CREATE_VIEW':
            return self.execute_create_view(plan)
        elif plan_type == 'DROP_VIEW':
            return self.execute_drop_view(plan)
        else:
            raise ValueError(f"Unknown plan type: {plan_type}")