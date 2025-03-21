from pymongo import MongoClient
from bson import ObjectId
import logging
import json
import re

class ExecutionEngine:
    def __init__(self, catalog_manager, index_manager):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager
        self.client = catalog_manager.client
        self.current_database = catalog_manager.get_current_database()
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
        
        # Initialize result
        result = []
        
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
        # Extract tables and condition
        table1 = plan.get('table1', '')
        table2 = plan.get('table2', '')
        condition = plan.get('condition')
        
        # If condition is None, try to extract it from table2 (which may contain "ON clause")
        if condition is None and ' ON ' in table2.upper():
            parts = table2.split(' ON ', 1)
            table2 = parts[0].strip()
            condition = parts[1].strip()
        
        # Handle table aliases
        table1_parts = table1.split()
        table1_name = table1_parts[0]
        table1_alias = table1_parts[1] if len(table1_parts) > 1 else table1_name
        
        table2_parts = table2.split()
        table2_name = table2_parts[0]
        table2_alias = table2_parts[1] if len(table2_parts) > 1 else table2_name
        
        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        db = self.client[db_name]
        
        # Check if collections exist
        if table1_name not in db.list_collection_names():
            return {"error": f"Table '{table1_name}' does not exist in database '{db_name}'"}
        if table2_name not in db.list_collection_names():
            return {"error": f"Table '{table2_name}' does not exist in database '{db_name}'"}
        
        collection1 = db[table1_name]
        collection2 = db[table2_name]
        
        # Parse join condition (e.g., "e.dept_id = d.id")
        if not condition:
            return {"error": "No join condition specified"}
        
        logging.debug(f"Join condition: {condition}")
        col1, col2 = condition.split('=')
        col1 = col1.strip()
        col2 = col2.strip()
        
        # Handle column references with aliases (e.g., "e.dept_id")
        if '.' in col1:
            alias1, col1_name = col1.split('.')
        else:
            col1_name = col1
            
        if '.' in col2:
            alias2, col2_name = col2.split('.')
        else:
            col2_name = col2
        
        # Build hash table for the smaller table (typically collection1)
        hash_table = {}
        for doc in collection1.find():
            key = doc.get(col1_name.lower())  # MongoDB field names are case-sensitive
            if key not in hash_table:
                hash_table[key] = []
            hash_table[key].append(doc)
        
        # Perform the join
        result = []
        for doc in collection2.find():
            key = doc.get(col2_name.lower())  # MongoDB field names are case-sensitive
            if key in hash_table:
                for match in hash_table[key]:
                    # Create a combined document with aliased fields if needed
                    combined = {}
                    
                    # Add fields from first table with alias prefix
                    for field, value in match.items():
                        if field != '_id':  # Skip MongoDB internal IDs
                            combined[f"{table1_alias.lower()}.{field}"] = value
                    
                    # Add fields from second table with alias prefix
                    for field, value in doc.items():
                        if field != '_id':  # Skip MongoDB internal IDs
                            combined[f"{table2_alias.lower()}.{field}"] = value
                    
                    result.append(combined)
        
        # Process the result based on the requested columns
        columns = plan.get('columns', ['*'])
        formatted_result = []
        
        for doc in result:
            row = []
            for col in columns:
                if col == '*':
                    # Include all columns
                    row.extend(doc.values())
                else:
                    # Include only the specified column
                    col = col.lower()  # Case-insensitive matching
                    if col in doc:
                        row.append(doc[col])
                    else:
                        # Try with alias.column format
                        found = False
                        for key in doc:
                            if key.lower() == col.lower():
                                row.append(doc[key])
                                found = True
                                break
                        if not found:
                            row.append(None)
            formatted_result.append(row)
        
        return {
            "columns": columns,
            "rows": formatted_result
        }
    
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
        Execute INSERT operation.
        """
        table_name = plan.get('table')
        columns = plan.get('columns', [])
        values_list = plan.get('values', [])
        
        # Validate that we have a table name
        if not table_name:
            return {"error": "No table name specified for INSERT"}
        
        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected. Use 'USE database_name' first."}
        
        db = self.client[db_name]
        
        # Validate that the table exists
        if table_name not in db.list_collection_names():
            return {"error": f"Table '{table_name}' does not exist in database '{db_name}'."}
        
        # Get the collection
        collection = db[table_name]
        
        # Format values into documents
        docs = []
        for values in values_list:
            doc = {}
            if len(columns) != len(values):
                return {"error": "Column count doesn't match value count"}
            
            for i, col in enumerate(columns):
                doc[col] = values[i]
            
            docs.append(doc)
        
        # Insert the documents
        if docs:
            result = collection.insert_many(docs)
            return {
                "message": f"Inserted {len(result.inserted_ids)} document(s) into '{db_name}.{table_name}'"
            }
        else:
            return {"error": "No values to insert"}
    
    def execute_update(self, plan):
        """
        Execute an UPDATE query.
        """
        table_name = plan['table']
        condition = plan.get('condition')
        updates = plan.get('set', {})  # This should match the field in the plan
        
        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected. Use 'USE database_name' first."}
            
        db = self.client[db_name]
        
        # Validate that the table exists
        if table_name not in db.list_collection_names():
            return {"error": f"Table '{table_name}' does not exist in database '{db_name}'."}
        
        collection = db[table_name]
        
        if not condition:
            return {"error": "No condition provided for UPDATE query."}
        
        try:
            # Parse the condition using existing utility method
            query_filter = self._parse_condition(condition)
            
            # Parse updates from the 'set' field in the plan
            update_dict = {}
            if isinstance(updates, dict):
                update_dict = updates
            else:
                # Fallback for string format like "name = 'Bob'"
                for update in updates if isinstance(updates, list) else [updates]:
                    key, val = update.split('=', 1)
                    key = key.strip()
                    val = val.strip()
                    
                    # Handle quoted strings
                    if val.startswith("'") and val.endswith("'"):
                        val = val[1:-1]
                    # Handle numbers
                    elif val.isdigit():
                        val = int(val)
                    elif re.match(r'^[0-9]*\.[0-9]+$', val):
                        val = float(val)
                        
                    update_dict[key] = val
            
            # Update MongoDB
            result = collection.update_many(query_filter, {"$set": update_dict})
            return {"message": f"Updated {result.modified_count} records in {db_name}.{table_name}."}
        except Exception as e:
            return {"error": f"Error in UPDATE operation: {str(e)}"}
    
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
        """
        Execute DROP DATABASE operation.
        """
        database_name = plan.get('database')
        
        if not database_name:
            return {"error": "No database name specified"}
        
        # Check if the database exists
        client = self.client
        all_dbs = client.list_database_names()
        
        if database_name not in all_dbs:
            return {"message": f"Database '{database_name}' does not exist."}
        
        # Drop the database
        client.drop_database(database_name)
        
        # If this was the current database, reset to dbms_project
        if self.catalog_manager.get_current_database() == database_name:
            self.catalog_manager.set_current_database('dbms_project')
        
        # Clean up catalog entries for this database
        catalog_db = self.client['dbms_project']
        catalog_db.catalog.delete_many({"database": database_name})
        catalog_db.indexes.delete_many({"database": database_name})
        
        return {"message": f"Database '{database_name}' dropped."}
    
    def execute_create_index(self, plan):
        """
        Execute CREATE INDEX operation.
        """
        index_name = plan['index_name']
        table_name = plan['table']
        column = plan['column']
        is_unique = plan.get('unique', False)
        
        if not table_name:
            return {"error": "Table name must be specified for CREATE INDEX"}
        
        # First, check if table exists
        db_name = self.catalog_manager.get_current_database()
        db = self.client[db_name]
        
        if table_name not in db.list_collection_names():
            return {"error": f"Table '{table_name}' does not exist in database '{db_name}'."}
        
        # Check if index already exists
        indexes = self.catalog_manager.get_indexes_for_table(table_name)
        if index_name in indexes:
            return {"error": f"Index '{index_name}' already exists on '{table_name}'."}
        
        # Register the index in the catalog
        try:
            result = self.catalog_manager.create_index(table_name, index_name, column, is_unique)
            if result:
                return {"message": f"Index '{index_name}' created on '{table_name}.{column}'"}
            else:
                return {"error": f"Failed to create index '{index_name}' on '{table_name}'."}
        except Exception as e:
            return {"error": f"Error creating index: {str(e)}"}
    
    def execute_drop_index(self, plan):
        """
        Execute DROP INDEX operation.
        """
        index_name = plan['index_name']
        table_name = plan['table']
        
        if not table_name:
            return {"error": "Table name must be specified for DROP INDEX"}
            
        if not index_name:
            return {"error": "Index name must be specified for DROP INDEX"}
        
        # Drop the index in MongoDB
        db_name = self.catalog_manager.get_current_database()
        
        # Remove the index from the catalog
        try:
            result = self.catalog_manager.drop_index(table_name, index_name)
            if result:
                # Also remove from the index manager
                self.index_manager.drop_index(f"{table_name}.{index_name}")
                return {"message": f"Index '{index_name}' dropped from table '{table_name}'."}
            else:
                return {"error": f"Failed to drop index '{index_name}' from table '{table_name}'."}
        except Exception as e:
            return {"error": f"Error dropping index: {str(e)}"}

    def execute_show(self, plan):
        """
        Execute SHOW commands.
        """
        object_type = plan.get('object')
        
        if not object_type:
            return {"error": "No object type specified for SHOW command"}
        
        if object_type.upper() == 'DATABASES':
            # List all user databases
            databases = self.catalog_manager.get_databases()
            return {
                "columns": ["Database Name"],
                "rows": [[db] for db in databases]
            }
        
        elif object_type.upper() == "TABLES":
            # List all tables in the current database
            tables = self.catalog_manager.get_tables()
            return {
                "columns": ["Table Name"],
                "rows": [[table] for table in tables]
            }
        
        elif object_type.upper() == "INDEXES":
            # List indexes
            table_name = plan.get('table')
            
            if table_name:
                # Show indexes for a specific table
                indexes = self.catalog_manager.get_indexes_for_table(table_name)
                return {
                    "columns": ["Index Name", "Column", "Unique"],
                    "rows": [[idx, info.get('column', ''), "Yes" if info.get('unique', False) else "No"] 
                            for idx, info in indexes.items()]
                }
            else:
                # Show all indexes
                all_indexes = self.catalog_manager.get_all_indexes()
                return {
                    "columns": ["Table", "Index Name", "Column", "Unique"],
                    "rows": [[table, idx, info.get('column', ''), "Yes" if info.get('unique', False) else "No"] 
                            for table, indexes in all_indexes.items() 
                            for idx, info in indexes.items()]
                }
        
        else:
            return {"error": f"Unknown object type: {object_type} for SHOW command"}

    def execute_create_table(self, plan):
        """
        Execute CREATE TABLE operation.
        """
        table_name = plan.get('table')
        column_strings = plan.get('columns', [])
        
        if not table_name:
            return {"error": "No table name specified"}
        
        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected. Use 'USE database_name' first."}
        
        db = self.client[db_name]
        
        # Check if table already exists
        if table_name in db.list_collection_names():
            return {"error": f"Table '{table_name}' already exists in database '{db_name}'"}
        
        # Parse column definitions
        columns = []
        constraints = []
        
        for col_str in column_strings:
            if col_str.upper().startswith(('PRIMARY', 'FOREIGN', 'UNIQUE', 'CHECK', 'CONSTRAINT')):
                constraints.append(col_str)
                continue
            
            parts = col_str.split()
            if len(parts) >= 2:
                col_name = parts[0]
                col_type = parts[1]
                
                # Extract any inline constraints
                col_constraints = ' '.join(parts[2:]) if len(parts) > 2 else ''
                
                columns.append({
                    'name': col_name,
                    'type': col_type,
                    'constraints': col_constraints
                })
        
        # Create the table in MongoDB and catalog
        try:
            self.catalog_manager.create_table(table_name, columns, constraints)
            return {"message": f"Table '{table_name}' created in database '{db_name}'"}
        except Exception as e:
            logging.error(f"Error creating table: {str(e)}")
            return {"error": f"Error creating table: {str(e)}"}

    def execute_drop_table(self, plan):
        """
        Execute DROP TABLE operation.
        """
        table_name = plan['table']
        
        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected. Use 'USE database_name' first."}
        
        db = self.client[db_name]
        
        # Check if the table exists
        if table_name not in db.list_collection_names():
            return {"error": f"Table '{table_name}' does not exist in database '{db_name}'."}
        
        # Drop the table (collection) in MongoDB
        db.drop_collection(table_name)
        
        # Remove the table from the catalog
        self.catalog_manager.drop_table(table_name)
        
        # Drop any associated indexes
        indexes = self.catalog_manager.get_indexes_for_table(table_name)
        for index_name in indexes:
            self.index_manager.drop_index(f"{table_name}.{index_name}")
        
        return {"message": f"Table '{table_name}' dropped successfully from database '{db_name}'."}
    
    def execute_visualize(self, plan):
        """
        Execute a VISUALIZE command.
        """
        object_type = plan.get('object')
        
        if object_type == "INDEX":
            return self.execute_visualize_index(plan)
        
        return {"error": f"Unknown visualization object: {object_type}"}

    def execute_visualize_index(self, plan):
        """
        Execute a VISUALIZE INDEX command.
        """
        index_name = plan.get('index_name')
        table_name = plan.get('table')
        
        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected. Use 'USE database_name' first."}
        
        # Determine the index name format based on parameters
        if table_name and index_name:
            full_index_name = f"{table_name}.{index_name}"
        elif table_name:
            # Visualize all indexes for the table
            indexes = self.catalog_manager.get_indexes_for_table(table_name)
            if not indexes:
                return {"error": f"No indexes found for table '{table_name}'"}
                
            results = []
            for idx_name in indexes:
                full_index_name = f"{table_name}.{idx_name}"
                result = self.visualize_index(full_index_name)
                if result:
                    results.append(result)
                    
            return {
                "message": f"Visualized {len(results)} indexes for table '{table_name}'",
                "visualizations": results
            }
        else:
            # Visualize all indexes
            count = self.visualize_all_indexes()
            return {"message": f"Visualized {count} indexes"}
        
        # Visualize specific index
        result = self.visualize_index(full_index_name)
        
        if result:
            return {
                "message": f"Index '{full_index_name}' visualized successfully",
                "visualization": result
            }
        else:
            return {"error": f"Failed to visualize index '{full_index_name}'"}
        
    def visualize_index(self, index_name):
        """
        Visualize a single index.
        """
        # Get the index from the index manager
        index = self.index_manager.get_index(index_name)
        if not index:
            return None
            
        # Generate a visualization
        visualization_path = f"visualizations/{index_name}_visualization.png"
        try:
            # Check if we have the BPlusTreeVisualizer
            from bptree import BPlusTreeVisualizer
            visualizer = BPlusTreeVisualizer()
            actual_path = index.visualize(visualizer, output_name=index_name)
            
            return {
                "index_name": index_name,
                "visualization_path": actual_path or visualization_path
            }
        except ImportError:
            logging.warning("BPlusTreeVisualizer not available. Install graphviz for visualizations.")
            return {
                "index_name": index_name,
                "error": "Visualization libraries not available"
            }
        
    def visualize_all_indexes(self):
        """
        Visualize all indexes in the current database.
        """
        try:
            # Check if we're running the visualization code
            count = self.index_manager.visualize_all_indexes()
            return count
        except Exception as e:
            logging.error(f"Error visualizing all indexes: {str(e)}")
            return 0
    
    def execute(self, plan):
        """
        Execute a query plan.
        """
        
        if not plan:
            return {"error": "No execution plan provided"}
        
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
        elif plan_type == 'USE_DATABASE':
            return self.execute_use_database(plan)
        elif plan_type == "VISUALIZE":
            return self.execute_visualize(plan)
        else:
            return {"error": f"Unknown plan type: {plan_type}"}

    def execute_use_database(self, plan):
        """
        Execute USE DATABASE operation.
        """
        database_name = plan.get('database')
        
        if not database_name:
            return {"error": "No database name specified"}
        
        # Check if the database exists
        client = self.client
        all_dbs = client.list_database_names()
        
        # MongoDB is case-sensitive, so check exact match
        if database_name not in all_dbs:
            # Try to create it if it doesn't exist
            client[database_name]
            logging.info(f"Database '{database_name}' not found, created.")
        
        # Set the current database
        self.current_database = database_name
        
        # Also store in catalog manager
        self.catalog_manager.set_current_database(database_name)
        
        return {"message": f"Now using database '{database_name}'"}