import logging
import json
import re
import traceback
import os
from bptree import BPlusTree
import datetime
import shutil

class ExecutionEngine:
    def __init__(self, catalog_manager, index_manager):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager
        self.current_database = catalog_manager.get_current_database()
        self.transaction_stack = []
        self.preferences = self.catalog_manager.get_preferences()
    
    def _parse_value(self, value_str):
        """
        Parse a value string into the appropriate Python type.
        
        Args:
            value_str: String representation of a value
            
        Returns:
            The value converted to the appropriate type
        """
        # Handle quoted strings
        if value_str.startswith('"') or value_str.startswith("'"):
            # Remove quotes
            return value_str[1:-1] if value_str.endswith('"') or value_str.endswith("'") else value_str[1:]
        # Handle NULL keyword
        elif value_str.upper() == 'NULL':
            return None
        # Handle boolean literals
        elif value_str.upper() in ('TRUE', 'FALSE'):
            return value_str.upper() == 'TRUE'
        else:
            # Try to convert to number if possible
            try:
                if '.' in value_str:
                    return float(value_str)
                else:
                    return int(value_str)
            except ValueError:
                return value_str
    
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
                    value = value[1:-1]  # Remove quotes
                # Handle numbers
                elif value.isdigit():
                    value = int(value)
                elif re.match(r'^[0-9]*\.[0-9]+$', value):
                    value = float(value)
                
                # MongoDB is case-sensitive for field names
                # For consistency, use lowercase field names everywhere
                column_lower = column.lower()
                
                # Build MongoDB query with correct operator
                if op == '=':
                    return {column_lower: value}
                else:
                    return {column_lower: {operators[op]: value}}
        
        # If no operator found, return empty filter with a warning
        logging.warning(f"Could not parse condition: {condition_str}")
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
        """Execute INSERT operation."""
        table_name = plan.get('table')
        columns = plan.get('columns', [])
        values_list = plan.get('values', [])
        
        # Validate that we have a table name
        if not table_name:
            return {"error": "No table name specified for INSERT", "status": "error", "type": "error"}
        
        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected. Use 'USE database_name' first.", "status": "error", "type": "error"}
        
        # Validate table exists
        tables = self.catalog_manager.list_tables(db_name)
        if table_name not in tables:
            return {"error": f"Table '{table_name}' does not exist in database '{db_name}'", "status": "error", "type": "error"}
        
        # Format values into records
        records = []
        for values in values_list:
            if len(columns) != len(values):
                return {"error": f"Column count ({len(columns)}) does not match value count ({len(values)})", "status": "error", "type": "error"}
            
            record = {}
            for i, col in enumerate(columns):
                record[col] = values[i]
            
            # Insert the record
            try:
                result = self.catalog_manager.insert_record(table_name, record)
                if result is not True:
                    return {"error": str(result), "status": "error", "type": "error"}
                records.append(record)
            except Exception as e:
                return {"error": f"Error inserting record: {str(e)}", "status": "error", "type": "error"}
        
        return {
            "message": f"Inserted {len(records)} record(s) into '{db_name}.{table_name}'", 
            "status": "success", 
            "type": "insert_result",
            "rows": records
        }

    def _parse_condition_to_list(self, condition_str):
        """
        Parse a SQL condition string into a list of condition dictionaries for B+ tree querying.
        
        Handles basic conditions like:
        - age > 30
        - name = 'John'
        - price <= 100
        
        Args:
            condition_str: SQL condition string (e.g., "age > 30")
            
        Returns:
            List of condition dictionaries for query processing
        """
        if not condition_str:
            return []
        
        # Basic parsing for simpler conditions (focus on getting this working first)
        conditions = []
        
        # Check if this is a simple condition with a comparison operator
        for op in ['>=', '<=', '!=', '<>', '=', '>', '<']:
            if op in condition_str:
                parts = condition_str.split(op, 1)
                if len(parts) == 2:
                    column = parts[0].strip()
                    value_str = parts[1].strip()
                    value = self._parse_value(value_str)
                    
                    # Map <> to !=
                    operator = op if op != '<>' else '!='
                    
                    conditions.append({
                        'column': column,
                        'operator': operator,
                        'value': value
                    })
                    break
        
        logging.debug(f"Parsed condition '{condition_str}' to: {conditions}")
        return conditions
        
    def parse_expression(self, tokens, start=0):
        conditions = []
        i = start
        current_condition = None
        current_operator = 'AND'  # Default operator
        
        while i < len(tokens):
            token = tokens[i].upper()
            
            # Handle parenthesized expressions
            if token == '(':
                sub_conditions, end_idx = parse_expression(self, tokens, i + 1)
                
                if current_condition is None:
                    current_condition = {'type': 'group', 'operator': current_operator, 'conditions': sub_conditions}
                else:
                    # Join with current condition using current_operator
                    conditions.append(current_condition)
                    current_condition = {'type': 'group', 'operator': current_operator, 'conditions': sub_conditions}
                
                i = end_idx + 1  # Skip past the closing parenthesis
                continue
            elif token == ')':
                # End of parenthesized expression
                if current_condition is not None:
                    conditions.append(current_condition)
                return conditions, i
            
            # Handle logical operators
            elif token in ('AND', 'OR'):
                current_operator = token
                i += 1
                continue
            
            # Handle NOT operator
            elif token == 'NOT':
                # Look ahead to handle NOT IN, NOT BETWEEN, NOT LIKE
                if i + 1 < len(tokens) and tokens[i + 1].upper() in ('IN', 'BETWEEN', 'LIKE'):
                    token = f"NOT {tokens[i + 1].upper()}"
                    i += 1  # Skip the next token as we've combined it
                else:
                    # Standalone NOT - needs to be applied to the next condition
                    next_cond, end_idx = parse_expression(self, tokens, i + 1)
                    current_condition = {'type': 'NOT', 'condition': next_cond[0] if next_cond else {}}
                    i = end_idx
                    continue
            
            # Process condition based on operator
            if i + 2 < len(tokens):  # Ensure we have at least 3 tokens (column, operator, value)
                column = tokens[i]
                operator = tokens[i + 1].upper()
                
                # Handle special operators
                if operator in ('IN', 'NOT IN'):
                    # Parse IN list: column IN (val1, val2, ...)
                    values = []
                    if i + 3 < len(tokens) and tokens[i + 2] == '(':
                        j = i + 3
                        while j < len(tokens) and tokens[j] != ')':
                            if tokens[j] != ',':
                                values.append(self._parse_value(tokens[j]))
                            j += 1
                        
                        current_condition = {
                            'column': column,
                            'operator': operator,
                            'value': values
                        }
                        i = j + 1  # Skip to after the closing parenthesis
                        continue
                
                elif operator in ('BETWEEN', 'NOT BETWEEN'):
                    # Parse BETWEEN: column BETWEEN value1 AND value2
                    if i + 4 < len(tokens) and tokens[i + 3].upper() == 'AND':
                        value1 = self._parse_value(tokens[i + 2])
                        value2 = self._parse_value(tokens[i + 4])
                        
                        current_condition = {
                            'column': column,
                            'operator': operator,
                            'value': [value1, value2]
                        }
                        i += 5  # Skip all processed tokens
                        continue
                
                elif operator in ('LIKE', 'NOT LIKE'):
                    # Handle LIKE operator
                    pattern = self._parse_value(tokens[i + 2])
                    
                    current_condition = {
                        'column': column,
                        'operator': operator,
                        'value': pattern
                    }
                    i += 3
                    continue
                
                elif operator in ('IS'):
                    # Handle IS NULL or IS NOT NULL
                    if i + 2 < len(tokens) and tokens[i + 2].upper() == 'NULL':
                        current_condition = {
                            'column': column,
                            'operator': 'IS NULL',
                            'value': None
                        }
                        i += 3
                    elif i + 3 < len(tokens) and tokens[i + 2].upper() == 'NOT' and tokens[i + 3].upper() == 'NULL':
                        current_condition = {
                            'column': column,
                            'operator': 'IS NOT NULL',
                            'value': None
                        }
                        i += 4
                    continue
                
                # Standard comparison operators
                elif operator in ('=', '>', '<', '>=', '<=', '!=', '<>'):
                    value = self._parse_value(tokens[i + 2])
                    
                    # Map <> to !=
                    if operator == '<>':
                        operator = '!='
                    
                    current_condition = {
                        'column': column,
                        'operator': operator,
                        'value': value
                    }
                    i += 3
                    continue
            
            # If nothing matched, just advance
            i += 1
            
        # Add the last condition if it exists
        if current_condition is not None:
            conditions.append(current_condition)
        
        return conditions, len(tokens) - 1

    def _flatten_conditions(self, conditions):
        """
        Flatten nested condition structures into a list of simple conditions.
        Used by query engine when not optimizing with complex expressions.
        
        Args:
            conditions: Nested structure of conditions
        
        Returns:
            List of simplified conditions for basic B+ tree queries
        """
        result = []
        
        # For single conditions
        if isinstance(conditions, dict):
            conditions = [conditions]
        
        for condition in conditions:
            if condition.get('type') == 'group':
                # Recursively process sub-conditions
                sub_results = self._flatten_conditions(condition.get('conditions', []))
                result.extend(sub_results)
            elif condition.get('type') == 'NOT':
                # Negate the condition and add it (if simple enough to negate)
                sub_condition = condition.get('condition', {})
                if 'operator' in sub_condition and 'column' in sub_condition:
                    negated = self._negate_condition(sub_condition)
                    if negated:
                        result.append(negated)
            elif 'column' in condition and 'operator' in condition:
                # Standard condition
                result.append(condition)
        
        return result

    def _negate_condition(self, condition):
        """
        Negate a simple condition for the NOT operator.
        
        Args:
            condition: Original condition dictionary
        
        Returns:
            Negated condition dictionary
        """
        op_map = {
            '=': '!=',
            '!=': '=',
            '<>': '=',
            '>': '<=',
            '<': '>=',
            '>=': '<',
            '<=': '>',
            'LIKE': 'NOT LIKE',
            'NOT LIKE': 'LIKE',
            'IN': 'NOT IN',
            'NOT IN': 'IN',
            'BETWEEN': 'NOT BETWEEN',
            'NOT BETWEEN': 'BETWEEN',
            'IS NULL': 'IS NOT NULL',
            'IS NOT NULL': 'IS NULL'
        }
        
        if condition['operator'] in op_map:
            return {
                'column': condition['column'],
                'operator': op_map[condition['operator']],
                'value': condition['value']
            }
        
        # If operator can't be simply negated
        return None
    
    def execute_select(self, plan):
        """Execute a SELECT query and return results."""
        # Get table name
        table_name = plan.get('table')
        if not table_name and 'tables' in plan and plan['tables']:
            table_name = plan['tables'][0]
                
        if not table_name:
            return {"error": "No table specified", "status": "error"}
        
        # Get column list - preserve original case
        columns = plan.get('columns', ['*'])
        
        # Get current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected. Use 'USE database_name' first.", "status": "error"}
        
        try:
            # Verify table exists - CASE INSENSITIVE COMPARISON
            tables = self.catalog_manager.list_tables(db_name)
            
            # Create case-insensitive lookup for table names
            tables_lower = {table.lower(): table for table in tables}
            
            # Try to find the table case-insensitively
            actual_table_name = table_name  # Default
            if table_name.lower() in tables_lower:
                # Use the correct case from the tables list
                actual_table_name = tables_lower[table_name.lower()]
                logging.debug(f"Using case-corrected table name: {actual_table_name} instead of {table_name}")
            elif table_name not in tables:
                return {"error": f"Table '{table_name}' does not exist in database '{db_name}'", "status": "error"}
            
            # Build condition for query
            conditions = []
            condition = plan.get('condition')
            if condition:
                logging.debug(f"Parsing condition: {condition}")
                conditions = self._parse_condition_to_list(condition)
                logging.debug(f"Parsed conditions: {conditions}")
            
            # Query the data using catalog manager - always get all columns first
            results = self.catalog_manager.query_with_condition(actual_table_name, conditions, ['*'])
            
            logging.debug(f"Query returned {len(results) if results else 0} results")
            
            # Format results for client display
            if not results:
                return {
                    "columns": columns if '*' not in columns else [],
                    "rows": [],
                    "status": "success"
                }
            
            # Create a mapping of lowercase column names to original case
            column_case_map = {}
            if results:
                first_record = results[0]
                for col_name in first_record:
                    column_case_map[col_name.lower()] = col_name
            
            # Apply ORDER BY if specified
            order_by = plan.get('order_by')
            if order_by and results:
                # Extract ordering information
                order_info = None
                direction = 'ASC'
                
                # Handle different order_by formats
                if isinstance(order_by, dict):
                    order_col = order_by.get('column', '')
                    direction = order_by.get('direction', 'ASC')
                    order_info = f"{order_col} {direction}"
                elif isinstance(order_by, list) and order_by:
                    if isinstance(order_by[0], dict):
                        order_col = order_by[0].get('column', '')
                        direction = order_by[0].get('direction', 'ASC')
                        order_info = f"{order_col} {direction}"
                    else:
                        order_info = str(order_by[0])
                else:
                    order_info = str(order_by)
                
                # Extract column name and direction - case insensitive
                parts = order_info.split()
                order_col = parts[0] if parts else ""
                direction = parts[1].upper() if len(parts) > 1 else "ASC"
                desc = direction == 'DESC'
                
                # Log using original case for debugging clarity
                logging.debug(f"Processing ORDER BY: {order_col} {direction}")
                
                # Get the original case version of the column name if available
                for col in column_case_map:
                    if col.lower() == order_col.lower():
                        order_col = column_case_map[col.lower()]
                        break
                
                # Sort the results
                if order_col:
                    # Define a case-insensitive sort key function
                    def get_sort_key(record):
                        # Find the matching column name (case insensitive)
                        column_value = None
                        for record_col in record:
                            if record_col.lower() == order_col.lower():
                                column_value = record[record_col]
                                break
                        
                        # Handle None values for sorting
                        if column_value is None:
                            return (1, None) if not desc else (0, None)
                        
                        return (0, column_value)
                    
                    # Apply the sort
                    results.sort(key=get_sort_key, reverse=desc)
                    logging.debug(f"Sorted {len(results)} rows by {order_col} {direction}")
            
            # Apply LIMIT if specified
            limit = plan.get('limit')
            if limit is not None and isinstance(limit, int) and results and limit > 0:
                results = results[:limit]
                logging.debug(f"Applied LIMIT {limit}, now {len(results)} results")
            
            # Project columns (select specific columns or all)
            result_columns = []
            result_rows = []
            
            # Figure out which columns to include in the result, with original case
            if '*' in columns:
                # Use all columns from first record (original case)
                result_columns = list(results[0].keys())
            else:
                # For specific columns, find the original case from data
                result_columns = []
                for col in columns:
                    # Find matching column with original case
                    original_case_col = None
                    for actual_col in column_case_map:
                        if actual_col.lower() == col.lower():
                            original_case_col = column_case_map[actual_col]
                            break
                    
                    # Use original case if found, otherwise use as provided
                    result_columns.append(original_case_col or col)
            
            # Create rows with the selected columns
            for record in results:
                row = []
                for column_name in result_columns:
                    # Find the matching column case-insensitively
                    value = None
                    for record_col in record:
                        if record_col.lower() == column_name.lower():
                            value = record[record_col]
                            break
                    row.append(value)
                result_rows.append(row)
            
            logging.debug(f"Final result: {len(result_rows)} rows, {len(result_columns)} columns")
            
            return {
                "columns": result_columns,  # Original case preserved
                "rows": result_rows,
                "status": "success"
            }
            
        except Exception as e:
            import traceback
            logging.error(f"Error executing SELECT: {str(e)}")
            logging.error(traceback.format_exc())
            return {"error": f"Error executing SELECT: {str(e)}", "status": "error"}

    def _parse_condition_to_dict(self, condition_str):
        """Parse a condition string into a condition dictionary for B+ tree querying."""
        if not condition_str:
            return {}
        
        # Basic operators mapping
        operators = {
            '=': '=',
            '>': '>',
            '<': '<',
            '>=': '>=',
            '<=': '<=',
            '!=': '!='
        }
        
        # Try to match an operator
        for op in sorted(operators.keys(), key=len, reverse=True):  # Process longer ops first
            if op in condition_str:
                column, value = condition_str.split(op, 1)
                column = column.strip()
                value = value.strip()
                
                # Handle quoted strings
                if value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]  # Remove quotes
                # Handle numbers
                elif value.isdigit():
                    value = int(value)
                elif re.match(r'^[0-9]*\.[0-9]+$', value):
                    value = float(value)
                
                return {
                    'column': column,
                    'operator': operators[op],
                    'value': value
                }
        
        # If no operator found, return empty filter with a warning
        logging.warning(f"Could not parse condition: {condition_str}")
        return {}
    
    def execute_distinct(self, plan):
        """
        Execute a DISTINCT query.
        """
        table_name = plan['table']
        column = plan['column']
        
        # Get current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected. Use 'USE database_name' first.", "status": "error"}
        
        # Verify the table exists
        if not self.catalog_manager.list_tables(db_name) or table_name not in self.catalog_manager.list_tables(db_name):
            return {"error": f"Table '{table_name}' does not exist", "status": "error"}
        
        # Use catalog manager to get data
        results = self.catalog_manager.query_with_condition(table_name, [], [column])
        
        # Extract distinct values
        distinct_values = set()
        for record in results:
            if column in record and record[column] is not None:
                distinct_values.add(record[column])
        
        return {
            "columns": [column],
            "rows": [[value] for value in distinct_values],
            "status": "success"
        }

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
        
        # Get all records from the table using catalog manager
        table_name = plan['table']
        all_docs = self.catalog_manager.query_with_condition(table_name, [], ['*'])
        
        # Filter out records that are in the result
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
        """Execute DROP DATABASE operation."""
        database_name = plan.get('database')
        
        if not database_name:
            return {"error": "No database name specified"}
        
        # Use catalog manager to drop database
        result = self.catalog_manager.drop_database(database_name)
        
        # If this was the current database, reset it
        if self.catalog_manager.get_current_database() == database_name:
            # Choose another database if available
            available_dbs = self.catalog_manager.list_databases()
            if available_dbs:
                self.catalog_manager.set_current_database(available_dbs[0])
            else:
                self.catalog_manager.set_current_database(None)
        
        return {"message": result, "status": "success", "type": "drop_database_result"}
        
    def execute_use_database(self, plan):
        """Execute USE DATABASE operation."""
        database_name = plan.get('database')
        
        if not database_name:
            return {"error": "No database name specified"}
        
        # Check if database exists
        all_dbs = self.catalog_manager.list_databases()
        
        if database_name not in all_dbs:
            return {"error": f"Database '{database_name}' does not exist"}
        
        # Set the current database
        self.catalog_manager.set_current_database(database_name)
        self.current_database = database_name
        
        return {"message": f"Now using database '{database_name}'"}

    def execute_show(self, plan):
        """Execute SHOW commands."""
        object_type = plan.get('object')
        
        if not object_type:
            return {"error": "No object type specified for SHOW command", "status": "error"}
        
        if object_type.upper() == 'DATABASES':
            # List all databases
            databases = self.catalog_manager.list_databases()
            return {
                "columns": ["Database Name"],
                "rows": [[db] for db in databases],
                "status": "success"
            }
        
        elif object_type.upper() == "TABLES":
            # List all tables in the current database
            db_name = self.catalog_manager.get_current_database()
            if not db_name:
                return {"error": "No database selected. Use 'USE database_name' first.", "status": "error"}
                
            tables = self.catalog_manager.list_tables(db_name)
            return {
                "columns": ["Table Name"],
                "rows": [[table] for table in tables],
                "status": "success"
            }
        
        elif object_type.upper() == "INDEXES":
            # Show indexes
            table_name = plan.get('table')
            db_name = self.catalog_manager.get_current_database()
            if not db_name:
                return {"error": "No database selected. Use 'USE database_name' first.", "status": "error"}
            
            if table_name:
                # Show indexes for specific table
                indexes = self.catalog_manager.get_indexes_for_table(table_name)
                if not indexes:
                    return {
                        "columns": ["Table", "Column", "Index Name", "Type"],
                        "rows": [],
                        "status": "success",
                        "message": f"No indexes found for table '{table_name}'"
                    }
                    
                rows = []
                for idx_name, idx_info in indexes.items():
                    rows.append([
                        table_name,
                        idx_info.get("column", ""),
                        idx_name,
                        idx_info.get("type", "BTREE")
                    ])
                
                return {
                    "columns": ["Table", "Column", "Index Name", "Type"],
                    "rows": rows,
                    "status": "success"
                }
            else:
                # Show all indexes in the current database
                all_indexes = []
                for index_id, index_info in self.catalog_manager.indexes.items():
                    if index_id.startswith(f"{db_name}."):
                        parts = index_id.split('.')
                        if len(parts) >= 3:
                            table = parts[1]
                            column = index_info.get("column", "")
                            index_name = parts[2]
                            index_type = index_info.get("type", "BTREE")
                            all_indexes.append([table, column, index_name, index_type])
                
                return {
                    "columns": ["Table", "Column", "Index Name", "Type"],
                    "rows": all_indexes,
                    "status": "success"
                }
        
        else:
            return {"error": f"Unknown object type: {object_type} for SHOW command", "status": "error"}

    def execute_create_table(self, plan):
        """Execute CREATE TABLE operation."""
        table_name = plan.get('table')
        column_strings = plan.get('columns', [])
        
        if not table_name:
            return {"error": "No table name specified", "status": "error", "type": "error"}
        
        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected. Use 'USE database_name' first.", "status": "error", "type": "error"}
        
        # Check if table already exists
        existing_tables = self.catalog_manager.list_tables(db_name)
        if table_name in existing_tables:
            return {"error": f"Table '{table_name}' already exists in database '{db_name}'", "status": "error", "type": "error"}
        
        # Parse column definitions
        columns = []
        constraints = []
        
        for col_str in column_strings:
            if col_str.upper().startswith(('PRIMARY', 'FOREIGN', 'UNIQUE', 'CHECK', 'CONSTRAINT')):
                constraints.append(col_str)
                continue
            
            # Extract column definition parts
            parts = col_str.split()
            if len(parts) >= 2:
                col_name = parts[0]
                col_type = parts[1]
                
                # Create column definition
                col_def = {
                    "name": col_name,
                    "type": col_type
                }
                
                # Check for additional column attributes
                col_str_upper = col_str.upper()
                
                # Handle PRIMARY KEY
                if "PRIMARY KEY" in col_str_upper:
                    col_def["primary_key"] = True
                
                # Handle NOT NULL
                if "NOT NULL" in col_str_upper:
                    col_def["nullable"] = False
                
                # Handle IDENTITY(seed, increment)
                if "IDENTITY" in col_str_upper:
                    col_def["identity"] = True
                    
                    # Extract seed and increment values if specified
                    identity_match = re.search(r"IDENTITY\s*\((\d+),\s*(\d+)\)", col_str, re.IGNORECASE)
                    if identity_match:
                        col_def["identity_seed"] = int(identity_match.group(1))
                        col_def["identity_increment"] = int(identity_match.group(2))
                    else:
                        # Default seed=1, increment=1
                        col_def["identity_seed"] = 1
                        col_def["identity_increment"] = 1
                
                # Add the column definition
                columns.append(col_def)
        
        # Create the table through catalog manager
        try:
            result = self.catalog_manager.create_table(table_name, columns, constraints)
            if result is True:
                return {"message": f"Table '{table_name}' created in database '{db_name}'", "status": "success", "type": "create_table_result"}
            else:
                return {"error": result, "status": "error", "type": "error"}
        except Exception as e:
            logging.error(f"Error creating table: {str(e)}")
            return {"error": f"Error creating table: {str(e)}", "status": "error", "type": "error"}

    def execute_drop_table(self, plan):
        """Execute DROP TABLE operation."""
        table_name = plan.get('table')
        
        if not table_name:
            return {"error": "No table name specified"}
        
        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected. Use 'USE database_name' first."}
        
        # Use catalog manager to drop the table
        try:
            result = self.catalog_manager.drop_table(table_name)
            
            if isinstance(result, str) and "does not exist" in result:
                return {"error": result}
                
            return {"message": f"Table '{table_name}' dropped from database '{db_name}'"}
        except Exception as e:
            logging.error(f"Error dropping table: {str(e)}")
            return {"error": f"Error dropping table: {str(e)}"}
    
    def execute_get_table_data(self, table_name):
        """Get all data from a table."""
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."
        
        table_id = f"{db_name}.{table_name}"
        
        # Check if table exists
        if table_id not in self.tables:
            return f"Table {table_name} does not exist."
        
        # Load the B+ tree
        table_file = os.path.join(self.tables_dir, db_name, f"{table_name}.tbl")
        if not os.path.exists(table_file):
            return f"Table data file not found."
        
        tree = BPlusTree.load_from_file(table_file)
        
        # Get all records using a wide range query
        results = tree.range_query(float('-inf'), float('inf'))
        
        return results

    def execute_create_index(self, plan):
        """Execute CREATE INDEX operation."""
        index_name = plan.get('index_name')
        table_name = plan.get('table')
        column = plan.get('column')
        is_unique = plan.get('unique', False)
        
        if not table_name:
            return {"error": "Table name must be specified for CREATE INDEX"}
        
        if not column:
            return {"error": "Column name must be specified for CREATE INDEX"}
        
        logging.info(f"Creating index '{index_name}' on {table_name}.{column}")
        
        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected. Use 'USE database_name' first."}
        
        # Verify table exists
        tables = self.catalog_manager.list_tables(db_name)
        if table_name not in tables:
            return {"error": f"Table '{table_name}' does not exist in database '{db_name}'"}
        
        # Create the index
        try:
            result = self.catalog_manager.create_index(table_name, column, "BTREE", is_unique)
            if isinstance(result, str) and "already exists" in result:
                return {"error": result}
                
            return {"message": f"Index '{index_name}' created on '{table_name}.{column}'"}
        except Exception as e:
            logging.error(f"Error creating index: {str(e)}")
            return {"error": f"Error creating index: {str(e)}"}
    
    def execute_drop_index(self, plan):
        """Execute DROP INDEX operation."""
        index_name = plan['index_name']
        table_name = plan['table']
        
        if not table_name:
            return {"error": "Table name must be specified for DROP INDEX"}
            
        if not index_name:
            return {"error": "Index name must be specified for DROP INDEX"}
        
        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected. Use 'USE database_name' first."}
        
        # Drop the index through catalog_manager
        try:
            result = self.catalog_manager.drop_index(table_name, index_name)
            if isinstance(result, str) and "does not exist" in result:
                return {"error": result}
                
            return {"message": f"Index '{index_name}' dropped from table '{table_name}'."}
        except Exception as e:
            logging.error(f"Error dropping index: {str(e)}")
            return {"error": f"Error dropping index: {str(e)}"}
    
    def drop_index(self, table_name, column_name):
        """Drop an index from a table column."""
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."
        
        table_id = f"{db_name}.{table_name}"
        index_id = f"{table_id}.{column_name}"
        
        # Check if index exists
        if index_id not in self.indexes:
            return f"Index on {table_name}.{column_name} does not exist."
        
        # Remove from indexes catalog
        del self.indexes[index_id]
        
        # Remove physical file
        index_file = os.path.join(self.indexes_dir, f"{db_name}_{table_name}_{column_name}.idx")
        if os.path.exists(index_file):
            os.remove(index_file)
        
        # Save changes
        self._save_json(self.indexes_file, self.indexes)
        
        logging.info(f"Index dropped on {table_name}.{column_name}")
        return f"Index dropped on {table_name}.{column_name}"
    
    def query_with_condition(self, table_name, conditions=None, columns=None):
        """Query table data with conditions."""
        if conditions is None:
            conditions = []
        if columns is None:
            columns = ['*']
        
        db_name = self.get_current_database()
        if not db_name:
            return []
        
        # Case-insensitive table lookup
        actual_table_name = None
        db_tables = self.list_tables(db_name)
        
        # Direct match first
        if table_name in db_tables:
            actual_table_name = table_name
        else:
            # Try case-insensitive match
            for db_table in db_tables:
                if db_table.lower() == table_name.lower():
                    actual_table_name = db_table
                    break
        
        if not actual_table_name:
            logging.warning(f"Table '{table_name}' not found in database '{db_name}'")
            return []
        
        # Use the correct table name for the lookup
        table_id = f"{db_name}.{actual_table_name}"
        
        # Check if table exists
        if table_id not in self.tables:
            return []
        
        # Load the table file
        table_file = os.path.join(self.tables_dir, db_name, f"{actual_table_name}.tbl")
        if not os.path.exists(table_file):
            return []
        
        try:
            # Load B+ tree
            tree = BPlusTree.load_from_file(table_file)
            
            # Rest of the method remains the same...
            # Get all records
            all_records = tree.range_query(float('-inf'), float('inf'))
            
            # Process records and apply conditions
            results = []
            for record_key, record in all_records:
                # Check if record matches all conditions
                matches = True
                for condition in conditions:
                    col = condition.get('column')
                    op = condition.get('operator')
                    val = condition.get('value')
                    
                    if col not in record:
                        matches = False
                        break
                    
                    # Apply operator
                    if op == '=':
                        if record[col] != val:
                            matches = False
                            break
                    elif op == '>':
                        if record[col] <= val:
                            matches = False
                            break
                    elif op == '<':
                        if record[col] >= val:
                            matches = False
                            break
                    elif op == '>=':
                        if record[col] < val:
                            matches = False
                            break
                    elif op == '<=':
                        if record[col] > val:
                            matches = False
                            break
                    elif op == '!=':
                        if record[col] == val:
                            matches = False
                            break
                
                if matches:
                    # Project selected columns
                    if columns == ['*']:
                        results.append(record)
                    else:
                        projected = {}
                        for col in columns:
                            if col in record:
                                projected[col] = record[col]
                        results.append(projected)
            
            return results
        
        except Exception as e:
            logging.error(f"Error querying table: {e}")
            return []

    def execute_begin_transaction(self):
        """Begin a new transaction."""
        result = self.catalog_manager.begin_transaction()
        return {"message": result}
    
    def execute_commit_transaction(self):
        """Commit the current transaction."""
        result = self.catalog_manager.commit_transaction()
        return {"message": result}
    
    def execute_rollback_transaction(self):
        """Rollback the current transaction."""
        result = self.catalog_manager.rollback_transaction()
        return {"message": result}

    def update_records(self, table_name, updates, conditions=None):
        """Update records in a table based on conditions."""
        if conditions is None:
            conditions = []
            
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."
        
        table_id = f"{db_name}.{table_name}"
        
        # Check if table exists
        if table_id not in self.tables:
            return f"Table {table_name} does not exist."
        
        # Load the B+ tree
        table_file = os.path.join(self.tables_dir, db_name, f"{table_name}.tbl")
        if not os.path.exists(table_file):
            return f"Table data file not found."
        
        tree = BPlusTree.load_from_file(table_file)
        
        # Get all records
        all_records = tree.range_query(float('-inf'), float('inf'))
        
        # Apply updates based on conditions
        records_updated = 0
        for record_key, record in all_records:
            # Check if record matches conditions
            matches = True
            for condition in conditions:
                col = condition.get('column')
                op = condition.get('operator')
                val = condition.get('value')
                
                if col not in record:
                    matches = False
                    break
                    
                if op == '=':
                    if record[col] != val:
                        matches = False
                        break
                elif op == '>':
                    if record[col] <= val:
                        matches = False
                        break
                # ... (other operators)
            
            if matches:
                # Apply updates
                updated_record = record.copy()
                for field, value in updates.items():
                    updated_record[field] = value
                
                # Remove old record and insert updated one
                tree.insert(record_key, updated_record)
                records_updated += 1
        
        # Save the updated tree
        tree.save_to_file(table_file)
        
        # Update indexes
        self._update_indexes_after_modify(db_name, table_name, all_records)
        
        return f"{records_updated} records updated."
    
    def execute_delete(self, plan):
        """Execute a DELETE query."""
        table_name = plan['table']
        condition = plan.get('condition') or plan.get('where')  # Try both fields
        
        # Get current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected. Use 'USE database_name' first.", "status": "error", "type": "error"}
        
        # Verify table exists
        tables = self.catalog_manager.list_tables(db_name)
        
        # Case-insensitive table lookup
        actual_table_name = table_name  # Default
        if table_name.lower() in [t.lower() for t in tables]:
            for t in tables:
                if t.lower() == table_name.lower():
                    actual_table_name = t
                    break
        elif table_name not in tables:
            return {"error": f"Table '{table_name}' does not exist in database '{db_name}'.", "status": "error", "type": "error"}
        
        try:
            # Parse condition into our format
            conditions = []
            if condition:
                logging.debug(f"Parsing DELETE condition: {condition}")
                conditions = self._parse_condition_to_list(condition)
                logging.debug(f"Parsed DELETE conditions: {conditions}")
            
            # Delete records
            result = self.catalog_manager.delete_records(actual_table_name, conditions)
            
            # Extract count from result message
            count = 0
            if isinstance(result, str):
                try:
                    count = int(result.split()[0])
                except:
                    pass
                    
            return {
                "message": f"Deleted {count} records from {actual_table_name}.", 
                "status": "success", 
                "type": "delete_result",
                "count": count
            }
        except Exception as e:
            logging.error(f"Error in DELETE operation: {str(e)}")
            logging.error(traceback.format_exc())
            return {"error": f"Error in DELETE operation: {str(e)}", "status": "error", "type": "error"}

    def execute_update(self, plan):
        """Execute an UPDATE query."""
        table_name = plan['table']
        condition = plan.get('condition') or plan.get('where')  # Try both fields
        updates = plan.get('set', {})
        
        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected. Use 'USE database_name' first.", "status": "error"}
            
        # Verify table exists
        tables = self.catalog_manager.list_tables(db_name)
        if table_name not in tables:
            return {"error": f"Table '{table_name}' does not exist in database '{db_name}'.", "status": "error"}
        
        try:
            # Parse the condition
            conditions = []
            if condition:
                parsed_condition = self._parse_condition_to_dict(condition)
                if parsed_condition:
                    conditions.append(parsed_condition)
            
            # Process updates
            update_dict = {}
            for key, val in updates.items():
                # Handle quoted strings
                if isinstance(val, str) and val.startswith("'") and val.endswith("'"):
                    val = val[1:-1]  # Remove quotes
                # Handle numeric conversions
                elif isinstance(val, str) and val.isdigit():
                    val = int(val)
                elif isinstance(val, str) and re.match(r'^[0-9]*\.[0-9]+$', val):
                    val = float(val)
                
                update_dict[key] = val
            
            if not update_dict:
                return {"error": "No fields to update specified", "status": "error"}
            
            # Update records
            result = self.catalog_manager.update_records(table_name, update_dict, conditions)
            
            # Extract count from result message
            count = 0
            if isinstance(result, str):
                try:
                    count = int(result.split()[0])
                except:
                    pass
            
            return {"message": f"Updated {count} records in {table_name}.", "status": "success"}
        except Exception as e:
            logging.error(f"Error in UPDATE operation: {str(e)}")
            logging.error(traceback.format_exc())
            return {"error": f"Error in UPDATE operation: {str(e)}", "status": "error"}
    
    def _update_indexes_after_modify(self, db_name, table_name, current_records):
        """Update all indexes for a table after records are modified."""
        table_id = f"{db_name}.{table_name}"
        
        # Find all indexes for this table
        table_indexes = []
        for index_id, index_def in self.indexes.items():
            if index_def.get("table") == table_id:
                table_indexes.append((index_id, index_def))
        
        # Rebuild each index
        for index_id, index_def in table_indexes:
            column_name = index_def.get("column")
            index_file = os.path.join(self.indexes_dir, f"{db_name}_{table_name}_{column_name}.idx")
            
            # Create a new index tree
            index_tree = BPlusTree(order=50, name=f"{table_name}_{column_name}_index")
            
            # Populate the index with current records
            for record_key, record in current_records:
                if column_name in record:
                    index_tree.insert(record[column_name], record_key)
            
            # Save the updated index
            index_tree.save_to_file(index_file)
            
    def list_databases(self):
        """Get a list of all databases."""
        return list(self.databases.keys())
    
    def list_tables(self, db_name=None):
        """Get a list of tables in a database."""
        if db_name is None:
            db_name = self.get_current_database()
            if not db_name:
                return "No database selected."
        
        if db_name not in self.databases:
            return f"Database {db_name} does not exist."
        
        return self.databases[db_name].get("tables", [])
    
    def get_table_schema(self, table_name):
        """Get the schema of a table."""
        db_name = self.get_current_database()
        if not db_name:
            return "No database selected."
        
        table_id = f"{db_name}.{table_name}"
        
        if table_id not in self.tables:
            return f"Table {table_name} does not exist."
        
        return {
            "columns": self.tables[table_id].get("columns", []),
            "constraints": self.tables[table_id].get("constraints", [])
        }
    
    def execute_visualize(self, plan):
        """
        Execute a VISUALIZE command.
        """
        # Get visualization object type
        object_type = plan.get('object')
        
        if object_type == "BPTREE":
            index_name = plan.get('index_name')
            table_name = plan.get('table')
            
            # Get current database
            db_name = self.catalog_manager.get_current_database()
            if not db_name:
                return {"error": "No database selected. Use 'USE database_name' first.", "status": "error"}
            
            # Get index manager to access B+ trees
            index_manager = self.index_manager
            
            # If both table and index are specified, visualize that specific index
            if table_name and index_name and index_name.upper() != "ON":
                # Try to get the index object
                full_index_name = f"{table_name}.{index_name}"
                index_obj = index_manager.get_index(full_index_name)
                
                if not index_obj:
                    # Get indexes from catalog to see if it exists
                    indexes = self.catalog_manager.get_indexes_for_table(table_name)
                    if not indexes or index_name not in indexes:
                        return {
                            "error": f"Index '{index_name}' not found for table '{table_name}'",
                            "status": "error"
                        }
                    else:
                        # Index exists in catalog but file not found - rebuild it
                        try:
                            logging.info(f"Attempting to rebuild index {index_name} on {table_name}...")
                            column = indexes[index_name].get("column")
                            is_unique = indexes[index_name].get("unique", False)
                            index_obj = index_manager.build_index(table_name, index_name, column, is_unique, db_name)
                        except Exception as e:
                            return {"error": f"Error rebuilding index: {str(e)}", "status": "error"}
                
                # Now visualize the index
                if index_obj:
                    try:
                        # Try NetworkX visualization first
                        try:
                            from bptree_networkx import BPlusTreeNetworkXVisualizer
                            visualizer = BPlusTreeNetworkXVisualizer(output_dir='visualizations')
                            viz_path = visualizer.visualize_tree(index_obj, output_name=full_index_name)
                            
                            if viz_path:
                                # Convert to absolute path
                                abs_path = os.path.abspath(viz_path)
                                logging.info(f"Generated visualization at {abs_path}")
                                
                                # Generate text representation
                                text_repr = self._get_tree_text(index_obj)
                                
                                return {
                                    "message": f"B+ Tree visualization for '{index_name}' on '{table_name}'",
                                    "status": "success",
                                    "visualization_path": abs_path,
                                    "text_representation": text_repr
                                }
                        except ImportError:
                            logging.warning("NetworkX not available, falling back to Graphviz")
                        
                        # Fall back to Graphviz
                        viz_path = index_obj.visualize(
                            index_manager.visualizer, 
                            f"{full_index_name}_visualization"
                        )
                        
                        if viz_path:
                            # Convert to absolute path
                            abs_path = os.path.abspath(viz_path)
                            logging.info(f"Generated visualization at {abs_path}")
                            
                            # Generate text representation
                            text_repr = self._get_tree_text(index_obj)
                            
                            return {
                                "message": f"B+ Tree visualization for '{index_name}' on '{table_name}'",
                                "status": "success",
                                "visualization_path": abs_path,
                                "text_representation": text_repr
                            }
                        else:
                            return {
                                "message": f"Text representation for '{index_name}' on '{table_name}'",
                                "status": "success",
                                "text_representation": self._get_tree_text(index_obj)
                            }
                            
                    except Exception as e:
                        logging.error(f"Error visualizing B+ tree: {str(e)}")
                        return {"error": f"Error visualizing B+ tree: {str(e)}", "status": "error"}
                else:
                    return {"error": f"Could not find or rebuild index '{index_name}'", "status": "error"}
                
    def _get_tree_text(self, tree):
        """Generate a text representation of the tree"""
        lines = [f"B+ Tree '{tree.name}' (Order: {tree.order})"]
        
        def print_node(node, level=0, prefix='Root: '):
            indent = '  ' * level
            
            # Print the current node
            if hasattr(node, 'leaf') and node.leaf:
                key_values = []
                for item in node.keys:
                    if isinstance(item, tuple) and len(item) >= 2:
                        key_values.append(f"{item[0]}:{item[1]}")
                    else:
                        key_values.append(str(item))
                lines.append(f"{indent}{prefix}LEAF {{{', '.join(key_values)}}}")
            else:
                lines.append(f"{indent}{prefix}NODE {{{', '.join(map(str, node.keys))}}}")
            
            # Print children recursively
            if hasattr(node, 'children'):
                for i, child in enumerate(node.children):
                    child_prefix = f"Child {i}: "
                    print_node(child, level + 1, child_prefix)
        
        print_node(tree.root)
        return '\n'.join(lines)
            
    def _count_nodes(self, node):
        """Count total nodes in the tree"""
        if node is None:
            return 0
        count = 1  # Count this node
        if hasattr(node, 'children'):
            for child in node.children:
                count += self._count_nodes(child)
        return count

    def _count_leaves(self, node):
        """Count leaf nodes in the tree"""
        if node is None:
            return 0
        if hasattr(node, 'leaf') and node.leaf:
            return 1
        count = 0
        if hasattr(node, 'children'):
            for child in node.children:
                count += self._count_leaves(child)
        return count

    def _tree_height(self, node, level=0):
        """Calculate the height of the tree"""
        if node is None:
            return level
        if hasattr(node, 'leaf') and node.leaf:
            return level + 1
        if hasattr(node, 'children') and node.children:
            return self._tree_height(node.children[0], level + 1)
        return level + 1

    def execute_visualize_index(self, plan):
        """
        Execute a VISUALIZE INDEX command.
        """
        index_name = plan.get('index_name')
        table_name = plan.get('table')
        
        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected. Use 'USE database_name' first.", "status": "error"}
        
        # Determine the index name format based on parameters
        if table_name and index_name:
            full_index_name = f"{table_name}.{index_name}"
        elif table_name:
            # Visualize all indexes for the table
            indexes = self.catalog_manager.get_indexes_for_table(table_name)
            if not indexes:
                return {"error": f"No indexes found for table '{table_name}'", "status": "error"}
                
            results = []
            for idx_name in indexes:
                full_index_name = f"{table_name}.{idx_name}"
                result = self.visualize_index(full_index_name)
                if result:
                    results.append(result)
                    
            return {
                "message": f"Visualized {len(results)} indexes for table '{table_name}'",
                "visualizations": results,
                "status": "success"
            }
        else:
            # Visualize all indexes
            count = self.visualize_all_indexes()
            return {"message": f"Visualized {count} indexes", "status": "success"}
        
        # Visualize specific index
        result = self.visualize_index(full_index_name)
        
        if result:
            return {
                "message": f"Index '{full_index_name}' visualized successfully",
                "visualization": result,
                "status": "success"
            }
        else:
            return {"error": f"Failed to visualize index '{full_index_name}'", "status": "error"}
        
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
        Execute the query plan.
        """
        # Keep the type field for internal routing only
        plan_type = plan.get('type')
        
        if not plan_type:
            return {"error": "No operation type specified in plan", "status": "error"}
        
        # Process based on query type
        result = None
        logging.debug(f"Executing plan of type {plan_type}: {plan}")
        try:
            if plan_type == 'SELECT':
                result = self.execute_select(plan)
            elif plan_type == 'INSERT':
                result = self.execute_insert(plan)
            elif plan_type == 'UPDATE':
                result = self.execute_update(plan)
            elif plan_type == 'DELETE':
                result = self.execute_delete(plan)
            elif plan_type == 'CREATE_TABLE':
                result = self.execute_create_table(plan)
            elif plan_type == 'DROP_TABLE':
                result = self.execute_drop_table(plan)
            elif plan_type == 'CREATE_DATABASE':
                result = self.execute_create_database(plan)
            elif plan_type == 'DROP_DATABASE':
                result = self.execute_drop_database(plan)
            elif plan_type == 'USE_DATABASE':
                result = self.execute_use_database(plan)
            elif plan_type == 'SHOW':
                result = self.execute_show(plan)
            elif plan_type == 'CREATE_INDEX':
                result = self.execute_create_index(plan)
            elif plan_type == 'DROP_INDEX':
                result = self.execute_drop_index(plan)
            elif plan_type == 'VISUALIZE':
                result = self.execute_visualize(plan)
            elif plan_type == 'BEGIN_TRANSACTION':
                result = self.execute_begin_transaction()
            elif plan_type == 'COMMIT':
                result = self.execute_commit_transaction()
            elif plan_type == 'ROLLBACK':
                result = self.execute_rollback_transaction()
            elif plan_type == 'SET':
                result = self.execute_set_preference(plan)
            elif plan_type == 'CREATE_VIEW':
                result = self.execute_create_view(plan)
            elif plan_type == 'DROP_VIEW':
                result = self.execute_drop_view(plan)
            else:
                return {"error": f"Unsupported operation type: {plan_type}", "status": "error"}
            
            if isinstance(result, dict) and "status" not in result:
                result["status"] = "success"
            
            # Ensure type field is present
            if isinstance(result, dict) and "type" not in result:
                result["type"] = f"{plan_type.lower()}_result"
                
            return result
            
        except Exception as e:
            import traceback
            logging.error(f"Error executing {plan_type}: {str(e)}")
            logging.error(traceback.format_exc())
            return {"error": f"Error executing {plan_type}: {str(e)}", "status": "error", "type": "error"}