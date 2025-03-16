from pymongo import MongoClient
from bson import ObjectId
import json

class ExecutionEngine:
    def __init__(self, catalog_manager, index_manager):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager
        self.client = MongoClient('localhost', 27017)
        self.transaction_stack = []
        self.preferences = self.catalog_manager.get_preferences()
    
    def execute_aggregation(self, plan):
        """
        Execute aggregation queries.
        """
        if plan['type'] == "AVG":
            return self.execute_avg(plan)
        elif plan['type'] == "MIN":
            return self.execute_min(plan)
        elif plan['type'] == "MAX":
            return self.execute_max(plan)
        else:
            raise ValueError("Unsupported aggregation type.")
    
    def execute_avg(self, plan):
        """
        Execute AVG aggregation.
        """
        table_name = plan['table']
        column = plan['column']
        
        db = self.client['dbms_project']
        collection = db[table_name]
        
        total = 0
        count = 0
        for doc in collection.find():
            total += int(doc[column])
            count += 1
        
        return total / count if count > 0 else 0
    
    def execute_min(self, plan):
        """
        Execute MIN aggregation.
        """
        table_name = plan['table']
        column = plan['column']
        
        db = self.client['dbms_project']
        collection = db[table_name]
        
        min_val = None
        for doc in collection.find():
            if min_val is None or doc[column] < min_val:
                min_val = doc[column]
        
        return min_val
    
    def execute_max(self, plan):
        """
        Execute MAX aggregation.
        """
        table_name = plan['table']
        column = plan['column']
        
        db = self.client['dbms_project']
        collection = db[table_name]
        
        max_val = None
        for doc in collection.find():
            if max_val is None or doc[column] > max_val:
                max_val = doc[column]
        
        return max_val
    
    def execute_sum(self, plan):
        """
        Execute SUM aggregation.
        """
        table_name = plan['table']
        column = plan['column']
        
        db = self.client['dbms_project']
        collection = db[table_name]
        
        total = 0
        for doc in collection.find():
            total += int(doc[column])
        
        return total
    
    def execute_count(self, plan):
        """
        Execute COUNT aggregation.
        """
        table_name = plan['table']
        
        db = self.client['dbms_project']
        collection = db[table_name]
        
        return collection.count_documents({})
    
    def execute_top(self, plan):
        """
        Execute TOP N aggregation.
        """
        table_name = plan['table']
        column = plan['column']
        n = plan['n']
        
        db = self.client['dbms_project']
        collection = db[table_name]
        
        result = list(collection.find().sort(column, -1).limit(n))
        return result
    
    def execute_join(self, plan):
        """
        Execute a JOIN query.
        """
        if plan['type'] == "HASH_JOIN":
            return self.execute_hash_join(plan)
        elif plan['type'] == "SORT_MERGE_JOIN":
            return self.execute_sort_merge_join(plan)
        elif plan['type'] == "INDEX_JOIN":
            return self.execute_index_join(plan)
        else:
            return self.execute_nested_loop_join(plan)
    
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
        Execute a SELECT query.
        """
        
        max_results = self.preferences.get("max_results", 10)
        
        table_name = plan['table']
        condition = plan.get('condition')
        subquery = plan.get('subquery')
        
        db = self.client['dbms_project']
        collection = db[plan['table']]
        result = list(collection.find().limit(max_results))
        
        if subquery:
            # Execute the subquery first
            subquery_result = self.execute_select(subquery)
            if not subquery_result:
                return []
            
            # Use the subquery result in the main query
            column, value = condition.split('=')
            column = column.strip()
            value = subquery_result[0][column.strip()]
            
            # Query MongoDB
            result = list(collection.find({column: value}))
            
            if self.preferences.get("pretty_print", False):
                return json.dumps(result, indent=4)
            else:
                return result
        elif condition:
            # Parse the condition (e.g., "id = 1")
            column, value = condition.split('=')
            column = column.strip()
            value = value.strip().strip("'")  # Remove quotes from strings
            
            # Query MongoDB
            result = list(collection.find({column: value}))
            
            if self.preferences.get("pretty_print", False):
                return json.dumps(result, indent=4)
            else:
                return result
        else:
            # Full table scan
            result = list(collection.find())
            
            if self.preferences.get("pretty_print", False):
                return json.dumps(result, indent=4)
            else:
                return result

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
        record = plan['record']
        
        db = self.client['dbms_project']
        collection = db[table_name]
        
        # Insert into MongoDB
        result = collection.insert_one(record)
        
        # Update B+ Tree index if applicable
        for column, index in self.catalog_manager.get_indexes(table_name).items():
            index_name = f"{table_name}.idx_{column}"
            self.index_manager.insert_into_index(index_name, record[column], str(result.inserted_id))
        
        return f"Inserted record into {table_name}."
    
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
    
    def execute(self, plan):
        """
        Execute the given plan.
        """
        if plan['type'] == "SELECT":
            return self.execute_select(plan)
        elif plan['type'] == "INSERT":
            return self.execute_insert(plan)
        elif plan['type'] == "UPDATE":
            return self.execute_update(plan)
        elif plan['type'] == "DELETE":
            return self.execute_delete(plan)
        elif plan['type'] == "CREATE_TABLE":
            return self.execute_create_table(plan)
        elif plan['type'] == "DROP_TABLE":
            return self.execute_drop_table(plan)
        elif plan['type'] == "BEGIN_TRANSACTION":
            return self.execute_begin_transaction()
        elif plan['type'] == "COMMIT_TRANSACTION":
            return self.execute_commit_transaction()
        elif plan['type'] == "ROLLBACK_TRANSACTION":
            return self.execute_rollback_transaction()
        elif plan['type'] == "SET_PREFERENCE":
            return self.execute_set_preference(plan)
        elif plan['type'] == "CREATE_VIEW":
            return self.execute_create_view(plan)
        elif plan['type'] == "DROP_VIEW":
            return self.execute_drop_view(plan)
        else:
            raise ValueError("Unsupported plan type.")