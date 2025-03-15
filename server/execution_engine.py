from pymongo import MongoClient
from bson import ObjectId

class ExecutionEngine:
    def __init__(self, catalog_manager, index_manager):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager
        self.client = MongoClient('localhost', 27017)
    
    def execute(self, plan):
        """
        Execute the given plan.
        """
        if plan['type'] == "SELECT":
            return self.execute_select(plan)
        elif plan['type'] == "INSERT":
            return self.execute_insert(plan)
        elif plan['type'] == "DELETE":
            return self.execute_delete(plan)
        else:
            raise ValueError("Unsupported plan type.")
    
    def execute_select(self, plan):
        """
        Execute a SELECT query.
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
            
            # Query MongoDB
            result = list(collection.find({column: value}))
            return result
        else:
            # Full table scan
            return list(collection.find())
    
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