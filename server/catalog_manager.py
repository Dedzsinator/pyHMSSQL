from pymongo import MongoClient
import logging
from hashlib import sha256

class CatalogManager:
    def __init__(self):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['dbms_project']
        self.databases = self.db['databases']
        self.tables = self.db['tables']
        self.indexes = self.db['indexes']
        self.preferences = self.db['preferences']
        self.users = self.db['users']
        self.views = self.db['views']
        self.temp_tables = {}
        self.procedures = self.db['procedures']
        self.functions = self.db['functions']
        self.triggers = self.db['triggers']
        logging.info("CatalogManager initialized.")
    
    def register_user(self, username, password, role="user"):
        """
        Register a new user.
        """
        if self.users.find_one({"username": username}):
            return "Username already exists."
        
        # Hash the password
        hashed_password = sha256(password.encode()).hexdigest()
        
        # Insert the user into the database
        self.users.insert_one({"username": username, "password": hashed_password, "role": role})
        return "User registered successfully."
    
    def authenticate_user(self, username, password):
        """
        Authenticate a user.
        """
        user = self.users.find_one({"username": username})
        if not user:
            return None
        
        # Verify the password
        hashed_password = sha256(password.encode()).hexdigest()
        if user["password"] == hashed_password:
            return user
        else:
            return None
    
    def create_database(self, db_name):
        logging.debug(f"Creating database: {db_name}")
        if self.databases.find_one({"_id": db_name}):
            logging.warning(f"Database {db_name} already exists.")
            return f"Database {db_name} already exists."
        self.databases.insert_one({"_id": db_name, "tables": []})
        logging.info(f"Database {db_name} created.")
        return f"Database {db_name} created."
    
    def drop_database(self, db_name):
        logging.debug(f"Dropping database: {db_name}")
        if not self.databases.find_one({"_id": db_name}):
            logging.warning(f"Database {db_name} does not exist.")
            return f"Database {db_name} does not exist."
        self.databases.delete_one({"_id": db_name})
        self.tables.delete_many({"_id": {"$regex": f"^{db_name}."}})
        self.indexes.delete_many({"_id": {"$regex": f"^{db_name}."}})
        logging.info(f"Database {db_name} dropped.")
        return f"Database {db_name} dropped."
    
    def create_table(self, table_name, columns, constraints):
        """
        Register a new table and its schema in the catalog.
        """
        schema = {
            "columns": columns,
            "constraints": constraints
        }
        
        # Store the schema in a special collection
        self.db['catalog'].insert_one({
            "table_name": table_name,
            "schema": schema
        })
    
    def drop_table(self, db_name, table_name):
        logging.debug(f"Dropping table: {table_name} from database: {db_name}")
        if not self.databases.find_one({"_id": db_name}):
            logging.warning(f"Database {db_name} does not exist.")
            return f"Database {db_name} does not exist."
        table_id = f"{db_name}.{table_name}"
        if not self.tables.find_one({"_id": table_id}):
            logging.warning(f"Table {table_name} does not exist in database {db_name}.")
            return f"Table {table_name} does not exist in database {db_name}."
        self.tables.delete_one({"_id": table_id})
        self.databases.update_one({"_id": db_name}, {"$pull": {"tables": table_name}})
        self.indexes.delete_many({"_id": {"$regex": f"^{db_name}.{table_name}."}})
        logging.info(f"Table {table_name} dropped from database {db_name}.")
        return f"Table {table_name} dropped from database {db_name}."
    
    def create_index(self, db_name, table_name, index_name, column):
        logging.debug(f"Creating index: {index_name} on column: {column} in table: {table_name} of database: {db_name}")
        if not self.databases.find_one({"_id": db_name}):
            logging.warning(f"Database {db_name} does not exist.")
            return f"Database {db_name} does not exist."
        table_id = f"{db_name}.{table_name}"
        if not self.tables.find_one({"_id": table_id}):
            logging.warning(f"Table {table_name} does not exist in database {db_name}.")
            return f"Table {table_name} does not exist in database {db_name}."
        if column not in self.tables.find_one({"_id": table_id})['columns']:
            logging.warning(f"Column {column} does not exist in table {table_name}.")
            return f"Column {column} does not exist in table {table_name}."
        index_id = f"{db_name}.{table_name}.{index_name}"
        self.indexes.insert_one({"_id": index_id, "column": column, "type": "B+ Tree"})
        logging.info(f"Index {index_name} created on column {column} in table {table_name}.")
        return f"Index {index_name} created on column {column} in table {table_name}."
    
    def get_indexes(self, table_name):
        """
        Retrieve all indexes for a table.
        """
        logging.debug(f"Retrieving indexes for table: {table_name}")
        indexes = self.indexes.find({"_id": {"$regex": f"^{table_name}."}})
        logging.info(f"Retrieved indexes for table: {table_name}")
        return {index['column']: index for index in indexes}
    
    def get_preferences(self, user_id=None):
        """
        Retrieve preferences for a user (or global preferences if user_id is None).
        """
        query = {"user_id": user_id} if user_id else {}
        return self.preferences.find_one(query, {"_id": 0}) or {}
    
    def update_preferences(self, preferences, user_id=None):
        """
        Update preferences for a user (or global preferences if user_id is None).
        """
        query = {"user_id": user_id} if user_id else {}
        self.preferences.update_one(query, {"$set": preferences}, upsert=True)
        return "Preferences updated."

    def create_view(self, view_name, query):
        """
        Create a new view.
        """
        if self.views.find_one({"name": view_name}):
            return f"View {view_name} already exists."
        
        self.views.insert_one({"name": view_name, "query": query})
        return f"View {view_name} created."
    
    def drop_view(self, view_name):
        """
        Drop a view.
        """
        if not self.views.find_one({"name": view_name}):
            return f"View {view_name} does not exist."
        
        self.views.delete_one({"name": view_name})
        return f"View {view_name} dropped."
    
    def get_view_query(self, view_name):
        """
        Retrieve the query for a view.
        """
        view = self.views.find_one({"name": view_name})
        return view["query"] if view else None

    def create_temp_table(self, session_id, table_name, columns):
        """
        Create a temporary table.
        """
        if session_id not in self.temp_tables:
            self.temp_tables[session_id] = {}
        
        if table_name in self.temp_tables[session_id]:
            return f"Temporary table {table_name} already exists."
        
        self.temp_tables[session_id][table_name] = {"columns": columns, "data": []}
        return f"Temporary table {table_name} created."
    
    def drop_temp_table(self, session_id, table_name):
        """
        Drop a temporary table.
        """
        if session_id not in self.temp_tables or table_name not in self.temp_tables[session_id]:
            return f"Temporary table {table_name} does not exist."
        
        del self.temp_tables[session_id][table_name]
        return f"Temporary table {table_name} dropped."

    def create_procedure(self, procedure_name, procedure_body):
        """
        Create a stored procedure.
        """
        if self.procedures.find_one({"name": procedure_name}):
            return f"Procedure {procedure_name} already exists."
        
        self.procedures.insert_one({"name": procedure_name, "body": procedure_body})
        return f"Procedure {procedure_name} created."
    
    def drop_procedure(self, procedure_name):
        """
        Drop a stored procedure.
        """
        if not self.procedures.find_one({"name": procedure_name}):
            return f"Procedure {procedure_name} does not exist."
        
        self.procedures.delete_one({"name": procedure_name})
        return f"Procedure {procedure_name} dropped."
    
    def get_procedure(self, procedure_name):
        """
        Retrieve a stored procedure.
        """
        return self.procedures.find_one({"name": procedure_name})
    
    def create_function(self, function_name, function_body):
        """
        Create a function.
        """
        if self.functions.find_one({"name": function_name}):
            return f"Function {function_name} already exists."
        
        self.functions.insert_one({"name": function_name, "body": function_body})
        return f"Function {function_name} created."
    
    def drop_function(self, function_name):
        """
        Drop a function.
        """
        if not self.functions.find_one({"name": function_name}):
            return f"Function {function_name} does not exist."
        
        self.functions.delete_one({"name": function_name})
        return f"Function {function_name} dropped."
    
    def get_function(self, function_name):
        """
        Retrieve a function.
        """
        return self.functions.find_one({"name": function_name})
    
    def create_trigger(self, trigger_name, event, table_name, trigger_body):
        """
        Create a trigger.
        """
        if self.triggers.find_one({"name": trigger_name}):
            return f"Trigger {trigger_name} already exists."
        
        self.triggers.insert_one({"name": trigger_name, "event": event, "table": table_name, "body": trigger_body})
        return f"Trigger {trigger_name} created."
    
    def drop_trigger(self, trigger_name):
        """
        Drop a trigger.
        """
        if not self.triggers.find_one({"name": trigger_name}):
            return f"Trigger {trigger_name} does not exist."
        
        self.triggers.delete_one({"name": trigger_name})
        return f"Trigger {trigger_name} dropped."
    
    def get_trigger(self, trigger_name):
        """
        Retrieve a trigger.
        """
        return self.triggers.find_one({"name": trigger_name})