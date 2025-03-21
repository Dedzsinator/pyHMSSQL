import pymongo
from pymongo import MongoClient, ASCENDING
import logging
from hashlib import sha256
import datetime

class CatalogManager:
    def __init__(self, client):
        self.client = client
        self.current_database = None
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
        db = self.client['dbms_project']
        if 'preferences' not in db.list_collection_names():
            db.create_collection('preferences')
            
        pref = db.preferences.find_one({"name": "current_database"})
        if pref and 'value' in pref:
            self.current_database = pref['value']
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
    
    def create_table(self, table_name, columns, constraints=None):
        """
        Create a table in the catalog.
        """
        if constraints is None:
            constraints = []
        
        db_name = self.get_current_database()
        db = self.client[db_name]
        
        # Create the table in current database
        if table_name not in db.list_collection_names():
            db.create_collection(table_name)
        
        # Register in catalog
        catalog_db = self.client['dbms_project']
        catalog_db.catalog.insert_one({
            "database": db_name,
            "name": table_name,
            "columns": columns,
            "constraints": constraints,
            "created_at": datetime.datetime.now()
        })
        
        return True

    def drop_table(self, table_name):
        """
        Drop a table from the catalog and the database.
        """
        db_name = self.get_current_database()
        db = self.client[db_name]
        
        # Drop the collection in MongoDB
        if table_name in db.list_collection_names():
            db.drop_collection(table_name)
        
        # Remove from catalog
        catalog_db = self.client['dbms_project']
        catalog_db.catalog.delete_one({"database": db_name, "name": table_name})
        
        # Also remove any indexes for this table
        catalog_db.indexes.delete_many({"database": db_name, "table": table_name})
        
        return True
    
    def create_index(self, table_name, index_name, column, is_unique=False):
        """
        Create an index in the catalog.
        """
        db_name = self.get_current_database()
        
        # Register in catalog
        catalog_db = self.client['dbms_project']
        catalog_db.indexes.insert_one({
            "database": db_name,
            "table": table_name,
            "name": index_name,
            "column": column,
            "unique": is_unique,
            "created_at": datetime.datetime.now()
        })
        
        # Create actual index in MongoDB
        db = self.client[db_name]
        if table_name in db.list_collection_names():
            collection = db[table_name]
            collection.create_index([(column, pymongo.ASCENDING)], 
                                unique=is_unique, 
                                name=index_name)
        
        return True

    def drop_index(self, table_name, index_name):
        """
        Drop an index from the catalog.
        """
        db_name = self.get_current_database()
        
        # Remove from catalog
        catalog_db = self.client['dbms_project']
        catalog_db.indexes.delete_one({
            "database": db_name,
            "table": table_name,
            "name": index_name
        })
        
        # Drop actual index in MongoDB
        db = self.client[db_name]
        if table_name in db.list_collection_names():
            collection = db[table_name]
            collection.drop_index(index_name)
        
        return True

    def set_current_database(self, database_name):
        """
        Set the current database for subsequent operations.
        """
        # Store the current database name
        self.current_database = database_name
        
        # Save to preferences in MongoDB
        db = self.client['dbms_project']
        db.preferences.update_one(
            {"name": "current_database"},
            {"$set": {"value": database_name}},
            upsert=True
        )
        
        logging.info(f"Current database set to: {database_name}")
        return True

    def get_current_database(self):
        """
        Get the current database name.
        """
        return self.current_database or 'dbms_project'
    
    def get_databases(self):
        """
        Get all user databases (excluding system databases).
        """
        dbs = [db for db in self.client.list_database_names() 
            if db not in ['admin', 'config', 'local']]
        return dbs

    def get_tables(self):
        """
        Get all tables in the current database.
        """
        db_name = self.get_current_database()
        db = self.client[db_name]
        
        # Get list of collections in the current database
        collections = db.list_collection_names()
        
        # Filter out system collections
        tables = [col for col in collections if not col.startswith('system.')]
        
        return tables

    def get_indexes_for_table(self, table_name):
        """
        Get all indexes for a specific table.
        """
        db_name = self.get_current_database()
        
        # Get from catalog
        catalog_db = self.client['dbms_project']
        indexes = {}
        
        for idx_doc in catalog_db.indexes.find({"database": db_name, "table": table_name}):
            index_name = idx_doc.get("name")
            indexes[index_name] = {
                "column": idx_doc.get("column"),
                "unique": idx_doc.get("unique", False)
            }
        
        return indexes

    def get_all_indexes(self):
        """
        Get all indexes in the current database.
        """
        db_name = self.get_current_database()
        
        # Get from catalog
        catalog_db = self.client['dbms_project']
        index_map = {}
        
        for idx_doc in catalog_db.indexes.find({"database": db_name}):
            table_name = idx_doc.get("table")
            index_name = idx_doc.get("name")
            
            if table_name not in index_map:
                index_map[table_name] = {}
                
            index_map[table_name][index_name] = {
                "column": idx_doc.get("column"),
                "unique": idx_doc.get("unique", False)
            }
        
        return index_map

    def get_views(self):
        """
        Get all views in the database.
        """
        db = self.client['dbms_project']
        views = {}
        
        for view_doc in db.views.find():
            views[view_doc.get("name")] = view_doc.get("query")
        
        return views

    def get_columns(self, table_name):
        """
        Get all columns for a specific table.
        """
        db = self.client['dbms_project']
        table_info = db.catalog.find_one({"name": table_name})
        
        if not table_info:
            return []
        
        return table_info.get("columns", [])

    def get_create_statement(self, table_name):
        """
        Get the CREATE TABLE statement for a specific table.
        """
        db = self.client['dbms_project']
        table_info = db.catalog.find_one({"name": table_name})
        
        if not table_info:
            return ""
        
        columns = table_info.get("columns", [])
        col_stmts = []
        
        for col in columns:
            col_stmt = f"{col['name']} {col['type']}"
            if 'constraints' in col:
                col_stmt += f" {col['constraints']}"
            col_stmts.append(col_stmt)
        
        return f"CREATE TABLE {table_name} ({', '.join(col_stmts)});"
    
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