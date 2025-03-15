from pymongo import MongoClient
import logging

class CatalogManager:
    def __init__(self):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['dbms_project']
        self.databases = self.db['databases']
        self.tables = self.db['tables']
        self.indexes = self.db['indexes']
        logging.info("CatalogManager initialized.")
    
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
    
    def create_table(self, db_name, table_name, columns):
        logging.debug(f"Creating table: {table_name} in database: {db_name} with columns: {columns}")
        if not self.databases.find_one({"_id": db_name}):
            logging.warning(f"Database {db_name} does not exist.")
            return f"Database {db_name} does not exist."
        table_id = f"{db_name}.{table_name}"
        if self.tables.find_one({"_id": table_id}):
            logging.warning(f"Table {table_name} already exists in database {db_name}.")
            return f"Table {table_name} already exists in database {db_name}."
        self.tables.insert_one({"_id": table_id, "columns": columns})
        self.databases.update_one({"_id": db_name}, {"$push": {"tables": table_name}})
        logging.info(f"Table {table_name} created in database {db_name}.")
        return f"Table {table_name} created in database {db_name}."
    
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