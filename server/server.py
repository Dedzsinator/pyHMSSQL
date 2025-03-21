import sys
import os
import socket
import json
import traceback
import logging
import uuid
import re
import datetime
from logging.handlers import RotatingFileHandler

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from catalog_manager import CatalogManager
from index_manager import IndexManager
from planner import Planner
from execution_engine import ExecutionEngine
from optimizer import Optimizer
from shared.constants import SERVER_HOST, SERVER_PORT
from shared.utils import send_data, receive_data

# Import sqlparse for SQL parsing
import sqlparse

def setup_logging():
    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    # Generate log filename with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f'dbms_server_{timestamp}.log')
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Remove any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create a rotating file handler
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    
    # Set formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    # Add file handler
    root_logger.addHandler(file_handler)
    
    # Add console handler for warnings and errors only
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Log startup message
    logging.info("==== DBMS Server Starting ====")
    logging.info(f"Logging to: {log_file}")
    
    return log_file

# Initialize logging
log_file = setup_logging()

class DBMSServer:
    def __init__(self, host='localhost', port=9999, mongo_uri='mongodb://localhost:27017/'):
        # Create MongoDB client
        from pymongo import MongoClient
        self.mongo_client = MongoClient(mongo_uri)
        self.catalog_manager = CatalogManager(self.mongo_client)
        self.index_manager = IndexManager('indexes')
        
        # Set up unified logging
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Use a single log file for all components
        log_file = os.path.join(log_dir, 'query_planner.log')
        
        # Configure root logger once
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        
        # Remove any existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Add file handler
        file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(file_handler)
        
        # Silent console handler
        class NullHandler(logging.Handler):
            def emit(self, record):
                pass
        
        console_handler = NullHandler()
        root_logger.addHandler(console_handler)
        
        # Initialize components with the logging already set up
        self.planner = Planner(self.catalog_manager, self.index_manager)
        self.optimizer = Optimizer(self.catalog_manager, self.index_manager)
        self.execution_engine = ExecutionEngine(self.catalog_manager, self.index_manager)
        self.sessions = {}
    
    def _validate_session(self, session_token):
        """Validate a session token"""
        return session_token in self.sessions
    
    def handle_request(self, data):
        """
        Handle incoming request.
        """
        # Only use 'action' field for determining request type
        request_type = data.get('action')
        
        if not request_type:
            logging.error(f"Missing 'action' field in request: {data}")
            return {"error": "Missing 'action' field in request", "status": "error"}
        
        # Process the request based on action
        if request_type == 'login':
            return self.handle_login(data)
        elif request_type == 'query':
            # Check if this is a VISUALIZE query and redirect
            query = data.get('query', '').strip().upper()
            if query.startswith('VISUALIZE'):
                return self.handle_visualize(data)
            return self.handle_query(data)
        elif request_type == 'register':
            return self.handle_register(data)
        elif request_type == 'visualize':
            return self.handle_visualize(data)
        elif request_type == 'logout':
            return self.handle_logout(data)
        else:
            logging.error(f"Unknown request type: {request_type}")
            return {"error": f"Unknown request type: {request_type}", "status": "error"}

    def handle_visualize(self, data):
        """Handle visualization requests."""
        session_id = data.get('session_id')
        if session_id not in self.sessions:
            logging.warning(f"Unauthorized visualize attempt with invalid session ID: {session_id}")
            return {"error": "Unauthorized. Please log in.", "status": "error"}
        
        user = self.sessions[session_id]
        query = data.get('query', '')
        logging.info(f"Visualization request from {user.get('username', 'Unknown')}: {query}")
        
        # Parse as a visualization command
        parsed = self._parse_visualize_command(query)
        
        if parsed:
            # Execute the visualization
            try:
                result = self.execution_engine.execute(parsed)
                if isinstance(result, dict) and 'type' in result:
                    del result['type']  # Remove type field from response
                
                # Ensure proper response format
                if isinstance(result, dict) and "status" not in result:
                    result["status"] = "success"
                    
                return result
            except Exception as e:
                error_msg = f"Error executing visualization: {str(e)}"
                logging.error(error_msg)
                return {"error": error_msg, "status": "error"}
        else:
            return {"error": "Failed to parse visualization command", "status": "error"}

    def parse_sql(self, sql):
        """
        Parse SQL query into a structured format.
        """
        try:
            # Parse SQL query
            parsed = sqlparse.parse(sql)
            if not parsed:
                return {"error": "Failed to parse SQL statement"}
            
            # Get the first statement (we only support one at a time)
            stmt = parsed[0]
            
            # Handle SHOW commands specially since sqlparse may not recognize them correctly
            if sql.strip().upper().startswith("SHOW "):
                result = {"type": "SHOW", "query": sql}
                self._extract_show_elements(stmt, result)
                return result
            
            # Handle USE DATABASE command
            if sql.strip().upper().startswith("USE "):
                db_name = sql.strip()[4:].strip()
                return {
                    "type": "USE",
                    "database": db_name,
                    "query": sql
                }
            
            if sql.upper().startswith("VISUALIZE"):
                return self._parse_visualize_command(sql)
            
            # Initialize result with the query
            result = {"type": None, "query": sql}
            
            # Determine statement type
            stmt_type = stmt.get_type()
            
            if stmt_type == "SELECT":
                result["type"] = "SELECT"
                self._extract_select_elements(stmt, result)
            elif stmt_type == "INSERT":
                result["type"] = "INSERT"
                self._extract_insert_elements(stmt, result)
            elif stmt_type == "UPDATE":
                result["type"] = "UPDATE"
                self._extract_update_elements(stmt, result)
            elif stmt_type == "DELETE":
                result["type"] = "DELETE"
                self._extract_delete_elements(stmt, result)
            elif stmt_type == "CREATE":
                result["type"] = "CREATE"
                self._extract_create_elements(stmt, result)
            elif stmt_type == "DROP":
                result["type"] = "DROP"
                self._extract_drop_elements(stmt, result)
            elif stmt_type == "SHOW":
                result["type"] = "SHOW"
                self._extract_show_elements(stmt, result)
            else:
                result["error"] = "Unsupported SQL statement type"
            
            return result
        except Exception as e:
            logging.error(f"Error extracting elements: {str(e)}")
            logging.error(traceback.format_exc())
            return {"error": f"Error extracting SQL elements: {str(e)}"}

    def _extract_select_elements(self, parsed, result):
        """Extract elements from a SELECT query"""
        # Initialize components
        columns = []
        tables = []
        condition = None
        order_by = None
        limit = None
        join_type = None
        join_condition = None
        
        # First extract the table name(s) directly from the SQL
        raw_sql = str(parsed).upper()
        if " FROM " in raw_sql:
            parts = raw_sql.split(" FROM ", 1)
            if len(parts) > 1:
                from_clause = parts[1].strip()
                # Table part goes up to WHERE, ORDER BY, GROUP BY, HAVING, LIMIT, etc.
                end_keywords = [" WHERE ", " ORDER BY ", " GROUP BY ", " HAVING ", " LIMIT "]
                table_end = len(from_clause)
                for keyword in end_keywords:
                    pos = from_clause.find(keyword)
                    if pos != -1 and pos < table_end:
                        table_end = pos
                
                tables_part = from_clause[:table_end].strip()
                # Handle JOIN
                if " JOIN " in tables_part:
                    # This is a complex JOIN query, needs special handling
                    tables = [t.strip() for t in re.split(r'\s+JOIN\s+|\s*,\s*', tables_part)]
                else:
                    # Simple table list
                    tables = [t.strip() for t in tables_part.split(',')]
        
        # Now extract column names
        col_part = raw_sql.split(" FROM ")[0].replace("SELECT", "", 1).strip()
        if col_part == "*":
            columns = ["*"]
        else:
            # Handle multiple columns
            columns = [c.strip() for c in col_part.split(',')]
        
        # Extract WHERE condition
        if " WHERE " in raw_sql:
            where_parts = raw_sql.split(" WHERE ", 1)
            if len(where_parts) > 1:
                condition_part = where_parts[1]
                # Condition ends at the next clause
                end_keywords = [" ORDER BY ", " GROUP BY ", " HAVING ", " LIMIT "]
                condition_end = len(condition_part)
                for keyword in end_keywords:
                    pos = condition_part.find(keyword)
                    if pos != -1 and pos < condition_end:
                        condition_end = pos
                
                condition = condition_part[:condition_end].strip()
        
        # Extract ORDER BY
        if " ORDER BY " in raw_sql:
            order_parts = raw_sql.split(" ORDER BY ", 1)
            if len(order_parts) > 1:
                order_part = order_parts[1]
                # Order by ends at LIMIT
                limit_pos = order_part.find(" LIMIT ")
                if limit_pos != -1:
                    order_by = order_part[:limit_pos].strip()
                else:
                    order_by = order_part.strip()
        
        # Extract LIMIT
        if " LIMIT " in raw_sql:
            limit_parts = raw_sql.split(" LIMIT ", 1)
            if len(limit_parts) > 1:
                limit = limit_parts[1].strip()
                try:
                    limit = int(limit)
                except:
                    limit = None
        
        # Update result with extracted components
        result.update({
            "columns": columns,
            "tables": tables,
            "condition": condition,
            "order_by": order_by,
            "limit": limit,
            "join_type": join_type,
            "join_condition": join_condition
        })

    def _extract_insert_elements(self, parsed, result):
        """Extract elements from an INSERT statement"""
        # Get original SQL to preserve case
        raw_sql = str(parsed)
        
        # Extract table name
        table_match = re.search(r'INSERT\s+INTO\s+(\w+)', raw_sql, re.IGNORECASE)
        table_name = table_match.group(1) if table_match else None
        
        # Extract column names
        columns = []
        if "(" in raw_sql and ")" in raw_sql:
            # First set of parentheses should be column names
            cols_match = re.search(r'INSERT\s+INTO\s+\w+\s*\(([^)]+)\)', raw_sql, re.IGNORECASE)
            if cols_match:
                cols_str = cols_match.group(1)
                columns = [col.strip() for col in cols_str.split(',')]
        
        # Extract values
        values = []
        values_match = re.search(r'VALUES\s*\(([^)]+)\)', raw_sql, re.IGNORECASE)
        if values_match:
            values_str = values_match.group(1)
            value_list = []
            
            # Parse individual values, handling strings and numbers
            for val in values_str.split(','):
                val = val.strip()
                if val.startswith("'") and val.endswith("'"):
                    # String value
                    value_list.append(val[1:-1])
                elif val.isdigit():
                    # Integer
                    value_list.append(int(val))
                elif re.match(r'^[0-9]+\.[0-9]+$', val):
                    # Float
                    value_list.append(float(val))
                else:
                    # Other (keep as is)
                    value_list.append(val)
            
            values.append(value_list)
        
        # Update result with extracted components
        result.update({
            "table": table_name,
            "columns": columns,
            "values": values
        })

    def _extract_update_elements(self, parsed, result):
        """Extract elements from an UPDATE statement"""
        table_name = None
        set_pairs = {}
        where_clause = None
        
        # Get original SQL to preserve case
        raw_sql = str(parsed)
        
        # Extract table name (comes after UPDATE keyword)
        table_match = re.search(r'UPDATE\s+(\w+)', raw_sql, re.IGNORECASE)
        if table_match:
            table_name = table_match.group(1)
        
        # Extract SET clause - improved regex to properly match everything between SET and WHERE (or end of string)
        set_match = re.search(r'SET\s+(.*?)(?:\s+WHERE\s+|$)', raw_sql, re.IGNORECASE)
        if set_match:
            set_clause = set_match.group(1).strip()
            logging.debug(f"Extracted SET clause: {set_clause}")
            
            # Process each assignment (column = value)
            for assignment in set_clause.split(','):
                if '=' in assignment:
                    column, value = assignment.split('=', 1)
                    set_pairs[column.strip()] = value.strip()
                    
            logging.debug(f"Final SET pairs: {set_pairs}")
        
        # Extract WHERE clause
        where_match = re.search(r'WHERE\s+(.+)$', raw_sql, re.IGNORECASE)
        if where_match:
            where_clause = where_match.group(1).strip()
        
        # Update result with extracted components
        result.update({
            "table": table_name,
            "set": set_pairs,
            "where": where_clause,
            "condition": where_clause  # Also add as "condition" for consistency
        })

    def _extract_delete_elements(self, parsed, result):
        """Extract elements from a DELETE statement"""
        table_name = None
        where_clause = None
        
        # Get original SQL to preserve case
        raw_sql = str(parsed)
        
        # Extract table name (comes after FROM keyword)
        table_match = re.search(r'DELETE\s+FROM\s+(\w+)', raw_sql, re.IGNORECASE)
        if table_match:
            table_name = table_match.group(1)
        
        # Extract WHERE clause
        where_match = re.search(r'WHERE\s+(.+)$', raw_sql, re.IGNORECASE)
        if where_match:
            where_clause = where_match.group(1).strip()
        
        # Update result with extracted components
        result.update({
            "table": table_name,
            "where": where_clause,
            "condition": where_clause  # Also add as "condition" for consistency
        })

    def _extract_create_elements(self, parsed, result):
        """Extract elements from a CREATE statement"""
        table_name = None
        database_name = None
        index_name = None
        column_name = None
        is_unique = False
        columns = []
        constraints = []
        
        # Get original SQL to preserve case
        raw_sql = str(parsed)
        
        # Check the statement type
        create_type = None
        if re.search(r'CREATE\s+TABLE', raw_sql, re.IGNORECASE):
            create_type = "TABLE"
        elif re.search(r'CREATE\s+DATABASE', raw_sql, re.IGNORECASE):
            create_type = "DATABASE"
        elif re.search(r'CREATE\s+UNIQUE\s+INDEX', raw_sql, re.IGNORECASE):
            create_type = "INDEX"
            is_unique = True
        elif re.search(r'CREATE\s+INDEX', raw_sql, re.IGNORECASE):
            create_type = "INDEX"
        
        # Extract index details
        if create_type == "INDEX":
            if is_unique:
                pattern = r'CREATE\s+UNIQUE\s+INDEX\s+(\w+)\s+ON\s+(\w+)\s*\(([^)]+)\)'
            else:
                pattern = r'CREATE\s+INDEX\s+(\w+)\s+ON\s+(\w+)\s*\(([^)]+)\)'
            
            match = re.search(pattern, raw_sql, re.IGNORECASE)
            if match:
                index_name = match.group(1)
                table_name = match.group(2)
                column_name = match.group(3).strip()
        
        # Extract table details
        elif create_type == "TABLE":
            # Extract table name
            match = re.search(r'CREATE\s+TABLE\s+(\w+)', raw_sql, re.IGNORECASE)
            if match:
                table_name = match.group(1)
            
            # Extract column definitions
            if "(" in raw_sql and ")" in raw_sql:
                col_text = raw_sql.split("(", 1)[1].rsplit(")", 1)[0]
                col_defs = [c.strip() for c in col_text.split(',')]
                for col_def in col_defs:
                    if col_def:
                        if re.match(r'^\s*(PRIMARY|FOREIGN|UNIQUE|CHECK|CONSTRAINT)', col_def, re.IGNORECASE):
                            constraints.append(col_def)
                        else:
                            columns.append(col_def)
        
        # Extract database name
        elif create_type == "DATABASE":
            match = re.search(r'CREATE\s+DATABASE\s+(\w+)', raw_sql, re.IGNORECASE)
            if match:
                database_name = match.group(1)
        
        # Update result with extracted components
        result.update({
            "create_type": create_type,
            "table": table_name,
            "database": database_name,
            "index": index_name,
            "column": column_name,
            "unique": is_unique,
            "columns": columns,
            "constraints": constraints
        })

    def _extract_drop_elements(self, parsed, result):
        """Extract elements from a DROP statement"""
        table_name = None
        database_name = None
        index_name = None
        
        # Get original SQL to preserve case
        raw_sql = str(parsed)
        
        # Determine what type of DROP statement this is
        drop_type = None
        if re.search(r'DROP\s+TABLE', raw_sql, re.IGNORECASE):
            drop_type = "TABLE"
        elif re.search(r'DROP\s+DATABASE', raw_sql, re.IGNORECASE):
            drop_type = "DATABASE"
        elif re.search(r'DROP\s+INDEX', raw_sql, re.IGNORECASE):
            drop_type = "INDEX"
        elif re.search(r'DROP\s+VIEW', raw_sql, re.IGNORECASE):
            drop_type = "VIEW"
        
        # Extract the names based on the type
        if drop_type == "TABLE":
            # Format: DROP TABLE table_name
            match = re.search(r'DROP\s+TABLE\s+(\w+)', raw_sql, re.IGNORECASE)
            if match:
                table_name = match.group(1)
        
        elif drop_type == "DATABASE":
            # Format: DROP DATABASE db_name
            match = re.search(r'DROP\s+DATABASE\s+(\w+)', raw_sql, re.IGNORECASE)
            if match:
                database_name = match.group(1)
        
        elif drop_type == "INDEX":
            # Format: DROP INDEX index_name ON table_name
            match = re.search(r'DROP\s+INDEX\s+(\w+)\s+ON\s+(\w+)', raw_sql, re.IGNORECASE)
            if match:
                index_name = match.group(1)
                table_name = match.group(2)
        
        # Update result with extracted components
        result.update({
            "drop_type": drop_type,
            "table": table_name,
            "database": database_name,
            "index": index_name
        })

    def _extract_show_elements(self, parsed, result):
        """Extract elements from a SHOW statement"""
        object_type = None
        table_name = None
        
        # Get the raw SQL directly
        raw_sql = str(parsed).upper()
        
        if "SHOW DATABASES" in raw_sql:
            object_type = "DATABASES"
        elif "SHOW TABLES" in raw_sql:
            object_type = "TABLES"
        elif "SHOW INDEXES FOR" in raw_sql:
            object_type = "INDEXES"
            # Extract table name after FOR 
            parts = str(parsed).split("FOR", 1)
            if len(parts) > 1:
                table_name = parts[1].strip()
        elif "SHOW INDEXES" in raw_sql or "SHOW INDICES" in raw_sql:
            object_type = "INDEXES"
        elif "SHOW COLUMNS" in raw_sql:
            object_type = "COLUMNS"
            # Extract table name
            if "FROM" in raw_sql:
                parts = str(parsed).split("FROM", 1)
                if len(parts) > 1:
                    table_name = parts[1].strip()
        
        # Update result
        result.update({
            "object": object_type,
            "table": table_name
        })
        
        # Log the extracted components for debugging
        logging.debug(f"SHOW command: object_type={object_type}, table={table_name}")

    def _extract_create_index_elements(self, parsed, result):
        """Extract elements from a CREATE INDEX statement"""
        index_name = None
        table_name = None
        column_name = None
        is_unique = False
        
        # Check if it's a UNIQUE INDEX
        for token in parsed.tokens:
            if token.is_keyword and token.value.upper() == "UNIQUE":
                is_unique = True
                break
        
        # Process tokens to extract other elements
        for i, token in enumerate(parsed.tokens):
            if token.is_whitespace:
                continue
                
            # Find the index name
            if token.ttype is None and isinstance(token, sqlparse.sql.Identifier) and not index_name:
                # This should be right after CREATE INDEX
                index_name = str(token)
                
            # Find the ON keyword and the table name after it
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "ON":
                if i + 1 < len(parsed.tokens):
                    table_token = parsed.tokens[i + 1]
                    if not table_token.is_whitespace:
                        table_name = str(table_token)
            
            # Find the column in parentheses
            if str(token) == "(" and table_name:
                if i + 1 < len(parsed.tokens):
                    column_token = parsed.tokens[i + 1]
                    if isinstance(column_token, sqlparse.sql.Identifier):
                        column_name = str(column_token)
                    elif isinstance(column_token, sqlparse.sql.IdentifierList):
                        # For now, just take the first column if there are multiple
                        identifiers = list(column_token.get_identifiers())
                        if identifiers:
                            column_name = str(identifiers[0])
        
        # Update result
        result.update({
            "index": index_name,
            "table": table_name,
            "column": column_name,
            "unique": is_unique
        })
    
    def _parse_visualize_command(self, query):
        """
        Parse a VISUALIZE command.
        """
        result = {"type": "VISUALIZE"}
        
        # Check for VISUALIZE INDEX
        if "INDEX" in query.upper():
            result["object"] = "INDEX"
            
            # Extract index name if specified
            match = re.search(r'VISUALIZE\s+INDEX\s+(\w+)', query, re.IGNORECASE)
            if match:
                result["index_name"] = match.group(1)
                
            # Check for ON table
            match = re.search(r'ON\s+(\w+)', query, re.IGNORECASE)
            if match:
                result["table"] = match.group(1)
        
        # Add more visualization types as needed
        
        return result
    
    def handle_login(self, data):
        """Handle user login."""
        username = data.get('username')
        logging.info(f"Login attempt for user: {username}")
        
        password = data.get('password')
        
        user = self.catalog_manager.authenticate_user(username, password)
        if user:
            # Create a new session
            session_id = str(uuid.uuid4())
            self.sessions[session_id] = user
            logging.info(f"User {username} logged in successfully (role: {user['role']})")
            return {
                "session_id": session_id, 
                "role": user["role"],
                "status": "success",
                "message": f"Login successful as {username} ({user['role']})"
            }
        else:
            logging.warning(f"Failed login attempt for user: {username}")
            return {"error": "Invalid username or password.", "status": "error"}
    
    def handle_logout(self, data):
        """Handle user logout."""
        session_id = data.get('session_id')
        if session_id in self.sessions:
            username = self.sessions[session_id].get('username', 'Unknown')
            del self.sessions[session_id]
            logging.info(f"User {username} logged out successfully")
            return {"message": "Logged out successfully.", "status": "success"}
        else:
            logging.warning(f"Invalid logout attempt with session ID: {session_id}")
            return {"error": "Invalid session ID.", "status": "error"}

    def handle_register(self, data):
        """Handle user registration."""
        username = data.get('username')
        logging.info(f"Registration attempt for user: {username}")
        
        password = data.get('password')
        role = data.get('role', "user")
        
        result = self.catalog_manager.register_user(username, password, role)
        if "error" not in str(result).lower():
            logging.info(f"User {username} registered successfully with role: {role}")
            return {"message": f"User {username} registered successfully as {role}", "status": "success"}
        else:
            logging.warning(f"Failed registration for user {username}: {result}")
            if isinstance(result, str):
                return {"error": result, "status": "error"}
            return result

    def handle_query(self, data):
        """Handle query execution with role-based access control."""
        session_id = data.get('session_id')
        if session_id not in self.sessions:
            logging.warning(f"Unauthorized query attempt with invalid session ID: {session_id}")
            return {"error": "Unauthorized. Please log in.", "status": "error"}
        
        user = self.sessions[session_id]
        query = data.get('query')
        logging.info(f"Query from {user.get('username', 'Unknown')}: {query}")
        
        # Check role-based access control
        if self.is_query_allowed(user["role"], query):
            # Parse the SQL query using the Python parser
            parsed_query = self.parse_sql(query)
            if "error" in parsed_query:
                error_msg = f"Error parsing SQL query: {parsed_query['error']}"
                logging.error(error_msg)
                return {"error": error_msg, "status": "error"}
            
            try:
                # Plan and optimize the query
                logging.debug("Planning query...")
                plan = self.planner.plan_query(parsed_query)
                logging.debug("Optimizing query...")
                optimized_plan = self.optimizer.optimize(plan)
                
                # Execute the query
                logging.debug("Executing query...")
                result = self.execution_engine.execute(optimized_plan)
                logging.info(f"Query executed successfully")
                
                # Ensure proper response format
                if isinstance(result, str):
                    result = {"message": result, "status": "success"}
                elif isinstance(result, dict) and "status" not in result:
                    result["status"] = "success"
                
                # Don't check for type field in logging - it's removed intentionally
                return result
            except Exception as e:
                error_msg = f"Error executing query: {str(e)}"
                logging.error(error_msg)
                return {"error": error_msg, "status": "error"}
        else:
            logging.warning(f"Access denied for query: {query}")
            return {"error": "Access denied. You do not have permission to execute this query.", "status": "error"}
        
    def is_query_allowed(self, role, query):
        """
        Check if a query is allowed for the user's role.
        """
        # Admin can do anything
        if role == "admin":
            return True
            
        # Convert to uppercase for case-insensitive comparison
        query_upper = query.upper().strip()
        
        # Regular users cannot do certain operations
        if any(query_upper.startswith(prefix) for prefix in [
                "DROP DATABASE", 
                "CREATE DATABASE", 
                "CREATE USER",
                "DROP USER",
                "ALTER USER",
                "GRANT",
                "REVOKE"
            ]):
            return False
        
        # All other queries are allowed for all roles
        return True
    
    def display_result(self, result):
        """Display the result of a query in a formatted table"""
        # Handle string responses for backward compatibility
        if isinstance(result, str):
            print(result)
            return
            
        # First check if there's a simple message to display
        if "message" in result:
            print(result["message"])
            return
            
        # Then check for rows and columns (table data)
        if "rows" in result and "columns" in result:
            # Display results as a table
            columns = result["columns"]
            rows = result["rows"]
            
            # Calculate column widths
            col_widths = [len(str(col)) for col in columns]
            for row in rows:
                for i, cell in enumerate(row):
                    if i < len(col_widths):
                        col_widths[i] = max(col_widths[i], len(str(cell)))
            
            # Print header
            header = " | ".join(str(col).ljust(col_widths[i]) for i, col in enumerate(columns))
            separator = "-+-".join("-" * width for width in col_widths)
            print(header)
            print(separator)
            
            # Print rows
            for row in rows:
                row_str = " | ".join(str(cell).ljust(col_widths[i]) if i < len(col_widths) else str(cell) 
                                    for i, cell in enumerate(row))
                print(row_str)
            
            print(f"\n{len(rows)} row(s) returned")
        # Handle error messages
        elif "error" in result:
            print(f"Error: {result['error']}")
        # Handle any other data formats
        else:
            # If it's just key-value pairs without a clear format, print them nicely
            for key, value in result.items():
                if key not in ["status", "type"]:  # Skip non-content fields
                    print(f"{key}: {value}")

def start_server():
    # Make sure sqlparse is installed
    try:
        import sqlparse
    except ImportError:
        logging.critical("Error: sqlparse library is required but not installed.")
        print("Error: sqlparse library is required but not installed.")
        print("Please install it using: pip install sqlparse")
        sys.exit(1)
    
    server = DBMSServer()
    
    # Test the SQL parser
    test_queries = [
        "SELECT id, name FROM users WHERE age > 18",
        "INSERT INTO users (id, name) VALUES (1, 'test')",
        "UPDATE users SET name = 'John' WHERE id = 1",
        "DELETE FROM users WHERE id = 1",
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"
    ]
    
    logging.info("Testing SQL parser with example queries")
    for query in test_queries:
        try:
            parsed = server.parse_sql(query)
            logging.info(f"Test query: {query}")
            logging.info(f"Parsed result: {json.dumps(parsed)}")
        except Exception as e:
            logging.error(f"Error parsing '{query}': {str(e)}")
    
    # Print a message to console indicating where logs will be stored
    print(f"DBMS Server starting. Logs will be stored in: {log_file}")
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((SERVER_HOST, SERVER_PORT))
    sock.listen(5)
    logging.info(f"Server listening on {SERVER_HOST}:{SERVER_PORT}")
    print(f"Server listening on {SERVER_HOST}:{SERVER_PORT}...")
    
    try:
        while True:
            client, address = sock.accept()
            logging.info(f"Connection from {address}")
            print(f"Connection from {address}")
            
            try:
                data = receive_data(client)
                if data:
                    response = server.handle_request(data)
                    send_data(client, response)
                client.close()
            except Exception as e:
                error_msg = f"Error handling client: {str(e)}"
                logging.error(error_msg)
                logging.error(traceback.format_exc())
                print(error_msg)
                client.close()
    except KeyboardInterrupt:
        shutdown_msg = "Server shutting down..."
        logging.info(shutdown_msg)
        print(shutdown_msg)
    finally:
        sock.close()
        logging.info("Server socket closed")

if __name__ == "__main__":
    start_server()