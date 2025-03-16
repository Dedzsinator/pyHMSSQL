import sys
import os
import socket
import json
import traceback
import logging
import uuid
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

# Configure logging
# Configure logging to file
def setup_logging():
    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    # Generate log filename with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f'dbms_server_{timestamp}.log')
    
    # Create a rotating file handler (max 10MB per file, keep 5 backup files)
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    
    # Set formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    
    # Remove any existing handlers (like stdout handlers)
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add our file handler
    root_logger.addHandler(file_handler)
    
    # Log startup message
    logging.info("==== DBMS Server Starting ====")
    logging.info(f"Logging to: {log_file}")
    
    return log_file

# Initialize logging
log_file = setup_logging()

class DBMSServer:
    def __init__(self):
        self.catalog_manager = CatalogManager()
        self.index_manager = IndexManager('indexes')
        self.planner = Planner(self.catalog_manager, self.index_manager)
        self.optimizer = Optimizer(self.catalog_manager, self.index_manager)
        self.execution_engine = ExecutionEngine(self.catalog_manager, self.index_manager)
        self.sessions = {}
    
    def handle_request(self, data):
        action = data.get('action')
        logging.info(f"Handling request: {action}")
        
        if action == 'login':
            return self.handle_login(data)
        elif action == 'register':
            return self.handle_register(data)
        elif action == 'logout':
            return self.handle_logout(data)
        elif action == 'query':
            return self.handle_query(data)
        else:
            logging.warning(f"Invalid action: {action}")
            return "Invalid action."

    def parse_sql(self, query):
        """
        Parse SQL query using sqlparse library and return structured representation.
        """
        try:
            logging.debug(f"Parsing SQL query: {query}")
            parsed = sqlparse.parse(query)[0]
            stmt_type = parsed.get_type()
            
            # Handle special case for SHOW commands which sqlparse may not recognize properly
            if query.upper().startswith("SHOW "):
                stmt_type = "SHOW"
            
            # Create a result structure that's compatible with what our query planner expects
            result = {
                "type": stmt_type,
                "query": query,  # Store the original query for reference
            }
            
            # Extract common elements based on statement type
            if stmt_type == "SELECT":
                self._extract_select_elements(parsed, result)
            elif stmt_type == "INSERT":
                self._extract_insert_elements(parsed, result)
            elif stmt_type == "UPDATE":
                self._extract_update_elements(parsed, result)
            elif stmt_type == "DELETE":
                self._extract_delete_elements(parsed, result)
            elif stmt_type == "CREATE":
                self._extract_create_elements(parsed, result)
            elif stmt_type == "DROP":
                self._extract_drop_elements(parsed, result)
            elif stmt_type == "SHOW":
                self._extract_show_elements(parsed, result)
            
            logging.debug(f"Parsed SQL result: {result}")
            return result
            
        except Exception as e:
            logging.error(f"Error parsing SQL: {str(e)}")
            logging.error(traceback.format_exc())
            # Return minimal information on error
            return {
                "type": query.strip().split()[0].upper() if query and query.strip() else "UNKNOWN",
                "error": str(e)
            }
    
    def _extract_select_elements(self, parsed, result):
        """Extract elements from a SELECT statement"""
        # Initialize components
        columns = []
        tables = []
        where_clause = None
        join_conditions = []
        group_by = []
        having = None
        order_by = []
        limit = None
        
        # Process tokens
        from_seen = False
        group_by_seen = False
        order_by_seen = False
        
        for token in parsed.tokens:
            # Skip whitespace and punctuation
            if token.is_whitespace or str(token) == ';':
                continue
                
            # Extract column list (after SELECT keyword)
            if token.ttype is None and hasattr(token, 'get_identifiers') and not from_seen:
                columns = [str(col) for col in token.get_identifiers()]
            
            # Mark when we see FROM keyword
            if token.is_keyword and token.value.upper() == "FROM":
                from_seen = True
                continue
                
            # Extract table names (after FROM keyword)
            if from_seen and token.ttype is None and hasattr(token, 'get_identifiers'):
                tables = [str(t) for t in token.get_identifiers()]
                from_seen = False  # Reset to avoid capturing other tokens
            
            # Extract WHERE clause
            if token.is_keyword and token.value.upper() == "WHERE":
                # Find the next token which should be the condition
                idx = parsed.token_index(token)
                if idx < len(parsed.tokens) - 1:
                    # Get everything until the next major clause
                    where_tokens = []
                    for t in parsed.tokens[idx+1:]:
                        if t.is_keyword and t.value.upper() in ("GROUP", "ORDER", "LIMIT"):
                            break
                        if not t.is_whitespace:
                            where_tokens.append(str(t))
                    where_clause = " ".join(where_tokens).strip()
            
            # Extract GROUP BY clause
            if token.is_keyword and token.value.upper() == "GROUP":
                group_by_seen = True
                continue
                
            if group_by_seen and token.is_keyword and token.value.upper() == "BY":
                idx = parsed.token_index(token)
                if idx < len(parsed.tokens) - 1:
                    for t in parsed.tokens[idx+1:]:
                        if t.is_keyword and t.value.upper() in ("HAVING", "ORDER", "LIMIT"):
                            break
                        if not t.is_whitespace and str(t) != ',':
                            group_by.append(str(t).strip())
                group_by_seen = False
            
            # Extract ORDER BY clause
            if token.is_keyword and token.value.upper() == "ORDER":
                order_by_seen = True
                continue
                
            if order_by_seen and token.is_keyword and token.value.upper() == "BY":
                idx = parsed.token_index(token)
                if idx < len(parsed.tokens) - 1:
                    for t in parsed.tokens[idx+1:]:
                        if t.is_keyword and t.value.upper() == "LIMIT":
                            break
                        if not t.is_whitespace and str(t) != ',':
                            order_by.append(str(t).strip())
                order_by_seen = False
            
            # Extract LIMIT clause
            if token.is_keyword and token.value.upper() == "LIMIT":
                idx = parsed.token_index(token)
                if idx < len(parsed.tokens) - 1:
                    limit_token = parsed.tokens[idx+1]
                    if not limit_token.is_whitespace:
                        limit = str(limit_token)
        
        # Update result with extracted components
        result.update({
            "columns": columns,
            "tables": tables,
            "where": where_clause,
            "joins": join_conditions,
            "groupBy": group_by,
            "having": having,
            "orderBy": order_by,
            "limit": limit
        })
    
    def _extract_insert_elements(self, parsed, result):
        """Extract elements from an INSERT statement"""
        table_name = None
        columns = []
        values = []
        
        # Process tokens to find the table name and values
        into_seen = False
        values_seen = False
        
        for token in parsed.tokens:
            # Skip whitespace and punctuation
            if token.is_whitespace or str(token) == ';':
                continue
                
            # Find the table name after INTO
            if token.is_keyword and token.value.upper() == "INTO":
                into_seen = True
                continue
                
            if into_seen and token.ttype is None:
                # This should be the table identifier
                if hasattr(token, 'get_name'):
                    table_name = token.get_name()
                else:
                    table_name = str(token)
                into_seen = False
            
            # Extract column names if specified
            if str(token) == "(" and table_name and not values_seen:
                idx = parsed.token_index(token)
                # Look for closing parenthesis
                for i in range(idx + 1, len(parsed.tokens)):
                    if str(parsed.tokens[i]) == ")":
                        # Extract everything between parentheses
                        col_tokens = parsed.tokens[idx+1:i]
                        columns = []
                        for t in col_tokens:
                            if not t.is_whitespace and str(t) != ',':
                                columns.append(str(t).strip('`"\''))
                        break
            
            # Extract VALUES clause
            if token.is_keyword and token.value.upper() == "VALUES":
                values_seen = True
                continue
                
            # Extract actual values
            if values_seen and str(token) == "(":
                idx = parsed.token_index(token)
                # Look for closing parenthesis
                for i in range(idx + 1, len(parsed.tokens)):
                    if str(parsed.tokens[i]) == ")":
                        # Extract everything between parentheses
                        val_tokens = parsed.tokens[idx+1:i]
                        row_values = []
                        for t in val_tokens:
                            if not t.is_whitespace and str(t) != ',':
                                row_values.append(str(t).strip())
                        values.append(row_values)
                        break
        
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
        
        # Process tokens
        update_seen = False
        set_seen = False
        
        for token in parsed.tokens:
            # Skip whitespace and punctuation
            if token.is_whitespace or str(token) == ';':
                continue
                
            # Find the table name after UPDATE
            if token.is_keyword and token.value.upper() == "UPDATE":
                update_seen = True
                continue
                
            if update_seen and token.ttype is None:
                # This should be the table identifier
                if hasattr(token, 'get_name'):
                    table_name = token.get_name()
                else:
                    table_name = str(token)
                update_seen = False
            
            # Extract SET pairs
            if token.is_keyword and token.value.upper() == "SET":
                set_seen = True
                continue
                
            if set_seen and token.ttype is None:
                # This is the assignments list
                assignments = []
                if hasattr(token, 'tokens'):
                    # This handles multiple assignments
                    for item in token.tokens:
                        if not item.is_whitespace and str(item) != ',':
                            assignments.append(str(item))
                else:
                    # This is a single assignment
                    assignments.append(str(token))
                
                # Parse each assignment as column=value
                for assignment in assignments:
                    if '=' in assignment:
                        column, value = assignment.split('=', 1)
                        set_pairs[column.strip()] = value.strip()
                
                set_seen = False
            
            # Extract WHERE clause
            if token.is_keyword and token.value.upper() == "WHERE":
                # Find the next token which should be the condition
                idx = parsed.token_index(token)
                if idx < len(parsed.tokens) - 1:
                    where_tokens = parsed.tokens[idx+1:]
                    where_clause = " ".join(str(t) for t in where_tokens if not t.is_whitespace).strip()
        
        # Update result with extracted components
        result.update({
            "table": table_name,
            "set": set_pairs,
            "where": where_clause
        })
    
    def _extract_delete_elements(self, parsed, result):
        """Extract elements from a DELETE statement"""
        table_name = None
        where_clause = None
        
        # Process tokens
        from_seen = False
        
        for token in parsed.tokens:
            # Skip whitespace and punctuation
            if token.is_whitespace or str(token) == ';':
                continue
                
            # Find the table name after FROM
            if token.is_keyword and token.value.upper() == "FROM":
                from_seen = True
                continue
                
            if from_seen and token.ttype is None:
                # This should be the table identifier
                if hasattr(token, 'get_name'):
                    table_name = token.get_name()
                else:
                    table_name = str(token)
                from_seen = False
            
            # Extract WHERE clause
            if token.is_keyword and token.value.upper() == "WHERE":
                # Find the next token which should be the condition
                idx = parsed.token_index(token)
                if idx < len(parsed.tokens) - 1:
                    where_tokens = parsed.tokens[idx+1:]
                    where_clause = " ".join(str(t) for t in where_tokens if not t.is_whitespace).strip()
        
        # Update result with extracted components
        result.update({
            "table": table_name,
            "where": where_clause
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
        
        # Process tokens
        table_seen = False
        database_seen = False
        index_seen = False
        unique_seen = False
        on_seen = False
        column_list_started = False
        
        for token in parsed.tokens:
            # Skip whitespace and punctuation
            if token.is_whitespace or str(token) == ';':
                continue
                
            # Check if it's a UNIQUE INDEX
            if token.is_keyword and token.value.upper() == "UNIQUE":
                unique_seen = True
                is_unique = True
                continue
                
            # Check if it's a CREATE INDEX statement
            if token.is_keyword and token.value.upper() == "INDEX":
                index_seen = True
                continue
                
            # Check if it's a CREATE TABLE statement
            if token.is_keyword and token.value.upper() == "TABLE":
                table_seen = True
                continue
                
            # Check if it's a CREATE DATABASE statement
            if token.is_keyword and token.value.upper() == "DATABASE":
                database_seen = True
                continue
                
            # Check if it's the ON keyword (for CREATE INDEX ON table)
            if token.is_keyword and token.value.upper() == "ON":
                on_seen = True
                continue
                
            # Find the index name after INDEX
            if index_seen and token.ttype is None and not on_seen:
                # This should be the index identifier
                if hasattr(token, 'get_name'):
                    index_name = token.get_name()
                else:
                    index_name = str(token)
                index_seen = False
                
            # Find the table name after ON (for CREATE INDEX)
            if on_seen and token.ttype is None and not column_list_started:
                # This should be the table identifier
                if hasattr(token, 'get_name'):
                    table_name = token.get_name()
                else:
                    table_name = str(token)
                on_seen = False
                
            # Find the table name after TABLE
            if table_seen and token.ttype is None and not columns:
                # This should be the table identifier
                if hasattr(token, 'get_name'):
                    table_name = token.get_name()
                else:
                    table_name = str(token)
                table_seen = False
                
            # Find the database name after DATABASE
            if database_seen and token.ttype is None:
                # This should be the database identifier
                if hasattr(token, 'get_name'):
                    database_name = token.get_name()
                else:
                    database_name = str(token)
                database_seen = False
            
            # Extract column definitions for CREATE TABLE or column name for CREATE INDEX
            if str(token) == "(" and (table_name or index_name):
                column_list_started = True
                idx = parsed.token_index(token)
                # Look for closing parenthesis
                for i in range(idx + 1, len(parsed.tokens)):
                    if str(parsed.tokens[i]) == ")":
                        # Extract everything between parentheses
                        col_tokens = parsed.tokens[idx+1:i]
                        
                        if index_name:
                            # For CREATE INDEX, get the column name
                            for t in col_tokens:
                                if not t.is_whitespace and str(t) != ',':
                                    column_name = str(t).strip()
                                    break
                        else:
                            # For CREATE TABLE, split by commas to get individual column definitions
                            def_str = "".join(str(t) for t in col_tokens)
                            col_defs = [d.strip() for d in def_str.split(',')]
                            
                            for col_def in col_defs:
                                # Check if this is a column definition or constraint
                                words = col_def.split()
                                if not words:
                                    continue
                                    
                                if words[0].upper() in ('PRIMARY', 'FOREIGN', 'UNIQUE', 'CHECK', 'CONSTRAINT'):
                                    constraints.append(col_def)
                                else:
                                    # This is a column definition
                                    columns.append(col_def)
                        
                        break
        
        # Update result with extracted components
        result.update({
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
        
        # Process tokens
        table_seen = False
        database_seen = False
        index_seen = False
        on_seen = False
        
        for token in parsed.tokens:
            # Skip whitespace and punctuation
            if token.is_whitespace or str(token) == ';':
                continue
                
            # Check if it's a DROP TABLE statement
            if token.is_keyword and token.value.upper() == "TABLE":
                table_seen = True
                continue
                
            # Check if it's a DROP DATABASE statement
            if token.is_keyword and token.value.upper() == "DATABASE":
                database_seen = True
                continue
                
            # Check if it's a DROP INDEX statement
            if token.is_keyword and token.value.upper() == "INDEX":
                index_seen = True
                continue
                
            # Check if it's the ON keyword (for DROP INDEX ON table)
            if token.is_keyword and token.value.upper() == "ON":
                on_seen = True
                continue
                
            # Find the table name after TABLE
            if table_seen and token.ttype is None:
                # This should be the table identifier
                if hasattr(token, 'get_name'):
                    table_name = token.get_name()
                else:
                    table_name = str(token)
                table_seen = False
                
            # Find the database name after DATABASE
            if database_seen and token.ttype is None:
                # This should be the database identifier
                if hasattr(token, 'get_name'):
                    database_name = token.get_name()
                else:
                    database_name = str(token)
                database_seen = False
                
            # Find the index name after INDEX
            if index_seen and token.ttype is None and not on_seen:
                # This should be the index identifier
                if hasattr(token, 'get_name'):
                    index_name = token.get_name()
                else:
                    index_name = str(token)
                index_seen = False
                
            # Find the table name after ON (for DROP INDEX)
            if on_seen and token.ttype is None:
                # This should be the table identifier for the index
                if hasattr(token, 'get_name'):
                    table_name = token.get_name()
                else:
                    table_name = str(token)
                on_seen = False
        
        # Update result with extracted components
        result.update({
            "table": table_name,
            "database": database_name,
            "index": index_name
        })

    def _extract_show_elements(self, parsed, result):
        """Extract elements from a SHOW statement"""
        object_type = None
        table_name = None
        
        # Process tokens
        for token in parsed.tokens:
            # Skip whitespace and punctuation
            if token.is_whitespace or str(token) == ';':
                continue
                
            # The token right after "SHOW" is what we want to show
            if token.is_keyword:
                object_type = token.value.upper()
                
            # Check if we're showing indexes FOR a specific table
            if token.is_keyword and token.value.upper() == "FOR" and object_type == "INDEXES":
                # The next token should be the table name
                idx = parsed.token_index(token)
                if idx < len(parsed.tokens) - 1:
                    table_token = parsed.tokens[idx + 1]
                    if hasattr(table_token, 'get_name'):
                        table_name = table_token.get_name()
                    else:
                        table_name = str(table_token)
        
        # Update result with extracted components
        result.update({
            "object": object_type,
            "table": table_name
        })

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
            return {"session_id": session_id, "role": user["role"]}
        else:
            logging.warning(f"Failed login attempt for user: {username}")
            return "Invalid username or password."
    
    def handle_register(self, data):
        """Handle user registration."""
        username = data.get('username')
        logging.info(f"Registration attempt for user: {username}")
        
        password = data.get('password')
        role = data.get('role', "user")
        
        result = self.catalog_manager.register_user(username, password, role)
        if "error" not in str(result).lower():
            logging.info(f"User {username} registered successfully with role: {role}")
        else:
            logging.warning(f"Failed registration for user {username}: {result}")
        return result

    def handle_logout(self, data):
        """Handle user logout."""
        session_id = data.get('session_id')
        if session_id in self.sessions:
            username = self.sessions[session_id].get('username', 'Unknown')
            del self.sessions[session_id]
            logging.info(f"User {username} logged out successfully")
            return "Logged out successfully."
        else:
            logging.warning(f"Invalid logout attempt with session ID: {session_id}")
            return "Invalid session ID."

    def handle_query(self, data):
        """Handle query execution with role-based access control."""
        session_id = data.get('session_id')
        if session_id not in self.sessions:
            logging.warning(f"Unauthorized query attempt with invalid session ID: {session_id}")
            return "Unauthorized. Please log in."
        
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
                return error_msg
            
            # Plan and optimize the query
            logging.debug("Planning query...")
            plan = self.planner.plan_query(parsed_query)
            logging.debug("Optimizing query...")
            optimized_plan = self.optimizer.optimize(plan)
            
            # Execute the query
            logging.debug("Executing query...")
            result = self.execution_engine.execute(optimized_plan)
            logging.info(f"Query executed successfully")
            return result
        else:
            logging.warning(f"Access denied for query: {query}")
            return "Access denied. You do not have permission to execute this query."

    def is_query_allowed(self, role, query):
        """Check if the query is allowed for the given role."""
        # Only admins can perform these operations
        if role != "admin" and (
            "CREATE TABLE" in query.upper() or 
            "DROP TABLE" in query.upper() or
            "CREATE DATABASE" in query.upper() or
            "DROP DATABASE" in query.upper() or
            "CREATE INDEX" in query.upper() or
            "DROP INDEX" in query.upper()
        ):
            logging.warning(f"Permission denied: {role} role attempted restricted operation")
            return False
        return True
    
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