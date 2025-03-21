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
from parser import SQLParser

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
    root_logger.setLevel(logging.DEBUG)  # Set to DEBUG to capture all messages
    
    # Remove any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create a rotating file handler
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    
    # Set formatter with more detailed information
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
        
        # Initialize SQL parser
        self.sql_parser = SQLParser()
        
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
        
        # Parse as a visualization command using the parser
        parsed = self.sql_parser.parse_visualize_command(query)
        
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
                logging.error(traceback.format_exc())
                return {"error": error_msg, "status": "error"}
        else:
            return {"error": "Failed to parse visualization command", "status": "error"}

    def parse_sql(self, sql):
        """
        Parse SQL query into a structured format using the SQL parser.
        """
        return self.sql_parser.parse_sql(sql)
    
    def _parse_visualize_command(self, query):
        """
        Parse a VISUALIZE command.
        """
        result = {"type": "VISUALIZE"}
        
        # Check for VISUALIZE BPTREE
        if "BPTREE" in query.upper():
            result["object"] = "BPTREE"
            
            # Check for the pattern: VISUALIZE BPTREE <index_name> ON <table_name>
            match = re.search(r'VISUALIZE\s+BPTREE\s+(\w+)\s+ON\s+(\w+)', query, re.IGNORECASE)
            if match:
                # We have both an index name and table name
                result["index_name"] = match.group(1)
                result["table"] = match.group(2)
            else:
                # Check for pattern: VISUALIZE BPTREE ON <table_name>
                match = re.search(r'VISUALIZE\s+BPTREE\s+ON\s+(\w+)', query, re.IGNORECASE)
                if match:
                    result["table"] = match.group(1)
                    # Don't set index_name to indicate all indexes on this table
                # Otherwise, just use VISUALIZE BPTREE (for all B+ trees)
        
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