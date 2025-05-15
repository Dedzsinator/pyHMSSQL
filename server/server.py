"""_summary_

Returns:
    _type_: _description_
"""
from logging.handlers import RotatingFileHandler
import datetime
import re
import uuid
import threading
import platform
import time
import logging
import traceback
import json
import socket
import sys
import os
from concurrent.futures import ThreadPoolExecutor
import argparse  # Make sure argparse is imported at the top level

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Standard library imports

# Local imports - these need the path modification above
from parser import SQLParser  # noqa: E402
from shared.utils import receive_data, send_data  # noqa: E402
from shared.constants import SERVER_HOST, SERVER_PORT  # noqa: E402
from optimizer import Optimizer  # noqa: E402
from execution_engine import ExecutionEngine  # noqa: E402
from planner import Planner  # noqa: E402
from ddl_processor.index_manager import IndexManager  # noqa: E402
from catalog_manager import CatalogManager  # noqa: E402


def setup_logging():
    """
    Configure logging with colored output for better visualization of plans.

    Returns:
        str: Path to the log file
    """
    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(os.path.dirname(
        os.path.abspath(__file__)), "../logs")
    os.makedirs(logs_dir, exist_ok=True)

    # Generate log filename with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f"dbms_server_{timestamp}.log")

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)  # Set to INFO for plans to be visible

    # Remove any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create a rotating file handler
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
    )

    # Set formatter with more detailed information
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)

    # Add console handler for better debugging
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    # Add handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Log startup message
    logging.info("==== DBMS Server Starting ====")
    logging.info("Logging to: %s", log_file)

    return log_file

# Initialize logging
log_file = setup_logging()

DISCOVERY_PORT = 9998
BROADCAST_INTERVAL = 5  # seconds

class DBMSServer:
    """_summary_"""

    def __init__(self, host="localhost", port=9999, data_dir="data", server_name=None):
        self.catalog_manager = CatalogManager(data_dir)
        self.index_manager = IndexManager(self.catalog_manager)
        self.execution_engine = ExecutionEngine(self.catalog_manager, self.index_manager)
        self.sql_parser = SQLParser(self.execution_engine)
        self.planner = Planner(self.catalog_manager, self.index_manager)
        self.optimizer = Optimizer(self.catalog_manager, self.index_manager)

        self.host = host
        self.port = port
        # Use provided server name or generate default from host
        self.server_name = server_name or f"{platform.node()}'s HMSSQL Server"

        self.sessions = {}
        
        self.active_transactions = {} 
        
        logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../logs")
        os.makedirs(logs_dir, exist_ok=True)

        # Start the discovery broadcast thread
        self.discovery_thread = threading.Thread(target=self._broadcast_presence, daemon=True)
        self.discovery_thread.start()
        logging.info("Discovery broadcast started for server: %s", self.server_name)

    def _store_transaction_id(self, session_id, transaction_id):
        """Store a transaction ID for a session."""
        if transaction_id:
            self.active_transactions[session_id] = transaction_id
            logging.info(f"Stored transaction {transaction_id} for session {session_id}")

    def _get_transaction_id(self, session_id):
        """Get the active transaction ID for a session."""
        return self.active_transactions.get(session_id)
        
    def _clear_transaction_id(self, session_id):
        """Clear the transaction ID for a session after commit/rollback."""
        if session_id in self.active_transactions:
            transaction_id = self.active_transactions.pop(session_id)
            logging.info(f"Cleared transaction {transaction_id} for session {session_id}")

    def _log_query_audit(self, username, query):
        """
        Log a query for audit purposes.

        Args:
            username: The username of the user executing the query
            query: The SQL query string
        """
        try:
            timestamp = datetime.datetime.now().isoformat()
            log_entry = {"timestamp": timestamp,
                        "username": username, "query": query}

            # Log to the audit log file
            logs_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "../logs"
            )
            audit_log_file = os.path.join(logs_dir, "query_audit.log")

            with open(audit_log_file, "a", encoding="utf-8") as f:
                f.write(f"{timestamp} | {username} | {query}\n")

            # Also log to the standard logger
            logging.info("AUDIT: User %s executed: %s", username, query)

            # Optionally store in a database for more advanced auditing
            # Just log an error instead of trying to use self.audit_db which doesn't exist
            # self._store_audit_log(log_entry)
        except Exception as e:
            # Don't let logging errors affect query execution
            logging.error(f"Failed to log query audit: {str(e)}")

    def _broadcast_presence(self):
        """Broadcast server presence on the network"""
        discovery_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        # Prepare server info
        server_info = {
            "name": self.server_name,
            "host": self.host if self.host != "localhost" else\
                socket.gethostbyname(socket.gethostname()),
            "port": self.port,
            "service": "HMSSQL"
        }

        encoded_info = json.dumps(server_info).encode('utf-8')

        while True:
            try:
                # Broadcast to the network
                discovery_socket.sendto(encoded_info, ('<broadcast>', DISCOVERY_PORT))
                logging.debug("Broadcast server info: %s", server_info)
            except RuntimeError as e:
                logging.error("Error broadcasting presence: %s", str(e))

            # Wait before next broadcast
            time.sleep(BROADCAST_INTERVAL)

    def _log_plan_details(self, plan, indent=0):
        """
        Helper function to log plan details in a readable format.

        Args:
            plan: The execution/optimization plan
            indent: Current indentation level for formatting nested plans
        """
        indent_str = "  " * indent
        plan_type = plan.get("type", "UNKNOWN")

        # Log plan type with indentation
        logging.info("%s%s", indent_str, plan_type)

        # Log plan details based on type
        for key, value in plan.items():
            if key == "type" or key == "child" or isinstance(value, dict):
                continue

            # Format lists nicely
            if isinstance(value, list):
                if value and len(value) > 0:
                    if isinstance(value[0], dict):
                        logging.info("%s  %s:", indent_str, key)
                        for item in value:
                            logging.info("%s    - %s", indent_str, item)
                    else:
                        logging.info("%s  %s: %s", indent_str, key, value)
                else:
                    logging.info("%s  %s: []", indent_str, key)
            else:
                logging.info("%s  %s: %s", indent_str, key, value)

        # Recursively log child plans
        if "child" in plan and isinstance(plan["child"], dict):
            logging.info("%s  child:", indent_str)
            self._log_plan_details(plan["child"], indent + 1)

    def _validate_session(self, session_token):
        """Validate a session token"""
        return session_token in self.sessions

    def handle_request(self, data):
        """
        Handle incoming request.
        """
        try:
            # Use 'action' field for determining request type
            request_type = data.get(
                "action", data.get("type"))  # Try both fields

            if not request_type:
                logging.error("Missing request type in request: %s", data)
                return {"error": "Missing request type in request", "status": "error"}

            # Process the request based on action
            if request_type == "login":
                return self.handle_login(data)
            elif request_type == "query":
                # Extract the query string from the data dictionary
                query = data.get("query", "")
                session_id = data.get("session_id")
                
                # Get user from session if available
                user = None
                if session_id in self.sessions:
                    user = self.sessions[session_id].get("username")
                    
                # Check if this is a VISUALIZE query and redirect
                if isinstance(query, str) and query.strip().upper().startswith("VISUALIZE"):
                    return self.handle_visualize(data)
                    
                # Pass the extracted query string instead of the whole data dictionary
                return self.handle_query(query, session_id, user)
            elif request_type == "register":
                return self.handle_register(data)
            elif request_type == "visualize":
                return self.handle_visualize(data)
            elif request_type == "logout":
                return self.handle_logout(data)
            else:
                logging.error("Unknown request type: %s", request_type)
                return {
                    "error": f"Unknown request type: {request_type}",
                    "status": "error",
                }
        except (TypeError, ValueError, KeyError, AttributeError, RuntimeError) as e:
            logging.error("Error handling request: %s", str(e))
            logging.error(traceback.format_exc())
            return {"error": f"Server error: {str(e)}", "status": "error"}

    def handle_visualize(self, data):
        """Handle visualization requests."""
        session_id = data.get("session_id")
        if session_id not in self.sessions:
            logging.warning(
                "Unauthorized visualize attempt with invalid session ID: {%s",
                session_id,
            )
            return {"error": "Unauthorized. Please log in.", "status": "error"}

        user = self.sessions[session_id]
        query = data.get("query", "")
        logging.info(
            "Visualization request from %s: %s", user.get(
                "username", "Unknown"), query
        )

        # Parse as a visualization command using the parser
        parsed = self.sql_parser.parse_visualize_command(query)

        if parsed:
            # Execute the visualization
            try:
                result = self.execution_engine.execute(parsed)
                if isinstance(result, dict) and "type" in result:
                    del result["type"]  # Remove type field from response

                # Ensure proper response format
                if isinstance(result, dict) and "status" not in result:
                    result["status"] = "success"

                return result
            except (TypeError, ValueError, KeyError, AttributeError, RuntimeError) as e:
                error_msg = "Error executing visualization: %s", str(e)
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
            match = re.search(
                r"VISUALIZE\s+BPTREE\s+(\w+)\s+ON\s+(\w+)", query, re.IGNORECASE
            )
            if match:
                # We have both an index name and table name
                result["index_name"] = match.group(1)
                result["table"] = match.group(2)
            else:
                # Check for pattern: VISUALIZE BPTREE ON <table_name>
                match = re.search(
                    r"VISUALIZE\s+BPTREE\s+ON\s+(\w+)", query, re.IGNORECASE
                )
                if match:
                    result["table"] = match.group(1)
                    # Don't set index_name to indicate all indexes on this table
                # Otherwise, just use VISUALIZE BPTREE (for all B+ trees)

        return result

    def handle_login(self, data):
        """Handle user login."""
        username = data.get("username")
        logging.info("Login attempt for user: %s", username)

        password = data.get("password")

        user = self.catalog_manager.authenticate_user(username, password)
        if user:
            # Create a new session
            session_id = str(uuid.uuid4())
            self.sessions[session_id] = user
            logging.info(
                "User %s logged in successfully (role: %s)", username, user["role"]
            )
            return {
                "session_id": session_id,
                "role": user["role"],
                "status": "success",
                "message": f"Login successful as {username} ({user['role']})",
            }
        else:
            logging.warning("Failed login attempt for user: %s", username)
            return {"error": "Invalid username or password.", "status": "error"}

    def handle_logout(self, data):
        """Handle user logout."""
        session_id = data.get("session_id")
        if session_id in self.sessions:
            username = self.sessions[session_id].get("username", "Unknown")
            del self.sessions[session_id]
            logging.info("User %s logged out successfully", username)
            return {"message": "Logged out successfully.", "status": "success"}
        else:
            logging.warning(
                "Invalid logout attempt with session ID: %s", session_id)
            return {"error": "Invalid session ID.", "status": "error"}

    def handle_register(self, data):
        """Handle user registration."""
        username = data.get("username")
        logging.info("Registration attempt for user: %s", username)

        password = data.get("password")
        role = data.get("role", "user")

        result = self.catalog_manager.register_user(username, password, role)
        if "error" not in str(result).lower():
            logging.info(
                "User %s registered successfully with role: %s", username, role
            )
            return {
                "message": f"User {username} registered successfully as {role}",
                "status": "success",
            }
        else:
            logging.warning(
                "Failed registration for user %s: %s", username, result)
            if isinstance(result, str):
                return {"error": result, "status": "error"}
            return result

    def _log_query(self, username, query):
        """
        Log a query for audit purposes.

        Args:
            username: The username of the user executing the query
            query: The SQL query string
        """
        try:
            timestamp = datetime.datetime.now().isoformat()
            log_entry = {"timestamp": timestamp,
                         "username": username, "query": query}

            # Log to the audit log file
            logs_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "../logs"
            )
            audit_log_file = os.path.join(logs_dir, "query_audit.log")

            with open(audit_log_file, "a", encoding="utf-8") as f:
                f.write(f"{timestamp} | {username} | {query}\n")

            # Also log to the standard logger
            logging.info("AUDIT: User %s executed: %s", username, query)

            # Optionally store in a database for more advanced auditing
            self._store_audit_log(log_entry)

        except (IOError, PermissionError, FileNotFoundError) as e:
            # Don't let logging errors affect query execution
            logging.error("Failed to log query: %s", str(e))

    def _store_audit_log(self, log_entry):
        """
        Store audit log entry in a persistent store for advanced querying.
        This is an optional enhancement for more sophisticated auditing.

        Args:
            log_entry: Dictionary containing audit information
        """
        try:
            self.audit_db.audit_logs.insert_one(log_entry)
        except (
            ValueError,
            SyntaxError,
            TypeError,
            AttributeError,
            KeyError,
            RuntimeError,
        ) as e:
            logging.error("Failed to store audit log: %s", str(e))

    def handle_query(self, query, session_id=None, user=None):
        """Handle a query request."""
        start_time = time.time()
        if not query:
            return {"error": "Empty query", "status": "error"}
        
        logging.info("Query from %s: %s", user or "anonymous", query)
        
        # Log to audit trail
        try:
            self._log_query_audit(user, query)
        except AttributeError:
            # Fallback to _log_query if _log_query_audit doesn't exist
            try:
                self._log_query(user, query)
            except AttributeError:
                logging.error("Failed to log audit: neither _log_query_audit nor _log_query methods defined")
        
        # For debugging
        logging.info("Current database: %s", self.catalog_manager.get_current_database())
        
        # Check for transaction control statements
        is_transaction_start = query.strip().upper().startswith("BEGIN TRANSACTION")
        is_transaction_commit = query.strip().upper().startswith("COMMIT")
        is_transaction_rollback = query.strip().upper().startswith("ROLLBACK")
        
        if is_transaction_start:
            logging.info("Detected BEGIN TRANSACTION statement")
        elif is_transaction_commit:
            logging.info("Detected COMMIT statement")
        elif is_transaction_rollback:
            logging.info("Detected ROLLBACK TRANSACTION statement")
        
        try:
            # Parse the query - FIXED: using parse_sql instead of parse_query
            parsed_query = self.sql_parser.parse_sql(query)
            logging.info("▶️ Parsed query: %s", parsed_query)
            
            # Plan the query
            plan = self.planner.plan_query(parsed_query)
            
            # Add session ID to plan
            if session_id:
                plan["session_id"] = session_id
            
            # Important: Add transaction ID to the plan if in transaction
            if session_id and session_id in self.active_transactions:
                plan["transaction_id"] = self.active_transactions[session_id]
                logging.info(f"Added transaction ID {plan['transaction_id']} to query plan")
            
            # Execute the query
            result = self.execution_engine.execute(plan)
            
            # Handle transaction operations
            if is_transaction_start and result.get("status") == "success":
                # Store transaction ID in session
                transaction_id = result.get("transaction_id")
                if session_id and transaction_id:
                    self._store_transaction_id(session_id, transaction_id)
            elif (is_transaction_commit or is_transaction_rollback) and result.get("status") == "success":
                # Clear transaction ID after commit or rollback
                if session_id and session_id in self.active_transactions:
                    self._clear_transaction_id(session_id)
            
            # Calculate query execution time
            execution_time_ms = (time.time() - start_time) * 1000
            
            # Add execution time to result
            if isinstance(result, dict):
                result["execution_time_ms"] = round(execution_time_ms, 2)
            
            logging.info("⏱️ Query executed in %.2f ms", execution_time_ms)
            
            return result
            
        except Exception as e:
            error_msg = str(e)
            stack_trace = traceback.format_exc()
            logging.error("Error executing query: %s", error_msg)
            logging.error(stack_trace)
            return {"error": error_msg, "status": "error", "execution_time_ms": round((time.time() - start_time) * 1000, 2)}

    def _store_transaction_id(self, session_id, transaction_id):
        """Store a transaction ID for a session."""
        if session_id and transaction_id:
            self.active_transactions[session_id] = transaction_id
            logging.info(f"Stored transaction ID {transaction_id} for session {session_id}")
        else:
            logging.warning(f"Couldn't store transaction ID: session_id={session_id}, transaction_id={transaction_id}")

    def _clear_transaction_id(self, session_id):
        """Clear the transaction ID for a session."""
        if session_id and session_id in self.active_transactions:
            transaction_id = self.active_transactions.pop(session_id)
            logging.info(f"Cleared transaction ID {transaction_id} for session {session_id}")
        else:
            logging.warning(f"No transaction found for session {session_id}")

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
        if any(
            query_upper.startswith(prefix)
            for prefix in [
                "DROP DATABASE",
                "CREATE DATABASE",
                "CREATE USER",
                "DROP USER",
                "ALTER USER",
                "GRANT",
                "REVOKE",
            ]
        ):
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
            header = " | ".join(
                str(col).ljust(col_widths[i]) for i, col in enumerate(columns)
            )
            separator = "-+-".join("-" * width for width in col_widths)
            print(header)
            print(separator)

            # Print rows
            for row in rows:
                row_str = " | ".join(
                    str(cell).ljust(col_widths[i]) if i < len(
                        col_widths) else str(cell)
                    for i, cell in enumerate(row)
                )
                print(row_str)

            print("\n{%i row(s) returned", len(rows))
        # Handle error messages
        elif "error" in result:
            print(f"Error: {str(result['error'])}")
        # Handle any other data formats
        else:
            # If it's just key-value pairs without a clear format, print them nicely
            for key, value in result.items():
                if key not in ["status", "type"]:  # Skip non-content fields
                    print(f"{key}: {value}")

def start_server(server_name=None):
    """_summary_"""
    # Make sure sqlparse is installed
    try:
        import sqlparse
    except ImportError:
        logging.critical("Error: sqlparse library is required but not installed.")
        print("Error: sqlparse library is required but not installed.")
        print("Please install it using: pip install sqlparse")
        sys.exit(1)

    server = DBMSServer(data_dir="data", server_name=server_name)

    # Print a message to console indicating where logs will be stored
    print(f"DBMS Server starting. Logs will be stored in: {log_file}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((SERVER_HOST, SERVER_PORT))
    sock.listen(5)
    logging.info("Server listening on %s:%s", SERVER_HOST, SERVER_PORT)
    print(f"Server listening on {SERVER_HOST}:{SERVER_PORT}...")

    try:
        while True:
            client, address = sock.accept()
            logging.info("Connection from %s", address)
            print(f"Connection from {address}")

            try:
                data = receive_data(client)
                if data:
                    response = server.handle_request(data)
                    send_data(client, response)
                client.close()
            except (
                ConnectionError,
                json.JSONDecodeError,
                ValueError,
                BrokenPipeError,
            ) as e:
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
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Start the HMSSQL server')
    parser.add_argument('--name', help='Custom server name')
    # Add REST API related arguments
    parser.add_argument('--use-api', action='store_true', help='Start the REST API server')
    parser.add_argument('--api-host', default='0.0.0.0', help='Host address for REST API to bind')
    parser.add_argument('--api-port', type=int, default=5000, help='Port for REST API to bind')
    parser.add_argument('--api-debug', action='store_true', help='Run REST API in debug mode')

    args = parser.parse_args()
    
    # Check if we should start the REST API server
    if args.use_api:
        try:
            # Import here to avoid circular imports
            from rest_api import app
            
            print(f"Starting HMSSQL REST API server on {args.api_host}:{args.api_port}")
            logging.info(f"Starting HMSSQL REST API server on {args.api_host}:{args.api_port}")
            app.run(host=args.api_host, port=args.api_port, debug=args.api_debug)
        except ImportError as e:
            logging.critical(f"Failed to import REST API module: {str(e)}")
            print(f"Error: Failed to import REST API module. Make sure it's installed.")
            print(f"Error details: {str(e)}")
            sys.exit(1)
    else:
        # Start the regular socket server
        start_server(server_name=args.name)
