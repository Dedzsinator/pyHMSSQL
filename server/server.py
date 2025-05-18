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
import hashlib

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Standard library imports

# Local imports - these need the path modification above
from parser import SQLParser  # noqa: E402
from shared.utils import receive_data, send_data  # noqa: E402
from shared.constants import SERVER_HOST, SERVER_PORT  # noqa: E402
from optimizer import Optimizer, TableStatistics  # noqa: E402
from execution_engine import ExecutionEngine  # noqa: E402
from planner import Planner  # noqa: E402
from ddl_processor.index_manager import IndexManager  # noqa: E402
from catalog_manager import CatalogManager  # noqa: E402
from buffer_pool_manager import BufferPool, CacheStrategy  # noqa: E402
from scaler import ReplicationManager, ReplicationMode,ReplicaRole  # noqa: E402
from buffer_pool_manager import BufferPool, CacheStrategy, QueryResultCache  # noqa: E402

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

        # Initialize statistics manager for cost-based optimization
        self.statistics_manager = TableStatistics(self.catalog_manager)

        # Initialize buffer pool for caching
        self.buffer_pool = BufferPool(capacity=1000, strategy=CacheStrategy.HYBRID)

        # Initialize query cache
        self.query_cache = QueryResultCache(capacity=100)

        # Initialize execution engine with new components
        self.execution_engine = ExecutionEngine(
            self.catalog_manager,
            self.index_manager
        )

        # Initialize parser
        self.sql_parser = SQLParser(self.execution_engine)

        # Initialize enhanced optimizer with statistics
        self.optimizer = Optimizer(self.catalog_manager, self.index_manager)

        # Initialize planner with optimizer
        self.planner = Planner(self.catalog_manager, self.index_manager)

        # Initialize replication manager (start as primary)
        self.replication_manager = ReplicationManager(
            mode=ReplicationMode.SEMI_SYNC,
            sync_replicas=1
        )

        self.host = host
        self.port = port
        # Use provided server name or generate default from host
        self.server_name = server_name or f"{platform.node()}'s HMSSQL Server"

        self.sessions = {}

        self.active_transactions = {}

        self._statistics_manager = None
        self._optimizer = None
        self._parallel_coordinator = None

        # Property for lazy loading
        @property
        def statistics_manager(self):
            if self._statistics_manager is None:
                self._statistics_manager = TableStatistics(self.catalog_manager)
            return self._statistics_manager

        @property
        def optimizer(self):
            if self._optimizer is None:
                self._optimizer = Optimizer(self.catalog_manager, self.index_manager)
            return self._optimizer

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
        except RuntimeError as e:
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

            # Extract query for multiple handlers that need it
            query = data.get("query", "")
            session_id = data.get("session_id")

            # Handle CACHE command as a special case if query starts with CACHE
            if isinstance(query, str) and query.strip().upper().startswith("CACHE "):
                return self.handle_cache_command(query, session_id)

            # Process the request based on action
            if request_type == "login":
                return self.handle_login(data)
            elif request_type == "query":
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
            elif request_type == "node":
                # New handler for node management commands
                return self.handle_node_command(data)
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
                else:
                    # Default case - visualize all B+ trees
                    result["table"] = None
                    result["index_name"] = None

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

    def handle_node_command(self, data):
        """Handle node management commands for replication."""
        command = data.get("command")

        if command == "set_as_replica":
            primary_host = data.get("primary_host")
            primary_port = data.get("primary_port")

            if not primary_host or not primary_port:
                return {"error": "Missing primary host or port", "status": "error"}

            # Try to register with the primary
            success = self.replication_manager.register_as_replica(primary_host, primary_port)

            if success:
                return {
                    "status": "success",
                    "message": f"Registered as replica to {primary_host}:{primary_port}"
                }
            else:
                return {"error": "Failed to register as replica", "status": "error"}

        elif command == "get_status":
            # Get replication status
            if self.replication_manager.role == ReplicaRole.PRIMARY:
                replicas = self.replication_manager.get_replica_status()
                return {
                    "status": "success",
                    "role": "primary",
                    "replicas": replicas,
                    "server_id": self.replication_manager.server_id
                }
            else:
                return {
                    "status": "success",
                    "role": "replica",
                    "primary_id": self.replication_manager.primary_id,
                    "lag": self.replication_manager.oplog_position
                }

        elif command == "promote_to_primary":
            # Only allow if current role is replica
            if self.replication_manager.role != ReplicaRole.REPLICA:
                return {"error": "Only a replica can be promoted", "status": "error"}

            # by trying to connect to it or verify the election mechanism
            try:
                # Get the primary information
                primary_host = self.replication_manager.primary_host
                primary_port = self.replication_manager.primary_port

                # Try to connect to primary to see if it's still alive
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(3.0)  # Short timeout
                primary_alive = False

                try:
                    sock.connect((primary_host, primary_port))
                    # If we can connect, primary is still alive
                    primary_alive = True
                except (ConnectionRefusedError, socket.timeout, socket.error):
                    # Can't connect - primary is likely down
                    primary_alive = False
                finally:
                    sock.close()

                if primary_alive:
                    return {
                        "error": "Cannot promote to primary: current primary is still active",
                        "status": "error"
                    }
            except RuntimeError as e:
                logging.error(f"Error checking primary status: {str(e)}")
                # Continue with promotion, but log the error

            # Update role to primary
            self.replication_manager.role = ReplicaRole.PRIMARY
            self.replication_manager.primary_id = None

            # Start accepting write operations
            logging.info(f"Promoted from replica to primary server")

            return {
                "status": "success",
                "message": "Promoted to primary"
            }

        return {"error": "Unknown node command", "status": "error"}

    def handle_query(self, query, session_id=None, user=None):
        """Handle a query request with enhanced optimization and smart caching."""
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

        # Fast path for common metadata queries
        if query.strip().upper() == "SHOW DATABASES":
            databases = self.catalog_manager.list_databases()
            return {
                "status": "success",
                "columns": ["Database"],
                "rows": [[db] for db in databases],
                "execution_time_ms": round((time.time() - start_time) * 1000, 2)
            }

        if query.strip().upper() == "SHOW TABLES":
            db = self.catalog_manager.get_current_database()
            if not db:
                return {"error": "No database selected", "status": "error"}

            tables = self.catalog_manager.list_tables()
            return {
                "status": "success",
                "columns": ["Table"],
                "rows": [[tbl] for tbl in tables],
                "execution_time_ms": round((time.time() - start_time) * 1000, 2)
            }

        # Check if this is a DML query that modifies data
        is_modifying_query = query.strip().upper().startswith(("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "BEGIN", "COMMIT", "ROLLBACK"))

        # Only check cache for non-modifying queries
        query_hash = hashlib.md5(query.strip().lower().encode()).hexdigest()
        if not is_modifying_query:
            cached_result = self.query_cache.get(query_hash)

            if cached_result:
                logging.info("Query cache hit for: %s", query)
                # Add execution time for consistency
                if isinstance(cached_result, dict) and "execution_time_ms" not in cached_result:
                    cached_result["execution_time_ms"] = 0.0
                return cached_result

        # Current database is useful for debugging
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
            # Parse the query
            parsed_query = self.sql_parser.parse_sql(query)
            logging.info("▶️ Parsed query: %s", parsed_query)

            # Plan the query
            plan = self.planner.plan_query(parsed_query)

            # Check if plan is None or invalid
            if not plan or not isinstance(plan, dict):
                return {"error": "Failed to generate a valid query plan", "status": "error"}

            # Ensure plan has a 'type' key
            if "type" not in plan and parsed_query and "type" in parsed_query:
                plan["type"] = parsed_query["type"]
                logging.info(f"Added missing 'type' from parsed query: {plan['type']}")

            # Special handling for UNION/INTERSECT/EXCEPT operations
            if parsed_query and parsed_query.get("type") in ["UNION", "INTERSECT", "EXCEPT"]:
                if "type" not in plan:
                    plan["type"] = parsed_query["type"]
                # Make sure left and right queries are included
                if "left" in parsed_query and "left" not in plan:
                    plan["left"] = parsed_query["left"]
                if "right" in parsed_query and "right" not in plan:
                    plan["right"] = parsed_query["right"]

            if is_modifying_query and parsed_query.get("type") == "INSERT" and "values" in parsed_query:
                # Make sure plan uses the correct values from the parsed query
                if "values" in parsed_query and "values" in plan:
                    plan["values"] = parsed_query["values"]

            # Get table name for potential cache update
            table_name = None
            if "table" in plan:
                table_name = plan["table"]
            elif "table_name" in plan:
                table_name = plan["table_name"]

            # Add session ID to plan
            if session_id:
                plan["session_id"] = session_id

            # Add transaction ID to the plan if in transaction
            if session_id and session_id in self.active_transactions:
                plan["transaction_id"] = self.active_transactions[session_id]
                logging.info(f"Added transaction ID {plan['transaction_id']} to query plan")

            # Apply optimization with safeguards
            try:
                optimized_plan = self.optimizer.optimize(plan)
                logging.info("✅ Optimized query plan")

                # Ensure optimized_plan retains type information
                if "type" not in optimized_plan and "type" in plan:
                    optimized_plan["type"] = plan["type"]
                    logging.info(f"Preserved plan type after optimization: {optimized_plan['type']}")

            except (TypeError, AttributeError, KeyError) as e:
                # Bypass optimization on error
                logging.warning(f"Bypassing optimizer due to error: {str(e)}. Using unoptimized plan.")
                optimized_plan = plan.copy() if isinstance(plan, dict) else plan

                # Ensure plan type is present
                if isinstance(optimized_plan, dict) and "type" not in optimized_plan and parsed_query and "type" in parsed_query:
                    optimized_plan["type"] = parsed_query["type"]

            # Execute the query with optimized plan
            result = self.execution_engine.execute(optimized_plan)

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

            # Calculate execution time
            execution_time_ms = (time.time() - start_time) * 1000

            # Add execution time to result
            if isinstance(result, dict):
                result["execution_time_ms"] = round(execution_time_ms, 2)

            # Cache management
            if not is_modifying_query:
                # Cache the result for SELECT queries with related table information
                affected_tables = []

                # Determine tables affected by this query
                if table_name:
                    affected_tables.append(table_name)
                elif "tables" in parsed_query:
                    affected_tables.extend(parsed_query["tables"])

                # Cache with table information
                if hasattr(self.query_cache, 'put'):
                    self.query_cache.put(query_hash, result, affected_tables=affected_tables)
            else:
                # For modifying queries, invalidate related caches
                if table_name:
                    self.invalidate_caches_for_table(table_name)

            logging.info("⏱️ Query executed in %.2f ms", execution_time_ms)
            return result

        except RuntimeError as e:
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

    def handle_cache_command(self, query, session_id):
        """Handle CACHE management commands.

        Args:
            query: The SQL query string (should start with CACHE)
            session_id: The session ID

        Returns:
            dict: Command result
        """
        # Check if user has admin rights
        if session_id not in self.sessions or self.sessions[session_id].get("role") != "admin":
            return {
                "error": "CACHE commands require admin privileges",
                "status": "error"
            }

        query_upper = query.strip().upper()

        # Handle different cache commands
        if query_upper == "CACHE STATS":
            # Get cache statistics
            stats = self.get_cache_stats()
            return {
                "status": "success",
                "stats": stats,
                "message": "Cache statistics retrieved successfully"
            }
        elif query_upper == "CACHE CLEAR ALL":
            # Clear all caches
            if hasattr(self, 'query_cache'):
                self.query_cache.clear()
            if hasattr(self, 'buffer_pool'):
                self.buffer_pool.flush_all()
            return {
                "status": "success",
                "message": "All caches cleared successfully"
            }
        elif query_upper.startswith("CACHE CLEAR TABLE "):
            # Extract table name
            table_name = query_upper.replace("CACHE CLEAR TABLE ", "").strip()
            if not table_name:
                return {"error": "Table name required", "status": "error"}

            # Invalidate caches for this table
            stats = self.invalidate_caches_for_table(table_name)
            return {
                "status": "success",
                "stats": stats,
                "message": f"Cache entries for table {table_name} cleared successfully"
            }
        else:
            return {
                "error": "Unknown CACHE command. Supported commands: CACHE STATS, CACHE CLEAR ALL, CACHE CLEAR TABLE <name>",
                "status": "error"
            }

    def get_cache_stats(self):
        """Get statistics about all caching components.

        Returns:
            dict: Combined statistics from all caching components
        """
        stats = {}

        # Buffer pool stats
        if hasattr(self, 'buffer_pool'):
            try:
                stats["buffer_pool"] = self.buffer_pool.get_stats()
            except RuntimeError as e:
                stats["buffer_pool"] = {"error": str(e)}

        # Query cache stats
        if hasattr(self, 'query_cache'):
            try:
                stats["query_cache"] = self.query_cache.get_stats()
            except RuntimeError as e:
                stats["query_cache"] = {"error": str(e)}

        # Overall memory usage estimation
        import psutil
        try:
            process = psutil.Process()
            stats["memory_usage"] = {
                "rss": process.memory_info().rss / (1024 * 1024),  # MB
                "vms": process.memory_info().vms / (1024 * 1024),  # MB
                "percent": process.memory_percent()
            }
        except (ImportError, Exception) as e:
            stats["memory_usage"] = {"error": str(e)}

        return stats

    def invalidate_caches_for_table(self, table_name):
        """Invalidate all caches related to a specific table.

        This method centralizes cache invalidation logic to ensure consistency
        when tables are modified.

        Args:
            table_name: Name of the modified table

        Returns:
            dict: Statistics about what was invalidated
        """
        stats = {
            "buffer_pool_pages": 0,
            "query_cache_entries": 0,
            "statistics_entries": None
        }

        # Invalidate buffer pool pages for this table
        if hasattr(self, 'buffer_pool'):
            try:
                if hasattr(self.buffer_pool, 'invalidate'):
                    pages_invalidated = self.buffer_pool.invalidate(table_name)
                    stats["buffer_pool_pages"] = pages_invalidated
                # Also check for index invalidation method
                if hasattr(self.buffer_pool, 'invalidate_index'):
                    self.buffer_pool.invalidate_index(table_name)
            except RuntimeError as e:
                logging.error(f"Error invalidating buffer pool for {table_name}: {str(e)}")

        # Invalidate query cache entries for this table
        if hasattr(self, 'query_cache'):
            try:
                # Try version update first (preferred method)
                if hasattr(self.query_cache, 'update_table_version'):
                    self.query_cache.update_table_version(table_name)
                    stats["query_cache_entries"] = 0  # Not actually invalidating any entries yet
                # Fallback to direct invalidation if available
                elif hasattr(self.query_cache, 'invalidate_for_table'):
                    entries_invalidated = self.query_cache.invalidate_for_table(table_name)
                    stats["query_cache_entries"] = entries_invalidated
            except RuntimeError as e:
                logging.error(f"Error invalidating query cache for {table_name}: {str(e)}")

        # Invalidate statistics for this table
        if hasattr(self, 'statistics_manager'):
            try:
                self.statistics_manager.invalidate_statistics(table_name)
            except RuntimeError as e:
                logging.error(f"Error invalidating statistics for {table_name}: {str(e)}")

        return stats

    def shutdown(self):
        """Perform clean shutdown tasks."""
        logging.info("Server shutting down, performing cleanup...")

        # Flush buffer pool
        if hasattr(self, 'buffer_pool'):
            try:
                flush_result = self.buffer_pool.flush_all()
                if flush_result:
                    logging.info("Successfully flushed all dirty pages from buffer pool")
                else:
                    logging.warning("Failed to flush all dirty pages from buffer pool")
            except RuntimeError as e:
                logging.error(f"Error flushing buffer pool: {str(e)}")

        # Close any database connections
        if hasattr(self, 'catalog_manager'):
            try:
                self.catalog_manager.close_all_connections()
                logging.info("Closed all database connections")
            except RuntimeError as e:
                logging.error(f"Error closing database connections: {str(e)}")

        # Stop replication
        if hasattr(self, 'replication_manager'):
            try:
                self.replication_manager.shutdown()
                logging.info("Shut down replication manager")
            except RuntimeError as e:
                logging.error(f"Error shutting down replication manager: {str(e)}")

        logging.info("Server shutdown complete")

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

def start_server(server_name=None, replication_mode=ReplicationMode.SEMI_SYNC, sync_replicas=1):
    """Start the DBMS server with the specified configuration."""
    # Make sure sqlparse is installed
    try:
        import sqlparse
    except ImportError:
        logging.critical("Error: sqlparse library is required but not installed.")
        print("Error: sqlparse library is required but not installed.")
        print("Please install it using: pip install sqlparse")
        sys.exit(1)

    server = DBMSServer(data_dir="data", server_name=server_name)

    # Configure replication manager
    server.replication_manager.mode = replication_mode
    server.replication_manager.sync_replicas = sync_replicas

    # Print a message to console indicating where logs will be stored
    print(f"DBMS Server starting. Logs will be stored in: {log_file}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((SERVER_HOST, SERVER_PORT))
    sock.listen(5)
    logging.info("Server listening on %s:%s", SERVER_HOST, SERVER_PORT)
    print(f"Server listening on {SERVER_HOST}:{SERVER_PORT}...")

    # Start a thread to listen for incoming connections - NON-DAEMON thread
    server_thread = threading.Thread(target=_server_connection_handler,
                                    args=(sock, server), daemon=False)
    server_thread.start()

    # Return server instance for further configuration
    return server

def _server_connection_handler(sock, server):
    """Handle incoming connections to the server."""
    try:
        while True:
            client, address = sock.accept()
            logging.info("Connection from %s", address)
            print(f"Connection from {address}")

            try:
                # Read the data size
                size_bytes = client.recv(4)
                if not size_bytes:
                    client.close()
                    logging.warning("Empty size received from %s, closing connection", address)
                    continue

                size = int.from_bytes(size_bytes, byteorder="big")
                data_bytes = client.recv(size)

                if not data_bytes:
                    client.close()
                    logging.warning("Empty data received from %s, closing connection", address)
                    continue

                # Parse the JSON data
                data = json.loads(data_bytes.decode())

                # Handle the request
                response = server.handle_request(data)

                # Send the response
                response_data = json.dumps(response).encode()
                client.sendall(len(response_data).to_bytes(4, byteorder="big"))
                client.sendall(response_data)
                client.close()

            except (
                ConnectionError,
                json.JSONDecodeError,
                ValueError,
                BrokenPipeError,
            ) as e:
                logging.error("Error handling client connection: %s", str(e))
                try:
                    client.close()
                except RuntimeError:
                    pass
            except RuntimeError as e:
                logging.error("Unexpected error: %s", str(e))
                logging.error(traceback.format_exc())
                try:
                    client.close()
                except RuntimeError:
                    pass
    except KeyboardInterrupt:
        shutdown_msg = "Server shutting down..."
        logging.info(shutdown_msg)
        print(shutdown_msg)
        # Call shutdown cleanup
        try:
            server.shutdown()
        except RuntimeError as e:
            logging.error("Error during server shutdown: %s", str(e))
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

    # Add replication arguments
    parser.add_argument('--replica-of', help='Primary server to replicate from (host:port)')
    parser.add_argument('--sync-mode', choices=['sync', 'semi-sync', 'async'], default='semi-sync',
                       help='Replication synchronization mode')
    parser.add_argument('--sync-replicas', type=int, default=1,
                       help='Number of replicas to wait for in semi-sync mode')

    args = parser.parse_args()

    # Configure replication mode
    replication_mode = ReplicationMode.SEMI_SYNC
    if args.sync_mode == 'sync':
        replication_mode = ReplicationMode.SYNC
    elif args.sync_mode == 'async':
        replication_mode = ReplicationMode.ASYNC

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
        server = start_server(server_name=args.name, replication_mode=replication_mode,
                            sync_replicas=args.sync_replicas)

        # If configured as replica, register with primary
        if args.replica_of and server:
            try:
                primary_host, primary_port = args.replica_of.split(':')
                primary_port = int(primary_port)

                # Register with primary
                SUCCESS = server.replication_manager.\
                    register_as_replica(primary_host, primary_port)

                if SUCCESS:
                    logging.info("Successfully registered \
                        as replica of %s:%s", primary_host, primary_port)
                    print(f"Successfully registered as replica of {primary_host}:{primary_port}")
                else:
                    logging.error("Failed to register \
                        as replica of %s:%s", primary_host, primary_port)
                    print(f"Failed to register as replica of {primary_host}:{primary_port}")
            except ValueError:
                logging.error("Invalid primary server \
                    specification: %s. Use host:port format.", args.replica_of)
                print(f"Invalid primary server specification:\
                    {args.replica_of}. Use host:port format.")

        try:
            # This is an infinite loop that keeps the main thread alive
            # but allows for keyboard interrupts
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            print("Keyboard interrupt received, shutting down...")
            server.shutdown()
