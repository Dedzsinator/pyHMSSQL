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
import argparse  # Make sure argparse is imported at the top level
from typing import Optional, Tuple

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Standard library imports

# Local imports - these need the path modification above
from parser import SQLParser  # noqa: E402
from shared.constants import SERVER_HOST, SERVER_PORT  # noqa: E402
from optimizer import Optimizer, TableStatistics  # noqa: E402
from execution_engine import ExecutionEngine  # noqa: E402
from planner import Planner  # noqa: E402
from ddl_processor.index_manager import IndexManager  # noqa: E402
from catalog_manager import CatalogManager  # noqa: E402
from scaler import ReplicationManager, ReplicationMode, ReplicaRole  # noqa: E402

# Import geo-aware load balancer (optional)
try:
    from geo_load_balancer import GeoLoadBalancerIntegration
    GEO_LOAD_BALANCER_AVAILABLE = True
except ImportError:
    GEO_LOAD_BALANCER_AVAILABLE = False

# Import CyCore high-performance components (optional)
try:
    from cycore import HLCTimestamp, HybridLogicalClock, SwissMap
    from cycore import get_info as cycore_info
    CYCORE_AVAILABLE = True
    CYCORE_INFO = cycore_info()
except ImportError:
    HLCTimestamp = HybridLogicalClock = SwissMap = None
    CYCORE_AVAILABLE = False
    CYCORE_INFO = {"hlc_implementation": "none", "hashmap_implementation": "none"}

# Import Rust sidecar integration (optional)
try:
    from rust_sidecar import HybridGeoLoadBalancer, SidecarConfig
    RUST_SIDECAR_AVAILABLE = True
except ImportError:
    HybridGeoLoadBalancer = SidecarConfig = None
    RUST_SIDECAR_AVAILABLE = False


def setup_logging():
    """
    Configure logging with output only to a log file, not to console.

    Returns:
        str: Path to the log file
    """
    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../logs")
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

    # Add only the file handler, not the console handler
    root_logger.addHandler(file_handler)

    # Log startup message (only goes to file)
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
        global CYCORE_AVAILABLE, RUST_SIDECAR_AVAILABLE
        
        self.catalog_manager = CatalogManager(data_dir)
        self.index_manager = IndexManager(self.catalog_manager)

        # Initialize statistics manager for cost-based optimization
        self.statistics_manager = TableStatistics(self.catalog_manager)

        # Initialize enhanced optimizer with statistics
        self.optimizer = Optimizer(self.catalog_manager, self.index_manager)

        # Initialize planner with optimizer
        self.planner = Planner(self.catalog_manager, self.index_manager)

        self.execution_engine = ExecutionEngine(
            self.catalog_manager, self.index_manager, self.planner
        )

        # Initialize parser
        self.sql_parser = SQLParser(self.execution_engine)

        # Initialize replication manager (start as primary)
        self.replication_manager = ReplicationManager(
            mode=ReplicationMode.SEMI_SYNC, sync_replicas=1
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
        self.discovery_thread = threading.Thread(
            target=self._broadcast_presence, daemon=True
        )
        self.discovery_thread.start()
        logging.info("Discovery broadcast started for server: %s", self.server_name)
        
        # Initialize high-performance components
        self.hlc_clock = None
        self.performance_cache = None
        
        # Initialize CyCore components if available
        if CYCORE_AVAILABLE:
            try:
                # Initialize HLC for distributed timestamps
                self.hlc_clock = HybridLogicalClock()
                logging.info("✓ CyCore HLC initialized: %s", CYCORE_INFO.get('hlc_implementation'))
                
                # Initialize high-performance cache using SwissMap
                if SwissMap:
                    self.performance_cache = SwissMap()
                    logging.info("✓ CyCore hashmap initialized: %s", CYCORE_INFO.get('hashmap_implementation'))
                    
            except Exception as e:
                logging.warning("Failed to initialize CyCore components: %s", e)
                CYCORE_AVAILABLE = False
        
        # Initialize geo-aware load balancer (optional)
        self.geo_load_balancer = None
        self.rust_sidecar = None
        
        # Initialize Rust sidecar integration if available
        if RUST_SIDECAR_AVAILABLE:
            try:
                sidecar_config = SidecarConfig(
                    enabled=True,
                    sidecar_port=19999,
                    socket_path="/tmp/pyhmssql_geo_router.sock"
                )
                # We'll start this later in the start_server method
                self.rust_sidecar_config = sidecar_config
                logging.info("✓ Rust sidecar configuration ready")
            except Exception as e:
                logging.warning("Failed to configure Rust sidecar: %s", e)
                RUST_SIDECAR_AVAILABLE = False
        if GEO_LOAD_BALANCER_AVAILABLE:
            try:
                geo_config = {
                    "enable_follower_reads": True,
                    "default_staleness_ms": 1000,
                    "health_check_interval": 10
                }
                self.geo_load_balancer = GeoLoadBalancerIntegration(self, geo_config)
                logging.info("Geo-aware load balancer initialized")
            except Exception as e:
                logging.warning(f"Failed to initialize geo-aware load balancer: {e}")

    def _store_transaction_id(self, session_id, transaction_id):
        """Store a transaction ID for a session."""
        if transaction_id:
            self.active_transactions[session_id] = transaction_id
            logging.info(
                f"Stored transaction {transaction_id} for session {session_id}"
            )

    def _get_transaction_id(self, session_id):
        """Get the active transaction ID for a session."""
        return self.active_transactions.get(session_id)

    def _clear_transaction_id(self, session_id):
        """Clear the transaction ID for a session after commit/rollback."""
        if session_id in self.active_transactions:
            transaction_id = self.active_transactions.pop(session_id)
            logging.info(
                f"Cleared transaction {transaction_id} for session {session_id}"
            )

    def _log_query_audit(self, username, query):
        """
        Log a query for audit purposes.

        Args:
            username: The username of the user executing the query
            query: The SQL query string
        """
        try:
            timestamp = datetime.datetime.now().isoformat()
            log_entry = {"timestamp": timestamp, "username": username, "query": query}

            # Log to the audit log file
            logs_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "../logs"
            )
            os.makedirs(logs_dir, exist_ok=True)
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

    def _broadcast_presence(self):
        """Broadcast server presence on the network"""
        discovery_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        # Prepare server info
        server_info = {
            "name": self.server_name,
            "host": (
                self.host
                if self.host != "localhost"
                else socket.gethostbyname(socket.gethostname())
            ),
            "port": self.port,
            "service": "HMSSQL",
        }

        encoded_info = json.dumps(server_info).encode("utf-8")

        while True:
            try:
                # Broadcast to the network
                discovery_socket.sendto(encoded_info, ("<broadcast>", DISCOVERY_PORT))
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
        """Handle incoming request."""
        try:
            # Use 'action' field for determining request type
            request_type = data.get("action", data.get("type"))  # Try both fields

            if not request_type:
                return {
                    "error": "Invalid request: missing action/type",
                    "status": "error",
                }

            # Extract query for multiple handlers that need it
            query = data.get("query", "")
            session_id = data.get("session_id")

            # Process the request based on action
            if request_type == "login":
                return self.handle_login(data)
            if request_type == "query":
                # Get user from session if available
                user = None
                if session_id in self.sessions:
                    user = self.sessions[session_id].get("username")

                # Check if this is a VISUALIZE query and redirect
                if isinstance(query, str) and query.strip().upper().startswith(
                    "VISUALIZE"
                ):
                    return self.handle_visualize(data)

                # Pass the extracted query string instead of the whole data dictionary
                return self.handle_query(query, session_id, user)
            if request_type == "register":
                return self.handle_register(data)
            if request_type == "visualize":
                return self.handle_visualize(data)
            if request_type == "logout":
                return self.handle_logout(data)
            if request_type == "node":
                # New handler for node management commands
                return self.handle_node_command(data)

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
            "Visualization request from %s: %s", user.get("username", "Unknown"), query
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
            logging.warning("Invalid logout attempt with session ID: %s", session_id)
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
            logging.warning("Failed registration for user %s: %s", username, result)
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
            log_entry = {"timestamp": timestamp, "username": username, "query": query}

            # Log to the audit log file
            logs_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "../logs"
            )
            os.makedirs(logs_dir, exist_ok=True)
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
            # Check if audit_db is available
            if hasattr(self, "audit_db") and self.audit_db is not None:
                self.audit_db.audit_logs.insert_one(log_entry)
            else:
                # If audit_db is not available, just log it
                logging.debug("Audit DB not available, logging to file only")
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
            success = self.replication_manager.register_as_replica(
                primary_host, primary_port
            )

            if success:
                return {
                    "status": "success",
                    "message": f"Registered as replica to {primary_host}:{primary_port}",
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
                    "server_id": self.replication_manager.server_id,
                }
            else:
                return {
                    "status": "success",
                    "role": "replica",
                    "primary_id": self.replication_manager.primary_id,
                    "lag": self.replication_manager.oplog_position,
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
                        "status": "error",
                    }
            except RuntimeError as e:
                logging.error(f"Error checking primary status: {str(e)}")
                # Continue with promotion, but log the error

            # Update role to primary
            self.replication_manager.role = ReplicaRole.PRIMARY
            self.replication_manager.primary_id = None

            # Start accepting write operations
            logging.info(f"Promoted from replica to primary server")

            return {"status": "success", "message": "Promoted to primary"}

        return {"error": "Unknown node command", "status": "error"}

    async def route_client_request(self, client_address: str, query_type: str = "read") -> Optional[Tuple[str, int]]:
        """
        Route client request using geo-aware load balancer.
        
        Args:
            client_address: Client IP address  
            query_type: Type of query (read/write/admin)
            
        Returns:
            Tuple of (host, port) for best replica, or None if routing fails
        """
        if not self.geo_load_balancer:
            # Fallback to local server
            return (self.host, self.port)
        
        try:
            return await self.geo_load_balancer.route_client_request(client_address, query_type)
        except Exception as e:
            logging.error(f"Geo load balancer routing failed: {e}")
            # Fallback to local server
            return (self.host, self.port)

    def update_replica_health_metrics(self):
        """Update health metrics for geo-aware load balancer"""
        if not self.geo_load_balancer:
            return
        
        try:
            # Collect health metrics
            import psutil
            
            health_metrics = {
                "healthy": True,
                "cpu_load": psutil.cpu_percent(interval=1) / 100.0,
                "memory_usage": psutil.virtual_memory().percent / 100.0,
                "disk_usage": psutil.disk_usage('/').percent / 100.0,
                "connection_count": len(self.sessions),
                "is_leader": getattr(self.replication_manager, 'role', None) == ReplicaRole.PRIMARY,
                "raft_term": getattr(self.replication_manager, 'current_term', 0),
                "log_index": getattr(self.replication_manager, 'log_index', 0),
                "replication_lag_ms": 0  # TODO: Calculate actual lag
            }
            
            # Update load balancer
            node_id = self.server_name
            self.geo_load_balancer.load_balancer.update_replica_health(node_id, health_metrics)
            
        except Exception as e:
            logging.error(f"Failed to update replica health metrics: {e}")

    # ...existing code...

    def shutdown(self):
        """Perform clean shutdown tasks."""
        logging.info("Server shutting down, performing cleanup...")

        # Close any database connections
        if hasattr(self, "catalog_manager"):
            try:
                self.catalog_manager.close_all_connections()
                logging.info("Closed all database connections")
            except RuntimeError as e:
                logging.error(f"Error closing database connections: {str(e)}")

        # Stop replication
        if hasattr(self, "replication_manager"):
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
        if isinstance(result, dict) and "message" in result:
            print(result["message"])
            return

        # Handle list of dictionaries (should be converted by handle_query now)
        if isinstance(result, list) and result and isinstance(result[0], dict):
            # This shouldn't happen anymore with the fix, but keep as fallback
            if not result:
                print("0 row(s) returned")
                return

            # Get all unique columns from all records
            all_columns = set()
            for record in result:
                all_columns.update(record.keys())

            # Sort columns for consistent display
            columns = sorted(list(all_columns))

            # Prepare rows data
            rows = []
            for record in result:
                row = []
                for col in columns:
                    value = record.get(col)
                    # Handle None values
                    if value is None:
                        row.append("NULL")
                    else:
                        row.append(str(value))
                rows.append(row)

        # Then check for rows and columns (table data) - this should be the main path now
        elif isinstance(result, dict) and "rows" in result and "columns" in result:
            columns = result["columns"]
            rows = result["rows"]
        else:
            # Handle error messages
            if isinstance(result, dict) and "error" in result:
                print(f"Error: {str(result['error'])}")
                return
            # Handle any other data formats
            elif isinstance(result, dict):
                for key, value in result.items():
                    if key not in ["status", "type"]:  # Skip non-content fields
                        print(f"{key}: {value}")
                return
            else:
                print(str(result))
                return

        # Display results as a table
        if not rows:
            print("0 row(s) returned")
            return

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
                str(cell).ljust(col_widths[i]) if i < len(col_widths) else str(cell)
                for i, cell in enumerate(row)
            )
            print(row_str)

        print(f"\n{len(rows)} row(s) returned")


def start_server(
    server_name=None, replication_mode=ReplicationMode.SEMI_SYNC, sync_replicas=1
):
    """Start the DBMS server with the specified configuration."""
    server = DBMSServer(data_dir="data", server_name=server_name)

    # Configure replication manager
    server.replication_manager.mode = replication_mode
    server.replication_manager.sync_replicas = sync_replicas

    # Initialize and start Rust sidecar if available
    if RUST_SIDECAR_AVAILABLE and hasattr(server, 'rust_sidecar_config'):
        try:
            import asyncio
            # Start sidecar in background
            def start_sidecar():
                try:
                    async def run_sidecar():
                        from rust_sidecar import SidecarManager
                        sidecar_manager = SidecarManager(server.rust_sidecar_config)
                        if await sidecar_manager.start():
                            logging.info("✓ Rust geo-routing sidecar started")
                            server.rust_sidecar = sidecar_manager
                        else:
                            logging.warning("Failed to start Rust sidecar")
                    
                    # Run in new event loop
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(run_sidecar())
                except Exception as e:
                    logging.warning("Rust sidecar startup failed: %s", e)
            
            # Start sidecar in background thread
            sidecar_thread = threading.Thread(target=start_sidecar, daemon=True)
            sidecar_thread.start()
            
        except Exception as e:
            logging.warning("Failed to initialize Rust sidecar: %s", e)

    # Print a message to console indicating where logs will be stored
    print(f"DBMS Server starting. Logs will be stored in: {log_file}")
    
    # Print component integration status
    if CYCORE_AVAILABLE:
        print(f"✓ CyCore integration: HLC={CYCORE_INFO.get('hlc_implementation')}, HashMap={CYCORE_INFO.get('hashmap_implementation')}")
    if RUST_SIDECAR_AVAILABLE:
        print("✓ Rust sidecar integration: Available")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((SERVER_HOST, SERVER_PORT))
    sock.listen(5)
    logging.info("Server listening on %s:%s", SERVER_HOST, SERVER_PORT)
    print(f"Server listening on {SERVER_HOST}:{SERVER_PORT}...")

    # Start a thread to listen for incoming connections - NON-DAEMON thread
    server_thread = threading.Thread(
        target=_server_connection_handler, args=(sock, server), daemon=False
    )
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
                    logging.warning(
                        "Empty size received from %s, closing connection", address
                    )
                    continue

                size = int.from_bytes(size_bytes, byteorder="big")
                data_bytes = client.recv(size)

                if not data_bytes:
                    client.close()
                    logging.warning(
                        "Empty data received from %s, closing connection", address
                    )
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
    parser = argparse.ArgumentParser(description="Start the HMSSQL server")
    parser.add_argument("--name", help="Custom server name")
    # Add REST API related arguments
    parser.add_argument(
        "--use-api", action="store_true", help="Start the REST API server"
    )
    parser.add_argument(
        "--api-host", default="0.0.0.0", help="Host address for REST API to bind"
    )
    parser.add_argument(
        "--api-port", type=int, default=5000, help="Port for REST API to bind"
    )
    parser.add_argument(
        "--api-debug", action="store_true", help="Run REST API in debug mode"
    )

    # Add replication arguments
    parser.add_argument(
        "--replica-of", help="Primary server to replicate from (host:port)"
    )
    parser.add_argument(
        "--sync-mode",
        choices=["sync", "semi-sync", "async"],
        default="semi-sync",
        help="Replication synchronization mode",
    )
    parser.add_argument(
        "--sync-replicas",
        type=int,
        default=1,
        help="Number of replicas to wait for in semi-sync mode",
    )

    args = parser.parse_args()

    # Configure replication mode
    replication_mode = ReplicationMode.SEMI_SYNC
    if args.sync_mode == "sync":
        replication_mode = ReplicationMode.SYNC
    elif args.sync_mode == "async":
        replication_mode = ReplicationMode.ASYNC

    # Check if we should start the REST API server
    if args.use_api:
        try:
            # Import here to avoid circular imports
            from rest_api import app

            print(f"Starting HMSSQL REST API server on {args.api_host}:{args.api_port}")
            logging.info(
                f"Starting HMSSQL REST API server on {args.api_host}:{args.api_port}"
            )
            app.run(host=args.api_host, port=args.api_port, debug=args.api_debug)
        except ImportError as e:
            logging.critical(f"Failed to import REST API module: {str(e)}")
            print(f"Error: Failed to import REST API module. Make sure it's installed.")
            print(f"Error details: {str(e)}")
            sys.exit(1)
    else:
        server = start_server(
            server_name=args.name,
            replication_mode=replication_mode,
            sync_replicas=args.sync_replicas,
        )

        # If configured as replica, register with primary
        if args.replica_of and server:
            try:
                primary_host, primary_port = args.replica_of.split(":")
                primary_port = int(primary_port)

                # Register with primary
                SUCCESS = server.replication_manager.register_as_replica(
                    primary_host, primary_port
                )

                if SUCCESS:
                    logging.info(
                        "Successfully registered \
                        as replica of %s:%s",
                        primary_host,
                        primary_port,
                    )
                    print(
                        f"Successfully registered as replica of {primary_host}:{primary_port}"
                    )
                else:
                    logging.error(
                        "Failed to register \
                        as replica of %s:%s",
                        primary_host,
                        primary_port,
                    )
                    print(
                        f"Failed to register as replica of {primary_host}:{primary_port}"
                    )
            except ValueError:
                logging.error(
                    "Invalid primary server \
                    specification: %s. Use host:port format.",
                    args.replica_of,
                )
                print(
                    f"Invalid primary server specification:\
                    {args.replica_of}. Use host:port format."
                )

        try:
            # This is an infinite loop that keeps the main thread alive
            # but allows for keyboard interrupts
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            print("Keyboard interrupt received, shutting down...")
            server.shutdown()
