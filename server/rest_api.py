"""REST API for HMSSQL Database

This module provides a REST API interface to the HMSSQL database system.
"""
import os
import sys
import uuid
import datetime
import logging
import traceback
from flask import Flask, request, jsonify, g
from flask_cors import CORS
from functools import wraps

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import database components
from catalog_manager import CatalogManager
from ddl_processor.index_manager import IndexManager
from execution_engine import ExecutionEngine
from optimizer import Optimizer
from parser import SQLParser
from planner import Planner

# Create Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("../logs/rest_api.log"),
        logging.StreamHandler()
    ]
)

# Create database components
catalog_manager = CatalogManager(data_dir="data")
index_manager = IndexManager(catalog_manager)
planner = Planner(catalog_manager, index_manager)
execution_engine = ExecutionEngine(catalog_manager, index_manager, planner)
sql_parser = SQLParser(execution_engine)
planner = Planner(catalog_manager, index_manager)
optimizer = Optimizer(catalog_manager, index_manager)

# Store active sessions
sessions = {}

def require_auth(f):
    """Decorator to require authentication for API endpoints."""
    @wraps(f)
    def decorated_function(*func_args, **kwargs):
        # Get token from Authorization header
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"error": "Unauthorized. Missing or invalid token.", "status": "error"}), 401

        session_id = auth_header.split("Bearer ")[1].strip()
        if session_id not in sessions:
            return jsonify({"error": "Unauthorized. Invalid session.", "status": "error"}), 401

        # Store user in Flask's g object for use in the view function
        g.user = sessions[session_id]
        g.session_id = session_id
        return f(*func_args, **kwargs)
    return decorated_function

@app.route("/api/login", methods=["POST"])
def login():
    """Handle user login via the REST API."""
    data = request.json
    if not data:
        return jsonify({"error": "Missing request body", "status": "error"}), 400

    username = data.get("username")
    password = data.get("password")

    if not username or not password:
        return jsonify({"error": "Username and password required", "status": "error"}), 400

    user = catalog_manager.authenticate_user(username, password)
    if user:
        session_id = str(uuid.uuid4())
        sessions[session_id] = user
        logging.info(f"User {username} logged in successfully (role: {user['role']})")
        return jsonify({
            "session_id": session_id,
            "role": user["role"],
            "status": "success",
            "message": f"Login successful as {username} ({user['role']})"
        })

    logging.warning(f"Failed login attempt for user: {username}")
    return jsonify({"error": "Invalid username or password.", "status": "error"}), 401

@app.route("/api/register", methods=["POST"])
def register():
    """Handle user registration via the REST API."""
    data = request.json
    if not data:
        return jsonify({"error": "Missing request body", "status": "error"}), 400

    username = data.get("username")
    password = data.get("password")
    role = data.get("role", "user")

    if not username or not password:
        return jsonify({"error": "Username and password required", "status": "error"}), 400

    result = catalog_manager.register_user(username, password, role)
    if "error" not in str(result).lower():
        logging.info(f"User {username} registered successfully with role: {role}")
        return jsonify({
            "message": f"User {username} registered successfully as {role}",
            "status": "success"
        })

    logging.warning(f"Failed registration for user {username}: {result}")
    if isinstance(result, str):
        return jsonify({"error": result, "status": "error"}), 400
    return jsonify(result), 400

@app.route("/api/logout", methods=["POST"])
@require_auth
def logout():
    """Handle user logout via the REST API."""
    session_id = g.session_id
    username = g.user.get("username", "Unknown")

    del sessions[session_id]
    logging.info(f"User {username} logged out successfully")
    return jsonify({
        "message": "Logged out successfully.",
        "status": "success"
    })

@app.route("/api/query", methods=["POST"])
@require_auth
def execute_query():
    """Execute a SQL query via the REST API."""
    data = request.json
    if not data:
        return jsonify({"error": "Missing request body", "status": "error"}), 400

    query = data.get("query")
    if not query:
        return jsonify({"error": "Missing query parameter", "status": "error"}), 400

    user = g.user
    username = user.get("username", "Unknown")

    # Log the query for audit
    _log_query(username, query)

    # Check if query is allowed for the user's role
    role = user.get("role", "user")
    if not is_query_allowed(role, query):
        logging.warning(f"User {username} attempted unauthorized query: {query}")
        return jsonify({
            "error": "You don't have permission to execute this query.",
            "status": "error"
        }), 403

    # Parse and execute with planner and optimizer integration
    try:
        # 1. Parse SQL query
        parsed = sql_parser.parse_sql(query)
        if "error" in parsed:
            logging.error(f"SQL parsing error: {parsed['error']}")
            return jsonify({"error": parsed["error"], "status": "error"}), 400

        # 2. Generate execution plan using planner
        execution_plan = planner.plan_query(parsed)
        if "error" in execution_plan:
            logging.error(f"Error generating plan: {execution_plan['error']}")
            return jsonify({"error": execution_plan["error"], "status": "error"}), 400

        # 3. Optimize the execution plan
        optimized_plan = optimizer.optimize(execution_plan)
        if "error" in optimized_plan:
            logging.error(f"Error optimizing plan: {optimized_plan['error']}")
            return jsonify({"error": optimized_plan["error"], "status": "error"}), 400

        # 4. Execute the optimized plan
        start_time = datetime.datetime.now()
        result = execution_engine.execute(optimized_plan)
        execution_time = (datetime.datetime.now() - start_time).total_seconds()

        # Add execution time to result
        if isinstance(result, dict):
            result["execution_time_ms"] = round(execution_time * 1000, 2)

        logging.info(f"Query executed in {round(execution_time * 1000, 2)} ms")
        return jsonify(result)

    except (ValueError, SyntaxError, TypeError, AttributeError, KeyError, RuntimeError) as e:
        logging.error(f"Error executing query: {str(e)}")
        logging.error(traceback.format_exc())
        return jsonify({"error": str(e), "status": "error"}), 500

@app.route("/api/databases", methods=["GET"])
@require_auth
def get_databases():
    """Get a list of all databases."""
    try:
        databases = catalog_manager.list_databases()
        return jsonify({
            "columns": ["Database Name"],
            "rows": [[db] for db in databases],
            "status": "success"
        })
    except RuntimeError as e:
        logging.error(f"Error listing databases: {str(e)}")
        return jsonify({"error": str(e), "status": "error"}), 500

@app.route("/api/tables", methods=["GET"])
@require_auth
def get_tables():
    """Get a list of tables in the current database."""
    db_name = request.args.get("database")

    try:
        if not db_name:
            db_name = catalog_manager.get_current_database()
            if not db_name:
                return jsonify({"error": "No database selected", "status": "error"}), 400

        tables = catalog_manager.list_tables(db_name)
        return jsonify({
            "columns": ["Table Name"],
            "rows": [[table] for table in tables],
            "status": "success",
            "database": db_name
        })
    except RuntimeError as e:
        logging.error(f"Error listing tables: {str(e)}")
        return jsonify({"error": str(e), "status": "error"}), 500

@app.route("/api/indexes", methods=["GET"])
@require_auth
def get_indexes():
    """Get indexes for a table or all tables in current database."""
    table_name = request.args.get("table")

    try:
        db_name = catalog_manager.get_current_database()
        if not db_name:
            return jsonify({"error": "No database selected", "status": "error"}), 400

        if table_name:
            # Show indexes for specific table
            indexes = catalog_manager.get_indexes_for_table(table_name)
            if not indexes:
                return jsonify({
                    "columns": ["Table", "Column", "Index Name", "Type"],
                    "rows": [],
                    "status": "success",
                    "message": f"No indexes found for table '{table_name}'"
                })

            rows = []
            for idx_name, idx_info in indexes.items():
                column_name = idx_info.get("column", "")
                rows.append([
                    table_name,
                    column_name,
                    f"{idx_name}",
                    idx_info.get("type", "BTREE"),
                ])

            return jsonify({
                "columns": ["Table", "Column", "Index Name", "Type"],
                "rows": rows,
                "status": "success"
            })

        # Get all indexes in the current database
        all_indexes = []
        for index_id, index_info in catalog_manager.indexes.items():
            if index_id.startswith(f"{db_name}."):
                parts = index_id.split(".")
                if len(parts) >= 3:
                    table = parts[1]
                    column = index_info.get("column", "")
                    index_name = parts[2]
                    index_type = index_info.get("type", "BTREE")
                    all_indexes.append([
                        table,
                        column,
                        f"{index_name}",
                        index_type
                    ])

            return jsonify({
                "columns": ["Table", "Column", "Index Name", "Type"],
                "rows": all_indexes,
                "status": "success"
            })
    except RuntimeError as e:
        logging.error(f"Error listing indexes: {str(e)}")
        return jsonify({"error": str(e), "status": "error"}), 500

@app.route("/api/table/<table_name>", methods=["GET"])
@require_auth
def get_table_schema(table_name):
    """Get schema information for a table."""
    try:
        db_name = catalog_manager.get_current_database()
        if not db_name:
            return jsonify({"error": "No database selected", "status": "error"}), 400

        table_id = f"{db_name}.{table_name}"
        if table_id not in catalog_manager.tables:
            return jsonify({"error": f"Table '{table_name}' not found", "status": "error"}), 404

        table_info = catalog_manager.tables[table_id]
        columns = []

        for col_name, col_info in table_info.get("columns", {}).items():
            columns.append({
                "name": col_name,
                "type": col_info.get("type", "UNKNOWN"),
                "nullable": col_info.get("nullable", True),
                "primary_key": col_info.get("primary_key", False),
                "foreign_key": col_info.get("foreign_key", None)
            })

        return jsonify({
            "table_name": table_name,
            "columns": columns,
            "status": "success"
        })
    except RuntimeError as e:
        logging.error(f"Error getting table schema: {str(e)}")
        return jsonify({"error": str(e), "status": "error"}), 500

@app.route("/api/status", methods=["GET"])
@require_auth
def get_status():
    """Get server status information."""
    user = g.user
    username = user.get("username", "Unknown")
    role = user.get("role", "user")
    current_db = catalog_manager.get_current_database()

    return jsonify({
        "status": "success",
        "server_info": {
            "user": username,
            "role": role,
            "current_database": current_db,
            "session_active": True
        }
    })

@app.route("/api/use_database", methods=["POST"])
@require_auth
def use_database():
    """Set the current database for the session."""
    data = request.json
    if not data:
        return jsonify({"error": "Missing request body", "status": "error"}), 400

    db_name = data.get("database")
    if not db_name:
        return jsonify({"error": "Missing database parameter", "status": "error"}), 400

    try:
        catalog_manager.set_current_database(db_name)
        return jsonify({
            "message": f"Using database '{db_name}'",
            "status": "success"
        })
    except RuntimeError as e:
        logging.error(f"Error setting current database: {str(e)}")
        return jsonify({"error": str(e), "status": "error"}), 500

def is_query_allowed(role, query):
    """Check if a query is allowed for the user's role."""
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

def _log_query(username, query):
    """Log a query for audit purposes."""
    try:
        timestamp = datetime.datetime.now().isoformat()

        # Log to the audit log file
        logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../logs")
        os.makedirs(logs_dir, exist_ok=True)
        audit_log_file = os.path.join(logs_dir, "rest_api_audit.log")

        with open(audit_log_file, "a", encoding="utf-8") as f:
            f.write(f"{timestamp} | {username} | {query}\n")

        # Also log to the standard logger
        logging.info(f"AUDIT: User {username} executed: {query}")
    except RuntimeError as e:
        # Don't let logging errors affect query execution
        logging.error(f"Failed to log query: {str(e)}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Start the HMSSQL REST API server")
    parser.add_argument("--host", default="0.0.0.0", help="Host address to bind")
    parser.add_argument("--port", type=int, default=5000, help="Port to bind")
    parser.add_argument("--debug", action="store_true", help="Run in debug mode")

    args = parser.parse_args()

    print(f"Starting HMSSQL REST API server on {args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=args.debug)
