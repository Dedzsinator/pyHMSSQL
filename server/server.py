import sys
import os
import socket
import json
import traceback
import logging
import subprocess
import uuid

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from catalog_manager import CatalogManager
from index_manager import IndexManager
from planner import Planner
from execution_engine import ExecutionEngine
from optimizer import Optimizer
from shared.constants import SERVER_HOST, SERVER_PORT
from shared.utils import send_data, receive_data

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

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
        if action == 'login':
            return self.handle_login(data)
        elif action == 'register':
            return self.handle_register(data)
        elif action == 'logout':
            return self.handle_logout(data)
        elif action == 'query':
            return self.handle_query(data)
        else:
            return "Invalid action."

def parse_sql(query):
    """
    Call the Haskell parser to parse the SQL query.
    """
    try:
        result = subprocess.run(
            ["./sqlparser"],  # Path to the Haskell parser executable
            input=query,
            text=True,
            capture_output=True,
            check=True
        )
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error parsing SQL: {e.stderr}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from parser: {e}")
        return None

def handle_login(self, data):
        """
        Handle user login.
        """
        username = data.get('username')
        password = data.get('password')
        
        user = self.catalog_manager.authenticate_user(username, password)
        if user:
            # Create a new session
            session_id = str(uuid.uuid4())
            self.sessions[session_id] = user
            return {"session_id": session_id, "role": user["role"]}
        else:
            return "Invalid username or password."
    
def handle_register(self, data):
    """
    Handle user registration.
    """
    username = data.get('username')
    password = data.get('password')
    role = data.get('role', "user")
    
    return self.catalog_manager.register_user(username, password, role)

def handle_logout(self, data):
    """
    Handle user logout.
    """
    session_id = data.get('session_id')
    if session_id in self.sessions:
        del self.sessions[session_id]
        return "Logged out successfully."
    else:
        return "Invalid session ID."

def handle_query(self, data):
    """
    Handle query execution with role-based access control.
    """
    session_id = data.get('session_id')
    if session_id not in self.sessions:
        return "Unauthorized. Please log in."
    
    user = self.sessions[session_id]
    query = data.get('query')
    
    # Check role-based access control
    if self.is_query_allowed(user["role"], query):
        # Parse the SQL query using the Haskell parser
        parsed_query = parse_sql(query)
        if not parsed_query:
            return "Error parsing SQL query."
        
        # Plan and optimize the query
        plan = self.planner.plan_query(parsed_query)
        optimized_plan = self.optimizer.optimize(plan)
        
        # Execute the query
        return self.execution_engine.execute(optimized_plan)
    else:
        return "Access denied. You do not have permission to execute this query."

def is_query_allowed(self, role, query):
    """
    Check if the query is allowed for the given role.
    """
    # Example: Only admins can create or drop tables
    if role != "admin" and ("CREATE TABLE" in query.upper() or "DROP TABLE" in query.upper()):
        return False
    return True

def start_server():
    server = DBMSServer()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((SERVER_HOST, SERVER_PORT))
    sock.listen(5)
    print(f"Server listening on {SERVER_HOST}:{SERVER_PORT}...")
    
    while True:
        client_sock, addr = sock.accept()
        print(f"Connection from {addr}")
        data = receive_data(client_sock)
        response = server.handle_request(data)
        send_data(client_sock, {'response': response})
        client_sock.close()

if __name__ == "__main__":
    start_server()