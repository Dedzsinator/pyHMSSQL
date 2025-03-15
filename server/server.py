import sys
import os
import socket
import json
import traceback

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.constants import SERVER_HOST, SERVER_PORT
from shared.utils import send_data, receive_data
from catalog_manager import CatalogManager
from index_manager import IndexManager
from planner import Planner
from execution_engine import ExecutionEngine

class DBMSServer:
    def __init__(self):
        self.catalog_manager = CatalogManager()
        self.index_manager = IndexManager('indexes')
        self.planner = Planner(self.catalog_manager, self.index_manager)
        self.execution_engine = ExecutionEngine(self.catalog_manager, self.index_manager)
    
    def handle_request(self, data):
        try:
            action = data.get('action')
            if action == 'create_database':
                return self.catalog_manager.create_database(data['db_name'])
            elif action == 'drop_database':
                return self.catalog_manager.drop_database(data['db_name'])
            elif action == 'create_table':
                return self.catalog_manager.create_table(data['db_name'], data['table_name'], data['columns'])
            elif action == 'drop_table':
                return self.catalog_manager.drop_table(data['db_name'], data['table_name'])
            elif action == 'create_index':
                return self.catalog_manager.create_index(data['db_name'], data['table_name'], data['index_name'], data['column'])
            elif action == 'query':
                plan = self.planner.plan_query(data['query'])
                return self.execution_engine.execute(plan)
            else:
                return "Invalid action."
        except Exception as e:
            print(f"Error handling request: {e}")
            traceback.print_exc()
            return f"Internal server error: {e}"

def start_server():
    server = DBMSServer()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((SERVER_HOST, SERVER_PORT))
    sock.listen(5)
    print(f"Server listening on {SERVER_HOST}:{SERVER_PORT}...")
    
    while True:
        client_sock, addr = sock.accept()
        print(f"Connection from {addr}")
        try:
            data = receive_data(client_sock)
            if data is None:
                print("Received invalid or no data from client.")
                client_sock.close()
                continue
            
            response = server.handle_request(data)
            send_data(client_sock, {'response': response})
        except Exception as e:
            print(f"Error processing client request: {e}")
            traceback.print_exc()
        finally:
            client_sock.close()

if __name__ == "__main__":
    start_server()