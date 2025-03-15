import sys
import os
import socket
import json

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.constants import SERVER_HOST, SERVER_PORT
from shared.utils import send_data, receive_data

class DBMSClient:
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((SERVER_HOST, SERVER_PORT))
    
    def send_command(self, command):
        try:
            send_data(self.sock, command)
            response = receive_data(self.sock)
            if response:
                print(response['response'])
            else:
                print("No response received from the server.")
        except Exception as e:
            print(f"Error communicating with server: {e}")
        finally:
            self.sock.close()

if __name__ == "__main__":
    while True:
        cmd = input("DBMS> ").strip().split()
        if not cmd:
            continue
        
        client = DBMSClient()
        
        if cmd[0].lower() == 'create' and cmd[1].lower() == 'database':
            client.send_command({'action': 'create_database', 'db_name': cmd[2]})
        elif cmd[0].lower() == 'drop' and cmd[1].lower() == 'database':
            client.send_command({'action': 'drop_database', 'db_name': cmd[2]})
        elif cmd[0].lower() == 'create' and cmd[1].lower() == 'table':
            db_name = cmd[2]
            table_name = cmd[3]
            columns = {}
            for col_def in cmd[4:]:
                col_name, col_type = col_def.split(':')
                columns[col_name] = {'type': col_type}
            client.send_command({'action': 'create_table', 'db_name': db_name, 'table_name': table_name, 'columns': columns})
        elif cmd[0].lower() == 'drop' and cmd[1].lower() == 'table':
            client.send_command({'action': 'drop_table', 'db_name': cmd[2], 'table_name': cmd[3]})
        elif cmd[0].lower() == 'create' and cmd[1].lower() == 'index':
            db_name = cmd[2]
            table_name = cmd[3]
            index_name = cmd[4]
            column = cmd[5]
            client.send_command({'action': 'create_index', 'db_name': db_name, 'table_name': table_name, 'index_name': index_name, 'column': column})
        elif cmd[0].lower() == 'query':
            query = ' '.join(cmd[1:])
            client.send_command({'action': 'query', 'query': query})
        elif cmd[0].lower() == 'exit':
            break
        else:
            print("Invalid command.")