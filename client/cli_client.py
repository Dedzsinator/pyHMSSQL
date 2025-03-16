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
            """
            Send a command to the server and print the response.
            """
            try:
                send_data(self.sock, command)
                response = receive_data(self.sock)
                if response:
                    print(response['response'])
                else:
                    print("No response received from the server.")
            except Exception as e:
                print(f"Error communicating with server: {e}")
        
    def close(self):
        """
        Close the connection to the server.
        """
        self.sock.close()

def execute_commands_from_file(file_path, client):
    """
    Execute commands from a file line by line.
    """
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return
    
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if line:  # Skip empty lines
                print(f"Executing: {line}")
                if line.lower().startswith("files"):
                    print("Error: Nested file execution is not allowed.")
                    continue
                client.send_command({'action': 'query', 'query': line})

def main():
    client = DBMSClient()
    session_id = None
    
    while True:
        cmd = input("DBMS> ").strip()
        if not cmd:
            continue
        
        if cmd.lower().startswith("login"):
            # Handle the "login" command
            parts = cmd.split()
            if len(parts) != 3:
                print("Usage: login <username> <password>")
            else:
                username = parts[1]
                password = parts[2]
                response = client.send_command({'action': 'login', 'username': username, 'password': password})
                if isinstance(response, dict) and 'session_id' in response:
                    session_id = response['session_id']
                    print(f"Logged in as {username} with role {response['role']}")
                else:
                    print(response)
        elif cmd.lower().startswith("register"):
            # Handle the "register" command
            parts = cmd.split()
            if len(parts) < 3:
                print("Usage: register <username> <password> [role]")
            else:
                username = parts[1]
                password = parts[2]
                role = parts[3] if len(parts) > 3 else "user"
                response = client.send_command({'action': 'register', 'username': username, 'password': password, 'role': role})
                print(response)
        elif cmd.lower() == "logout":
            # Handle the "logout" command
            if session_id:
                response = client.send_command({'action': 'logout', 'session_id': session_id})
                print(response)
                session_id = None
            else:
                print("Not logged in.")
        elif cmd.lower() == "exit":
            client.close()
            break
        else:
            # Handle regular commands
            if not session_id:
                print("Please log in first.")
            else:
                response = client.send_command({'action': 'query', 'session_id': session_id, 'query': cmd})
                print(response)

if __name__ == "__main__":
    main()