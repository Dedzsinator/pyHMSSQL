import sys
import os
import socket
import json
import getpass
import cmd
import textwrap

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.constants import SERVER_HOST, SERVER_PORT
from shared.utils import send_data, receive_data

class DBMSClient(cmd.Cmd):
    intro = textwrap.dedent("""
    ╔═══════════════════════════════════════════════════╗
    ║           Welcome to HMS-SQL Database CLI         ║
    ╠═══════════════════════════════════════════════════╣
    ║ Type 'help' for a list of commands                ║
    ║ Type 'login <username>' to log in                 ║
    ║ Type 'register <username>' to create an account   ║
    ║ Type 'exit' to quit                               ║
    ╚═══════════════════════════════════════════════════╝
    """)
    prompt = "hms-sql> "
    
    def __init__(self, host=SERVER_HOST, port=SERVER_PORT):
        super().__init__()
        self.host = host
        self.port = port
        self.session_id = None
        self.role = None
        self.username = None
    
    def do_login(self, username):
        """
        Log in to the database.
        Usage: login <username>
        """
        if not username:
            print("Please provide a username")
            return
        
        password = getpass.getpass("Password: ")
        
        # Prepare request
        request = {
            "action": "login",
            "username": username,
            "password": password
        }
        
        # Send request to server
        response = self.send_request(request)
        
        # Handle response
        if isinstance(response, dict) and "session_id" in response:
            self.session_id = response["session_id"]
            self.role = response["role"]
            self.username = username
            print(f"Login successful! Welcome, {username} (Role: {self.role})")
            self.prompt = f"hms-sql [{username}]> "
        else:
            print(f"Login failed: {response}")
    
    def do_register(self, arg):
        """
        Register a new user account.
        Usage: register <username> [admin|user]
        """
        args = arg.split()
        if not args:
            print("Please provide a username")
            return
            
        username = args[0]
        role = "admin" if len(args) > 1 and args[1].lower() == "admin" else "user"
        
        password = getpass.getpass("Password: ")
        confirm_pwd = getpass.getpass("Confirm Password: ")
        
        if password != confirm_pwd:
            print("Passwords do not match")
            return
        
        # Prepare request
        request = {
            "action": "register",
            "username": username,
            "password": password,
            "role": role
        }
        
        # Send request to server
        response = self.send_request(request)
        
        # Handle response
        print(response)
    
    def do_logout(self, arg):
        """
        Log out from the current session.
        Usage: logout
        """
        if not self.session_id:
            print("You are not logged in")
            return
        
        # Prepare request
        request = {
            "action": "logout",
            "session_id": self.session_id
        }
        
        # Send request to server
        response = self.send_request(request)
        
        # Handle response
        print(response)
        self.session_id = None
        self.role = None
        self.username = None
        self.prompt = "hms-sql> "
    
    def do_query(self, sql_query):
        """
        Execute an SQL query.
        Usage: query <SQL statement>
        Example: query SELECT * FROM users
        """
        if not sql_query:
            print("Please provide an SQL query")
            return
            
        if not self.session_id:
            print("You must log in first")
            return
        
        # Prepare request
        request = {
            "action": "query",
            "session_id": self.session_id,
            "query": sql_query
        }
        
        # Send request to server
        response = self.send_request(request)
        
        # Handle response
        if isinstance(response, dict):
            self.display_result(response)
        else:
            print(response)
    
    def do_exit(self, arg):
        """
        Exit the CLI client.
        Usage: exit
        """
        if self.session_id:
            print("Logging out before exit...")
            self.do_logout("")
            
        print("Goodbye!")
        return True
    
    def do_status(self, arg):
        """
        Display current connection status.
        Usage: status
        """
        if self.session_id:
            print(f"Logged in as: {self.username}")
            print(f"Role: {self.role}")
            print(f"Session ID: {self.session_id}")
        else:
            print("Not logged in")
        
        print(f"Server: {self.host}:{self.port}")
    
    def send_request(self, request):
        """Send a request to the server and return the response"""
        try:
            # Create a new socket for each request
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.host, self.port))
            
            # Send request
            send_data(sock, request)
            
            # Receive response
            response = receive_data(sock)
            
            # Close the socket
            sock.close()
            
            return response
        except ConnectionRefusedError:
            print(f"Error: Could not connect to server at {self.host}:{self.port}")
            return None
        except Exception as e:
            print(f"Error: {str(e)}")
            return None
    
    def display_result(self, result):
        """Display the result of a query in a formatted table"""
        if "rows" in result and "columns" in result:
            # Display results as a table
            columns = result["columns"]
            rows = result["rows"]
            
            # Calculate column widths
            col_widths = [len(col) for col in columns]
            for row in rows:
                for i, val in enumerate(row):
                    col_widths[i] = max(col_widths[i], len(str(val)))
            
            # Print header
            header = " | ".join(col.ljust(col_widths[i]) for i, col in enumerate(columns))
            separator = "-+-".join("-" * width for width in col_widths)
            print(header)
            print(separator)
            
            # Print rows
            for row in rows:
                row_str = " | ".join(str(val).ljust(col_widths[i]) for i, val in enumerate(row))
                print(row_str)
            
            print(f"\n{len(rows)} row(s) returned")
        else:
            # Just print the result as is
            print(json.dumps(result, indent=2))

def main():
    # Get server host and port from command line arguments if provided
    host = SERVER_HOST
    port = SERVER_PORT
    
    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        try:
            port = int(sys.argv[2])
        except ValueError:
            print(f"Invalid port number: {sys.argv[2]}")
            sys.exit(1)
    
    # Create and run the client
    client = DBMSClient(host, port)
    
    try:
        client.cmdloop()
    except KeyboardInterrupt:
        print("\nExiting...")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()