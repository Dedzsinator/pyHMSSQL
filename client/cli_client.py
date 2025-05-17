import sys
import os
import curses
import threading
import socket
import json
import time
import socket
import json
import getpass
import cmd
import textwrap

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.utils import send_data, receive_data
from shared.constants import SERVER_HOST, SERVER_PORT

# Add these constants at the top with the other imports
DISCOVERY_PORT = 9998
DISCOVERY_TIMEOUT = 3  # seconds

class ServerDiscoverer:
    """Discovers HMSSQL servers on the network"""

    def __init__(self):
        self.servers = {}
        self.discovery_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.discovery_socket.bind(('', DISCOVERY_PORT))
        self.running = False

    def start_discovery(self):
        """Start listening for server broadcasts"""
        self.running = True
        self.discovery_thread = threading.Thread(target=self._discover, daemon=True)
        self.discovery_thread.start()

    def stop_discovery(self):
        """Stop discovery process"""
        self.running = False
        self.discovery_thread.join(timeout=1.0)

    def _discover(self):
        """Listen for server broadcasts"""
        self.discovery_socket.settimeout(0.5)  # Use a short timeout for responsive stopping

        while self.running:
            try:
                data, addr = self.discovery_socket.recvfrom(4096)
                try:
                    server_info = json.loads(data.decode('utf-8'))
                    if server_info.get('service') == 'HMSSQL':
                        # Use host:port as a unique key
                        server_key = f"{server_info['host']}:{server_info['port']}"
                        server_info['last_seen'] = time.time()
                        self.servers[server_key] = server_info
                except json.JSONDecodeError:
                    pass
            except socket.timeout:
                # This is expected with the short timeout
                pass
            except RuntimeError as e:
                print(f"Discovery error: {str(e)}")

            # Clean up old servers (not seen in the last minute)
            current_time = time.time()
            for key in list(self.servers.keys()):
                if current_time - self.servers[key]['last_seen'] > 60:
                    del self.servers[key]

    def get_available_servers(self):
        """Return list of available servers"""
        return list(self.servers.values())

class ServerSelectionMenu:
    """Interactive menu for server selection"""

    def __init__(self, servers):
        self.servers = servers
        self.selected_index = 0

    def display(self):
        """Display the server selection menu using curses"""
        # Initialize curses
        stdscr = curses.initscr()
        curses.noecho()
        curses.cbreak()
        stdscr.keypad(True)

        try:
            curses.start_color()
            curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_BLUE)  # Selected item
            curses.init_pair(2, curses.COLOR_WHITE, curses.COLOR_BLACK)  # Normal items

            selected_server = None

            while True:
                stdscr.clear()
                height, width = stdscr.getmaxyx()

                # Display title
                title = "Available HMSSQL Servers"
                stdscr.addstr(1, (width - len(title)) // 2, title, curses.A_BOLD)

                # Display servers
                if not self.servers:
                    msg = "No servers found. Press 'r' to refresh or 'q' to quit."
                    stdscr.addstr(3, (width - len(msg)) // 2, msg)
                else:
                    for i, server in enumerate(self.servers):
                        y = 3 + i
                        if y >= height - 3:
                            break

                        server_text = f"{server['name']} ({server['host']}:{server['port']})"

                        if i == self.selected_index:
                            stdscr.attron(curses.color_pair(1))
                            stdscr.addstr(y, 2, server_text)
                            stdscr.attroff(curses.color_pair(1))
                        else:
                            stdscr.attron(curses.color_pair(2))
                            stdscr.addstr(y, 2, server_text)
                            stdscr.attroff(curses.color_pair(2))

                # Display instructions
                instructions = "↑/↓: Navigate | Enter: Select | r: Refresh | q: Quit"
                stdscr.addstr(height-2, (width - len(instructions)) // 2, instructions)

                # Refresh the screen
                stdscr.refresh()

                # Get user input
                key = stdscr.getch()

                if key == curses.KEY_UP and self.selected_index > 0:
                    self.selected_index -= 1
                elif key == curses.KEY_DOWN and self.selected_index < len(self.servers) - 1:
                    self.selected_index += 1
                elif key == ord('\n') and self.servers:  # Enter key
                    selected_server = self.servers[self.selected_index]
                    break
                elif key == ord('r'):  # Refresh
                    # Return None with refresh flag
                    return None, True
                elif key == ord('q'):  # Quit
                    break

            return selected_server, False

        finally:
            # Clean up curses
            curses.nocbreak()
            stdscr.keypad(False)
            curses.echo()
            curses.endwin()

class DBMSClient(cmd.Cmd):
    """
    Command Line Interface for HMS-SQL Database
    """

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

    def select_server(self):
        """Discover and select an available server"""
        print("Scanning for HMSSQL servers...")

        # Create the discoverer
        discoverer = ServerDiscoverer()
        discoverer.start_discovery()

        try:
            # Give some time for initial discovery
            time.sleep(DISCOVERY_TIMEOUT)

            refresh = True
            while refresh:
                # Get available servers
                available_servers = discoverer.get_available_servers()

                # Display the selection menu
                menu = ServerSelectionMenu(available_servers)
                selected_server, refresh = menu.display()

                if not refresh and selected_server:
                    self.host = selected_server['host']
                    self.port = selected_server['port']
                    print(f"Selected server: {selected_server['name']} ({self.host}:{self.port})")
                    return True

                if not refresh:
                    # User quit without selecting
                    return False

                # User wants to refresh - wait a moment
                print("Refreshing server list...")
                time.sleep(DISCOVERY_TIMEOUT)

        finally:
            # Stop discovery when done
            discoverer.stop_discovery()

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
        request = {"action": "login",
                   "username": username, "password": password}

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
        role = "admin" if len(
            args) > 1 and args[1].lower() == "admin" else "user"

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
            "role": role,
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
        request = {"action": "logout", "session_id": self.session_id}

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
            print("Error: SQL query cannot be empty.")
            return

        if not self.session_id:
            print("Error: You are not logged in. Please login first.")
            return

        # Prepare request
        request = {"action": "query",
                   "session_id": self.session_id, "query": sql_query}

        # Send request to server
        response = self.send_request(request)

        # Handle response
        if isinstance(response, dict):
            self.display_result(response)
        else:
            print(response)

    def do_visualize(self, arg):
        """
        Execute a visualization query.
        Usage: visualize <visualization command>
        Example: visualize BPTREE idx_customer_email ON customers
        """
        if not self.session_id:
            print("Error: You are not logged in. Please login first.")
            return

        # Full command
        command = "VISUALIZE " + arg

        # Prepare request
        request = {"action": "query",
                   "session_id": self.session_id, "query": command}

        # Send request to server
        response = self.send_request(request)

        # Handle response specifically for visualizations
        if isinstance(response, dict):
            self.handle_visualization_response(response)
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

            # Send request - ensure we're using the utilities correctly
            try:
                send_data(sock, request)
            except Exception as e:
                print(f"Error sending data: {str(e)}")
                return None

            # Receive response
            try:
                response = receive_data(sock)
            except Exception as e:
                print(f"Error receiving data: {str(e)}")
                return None

            # Close the socket
            sock.close()

            return response
        except ConnectionRefusedError:
            print(f"Error: Could not connect to server at {self.host}:{self.port}")
            return None
        except RuntimeError as e:
            print(f"Error: {str(e)}")
            return None

    def display_result(self, result):
        """Display the result of a query in a formatted table"""
        # If the result has status='error', it's an error message
        if isinstance(result, dict) and result.get("status") == "error" and "error" in result:
            print(f"Error: {result['error']}")
            return

        # Check if this is a SHOW ALL_TABLES result - prioritize showing the tree
        if isinstance(result, dict) and "rows" in result and "columns" in result:
            # Check if the query was SHOW ALL_TABLES
            if (len(result["columns"]) >= 2 and 
                "DATABASE_NAME" in result["columns"] and 
                "TABLE_NAME" in result["columns"]):
                self.display_tables_tree(result)
                return

        # Show message if present and no special handling was done above
        if isinstance(result, dict) and "message" in result:
            print(result["message"])
            return

        # Normal table display for other results
        if isinstance(result, dict) and "rows" in result and "columns" in result:
            columns = result["columns"]
            rows = result["rows"]

            # If no rows, show a simple message
            if not rows:
                print("Query executed successfully. No results to display.")
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
                    str(cell).ljust(col_widths[i]) if i < len(
                        col_widths) else str(cell)
                    for i, cell in enumerate(row)
                )
                print(row_str)

            print(f"\n{len(rows)} row(s) returned")
        # Handle error messages
        elif isinstance(result, dict) and "error" in result:
            print(f"Error: {result['error']}")
        # Handle any other data formats
        else:
            # Just print key-value pairs without status or type fields
            if isinstance(result, dict):
                for key, value in result.items():
                    if key not in ["status", "type"]:
                        print(f"{key}: {value}")
            else:
                print(result)
            
    def do_query(self, sql_query):
        """
        Execute an SQL query or run a script file.
        Usage: query <SQL statement>
            query SCRIPT <filepath>
        Example: query SELECT * FROM users
                query SCRIPT my_queries.sql
        """
        if not sql_query:
            print("Error: SQL query cannot be empty.")
            return

        if not self.session_id:
            print("Error: You are not logged in. Please login first.")
            return
            
        # Check if this is a SCRIPT command
        if sql_query.upper().startswith("SCRIPT "):
            # Extract the filepath
            filepath = sql_query[7:].strip()
            
            # Clean up filepath - remove quotes if present
            if filepath.startswith('"') and filepath.endswith('"'):
                filepath = filepath[1:-1]
            elif filepath.startswith("'") and filepath.endswith("'"):
                filepath = filepath[1:-1]
            
            # Expand relative paths
            if not os.path.isabs(filepath):
                filepath = os.path.abspath(os.path.join(os.getcwd(), filepath))
            
            try:
                if not os.path.exists(filepath):
                    print(f"Error: File not found: {filepath}")
                    return
                
                print(f"Executing script from {filepath}...")
                
                with open(filepath, 'r') as file:
                    lines = file.readlines()
                
                total_commands = 0
                successful_commands = 0
                failed_commands = 0
                
                for i, line in enumerate(lines, 1):
                    # Skip empty lines and comments
                    line = line.strip()
                    if not line or line.startswith('--') or line.startswith('#'):
                        continue
                    
                    total_commands += 1
                    print(f"\nExecuting command {total_commands}: {line}")
                    
                    # Execute the query
                    request = {
                        "action": "query",
                        "session_id": self.session_id, 
                        "query": line
                    }
                    
                    response = self.send_request(request)
                    
                    # Display result
                    if isinstance(response, dict):
                        if response.get("status") == "error":
                            print(f"Error: {response.get('error', 'Unknown error')}")
                            failed_commands += 1
                        else:
                            self.display_result(response)
                            successful_commands += 1
                    else:
                        print(response)
                        if "error" in str(response).lower():
                            failed_commands += 1
                        else:
                            successful_commands += 1
                
                print(f"\nScript execution complete.")
                print(f"Total commands executed: {total_commands}")
                print(f"Successful: {successful_commands}")
                print(f"Failed: {failed_commands}")
                
            except Exception as e:
                print(f"Error executing script: {str(e)}")
            
        else:
            # Regular query execution
            # Prepare request
            request = {"action": "query", 
                    "session_id": self.session_id, "query": sql_query}

            # Send request to server
            response = self.send_request(request)

            # Handle response
            if isinstance(response, dict):
                self.display_result(response)
            else:
                print(response)

    def do_REPLICATE(self, arg):
        """
        Setup replication with another node
        Usage: REPLICATE AS REPLICA OF primary_host primary_port
            REPLICATE STATUS
        """
        if not arg:
            print("Error: Missing arguments. Use 'REPLICATE AS REPLICA OF host port' or 'REPLICATE STATUS'")
            return
        
        args = arg.split()
        
        if arg.upper().startswith("STATUS"):
            # Get replication status
            response = self._send_command({"command": "get_status", "action": "node"})
            if response and response.get("status") == "success":
                role = response.get("role", "unknown")
                print(f"Current node role: {role}")
                
                if role == "primary":
                    replicas = response.get("replicas", {})
                    print(f"Replicas ({len(replicas)}):")
                    for replica_id, info in replicas.items():
                        status = info.get("status", "unknown")
                        host = info.get("host", "unknown")
                        port = info.get("port", "unknown")
                        lag = info.get("lag", 0)
                        print(f"  - {replica_id[:8]}: {host}:{port} ({status}, lag: {lag})")
                elif role == "replica":
                    primary_id = response.get("primary_id", "unknown")
                    lag = response.get("lag", 0)
                    print(f"Primary: {primary_id[:8]}")
                    print(f"Replication lag: {lag} operations")
            else:
                print(f"Error: {response.get('error', 'Unknown error')}")
        
        elif len(args) >= 5 and args[0].upper() == "AS" and args[1].upper() == "REPLICA" and args[2].upper() == "OF":
            # Register as replica
            primary_host = args[3]
            primary_port = args[4]
            
            try:
                primary_port = int(primary_port)
            except ValueError:
                print(f"Error: Port must be a number")
                return
            
            response = self._send_command({
                "command": "set_as_replica", 
                "primary_host": primary_host,
                "primary_port": primary_port,
                "action": "node"
            })
            
            if response and response.get("status") == "success":
                print(response.get("message", "Successfully registered as replica"))
            else:
                print(f"Error: {response.get('error', 'Unknown error')}")
        else:
            print("Error: Invalid syntax. Use 'REPLICATE AS REPLICA OF host port' or 'REPLICATE STATUS'")

    def do_PROMOTE(self, arg):
        """
        Promote this node to primary (use in case primary fails)
        Usage: PROMOTE
        """
        response = self._send_command({"command": "promote_to_primary", "action": "node"})
        
        if response and response.get("status") == "success":
            print(response.get("message", "Successfully promoted to primary"))
        else:
            print(f"Error: {response.get('error', 'Unknown error')}")

    def _send_command(self, command_data):
        """Send a command to the server."""
        # Add session ID if available
        if self.session_id:
            command_data["session_id"] = self.session_id
        
        try:
            data = json.dumps(command_data).encode()
            self.sock.sendall(len(data).to_bytes(4, byteorder="big"))
            self.sock.sendall(data)
            
            # Receive response
            response_size_bytes = self.sock.recv(4)
            response_size = int.from_bytes(response_size_bytes, byteorder="big")
            response_data = self.sock.recv(response_size)
            response = json.loads(response_data.decode())
            
            return response
        except ConnectionError:
            print("Error: Connection to server lost.")
            return None

    def display_tables_tree(self, result):
        """Display tables in a tree structure organized by database"""
        if not result.get("rows"):
            print("No tables found.")
            return
        
        # Get the indices of the database and table name columns
        db_idx = result["columns"].index("DATABASE_NAME") if "DATABASE_NAME" in result["columns"] else 0
        table_idx = result["columns"].index("TABLE_NAME") if "TABLE_NAME" in result["columns"] else 1
        
        # Organize tables by database
        databases = {}
        for row in result["rows"]:
            db_name = row[db_idx]
            table_name = row[table_idx]
            
            if db_name not in databases:
                databases[db_name] = []
            databases[db_name].append(table_name)
        
        # Print the tree
        print("Database Schema:")
        print("└── System")
        for db_name, tables in sorted(databases.items()):
            print(f"│   └── {db_name}")
            for i, table in enumerate(sorted(tables)):
                prefix = "│       └── " if i == len(tables) - 1 else "│       ├── "
                print(f"{prefix}{table}")
        
        print(f"\n{len(result['rows'])} table(s) in {len(databases)} database(s)")

    def handle_visualization_response(self, response):
        """Handle visualization response from server"""
        if response.get("status") == "success":
            if "visualization_path" in response:
                print(f"Visualization saved to: {response['visualization_path']}")

                # On Windows, try to open the image
                if os.name == "nt" and response["visualization_path"].endswith(".png"):
                    import subprocess

                    try:
                        subprocess.Popen(
                            ["start", response["visualization_path"]], shell=True
                        )
                        print(
                            f"Opening visualization file: {response['visualization_path']}"
                        )
                    except RuntimeError as e:
                        print(f"Error opening visualization: {str(e)}")

            if "text_representation" in response:
                print("\nB+ Tree Text Representation:")
                print(response["text_representation"])

            if "node_count" in response:
                print(f"\nB+ Tree Statistics:")
                print(f"Nodes: {response['node_count']}")
                print(f"Leaf nodes: {response.get('leaf_count', 'N/A')}")
                print(f"Height: {response.get('height', 'N/A')}")
        elif "error" in response:
            print(f"Error: {response['error']}")
        else:
            print(f"Unknown response format: {response}")

    # Add a method to handle index commands directly without the query prefix
    def do_CREATE(self, arg):
        """
        Handle CREATE INDEX commands directly
        Usage: CREATE INDEX idx_name ON table_name (column_name)
        """
        if arg.upper().startswith("INDEX"):
            self.do_query(f"CREATE {arg}")
        else:
            # For other CREATE commands, use query prefix
            self.do_query(f"CREATE {arg}")

    def do_USE(self, arg):
        """
        Handle USE database command
        Usage: USE database_name
        """
        self.do_query(f"USE {arg}")


def main():
    """_summary_
    """
    # Get server host and port from command line arguments if provided
    host = SERVER_HOST
    port = SERVER_PORT

    # Process command-line arguments
    if len(sys.argv) > 1:
        # If first argument is "discover", start server discovery mode
        if sys.argv[1].lower() == "discover":
            discovery_mode = True
        else:
            # Otherwise, treat as normal host argument
            host = sys.argv[1]
            discovery_mode = False

        if len(sys.argv) > 2 and not discovery_mode:
            try:
                port = int(sys.argv[2])
            except ValueError:
                print(f"Invalid port number: {sys.argv[2]}")
                sys.exit(1)
    else:
        # Default to discovery mode
        discovery_mode = True

    # Create client instance
    client = DBMSClient(host, port)

    try:
        # If in discovery mode, show server selection menu
        if discovery_mode:
            if not client.select_server():
                print("No server selected. Exiting...")
                return

        # Start the command loop
        client.cmdloop()
    except KeyboardInterrupt:
        print("\nExiting...")
    except RuntimeError as e:
        print(f"Error: {str(e)}")


if __name__ == "__main__":
    main()
