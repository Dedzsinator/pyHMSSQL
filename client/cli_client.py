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

            # Send request
            send_data(sock, request)

            # Receive response
            response = receive_data(sock)

            # Close the socket
            sock.close()

            return response
        except ConnectionRefusedError:
            print(f"Error: Could not connect to server at {
                  self.host}:{self.port}")
            return None
        except RuntimeError as e:
            print(f"Error: {str(e)}")
            return None

    def display_result(self, result):
        """Display the result of a query in a formatted table"""
        # Remove type field if present to avoid confusion
        if isinstance(result, dict) and "type" in result:
            del result["type"]

        # First check if there's a simple message to display
        if isinstance(result, dict) and "message" in result:
            print(result["message"])
            return

        # Then check for rows and columns (table data)
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

    def handle_visualization_response(self, response):
        """Handle visualization response from server"""
        if response.get("status") == "success":
            if "visualization_path" in response:
                print(f"Visualization saved to: {
                      response['visualization_path']}")

                # On Windows, try to open the image
                if os.name == "nt" and response["visualization_path"].endswith(".png"):
                    import subprocess

                    try:
                        subprocess.Popen(
                            ["start", response["visualization_path"]], shell=True
                        )
                        print(
                            f"Opening visualization file: {
                                response['visualization_path']
                            }"
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
