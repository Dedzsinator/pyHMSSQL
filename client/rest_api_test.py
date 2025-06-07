"""
REST Client for HMSSQL Database

This module provides a client interface to the HMSSQL REST API.
"""

import json
import sys
import requests


class HMSSQLRestClient:
    """Client for interacting with the HMSSQL REST API."""

    def __init__(self, host="localhost", port=5000):
        """
        Initialize the REST client.

        Args:
            host (str): The hostname or IP of the HMSSQL REST server
            port (int): The port number of the HMSSQL REST server
        """
        self.base_url = f"http://{host}:{port}/api"
        self.session_id = None
        self.headers = {}

    def login(self, username, password):
        """
        Log in to the HMSSQL server.

        Args:
            username (str): The username to authenticate with
            password (str): The password for the user

        Returns:
            dict: Response containing session information or error details
        """
        response = requests.post(
            f"{self.base_url}/login",
            json={"username": username, "password": password},
            timeout=30
        )

        data = response.json()
        if response.status_code == 200 and "session_id" in data:
            self.session_id = data["session_id"]
            self.headers = {"Authorization": f"Bearer {self.session_id}"}
            return data

        return data

    def register(self, username, password, role="user"):
        """
        Register a new user.

        Args:
            username (str): The username for the new user
            password (str): The password for the new user
            role (str): The role for the new user (default: "user")

        Returns:
            dict: Response containing registration result or error details
        """
        response = requests.post(
            f"{self.base_url}/register",
            json={"username": username, "password": password, "role": role},
            timeout=30
        )

        return response.json()

    def logout(self):
        """
        Log out from the HMSSQL server.

        Returns:
            dict: Response containing logout result or error details
        """
        if not self.session_id:
            return {"error": "Not logged in", "status": "error"}

        response = requests.post(f"{self.base_url}/logout", headers=self.headers, timeout=30)

        data = response.json()
        if response.status_code == 200:
            self.session_id = None
            self.headers = {}

        return data

    def execute_query(self, query):
        """
        Execute a SQL query.

        Args:
            query (str): The SQL query to execute

        Returns:
            dict: Response containing query results or error details
        """
        if not self.session_id:
            return {"error": "Not logged in", "status": "error"}

        response = requests.post(
            f"{self.base_url}/query",
            json={"query": query},
            headers=self.headers,
            timeout=30
        )

        return response.json()

    def get_databases(self):
        """
        Get a list of all databases.

        Returns:
            dict: Response containing database list or error details
        """
        if not self.session_id:
            return {"error": "Not logged in", "status": "error"}

        response = requests.get(f"{self.base_url}/databases", headers=self.headers, timeout=30)

        return response.json()

    def get_tables(self, database=None):
        """
        Get a list of tables in the specified database or current database.

        Args:
            database (str, optional): The database name to query. If None, uses the current database.

        Returns:
            dict: Response containing table list or error details
        """
        if not self.session_id:
            return {"error": "Not logged in", "status": "error"}

        url = f"{self.base_url}/tables"
        if database:
            url += f"?database={database}"

        response = requests.get(url, headers=self.headers, timeout=30)

        return response.json()

    def get_indexes(self, table=None):
        """
        Get indexes for a table or all tables in current database.

        Args:
            table (str, optional): The table name to query indexes for. If None, gets all indexes.

        Returns:
            dict: Response containing index information or error details
        """
        if not self.session_id:
            return {"error": "Not logged in", "status": "error"}

        url = f"{self.base_url}/indexes"
        if table:
            url += f"?table={table}"

        response = requests.get(url, headers=self.headers, timeout=30)

        return response.json()

    def get_table_schema(self, table_name):
        """
        Get schema information for a table.

        Args:
            table_name (str): The name of the table to get schema for

        Returns:
            dict: Response containing table schema or error details
        """
        if not self.session_id:
            return {"error": "Not logged in", "status": "error"}

        response = requests.get(
            f"{self.base_url}/table/{table_name}",
            headers=self.headers,
            timeout=30
        )

        return response.json()

    def get_status(self):
        """
        Get server status information.

        Returns:
            dict: Response containing status information or error details
        """
        if not self.session_id:
            return {"error": "Not logged in", "status": "error"}

        response = requests.get(f"{self.base_url}/status", headers=self.headers, timeout=30)

        return response.json()

    def use_database(self, database):
        """
        Set the current database.

        Args:
            database (str): The name of the database to use

        Returns:
            dict: Response containing result or error details
        """
        if not self.session_id:
            return {"error": "Not logged in", "status": "error"}

        response = requests.post(
            f"{self.base_url}/use_database",
            json={"database": database},
            headers=self.headers,
            timeout=30
        )

        return response.json()

# Example usage
if __name__ == "__main__":

    # Create client
    client = HMSSQLRestClient()

    # Attempt login
    print("Logging in...")
    login_result = client.login("admin", "admin")
    print(json.dumps(login_result, indent=2))

    if "error" in login_result:
        print("Login failed. Trying to register admin user...")
        register_result = client.register("admin", "admin", "admin")
        print(json.dumps(register_result, indent=2))

        if "error" not in register_result:
            login_result = client.login("admin", "admin")
            print(json.dumps(login_result, indent=2))

    if "error" in login_result:
        print("Could not authenticate. Exiting.")
        sys.exit(1)

    # Get status
    print("\nGetting status...")
    status = client.get_status()
    print(json.dumps(status, indent=2))

    # Create a database if none exists
    print("\nCreating test database...")
    create_db_result = client.execute_query("CREATE DATABASE IF NOT EXISTS test_db")
    print(json.dumps(create_db_result, indent=2))

    # Use the test database
    print("\nSwitching to test database...")
    use_db_result = client.use_database("test_db")
    print(json.dumps(use_db_result, indent=2))

    # Create a table
    print("\nCreating test table...")
    create_table_result = client.execute_query("""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100),
            age INT
        )
    """)
    print(json.dumps(create_table_result, indent=2))

    # Insert some data
    print("\nInserting test data...")
    insert_result = client.execute_query("""
        INSERT INTO users (id, name, email, age) VALUES
        (1, 'Alice', 'alice@example.com', 30),
        (2, 'Bob', 'bob@example.com', 25),
        (3, 'Charlie', 'charlie@example.com', 35)
    """)
    print(json.dumps(insert_result, indent=2))

    # Query the data
    print("\nQuerying data...")
    query_result = client.execute_query("SELECT * FROM users")
    print(json.dumps(query_result, indent=2))

    # Get all tables
    print("\nGetting tables in test_db...")
    tables_result = client.get_tables()
    print(json.dumps(tables_result, indent=2))

    # Get table schema
    print("\nGetting schema for users table...")
    schema_result = client.get_table_schema("users")
    print(json.dumps(schema_result, indent=2))

    # Create an index
    print("\nCreating an index...")
    index_result = client.execute_query("CREATE INDEX idx_user_email ON users (email)")
    print(json.dumps(index_result, indent=2))

    # Get indexes
    print("\nGetting indexes for users table...")
    indexes_result = client.get_indexes("users")
    print(json.dumps(indexes_result, indent=2))

    # Logout
    print("\nLogging out...")
    logout_result = client.logout()
    print(json.dumps(logout_result, indent=2))
