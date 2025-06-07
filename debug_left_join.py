#!/usr/bin/env python3

import sys
import os

# Get the absolute path to the server directory
project_root = os.path.abspath(os.path.dirname(__file__))
server_dir = os.path.join(project_root, 'server')

# Add server directory to path
if server_dir not in sys.path:
    sys.path.insert(0, server_dir)

# Add project root to path as well
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from database_manager import DatabaseManager
from schema_manager import SchemaManager
from catalog_manager import CatalogManager
from execution_engine import ExecutionEngine

import tempfile

# Create test setup
def main():
    # Create temporary directory
    temp_dir = tempfile.mkdtemp()
    
    # Create database manager with test config
    config = {
        'database_dir': os.path.join(temp_dir, 'database'),
        'storage_type': 'file',
        'log_level': 'INFO'
    }
    
    # Initialize components
    db_manager = DatabaseManager(config)
    schema_manager = SchemaManager(db_manager)
    catalog_manager = CatalogManager(db_manager)
    execution_engine = ExecutionEngine(schema_manager, catalog_manager, db_manager)
    
    # Set up tables
    print("🔧 Setting up tables...")
    
    # Create customers table
    schema_manager.execute_create_table({
        "type": "CREATE_TABLE",
        "table": "customers",
        "columns": [
            "id INT NOT NULL PRIMARY KEY",
            "name TEXT NOT NULL",
            "email TEXT"
        ]
    })

    # Create orders table with foreign key to customers
    schema_manager.execute_create_table({
        "type": "CREATE_TABLE",
        "table": "orders",
        "columns": [
            "id INT NOT NULL PRIMARY KEY",
            "customer_id INT NOT NULL",
            "amount DECIMAL NOT NULL",
            "FOREIGN KEY (customer_id) REFERENCES customers(id)"
        ]
    })

    # Insert sample data in customers
    for i in range(1, 101):
        catalog_manager.insert_record(
            "customers",
            {"id": i, "name": f"Customer {i}", "email": f"customer{i}@example.com"}
        )

    # Insert sample data in orders (multiple orders per customer)
    for i in range(1, 301):
        customer_id = ((i - 1) % 100) + 1  # Maps to customer IDs 1-100
        catalog_manager.insert_record(
            "orders",
            {"id": i, "customer_id": customer_id, "amount": i * 10.5}
        )

    # Create index
    schema_manager.execute_create_index({
        "type": "CREATE_INDEX",
        "index_name": "idx_customer_id",
        "table": "orders",
        "column": "customer_id"
    })

    # Add customer without orders
    catalog_manager.insert_record(
        "customers",
        {"id": 500, "name": "No Orders Customer", "email": "noorders@example.com"}
    )

    print("🔧 Running LEFT JOIN...")
    
    # Execute LEFT JOIN query
    plan = {
        "type": "JOIN",
        "table1": "customers",
        "table2": "orders",
        "condition": "customers.id = orders.customer_id",
        "join_type": "LEFT"
    }

    result = execution_engine.execute(plan)

    # Analyze results
    assert result["status"] == "success"
    rows = result.get("rows", [])
    columns = result.get("columns", [])
    
    print(f"🔍 DEBUG: Total rows: {len(rows)}")
    print(f"🔍 DEBUG: Columns: {columns}")
    print(f"🔍 DEBUG: First row: {rows[0] if rows else 'No rows'}")
    print(f"🔍 DEBUG: Last few rows: {rows[-3:] if len(rows) >= 3 else rows}")

    # Find customer 500 records
    customer_500_records = []
    for i, row in enumerate(rows):
        if row[0] == 500:  # Assuming first column is customer id
            customer_500_records.append((i, row))
            
    print(f"🔍 DEBUG: Found {len(customer_500_records)} records with customer_id=500")
    for idx, record in customer_500_records:
        print(f"🔍 DEBUG: Customer 500 record at index {idx}: {record}")

    # Clean up
    schema_manager.execute_drop_table({"type": "DROP_TABLE", "table": "orders"})
    schema_manager.execute_drop_table({"type": "DROP_TABLE", "table": "customers"})

if __name__ == "__main__":
    main()
