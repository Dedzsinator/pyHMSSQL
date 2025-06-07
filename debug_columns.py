#!/usr/bin/env python3
"""
Debug script to understand the column structure in LEFT JOIN results.
"""

import sys
import os
import tempfile

# Add server directory to path like the tests do
project_root = os.path.abspath(os.path.dirname(__file__))
server_dir = os.path.join(project_root, 'server')
if server_dir not in sys.path:
    sys.path.insert(0, server_dir)

from catalog_manager import CatalogManager
from schema_manager import SchemaManager  
from execution_engine import ExecutionEngine

def test_left_join_columns():
    """Test LEFT JOIN to understand column structure."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Initialize components
        catalog_manager = CatalogManager("test_db", temp_dir)
        schema_manager = SchemaManager(catalog_manager)
        execution_engine = ExecutionEngine(catalog_manager, schema_manager)
        
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
        
        # Create orders table
        schema_manager.execute_create_table({
            "type": "CREATE_TABLE",
            "table": "orders",
            "columns": [
                "id INT NOT NULL PRIMARY KEY",
                "customer_id INT NOT NULL",
                "amount DECIMAL NOT NULL"
            ]
        })
        
        # Insert test data - customers 1-3
        for i in range(1, 4):
            catalog_manager.insert_record(
                "customers",
                {"id": i, "name": f"Customer {i}", "email": f"customer{i}@example.com"}
            )
        
        # Insert customer 500 (no orders)
        catalog_manager.insert_record(
            "customers", 
            {"id": 500, "name": "No Orders Customer", "email": "noorders@example.com"}
        )
        
        # Insert some orders for customers 1-2 only (customer 3 and 500 have no orders)
        for i in range(1, 3):
            catalog_manager.insert_record(
                "orders",
                {"id": i, "customer_id": i, "amount": i * 10.5}
            )
        
        # Create index on customer_id
        schema_manager.execute_create_index({
            "type": "CREATE_INDEX",
            "index_name": "idx_customer_id",
            "table": "orders", 
            "column": "customer_id"
        })
        
        # Execute LEFT JOIN
        plan = {
            "type": "JOIN",
            "table1": "customers",
            "table2": "orders", 
            "condition": "customers.id = orders.customer_id",
            "join_type": "LEFT"
        }
        
        result = execution_engine.execute(plan)
        
        if result["status"] != "success":
            print(f"❌ JOIN failed: {result}")
            return
            
        rows = result.get("rows", [])
        columns = result.get("columns", [])
        
        print(f"✅ JOIN successful!")
        print(f"📊 Total rows: {len(rows)}")
        print(f"📊 Columns: {columns}")
        print(f"📊 Column count: {len(columns)}")
        
        # Print first few rows
        print("\n🔍 First few rows:")
        for i, row in enumerate(rows[:5]):
            print(f"  Row {i}: {row}")
            
        # Find customer 500 specifically
        customer_500_rows = []
        for i, row in enumerate(rows):
            # Check each column for customer ID 500
            for j, value in enumerate(row):
                if value == 500:
                    customer_500_rows.append((i, j, row))
                    break
                    
        print(f"\n🔍 Customer 500 records found: {len(customer_500_rows)}")
        for row_idx, col_idx, row in customer_500_rows:
            print(f"  Row {row_idx}, Column {col_idx} ({columns[col_idx] if col_idx < len(columns) else 'unknown'}): {row}")
            
        # Find customer 3 (also should have no orders)
        customer_3_rows = []
        for i, row in enumerate(rows):
            for j, value in enumerate(row):
                if value == 3:
                    customer_3_rows.append((i, j, row))
                    break
                    
        print(f"\n🔍 Customer 3 records found: {len(customer_3_rows)}")
        for row_idx, col_idx, row in customer_3_rows:
            print(f"  Row {row_idx}, Column {col_idx} ({columns[col_idx] if col_idx < len(columns) else 'unknown'}): {row}")

if __name__ == "__main__":
    test_left_join_columns()
