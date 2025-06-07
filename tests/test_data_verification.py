#!/usr/bin/env python3
"""
Simple test to verify data persistence in the comprehensive profiling context.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../server'))

from catalog_manager import CatalogManager
from ddl_processor.schema_manager import SchemaManager

def test_data_verification():
    """Test data insertion and retrieval in the profiling test database."""
    print("=== Data Verification Test ===")
    
    # Initialize components
    catalog_manager = CatalogManager()
    schema_manager = SchemaManager(catalog_manager)
    
    print(f"Initial current database: {catalog_manager.get_current_database()}")
    
    # Create test database
    try:
        catalog_manager.create_database("verification_test_db")
        catalog_manager.set_current_database("verification_test_db")
        print(f"Created and switched to database: {catalog_manager.get_current_database()}")
    except Exception as e:
        print(f"Database creation error: {e}")
        return
    
    # Create test table
    try:
        result = schema_manager.execute_create_table({
            "type": "CREATE_TABLE",
            "table": "test_products",
            "columns": [
                "product_id INT PRIMARY KEY",
                "product_name TEXT",
                "category TEXT",
                "price DECIMAL",
                "stock INT"
            ]
        })
        print(f"Create table result: {result}")
    except Exception as e:
        print(f"Table creation error: {e}")
        return
    
    print(f"Current database after table creation: {catalog_manager.get_current_database()}")
    
    # Insert test records
    print("Inserting test records...")
    records_inserted = 0
    try:
        for i in range(1, 11):  # Insert 10 records
            catalog_manager.insert_record("test_products", {
                "product_id": i,
                "product_name": f"Test Product {i}",
                "category": ["Electronics", "Books", "Furniture"][i % 3],
                "price": round(10.0 + (i * 5.5), 2),
                "stock": i * 10
            })
            records_inserted += 1
        print(f"Successfully inserted {records_inserted} records")
    except Exception as e:
        print(f"Insert error after {records_inserted} records: {e}")
        return
    
    print(f"Current database after insertions: {catalog_manager.get_current_database()}")
    
    # Query the records back
    try:
        results = catalog_manager.query_with_condition("test_products")
        print(f"Query returned {len(results) if results else 0} records")
        
        if results and len(results) > 0:
            print("Sample of retrieved records:")
            for i, record in enumerate(results[:3]):  # Show first 3 records
                print(f"  Record {i+1}: {record}")
        else:
            print("No records retrieved!")
    except Exception as e:
        print(f"Query error: {e}")
    
    print(f"Final current database: {catalog_manager.get_current_database()}")
    print("=== Test Complete ===")

if __name__ == "__main__":
    test_data_verification()
