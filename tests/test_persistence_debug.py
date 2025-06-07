#!/usr/bin/env python3
"""
Debug script to investigate data persistence issue in pyHMSSQL.
This script will step through the INSERT and SELECT operations to identify
where the data persistence problem occurs.
"""
import sys
import os
import logging
import tempfile
import shutil

# Add the server directory to Python path
sys.path.insert(0, '/home/deginandor/Documents/Programming/pyHMSSQL/server')

from catalog_manager import CatalogManager
from bptree_wrapper import BPlusTreeFactory

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def debug_data_persistence():
    """Debug data persistence issue step by step."""
    
    print("=== DEBUGGING DATA PERSISTENCE ISSUE ===")
    
    # Create a temporary directory for testing
    temp_dir = tempfile.mkdtemp(prefix="pyhmssql_debug_")
    print(f"Using temporary directory: {temp_dir}")
    
    try:
        # Initialize catalog manager
        print("\n1. Initializing CatalogManager...")
        catalog_manager = CatalogManager(data_dir=temp_dir)
        
        # Create a test database
        print("\n2. Creating test database...")
        catalog_manager.create_database("debug_test_db")
        catalog_manager.set_current_database("debug_test_db")
        
        # Create a simple test table
        print("\n3. Creating test table...")
        table_columns = {
            "id": {"type": "INT", "primary_key": True, "identity": True, "identity_seed": 1, "identity_increment": 1},
            "name": {"type": "VARCHAR(100)", "primary_key": False, "identity": False, "not_null": True},
            "age": {"type": "INT", "primary_key": False, "identity": False, "not_null": False}
        }
        result = catalog_manager.create_table("test_table", table_columns)
        print(f"Table creation result: {result}")
        
        # Test 1: Direct B+ tree operations
        print("\n=== TEST 1: Direct B+ tree operations ===")
        
        # Create a new B+ tree directly
        print("Creating new B+ tree...")
        test_tree = BPlusTreeFactory.create(order=50, name="direct_test_tree")
        
        # Insert data directly into the tree
        print("Inserting data directly into tree...")
        test_tree.insert(1.0, {"id": 1, "name": "John", "age": 25})
        test_tree.insert(2.0, {"id": 2, "name": "Jane", "age": 30})
        
        # Query the tree directly
        print("Querying tree directly...")
        result1 = test_tree.search(1.0)
        result2 = test_tree.search(2.0)
        range_results = test_tree.range_query(float("-inf"), float("inf"))
        
        print(f"Direct search result 1: {result1}")
        print(f"Direct search result 2: {result2}")
        print(f"Direct range query results: {range_results}")
        
        # Save tree to file
        test_file = os.path.join(temp_dir, "direct_test.tbl")
        print(f"Saving tree to file: {test_file}")
        test_tree.save_to_file(test_file)
        
        # Load tree from file
        print("Loading tree from file...")
        loaded_tree = BPlusTreeFactory.load_from_file(test_file)
        
        if loaded_tree:
            print("Successfully loaded tree from file")
            
            # Query the loaded tree
            loaded_result1 = loaded_tree.search(1.0)
            loaded_result2 = loaded_tree.search(2.0)
            loaded_range_results = loaded_tree.range_query(float("-inf"), float("inf"))
            
            print(f"Loaded search result 1: {loaded_result1}")
            print(f"Loaded search result 2: {loaded_result2}")
            print(f"Loaded range query results: {loaded_range_results}")
        else:
            print("ERROR: Failed to load tree from file")
        
        # Test 2: CatalogManager insert operation
        print("\n=== TEST 2: CatalogManager insert operation ===")
        
        # Insert a record using CatalogManager
        print("Inserting record using CatalogManager...")
        test_record = {"name": "Alice", "age": 28}
        insert_result = catalog_manager.insert_record("test_table", test_record)
        print(f"Insert result: {insert_result}")
        
        # Test 3: Check table file directly
        print("\n=== TEST 3: Check table file directly ===")
        
        # Get table file path
        table_file = os.path.join(temp_dir, "tables", "debug_test_db", "test_table.tbl")
        print(f"Table file path: {table_file}")
        
        if os.path.exists(table_file):
            print("Table file exists")
            
            # Load the table file directly
            print("Loading table file directly...")
            table_tree = BPlusTreeFactory.load_from_file(table_file)
            
            if table_tree:
                print("Successfully loaded table tree")
                
                # Get all records from table tree
                all_records = table_tree.range_query(float("-inf"), float("inf"))
                print(f"All records in table tree: {all_records}")
                
                # Try different methods to get data
                if hasattr(table_tree, '_get_all_items'):
                    all_items = table_tree._get_all_items()
                    print(f"All items from _get_all_items: {all_items}")
                
            else:
                print("ERROR: Failed to load table tree from file")
        else:
            print("ERROR: Table file does not exist")
        
        # Test 4: CatalogManager query operation
        print("\n=== TEST 4: CatalogManager query operation ===")
        
        # Query using catalog manager with correct method name
        print("Querying records using CatalogManager...")
        try:
            query_results = catalog_manager.query_with_condition("test_table")
            print(f"Query results: {query_results}")
            
            # Also test with specific conditions
            print("\nTesting with conditions...")
            condition_records = catalog_manager.query_with_condition(
                "test_table", 
                conditions=[{"column": "name", "operator": "=", "value": "Alice"}]
            )
            print(f"Found {len(condition_records)} records matching condition")
            for record in condition_records:
                print(f"  Filtered Record: {record}")
                
        except Exception as e:
            print(f"Query via CatalogManager failed: {e}")
            import traceback
            print(f"Error details: {traceback.format_exc()}")
        
        # Test 5: Debug table contents method
        print("\n=== TEST 5: Debug table contents ===")
        debug_contents = catalog_manager.debug_table_contents("test_table")
        print(f"Debug table contents: {debug_contents}")
        
        # Test 6: Batch insert test
        print("\n=== TEST 6: Batch insert test ===")
        
        # Try batch insert
        batch_records = [
            {"name": "Bob", "age": 35},
            {"name": "Charlie", "age": 40}
        ]
        batch_result = catalog_manager.insert_records_batch("test_table", batch_records)
        print(f"Batch insert result: {batch_result}")
        
        # Query again after batch insert
        query_results_after_batch = catalog_manager.query_with_condition("test_table")
        print(f"Query results after batch insert: {query_results_after_batch}")
        
        # Test 7: File system analysis
        print("\n=== TEST 7: File system analysis ===")
        
        # List all files in the data directory
        print("Files in data directory:")
        for root, dirs, files in os.walk(temp_dir):
            level = root.replace(temp_dir, '').count(os.sep)
            indent = ' ' * 2 * level
            print(f"{indent}{os.path.basename(root)}/")
            subindent = ' ' * 2 * (level + 1)
            for file in files:
                file_path = os.path.join(root, file)
                file_size = os.path.getsize(file_path)
                print(f"{subindent}{file} ({file_size} bytes)")
                
                # If it's a .tbl file, try to examine it
                if file.endswith('.tbl'):
                    print(f"{subindent}  Examining {file}...")
                    try:
                        tree = BPlusTreeFactory.load_from_file(file_path)
                        if tree:
                            items = tree.range_query(float("-inf"), float("inf"))
                            print(f"{subindent}  Contains {len(items)} items: {items}")
                        else:
                            print(f"{subindent}  Failed to load tree from file")
                    except Exception as e:
                        print(f"{subindent}  Error loading tree: {e}")
        
    except Exception as e:
        print(f"ERROR during debugging: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Clean up temporary directory
        print(f"\nCleaning up temporary directory: {temp_dir}")
        try:
            shutil.rmtree(temp_dir)
        except Exception as e:
            print(f"Warning: Could not clean up temp directory: {e}")

if __name__ == "__main__":
    debug_data_persistence()
