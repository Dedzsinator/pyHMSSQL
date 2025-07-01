#!/usr/bin/env python3
"""
Comprehensive integration test for pyHMSSQL with optimized B+ tree.
Tests all components working together.
"""

import os
import sys
import time
import json
import subprocess
import socket
import threading
import logging
from pathlib import Path

# Add server directory to Python path for imports
server_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'server')
if server_dir not in sys.path:
    sys.path.insert(0, server_dir)

# Add root directory to Python path for server module imports  
root_dir = os.path.dirname(os.path.abspath(__file__))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IntegrationTest:
    def __init__(self):
        self.server_process = None
        self.server_port = 9999
        self.test_results = []
        
    def run_test(self, test_name, test_func):
        """Run a single test and record results"""
        logger.info(f"🧪 Running test: {test_name}")
        try:
            test_func()
            self.test_results.append((test_name, "PASS", None))
            logger.info(f"✅ {test_name} - PASSED")
            return True
        except Exception as e:
            self.test_results.append((test_name, "FAIL", str(e)))
            logger.error(f"❌ {test_name} - FAILED: {e}")
            return False
    
    def test_bptree_import(self):
        """Test if optimized B+ tree can be imported"""
        try:
            import bptree_optimized
            # Test class exists and can be instantiated
            tree = bptree_optimized.BPlusTreeOptimized()
            assert tree is not None
        except ImportError as e:
            raise Exception(f"Failed to import optimized B+ tree: {e}")
    
    def test_bptree_basic_operations(self):
        """Test basic B+ tree operations"""
        try:
            import bptree_optimized
            tree = bptree_optimized.BPlusTreeOptimized()
            
            # Test insertion
            tree.insert(10, "value10")
            tree.insert(5, "value5")
            tree.insert(15, "value15")
            
            # Test search
            result = tree.search(10)
            assert result == "value10", f"Expected 'value10', got {result}"
            
            # Test deletion
            tree.delete(5)
            result = tree.search(5)
            assert result is None, f"Expected None after deletion, got {result}"
            
        except Exception as e:
            raise Exception(f"B+ tree operations failed: {e}")
    
    def test_server_components_import(self):
        """Test if all server components can be imported"""
        components = [
            'catalog_manager',
            'unified_optimizer', 
            'table_stats'
        ]
        
        for component in components:
            try:
                __import__(component)
            except ImportError as e:
                raise Exception(f"Failed to import {component}: {e}")
    
    def test_document_store_functionality(self):
        """Test document store with catalog improvements"""
        try:
            # Import with proper module paths - fix relative import
            from multimodel.document_store.doc_adapter import DocumentStoreAdapter, DocumentQuery
            from catalog_manager import CatalogManager
            from transaction.transaction_manager import TransactionManager
            
            # Create test catalog and transaction manager
            catalog = CatalogManager()
            tx_manager = TransactionManager(catalog)
            doc_store = DocumentStoreAdapter(catalog, tx_manager)
            
            # Test collection operations
            collection_name = "test_collection"
            doc_store.create_collection(collection_name)
            
            # Test document insertion
            test_doc = {"name": "John", "age": 30, "city": "New York"}
            doc_id = doc_store.insert_document(collection_name, test_doc)
            assert doc_id is not None
            
            # Test document retrieval using find_documents
            query = DocumentQuery(collection_name, {"name": "John"})
            results = doc_store.find_documents(query)
            assert len(results) > 0
            assert results[0]["age"] == 30
            
        except Exception as e:
            raise Exception(f"Document store test failed: {e}")
    
    def test_type_system_constraints(self):
        """Test type system constraint validation"""
        try:
            # Import with proper module paths
            import sys
            sys.path.insert(0, 'server/multimodel/unified')
            from type_system import TypeRegistry, PrimitiveTypeDefinition, PrimitiveType
            
            # Create type registry
            registry = TypeRegistry()
            
            # Test primitive type registration
            int_type = registry.get_type("integer")
            assert int_type is not None
            assert int_type.name == "integer"
            
            # Test constraint validation would go here
            # For now, just test basic functionality
            
        except Exception as e:
            raise Exception(f"Type system test failed: {e}")
    
    def test_query_optimization(self):
        """Test unified query optimizer"""
        try:
            from unified_optimizer import UnifiedQueryOptimizer, OptimizationOptions, OptimizationLevel
            from catalog_manager import CatalogManager
            from index_manager import IndexManager
            
            # Create required dependencies
            catalog = CatalogManager()
            index_mgr = IndexManager(catalog)
            
            # Create optimizer with required arguments
            optimizer = UnifiedQueryOptimizer(catalog, index_mgr)
            assert optimizer is not None
            
            # Test basic optimization options
            options = OptimizationOptions.for_level(OptimizationLevel.STANDARD)
            assert options is not None
            
        except Exception as e:
            raise Exception(f"Query optimization test failed: {e}")
    
    def test_table_statistics(self):
        """Test table statistics collection"""
        try:
            from table_stats import TableStatistics
            from catalog_manager import CatalogManager
            
            # Create catalog and stats manager
            catalog = CatalogManager()
            stats = TableStatistics(catalog)
            assert stats is not None
            
        except Exception as e:
            raise Exception(f"Table statistics test failed: {e}")
    
    def start_server(self):
        """Start the pyHMSSQL server for integration testing"""
        try:
            logger.info("🚀 Starting pyHMSSQL server...")
            
            # Change to server directory
            server_dir = Path(__file__).parent / "server"
            
            # Start server
            self.server_process = subprocess.Popen(
                [sys.executable, "server.py"],
                cwd=server_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Wait for server to start
            max_attempts = 30
            for attempt in range(max_attempts):
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(1)
                    result = sock.connect_ex(('localhost', self.server_port))
                    sock.close()
                    
                    if result == 0:
                        logger.info("✅ Server started successfully")
                        time.sleep(2)  # Give server time to fully initialize
                        return True
                        
                except Exception:
                    pass
                
                time.sleep(1)
            
            raise Exception("Server failed to start within timeout")
            
        except Exception as e:
            if self.server_process:
                self.server_process.terminate()
            raise Exception(f"Failed to start server: {e}")
    
    def stop_server(self):
        """Stop the pyHMSSQL server"""
        if self.server_process:
            logger.info("🛑 Stopping server...")
            self.server_process.terminate()
            self.server_process.wait(timeout=10)
            self.server_process = None
    
    def test_server_connection(self):
        """Test connection to running server"""
        try:
            import socket
            import json
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect(('localhost', self.server_port))
            
            # Send a test request
            test_request = {
                "action": "ping",
                "type": "ping"
            }
            
            request_data = json.dumps(test_request).encode('utf-8')
            length = len(request_data)
            
            # Send length prefix + data
            sock.send(length.to_bytes(4, byteorder='big'))
            sock.send(request_data)
            
            # Receive response
            response_length = int.from_bytes(sock.recv(4), byteorder='big')
            response_data = sock.recv(response_length)
            response = json.loads(response_data.decode('utf-8'))
            
            sock.close()
            
            assert 'status' in response or 'pong' in response
            
        except Exception as e:
            raise Exception(f"Server connection test failed: {e}")
    
    def run_all_tests(self):
        """Run all integration tests"""
        logger.info("🧪 Starting pyHMSSQL Integration Tests")
        logger.info("=" * 50)
        
        # Unit tests (no server required)
        unit_tests = [
            ("B+ Tree Import", self.test_bptree_import),
            ("B+ Tree Operations", self.test_bptree_basic_operations),
            ("Server Components Import", self.test_server_components_import),
            ("Document Store Functionality", self.test_document_store_functionality),
            ("Type System Constraints", self.test_type_system_constraints),
            ("Query Optimization", self.test_query_optimization),
            ("Table Statistics", self.test_table_statistics),
        ]
        
        for test_name, test_func in unit_tests:
            self.run_test(test_name, test_func)
        
        # Integration tests (require server)
        try:
            self.start_server()
            
            integration_tests = [
                ("Server Connection", self.test_server_connection),
            ]
            
            for test_name, test_func in integration_tests:
                self.run_test(test_name, test_func)
                
        except Exception as e:
            logger.error(f"Failed to run integration tests: {e}")
        finally:
            self.stop_server()
        
        # Print results
        self.print_results()
    
    def print_results(self):
        """Print test results summary"""
        logger.info("=" * 50)
        logger.info("🧪 Test Results Summary")
        logger.info("=" * 50)
        
        passed = sum(1 for _, status, _ in self.test_results if status == "PASS")
        failed = sum(1 for _, status, _ in self.test_results if status == "FAIL")
        total = len(self.test_results)
        
        for test_name, status, error in self.test_results:
            status_emoji = "✅" if status == "PASS" else "❌"
            logger.info(f"{status_emoji} {test_name}: {status}")
            if error:
                logger.info(f"    Error: {error}")
        
        logger.info("=" * 50)
        logger.info(f"📊 Results: {passed}/{total} tests passed ({failed} failed)")
        
        if failed == 0:
            logger.info("🎉 All tests passed! System is working correctly.")
            return True
        else:
            logger.error(f"💥 {failed} tests failed. Please check the errors above.")
            return False

def main():
    """Main test runner"""
    # Add project root to Python path
    project_root = Path(__file__).parent
    sys.path.insert(0, str(project_root))
    
    # Change to project directory
    os.chdir(project_root)
    
    # Run tests
    test_runner = IntegrationTest()
    success = test_runner.run_all_tests()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
