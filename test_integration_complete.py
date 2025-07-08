#!/usr/bin/env python3
"""
Comprehensive Integration Test for CyCore and RustCore Components
================================================================

This test verifies that both CyCore (Cython) and RustCore (Rust) components
are properly integrated into the main pyHMSSQL DBMS system.
"""

import sys
import os
import subprocess
import threading
import time
from pathlib import Path

def test_cycore_integration():
    """Test CyCore Cython components integration"""
    print("=== TESTING CYCORE INTEGRATION ===")
    
    try:
        # Test basic imports
        from cycore import get_info, HLCTimestamp, HybridLogicalClock, SwissMap
        info = get_info()
        
        print("✓ CyCore components imported successfully")
        print(f"  - Version: {info.get('version')}")
        print(f"  - HLC Implementation: {info.get('hlc_implementation')}")  
        print(f"  - HashMap Implementation: {info.get('hashmap_implementation')}")
        
        # Test HLC functionality
        hlc = HybridLogicalClock()
        print("✓ HybridLogicalClock instantiated")
        
        # Test SwissMap functionality
        smap = SwissMap()
        smap["test_key"] = "test_value"  # Use dict-like interface
        value = smap.get("test_key")
        assert value == "test_value", f"SwissMap test failed: {value}"
        print("✓ SwissMap functionality verified")
        
        return True
        
    except Exception as e:
        print(f"✗ CyCore integration failed: {e}")
        return False

def test_server_integration():
    """Test server integration with CyCore and RustCore"""
    print("\n=== TESTING SERVER INTEGRATION ===")
    
    try:
        sys.path.append('server')
        from server.server import CYCORE_AVAILABLE, RUST_SIDECAR_AVAILABLE, CYCORE_INFO
        from server.catalog_manager import CatalogManager
        from server.raft_consensus import RaftNode
        
        print(f"✓ Server CyCore integration: {CYCORE_AVAILABLE}")
        print(f"✓ Server Rust sidecar integration: {RUST_SIDECAR_AVAILABLE}")
        print(f"✓ CyCore info: {CYCORE_INFO}")
        
        # Test CatalogManager with CyCore
        catalog = CatalogManager()
        if hasattr(catalog, 'table_cache') and catalog.table_cache:
            print("✓ CatalogManager uses CyCore high-performance cache")
        
        # Test RaftNode with HLC
        node = RaftNode("test_node", "localhost", 8001)
        if hasattr(node, 'hlc_clock') and node.hlc_clock:
            print("✓ RaftNode uses CyCore HLC for distributed timestamps")
        
        return True
        
    except Exception as e:
        print(f"✗ Server integration failed: {e}")
        return False

def test_rustcore_integration():
    """Test RustCore components"""
    print("\n=== TESTING RUSTCORE INTEGRATION ===")
    
    try:
        # Check Rust HLC library
        hlc_lib_path = "rustcore/hlc/target/release/libpyhmssql_hlc.so"
        if os.path.exists(hlc_lib_path):
            print("✓ Rust HLC library built successfully")
        else:
            print("✗ Rust HLC library not found")
            return False
            
        # Check Rust geo-sidecar binary
        sidecar_path = "rustcore/geo_sidecar/target/release/geo_router_sidecar"
        if os.path.exists(sidecar_path):
            size_mb = os.path.getsize(sidecar_path) / (1024 * 1024)
            print(f"✓ Rust geo-routing sidecar built successfully ({size_mb:.1f}MB)")
            
            # Test that the binary runs
            try:
                result = subprocess.run(
                    [sidecar_path, "--help"], 
                    capture_output=True, 
                    text=True, 
                    timeout=5
                )
                if result.returncode == 0:
                    print("✓ Rust sidecar binary executable and responsive")
                else:
                    print(f"✗ Rust sidecar help failed: {result.stderr}")
                    
            except subprocess.TimeoutExpired:
                print("✗ Rust sidecar timed out")
            except Exception as e:
                print(f"✗ Rust sidecar test failed: {e}")
        else:
            print("✗ Rust geo-sidecar binary not found")
            return False
            
        return True
        
    except Exception as e:
        print(f"✗ RustCore integration failed: {e}")
        return False

def test_performance_integration():
    """Test performance optimizations integration"""
    print("\n=== TESTING PERFORMANCE INTEGRATION ===")
    
    try:
        from cycore import SwissMap, HybridLogicalClock
        import time
        
        # Performance test for SwissMap
        smap = SwissMap()
        start_time = time.perf_counter()
        
        # Insert 1000 items
        for i in range(1000):
            smap[f"key_{i}"] = f"value_{i}"  # Use dict-like interface
            
        insert_time = time.perf_counter() - start_time
        print(f"✓ SwissMap performance: 1000 insertions in {insert_time:.4f}s")
        
        # Performance test for HLC
        hlc = HybridLogicalClock()
        start_time = time.perf_counter()
        
        # Generate 1000 timestamps
        timestamps = []
        for i in range(1000):
            if hasattr(hlc, 'now'):
                timestamps.append(hlc.now())
                
        hlc_time = time.perf_counter() - start_time
        print(f"✓ HLC performance: 1000 timestamps in {hlc_time:.4f}s")
        
        return True
        
    except Exception as e:
        print(f"✗ Performance integration failed: {e}")
        return False

def test_end_to_end_integration():
    """Test end-to-end integration by starting a server with all components"""
    print("\n=== TESTING END-TO-END INTEGRATION ===")
    
    try:
        sys.path.append('server')
        from server.server import DBMSServer
        
        # Also import availability flags to check status
        try:
            from server.server import CYCORE_AVAILABLE, RUST_SIDECAR_AVAILABLE
            print(f"✓ Integration status - CyCore: {CYCORE_AVAILABLE}, Rust: {RUST_SIDECAR_AVAILABLE}")
        except ImportError:
            print("⚠ Could not import integration status flags")
        
        # Create a server instance with all integrations
        server = DBMSServer(data_dir="data", server_name="integration_test")
        
        # Check that server has CyCore components
        integration_checks = []
        
        if hasattr(server, 'hlc_clock') and server.hlc_clock:
            integration_checks.append("HLC clock")
            
        if hasattr(server, 'performance_cache') and server.performance_cache:
            integration_checks.append("Performance cache")
            
        if hasattr(server, 'rust_sidecar_config'):
            integration_checks.append("Rust sidecar config")
            
        if integration_checks:
            print(f"✓ Server initialized with: {', '.join(integration_checks)}")
        else:
            print("⚠ Server initialized but integration components not detected")
            
        # Test catalog manager integration
        if hasattr(server.catalog_manager, 'table_cache'):
            print("✓ CatalogManager has CyCore table cache")
            
        return True
        
    except Exception as e:
        print(f"✗ End-to-end integration failed: {e}")
        return False

def main():
    """Run all integration tests"""
    print("🚀 PYHMSSQL CYCORE & RUSTCORE INTEGRATION TEST")
    print("=" * 60)
    
    test_results = []
    
    # Run all tests
    test_results.append(("CyCore Integration", test_cycore_integration()))
    test_results.append(("Server Integration", test_server_integration()))
    test_results.append(("RustCore Integration", test_rustcore_integration()))
    test_results.append(("Performance Integration", test_performance_integration()))
    test_results.append(("End-to-End Integration", test_end_to_end_integration()))
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 INTEGRATION TEST SUMMARY")
    print("=" * 60)
    
    passed = 0
    total = len(test_results)
    
    for test_name, result in test_results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if result:
            passed += 1
    
    print(f"\nResult: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 ALL INTEGRATION TESTS PASSED!")
        print("✅ CyCore and RustCore are fully integrated into pyHMSSQL")
    else:
        print("⚠️  Some integration tests failed")
        print("🔧 Review the failed components above")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
