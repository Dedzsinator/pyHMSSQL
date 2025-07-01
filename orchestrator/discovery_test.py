#!/usr/bin/env python3
"""
Simple test to debug discovery
"""
import sys
import logging
import time

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

try:
    print("Testing discovery directly...")
    import orchestrator
    
    # Create discovery instance
    from orchestrator import ClusterDiscovery, DatabaseOrchestrator
    
    # Create a simple orchestrator for discovery
    orch = DatabaseOrchestrator()
    
    print("Running manual discovery...")
    nodes = orch.discovery._discover_manual_nodes()
    print(f"Manual discovery found {len(nodes)} nodes:")
    for node in nodes:
        print(f"  - {node.node_id}: {node.host}:{node.port} ({node.health.value}, {node.role})")
    
    print("\nRunning UDP discovery...")
    udp_nodes = orch.discovery._discover_udp_nodes()
    print(f"UDP discovery found {len(udp_nodes)} nodes:")
    for node in udp_nodes:
        print(f"  - {node.node_id}: {node.host}:{node.port} ({node.health.value}, {node.role})")
    
    print("\nRunning full discovery...")
    all_nodes = orch.discovery.discover_nodes()
    print(f"Full discovery found {len(all_nodes)} nodes:")
    for node in all_nodes:
        print(f"  - {node.node_id}: {node.host}:{node.port} ({node.health.value}, {node.role})")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
