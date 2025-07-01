#!/usr/bin/env python3
"""
Debug script to test orchestrator functionality
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
    print("Importing orchestrator...")
    import orchestrator
    print("✅ Import successful")
    
    print("Creating orchestrator instance...")
    orch = orchestrator.DatabaseOrchestrator()
    print("✅ Orchestrator created")
    
    print("Waiting for discovery to run...")
    time.sleep(5)
    
    print("Getting cluster status...")
    status = orch.get_cluster_status()
    print(f"✅ Cluster status: {status}")
    
    print("Testing discovery directly...")
    nodes = orch.discovery.discover_nodes()
    print(f"✅ Direct discovery found {len(nodes)} nodes:")
    for node in nodes:
        print(f"  - {node.node_id}: {node.host}:{node.port} ({node.health.value})")
    
    print("Testing stats collector...")
    try:
        overview = orch.stats_collector.get_cluster_overview()
        print(f"✅ Stats overview: {overview}")
    except Exception as e:
        print(f"❌ Stats collector error: {e}")
        import traceback
        traceback.print_exc()
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
