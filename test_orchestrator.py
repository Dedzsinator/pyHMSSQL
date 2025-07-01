#!/usr/bin/env python3
"""
Simple test script for the orchestrator
"""
import sys
import os
sys.path.append('/home/deginandor/Documents/Programming/pyHMSSQL/orchestrator')

import json
import logging
import time

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Import and test the orchestrator
try:
    from orchestrator import DatabaseOrchestrator
    
    print("Creating orchestrator instance...")
    orch = DatabaseOrchestrator()
    
    print("Waiting 5 seconds for discovery...")
    time.sleep(5)
    
    print("Getting cluster status...")
    status = orch.get_cluster_status()
    
    print("Cluster Status:")
    print(json.dumps(status, indent=2))
    
    print("Test completed successfully!")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
