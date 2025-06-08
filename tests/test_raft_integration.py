#!/usr/bin/env python3
"""Integration tests for RAFT consensus with actual database operations.

This test suite validates the RAFT consensus implementation by:
1. Setting up a multi-node cluster
2. Testing leader election and failover
3. Verifying data consistency across nodes
4. Testing network partitions and recovery
5. Performance testing under load
"""

import asyncio
import json
import logging
import os
import random
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from server.raft_consensus import RaftCluster, RaftNode, NodeState, LogEntry
from server.scaler import EnhancedReplicationManager, ReplicationMode, FailoverPolicy
from shared.utils import send_data, receive_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MockDatabase:
    """Mock database for testing RAFT operations"""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.data = {}
        self.operation_log = []
        self.lock = threading.RLock()
    
    def apply_operation(self, operation: Dict):
        """Apply operation to the database"""
        with self.lock:
            op_type = operation.get("type")
            self.operation_log.append(operation)
            
            if op_type == "INSERT":
                table = operation.get("table", "default")
                if table not in self.data:
                    self.data[table] = []
                self.data[table].append(operation.get("record"))
            
            elif op_type == "UPDATE":
                table = operation.get("table", "default")
                if table in self.data:
                    condition = operation.get("condition", {})
                    updates = operation.get("updates", {})
                    for record in self.data[table]:
                        if self._matches_condition(record, condition):
                            record.update(updates)
            
            elif op_type == "DELETE":
                table = operation.get("table", "default")
                if table in self.data:
                    condition = operation.get("condition", {})
                    self.data[table] = [r for r in self.data[table] 
                                      if not self._matches_condition(r, condition)]
    
    def _matches_condition(self, record: Dict, condition: Dict) -> bool:
        """Check if record matches condition"""
        for key, value in condition.items():
            if record.get(key) != value:
                return False
        return True
    
    def get_state_hash(self) -> str:
        """Get hash of current database state"""
        import hashlib
        state_str = json.dumps(self.data, sort_keys=True)
        return hashlib.md5(state_str.encode()).hexdigest()


class RaftIntegrationTest(unittest.TestCase):
    """Integration tests for RAFT consensus"""
    
    def setUp(self):
        """Set up test cluster"""
        self.test_dir = Path(tempfile.mkdtemp())
        self.nodes = {}
        self.databases = {}
        self.replication_managers = {}
        
        # Cluster configuration
        self.cluster_config = {
            "cluster_id": "test-cluster",
            "nodes": {
                "node1": {"host": "127.0.0.1", "port": 15001},
                "node2": {"host": "127.0.0.1", "port": 15002},
                "node3": {"host": "127.0.0.1", "port": 15003}
            },
            "election_timeout_min": 150,
            "election_timeout_max": 300,
            "heartbeat_interval": 50
        }
        
        # Create test nodes
        for node_id, config in self.cluster_config["nodes"].items():
            self._create_test_node(node_id, config)
        
        # Wait for initial setup
        time.sleep(2)
    
    def tearDown(self):
        """Clean up test resources"""
        # Stop all nodes
        for manager in self.replication_managers.values():
            manager.running = False
        
        # Clean up temporary directory
        import shutil
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def _create_test_node(self, node_id: str, config: Dict):
        """Create a test node with RAFT consensus"""
        # Create mock database
        database = MockDatabase(node_id)
        self.databases[node_id] = database
        
        # Create replication manager with RAFT
        manager = EnhancedReplicationManager(
            server_id=node_id,
            mode=ReplicationMode.RAFT,
            failover_policy=FailoverPolicy.AUTOMATIC,
            cluster_config=self.cluster_config
        )
        
        # Set up database callback
        if manager.raft_cluster:
            manager.raft_cluster.set_database_callback(database.apply_operation)
        
        self.replication_managers[node_id] = manager
        self.nodes[node_id] = manager.raft_node
    
    def test_leader_election(self):
        """Test leader election process"""
        logger.info("Testing leader election...")
        
        # Wait for leader election
        time.sleep(5)
        
        # Check that exactly one leader exists
        leaders = []
        for node_id, node in self.nodes.items():
            if node and node.state == NodeState.LEADER:
                leaders.append(node_id)
        
        self.assertEqual(len(leaders), 1, f"Expected 1 leader, found {len(leaders)}: {leaders}")
        logger.info(f"Leader elected: {leaders[0]}")
    
    def test_log_replication(self):
        """Test log replication across nodes"""
        logger.info("Testing log replication...")
        
        # Wait for leader election
        time.sleep(5)
        
        # Find leader
        leader_node = None
        leader_manager = None
        for node_id, node in self.nodes.items():
            if node and node.state == NodeState.LEADER:
                leader_node = node
                leader_manager = self.replication_managers[node_id]
                break
        
        self.assertIsNotNone(leader_node, "No leader found")
        
        # Submit operations through leader
        test_operations = [
            {
                "type": "INSERT",
                "table": "users",
                "record": {"id": 1, "name": "Alice", "email": "alice@test.com"}
            },
            {
                "type": "INSERT", 
                "table": "users",
                "record": {"id": 2, "name": "Bob", "email": "bob@test.com"}
            },
            {
                "type": "UPDATE",
                "table": "users",
                "condition": {"id": 1},
                "updates": {"name": "Alice Smith"}
            }
        ]
        
        for operation in test_operations:
            if leader_manager.raft_cluster:
                leader_manager.raft_cluster.submit_operation(operation)
        
        # Wait for replication
        time.sleep(3)
        
        # Verify all nodes have the same state
        state_hashes = []
        for node_id, database in self.databases.items():
            state_hash = database.get_state_hash()
            state_hashes.append(state_hash)
            logger.info(f"Node {node_id} state hash: {state_hash}")
        
        # All nodes should have identical state
        self.assertTrue(all(h == state_hashes[0] for h in state_hashes),
                       "Not all nodes have identical state")
        
        logger.info("Log replication successful")
    
    def test_leader_failover(self):
        """Test automatic failover when leader fails"""
        logger.info("Testing leader failover...")
        
        # Wait for initial leader election
        time.sleep(5)
        
        # Find current leader
        original_leader_id = None
        for node_id, node in self.nodes.items():
            if node and node.state == NodeState.LEADER:
                original_leader_id = node_id
                break
        
        self.assertIsNotNone(original_leader_id, "No leader found")
        logger.info(f"Original leader: {original_leader_id}")
        
        # Simulate leader failure
        original_manager = self.replication_managers[original_leader_id]
        original_manager.running = False
        if original_manager.raft_cluster:
            original_manager.raft_cluster.running = False
        
        logger.info(f"Simulated failure of leader {original_leader_id}")
        
        # Wait for new leader election
        time.sleep(10)
        
        # Check that a new leader is elected
        new_leaders = []
        for node_id, node in self.nodes.items():
            if node_id != original_leader_id and node and node.state == NodeState.LEADER:
                new_leaders.append(node_id)
        
        self.assertEqual(len(new_leaders), 1, f"Expected 1 new leader, found {len(new_leaders)}")
        logger.info(f"New leader elected: {new_leaders[0]}")
        
        # Test that new leader can accept operations
        new_leader_manager = self.replication_managers[new_leaders[0]]
        test_operation = {
            "type": "INSERT",
            "table": "test_failover",
            "record": {"id": 1, "message": "After failover"}
        }
        
        if new_leader_manager.raft_cluster:
            new_leader_manager.raft_cluster.submit_operation(test_operation)
        
        time.sleep(2)
        
        # Verify operation was applied to remaining nodes
        for node_id, database in self.databases.items():
            if node_id != original_leader_id:
                self.assertIn("test_failover", database.data)
                self.assertEqual(len(database.data["test_failover"]), 1)
        
        logger.info("Leader failover successful")
    
    def test_network_partition_recovery(self):
        """Test cluster behavior during network partitions"""
        logger.info("Testing network partition recovery...")
        
        # Wait for initial leader election
        time.Sleep(5)
        
        # Find current leader
        leader_id = None
        for node_id, node in self.nodes.items():
            if node and node.state == NodeState.LEADER:
                leader_id = node_id
                break
        
        self.assertIsNotNone(leader_id, "No leader found")
        
        # Simulate network partition by isolating one follower
        partitioned_nodes = [node_id for node_id in self.nodes.keys() if node_id != leader_id]
        isolated_node = partitioned_nodes[0]
        
        logger.info(f"Isolating node: {isolated_node}")
        
        # Disable communication for isolated node
        isolated_manager = self.replication_managers[isolated_node]
        if isolated_manager.raft_cluster:
            isolated_manager.raft_cluster.is_partitioned = True
        
        # Submit operations to majority partition
        leader_manager = self.replication_managers[leader_id]
        test_operation = {
            "type": "INSERT",
            "table": "partition_test",
            "record": {"id": 1, "status": "majority_partition"}
        }
        
        if leader_manager.raft_cluster:
            leader_manager.raft_cluster.submit_operation(test_operation)
        
        time.sleep(2)
        
        # Verify majority partition can still accept operations
        majority_nodes = [node_id for node_id in self.nodes.keys() if node_id != isolated_node]
        for node_id in majority_nodes:
            database = self.databases[node_id]
            self.assertIn("partition_test", database.data)
        
        # Isolated node should not have the operation
        isolated_db = self.databases[isolated_node]
        self.assertNotIn("partition_test", isolated_db.data)
        
        # Recover from partition
        logger.info("Recovering from network partition...")
        if isolated_manager.raft_cluster:
            isolated_manager.raft_cluster.is_partitioned = False
        
        time.sleep(5)
        
        # Verify isolated node catches up
        self.assertIn("partition_test", isolated_db.data)
        
        logger.info("Network partition recovery successful")
    
    def test_concurrent_operations(self):
        """Test concurrent operations under load"""
        logger.info("Testing concurrent operations...")
        
        # Wait for leader election
        time.sleep(5)
        
        # Find leader
        leader_manager = None
        for node_id, node in self.nodes.items():
            if node and node.state == NodeState.LEADER:
                leader_manager = self.replication_managers[node_id]
                break
        
        self.assertIsNotNone(leader_manager, "No leader found")
        
        # Submit operations concurrently
        num_operations = 50
        operations_submitted = []
        
        def submit_operation(op_id):
            operation = {
                "type": "INSERT",
                "table": "concurrent_test",
                "record": {"id": op_id, "timestamp": time.time()}
            }
            operations_submitted.append(operation)
            if leader_manager.raft_cluster:
                leader_manager.raft_cluster.submit_operation(operation)
        
        # Submit operations in parallel
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(submit_operation, i) for i in range(num_operations)]
            for future in futures:
                future.result()
        
        # Wait for all operations to be applied
        time.sleep(5)
        
        # Verify all operations were applied consistently
        for node_id, database in self.databases.items():
            if "concurrent_test" in database.data:
                applied_count = len(database.data["concurrent_test"])
                self.assertEqual(applied_count, num_operations,
                               f"Node {node_id} applied {applied_count}/{num_operations} operations")
        
        logger.info(f"Concurrent operations test successful: {num_operations} operations")
    
    def test_data_consistency(self):
        """Test data consistency across all nodes"""
        logger.info("Testing data consistency...")
        
        # Wait for leader election
        time.sleep(5)
        
        # Submit a series of operations
        operations = []
        for i in range(20):
            operations.append({
                "type": "INSERT",
                "table": "consistency_test",
                "record": {"id": i, "value": f"test_{i}"}
            })
        
        # Find leader and submit operations
        leader_manager = None
        for node_id, node in self.nodes.items():
            if node and node.state == NodeState.LEADER:
                leader_manager = self.replication_managers[node_id]
                break
        
        self.assertIsNotNone(leader_manager)
        
        for operation in operations:
            if leader_manager.raft_cluster:
                leader_manager.raft_cluster.submit_operation(operation)
            time.sleep(0.1)  # Small delay between operations
        
        # Wait for replication
        time.sleep(3)
        
        # Verify all nodes have identical data
        reference_data = None
        for node_id, database in self.databases.items():
            current_data = database.data.get("consistency_test", [])
            if reference_data is None:
                reference_data = current_data
            else:
                self.assertEqual(len(current_data), len(reference_data),
                               f"Node {node_id} has different number of records")
                
                # Sort by id for comparison
                current_sorted = sorted(current_data, key=lambda x: x['id'])
                reference_sorted = sorted(reference_data, key=lambda x: x['id'])
                
                self.assertEqual(current_sorted, reference_sorted,
                               f"Node {node_id} has different data")
        
        logger.info("Data consistency test successful")


class RaftPerformanceTest(unittest.TestCase):
    """Performance tests for RAFT consensus"""
    
    def setUp(self):
        """Set up performance test environment"""
        self.test_dir = Path(tempfile.mkdtemp())
        self.cluster_config = {
            "cluster_id": "perf-test-cluster",
            "nodes": {
                "node1": {"host": "127.0.0.1", "port": 16001},
                "node2": {"host": "127.0.0.1", "port": 16002},
                "node3": {"host": "127.0.0.1", "port": 16003}
            },
            "election_timeout_min": 150,
            "election_timeout_max": 300,
            "heartbeat_interval": 25  # Faster heartbeat for performance
        }
        
        self.databases = {}
        self.replication_managers = {}
        
        # Create nodes
        for node_id, config in self.cluster_config["nodes"].items():
            database = MockDatabase(node_id)
            self.databases[node_id] = database
            
            manager = EnhancedReplicationManager(
                server_id=node_id,
                mode=ReplicationMode.RAFT,
                failover_policy=FailoverPolicy.AUTOMATIC,
                cluster_config=self.cluster_config
            )
            
            if manager.raft_cluster:
                manager.raft_cluster.set_database_callback(database.apply_operation)
            
            self.replication_managers[node_id] = manager
        
        time.sleep(3)  # Wait for leader election
    
    def tearDown(self):
        """Clean up performance test resources"""
        for manager in self.replication_managers.values():
            manager.running = False
        
        import shutil
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_throughput(self):
        """Test operation throughput"""
        logger.info("Testing RAFT throughput...")
        
        # Find leader
        leader_manager = None
        for node_id, manager in self.replication_managers.items():
            if manager.raft_node and manager.raft_node.state == NodeState.LEADER:
                leader_manager = manager
                break
        
        self.assertIsNotNone(leader_manager)
        
        # Measure throughput
        num_operations = 1000
        start_time = time.time()
        
        for i in range(num_operations):
            operation = {
                "type": "INSERT",
                "table": "throughput_test",
                "record": {"id": i, "data": f"payload_{i}"}
            }
            if leader_manager.raft_cluster:
                leader_manager.raft_cluster.submit_operation(operation)
        
        # Wait for all operations to be applied
        time.sleep(10)
        
        end_time = time.time()
        duration = end_time - start_time
        throughput = num_operations / duration
        
        logger.info(f"Throughput: {throughput:.2f} operations/second")
        logger.info(f"Total time: {duration:.2f} seconds for {num_operations} operations")
        
        # Verify all operations were applied
        for node_id, database in self.databases.items():
            if "throughput_test" in database.data:
                applied_count = len(database.data["throughput_test"])
                logger.info(f"Node {node_id}: {applied_count} operations applied")
    
    def test_latency(self):
        """Test operation latency"""
        logger.info("Testing RAFT latency...")
        
        # Find leader
        leader_manager = None
        for node_id, manager in self.replication_managers.items():
            if manager.raft_node and manager.raft_node.state == NodeState.LEADER:
                leader_manager = manager
                break
        
        self.assertIsNotNone(leader_manager)
        
        # Measure latency for individual operations
        latencies = []
        num_tests = 100
        
        for i in range(num_tests):
            operation = {
                "type": "INSERT",
                "table": "latency_test",
                "record": {"id": i, "timestamp": time.time()}
            }
            
            start_time = time.time()
            if leader_manager.raft_cluster:
                leader_manager.raft_cluster.submit_operation(operation)
            
            # Wait for operation to be applied (simplified check)
            time.sleep(0.1)
            end_time = time.time()
            
            latency = (end_time - start_time) * 1000  # Convert to milliseconds
            latencies.append(latency)
        
        # Calculate statistics
        avg_latency = sum(latencies) / len(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        
        logger.info(f"Average latency: {avg_latency:.2f} ms")
        logger.info(f"Min latency: {min_latency:.2f} ms")
        logger.info(f"Max latency: {max_latency:.2f} ms")


def run_integration_tests():
    """Run all integration tests"""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('raft_integration_tests.log'),
            logging.StreamHandler()
        ]
    )
    
    # Create test suite
    suite = unittest.TestSuite()
    
    # Add integration tests
    suite.addTest(unittest.makeSuite(RaftIntegrationTest))
    
    # Add performance tests
    suite.addTest(unittest.makeSuite(RaftPerformanceTest))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_integration_tests()
    sys.exit(0 if success else 1)
