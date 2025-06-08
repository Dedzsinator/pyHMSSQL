"""Enhanced replication manager with RAFT consensus for primary-replica architecture.

This module handles data replication across multiple database instances
for improved read scalability and fault tolerance, with RAFT consensus
for automatic failover similar to GitHub's Orchestrator.
"""
import logging
import threading
import time
import queue
import json
import socket
import uuid
import os
import sys
from enum import Enum
from typing import Dict, List, Optional, Any, Callable

# Import RAFT consensus implementation
from raft_consensus import RaftCluster, RaftNode, NodeState, LogEntry

class ReplicaRole(Enum):
    """Roles in the replication system"""
    PRIMARY = 1
    REPLICA = 2
    CANDIDATE = 3  # For leader election
    WITNESS = 4   # For monitoring and orchestration

class ReplicationMode(Enum):
    """Replication modes for different consistency guarantees"""
    SYNC = 1         # Fully synchronous - wait for all replicas
    SEMI_SYNC = 2    # Semi-synchronous - wait for some replicas
    ASYNC = 3        # Asynchronous - don't wait for replicas
    RAFT = 4         # RAFT consensus based replication

class FailoverPolicy(Enum):
    """Failover policies"""
    AUTOMATIC = 1    # Automatic failover using RAFT
    MANUAL = 2       # Manual failover only
    ORCHESTRATED = 3 # GitHub Orchestrator-style failover

class EnhancedReplicationManager:
    """Enhanced replication manager with RAFT consensus and orchestration capabilities"""

    def __init__(self, server_id=None, mode=ReplicationMode.RAFT, sync_replicas=1,
                 failover_policy=FailoverPolicy.AUTOMATIC, cluster_config=None):
        """Initialize the enhanced replication manager.

        Args:
            server_id: Unique ID for this server instance
            mode: Replication mode (SYNC, SEMI_SYNC, ASYNC, RAFT)
            sync_replicas: Number of replicas to wait for in SEMI_SYNC mode
            failover_policy: Failover policy for handling primary failures
            cluster_config: Configuration for RAFT cluster
        """
        self.server_id = server_id or str(uuid.uuid4())
        self.mode = mode
        self.sync_replicas = sync_replicas
        self.failover_policy = failover_policy
        
        # Traditional replication state
        self.role = ReplicaRole.REPLICA  # Start as replica, RAFT will determine leadership
        self.primary_id = None
        self.replicas = {}  # {replica_id: {host, port, last_seen, lag, health}}
        self.oplog = []  # Operation log for traditional replication
        self.oplog_position = 0  # Current position in oplog
        self.replication_queue = queue.Queue()  # Queue for async replication
        
        # RAFT consensus integration
        self.raft_cluster = None
        self.raft_node = None
        self.is_raft_enabled = mode == ReplicationMode.RAFT
        
        if self.is_raft_enabled and cluster_config:
            self.raft_cluster = RaftCluster(cluster_config)
            node_info = cluster_config.get("nodes", {}).get(self.server_id)
            if node_info:
                self.raft_node = self.raft_cluster.add_node(
                    self.server_id,
                    node_info["host"],
                    node_info["port"],
                    is_local=True
                )
                
                # Set up RAFT callbacks
                self.raft_cluster.set_database_callback(self._apply_raft_operation)
                self.raft_cluster.set_leadership_change_callback(self._on_leadership_change)
        
        # Threading and synchronization
        self.lock = threading.RLock()
        self.logger = logging.getLogger("enhanced_replication")
        self.running = True
        
        # Database integration
        self.catalog_manager = None
        self.execution_engine = None
        self.sql_parser = None
        
        # Health monitoring and metrics
        self.health_monitor = HealthMonitor(self)
        self.metrics = ReplicationMetrics()
        
        # Orchestration features
        self.orchestrator = DatabaseOrchestrator(self) if failover_policy == FailoverPolicy.ORCHESTRATED else None
        
        # Start background workers
        self._start_background_workers()

    def _start_background_workers(self):
        """Start background worker threads"""
        if not self.is_raft_enabled:
            # Traditional replication workers
            self.worker_thread = threading.Thread(target=self._replication_worker, daemon=True)
            self.worker_thread.start()
            
            self.heartbeat_thread = threading.Thread(target=self._heartbeat_worker, daemon=True)
            self.heartbeat_thread.start()
        
        # Health monitoring (always active)
        self.health_thread = threading.Thread(target=self.health_monitor.monitor_loop, daemon=True)
        self.health_thread.start()
        
        # Orchestrator (if enabled)
        if self.orchestrator:
            self.orchestrator_thread = threading.Thread(target=self.orchestrator.orchestration_loop, daemon=True)
            self.orchestrator_thread.start()

    def _apply_raft_operation(self, operation: Dict[str, Any]):
        """Apply RAFT operation to local database"""
        try:
            if not self.catalog_manager:
                self.logger.error("Cannot apply RAFT operation: catalog_manager not set")
                return
            
            op_type = operation.get("type")
            self.logger.info(f"Applying RAFT operation: {op_type}")
            
            if op_type == "INSERT":
                table_name = operation.get("table")
                record = operation.get("record")
                if table_name and record:
                    self.catalog_manager.insert_record(table_name, record)
                    self.metrics.increment("raft_operations_applied")
            
            elif op_type == "UPDATE":
                table_name = operation.get("table")
                condition = operation.get("condition")
                updates = operation.get("updates", {})
                if table_name and condition:
                    self.catalog_manager.update_records(table_name, condition, updates)
                    self.metrics.increment("raft_operations_applied")
            
            elif op_type == "DELETE":
                table_name = operation.get("table")
                condition = operation.get("condition")
                if table_name and condition:
                    self.catalog_manager.delete_records(table_name, condition)
                    self.metrics.increment("raft_operations_applied")
            
            elif op_type in ("CREATE_TABLE", "DROP_TABLE", "CREATE_INDEX", "DROP_INDEX"):
                query = operation.get("query")
                if query and self.sql_parser and self.execution_engine:
                    parsed = self.sql_parser.parse_sql(query)
                    self.execution_engine.execute(parsed)
                    self.metrics.increment("raft_operations_applied")
            
            self.metrics.increment("total_operations_applied")
            
        except Exception as e:
            self.logger.error(f"Error applying RAFT operation: {e}")
            self.metrics.increment("operation_errors")

    def _on_leadership_change(self, node_id: str, is_leader: bool):
        """Handle RAFT leadership changes"""
        if node_id == self.server_id:
            if is_leader:
                self.role = ReplicaRole.PRIMARY
                self.primary_id = self.server_id
                self.logger.info(f"Became primary through RAFT election")
                self.metrics.increment("leadership_gained")
            else:
                self.role = ReplicaRole.REPLICA
                self.logger.info(f"Lost leadership, becoming replica")
                self.metrics.increment("leadership_lost")
        else:
            if is_leader:
                self.primary_id = node_id
                self.role = ReplicaRole.REPLICA
                self.logger.info(f"New primary elected: {node_id}")

    def log_operation(self, operation: Dict[str, Any]) -> bool:
        """Log an operation for replication"""
        if self.is_raft_enabled:
            # Use RAFT consensus
            if self.raft_node and self.role == ReplicaRole.PRIMARY:
                success = self.raft_cluster.submit_operation(operation)
                if success:
                    self.metrics.increment("raft_operations_submitted")
                return success
            else:
                self.logger.warning("Cannot submit RAFT operation: not the leader")
                return False
        else:
            # Use traditional replication
            return self._log_operation_traditional(operation)

    def _log_operation_traditional(self, operation: Dict[str, Any]) -> bool:
        """Traditional operation logging (non-RAFT)"""
        with self.lock:
            if self.role != ReplicaRole.PRIMARY:
                self.logger.warning("Cannot log operation: not a primary")
                return False

            # Add metadata
            operation["timestamp"] = time.time()
            operation["server_id"] = self.server_id
            operation["oplog_id"] = len(self.oplog)

            # Add to oplog
            self.oplog.append(operation)

            # Replicate based on mode
            if self.mode == ReplicationMode.SYNC:
                return self._sync_replicate(operation)
            elif self.mode == ReplicationMode.SEMI_SYNC:
                return self._semi_sync_replicate(operation, self.sync_replicas)
            else:  # ASYNC
                self.replication_queue.put(operation)
                return True

    def promote_to_primary(self, force=False) -> bool:
        """Promote this node to primary (manual failover)"""
        if self.is_raft_enabled:
            # In RAFT mode, leadership is determined by election
            self.logger.warning("Cannot manually promote to primary in RAFT mode")
            return False
        
        if not force and self.role == ReplicaRole.PRIMARY:
            return True
        
        with self.lock:
            self.role = ReplicaRole.PRIMARY
            self.primary_id = self.server_id
            self.logger.info("Manually promoted to primary")
            self.metrics.increment("manual_promotions")
            return True

    def demote_to_replica(self, new_primary_id: str = None) -> bool:
        """Demote this node to replica"""
        if self.is_raft_enabled:
            self.logger.warning("Cannot manually demote in RAFT mode")
            return False
        
        with self.lock:
            if self.role == ReplicaRole.PRIMARY:
                self.role = ReplicaRole.REPLICA
                self.primary_id = new_primary_id
                self.logger.info(f"Demoted to replica, new primary: {new_primary_id}")
                self.metrics.increment("manual_demotions")
                return True
        return False

    def get_cluster_status(self) -> Dict[str, Any]:
        """Get comprehensive cluster status"""
        status = {
            "server_id": self.server_id,
            "role": self.role.name,
            "mode": self.mode.name,
            "failover_policy": self.failover_policy.name,
            "primary_id": self.primary_id,
            "is_raft_enabled": self.is_raft_enabled,
            "traditional_replicas": len(self.replicas),
            "health": self.health_monitor.get_health_status(),
            "metrics": self.metrics.get_all_metrics()
        }
        
        if self.is_raft_enabled and self.raft_cluster:
            status["raft_status"] = self.raft_cluster.get_cluster_status()
        
        if self.orchestrator:
            status["orchestrator"] = self.orchestrator.get_status()
        
        return status

    def add_replica(self, replica_id: str, host: str, port: int) -> bool:
        """Add a replica to the cluster"""
        if self.is_raft_enabled:
            # Add to RAFT cluster
            if self.raft_cluster:
                self.raft_cluster.add_node(replica_id, host, port)
                return True
            return False
        else:
            # Traditional replica addition
            with self.lock:
                if self.role != ReplicaRole.PRIMARY:
                    return False
                
                self.replicas[replica_id] = {
                    "host": host,
                    "port": port,
                    "last_seen": time.time(),
                    "lag": 0,
                    "status": "online",
                    "health_score": 100.0
                }
                
                self.logger.info(f"Added replica: {replica_id} at {host}:{port}")
                return True

    def remove_replica(self, replica_id: str) -> bool:
        """Remove a replica from the cluster"""
        with self.lock:
            if replica_id in self.replicas:
                del self.replicas[replica_id]
                self.logger.info(f"Removed replica: {replica_id}")
                return True
            return False

    def shutdown(self):
        """Shutdown the replication manager"""
        self.logger.info("Shutting down enhanced replication manager")
        self.running = False
        
        # Shutdown RAFT cluster
        if self.raft_cluster:
            self.raft_cluster.shutdown()
        
        # Shutdown orchestrator
        if self.orchestrator:
            self.orchestrator.shutdown()
        
        # Shutdown health monitor
        if self.health_monitor:
            self.health_monitor.shutdown()
        
        # Wait for threads
        try:
            if hasattr(self, 'worker_thread') and self.worker_thread.is_alive():
                self.worker_thread.join(timeout=5)
            if hasattr(self, 'heartbeat_thread') and self.heartbeat_thread.is_alive():
                self.heartbeat_thread.join(timeout=5)
            if hasattr(self, 'health_thread') and self.health_thread.is_alive():
                self.health_thread.join(timeout=5)
            if hasattr(self, 'orchestrator_thread') and self.orchestrator_thread.is_alive():
                self.orchestrator_thread.join(timeout=5)
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")

    # Keep existing traditional replication methods for backward compatibility
    def _sync_replicate(self, operation):
        # ... existing implementation
        pass

    def _semi_sync_replicate(self, operation, min_replicas):
        # ... existing implementation  
        pass

    def _replication_worker(self):
        # ... existing implementation
        pass

    def _heartbeat_worker(self):
        # ... existing implementation
        pass


class HealthMonitor:
    """Monitors health of database nodes and cluster"""
    
    def __init__(self, replication_manager):
        self.replication_manager = replication_manager
        self.logger = logging.getLogger("health_monitor")
        self.running = True
        self.health_metrics = {}
        
    def monitor_loop(self):
        """Main health monitoring loop"""
        while self.running:
            try:
                self._check_node_health()
                self._check_replication_lag()
                self._check_consensus_health()
                time.sleep(10)  # Check every 10 seconds
            except Exception as e:
                self.logger.error(f"Health monitor error: {e}")
                time.sleep(5)
    
    def _check_node_health(self):
        """Check health of individual nodes"""
        for replica_id, replica_info in self.replication_manager.replicas.items():
            try:
                # Check network connectivity
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(3)
                result = sock.connect_ex((replica_info["host"], replica_info["port"]))
                sock.close()
                
                if result == 0:
                    replica_info["status"] = "online"
                    replica_info["last_seen"] = time.time()
                else:
                    replica_info["status"] = "offline"
                    
            except Exception as e:
                self.logger.debug(f"Health check failed for {replica_id}: {e}")
                replica_info["status"] = "offline"
    
    def _check_replication_lag(self):
        """Monitor replication lag"""
        if self.replication_manager.role == ReplicaRole.PRIMARY:
            for replica_id, replica_info in self.replication_manager.replicas.items():
                lag = len(self.replication_manager.oplog) - replica_info.get("oplog_position", 0)
                replica_info["lag"] = lag
                
                if lag > 100:  # High lag threshold
                    self.logger.warning(f"High replication lag for {replica_id}: {lag}")
    
    def _check_consensus_health(self):
        """Check RAFT consensus health"""
        if self.replication_manager.is_raft_enabled and self.replication_manager.raft_node:
            status = self.replication_manager.raft_node.get_status()
            
            # Check for split brain or leadership issues
            if status["state"] == "candidate" and time.time() - status.get("election_start", 0) > 30:
                self.logger.error("Prolonged election detected - possible split brain")
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get overall health status"""
        online_replicas = sum(1 for r in self.replication_manager.replicas.values() if r["status"] == "online")
        total_replicas = len(self.replication_manager.replicas)
        
        return {
            "overall_health": "healthy" if online_replicas > total_replicas // 2 else "degraded",
            "online_replicas": online_replicas,
            "total_replicas": total_replicas,
            "primary_healthy": self.replication_manager.role == ReplicaRole.PRIMARY,
            "average_lag": self._calculate_average_lag()
        }
    
    def _calculate_average_lag(self) -> float:
        """Calculate average replication lag"""
        if not self.replication_manager.replicas:
            return 0.0
        
        total_lag = sum(r.get("lag", 0) for r in self.replication_manager.replicas.values())
        return total_lag / len(self.replication_manager.replicas)
    
    def shutdown(self):
        """Shutdown health monitor"""
        self.running = False


class ReplicationMetrics:
    """Tracks replication metrics and statistics"""
    
    def __init__(self):
        self.metrics = {
            "raft_operations_submitted": 0,
            "raft_operations_applied": 0,
            "traditional_operations_replicated": 0,
            "leadership_gained": 0,
            "leadership_lost": 0,
            "manual_promotions": 0,
            "manual_demotions": 0,
            "failovers_performed": 0,
            "total_operations_applied": 0,
            "operation_errors": 0,
            "network_errors": 0
        }
        self.lock = threading.Lock()
    
    def increment(self, metric: str, value: int = 1):
        """Increment a metric"""
        with self.lock:
            self.metrics[metric] = self.metrics.get(metric, 0) + value
    
    def set_metric(self, metric: str, value: Any):
        """Set a metric value"""
        with self.lock:
            self.metrics[metric] = value
    
    def get_metric(self, metric: str) -> Any:
        """Get a metric value"""
        with self.lock:
            return self.metrics.get(metric, 0)
    
    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all metrics"""
        with self.lock:
            return self.metrics.copy()


class DatabaseOrchestrator:
    """GitHub Orchestrator-style database cluster management"""
    
    def __init__(self, replication_manager):
        self.replication_manager = replication_manager
        self.logger = logging.getLogger("orchestrator")
        self.running = True
        self.discovery_interval = 30  # seconds
        self.topology = {}
        
    def orchestration_loop(self):
        """Main orchestration loop"""
        while self.running:
            try:
                self._discover_topology()
                self._analyze_cluster_health()
                self._perform_maintenance_tasks()
                time.sleep(self.discovery_interval)
            except Exception as e:
                self.logger.error(f"Orchestration error: {e}")
                time.sleep(10)
    
    def _discover_topology(self):
        """Discover cluster topology"""
        # Implementation for topology discovery
        pass
    
    def _analyze_cluster_health(self):
        """Analyze cluster health and perform actions"""
        # Implementation for health analysis
        pass
    
    def _perform_maintenance_tasks(self):
        """Perform routine maintenance tasks"""
        # Implementation for maintenance
        pass
    
    def trigger_failover(self, failed_primary_id: str, new_primary_id: str) -> bool:
        """Trigger a coordinated failover"""
        try:
            self.logger.info(f"Triggering failover from {failed_primary_id} to {new_primary_id}")
            
            # In RAFT mode, failover is automatic
            if self.replication_manager.is_raft_enabled:
                self.logger.info("RAFT will handle failover automatically")
                return True
            
            # Manual failover for traditional replication
            success = self.replication_manager.promote_to_primary()
            if success:
                self.replication_manager.metrics.increment("failovers_performed")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Failover failed: {e}")
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """Get orchestrator status"""
        return {
            "running": self.running,
            "topology_size": len(self.topology),
            "last_discovery": time.time(),
            "discovery_interval": self.discovery_interval
        }
    
    def shutdown(self):
        """Shutdown orchestrator"""
        self.running = False


# Keep the original ReplicationManager for backward compatibility
class ReplicationManager(EnhancedReplicationManager):
    """Legacy ReplicationManager for backward compatibility"""
    
    def __init__(self, server_id=None, mode=ReplicationMode.SEMI_SYNC, sync_replicas=1):
        # Use traditional replication mode by default for backward compatibility
        super().__init__(
            server_id=server_id,
            mode=mode,
            sync_replicas=sync_replicas,
            failover_policy=FailoverPolicy.MANUAL,
            cluster_config=None
        )

    def shutdown(self):
        """Stop the replication manager."""
        self.running = False

        try:
            if self.worker_thread and self.worker_thread.is_alive():
                self.worker_thread.join(timeout=5)

            if self.heartbeat_thread and self.heartbeat_thread.is_alive():
                self.heartbeat_thread.join(timeout=5)

            logging.info("Replication manager shutdown complete")
        except Exception as e:
            logging.error(f"Error during replication manager shutdown: {str(e)}")

    def register_as_replica(self, primary_host, primary_port):
        """Register this server as a replica to a primary.

        Args:
            primary_host: Hostname of the primary server
            primary_port: Port of the primary server

        Returns:
            bool: True if successful
        """
        try:
            # Connect to primary and register
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((primary_host, primary_port))

            # Send registration message
            message = {
                "type": "replica_register",
                "server_id": self.server_id,
                "host": socket.gethostname(),
                "port": 9999  # Assuming standard port
            }
            sock.sendall(json.dumps(message).encode('utf-8'))

            # Get response
            response = sock.recv(4096)
            response_data = json.loads(response.decode('utf-8'))
            sock.close()

            if response_data.get("status") == "success":
                with self.lock:
                    self.role = ReplicaRole.REPLICA
                    self.primary_id = response_data.get("primary_id")
                    self.oplog_position = response_data.get("oplog_position", 0)

                self.logger.info(f"Registered as replica to primary {self.primary_id}")
                return True
            else:
                self.logger.error(f"Failed to register as replica: {response_data.get('error')}")
                return False

        except RuntimeError as e:
            self.logger.error(f"Error registering as replica: {str(e)}")
            return False

    def register_replica(self, replica_id, host, port):
        """Register a new replica to this primary server.

        Args:
            replica_id: Unique ID of the replica
            host: Hostname of the replica
            port: Port of the replica

        Returns:
            dict: Registration response with oplog position
        """
        with self.lock:
            if self.role != ReplicaRole.PRIMARY:
                return {"status": "error", "error": "This server is not a primary"}

            self.replicas[replica_id] = {
                "host": host,
                "port": port,
                "last_seen": time.time(),
                "lag": 0,
                "status": "online"
            }

            self.logger.info(f"Registered new replica: {replica_id} at {host}:{port}")
            return {
                "status": "success",
                "primary_id": self.server_id,
                "oplog_position": len(self.oplog)
            }

    def log_operation(self, operation):
        """Log an operation to the oplog for replication.

        Args:
            operation: Dictionary with operation details

        Returns:
            bool: True if operation was successfully logged and replicated
        """
        with self.lock:
            if self.role != ReplicaRole.PRIMARY:
                self.logger.warning("Cannot log operation: not a primary")
                return False

            # Add timestamp and server_id to operation
            operation["timestamp"] = time.time()
            operation["server_id"] = self.server_id
            operation["oplog_id"] = len(self.oplog)

            # Add to oplog
            self.oplog.append(operation)

            # Replicate based on mode
            if self.mode == ReplicationMode.SYNC:
                return self._sync_replicate(operation)
            elif self.mode == ReplicationMode.SEMI_SYNC:
                return self._semi_sync_replicate(operation, self.sync_replicas)
            else:  # ASYNC
                self.replication_queue.put(operation)
                return True

    def _sync_replicate(self, operation):
        """Synchronously replicate an operation to all replicas.

        Args:
            operation: The operation to replicate

        Returns:
            bool: True if all replicas acknowledged
        """
        if not self.replicas:
            return True  # No replicas to sync with

        success_count = 0
        for replica_id, replica in self.replicas.items():
            if replica["status"] != "online":
                continue

            if self._send_operation_to_replica(replica_id, replica, operation):
                success_count += 1

        return success_count == len(self.replicas)

    def _semi_sync_replicate(self, operation, min_replicas):
        """Semi-synchronously replicate an operation.

        Args:
            operation: The operation to replicate
            min_replicas: Minimum number of replicas that must acknowledge

        Returns:
            bool: True if enough replicas acknowledged
        """
        if len(self.replicas) < min_replicas:
            self.logger.warning(f"Not enough replicas: have {len(self.replicas)}, need {min_replicas}")
            return len(self.replicas) == 0  # True if no replicas needed

        success_count = 0
        for replica_id, replica in self.replicas.items():
            if replica["status"] != "online":
                continue

            if self._send_operation_to_replica(replica_id, replica, operation):
                success_count += 1
                if success_count >= min_replicas:
                    break

        result = success_count >= min_replicas
        if not result:
            self.logger.warning(f"Semi-sync replication failed: only {success_count}/{min_replicas} acknowledged")
        return result

    def _send_operation_to_replica(self, replica_id, replica, operation):
        """Send an operation to a specific replica.

        Args:
            replica_id: ID of the replica
            replica: Replica information dictionary
            operation: The operation to send

        Returns:
            bool: True if acknowledged successfully
        """
        try:
            # Connect to replica
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)  # 5 second timeout
            sock.connect((replica["host"], replica["port"]))

            # Send operation
            message = {
                "type": "replicate_op",
                "operation": operation,
                "primary_id": self.server_id
            }
            sock.sendall(json.dumps(message).encode('utf-8'))

            # Get acknowledgement
            response = sock.recv(4096)
            response_data = json.loads(response.decode('utf-8'))
            sock.close()

            if response_data.get("status") == "success":
                # Update replica status
                with self.lock:
                    replica["last_seen"] = time.time()
                    replica["lag"] = len(self.oplog) - response_data.get("oplog_position", 0)
                return True
            else:
                self.logger.error(f"Replica {replica_id} error: {response_data.get('error')}")
                return False

        except RuntimeError as e:
            self.logger.error(f"Error sending to replica {replica_id}: {str(e)}")
            # Mark replica as potentially offline
            with self.lock:
                if time.time() - replica["last_seen"] > 30:  # 30 second threshold
                    replica["status"] = "offline"
            return False

    def apply_operation(self, operation):
        """Apply a replicated operation locally (for replicas).

        Args:
            operation: The operation to apply

        Returns:
            bool: True if operation was applied successfully
        """
        if self.role != ReplicaRole.REPLICA:
            self.logger.warning("Cannot apply operation: not a replica")
            return False

        # Check if operation is already applied or out of order
        if operation["oplog_id"] < self.oplog_position:
            # Already applied
            return True
        elif operation["oplog_id"] > self.oplog_position:
            # Out of order, store for later
            self.logger.warning(f"Received out-of-order operation: {operation['oplog_id']}, current position: {self.oplog_position}")
            return False

        # Apply the operation based on its type
        op_type = operation.get("type")

        # Apply the operation to the local database
        try:
            if not hasattr(self, 'catalog_manager'):
                self.logger.error("Cannot apply operation: catalog_manager not set")
                return False

            if op_type == "INSERT":
                table_name = operation.get("table")
                # Either use the record directly or extract from values/columns
                record = operation.get("record")
                if not record and operation.get("values") and operation.get("columns"):
                    # Convert values/columns to record format
                    columns = operation.get("columns", [])
                    values = operation.get("values", [[]])
                    if values and len(values) > 0:
                        record = dict(zip(columns, values[0]))

                if table_name and record:
                    self.catalog_manager.insert_record(table_name, record)
                else:
                    self.logger.warning("Incomplete INSERT operation data")

            elif op_type == "UPDATE":
                table_name = operation.get("table")
                record_id = operation.get("record_id")
                updates = operation.get("updates", {})
                condition = operation.get("condition")

                if table_name and (record_id or condition):
                    if record_id:
                        # Update by ID
                        self.catalog_manager.update_record_by_id(table_name, record_id, updates)
                    elif condition:
                        # Update by condition
                        self.catalog_manager.update_records(table_name, condition, updates)
                else:
                    self.logger.warning("Incomplete UPDATE operation data")

            elif op_type == "DELETE":
                table_name = operation.get("table")
                record_id = operation.get("record_id")
                condition = operation.get("condition")

                if table_name and (record_id or condition):
                    if record_id:
                        # Delete by ID
                        self.catalog_manager.delete_record_by_id(table_name, record_id)
                    elif condition:
                        # Delete by condition
                        self.catalog_manager.delete_records(table_name, condition)
                else:
                    self.logger.warning("Incomplete DELETE operation data")

            elif op_type in ("CREATE_TABLE", "DROP_TABLE", "CREATE_INDEX", "DROP_INDEX"):
                # For schema operations, we typically need to execute the original SQL
                # to ensure all side effects are properly applied
                query = operation.get("query")
                if query:
                    # Use SQL parser and execution engine to process the query directly
                    self.execution_engine.execute(self.sql_parser.parse_sql(query))
                else:
                    self.logger.warning(f"No query provided for {op_type} operation")

            # Advance oplog position
            self.oplog_position += 1
            return True

        except RuntimeError as e:
            self.logger.error(f"Error applying operation: {str(e)}")
            return False

    def _replication_worker(self):
        """Background worker thread for asynchronous replication."""
        while self.running:
            try:
                # Get an operation from the queue
                try:
                    operation = self.replication_queue.get(timeout=1)
                except queue.Empty:
                    continue

                # Try to replicate to all online replicas
                with self.lock:
                    for replica_id, replica in self.replicas.items():
                        if replica["status"] == "online":
                            self._send_operation_to_replica(replica_id, replica, operation)

                self.replication_queue.task_done()
            except RuntimeError as e:
                self.logger.error(f"Error in replication worker: {str(e)}")

    def _heartbeat_worker(self):
        """Background worker thread for replica heartbeats."""
        while self.running:
            try:
                if self.role == ReplicaRole.PRIMARY:
                    # Primary sends heartbeats to replicas
                    with self.lock:
                        for replica_id, replica in list(self.replicas.items()):
                            try:
                                # Send heartbeat
                                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                sock.settimeout(3)
                                sock.connect((replica["host"], replica["port"]))

                                message = {
                                    "type": "heartbeat",
                                    "primary_id": self.server_id,
                                    "timestamp": time.time()
                                }
                                sock.sendall(json.dumps(message).encode('utf-8'))

                                # Get response
                                response = sock.recv(4096)
                                response_data = json.loads(response.decode('utf-8'))
                                sock.close()

                                if response_data.get("status") == "success":
                                    replica["last_seen"] = time.time()
                                    replica["lag"] = len(self.oplog) - response_data.get("oplog_position", 0)
                                    replica["status"] = "online"

                            except RuntimeError as e:
                                self.logger.warning(f"Heartbeat to replica {replica_id} failed: {str(e)}")
                                if time.time() - replica["last_seen"] > 30:
                                    replica["status"] = "offline"

                elif self.role == ReplicaRole.REPLICA:
                    # Replica sends heartbeat to primary to report status
                    if self.primary_id:
                        # Implement primary heartbeat logic
                        pass

                # Wait before next heartbeat cycle
                time.sleep(5)

            except RuntimeError as e:
                self.logger.error(f"Error in heartbeat worker: {str(e)}")
                time.sleep(5)  # Wait before retrying

    def stop(self):
        """Stop the replication manager."""
        self.running = False
        self.worker_thread.join(timeout=5)
        self.heartbeat_thread.join(timeout=5)
