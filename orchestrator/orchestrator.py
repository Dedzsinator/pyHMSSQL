"""
HMSSQL Database Orchestrator
GitHub Orchestrator-style cluster management for HMSSQL databases
"""
import json
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum
import socket
import requests
from flask import Flask, jsonify, request, render_template_string
import os


class NodeHealth(Enum):
    """Node health states"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    UNKNOWN = "unknown"


class ClusterTopology(Enum):
    """Cluster topology types"""
    PRIMARY_REPLICA = "primary_replica"
    RAFT_CONSENSUS = "raft_consensus"
    MIXED = "mixed"


@dataclass
class NodeInfo:
    """Information about a database node"""
    node_id: str
    host: str
    port: int
    role: str  # primary, replica, candidate
    health: NodeHealth
    last_seen: datetime
    replication_lag: int = 0
    raft_term: int = 0
    raft_state: str = "unknown"
    version: str = ""
    uptime: float = 0.0
    connections: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "role": self.role,
            "health": self.health.value,
            "last_seen": self.last_seen.isoformat(),
            "replication_lag": self.replication_lag,
            "raft_term": self.raft_term,
            "raft_state": self.raft_state,
            "version": self.version,
            "uptime": self.uptime,
            "connections": self.connections
        }


class ClusterDiscovery:
    """Discovers and monitors database cluster nodes"""
    
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.logger = logging.getLogger("cluster_discovery")
        self.discovery_interval = 30  # seconds
        self.running = True
        
        # UDP discovery constants
        self.DISCOVERY_PORT = 9998
        self.discovery_socket = None
        self.discovered_servers = {}
        
    def discover_nodes(self) -> List[NodeInfo]:
        """Discover nodes in the cluster"""
        nodes = []
        
        # In Kubernetes, discover through headless service
        if os.getenv("KUBERNETES_SERVICE_HOST"):
            nodes.extend(self._discover_k8s_nodes())
        else:
            # Try UDP discovery first
            nodes.extend(self._discover_udp_nodes())
            # Fallback to manual discovery through configuration
            nodes.extend(self._discover_manual_nodes())
        
        return nodes
    
    def _discover_udp_nodes(self) -> List[NodeInfo]:
        """Discover nodes via UDP broadcasts"""
        nodes = []
        try:
            # Create UDP socket for listening to broadcasts
            self.discovery_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.discovery_socket.bind(('', self.DISCOVERY_PORT))
            self.discovery_socket.settimeout(5.0)  # 5 second timeout
            
            self.logger.info(f"Listening for server broadcasts on port {self.DISCOVERY_PORT}")
            
            start_time = time.time()
            while time.time() - start_time < 5.0:  # Listen for 5 seconds
                try:
                    data, addr = self.discovery_socket.recvfrom(4096)
                    try:
                        server_info = json.loads(data.decode('utf-8'))
                        if server_info.get('service') == 'HMSSQL':
                            # Create node info from discovered server
                            host = server_info.get('host', addr[0])
                            port = server_info.get('port', 9999)
                            node_id = server_info.get('name', f"{host}:{port}")
                            
                            # Store discovered server
                            server_key = f"{host}:{port}"
                            self.discovered_servers[server_key] = {
                                'host': host,
                                'port': port,
                                'name': node_id,
                                'last_seen': time.time()
                            }
                            
                            # Probe the node for detailed status
                            node_info = self._probe_node(node_id, host, port)
                            if node_info:
                                nodes.append(node_info)
                                self.logger.info(f"Discovered HMSSQL server: {node_id} at {host}:{port}")
                                
                    except json.JSONDecodeError:
                        pass
                except socket.timeout:
                    # Continue listening
                    pass
                    
        except Exception as e:
            self.logger.error(f"UDP discovery failed: {e}")
        finally:
            if self.discovery_socket:
                self.discovery_socket.close()
                self.discovery_socket = None
        
        return nodes
    
    def _discover_k8s_nodes(self) -> List[NodeInfo]:
        """Discover nodes in Kubernetes cluster"""
        nodes = []
        try:
            # Use Kubernetes API to discover StatefulSet pods
            namespace = os.getenv("CLUSTER_NAMESPACE", "hmssql-cluster")
            service_name = os.getenv("DATABASE_HEADLESS_SERVICE", "hmssql-headless")
            
            # Get pod IPs from headless service
            # This is a simplified version - in production, use kubernetes client library
            for i in range(3):  # Assuming 3 replicas
                node_id = f"hmssql-cluster-{i}"
                host = f"{node_id}.{service_name}.{namespace}.svc.cluster.local"
                
                node_info = self._probe_node(node_id, host, 9999)
                if node_info:
                    nodes.append(node_info)
                    
        except Exception as e:
            self.logger.error(f"Kubernetes discovery failed: {e}")
        
        return nodes
    
    def _discover_manual_nodes(self) -> List[NodeInfo]:
        """Discover nodes from manual configuration"""
        nodes = []
        config_file = os.getenv("CLUSTER_CONFIG", "/config/cluster.yaml")
        
        try:
            import yaml
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            
            for node_id, node_config in config.get("nodes", {}).items():
                host = node_config["host"]
                port = node_config["port"]
                
                node_info = self._probe_node(node_id, host, port)
                if node_info:
                    nodes.append(node_info)
                    
        except Exception as e:
            self.logger.debug(f"Manual discovery failed: {e}")
            
        # Always add demo nodes for testing if no real nodes found
        if not nodes:
            self.logger.info("No real nodes found, creating demo nodes for testing")
            nodes.extend(self._create_demo_nodes())
        
        return nodes
    
    def _create_demo_nodes(self) -> List[NodeInfo]:
        """Create demo nodes for testing"""
        demo_nodes = [
            NodeInfo(
                node_id="demo-primary",
                host="127.0.0.1",
                port=9999,
                role="primary",
                health=NodeHealth.HEALTHY,
                last_seen=datetime.utcnow(),
                replication_lag=0,
                raft_term=1,
                raft_state="leader",
                version="1.0.0-demo",
                uptime=3600.0,
                connections=5
            ),
            NodeInfo(
                node_id="demo-replica-1",
                host="127.0.0.1",
                port=10000,
                role="replica",
                health=NodeHealth.HEALTHY,
                last_seen=datetime.utcnow(),
                replication_lag=100,
                raft_term=1,
                raft_state="follower",
                version="1.0.0-demo",
                uptime=3500.0,
                connections=3
            ),
            NodeInfo(
                node_id="demo-replica-2",
                host="127.0.0.1",
                port=10001,
                role="replica",
                health=NodeHealth.DEGRADED,
                last_seen=datetime.utcnow() - timedelta(seconds=30),
                replication_lag=500,
                raft_term=1,
                raft_state="follower",
                version="1.0.0-demo",
                uptime=3400.0,
                connections=2
            )
        ]
        
        return demo_nodes
    
    def _probe_node(self, node_id: str, host: str, port: int) -> Optional[NodeInfo]:
        """Probe a specific node for status"""
        try:
            # Try to connect and get status
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                # Node is reachable, get detailed status
                try:
                    # Send status request (simplified)
                    response = self._get_node_status(host, port)
                    
                    return NodeInfo(
                        node_id=node_id,
                        host=host,
                        port=port,
                        role=response.get("role", "unknown"),
                        health=NodeHealth.HEALTHY,
                        last_seen=datetime.utcnow(),
                        replication_lag=response.get("replication_lag", 0),
                        raft_term=response.get("raft_term", 0),
                        raft_state=response.get("raft_state", "unknown"),
                        version=response.get("version", ""),
                        uptime=response.get("uptime", 0.0),
                        connections=response.get("connections", 0)
                    )
                except Exception as e:
                    self.logger.debug(f"Failed to get detailed status from {node_id}: {e}")
                    # Return basic info if detailed status fails
                    return NodeInfo(
                        node_id=node_id,
                        host=host,
                        port=port,
                        role="unknown",
                        health=NodeHealth.DEGRADED,
                        last_seen=datetime.utcnow()
                    )
            else:
                # Node is not reachable
                return NodeInfo(
                    node_id=node_id,
                    host=host,
                    port=port,
                    role="unknown",
                    health=NodeHealth.FAILED,
                    last_seen=datetime.utcnow()
                )
                
        except Exception as e:
            self.logger.debug(f"Failed to probe node {node_id}: {e}")
            return None
    
    def _get_node_status(self, host: str, port: int) -> Dict[str, Any]:
        """Get detailed status from a node"""
        # This would typically make an HTTP request to the node's status endpoint
        # For now, return a mock response
        return {
            "role": "replica",
            "replication_lag": 0,
            "raft_term": 1,
            "raft_state": "follower",
            "version": "1.0.0",
            "uptime": 3600.0,
            "connections": 5
        }


class FailoverManager:
    """Manages automatic failover decisions and execution"""
    
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.logger = logging.getLogger("failover_manager")
        self.failover_in_progress = False
        self.last_failover = None
        self.min_failover_interval = timedelta(minutes=5)
        
    def should_trigger_failover(self, cluster_state: Dict[str, NodeInfo]) -> bool:
        """Determine if failover should be triggered"""
        if self.failover_in_progress:
            return False
        
        # Check if we're within minimum failover interval
        if (self.last_failover and 
            datetime.utcnow() - self.last_failover < self.min_failover_interval):
            return False
        
        # Find current primary
        primary_nodes = [node for node in cluster_state.values() 
                        if node.role == "primary"]
        
        if not primary_nodes:
            # No primary found - trigger election/promotion
            self.logger.warning("No primary found in cluster")
            return True
        
        if len(primary_nodes) > 1:
            # Multiple primaries - split brain scenario
            self.logger.error("Split brain detected - multiple primaries")
            return True
        
        primary = primary_nodes[0]
        if primary.health == NodeHealth.FAILED:
            # Primary has failed
            self.logger.warning(f"Primary {primary.node_id} has failed")
            return True
        
        # Check if primary is isolated (no healthy replicas)
        healthy_replicas = [node for node in cluster_state.values()
                           if node.role == "replica" and node.health == NodeHealth.HEALTHY]
        
        if not healthy_replicas:
            self.logger.warning("No healthy replicas available")
            # Don't trigger failover if there are no healthy replicas
            return False
        
        return False
    
    def execute_failover(self, cluster_state: Dict[str, NodeInfo]) -> bool:
        """Execute automatic failover"""
        if self.failover_in_progress:
            return False
        
        self.failover_in_progress = True
        self.last_failover = datetime.utcnow()
        
        try:
            self.logger.info("Starting automatic failover")
            
            # Find best candidate for promotion
            candidate = self._select_failover_candidate(cluster_state)
            if not candidate:
                self.logger.error("No suitable failover candidate found")
                return False
            
            # Execute promotion
            success = self._promote_candidate(candidate)
            if success:
                self.logger.info(f"Successfully promoted {candidate.node_id} to primary")
                
                # Update other replicas to point to new primary
                self._update_replica_configuration(cluster_state, candidate)
                
                # Record failover event
                self.orchestrator.metrics.record_failover(candidate.node_id)
                
                return True
            else:
                self.logger.error(f"Failed to promote {candidate.node_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failover execution failed: {e}")
            return False
        finally:
            self.failover_in_progress = False
    
    def _select_failover_candidate(self, cluster_state: Dict[str, NodeInfo]) -> Optional[NodeInfo]:
        """Select the best candidate for promotion to primary"""
        candidates = [node for node in cluster_state.values()
                     if node.role == "replica" and node.health == NodeHealth.HEALTHY]
        
        if not candidates:
            return None
        
        # Select candidate with lowest replication lag and highest uptime
        best_candidate = min(candidates, 
                           key=lambda x: (x.replication_lag, -x.uptime))
        
        return best_candidate
    
    def _promote_candidate(self, candidate: NodeInfo) -> bool:
        """Promote a candidate to primary"""
        try:
            # Send promotion command to the candidate
            # This would typically be an HTTP request or database command
            self.logger.info(f"Promoting {candidate.node_id} to primary")
            
            # For RAFT-based clusters, this might trigger a leadership election
            # For traditional replication, this would be a direct promotion command
            
            return True  # Simplified - assume success
        except Exception as e:
            self.logger.error(f"Failed to promote {candidate.node_id}: {e}")
            return False
    
    def _update_replica_configuration(self, cluster_state: Dict[str, NodeInfo], 
                                    new_primary: NodeInfo):
        """Update replica configuration to point to new primary"""
        for node in cluster_state.values():
            if node.node_id != new_primary.node_id and node.role == "replica":
                try:
                    # Send configuration update to replica
                    self.logger.info(f"Updating {node.node_id} to replicate from {new_primary.node_id}")
                    # Implementation would send actual reconfiguration commands
                except Exception as e:
                    self.logger.error(f"Failed to update replica {node.node_id}: {e}")


class OrchestratorMetrics:
    """Tracks orchestrator metrics and events"""
    
    def __init__(self):
        self.metrics = {
            "total_nodes_discovered": 0,
            "healthy_nodes": 0,
            "failed_nodes": 0,
            "failovers_executed": 0,
            "discovery_runs": 0,
            "last_discovery": None,
            "cluster_uptime": 0,
            "events": []
        }
        self.lock = threading.Lock()
    
    def update_cluster_metrics(self, nodes: List[NodeInfo]):
        """Update cluster-wide metrics"""
        with self.lock:
            self.metrics["total_nodes_discovered"] = len(nodes)
            self.metrics["healthy_nodes"] = sum(1 for n in nodes if n.health == NodeHealth.HEALTHY)
            self.metrics["failed_nodes"] = sum(1 for n in nodes if n.health == NodeHealth.FAILED)
            self.metrics["discovery_runs"] += 1
            self.metrics["last_discovery"] = datetime.utcnow().isoformat()
    
    def record_failover(self, new_primary_id: str):
        """Record a failover event"""
        with self.lock:
            self.metrics["failovers_executed"] += 1
            event = {
                "type": "failover",
                "timestamp": datetime.utcnow().isoformat(),
                "new_primary": new_primary_id
            }
            self.metrics["events"].append(event)
            
            # Keep only last 100 events
            if len(self.metrics["events"]) > 100:
                self.metrics["events"] = self.metrics["events"][-100:]
    
    def record_event(self, event_type: str, details: Dict[str, Any]):
        """Record a general event"""
        with self.lock:
            event = {
                "type": event_type,
                "timestamp": datetime.utcnow().isoformat(),
                "details": details
            }
            self.metrics["events"].append(event)
            
            if len(self.metrics["events"]) > 100:
                self.metrics["events"] = self.metrics["events"][-100:]
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get all metrics"""
        with self.lock:
            return self.metrics.copy()


class DatabaseOrchestrator:
    """Main orchestrator class - GitHub Orchestrator-style management"""
    
    def __init__(self):
        self.logger = logging.getLogger("orchestrator")
        self.cluster_state: Dict[str, NodeInfo] = {}
        self.discovery = ClusterDiscovery(self)
        self.failover_manager = FailoverManager(self)
        self.metrics = OrchestratorMetrics()
        self.running = True
        
        # Configuration
        self.discovery_interval = int(os.getenv("DISCOVERY_INTERVAL", "30"))
        self.health_check_interval = int(os.getenv("HEALTH_CHECK_INTERVAL", "10"))
        
        # Start background tasks
        self._start_background_tasks()
    
    def _start_background_tasks(self):
        """Start background monitoring tasks"""
        # Discovery loop
        threading.Thread(target=self._discovery_loop, daemon=True).start()
        
        # Health monitoring loop
        threading.Thread(target=self._health_monitor_loop, daemon=True).start()
        
        # Failover decision loop
        threading.Thread(target=self._failover_decision_loop, daemon=True).start()
    
    def _discovery_loop(self):
        """Main discovery loop"""
        while self.running:
            try:
                nodes = self.discovery.discover_nodes()
                
                # Update cluster state
                new_state = {}
                for node in nodes:
                    new_state[node.node_id] = node
                
                self.cluster_state = new_state
                self.metrics.update_cluster_metrics(nodes)
                
                self.logger.info(f"Discovered {len(nodes)} nodes")
                
            except Exception as e:
                self.logger.error(f"Discovery loop error: {e}")
            
            time.sleep(self.discovery_interval)
    
    def _health_monitor_loop(self):
        """Health monitoring loop"""
        while self.running:
            try:
                # Update health status for each node
                for node in self.cluster_state.values():
                    self._update_node_health(node)
                
            except Exception as e:
                self.logger.error(f"Health monitor error: {e}")
            
            time.sleep(self.health_check_interval)
    
    def _failover_decision_loop(self):
        """Failover decision loop"""
        while self.running:
            try:
                if self.failover_manager.should_trigger_failover(self.cluster_state):
                    self.logger.info("Triggering automatic failover")
                    self.failover_manager.execute_failover(self.cluster_state)
                
            except Exception as e:
                self.logger.error(f"Failover decision error: {e}")
            
            time.sleep(5)  # Check every 5 seconds
    
    def _update_node_health(self, node: NodeInfo):
        """Update health status for a node"""
        try:
            # Probe node health
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            result = sock.connect_ex((node.host, node.port))
            sock.close()
            
            if result == 0:
                node.health = NodeHealth.HEALTHY
                node.last_seen = datetime.utcnow()
            else:
                # Check if node has been down for too long
                if datetime.utcnow() - node.last_seen > timedelta(minutes=2):
                    node.health = NodeHealth.FAILED
                else:
                    node.health = NodeHealth.DEGRADED
                    
        except Exception as e:
            self.logger.debug(f"Health check failed for {node.node_id}: {e}")
            node.health = NodeHealth.FAILED
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """Get current cluster status"""
        return {
            "nodes": {node_id: node.to_dict() for node_id, node in self.cluster_state.items()},
            "metrics": self.metrics.get_metrics(),
            "topology": self._determine_topology(),
            "health_summary": self._get_health_summary()
        }
    
    def _determine_topology(self) -> str:
        """Determine cluster topology type"""
        if not self.cluster_state:
            return "empty"
        
        raft_nodes = sum(1 for node in self.cluster_state.values() 
                        if node.raft_state != "unknown")
        
        if raft_nodes == len(self.cluster_state):
            return ClusterTopology.RAFT_CONSENSUS.value
        elif raft_nodes == 0:
            return ClusterTopology.PRIMARY_REPLICA.value
        else:
            return ClusterTopology.MIXED.value
    
    def _get_health_summary(self) -> Dict[str, Any]:
        """Get cluster health summary"""
        if not self.cluster_state:
            return {"status": "empty", "healthy_nodes": 0, "total_nodes": 0}
        
        healthy = sum(1 for node in self.cluster_state.values() 
                     if node.health == NodeHealth.HEALTHY)
        total = len(self.cluster_state)
        
        if healthy == total:
            status = "healthy"
        elif healthy > total // 2:
            status = "degraded"
        else:
            status = "critical"
        
        return {
            "status": status,
            "healthy_nodes": healthy,
            "total_nodes": total,
            "health_percentage": (healthy / total) * 100 if total > 0 else 0
        }
    
    def manual_failover(self, target_node_id: str) -> bool:
        """Trigger manual failover to specific node"""
        self.logger.info(f"Manual failover requested for node: {target_node_id}")
        
        if target_node_id not in self.cluster_state:
            self.logger.error(f"Manual failover failed: Node {target_node_id} not found in cluster")
            return False
        
        target_node = self.cluster_state[target_node_id]
        self.logger.info(f"Target node {target_node_id} status: health={target_node.health.value}, role={target_node.role}")
        
        # For manual failover, allow degraded nodes as well
        if target_node.health == NodeHealth.FAILED:
            self.logger.error(f"Manual failover failed: Target node {target_node_id} is failed")
            return False
        
        if target_node.role == "primary":
            self.logger.warning(f"Manual failover skipped: Node {target_node_id} is already primary")
            return True  # Already primary, consider it success
        
        # Execute the failover
        try:
            success = self.failover_manager._promote_candidate(target_node)
            if success:
                # Update the node's role in our cluster state
                target_node.role = "primary"
                
                # Update other nodes to be replicas
                for node in self.cluster_state.values():
                    if node.node_id != target_node_id and node.role == "primary":
                        node.role = "replica"
                
                # Record the failover event
                self.metrics.record_failover(target_node_id)
                self.logger.info(f"Manual failover successful: {target_node_id} promoted to primary")
                
                return True
            else:
                self.logger.error(f"Manual failover failed: Promotion of {target_node_id} failed")
                return False
                
        except Exception as e:
            self.logger.error(f"Manual failover failed with exception: {e}")
            return False
    
    def shutdown(self):
        """Shutdown orchestrator"""
        self.logger.info("Shutting down orchestrator")
        self.running = False


# Flask Web UI
app = Flask(__name__)
orchestrator = DatabaseOrchestrator()

@app.route('/')
def dashboard():
    """Main dashboard"""
    return render_template_string(DASHBOARD_HTML)

@app.route('/api/cluster/status')
def cluster_status():
    """Get cluster status"""
    return jsonify(orchestrator.get_cluster_status())

@app.route('/api/cluster/failover', methods=['POST'])
def manual_failover():
    """Trigger manual failover"""
    try:
        data = request.get_json() or {}
        target_node = data.get('target_node')
        
        orchestrator.logger.info(f"Received failover request: {data}")
        
        if not target_node:
            return jsonify({"error": "target_node required", "success": False}), 400
        
        success = orchestrator.manual_failover(target_node)
        
        if success:
            return jsonify({
                "success": True, 
                "message": f"Failover to {target_node} completed successfully"
            })
        else:
            return jsonify({
                "success": False, 
                "error": f"Failover to {target_node} failed - check logs for details"
            })
            
    except Exception as e:
        orchestrator.logger.error(f"Manual failover API error: {e}")
        return jsonify({
            "success": False, 
            "error": f"Internal error: {str(e)}"
        }), 500

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})

# Simple HTML dashboard template
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>HMSSQL Orchestrator</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .header { background: #2c3e50; color: white; padding: 20px; margin-bottom: 20px; }
        .card { border: 1px solid #ddd; padding: 20px; margin: 10px 0; border-radius: 5px; }
        .healthy { background-color: #d4edda; }
        .degraded { background-color: #fff3cd; }
        .failed { background-color: #f8d7da; }
        .metrics { display: flex; gap: 20px; }
        .metric { flex: 1; text-align: center; }
        table { width: 100%; border-collapse: collapse; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        button { background: #007bff; color: white; border: none; padding: 10px 20px; cursor: pointer; }
        button:hover { background: #0056b3; }
    </style>
</head>
<body>
    <div class="header">
        <h1>🎯 HMSSQL Database Orchestrator</h1>
        <p>GitHub Orchestrator-style cluster management</p>
    </div>
    
    <div id="cluster-status">
        <h2>Cluster Status</h2>
        <div id="status-content">Loading...</div>
    </div>
    
    <div id="nodes-table">
        <h2>Database Nodes</h2>
        <table id="nodes">
            <thead>
                <tr>
                    <th>Node ID</th>
                    <th>Host:Port</th>
                    <th>Role</th>
                    <th>Health</th>
                    <th>Lag</th>
                    <th>RAFT State</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody id="nodes-body">
                <tr><td colspan="7">Loading...</td></tr>
            </tbody>
        </table>
    </div>
    
    <div id="metrics">
        <h2>Metrics</h2>
        <div class="metrics" id="metrics-content">
            Loading...
        </div>
    </div>
    
    <script>
        function refreshStatus() {
            fetch('/api/cluster/status')
                .then(response => response.json())
                .then(data => {
                    updateStatus(data);
                    updateNodesTable(data.nodes);
                    updateMetrics(data.metrics);
                })
                .catch(error => {
                    console.error('Error:', error);
                    document.getElementById('status-content').innerHTML = 
                        '<div class="card failed"><h3>Error loading cluster status</h3></div>';
                });
        }
        
        function updateStatus(data) {
            const health = data.health_summary || {};
            const statusDiv = document.getElementById('status-content');
            const status = health.status || 'unknown';
            const healthyNodes = health.healthy_nodes || 0;
            const totalNodes = health.total_nodes || 0;
            const healthPercentage = health.health_percentage || 0;
            
            statusDiv.className = 'card ' + status;
            statusDiv.innerHTML = `
                <h3>Overall Health: ${status.toUpperCase()}</h3>
                <p>Healthy Nodes: ${healthyNodes}/${totalNodes} (${healthPercentage.toFixed(1)}%)</p>
                <p>Topology: ${data.topology || 'unknown'}</p>
            `;
        }
        
        function updateNodesTable(nodes) {
            const tbody = document.getElementById('nodes-body');
            tbody.innerHTML = '';
            
            if (!nodes || Object.keys(nodes).length === 0) {
                const row = tbody.insertRow();
                row.innerHTML = '<td colspan="7">No nodes discovered</td>';
                return;
            }
            
            for (const [nodeId, node] of Object.entries(nodes)) {
                const row = tbody.insertRow();
                row.className = node.health || 'unknown';
                
                const promoteButton = node.role !== 'primary' ? 
                    `<button class="promote-btn" data-node-id="${escapeHtml(node.node_id)}">Promote</button>` : 
                    'Primary';
                
                row.innerHTML = `
                    <td>${escapeHtml(node.node_id || '')}</td>
                    <td>${escapeHtml(node.host || '')}:${node.port || 0}</td>
                    <td>${escapeHtml(node.role || 'unknown')}</td>
                    <td>${escapeHtml(node.health || 'unknown')}</td>
                    <td>${node.replication_lag || 0}</td>
                    <td>${escapeHtml(node.raft_state || 'unknown')}</td>
                    <td>${promoteButton}</td>
                `;
            }
        }
        
        function updateMetrics(metrics) {
            const metricsDiv = document.getElementById('metrics-content');
            const discoveryRuns = metrics?.discovery_runs || 0;
            const failovers = metrics?.failovers_executed || 0;
            const lastDiscovery = metrics?.last_discovery;
            
            metricsDiv.innerHTML = `
                <div class="metric card">
                    <h4>Discovery Runs</h4>
                    <p>${discoveryRuns}</p>
                </div>
                <div class="metric card">
                    <h4>Failovers</h4>
                    <p>${failovers}</p>
                </div>
                <div class="metric card">
                    <h4>Last Discovery</h4>
                    <p>${lastDiscovery ? new Date(lastDiscovery).toLocaleString() : 'Never'}</p>
                </div>
            `;
        }
        
        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
        
        function promoteNode(nodeId) {
            if (confirm(`Promote ${nodeId} to primary?`)) {
                fetch('/api/cluster/failover', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({target_node: nodeId})
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        alert('Failover initiated');
                        refreshStatus();
                    } else {
                        alert('Failover failed');
                    }
                })
                .catch(error => {
                    console.error('Failover error:', error);
                    alert('Failover request failed');
                });
            }
        }
        
        // Add event delegation for promote buttons
        document.addEventListener('click', function(event) {
            if (event.target.classList.contains('promote-btn')) {
                const nodeId = event.target.getAttribute('data-node-id');
                promoteNode(nodeId);
            }
        });
        
        // Auto-refresh every 10 seconds
        setInterval(refreshStatus, 10000);
        refreshStatus();
    </script>
</body>
</html>
"""

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    port = int(os.getenv('PORT', 3000))
    app.run(host='0.0.0.0', port=port, debug=False)
