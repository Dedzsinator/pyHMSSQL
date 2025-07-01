"""
Comprehensive Statistics Collector for HMSSQL Cluster
Collects detailed metrics from all servers including performance, replication, and health data
"""

import json
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import psutil
import socket
import requests


class ServerRole(Enum):
    """Extended server roles for hierarchical topology"""

    PRIMARY_MASTER = "primary_master"  # Top-level master
    SEMI_MASTER = "semi_master"  # Regional/intermediate master
    REPLICA = "replica"  # Read replica
    CANDIDATE = "candidate"  # Election candidate
    OFFLINE = "offline"  # Server offline


@dataclass
class ServerStats:
    """Comprehensive server statistics"""

    # Basic info
    node_id: str
    host: str
    port: int
    role: ServerRole
    health_status: str
    last_updated: datetime = field(default_factory=datetime.utcnow)

    # Performance metrics
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    memory_total: int = 0
    disk_usage: float = 0.0
    disk_total: int = 0
    network_io: Dict[str, int] = field(default_factory=dict)

    # Database metrics
    active_connections: int = 0
    total_connections: int = 0
    queries_per_second: float = 0.0
    slow_queries: int = 0
    cache_hit_ratio: float = 0.0

    # Replication metrics
    replication_lag: int = 0
    replication_status: str = "unknown"
    master_node: Optional[str] = None
    replica_nodes: List[str] = field(default_factory=list)
    semi_master_nodes: List[str] = field(default_factory=list)

    # RAFT metrics
    raft_term: int = 0
    raft_state: str = "unknown"
    raft_votes_received: int = 0
    raft_log_entries: int = 0
    raft_commit_index: int = 0

    # Network topology
    parent_master: Optional[str] = None
    child_replicas: List[str] = field(default_factory=list)
    topology_level: int = 0  # 0=primary master, 1=semi-master, 2=replica

    # Historical metrics (last hour)
    cpu_history: List[float] = field(default_factory=list)
    memory_history: List[float] = field(default_factory=list)
    qps_history: List[float] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "role": self.role.value,
            "health_status": self.health_status,
            "last_updated": self.last_updated.isoformat(),
            "performance": {
                "cpu_usage": self.cpu_usage,
                "memory_usage": self.memory_usage,
                "memory_total": self.memory_total,
                "disk_usage": self.disk_usage,
                "disk_total": self.disk_total,
                "network_io": self.network_io,
            },
            "database": {
                "active_connections": self.active_connections,
                "total_connections": self.total_connections,
                "queries_per_second": self.queries_per_second,
                "slow_queries": self.slow_queries,
                "cache_hit_ratio": self.cache_hit_ratio,
            },
            "replication": {
                "lag": self.replication_lag,
                "status": self.replication_status,
                "master_node": self.master_node,
                "replica_nodes": self.replica_nodes,
                "semi_master_nodes": self.semi_master_nodes,
            },
            "raft": {
                "term": self.raft_term,
                "state": self.raft_state,
                "votes_received": self.raft_votes_received,
                "log_entries": self.raft_log_entries,
                "commit_index": self.raft_commit_index,
            },
            "topology": {
                "parent_master": self.parent_master,
                "child_replicas": self.child_replicas,
                "level": self.topology_level,
            },
            "history": {
                "cpu": self.cpu_history[-60:],  # Last 60 data points
                "memory": self.memory_history[-60:],
                "qps": self.qps_history[-60:],
            },
        }


class StatsCollector:
    """Collects comprehensive statistics from all cluster nodes"""

    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.logger = logging.getLogger("stats_collector")
        self.running = True

        # Stats storage
        self.server_stats: Dict[str, ServerStats] = {}
        self.collection_interval = 30  # seconds
        self.stats_lock = threading.RLock()

        # Topology tracking
        self.topology_graph = {}
        self.semi_masters = {}

        # Start collection thread
        threading.Thread(target=self._collection_loop, daemon=True).start()

    def _collection_loop(self):
        """Main stats collection loop"""
        while self.running:
            try:
                self._collect_all_stats()
                self._update_topology()
                time.sleep(self.collection_interval)
            except Exception as e:
                self.logger.error(f"Stats collection error: {e}")

    def _collect_all_stats(self):
        """Collect stats from all known servers"""
        with self.stats_lock:
            # Collect from discovered nodes
            for node_id, node_info in self.orchestrator.cluster_state.items():
                try:
                    stats = self._collect_node_stats(node_id, node_info)
                    if stats:
                        self._update_server_stats(stats)
                except Exception as e:
                    self.logger.debug(f"Failed to collect stats from {node_id}: {e}")

            # Collect from semi-masters
            for semi_master_id in self.semi_masters.keys():
                try:
                    if semi_master_id not in self.orchestrator.cluster_state:
                        # Semi-master might not be in main cluster state
                        stats = self._collect_semi_master_stats(semi_master_id)
                        if stats:
                            self._update_server_stats(stats)
                except Exception as e:
                    self.logger.debug(
                        f"Failed to collect stats from semi-master {semi_master_id}: {e}"
                    )

    def _collect_node_stats(self, node_id: str, node_info) -> Optional[ServerStats]:
        """Collect comprehensive stats from a single node"""
        try:
            # Get basic node info
            stats = ServerStats(
                node_id=node_id,
                host=node_info.host,
                port=node_info.port,
                role=self._determine_server_role(node_info),
                health_status=node_info.health.value,
            )

            # Collect performance metrics
            self._collect_performance_metrics(stats)

            # Collect database metrics
            self._collect_database_metrics(stats, node_info)

            # Collect replication metrics
            self._collect_replication_metrics(stats, node_info)

            # Collect RAFT metrics
            self._collect_raft_metrics(stats, node_info)

            # Update historical data
            self._update_historical_data(stats)

            return stats

        except Exception as e:
            self.logger.error(f"Error collecting stats from {node_id}: {e}")
            return None

    def _determine_server_role(self, node_info) -> ServerRole:
        """Determine the hierarchical role of a server"""
        if node_info.role == "primary":
            # Check if this is a primary master or semi-master
            if node_info.node_id in self.semi_masters:
                return ServerRole.SEMI_MASTER
            else:
                return ServerRole.PRIMARY_MASTER
        elif node_info.role == "replica":
            return ServerRole.REPLICA
        elif node_info.role == "candidate":
            return ServerRole.CANDIDATE
        else:
            return ServerRole.OFFLINE

    def _collect_performance_metrics(self, stats: ServerStats):
        """Collect system performance metrics"""
        try:
            # This would typically query the remote server
            # For now, simulate metrics
            stats.cpu_usage = self._get_simulated_cpu()
            stats.memory_usage = self._get_simulated_memory()
            stats.memory_total = 8 * 1024 * 1024 * 1024  # 8GB
            stats.disk_usage = self._get_simulated_disk()
            stats.disk_total = 100 * 1024 * 1024 * 1024  # 100GB
            stats.network_io = {
                "bytes_sent": self._get_simulated_network_sent(),
                "bytes_recv": self._get_simulated_network_recv(),
            }
        except Exception as e:
            self.logger.debug(f"Error collecting performance metrics: {e}")

    def _collect_database_metrics(self, stats: ServerStats, node_info):
        """Collect database-specific metrics"""
        try:
            # Simulate database metrics
            stats.active_connections = self._get_simulated_connections()
            stats.total_connections = stats.active_connections + 10
            stats.queries_per_second = self._get_simulated_qps()
            stats.slow_queries = self._get_simulated_slow_queries()
            stats.cache_hit_ratio = self._get_simulated_cache_hit_ratio()
        except Exception as e:
            self.logger.debug(f"Error collecting database metrics: {e}")

    def _collect_replication_metrics(self, stats: ServerStats, node_info):
        """Collect replication-specific metrics"""
        try:
            stats.replication_lag = node_info.replication_lag
            stats.replication_status = (
                "active" if node_info.health.value == "healthy" else "degraded"
            )

            # Determine master-replica relationships
            if stats.role == ServerRole.REPLICA:
                stats.master_node = self._find_master_for_replica(stats.node_id)
            elif stats.role in [ServerRole.PRIMARY_MASTER, ServerRole.SEMI_MASTER]:
                stats.replica_nodes = self._find_replicas_for_master(stats.node_id)
                if stats.role == ServerRole.PRIMARY_MASTER:
                    stats.semi_master_nodes = self._find_semi_masters_for_primary()
        except Exception as e:
            self.logger.debug(f"Error collecting replication metrics: {e}")

    def _collect_raft_metrics(self, stats: ServerStats, node_info):
        """Collect RAFT consensus metrics"""
        try:
            stats.raft_term = node_info.raft_term
            stats.raft_state = node_info.raft_state

            # Additional RAFT metrics would come from the actual RAFT node
            stats.raft_votes_received = 0
            stats.raft_log_entries = 100 + stats.raft_term * 10  # Simulated
            stats.raft_commit_index = stats.raft_log_entries - 5  # Simulated
        except Exception as e:
            self.logger.debug(f"Error collecting RAFT metrics: {e}")

    def _update_historical_data(self, stats: ServerStats):
        """Update historical metrics"""
        with self.stats_lock:
            if stats.node_id in self.server_stats:
                old_stats = self.server_stats[stats.node_id]
                stats.cpu_history = old_stats.cpu_history + [stats.cpu_usage]
                stats.memory_history = old_stats.memory_history + [stats.memory_usage]
                stats.qps_history = old_stats.qps_history + [stats.queries_per_second]

                # Keep only last hour of data (assuming 30s intervals = 120 points)
                stats.cpu_history = stats.cpu_history[-120:]
                stats.memory_history = stats.memory_history[-120:]
                stats.qps_history = stats.qps_history[-120:]

    def _update_server_stats(self, stats: ServerStats):
        """Update server stats in storage"""
        with self.stats_lock:
            self.server_stats[stats.node_id] = stats

    def _update_topology(self):
        """Update topology graph based on current stats"""
        with self.stats_lock:
            self.topology_graph = self._build_topology_graph()

    def _build_topology_graph(self) -> Dict[str, Any]:
        """Build hierarchical topology graph"""
        graph = {
            "nodes": [],
            "edges": [],
            "levels": {"primary_masters": [], "semi_masters": [], "replicas": []},
        }

        # Add nodes to graph
        for node_id, stats in self.server_stats.items():
            node = {
                "id": node_id,
                "label": f"{node_id}\n{stats.host}:{stats.port}",
                "role": stats.role.value,
                "health": stats.health_status,
                "level": stats.topology_level,
                "group": stats.role.value,
                "cpu": stats.cpu_usage,
                "memory": stats.memory_usage,
                "qps": stats.queries_per_second,
            }
            graph["nodes"].append(node)

            # Categorize by level
            if stats.role == ServerRole.PRIMARY_MASTER:
                graph["levels"]["primary_masters"].append(node_id)
            elif stats.role == ServerRole.SEMI_MASTER:
                graph["levels"]["semi_masters"].append(node_id)
            else:
                graph["levels"]["replicas"].append(node_id)

        # Add edges (replication relationships)
        for node_id, stats in self.server_stats.items():
            # Master -> Replica relationships
            for replica_id in stats.replica_nodes:
                graph["edges"].append(
                    {
                        "from": node_id,
                        "to": replica_id,
                        "type": "replication",
                        "lag": self.server_stats.get(replica_id, {}).replication_lag
                        or 0,
                    }
                )

            # Primary -> Semi-master relationships
            for semi_master_id in stats.semi_master_nodes:
                graph["edges"].append(
                    {
                        "from": node_id,
                        "to": semi_master_id,
                        "type": "semi_master",
                        "lag": self.server_stats.get(semi_master_id, {}).replication_lag
                        or 0,
                    }
                )

        return graph

    # Simulation methods for demo purposes
    def _get_simulated_cpu(self) -> float:
        import random

        return random.uniform(10, 80)

    def _get_simulated_memory(self) -> float:
        import random

        return random.uniform(30, 90)

    def _get_simulated_disk(self) -> float:
        import random

        return random.uniform(20, 70)

    def _get_simulated_network_sent(self) -> int:
        import random

        return random.randint(1000000, 10000000)

    def _get_simulated_network_recv(self) -> int:
        import random

        return random.randint(2000000, 15000000)

    def _get_simulated_connections(self) -> int:
        import random

        return random.randint(5, 50)

    def _get_simulated_qps(self) -> float:
        import random

        return random.uniform(10, 500)

    def _get_simulated_slow_queries(self) -> int:
        import random

        return random.randint(0, 5)

    def _get_simulated_cache_hit_ratio(self) -> float:
        import random

        return random.uniform(85, 99)

    def _find_master_for_replica(self, replica_id: str) -> Optional[str]:
        """Find master node for a replica"""
        for node_id, stats in self.server_stats.items():
            if replica_id in stats.replica_nodes:
                return node_id
        return None

    def _find_replicas_for_master(self, master_id: str) -> List[str]:
        """Find replica nodes for a master"""
        replicas = []
        for node_id, stats in self.server_stats.items():
            if stats.role == ServerRole.REPLICA and stats.master_node == master_id:
                replicas.append(node_id)
        return replicas

    def _find_semi_masters_for_primary(self) -> List[str]:
        """Find semi-master nodes for primary master"""
        semi_masters = []
        for node_id, stats in self.server_stats.items():
            if stats.role == ServerRole.SEMI_MASTER:
                semi_masters.append(node_id)
        return semi_masters

    def get_cluster_overview(self) -> Dict[str, Any]:
        """Get comprehensive cluster overview"""
        with self.stats_lock:
            overview = {
                "total_nodes": len(self.server_stats),
                "primary_masters": 0,
                "semi_masters": 0,
                "replicas": 0,
                "offline_nodes": 0,
                "total_connections": 0,
                "total_qps": 0,
                "avg_replication_lag": 0,
                "cluster_health": "unknown",
            }

            total_lag = 0
            lag_count = 0

            for stats in self.server_stats.values():
                # Count by role
                if stats.role == ServerRole.PRIMARY_MASTER:
                    overview["primary_masters"] += 1
                elif stats.role == ServerRole.SEMI_MASTER:
                    overview["semi_masters"] += 1
                elif stats.role == ServerRole.REPLICA:
                    overview["replicas"] += 1
                elif stats.role == ServerRole.OFFLINE:
                    overview["offline_nodes"] += 1

                # Aggregate metrics
                overview["total_connections"] += stats.active_connections
                overview["total_qps"] += stats.queries_per_second

                if stats.replication_lag > 0:
                    total_lag += stats.replication_lag
                    lag_count += 1

            # Calculate averages
            if lag_count > 0:
                overview["avg_replication_lag"] = total_lag / lag_count

            # Determine cluster health
            healthy_nodes = sum(
                1 for s in self.server_stats.values() if s.health_status == "healthy"
            )
            health_ratio = (
                healthy_nodes / len(self.server_stats) if self.server_stats else 0
            )

            if health_ratio >= 0.9:
                overview["cluster_health"] = "healthy"
            elif health_ratio >= 0.7:
                overview["cluster_health"] = "degraded"
            else:
                overview["cluster_health"] = "critical"

            return overview

    def get_server_stats(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed stats for a specific server"""
        with self.stats_lock:
            if node_id in self.server_stats:
                return self.server_stats[node_id].to_dict()
            return None

    def get_all_server_stats(self) -> Dict[str, Any]:
        """Get stats for all servers"""
        with self.stats_lock:
            return {
                node_id: stats.to_dict() for node_id, stats in self.server_stats.items()
            }

    def get_topology_graph(self) -> Dict[str, Any]:
        """Get current topology graph"""
        with self.stats_lock:
            return self.topology_graph.copy()

    def update_topology(self, new_topology: Dict[str, Any]) -> bool:
        """Update cluster topology based on user changes"""
        try:
            # This would implement topology changes
            # For now, just log the change
            self.logger.info(f"Topology update requested: {new_topology}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to update topology: {e}")
            return False
