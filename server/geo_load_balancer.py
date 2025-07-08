"""
Geo-Aware Load Balancer for pyHMSSQL Distributed DBMS

This module implements production-grade geo-aware load balancing with:
- Client IP geolocation and region detection
- Leader replica proximity routing with health checks
- Follower read optimization with staleness guarantees
- Automatic failover with closest-replica fallback
- Optional Rust sidecar service for high-performance routing

Inspired by CockroachDB's geo-partitioning and zone configs.
"""

import asyncio
import logging
import threading
import time
import json
import socket
import ipaddress
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict
import requests
import geoip2.database
import geoip2.errors

# Try to import Rust HLC for performance
try:
    from cycore.hlc_ts import HybridLogicalClock
    HLC_AVAILABLE = True
except ImportError:
    HLC_AVAILABLE = False
    # Fallback implementation
    class HybridLogicalClock:
        def __init__(self, node_id: str):
            self.node_id = node_id
            self._logical_time = 0
        
        def now(self) -> int:
            self._logical_time += 1
            return int(time.time() * 1000000) + self._logical_time

logger = logging.getLogger(__name__)


class RoutingStrategy(Enum):
    """Load balancing routing strategies"""
    CLOSEST_LEADER = auto()         # Route to closest healthy leader
    CLOSEST_REPLICA = auto()        # Route to closest replica (follower reads)
    LEAST_LOADED = auto()           # Route to least loaded replica
    ROUND_ROBIN = auto()            # Simple round-robin
    LATENCY_WEIGHTED = auto()       # Weight by measured latencies


class ReadConsistency(Enum):
    """Read consistency levels for follower reads"""
    STRONG = auto()                 # Read from leader only
    BOUNDED_STALENESS = auto()      # Read from follower with max staleness
    EVENTUAL = auto()               # Read from any replica


@dataclass
class GeoLocation:
    """Geographic location information"""
    country: str
    region: str
    city: str
    latitude: float
    longitude: float
    timezone: str


@dataclass
class ReplicaInfo:
    """Information about a database replica"""
    node_id: str
    host: str
    port: int
    is_leader: bool = False
    geo_location: Optional[GeoLocation] = None
    zone: str = "default"
    
    # Health and performance metrics
    healthy: bool = True
    last_heartbeat: float = field(default_factory=time.time)
    cpu_load: float = 0.0
    memory_usage: float = 0.0
    disk_usage: float = 0.0
    connection_count: int = 0
    
    # Replication metrics
    raft_term: int = 0
    log_index: int = 0
    replication_lag_ms: int = 0
    
    # Performance tracking
    avg_latency_ms: float = 0.0
    request_count: int = 0
    error_rate: float = 0.0


@dataclass
class ClientRequest:
    """Client request with routing information"""
    client_ip: str
    client_location: Optional[GeoLocation] = None
    query_type: str = "read"  # read, write, admin
    consistency_level: ReadConsistency = ReadConsistency.STRONG
    max_staleness_ms: int = 1000
    preferred_zone: Optional[str] = None


class GeoIPResolver:
    """GeoIP resolution with caching and fallbacks"""
    
    def __init__(self, geoip_db_path: Optional[str] = None):
        self.geoip_db_path = geoip_db_path
        self.reader = None
        self.cache = {}
        self.cache_ttl = 3600  # 1 hour
        
        # Initialize GeoIP database
        if geoip_db_path:
            try:
                self.reader = geoip2.database.Reader(geoip_db_path)
                logger.info(f"Loaded GeoIP database: {geoip_db_path}")
            except Exception as e:
                logger.warning(f"Failed to load GeoIP database: {e}")
    
    def resolve_location(self, ip_address: str) -> Optional[GeoLocation]:
        """Resolve IP address to geographic location"""
        # Check cache first
        cache_key = ip_address
        if cache_key in self.cache:
            cached_time, location = self.cache[cache_key]
            if time.time() - cached_time < self.cache_ttl:
                return location
        
        location = None
        
        # Try local GeoIP database first
        if self.reader:
            location = self._resolve_with_geoip2(ip_address)
        
        # Fallback to external service (with rate limiting)
        if not location:
            location = self._resolve_with_external_service(ip_address)
        
        # Cache result
        if location:
            self.cache[cache_key] = (time.time(), location)
        
        return location
    
    def _resolve_with_geoip2(self, ip_address: str) -> Optional[GeoLocation]:
        """Resolve using local GeoIP2 database"""
        try:
            response = self.reader.city(ip_address)
            return GeoLocation(
                country=response.country.name or "Unknown",
                region=response.subdivisions.most_specific.name or "Unknown", 
                city=response.city.name or "Unknown",
                latitude=float(response.location.latitude or 0.0),
                longitude=float(response.location.longitude or 0.0),
                timezone=response.location.time_zone or "UTC"
            )
        except geoip2.errors.AddressNotFoundError:
            logger.debug(f"IP {ip_address} not found in GeoIP database")
        except Exception as e:
            logger.warning(f"GeoIP2 resolution error for {ip_address}: {e}")
        
        return None
    
    def _resolve_with_external_service(self, ip_address: str) -> Optional[GeoLocation]:
        """Fallback to external geolocation service"""
        try:
            # Use a free service with rate limiting
            response = requests.get(
                f"http://ip-api.com/json/{ip_address}",
                timeout=2.0
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "success":
                    return GeoLocation(
                        country=data.get("country", "Unknown"),
                        region=data.get("regionName", "Unknown"),
                        city=data.get("city", "Unknown"),
                        latitude=float(data.get("lat", 0.0)),
                        longitude=float(data.get("lon", 0.0)),
                        timezone=data.get("timezone", "UTC")
                    )
        except Exception as e:
            logger.debug(f"External geolocation failed for {ip_address}: {e}")
        
        return None


class GeoAwareLoadBalancer:
    """
    Production-grade geo-aware load balancer for distributed DBMS.
    
    Features:
    - Geographic proximity routing
    - Health-aware replica selection
    - Follower read optimization
    - Zone-aware placement
    - Performance-based routing decisions
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        
        # Core components
        self.replicas: Dict[str, ReplicaInfo] = {}
        self.zones: Dict[str, List[str]] = defaultdict(list)
        self.geoip_resolver = GeoIPResolver(
            self.config.get("geoip_db_path")
        )
        
        # Routing configuration
        self.default_strategy = RoutingStrategy.CLOSEST_LEADER
        self.enable_follower_reads = self.config.get("enable_follower_reads", True)
        self.default_staleness_ms = self.config.get("default_staleness_ms", 1000)
        self.health_check_interval = self.config.get("health_check_interval", 10)
        
        # Performance tracking
        self.request_metrics: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {
                "count": 0,
                "total_latency": 0.0,
                "errors": 0,
                "last_request": 0.0
            }
        )
        
        # Background tasks
        self.running = False
        self.health_monitor_task = None
        self.metrics_task = None
        
        # Hybrid Logical Clock for consistency tracking
        if HLC_AVAILABLE:
            self.hlc = HybridLogicalClock("geo_lb")
        
        logger.info("Geo-aware load balancer initialized")
    
    async def start(self):
        """Start the load balancer"""
        if self.running:
            return
        
        self.running = True
        
        # Start background tasks
        self.health_monitor_task = asyncio.create_task(self._health_monitor())
        self.metrics_task = asyncio.create_task(self._metrics_collector())
        
        logger.info("Geo-aware load balancer started")
    
    async def stop(self):
        """Stop the load balancer"""
        self.running = False
        
        # Cancel background tasks
        if self.health_monitor_task:
            self.health_monitor_task.cancel()
        if self.metrics_task:
            self.metrics_task.cancel()
        
        logger.info("Geo-aware load balancer stopped")
    
    def register_replica(self, replica: ReplicaInfo):
        """Register a new replica"""
        self.replicas[replica.node_id] = replica
        self.zones[replica.zone].append(replica.node_id)
        
        logger.info(f"Registered replica {replica.node_id} in zone {replica.zone}")
    
    def unregister_replica(self, node_id: str):
        """Unregister a replica"""
        if node_id in self.replicas:
            replica = self.replicas[node_id]
            self.zones[replica.zone].remove(node_id)
            del self.replicas[node_id]
            
            logger.info(f"Unregistered replica {node_id}")
    
    def update_replica_health(self, node_id: str, health_metrics: Dict[str, Any]):
        """Update replica health metrics"""
        if node_id not in self.replicas:
            return
        
        replica = self.replicas[node_id]
        replica.last_heartbeat = time.time()
        replica.healthy = health_metrics.get("healthy", True)
        replica.cpu_load = health_metrics.get("cpu_load", 0.0)
        replica.memory_usage = health_metrics.get("memory_usage", 0.0)
        replica.disk_usage = health_metrics.get("disk_usage", 0.0)
        replica.connection_count = health_metrics.get("connection_count", 0)
        replica.replication_lag_ms = health_metrics.get("replication_lag_ms", 0)
        
        # Update Raft metrics
        replica.raft_term = health_metrics.get("raft_term", 0)
        replica.log_index = health_metrics.get("log_index", 0)
        replica.is_leader = health_metrics.get("is_leader", False)
    
    async def route_request(self, request: ClientRequest) -> Optional[ReplicaInfo]:
        """Route a client request to the best replica"""
        # Resolve client location if not provided
        if not request.client_location:
            request.client_location = self.geoip_resolver.resolve_location(
                request.client_ip
            )
        
        # Filter healthy replicas
        healthy_replicas = [
            replica for replica in self.replicas.values()
            if replica.healthy and self._is_replica_available(replica)
        ]
        
        if not healthy_replicas:
            logger.error("No healthy replicas available")
            return None
        
        # Route based on query type and consistency requirements
        if request.query_type == "write" or request.consistency_level == ReadConsistency.STRONG:
            return await self._route_to_leader(healthy_replicas, request)
        elif self.enable_follower_reads:
            return await self._route_for_follower_read(healthy_replicas, request)
        else:
            return await self._route_to_leader(healthy_replicas, request)
    
    async def _route_to_leader(self, candidates: List[ReplicaInfo], request: ClientRequest) -> Optional[ReplicaInfo]:
        """Route to the best available leader"""
        leaders = [r for r in candidates if r.is_leader]
        
        if not leaders:
            logger.warning("No healthy leaders available")
            return None
        
        if len(leaders) == 1:
            return leaders[0]
        
        # Select best leader based on strategy
        return self._select_best_replica(leaders, request, RoutingStrategy.CLOSEST_LEADER)
    
    async def _route_for_follower_read(self, candidates: List[ReplicaInfo], request: ClientRequest) -> Optional[ReplicaInfo]:
        """Route read to best replica (including followers)"""
        # Filter replicas that meet staleness requirements
        max_staleness = request.max_staleness_ms
        suitable_replicas = []
        
        for replica in candidates:
            if replica.is_leader:
                # Leaders are always suitable
                suitable_replicas.append(replica)
            elif replica.replication_lag_ms <= max_staleness:
                # Followers within staleness bounds
                suitable_replicas.append(replica)
        
        if not suitable_replicas:
            # Fallback to leaders only
            logger.warning("No replicas meet staleness requirements, falling back to leaders")
            return await self._route_to_leader(candidates, request)
        
        # Select best replica
        return self._select_best_replica(suitable_replicas, request, RoutingStrategy.CLOSEST_REPLICA)
    
    def _select_best_replica(self, candidates: List[ReplicaInfo], request: ClientRequest, strategy: RoutingStrategy) -> Optional[ReplicaInfo]:
        """Select the best replica from candidates using the specified strategy"""
        if not candidates:
            return None
        
        if len(candidates) == 1:
            return candidates[0]
        
        if strategy == RoutingStrategy.CLOSEST_LEADER or strategy == RoutingStrategy.CLOSEST_REPLICA:
            return self._select_closest_replica(candidates, request)
        elif strategy == RoutingStrategy.LEAST_LOADED:
            return self._select_least_loaded_replica(candidates)
        elif strategy == RoutingStrategy.LATENCY_WEIGHTED:
            return self._select_latency_weighted_replica(candidates)
        else:  # ROUND_ROBIN
            return self._select_round_robin_replica(candidates)
    
    def _select_closest_replica(self, candidates: List[ReplicaInfo], request: ClientRequest) -> ReplicaInfo:
        """Select geographically closest replica"""
        if not request.client_location:
            # Fallback to least loaded if no location info
            return self._select_least_loaded_replica(candidates)
        
        # Calculate distances
        def calculate_distance(replica: ReplicaInfo) -> float:
            if not replica.geo_location:
                return float('inf')  # Unknown location goes to end
            
            # Simple haversine distance calculation
            from math import radians, sin, cos, sqrt, atan2
            
            lat1, lon1 = radians(request.client_location.latitude), radians(request.client_location.longitude)
            lat2, lon2 = radians(replica.geo_location.latitude), radians(replica.geo_location.longitude)
            
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            
            a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
            c = 2 * atan2(sqrt(a), sqrt(1-a))
            
            return 6371 * c  # Earth radius in km
        
        # Sort by distance and return closest
        candidates_with_distance = [
            (replica, calculate_distance(replica)) for replica in candidates
        ]
        candidates_with_distance.sort(key=lambda x: x[1])
        
        return candidates_with_distance[0][0]
    
    def _select_least_loaded_replica(self, candidates: List[ReplicaInfo]) -> ReplicaInfo:
        """Select replica with lowest load score"""
        def load_score(replica: ReplicaInfo) -> float:
            # Weighted load score (0.0 = no load, 1.0+ = overloaded)
            cpu_weight = 0.4
            memory_weight = 0.3
            connection_weight = 0.3
            
            # Normalize connection count (assume 100 connections = 100% load)
            conn_normalized = min(replica.connection_count / 100.0, 1.0)
            
            return (cpu_weight * replica.cpu_load + 
                   memory_weight * replica.memory_usage +
                   connection_weight * conn_normalized)
        
        return min(candidates, key=load_score)
    
    def _select_latency_weighted_replica(self, candidates: List[ReplicaInfo]) -> ReplicaInfo:
        """Select replica with best latency characteristics"""
        # Prefer replicas with lower average latency and higher success rates
        def latency_score(replica: ReplicaInfo) -> float:
            metrics = self.request_metrics[replica.node_id]
            
            # Penalize high latency and error rates
            latency_penalty = replica.avg_latency_ms
            error_penalty = metrics["errors"] / max(metrics["count"], 1) * 1000
            
            return latency_penalty + error_penalty
        
        return min(candidates, key=latency_score)
    
    def _select_round_robin_replica(self, candidates: List[ReplicaInfo]) -> ReplicaInfo:
        """Simple round-robin selection"""
        # Use HLC time for deterministic selection
        if HLC_AVAILABLE:
            index = self.hlc.now() % len(candidates)
        else:
            index = int(time.time() * 1000) % len(candidates)
        
        return candidates[index]
    
    def _is_replica_available(self, replica: ReplicaInfo) -> bool:
        """Check if replica is available for requests"""
        # Check basic health
        if not replica.healthy:
            return False
        
        # Check heartbeat freshness
        heartbeat_timeout = self.health_check_interval * 3
        if time.time() - replica.last_heartbeat > heartbeat_timeout:
            return False
        
        # Check if overloaded
        if replica.cpu_load > 0.95 or replica.memory_usage > 0.95:
            return False
        
        return True
    
    async def _health_monitor(self):
        """Background health monitoring task"""
        while self.running:
            try:
                await self._check_replica_health()
                await asyncio.sleep(self.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
                await asyncio.sleep(5)
    
    async def _check_replica_health(self):
        """Check health of all replicas"""
        tasks = []
        for replica in self.replicas.values():
            tasks.append(self._ping_replica(replica))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _ping_replica(self, replica: ReplicaInfo):
        """Ping a replica to check health"""
        try:
            start_time = time.time()
            
            # Simple TCP connection test
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(replica.host, replica.port),
                timeout=2.0
            )
            
            writer.close()
            await writer.wait_closed()
            
            # Update metrics
            latency = (time.time() - start_time) * 1000
            replica.avg_latency_ms = (replica.avg_latency_ms * 0.8 + latency * 0.2)
            
            # Mark as healthy if we can connect
            replica.healthy = True
            replica.last_heartbeat = time.time()
            
        except Exception as e:
            logger.debug(f"Health check failed for {replica.node_id}: {e}")
            replica.healthy = False
    
    async def _metrics_collector(self):
        """Background metrics collection task"""
        while self.running:
            try:
                await self._collect_metrics()
                await asyncio.sleep(60)  # Collect every minute
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Metrics collector error: {e}")
                await asyncio.sleep(30)
    
    async def _collect_metrics(self):
        """Collect and aggregate metrics"""
        # Clean up old metrics
        current_time = time.time()
        for node_id, metrics in list(self.request_metrics.items()):
            if current_time - metrics["last_request"] > 3600:  # 1 hour old
                del self.request_metrics[node_id]
        
        # Log cluster health summary
        healthy_count = sum(1 for r in self.replicas.values() if r.healthy)
        leader_count = sum(1 for r in self.replicas.values() if r.is_leader and r.healthy)
        
        logger.info(f"Cluster health: {healthy_count}/{len(self.replicas)} replicas healthy, "
                   f"{leader_count} leaders available")
    
    def record_request_result(self, node_id: str, latency_ms: float, success: bool):
        """Record the result of a request for metrics"""
        if node_id not in self.replicas:
            return
        
        metrics = self.request_metrics[node_id]
        metrics["count"] += 1
        metrics["total_latency"] += latency_ms
        metrics["last_request"] = time.time()
        
        if not success:
            metrics["errors"] += 1
        
        # Update replica metrics
        replica = self.replicas[node_id]
        replica.request_count += 1
        if metrics["count"] > 0:
            replica.avg_latency_ms = metrics["total_latency"] / metrics["count"]
            replica.error_rate = metrics["errors"] / metrics["count"]
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """Get current cluster status"""
        zone_stats = {}
        for zone, node_ids in self.zones.items():
            healthy = sum(1 for nid in node_ids if self.replicas[nid].healthy)
            leaders = sum(1 for nid in node_ids if self.replicas[nid].is_leader and self.replicas[nid].healthy)
            
            zone_stats[zone] = {
                "total_replicas": len(node_ids),
                "healthy_replicas": healthy,
                "leaders": leaders,
                "avg_load": sum(self.replicas[nid].cpu_load for nid in node_ids) / len(node_ids) if node_ids else 0.0
            }
        
        return {
            "total_replicas": len(self.replicas),
            "healthy_replicas": sum(1 for r in self.replicas.values() if r.healthy),
            "available_leaders": sum(1 for r in self.replicas.values() if r.is_leader and r.healthy),
            "zones": zone_stats,
            "routing_strategy": self.default_strategy.name,
            "follower_reads_enabled": self.enable_follower_reads
        }


# Example usage and integration helper
class GeoLoadBalancerIntegration:
    """Integration helper for DBMS server"""
    
    def __init__(self, dbms_server, config: Dict[str, Any] = None):
        self.dbms_server = dbms_server
        self.load_balancer = GeoAwareLoadBalancer(config)
        
        # Auto-register local server as replica
        self._register_local_server()
    
    def _register_local_server(self):
        """Register the local DBMS server as a replica"""
        replica = ReplicaInfo(
            node_id=getattr(self.dbms_server, 'server_name', 'local'),
            host=self.dbms_server.host,
            port=self.dbms_server.port,
            is_leader=getattr(self.dbms_server.replication_manager, 'role', None) == 'PRIMARY',
            zone="local"
        )
        
        self.load_balancer.register_replica(replica)
    
    async def route_client_request(self, client_ip: str, query_type: str = "read") -> Optional[Tuple[str, int]]:
        """Route a client request and return (host, port)"""
        request = ClientRequest(
            client_ip=client_ip,
            query_type=query_type
        )
        
        replica = await self.load_balancer.route_request(request)
        if replica:
            return (replica.host, replica.port)
        
        return None


# Example configuration
EXAMPLE_CONFIG = {
    "geoip_db_path": "/usr/share/GeoIP/GeoLite2-City.mmdb",  # Optional GeoIP database
    "enable_follower_reads": True,
    "default_staleness_ms": 1000,
    "health_check_interval": 10,
    "zones": {
        "us-east-1": ["node1", "node2"],
        "us-west-2": ["node3", "node4"],
        "eu-west-1": ["node5", "node6"]
    }
}


if __name__ == "__main__":
    # Example usage
    async def test_geo_load_balancer():
        """Test the geo-aware load balancer"""
        print("Testing Geo-Aware Load Balancer...")
        
        # Create load balancer
        lb = GeoAwareLoadBalancer(EXAMPLE_CONFIG)
        await lb.start()
        
        # Register some test replicas
        replicas = [
            ReplicaInfo(
                node_id="us-east-1a",
                host="10.0.1.1",
                port=9999,
                is_leader=True,
                zone="us-east-1",
                geo_location=GeoLocation("US", "Virginia", "Ashburn", 39.0, -77.5, "America/New_York")
            ),
            ReplicaInfo(
                node_id="us-west-2a", 
                host="10.0.2.1",
                port=9999,
                zone="us-west-2",
                geo_location=GeoLocation("US", "Oregon", "Portland", 45.5, -122.7, "America/Los_Angeles")
            ),
            ReplicaInfo(
                node_id="eu-west-1a",
                host="10.0.3.1", 
                port=9999,
                zone="eu-west-1",
                geo_location=GeoLocation("Ireland", "Leinster", "Dublin", 53.3, -6.2, "Europe/Dublin")
            )
        ]
        
        for replica in replicas:
            lb.register_replica(replica)
        
        # Test routing
        test_requests = [
            ClientRequest(client_ip="8.8.8.8", query_type="read"),  # US client
            ClientRequest(client_ip="1.1.1.1", query_type="write"), # Write request
            ClientRequest(client_ip="208.67.222.222", query_type="read", consistency_level=ReadConsistency.EVENTUAL)
        ]
        
        for request in test_requests:
            replica = await lb.route_request(request)
            if replica:
                print(f"Routed {request.query_type} from {request.client_ip} to {replica.node_id} ({replica.zone})")
            else:
                print(f"Failed to route request from {request.client_ip}")
        
        # Print cluster status
        status = lb.get_cluster_status()
        print(f"Cluster status: {status}")
        
        await lb.stop()
    
    # Run test
    import asyncio
    asyncio.run(test_geo_load_balancer())
