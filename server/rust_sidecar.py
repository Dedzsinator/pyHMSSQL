"""
Optional Rust Sidecar Service for High-Performance Geo-Routing

This module provides an optional Rust sidecar process that can handle
high-frequency geo-routing decisions with minimal latency. The sidecar
communicates with the main Python process via gRPC or Unix sockets.

Features:
- Sub-millisecond routing decisions
- Memory-mapped GeoIP lookups
- Lockless routing table updates
- Connection pooling and health checking
- Hot reloading of routing configuration

This is an advanced optimization for high-throughput deployments.
"""

import asyncio
import logging
import json
import socket
import struct
import threading
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class SidecarConfig:
    """Configuration for the Rust sidecar service"""
    enabled: bool = False
    sidecar_port: int = 19999
    socket_path: str = "/tmp/pyhmssql_geo_router.sock"
    max_connections: int = 1000
    health_check_interval: int = 5
    routing_table_update_interval: int = 30


class RustSidecarClient:
    """Client for communicating with Rust geo-routing sidecar"""
    
    def __init__(self, config: SidecarConfig):
        self.config = config
        self.connected = False
        self.socket = None
        self.lock = threading.Lock()
        
        # Performance metrics
        self.request_count = 0
        self.total_latency = 0.0
        self.error_count = 0
    
    async def connect(self) -> bool:
        """Connect to the Rust sidecar"""
        if self.connected:
            return True
        
        try:
            if Path(self.config.socket_path).exists():
                # Use Unix socket for lowest latency
                self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self.socket.connect(self.config.socket_path)
            else:
                # Fallback to TCP
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.connect(("127.0.0.1", self.config.sidecar_port))
            
            self.socket.settimeout(1.0)  # 1 second timeout
            self.connected = True
            
            logger.info("Connected to Rust geo-routing sidecar")
            return True
            
        except Exception as e:
            logger.warning(f"Failed to connect to Rust sidecar: {e}")
            self.connected = False
            return False
    
    async def route_request(self, client_ip: str, query_type: str = "read") -> Optional[Dict[str, Any]]:
        """Route a request using the Rust sidecar"""
        if not self.connected:
            if not await self.connect():
                return None
        
        start_time = time.perf_counter()
        
        try:
            with self.lock:
                # Prepare request
                request = {
                    "client_ip": client_ip,
                    "query_type": query_type,
                    "timestamp": int(time.time() * 1000000)  # microseconds
                }
                
                request_data = json.dumps(request).encode('utf-8')
                
                # Send request (length-prefixed)
                length = struct.pack('!I', len(request_data))
                self.socket.sendall(length + request_data)
                
                # Receive response
                response_length_data = self._recv_exact(4)
                if not response_length_data:
                    raise ConnectionError("Failed to receive response length")
                
                response_length = struct.unpack('!I', response_length_data)[0]
                response_data = self._recv_exact(response_length)
                
                if not response_data:
                    raise ConnectionError("Failed to receive response data")
                
                response = json.loads(response_data.decode('utf-8'))
                
                # Update metrics
                latency = (time.perf_counter() - start_time) * 1000  # ms
                self.request_count += 1
                self.total_latency += latency
                
                return response
                
        except Exception as e:
            logger.error(f"Rust sidecar request failed: {e}")
            self.connected = False
            self.error_count += 1
            
            # Close socket on error
            if self.socket:
                try:
                    self.socket.close()
                except:
                    pass
                self.socket = None
            
            return None
    
    def _recv_exact(self, size: int) -> Optional[bytes]:
        """Receive exactly 'size' bytes from socket"""
        data = b''
        while len(data) < size:
            chunk = self.socket.recv(size - len(data))
            if not chunk:
                return None
            data += chunk
        return data
    
    async def update_routing_table(self, replicas: List[Dict[str, Any]]) -> bool:
        """Update the routing table in the sidecar"""
        if not self.connected:
            if not await self.connect():
                return False
        
        try:
            with self.lock:
                update = {
                    "type": "update_routing_table",
                    "replicas": replicas,
                    "timestamp": int(time.time() * 1000000)
                }
                
                update_data = json.dumps(update).encode('utf-8')
                length = struct.pack('!I', len(update_data))
                self.socket.sendall(length + update_data)
                
                # Wait for acknowledgment
                ack_data = self._recv_exact(4)
                if ack_data:
                    ack_length = struct.unpack('!I', ack_data)[0]
                    if ack_length > 0:
                        ack_response = self._recv_exact(ack_length)
                        if ack_response:
                            ack = json.loads(ack_response.decode('utf-8'))
                            return ack.get("success", False)
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to update routing table: {e}")
            return False
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get performance metrics"""
        avg_latency = 0.0
        if self.request_count > 0:
            avg_latency = self.total_latency / self.request_count
        
        error_rate = 0.0
        if self.request_count > 0:
            error_rate = self.error_count / self.request_count
        
        return {
            "connected": self.connected,
            "request_count": self.request_count,
            "average_latency_ms": avg_latency,
            "error_count": self.error_count,
            "error_rate": error_rate
        }
    
    def close(self):
        """Close connection to sidecar"""
        with self.lock:
            if self.socket:
                try:
                    self.socket.close()
                except:
                    pass
                self.socket = None
            self.connected = False


class SidecarManager:
    """Manager for the Rust sidecar service lifecycle"""
    
    def __init__(self, config: SidecarConfig):
        self.config = config
        self.client = RustSidecarClient(config) if config.enabled else None
        self.process = None
        self.health_task = None
        self.running = False
    
    async def start(self):
        """Start the sidecar service"""
        if not self.config.enabled or self.running:
            return
        
        # Try to start the Rust sidecar process
        if await self._start_sidecar_process():
            # Wait a moment for startup
            await asyncio.sleep(1)
            
            # Connect client
            if self.client and await self.client.connect():
                # Start health monitoring
                self.health_task = asyncio.create_task(self._health_monitor())
                self.running = True
                
                logger.info("Rust geo-routing sidecar started successfully")
                return True
        
        logger.warning("Failed to start Rust geo-routing sidecar")
        return False
    
    async def stop(self):
        """Stop the sidecar service"""
        if not self.running:
            return
        
        self.running = False
        
        # Stop health monitoring
        if self.health_task:
            self.health_task.cancel()
            try:
                await self.health_task
            except asyncio.CancelledError:
                pass
        
        # Close client connection
        if self.client:
            self.client.close()
        
        # Terminate sidecar process
        if self.process:
            try:
                self.process.terminate()
                await asyncio.sleep(1)
                if self.process.poll() is None:
                    self.process.kill()
            except:
                pass
        
        logger.info("Rust geo-routing sidecar stopped")
    
    async def _start_sidecar_process(self) -> bool:
        """Start the Rust sidecar binary"""
        # Look for the sidecar binary
        sidecar_paths = [
            "./target/release/geo_router_sidecar",
            "./target/debug/geo_router_sidecar", 
            "../target/release/geo_router_sidecar",
            "geo_router_sidecar"  # In PATH
        ]
        
        sidecar_binary = None
        for path in sidecar_paths:
            if Path(path).exists():
                sidecar_binary = path
                break
        
        if not sidecar_binary:
            logger.warning("Rust geo-routing sidecar binary not found")
            return False
        
        try:
            import subprocess
            
            # Start the sidecar process
            cmd = [
                sidecar_binary,
                "--port", str(self.config.sidecar_port),
                "--socket", self.config.socket_path,
                "--max-connections", str(self.config.max_connections)
            ]
            
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True
            )
            
            logger.info(f"Started Rust sidecar process: PID {self.process.pid}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start sidecar process: {e}")
            return False
    
    async def _health_monitor(self):
        """Monitor sidecar health"""
        while self.running:
            try:
                if self.client:
                    # Send a ping request to check health
                    response = await self.client.route_request("127.0.0.1", "ping")
                    if not response:
                        logger.warning("Sidecar health check failed")
                
                await asyncio.sleep(self.config.health_check_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
                await asyncio.sleep(5)
    
    async def route_request(self, client_ip: str, query_type: str = "read") -> Optional[Dict[str, Any]]:
        """Route request via sidecar if available"""
        if self.client and self.running:
            return await self.client.route_request(client_ip, query_type)
        return None
    
    async def update_routing_table(self, replicas: List[Dict[str, Any]]) -> bool:
        """Update routing table in sidecar"""
        if self.client and self.running:
            return await self.client.update_routing_table(replicas)
        return False
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get sidecar metrics"""
        if self.client:
            return self.client.get_metrics()
        return {"enabled": False}


# Integration with the main geo load balancer
class HybridGeoLoadBalancer:
    """
    Hybrid load balancer that can use both Python logic and Rust sidecar.
    
    The sidecar is used for high-frequency routing decisions while the Python
    implementation handles complex logic and fallbacks.
    """
    
    def __init__(self, main_lb, sidecar_config: SidecarConfig = None):
        self.main_lb = main_lb
        self.sidecar_config = sidecar_config or SidecarConfig()
        self.sidecar_manager = SidecarManager(self.sidecar_config)
        
        # Performance tracking
        self.sidecar_requests = 0
        self.fallback_requests = 0
    
    async def start(self):
        """Start hybrid load balancer"""
        await self.main_lb.start()
        
        if self.sidecar_config.enabled:
            await self.sidecar_manager.start()
    
    async def stop(self):
        """Stop hybrid load balancer"""
        await self.sidecar_manager.stop()
        await self.main_lb.stop()
    
    async def route_request(self, request) -> Optional[Any]:
        """Route request using sidecar if available, fallback to main LB"""
        # Try sidecar first for simple read requests
        if (self.sidecar_config.enabled and 
            request.query_type == "read" and 
            request.consistency_level.name == "EVENTUAL"):
            
            sidecar_result = await self.sidecar_manager.route_request(
                request.client_ip, 
                request.query_type
            )
            
            if sidecar_result:
                self.sidecar_requests += 1
                
                # Convert sidecar response to replica info
                return self._convert_sidecar_response(sidecar_result)
        
        # Fallback to main load balancer
        self.fallback_requests += 1
        return await self.main_lb.route_request(request)
    
    def _convert_sidecar_response(self, sidecar_result: Dict[str, Any]):
        """Convert sidecar response to main LB format"""
        # This would convert the sidecar's response format to the 
        # ReplicaInfo format expected by the main load balancer
        node_id = sidecar_result.get("node_id")
        if node_id and node_id in self.main_lb.replicas:
            return self.main_lb.replicas[node_id]
        return None
    
    async def update_sidecar_routing_table(self):
        """Update sidecar with current routing table"""
        if not self.sidecar_config.enabled:
            return
        
        # Convert replica info to sidecar format
        replicas_data = []
        for replica in self.main_lb.replicas.values():
            replica_data = {
                "node_id": replica.node_id,
                "host": replica.host, 
                "port": replica.port,
                "is_leader": replica.is_leader,
                "healthy": replica.healthy,
                "zone": replica.zone,
                "geo_location": {
                    "latitude": replica.geo_location.latitude if replica.geo_location else 0.0,
                    "longitude": replica.geo_location.longitude if replica.geo_location else 0.0,
                    "country": replica.geo_location.country if replica.geo_location else "Unknown"
                },
                "load_score": replica.cpu_load + replica.memory_usage,
                "latency_ms": replica.avg_latency_ms
            }
            replicas_data.append(replica_data)
        
        await self.sidecar_manager.update_routing_table(replicas_data)
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        total_requests = self.sidecar_requests + self.fallback_requests
        sidecar_ratio = 0.0
        if total_requests > 0:
            sidecar_ratio = self.sidecar_requests / total_requests
        
        return {
            "total_requests": total_requests,
            "sidecar_requests": self.sidecar_requests,
            "fallback_requests": self.fallback_requests,
            "sidecar_usage_ratio": sidecar_ratio,
            "sidecar_metrics": self.sidecar_manager.get_metrics()
        }


# Example Rust sidecar configuration
SIDECAR_CONFIG_EXAMPLE = {
    "enabled": False,  # Set to True to enable Rust sidecar
    "sidecar_port": 19999,
    "socket_path": "/tmp/pyhmssql_geo_router.sock",
    "max_connections": 1000,
    "health_check_interval": 5,
    "routing_table_update_interval": 30
}


if __name__ == "__main__":
    # Example usage
    async def test_sidecar():
        config = SidecarConfig(**SIDECAR_CONFIG_EXAMPLE)
        manager = SidecarManager(config)
        
        print(f"Sidecar enabled: {config.enabled}")
        
        if config.enabled:
            if await manager.start():
                print("Sidecar started successfully")
                
                # Test routing
                result = await manager.route_request("8.8.8.8", "read")
                print(f"Routing result: {result}")
                
                # Get metrics
                metrics = manager.get_metrics()
                print(f"Metrics: {metrics}")
                
                await manager.stop()
            else:
                print("Failed to start sidecar")
        else:
            print("Sidecar not enabled in configuration")
    
    asyncio.run(test_sidecar())
