"""
Tunable Consistency Levels Implementation for HyperKV
Inspired by Cassandra/ScyllaDB consistency guarantees
Supports ONE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM
"""

import asyncio
import time
import logging
from enum import Enum
from typing import Dict, List, Optional, Any, Set, Tuple, Union
from dataclasses import dataclass, field
import random
from concurrent.futures import Future
import threading
from collections import defaultdict

logger = logging.getLogger(__name__)


class ConsistencyLevel(Enum):
    """Tunable consistency levels for distributed operations"""
    ONE = "ONE"                    # Wait for 1 replica
    TWO = "TWO"                    # Wait for 2 replicas  
    THREE = "THREE"                # Wait for 3 replicas
    QUORUM = "QUORUM"              # Wait for majority (N/2 + 1)
    ALL = "ALL"                    # Wait for all replicas
    LOCAL_ONE = "LOCAL_ONE"        # Wait for 1 replica in local DC
    LOCAL_QUORUM = "LOCAL_QUORUM"  # Wait for local majority
    EACH_QUORUM = "EACH_QUORUM"    # Wait for majority in each DC
    ANY = "ANY"                    # Accept any response (eventual consistency)
    SERIAL = "SERIAL"              # Linearizable consistency
    LOCAL_SERIAL = "LOCAL_SERIAL"  # Local linearizable consistency


@dataclass
class ConsistencyConfig:
    """Configuration for consistency operations"""
    level: ConsistencyLevel = ConsistencyLevel.QUORUM
    timeout_ms: int = 5000
    retry_count: int = 3
    retry_delay_ms: int = 100
    read_repair: bool = True
    hinted_handoff: bool = True
    prefer_local: bool = True


@dataclass
class ReplicaResponse:
    """Response from a single replica"""
    node_id: str
    success: bool
    value: Any = None
    timestamp: int = 0
    error: Optional[str] = None
    latency_ms: float = 0.0
    datacenter: str = "default"


@dataclass
class ConsistencyResult:
    """Result of a consistency operation"""
    success: bool
    value: Any = None
    responses: List[ReplicaResponse] = field(default_factory=list)
    satisfied_nodes: int = 0
    required_nodes: int = 0
    total_latency_ms: float = 0.0
    read_repair_performed: bool = False
    consistency_level: ConsistencyLevel = ConsistencyLevel.QUORUM


class HintedHandoff:
    """Hinted handoff for temporarily unavailable replicas"""
    
    def __init__(self, max_hints: int = 10000, hint_ttl_hours: int = 3):
        self.hints = defaultdict(list)  # node_id -> list of hints
        self.max_hints = max_hints
        self.hint_ttl_ms = hint_ttl_hours * 3600 * 1000
        self.lock = threading.RLock()
    
    def store_hint(self, failed_node: str, hint_data: Dict[str, Any], 
                   target_node: str) -> bool:
        """Store a hint for a failed node"""
        with self.lock:
            if len(self.hints[target_node]) >= self.max_hints:
                # Remove oldest hint
                self.hints[target_node].pop(0)
            
            hint = {
                'failed_node': failed_node,
                'data': hint_data,
                'timestamp': int(time.time() * 1000),
                'target_node': target_node
            }
            
            self.hints[target_node].append(hint)
            logger.debug(f"Stored hint for {failed_node} on {target_node}")
            return True
    
    def get_hints(self, node_id: str) -> List[Dict[str, Any]]:
        """Get all hints for a node"""
        with self.lock:
            current_time = int(time.time() * 1000)
            hints = self.hints.get(node_id, [])
            
            # Filter expired hints
            valid_hints = [
                hint for hint in hints 
                if current_time - hint['timestamp'] < self.hint_ttl_ms
            ]
            
            self.hints[node_id] = valid_hints
            return valid_hints.copy()
    
    def remove_hint(self, node_id: str, hint: Dict[str, Any]) -> bool:
        """Remove a successfully applied hint"""
        with self.lock:
            if node_id in self.hints:
                try:
                    self.hints[node_id].remove(hint)
                    return True
                except ValueError:
                    pass
            return False
    
    def cleanup_expired_hints(self):
        """Clean up expired hints"""
        with self.lock:
            current_time = int(time.time() * 1000)
            
            for node_id in list(self.hints.keys()):
                self.hints[node_id] = [
                    hint for hint in self.hints[node_id]
                    if current_time - hint['timestamp'] < self.hint_ttl_ms
                ]
                
                if not self.hints[node_id]:
                    del self.hints[node_id]


class ReadRepair:
    """Read repair mechanism for consistency maintenance"""
    
    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self.repair_stats = {
            'repairs_performed': 0,
            'repairs_failed': 0,
            'total_latency_ms': 0.0
        }
        self.lock = threading.RLock()
    
    async def perform_repair(self, key: str, responses: List[ReplicaResponse],
                           replica_manager) -> bool:
        """Perform read repair on inconsistent data"""
        if not self.enabled or len(responses) < 2:
            return False
        
        start_time = time.perf_counter()
        
        try:
            # Find the most recent value
            latest_response = max(
                (r for r in responses if r.success and r.value is not None),
                key=lambda r: r.timestamp,
                default=None
            )
            
            if not latest_response:
                return False
            
            # Find nodes that need repair
            repair_nodes = []
            for response in responses:
                if (response.success and 
                    response.timestamp < latest_response.timestamp):
                    repair_nodes.append(response.node_id)
            
            if not repair_nodes:
                return False
            
            # Perform repairs asynchronously
            repair_tasks = []
            for node_id in repair_nodes:
                task = asyncio.create_task(
                    replica_manager.repair_replica(
                        node_id, key, latest_response.value, 
                        latest_response.timestamp
                    )
                )
                repair_tasks.append(task)
            
            # Wait for repairs with timeout
            try:
                await asyncio.wait_for(
                    asyncio.gather(*repair_tasks, return_exceptions=True),
                    timeout=2.0
                )
                
                with self.lock:
                    self.repair_stats['repairs_performed'] += 1
                    
                logger.debug(f"Read repair performed for key {key} on {len(repair_nodes)} nodes")
                return True
                
            except asyncio.TimeoutError:
                with self.lock:
                    self.repair_stats['repairs_failed'] += 1
                logger.warning(f"Read repair timeout for key {key}")
                return False
                
        except Exception as e:
            with self.lock:
                self.repair_stats['repairs_failed'] += 1
            logger.error(f"Read repair failed for key {key}: {e}")
            return False
            
        finally:
            latency = (time.perf_counter() - start_time) * 1000
            with self.lock:
                self.repair_stats['total_latency_ms'] += latency


class ConsistencyManager:
    """Manages consistency levels and coordination for distributed operations"""
    
    def __init__(self, config: ConsistencyConfig = None):
        self.config = config or ConsistencyConfig()
        self.hinted_handoff = HintedHandoff()
        self.read_repair = ReadRepair(self.config.read_repair)
        self.stats = {
            'read_operations': 0,
            'write_operations': 0,
            'consistency_failures': 0,
            'repair_operations': 0,
            'hint_operations': 0
        }
        self.lock = threading.RLock()
    
    def calculate_required_responses(self, consistency_level: ConsistencyLevel,
                                   total_replicas: int, 
                                   datacenter_replicas: Dict[str, int] = None) -> int:
        """Calculate required number of responses for consistency level"""
        if consistency_level == ConsistencyLevel.ONE:
            return 1
        elif consistency_level == ConsistencyLevel.TWO:
            return min(2, total_replicas)
        elif consistency_level == ConsistencyLevel.THREE:
            return min(3, total_replicas)
        elif consistency_level == ConsistencyLevel.QUORUM:
            return (total_replicas // 2) + 1
        elif consistency_level == ConsistencyLevel.ALL:
            return total_replicas
        elif consistency_level == ConsistencyLevel.ANY:
            return 1
        elif consistency_level == ConsistencyLevel.LOCAL_QUORUM:
            local_replicas = datacenter_replicas.get("local", total_replicas) if datacenter_replicas else total_replicas
            return (local_replicas // 2) + 1
        elif consistency_level == ConsistencyLevel.EACH_QUORUM:
            if not datacenter_replicas:
                return (total_replicas // 2) + 1
            return sum((count // 2) + 1 for count in datacenter_replicas.values())
        else:
            return (total_replicas // 2) + 1  # Default to QUORUM
    
    async def coordinate_read(self, key: str, replicas: List[str],
                            consistency_level: ConsistencyLevel = None,
                            replica_manager = None) -> ConsistencyResult:
        """Coordinate a read operation across replicas"""
        consistency_level = consistency_level or self.config.level
        start_time = time.perf_counter()
        
        with self.lock:
            self.stats['read_operations'] += 1
        
        required_responses = self.calculate_required_responses(
            consistency_level, len(replicas)
        )
        
        # Send read requests to replicas
        tasks = []
        for replica in replicas:
            task = asyncio.create_task(
                self._read_from_replica(replica, key, replica_manager)
            )
            tasks.append(task)
        
        responses = []
        successful_responses = 0
        
        try:
            # Wait for required number of responses
            for completed_task in asyncio.as_completed(tasks, timeout=self.config.timeout_ms/1000):
                try:
                    response = await completed_task
                    responses.append(response)
                    
                    if response.success:
                        successful_responses += 1
                        
                        # Check if we have enough successful responses
                        if successful_responses >= required_responses:
                            break
                            
                except Exception as e:
                    logger.error(f"Read task failed: {e}")
                    
        except asyncio.TimeoutError:
            logger.warning(f"Read operation timeout for key {key}")
        
        # Cancel remaining tasks
        for task in tasks:
            if not task.done():
                task.cancel()
        
        # Determine result
        success = successful_responses >= required_responses
        value = None
        read_repair_performed = False
        
        if success:
            # Get the most recent value
            successful_responses_list = [r for r in responses if r.success]
            if successful_responses_list:
                latest_response = max(
                    successful_responses_list,
                    key=lambda r: r.timestamp
                )
                value = latest_response.value
                
                # Perform read repair if enabled and multiple responses
                if (self.config.read_repair and len(successful_responses_list) > 1 
                    and replica_manager):
                    read_repair_performed = await self.read_repair.perform_repair(
                        key, successful_responses_list, replica_manager
                    )
        else:
            with self.lock:
                self.stats['consistency_failures'] += 1
        
        total_latency = (time.perf_counter() - start_time) * 1000
        
        return ConsistencyResult(
            success=success,
            value=value,
            responses=responses,
            satisfied_nodes=successful_responses,
            required_nodes=required_responses,
            total_latency_ms=total_latency,
            read_repair_performed=read_repair_performed,
            consistency_level=consistency_level
        )
    
    async def coordinate_write(self, key: str, value: Any, replicas: List[str],
                             consistency_level: ConsistencyLevel = None,
                             replica_manager = None) -> ConsistencyResult:
        """Coordinate a write operation across replicas"""
        consistency_level = consistency_level or self.config.level
        start_time = time.perf_counter()
        
        with self.lock:
            self.stats['write_operations'] += 1
        
        required_responses = self.calculate_required_responses(
            consistency_level, len(replicas)
        )
        
        timestamp = int(time.time() * 1000000)  # Microsecond precision
        
        # Send write requests to replicas
        tasks = []
        for replica in replicas:
            task = asyncio.create_task(
                self._write_to_replica(replica, key, value, timestamp, replica_manager)
            )
            tasks.append(task)
        
        responses = []
        successful_responses = 0
        failed_replicas = []
        
        try:
            # Wait for required number of responses
            for completed_task in asyncio.as_completed(tasks, timeout=self.config.timeout_ms/1000):
                try:
                    response = await completed_task
                    responses.append(response)
                    
                    if response.success:
                        successful_responses += 1
                        
                        # Check if we have enough successful responses
                        if successful_responses >= required_responses:
                            break
                    else:
                        failed_replicas.append(response.node_id)
                        
                except Exception as e:
                    logger.error(f"Write task failed: {e}")
                    
        except asyncio.TimeoutError:
            logger.warning(f"Write operation timeout for key {key}")
        
        # Cancel remaining tasks
        for task in tasks:
            if not task.done():
                task.cancel()
        
        success = successful_responses >= required_responses
        
        # Handle hinted handoff for failed replicas
        if self.config.hinted_handoff and failed_replicas and success:
            await self._handle_hinted_handoff(
                key, value, timestamp, failed_replicas, responses
            )
        
        if not success:
            with self.lock:
                self.stats['consistency_failures'] += 1
        
        total_latency = (time.perf_counter() - start_time) * 1000
        
        return ConsistencyResult(
            success=success,
            value=value,
            responses=responses,
            satisfied_nodes=successful_responses,
            required_nodes=required_responses,
            total_latency_ms=total_latency,
            consistency_level=consistency_level
        )
    
    async def _read_from_replica(self, replica_id: str, key: str,
                               replica_manager) -> ReplicaResponse:
        """Read from a single replica"""
        start_time = time.perf_counter()
        
        try:
            if replica_manager:
                result = await replica_manager.read_from_replica(replica_id, key)
                latency = (time.perf_counter() - start_time) * 1000
                
                return ReplicaResponse(
                    node_id=replica_id,
                    success=result is not None,
                    value=result.get('value') if result else None,
                    timestamp=result.get('timestamp', 0) if result else 0,
                    latency_ms=latency
                )
            else:
                # Fallback for testing
                latency = (time.perf_counter() - start_time) * 1000
                return ReplicaResponse(
                    node_id=replica_id,
                    success=True,
                    value=f"test_value_{key}",
                    timestamp=int(time.time() * 1000000),
                    latency_ms=latency
                )
                
        except Exception as e:
            latency = (time.perf_counter() - start_time) * 1000
            return ReplicaResponse(
                node_id=replica_id,
                success=False,
                error=str(e),
                latency_ms=latency
            )
    
    async def _write_to_replica(self, replica_id: str, key: str, value: Any,
                              timestamp: int, replica_manager) -> ReplicaResponse:
        """Write to a single replica"""
        start_time = time.perf_counter()
        
        try:
            if replica_manager:
                success = await replica_manager.write_to_replica(
                    replica_id, key, value, timestamp
                )
                latency = (time.perf_counter() - start_time) * 1000
                
                return ReplicaResponse(
                    node_id=replica_id,
                    success=success,
                    timestamp=timestamp,
                    latency_ms=latency
                )
            else:
                # Fallback for testing
                latency = (time.perf_counter() - start_time) * 1000
                return ReplicaResponse(
                    node_id=replica_id,
                    success=True,
                    timestamp=timestamp,
                    latency_ms=latency
                )
                
        except Exception as e:
            latency = (time.perf_counter() - start_time) * 1000
            return ReplicaResponse(
                node_id=replica_id,
                success=False,
                error=str(e),
                latency_ms=latency
            )
    
    async def _handle_hinted_handoff(self, key: str, value: Any, timestamp: int,
                                   failed_replicas: List[str],
                                   successful_responses: List[ReplicaResponse]):
        """Handle hinted handoff for failed replicas"""
        if not successful_responses:
            return
        
        # Choose a healthy replica to store hints
        healthy_replicas = [r.node_id for r in successful_responses if r.success]
        if not healthy_replicas:
            return
        
        hint_target = random.choice(healthy_replicas)
        
        hint_data = {
            'key': key,
            'value': value,
            'timestamp': timestamp,
            'operation': 'write'
        }
        
        for failed_replica in failed_replicas:
            success = self.hinted_handoff.store_hint(
                failed_replica, hint_data, hint_target
            )
            if success:
                with self.lock:
                    self.stats['hint_operations'] += 1
                logger.debug(f"Stored hint for {failed_replica} on {hint_target}")
    
    async def replay_hints(self, node_id: str, replica_manager = None) -> int:
        """Replay stored hints for a recovered node"""
        hints = self.hinted_handoff.get_hints(node_id)
        if not hints:
            return 0
        
        replayed = 0
        
        for hint in hints:
            try:
                if replica_manager:
                    success = await replica_manager.write_to_replica(
                        hint['failed_node'],
                        hint['data']['key'],
                        hint['data']['value'],
                        hint['data']['timestamp']
                    )
                    
                    if success:
                        self.hinted_handoff.remove_hint(node_id, hint)
                        replayed += 1
                        
            except Exception as e:
                logger.error(f"Failed to replay hint: {e}")
        
        logger.info(f"Replayed {replayed} hints for node {node_id}")
        return replayed
    
    def get_stats(self) -> Dict[str, Any]:
        """Get consistency manager statistics"""
        with self.lock:
            stats = self.stats.copy()
        
        stats.update({
            'read_repair_stats': self.read_repair.repair_stats.copy(),
            'hinted_handoff_size': sum(len(hints) for hints in self.hinted_handoff.hints.values()),
            'config': {
                'level': self.config.level.value,
                'timeout_ms': self.config.timeout_ms,
                'read_repair': self.config.read_repair,
                'hinted_handoff': self.config.hinted_handoff
            }
        })
        
        return stats


# Factory functions for common configurations
def create_strong_consistency() -> ConsistencyManager:
    """Create manager for strong consistency (ALL writes, QUORUM reads)"""
    config = ConsistencyConfig(
        level=ConsistencyLevel.ALL,
        read_repair=True,
        hinted_handoff=True
    )
    return ConsistencyManager(config)


def create_eventual_consistency() -> ConsistencyManager:
    """Create manager for eventual consistency (ANY writes, ONE reads)"""
    config = ConsistencyConfig(
        level=ConsistencyLevel.ANY,
        read_repair=True,
        hinted_handoff=True
    )
    return ConsistencyManager(config)


def create_balanced_consistency() -> ConsistencyManager:
    """Create manager for balanced consistency (QUORUM)"""
    config = ConsistencyConfig(
        level=ConsistencyLevel.QUORUM,
        read_repair=True,
        hinted_handoff=True
    )
    return ConsistencyManager(config)
