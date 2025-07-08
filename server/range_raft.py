"""
Range-based Raft Groups for Distributed Database Operations

This module extends the existing Raft implementation to support per-range
consensus groups, enabling horizontal scaling through range partitioning
similar to CockroachDB's approach.
"""

import asyncio
import json
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple, Set
from enum import Enum
import hashlib

# Import existing Raft infrastructure
from server.raft_consensus import RaftNode, LogEntry, NodeState
try:
    from cycore.hlc_ts import HybridLogicalClock, HLCTimestamp
    from cycore.swiss_map import RangeMap, SwissMap
    HLC_AVAILABLE = True
except ImportError:
    # Fallback if Cython modules not compiled
    HLC_AVAILABLE = False
    print("Warning: HLC and SwissMap not available, using fallback implementations")

logger = logging.getLogger(__name__)

class RangeState(Enum):
    """State of a range in the cluster"""
    ACTIVE = "active"
    SPLITTING = "splitting" 
    MERGING = "merging"
    OFFLINE = "offline"
    RELOCATING = "relocating"

@dataclass
class RangeDescriptor:
    """Metadata for a key range"""
    range_id: str
    start_key: bytes
    end_key: bytes
    replicas: List[str]  # Node IDs hosting this range
    leader_node: Optional[str] = None
    state: RangeState = RangeState.ACTIVE
    generation: int = 1
    last_modified: float = field(default_factory=time.time)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'range_id': self.range_id,
            'start_key': self.start_key.hex(),
            'end_key': self.end_key.hex(),
            'replicas': self.replicas,
            'leader_node': self.leader_node,
            'state': self.state.value,
            'generation': self.generation,
            'last_modified': self.last_modified
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RangeDescriptor':
        """Create from dictionary"""
        return cls(
            range_id=data['range_id'],
            start_key=bytes.fromhex(data['start_key']),
            end_key=bytes.fromhex(data['end_key']),
            replicas=data['replicas'],
            leader_node=data.get('leader_node'),
            state=RangeState(data['state']),
            generation=data['generation'],
            last_modified=data['last_modified']
        )

class RangeRaftGroup:
    """
    A Raft group responsible for a specific key range.
    
    Each range has its own Raft log and state machine, enabling
    independent consensus and load distribution.
    """
    
    def __init__(self, range_desc: RangeDescriptor, node_id: str, 
                 peers: List[Tuple[str, str, int]]):
        """
        Initialize a range-specific Raft group.
        
        Args:
            range_desc: Range descriptor defining the key range
            node_id: This node's identifier 
            peers: List of peer nodes (id, host, port)
        """
        self.range_desc = range_desc
        self.node_id = node_id
        
        # Create Raft node for this range
        self.raft_node = RaftNode(
            node_id=f"{node_id}@{range_desc.range_id}",
            host="0.0.0.0",  # Will be overridden by routing
            port=0,  # Will be managed by range router
            peers=peers,
            election_timeout_min=3.0,  # Faster than cluster-wide Raft
            election_timeout_max=6.0,
            heartbeat_interval=1.0
        )
        
        # Range-specific state
        self.applied_operations: Dict[str, Any] = {}
        self.pending_splits: Set[bytes] = set()
        self.range_lock = threading.RLock()
        
        # HLC for operation ordering
        if HLC_AVAILABLE:
            self.hlc = HybridLogicalClock()
        else:
            self.hlc = None
            
        # Statistics
        self.stats = {
            'operations_applied': 0,
            'range_splits': 0,
            'leadership_changes': 0,
            'last_operation_time': 0
        }
        
        # Set up Raft callbacks
        self.raft_node.set_apply_callback(self._apply_range_operation)
        self.raft_node.set_leadership_callback(self._on_leadership_change)
        
        logger.info(f"Created range Raft group for {range_desc.range_id}")
    
    def _apply_range_operation(self, entry: LogEntry):
        """Apply a range operation to the state machine"""
        try:
            operation = entry.operation
            op_type = operation.get('type')
            
            with self.range_lock:
                if op_type == 'PUT':
                    self._apply_put(operation)
                elif op_type == 'DELETE':
                    self._apply_delete(operation)
                elif op_type == 'SPLIT_RANGE':
                    self._apply_range_split(operation)
                elif op_type == 'MERGE_RANGE':
                    self._apply_range_merge(operation)
                else:
                    logger.warning(f"Unknown operation type: {op_type}")
                
                self.stats['operations_applied'] += 1
                self.stats['last_operation_time'] = time.time()
                
        except Exception as e:
            logger.error(f"Failed to apply range operation: {e}")
    
    def _apply_put(self, operation: Dict[str, Any]):
        """Apply PUT operation to range"""
        key = operation['key'].encode() if isinstance(operation['key'], str) else operation['key']
        value = operation['value']
        
        # Verify key is in our range
        if not self._key_in_range(key):
            logger.error(f"Key {key.hex()} not in range {self.range_desc.range_id}")
            return
        
        # Add HLC timestamp if available
        if self.hlc:
            timestamp = self.hlc.now()
            operation['hlc_timestamp'] = {
                'physical': timestamp.physical,
                'logical': timestamp.logical
            }
        
        self.applied_operations[key.hex()] = {
            'value': value,
            'timestamp': time.time(),
            'hlc': operation.get('hlc_timestamp'),
            'operation_id': operation.get('operation_id')
        }
        
        logger.debug(f"Applied PUT {key.hex()[:16]}... to range {self.range_desc.range_id}")
    
    def _apply_delete(self, operation: Dict[str, Any]):
        """Apply DELETE operation to range"""
        key = operation['key'].encode() if isinstance(operation['key'], str) else operation['key']
        
        if not self._key_in_range(key):
            logger.error(f"Key {key.hex()} not in range {self.range_desc.range_id}")
            return
        
        key_hex = key.hex()
        if key_hex in self.applied_operations:
            del self.applied_operations[key_hex]
            logger.debug(f"Applied DELETE {key_hex[:16]}... from range {self.range_desc.range_id}")
    
    def _apply_range_split(self, operation: Dict[str, Any]):
        """Apply range split operation"""
        split_key = operation['split_key'].encode() if isinstance(operation['split_key'], str) else operation['split_key']
        
        if not self._key_in_range(split_key):
            logger.error(f"Split key {split_key.hex()} not in range {self.range_desc.range_id}")
            return
        
        self.pending_splits.add(split_key)
        self.stats['range_splits'] += 1
        
        logger.info(f"Scheduled range split at {split_key.hex()[:16]}... for range {self.range_desc.range_id}")
    
    def _apply_range_merge(self, operation: Dict[str, Any]):
        """Apply range merge operation"""
        # Implementation for range merging
        logger.info(f"Applied range merge for range {self.range_desc.range_id}")
    
    def _key_in_range(self, key: bytes) -> bool:
        """Check if key falls within this range"""
        return self.range_desc.start_key <= key < self.range_desc.end_key
    
    def _on_leadership_change(self, is_leader: bool):
        """Handle leadership changes for this range"""
        with self.range_lock:
            if is_leader:
                self.range_desc.leader_node = self.node_id
                logger.info(f"Became leader for range {self.range_desc.range_id}")
            else:
                self.range_desc.leader_node = None
                logger.info(f"Lost leadership for range {self.range_desc.range_id}")
            
            self.stats['leadership_changes'] += 1
    
    def propose_operation(self, operation: Dict[str, Any]) -> bool:
        """
        Propose an operation to the range Raft group.
        
        Args:
            operation: Operation to propose
            
        Returns:
            bool: True if operation was accepted
        """
        if self.raft_node.state != NodeState.LEADER:
            return False
        
        # Add metadata
        operation['range_id'] = self.range_desc.range_id
        operation['timestamp'] = time.time()
        
        if self.hlc:
            hlc_ts = self.hlc.now()
            operation['hlc_timestamp'] = {
                'physical': hlc_ts.physical,
                'logical': hlc_ts.logical
            }
        
        return self.raft_node.add_log_entry(operation)
    
    def get_value(self, key: bytes) -> Optional[Any]:
        """Get value for a key from this range"""
        if not self._key_in_range(key):
            return None
        
        with self.range_lock:
            key_hex = key.hex()
            if key_hex in self.applied_operations:
                return self.applied_operations[key_hex]['value']
        
        return None
    
    def is_leader(self) -> bool:
        """Check if this node is leader for the range"""
        return self.raft_node.state == NodeState.LEADER
    
    def get_status(self) -> Dict[str, Any]:
        """Get range status"""
        with self.range_lock:
            return {
                'range_id': self.range_desc.range_id,
                'state': self.range_desc.state.value,
                'is_leader': self.is_leader(),
                'raft_state': self.raft_node.state.value,
                'raft_term': self.raft_node.current_term,
                'num_operations': len(self.applied_operations),
                'pending_splits': len(self.pending_splits),
                'stats': self.stats.copy()
            }
    
    def shutdown(self):
        """Shutdown the range Raft group"""
        logger.info(f"Shutting down range Raft group {self.range_desc.range_id}")
        self.raft_node.shutdown()

class RangeRouter:
    """
    Routes operations to appropriate range Raft groups and manages range lifecycle.
    
    Maintains routing metadata and handles range splits/merges.
    """
    
    def __init__(self, node_id: str, initial_ranges: List[RangeDescriptor] = None):
        """
        Initialize range router.
        
        Args:
            node_id: This node's identifier
            initial_ranges: Initial range descriptors (if None, creates default range)
        """
        self.node_id = node_id
        self.range_groups: Dict[str, RangeRaftGroup] = {}
        self.routing_lock = threading.RLock()
        
        # Range metadata storage
        if HLC_AVAILABLE:
            self.range_metadata = RangeMap()
            self.routing_cache = SwissMap()
        else:
            self.range_metadata = {}
            self.routing_cache = {}
        
        # Create initial ranges
        if initial_ranges:
            for range_desc in initial_ranges:
                self._create_range_group(range_desc)
        else:
            # Create default range covering entire keyspace
            default_range = RangeDescriptor(
                range_id="range_0",
                start_key=b'\x00',
                end_key=b'\xff' * 32,  # Large end key
                replicas=[node_id],
                leader_node=node_id
            )
            self._create_range_group(default_range)
        
        logger.info(f"Range router initialized with {len(self.range_groups)} ranges")
    
    def _create_range_group(self, range_desc: RangeDescriptor, peers: List[Tuple[str, str, int]] = None):
        """Create a new range Raft group"""
        if peers is None:
            peers = []  # Solo range for now
        
        range_group = RangeRaftGroup(range_desc, self.node_id, peers)
        
        with self.routing_lock:
            self.range_groups[range_desc.range_id] = range_group
            
            # Update routing metadata
            range_key = int.from_bytes(range_desc.start_key[:8].ljust(8, b'\x00'), 'big')
            if HLC_AVAILABLE:
                self.range_metadata[range_key] = json.dumps(range_desc.to_dict())
            else:
                self.range_metadata[range_key] = range_desc.to_dict()
    
    def find_range_for_key(self, key: bytes) -> Optional[RangeRaftGroup]:
        """Find the range group that should handle the given key"""
        with self.routing_lock:
            # Check cache first
            cache_key = key.hex()[:16]  # Use prefix for caching
            if HLC_AVAILABLE:
                cached_range_id = self.routing_cache.get(cache_key)
            else:
                cached_range_id = self.routing_cache.get(cache_key)
            
            if cached_range_id and cached_range_id in self.range_groups:
                range_group = self.range_groups[cached_range_id]
                if range_group._key_in_range(key):
                    return range_group
            
            # Linear search through ranges (TODO: optimize with interval tree)
            for range_group in self.range_groups.values():
                if range_group._key_in_range(key):
                    # Update cache
                    if HLC_AVAILABLE:
                        self.routing_cache[cache_key] = range_group.range_desc.range_id
                    else:
                        self.routing_cache[cache_key] = range_group.range_desc.range_id
                    return range_group
        
        return None
    
    def propose_operation(self, key: bytes, operation: Dict[str, Any]) -> bool:
        """
        Propose an operation for a specific key.
        
        Args:
            key: Key to operate on
            operation: Operation to propose
            
        Returns:
            bool: True if operation was proposed successfully
        """
        range_group = self.find_range_for_key(key)
        if not range_group:
            logger.error(f"No range found for key {key.hex()[:16]}...")
            return False
        
        if not range_group.is_leader():
            logger.debug(f"Not leader for range {range_group.range_desc.range_id}")
            return False
        
        # Add key to operation
        operation['key'] = key
        return range_group.propose_operation(operation)
    
    def get_value(self, key: bytes) -> Optional[Any]:
        """Get value for a key"""
        range_group = self.find_range_for_key(key)
        if not range_group:
            return None
        
        return range_group.get_value(key)
    
    def put(self, key: bytes, value: Any) -> bool:
        """Put a key-value pair"""
        operation = {
            'type': 'PUT',
            'value': value,
            'operation_id': self._generate_operation_id()
        }
        return self.propose_operation(key, operation)
    
    def delete(self, key: bytes) -> bool:
        """Delete a key"""
        operation = {
            'type': 'DELETE',
            'operation_id': self._generate_operation_id()
        }
        return self.propose_operation(key, operation)
    
    def split_range(self, range_id: str, split_key: bytes) -> bool:
        """Initiate a range split"""
        with self.routing_lock:
            if range_id not in self.range_groups:
                return False
            
            range_group = self.range_groups[range_id]
            if not range_group.is_leader():
                return False
            
            operation = {
                'type': 'SPLIT_RANGE',
                'split_key': split_key,
                'operation_id': self._generate_operation_id()
            }
            
            return range_group.propose_operation(operation)
    
    def add_range(self, start_key: int, end_key: int, replicas: List[str], leader: str):
        """
        Add a new range to the router.
        
        Args:
            start_key: Range start key (integer)
            end_key: Range end key (integer or float('inf'))
            replicas: List of replica node IDs
            leader: Leader node ID
        """
        # Convert integer keys to bytes
        if isinstance(start_key, int):
            start_key_bytes = start_key.to_bytes(8, 'big')
        else:
            start_key_bytes = start_key
            
        if end_key == float('inf'):
            end_key_bytes = b'\xff' * 32
        elif isinstance(end_key, int):
            end_key_bytes = end_key.to_bytes(8, 'big')
        else:
            end_key_bytes = end_key
        
        # Create range descriptor
        range_id = f"range_{start_key}_{end_key}"
        range_desc = RangeDescriptor(
            range_id=range_id,
            start_key=start_key_bytes,
            end_key=end_key_bytes,
            replicas=replicas,
            leader_node=leader
        )
        
        # Create the range group
        self._create_range_group(range_desc)
        logger.info(f"Added range {range_id} covering {start_key}-{end_key}")

    def route_operation(self, key: int, operation_type: str) -> Dict[str, Any]:
        """
        Route an operation to the appropriate range.
        
        Args:
            key: Key to route (integer)
            operation_type: Type of operation (GET, PUT, DELETE, etc.)
            
        Returns:
            Dict containing routing information
        """
        # Convert key to bytes
        if isinstance(key, int):
            key_bytes = key.to_bytes(8, 'big')
        else:
            key_bytes = key
        
        # Find the range for this key
        range_group = self.find_range_for_key(key_bytes)
        if not range_group:
            return {
                'error': f'No range found for key {key}',
                'key': key
            }
        
        return {
            'range_id': range_group.range_desc.range_id,
            'leader': range_group.range_desc.leader_node,
            'replicas': range_group.range_desc.replicas,
            'operation_type': operation_type,
            'key': key
        }
    
    def _generate_operation_id(self) -> str:
        """Generate unique operation ID"""
        return f"{self.node_id}_{int(time.time() * 1000000)}"
    
    def get_range_status(self) -> List[Dict[str, Any]]:
        """Get status of all ranges"""
        with self.routing_lock:
            return [rg.get_status() for rg in self.range_groups.values()]
    
    def get_routing_info(self) -> Dict[str, Any]:
        """Get routing information for debugging"""
        with self.routing_lock:
            return {
                'total_ranges': len(self.range_groups),
                'ranges': {
                    range_id: {
                        'start_key': rg.range_desc.start_key.hex(),
                        'end_key': rg.range_desc.end_key.hex(),
                        'is_leader': rg.is_leader(),
                        'num_operations': len(rg.applied_operations)
                    }
                    for range_id, rg in self.range_groups.items()
                },
                'cache_size': len(self.routing_cache) if HLC_AVAILABLE else len(self.routing_cache)
            }
    
    def shutdown(self):
        """Shutdown all range groups"""
        logger.info("Shutting down range router...")
        with self.routing_lock:
            for range_group in self.range_groups.values():
                range_group.shutdown()
            self.range_groups.clear()

# Example usage and testing
if __name__ == "__main__":
    import asyncio
    
    async def test_range_router():
        """Test the range router functionality"""
        print("Testing Range Router...")
        
        # Create router with default range
        router = RangeRouter("test_node_1")
        
        # Test basic operations
        test_key = b"test_key_12345"
        
        print(f"Putting value for key {test_key.hex()}")
        success = router.put(test_key, "test_value")
        print(f"Put success: {success}")
        
        # Give time for Raft consensus
        await asyncio.sleep(2)
        
        print(f"Getting value for key {test_key.hex()}")
        value = router.get_value(test_key)
        print(f"Retrieved value: {value}")
        
        # Print status
        status = router.get_range_status()
        print(f"Range status: {status}")
        
        # Cleanup
        router.shutdown()
    
    asyncio.run(test_range_router())
