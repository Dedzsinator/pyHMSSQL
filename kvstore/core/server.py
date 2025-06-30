"""
HyperKV Server - Main orchestration class
Integrates all components into a high-performance, distributed key-value store.
"""

import asyncio
import signal
import time
import logging
import threading
from typing import Dict, Any, Optional, List, Set, Union, Callable
from contextlib import asynccontextmanager
import json
import psutil
import os

from .config import HyperKVServerConfig
from .ttl import TTLManager
from .cache import CacheManager, EvictionPolicy
from ..crdt import CRDTValue, create_crdt, VectorClock, HybridLogicalClock
from ..raft import RaftNode, RaftConfig
from ..storage import StorageEngine, AOFWriter, SnapshotManager
from ..pubsub import PubSubManager
from ..networking import RedisProtocolHandler, TcpServer
from ..shards import AdvancedShardManager, ShardConfig, PlacementStrategy

logger = logging.getLogger(__name__)

class HyperKVServer:
    """
    Main HyperKV Server class that orchestrates all components.
    
    Features:
    - CRDT-compliant distributed key-value operations
    - TTL management with active/passive expiration
    - Multiple cache eviction policies
    - Raft consensus for distributed operation
    - Redis protocol compatibility
    - Pub/Sub messaging
    - Persistence with AOF and snapshots
    - High-performance networking
    - Comprehensive monitoring and metrics
    """
    
    def __init__(self, config: Optional[HyperKVServerConfig] = None):
        self.config = config or HyperKVServerConfig()
        
        # Server state
        self._running = False
        self._shutdown_event = asyncio.Event()
        
        # Core data storage
        self._data: Dict[str, CRDTValue] = {}
        self._data_lock = threading.RLock()
        
        # Vector/Hybrid Logical Clock for CRDT operations
        self._vector_clock = VectorClock(self.config.node_id)
        self._hlc = HybridLogicalClock(self.config.node_id)
        
        # Advanced Shard Manager for distributed storage
        self.shard_manager = None
        if getattr(self.config, 'enable_sharding', True):
            shard_config = {
                'num_shards': getattr(self.config, 'num_shards', 4),
                'placement_strategy': getattr(self.config, 'placement_strategy', PlacementStrategy.NUMA_AWARE.value),
                'enable_consistency': getattr(self.config, 'enable_consistency', True),
                'enable_compression': getattr(self.config, 'enable_compression', True),
                'enable_wal': getattr(self.config, 'enable_wal', True),
                'enable_zero_copy': getattr(self.config, 'enable_zero_copy', True),
                'memory_per_shard': self.config.max_memory // getattr(self.config, 'num_shards', 4)
            }
            self.shard_manager = AdvancedShardManager(shard_config)
        
        # Initialize components
        self._init_components()
        
        # Background tasks
        self._background_tasks: List[asyncio.Task] = []
        
        # Statistics and monitoring
        self.stats = {
            'start_time': time.time(),
            'total_operations': 0,
            'get_operations': 0,
            'set_operations': 0,
            'del_operations': 0,
            'errors': 0,
            'memory_usage': 0,
            'active_connections': 0
        }
        
        logger.info(f"HyperKV Server initialized (Node: {self.config.node_id})")
    
    def _init_components(self):
        """Initialize all server components"""
        
        # TTL Manager
        self.ttl_manager = TTLManager(
            check_interval=self.config.ttl_check_interval,
            max_keys_per_check=self.config.max_keys_per_ttl_check,
            on_expire_callback=self._on_key_expired
        )
        
        # Cache Manager
        self.cache_manager = CacheManager(
            eviction_policy=EvictionPolicy(self.config.eviction_policy),
            max_memory=self.config.max_memory,
            memory_threshold=self.config.memory_usage_threshold,
            eviction_batch_size=self.config.eviction_batch_size,
            sample_size=self.config.eviction_sample_size
        )
        
        # Storage Engine (handles all persistence)
        storage_config = {
            'backend': self.config.storage_backend,
            'data_dir': self.config.data_dir,
            'aof_enabled': self.config.aof_enabled,
            'aof_fsync_policy': self.config.aof_fsync_policy,
            'snapshot_enabled': self.config.snapshot_enabled,
            'snapshot_compression': True
        }
        self.storage = StorageEngine(storage_config)
        
        # Pub/Sub Manager
        pubsub_config = {
            'max_channels': 100000,
            'max_subscribers_per_channel': 1000,
            'message_buffer_size': 1000
        }
        self.pubsub = PubSubManager(pubsub_config)
        
        # Raft Node (if clustering enabled)
        self.raft_node = None
        if self.config.enable_clustering:
            from ..raft import RaftConfig
            raft_config = RaftConfig()
            # Get peers from cluster configuration
            peers = getattr(self.config.cluster, 'peers', [])
            self.raft_node = RaftNode(
                node_id=self.config.node_id,
                peers=peers,
                config=raft_config
            )
        
        # Protocol Handler
        self.protocol_handler = RedisProtocolHandler(self)
        
        # TCP Server
        tcp_config = {
            'max_connections': self.config.max_connections
        }
        self.tcp_server = TcpServer(
            host=self.config.host,
            port=self.config.port,
            handler=self.protocol_handler,
            config=tcp_config
        )
        
        logger.info("All components initialized successfully")
    
    async def start(self):
        """Start the HyperKV server"""
        if self._running:
            logger.warning("Server is already running")
            return
        
        logger.info("Starting HyperKV Server...")
        
        try:
            # Load persisted data
            await self._load_persisted_data()
            
            # Start TTL manager
            self.ttl_manager.start()
            
            # Start advanced shard manager
            if self.shard_manager:
                self.shard_manager.start()
                logger.info("Advanced shard manager started")
            
            # Start Raft node if clustering enabled
            if self.raft_node:
                await self.raft_node.start()
            
            # Start TCP server
            await self.tcp_server.start()
            
            # Start background tasks
            self._start_background_tasks()
            
            # Set up signal handlers
            self._setup_signal_handlers()
            
            self._running = True
            self.stats['start_time'] = time.time()
            
            logger.info(f"HyperKV Server started on {self.config.host}:{self.config.port}")
            
        except Exception as e:
            logger.error(f"Failed to start server: {e}")
            await self.stop()
            raise
    
    async def stop(self):
        """Stop the HyperKV server"""
        if not self._running:
            return
        
        logger.info("Stopping HyperKV Server...")
        
        self._running = False
        self._shutdown_event.set()
        
        try:
            # Stop background tasks
            for task in self._background_tasks:
                task.cancel()
            
            if self._background_tasks:
                await asyncio.gather(*self._background_tasks, return_exceptions=True)
            
            # Stop TCP server
            await self.tcp_server.stop()
            
            # Stop Raft node
            if self.raft_node:
                await self.raft_node.stop()
            
            # Stop shard manager
            if self.shard_manager:
                self.shard_manager.stop()
                logger.info("Advanced shard manager stopped")
            
            # Stop TTL manager
            await self.ttl_manager.stop()
            
            # Final snapshot
            await self._create_snapshot()
            
            # Close storage
            await self.storage.close()
            
            logger.info("HyperKV Server stopped successfully")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    
    def _start_background_tasks(self):
        """Start background monitoring and maintenance tasks"""
        self._background_tasks = [
            asyncio.create_task(self._stats_monitor()),
            asyncio.create_task(self._memory_monitor()),
            asyncio.create_task(self._snapshot_scheduler()),
            asyncio.create_task(self._cleanup_task())
        ]
        
        logger.info("Background tasks started")
    
    def _setup_signal_handlers(self):
        """Set up signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            asyncio.create_task(self.stop())
        
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
    
    # Core KV Operations
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Get a value by key with CRDT and TTL support.
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value or None if not found/expired
        """
        # Use shard manager if available for distributed storage
        if self.shard_manager:
            try:
                return await self.shard_manager.execute_on_shard(
                    key, lambda shard, k=key: shard.get(k)
                )
            except Exception as e:
                logger.error(f"Shard operation failed for key '{key}': {e}")
                # Fall back to local storage
        
        # Check TTL first (passive expiration)
        if self.ttl_manager.is_expired(key):
            await self._delete_key(key)
            self.stats['get_operations'] += 1
            self.stats['total_operations'] += 1
            return None
        
        # Try cache first
        cached_value = self.cache_manager.get(key)
        if cached_value is not None:
            self.stats['get_operations'] += 1
            self.stats['total_operations'] += 1
            return cached_value
        
        # Try storage
        with self._data_lock:
            if key in self._data:
                crdt_value = self._data[key]
                value = crdt_value.get_value()
                
                # Update cache
                has_ttl = self.ttl_manager.get_ttl(key) is not None
                self.cache_manager.put(key, value, has_ttl)
                
                self.stats['get_operations'] += 1
                self.stats['total_operations'] += 1
                return value
        
        # Try persistent storage
        try:
            stored_data = await self.storage.get(key)
            if stored_data:
                # Deserialize CRDT value
                crdt_value = self._deserialize_crdt(stored_data)
                if crdt_value:
                    value = crdt_value.get_value()
                    
                    # Load into memory
                    with self._data_lock:
                        self._data[key] = crdt_value
                    
                    # Update cache
                    has_ttl = self.ttl_manager.get_ttl(key) is not None
                    self.cache_manager.put(key, value, has_ttl)
                    
                    self.stats['get_operations'] += 1
                    self.stats['total_operations'] += 1
                    return value
        except Exception as e:
            logger.error(f"Error retrieving key '{key}' from storage: {e}")
            self.stats['errors'] += 1
        
        self.stats['get_operations'] += 1
        self.stats['total_operations'] += 1
        return None
    
    async def set(self, key: str, value: Any, ttl: Optional[float] = None, 
                  crdt_type: str = "lww") -> bool:
        """
        Set a key-value pair with CRDT support.
        
        Args:
            key: The key to set
            value: The value to set
            ttl: Time-to-live in seconds (optional)
            crdt_type: CRDT type to use (lww, orset, counter)
            
        Returns:
            True if successful
        """
        # Use shard manager if available for distributed storage
        if self.shard_manager:
            try:
                success = await self.shard_manager.execute_on_shard(
                    key, lambda shard, k=key, v=value: shard.set(k, v)
                )
                if success and ttl is not None:
                    self.ttl_manager.set_ttl(key, ttl)
                self.stats['set_operations'] += 1
                self.stats['total_operations'] += 1
                return success
            except Exception as e:
                logger.error(f"Shard operation failed for key '{key}': {e}")
                # Fall back to local storage
        
        try:
            # Update logical clocks
            self._vector_clock.tick()
            self._hlc.tick()
            
            # Create CRDT value
            crdt_value = create_crdt(
                crdt_type, 
                value, 
                node_id=self.config.node_id,
                timestamp=self._hlc.logical_time
            )
            
            # Store in memory
            with self._data_lock:
                self._data[key] = crdt_value
            
            # Update cache (store the CRDT value, not the original value)
            self.cache_manager.put(key, crdt_value, ttl is not None)
            
            # Set TTL if specified
            if ttl is not None:
                self.ttl_manager.set_ttl(key, ttl)
            
            # Persist to storage
            await self._persist_key(key, crdt_value)
            
            # Write to AOF
            await self._write_aof_command("SET", key, value, ttl)
            
            # Replicate if clustering enabled
            if self.raft_node and self.raft_node.is_leader():
                await self._replicate_command("SET", key, value, ttl)
            
            # Publish to subscribers
            await self.pubsub.publish(f"__keyspace@0__:{key}", "set")
            
            self.stats['set_operations'] += 1
            self.stats['total_operations'] += 1
            
            logger.debug(f"Set key '{key}' with CRDT type '{crdt_type}'")
            return True
            
        except Exception as e:
            logger.error(f"Error setting key '{key}': {e}")
            self.stats['errors'] += 1
            return False
    
    async def delete(self, key: str) -> bool:
        """
        Delete a key.
        
        Args:
            key: The key to delete
            
        Returns:
            True if key was deleted
        """
        # Use shard manager if available for distributed storage
        if self.shard_manager:
            try:
                existed = await self.shard_manager.execute_on_shard(
                    key, lambda shard, k=key: shard.delete(k)
                )
                if existed:
                    self.ttl_manager.remove_ttl(key)
                    self.stats['del_operations'] += 1
                    self.stats['total_operations'] += 1
                return existed
            except Exception as e:
                logger.error(f"Shard operation failed for key '{key}': {e}")
                # Fall back to local storage
        
        try:
            existed = await self._delete_key(key)
            
            if existed:
                # Write to AOF
                await self._write_aof_command("DEL", key)
                
                # Replicate if clustering enabled
                if self.raft_node and self.raft_node.is_leader():
                    await self._replicate_command("DEL", key)
                
                # Publish to subscribers
                await self.pubsub.publish(f"__keyspace@0__:{key}", "del")
                
                self.stats['del_operations'] += 1
                self.stats['total_operations'] += 1
            
            return existed
            
        except Exception as e:
            logger.error(f"Error deleting key '{key}': {e}")
            self.stats['errors'] += 1
            return False
    
    async def _delete_key(self, key: str) -> bool:
        """Internal method to delete a key from all storage layers"""
        existed = False
        
        # Remove from memory
        with self._data_lock:
            if key in self._data:
                del self._data[key]
                existed = True
        
        # Remove from cache
        if self.cache_manager.delete(key):
            existed = True
        
        # Remove TTL
        if self.ttl_manager.remove_ttl(key):
            existed = True
        
        # Remove from persistent storage
        try:
            if await self.storage.delete(key):
                existed = True
        except Exception as e:
            logger.error(f"Error deleting key '{key}' from storage: {e}")
        
        return existed
    
    async def exists(self, key: str) -> bool:
        """Check if a key exists"""
        # Check TTL first
        if self.ttl_manager.is_expired(key):
            await self._delete_key(key)
            return False
        
        # Check memory
        with self._data_lock:
            if key in self._data:
                return True
        
        # Check persistent storage
        try:
            return await self.storage.exists(key)
        except Exception as e:
            logger.error(f"Error checking existence of key '{key}': {e}")
            return False
    
    async def scan(self, cursor: int = 0, pattern: str = "*", 
                   count: int = 10) -> tuple[int, List[str]]:
        """Scan keys with pattern matching"""
        try:
            # Get keys from memory
            with self._data_lock:
                memory_keys = list(self._data.keys())
            
            # Get keys from storage
            storage_keys = await self.storage.scan(cursor, pattern, count)
            
            # Combine and deduplicate
            all_keys = list(set(memory_keys + storage_keys[1]))
            
            # Apply pattern matching (simple implementation)
            if pattern != "*":
                import fnmatch
                all_keys = [k for k in all_keys if fnmatch.fnmatch(k, pattern)]
            
            # Remove expired keys
            valid_keys = []
            for key in all_keys:
                if not self.ttl_manager.is_expired(key):
                    valid_keys.append(key)
                else:
                    await self._delete_key(key)
            
            # Paginate
            start_idx = min(cursor, len(valid_keys))
            end_idx = min(start_idx + count, len(valid_keys))
            page_keys = valid_keys[start_idx:end_idx]
            
            next_cursor = end_idx if end_idx < len(valid_keys) else 0
            
            return (next_cursor, page_keys)
            
        except Exception as e:
            logger.error(f"Error during scan: {e}")
            return (0, [])
    
    # TTL Operations
    
    async def expire(self, key: str, ttl: float) -> bool:
        """Set TTL for an existing key"""
        if not await self.exists(key):
            return False
        
        success = self.ttl_manager.set_ttl(key, ttl)
        if success:
            # Update cache volatile status
            value = await self.get(key)
            if value is not None:
                self.cache_manager.put(key, value, True)
            
            # Write to AOF
            await self._write_aof_command("EXPIRE", key, ttl)
        
        return success
    
    async def ttl(self, key: str) -> Optional[float]:
        """Get TTL for a key"""
        if not await self.exists(key):
            return None
        
        return self.ttl_manager.get_ttl(key)
    
    async def persist(self, key: str) -> bool:
        """Remove TTL from a key"""
        if not await self.exists(key):
            return False
        
        success = self.ttl_manager.remove_ttl(key)
        if success:
            # Update cache volatile status
            value = await self.get(key)
            if value is not None:
                self.cache_manager.put(key, value, False)
            
            # Write to AOF
            await self._write_aof_command("PERSIST", key)
        
        return success
    
    # Pub/Sub Operations
    
    async def publish(self, channel: str, message: Any) -> int:
        """Publish a message to a channel"""
        return await self.pubsub.publish(channel, message)
    
    async def subscribe(self, client_id: str, channels: List[str], 
                       callback: Callable) -> bool:
        """Subscribe to channels"""
        return await self.pubsub.subscribe(client_id, channels, callback)
    
    async def unsubscribe(self, client_id: str, channels: List[str] = None) -> bool:
        """Unsubscribe from channels"""
        return await self.pubsub.unsubscribe(client_id, channels)
    
    # CRDT Operations
    
    async def crdt_add(self, key: str, item: Any) -> bool:
        """Add an item to a CRDT set"""
        try:
            # Get or create LWW set
            existing_value = await self.get(key)
            if existing_value is None:
                # Create new LWW set
                crdt_set = create_crdt("lww_set", None, self.config.node_id)
            elif hasattr(existing_value, 'add'):
                crdt_set = existing_value
            else:
                # Convert existing value to CRDT set
                crdt_set = create_crdt("lww_set", None, self.config.node_id)
            
            # Add the item
            crdt_set.add(item)
            
            # Store back
            await self.set(key, crdt_set)
            
            # Write to AOF
            await self._write_aof_command("CRDT_ADD", key, item)
            
            return True
        except Exception as e:
            logger.error(f"Error in crdt_add: {e}")
            return False
    
    async def crdt_contains(self, key: str, item: Any) -> bool:
        """Check if an item is in a CRDT set"""
        try:
            value = await self.get(key)
            if value is None or not hasattr(value, 'contains'):
                return False
            return value.contains(item)
        except Exception as e:
            logger.error(f"Error in crdt_contains: {e}")
            return False
    
    async def crdt_increment(self, key: str, amount: int = 1) -> bool:
        """Increment a CRDT counter"""
        try:
            # Get or create counter
            existing_value = await self.get(key)
            if existing_value is None:
                # Create new counter
                counter = create_crdt("counter", 0, self.config.node_id)
            elif hasattr(existing_value, 'increment'):
                counter = existing_value
            else:
                # Convert existing value to CRDT counter
                counter = create_crdt("counter", 0, self.config.node_id)
            
            # Increment the counter
            counter.increment(amount)
            
            # Store back
            await self.set(key, counter)
            
            # Write to AOF
            await self._write_aof_command("CRDT_INCREMENT", key, amount)
            
            return True
        except Exception as e:
            logger.error(f"Error in crdt_increment: {e}")
            return False
    
    async def crdt_decrement(self, key: str, amount: int = 1) -> bool:
        """Decrement a CRDT counter"""
        try:
            # Get or create counter
            existing_value = await self.get(key)
            if existing_value is None:
                # Create new counter
                counter = create_crdt("counter", 0, self.config.node_id)
            elif hasattr(existing_value, 'decrement'):
                counter = existing_value
            else:
                # Convert existing value to CRDT counter
                counter = create_crdt("counter", 0, self.config.node_id)
            
            # Decrement the counter
            counter.decrement(amount)
            
            # Store back
            await self.set(key, counter)
            
            # Write to AOF
            await self._write_aof_command("CRDT_DECREMENT", key, amount)
            
            return True
        except Exception as e:
            logger.error(f"Error in crdt_decrement: {e}")
            return False
    
    async def crdt_value(self, key: str) -> Any:
        """Get the current value of a CRDT"""
        try:
            crdt = await self.get(key)
            if crdt is None:
                return None
            if hasattr(crdt, 'value'):
                return crdt.value()
            return crdt
        except Exception as e:
            logger.error(f"Error in crdt_value: {e}")
            return None
    
    # Persistence and Recovery
    
    async def _persist_key(self, key: str, crdt_value: CRDTValue):
        """Persist a key to storage"""
        try:
            serialized = self._serialize_crdt(crdt_value)
            if serialized:
                # Use a safer JSON serialization that handles complex objects
                serialized_bytes = self._safe_json_encode(serialized).encode('utf-8')
                await self.storage.set(key, serialized_bytes)
        except Exception as e:
            logger.error(f"Error persisting key '{key}': {e}")
    
    def _safe_json_encode(self, obj: Any) -> str:
        """Safely encode object to JSON, handling complex types"""
        def json_serializer(obj):
            """Custom JSON serializer for complex objects"""
            if hasattr(obj, '__dict__'):
                return obj.__dict__
            elif hasattr(obj, 'to_dict'):
                return obj.to_dict()
            elif isinstance(obj, set):
                return list(obj)
            else:
                # Convert to string as fallback
                return str(obj)
        
        try:
            return json.dumps(obj, default=json_serializer)
        except Exception:
            # Final fallback - convert to string representation
            return json.dumps(str(obj))
    
    async def _write_aof_command(self, command: str, *args):
        """Write a command to the AOF (handled by storage engine)"""
        # The storage engine's AOF writer handles this automatically
        # when we call storage.set(), storage.delete(), etc.
        pass
    
    async def _load_persisted_data(self):
        """Load data from persistent storage on startup"""
        logger.info("Loading persisted data...")
        
        try:
            # Initialize storage engine (handles snapshot and AOF loading)
            await self.storage.initialize()
            
            # Load CRDT data from storage into memory
            keys = await self.storage.keys()
            loaded_count = 0
            
            for key in keys:
                try:
                    value_bytes = await self.storage.get(key)
                    if value_bytes:
                        # Try to deserialize as CRDT first, fall back to raw bytes
                        try:
                            serialized_data = json.loads(value_bytes.decode('utf-8'))
                            if isinstance(serialized_data, dict) and 'type' in serialized_data:
                                crdt_value = self._deserialize_crdt(serialized_data)
                                if crdt_value:
                                    with self._data_lock:
                                        self._data[key] = crdt_value
                                    loaded_count += 1
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            # Store as raw bytes in a simple CRDT wrapper
                            from ..crdt import create_crdt
                            crdt_value = create_crdt('lww', value_bytes)
                            with self._data_lock:
                                self._data[key] = crdt_value
                            loaded_count += 1
                except Exception as e:
                    logger.warning(f"Failed to load key {key}: {e}")
            
            logger.info(f"Loaded {loaded_count} keys from persistent storage")
            
        except Exception as e:
            logger.error(f"Error loading persisted data: {e}")
    
    async def _create_snapshot(self):
        """Create a snapshot of current data"""
        try:
            # Create snapshot using storage engine
            snapshot_id = await self.storage.create_snapshot()
            if snapshot_id:
                logger.info(f"Created snapshot: {snapshot_id}")
            
        except Exception as e:
            logger.error(f"Error creating snapshot: {e}")
    
    def _serialize_crdt(self, crdt_value: CRDTValue) -> Optional[Dict]:
        """Serialize a CRDT value"""
        try:
            return crdt_value.to_dict()  # Use to_dict() instead of serialize()
        except Exception as e:
            logger.error(f"Error serializing CRDT: {e}")
            return None
    
    def _deserialize_crdt(self, data: Union[Dict, bytes]) -> Optional[CRDTValue]:
        """Deserialize a CRDT value"""
        try:
            # Handle bytes from storage
            if isinstance(data, bytes):
                data = json.loads(data.decode('utf-8'))
                
            crdt_type = data.get('type')
            if crdt_type:
                # Use create_crdt_value for proper initialization
                from kvstore.crdt import create_crdt_value
                crdt_value = create_crdt_value(crdt_type, self.config.node_id, initial_data=data)
                return crdt_value
        except Exception as e:
            logger.error(f"Error deserializing CRDT: {e}")
        
        return None
    
    # Replication (if clustering enabled)
    
    async def _replicate_command(self, command: str, *args):
        """Replicate a command to other nodes"""
        if not self.raft_node:
            return
        
        try:
            command_data = {
                'command': command,
                'args': args,
                'timestamp': time.time(),
                'node_id': self.config.node_id
            }
            
            await self.raft_node.replicate_log_entry(json.dumps(command_data))
            
        except Exception as e:
            logger.error(f"Error replicating command: {e}")
    
    # Event Handlers
    
    def _on_key_expired(self, key: str):
        """Callback when a key expires"""
        logger.debug(f"Key expired: {key}")
        
        # Remove from memory and cache
        asyncio.create_task(self._delete_key(key))
        
        # Publish expiration event
        asyncio.create_task(
            self.pubsub.publish(f"__keyevent@0__:expired", key)
        )
    
    # Background Tasks
    
    async def _stats_monitor(self):
        """Monitor and update server statistics"""
        while self._running:
            try:
                # Update memory usage
                process = psutil.Process()
                self.stats['memory_usage'] = process.memory_info().rss
                
                # Update active connections
                if self.tcp_server and hasattr(self.tcp_server, 'get_connection_count'):
                    self.stats['active_connections'] = self.tcp_server.get_connection_count()
                else:
                    self.stats['active_connections'] = 0
                
                await asyncio.sleep(5)  # Update every 5 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in stats monitor: {e}")
                await asyncio.sleep(1)
    
    async def _memory_monitor(self):
        """Monitor memory usage and trigger cleanup if needed"""
        while self._running:
            try:
                if self.config.max_memory:
                    current_usage = self.stats['memory_usage']
                    threshold = self.config.max_memory * self.config.memory_usage_threshold
                    
                    if current_usage > threshold:
                        logger.warning(f"Memory usage high: {current_usage}/{self.config.max_memory}")
                        
                        # Trigger cache eviction
                        evicted = self.cache_manager.force_eviction()
                        logger.info(f"Force evicted {evicted} cache entries")
                
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in memory monitor: {e}")
                await asyncio.sleep(1)
    
    async def _snapshot_scheduler(self):
        """Schedule periodic snapshots"""
        while self._running:
            try:
                # Create snapshot every hour
                await asyncio.sleep(3600)
                
                if self._running:
                    await self._create_snapshot()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in snapshot scheduler: {e}")
                await asyncio.sleep(60)
    
    async def _cleanup_task(self):
        """Periodic cleanup tasks"""
        while self._running:
            try:
                # Clean up expired pub/sub subscriptions
                if hasattr(self.pubsub, 'cleanup_expired_subscriptions'):
                    await self.pubsub.cleanup_expired_subscriptions()
                
                # Force garbage collection periodically
                import gc
                gc.collect()
                
                await asyncio.sleep(300)  # Every 5 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup task: {e}")
                await asyncio.sleep(60)
    
    # Utility Methods
    
    def get_stats(self) -> Dict[str, Any]:
        """Get server statistics (alias for get_info)"""
        info = self.get_info()
        # Include stats at top level for compatibility
        info.update(self.stats)
        return info
    
    def get_info(self) -> Dict[str, Any]:
        """Get comprehensive server information"""
        uptime = time.time() - self.stats['start_time']
        
        info = {
            'server': {
                'version': "1.0.0",
                'node_id': self.config.node_id,
                'cluster_name': self.config.cluster_name,
                'uptime_seconds': uptime,
                'running': self._running
            },
            'memory': {
                'used_memory': self.stats['memory_usage'],
                'max_memory': self.config.max_memory,
                'memory_usage_threshold': self.config.memory_usage_threshold
            },
            'stats': self.stats,
            'cache': self.cache_manager.get_stats(),
            'ttl': self.ttl_manager.get_stats(),
            'storage': self.storage.get_stats() if hasattr(self.storage, 'get_stats') else {},
            'pubsub': self.pubsub.get_stats(),
            'networking': {
                'host': self.config.host,
                'port': self.config.port,
                'max_connections': self.config.max_connections,
                'active_connections': self.stats['active_connections']
            }
        }
        
        if self.raft_node:
            info['clustering'] = {
                'enabled': True,
                'role': self.raft_node.get_role(),
                'term': self.raft_node.current_term,
                'leader': self.raft_node.leader_id
            }
        else:
            info['clustering'] = {'enabled': False}
        
        return info
    
    async def wait_for_shutdown(self):
        """Wait for server shutdown"""
        await self._shutdown_event.wait()
    
    def is_running(self) -> bool:
        """Check if server is running"""
        return self._running

# Context manager for easy server management
@asynccontextmanager
async def hyperkv_server(config: Optional[HyperKVServerConfig] = None):
    """Async context manager for HyperKV server"""
    server = HyperKVServer(config)
    try:
        await server.start()
        yield server
    finally:
        await server.stop()
