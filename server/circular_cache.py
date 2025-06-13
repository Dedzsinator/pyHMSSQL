"""
Circular Cache System for Query Plans and Results.

This module implements high-performance circular buffer caches for:
- Query execution plans
- Intermediate and final query results
- Transformed query fragments

Features:
- O(1) insertion and retrieval
- Fixed-size with circular FIFO eviction
- TTL-aware cache invalidation
- Schema change detection and invalidation
- Memory-efficient ring buffer implementation
- Thread-safe operations
- Cache hit/miss statistics
"""

import logging
import time
import threading
import hashlib
import pickle
import json
from typing import Dict, List, Tuple, Optional, Any, Union
from dataclasses import dataclass, field
from collections import OrderedDict
import weakref
import gc


@dataclass
class CacheEntry:
    """Represents a single cache entry."""
    key: str
    value: Any
    created_at: float
    last_accessed: float
    access_count: int = 0
    ttl: Optional[float] = None
    size_bytes: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def is_expired(self, current_time: float = None) -> bool:
        """Check if the cache entry has expired."""
        if self.ttl is None:
            return False
        
        if current_time is None:
            current_time = time.time()
        
        return (current_time - self.created_at) > self.ttl
    
    def update_access(self):
        """Update access statistics."""
        self.last_accessed = time.time()
        self.access_count += 1


class CircularBuffer:
    """
    Memory-efficient circular buffer implementation.
    
    Uses a fixed-size array with head/tail pointers for O(1) operations.
    Automatically overwrites oldest entries when capacity is reached.
    """
    
    def __init__(self, capacity: int):
        """
        Initialize circular buffer.
        
        Args:
            capacity: Maximum number of entries
        """
        if capacity <= 0:
            raise ValueError("Capacity must be positive")
        
        self.capacity = capacity
        self.buffer = [None] * capacity
        self.head = 0  # Points to next insertion position
        self.tail = 0  # Points to oldest entry
        self.size = 0
        self.full = False
    
    def put(self, item: Any) -> Optional[Any]:
        """
        Insert an item into the buffer.
        
        Args:
            item: Item to insert
            
        Returns:
            Evicted item if buffer was full, None otherwise
        """
        evicted = None
        
        if self.full:
            evicted = self.buffer[self.head]
        
        self.buffer[self.head] = item
        
        if self.full:
            self.tail = (self.tail + 1) % self.capacity
        
        self.head = (self.head + 1) % self.capacity
        
        if not self.full and self.head == self.tail:
            self.full = True
        
        if not self.full:
            self.size += 1
        
        return evicted
    
    def get_all(self) -> List[Any]:
        """Get all items in insertion order."""
        if not self.full and self.size == 0:
            return []
        
        items = []
        
        if self.full:
            # Get items from tail to end of buffer
            items.extend(self.buffer[self.tail:])
            # Get items from start of buffer to head
            if self.head > 0:
                items.extend(self.buffer[:self.head])
        else:
            # Buffer not full, items are from 0 to head
            items = self.buffer[:self.size]
        
        return [item for item in items if item is not None]
    
    def clear(self):
        """Clear all items from the buffer."""
        self.buffer = [None] * self.capacity
        self.head = 0
        self.tail = 0
        self.size = 0
        self.full = False


class CircularPlanCache:
    """
    High-performance circular cache for query execution plans.
    
    Caches compiled query plans with automatic eviction and invalidation.
    Uses content-based hashing for cache keys and supports TTL expiration.
    """
    
    def __init__(self, capacity: int = 1000, default_ttl: float = 3600.0):
        """
        Initialize the plan cache.
        
        Args:
            capacity: Maximum number of cached plans
            default_ttl: Default time-to-live in seconds
        """
        self.capacity = capacity
        self.default_ttl = default_ttl
        
        # Circular buffer for FIFO eviction
        self.buffer = CircularBuffer(capacity)
        
        # Hash table for O(1) lookups
        self.lookup_table: Dict[str, CacheEntry] = {}
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Statistics
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.invalidations = 0
        
        # Schema change tracking
        self.schema_version = 0
        self.table_versions: Dict[str, int] = {}
        
        logging.info(f"Initialized CircularPlanCache with capacity {capacity}")
    
    def put(self, query_signature: str, plan: Any, ttl: Optional[float] = None,
            tables_used: List[str] = None, metadata: Dict[str, Any] = None) -> bool:
        """
        Cache a query plan.
        
        Args:
            query_signature: Unique signature for the query
            plan: Query execution plan
            ttl: Time-to-live in seconds (optional)
            tables_used: List of tables used by the query
            metadata: Additional metadata
            
        Returns:
            True if successfully cached
        """
        with self.lock:
            try:
                # Create cache key
                cache_key = self._generate_cache_key(query_signature, tables_used)
                
                # Estimate size
                size_bytes = self._estimate_size(plan)
                
                # Create cache entry
                entry = CacheEntry(
                    key=cache_key,
                    value=plan,
                    created_at=time.time(),
                    last_accessed=time.time(),
                    ttl=ttl or self.default_ttl,
                    size_bytes=size_bytes,
                    metadata=metadata or {}
                )
                
                # Add schema version info
                entry.metadata['schema_version'] = self.schema_version
                entry.metadata['tables_used'] = tables_used or []
                
                # Check if we need to evict
                evicted_entry = self.buffer.put(entry)
                if evicted_entry:
                    # Remove evicted entry from lookup table
                    if evicted_entry.key in self.lookup_table:
                        del self.lookup_table[evicted_entry.key]
                    self.evictions += 1
                
                # Add to lookup table
                self.lookup_table[cache_key] = entry
                
                return True
                
            except Exception as e:
                logging.error(f"Error caching plan: {e}")
                return False
    
    def get(self, query_signature: str, tables_used: List[str] = None) -> Optional[Any]:
        """
        Retrieve a cached query plan.
        
        Args:
            query_signature: Unique signature for the query
            tables_used: List of tables used by the query
            
        Returns:
            Cached plan if found and valid, None otherwise
        """
        with self.lock:
            cache_key = self._generate_cache_key(query_signature, tables_used)
            
            if cache_key not in self.lookup_table:
                self.misses += 1
                return None
            
            entry = self.lookup_table[cache_key]
            
            # Check if expired
            if entry.is_expired():
                del self.lookup_table[cache_key]
                self.misses += 1
                return None
            
            # Check schema version
            if entry.metadata.get('schema_version', 0) < self.schema_version:
                del self.lookup_table[cache_key]
                self.invalidations += 1
                self.misses += 1
                return None
            
            # Check table versions
            for table in entry.metadata.get('tables_used', []):
                if (table in self.table_versions and 
                    self.table_versions[table] > entry.metadata.get('schema_version', 0)):
                    del self.lookup_table[cache_key]
                    self.invalidations += 1
                    self.misses += 1
                    return None
            
            # Update access statistics
            entry.update_access()
            self.hits += 1
            
            return entry.value
    
    def invalidate(self, query_signature: str = None, table_name: str = None):
        """
        Invalidate cached plans.
        
        Args:
            query_signature: Specific query to invalidate (optional)
            table_name: Invalidate all plans using this table (optional)
        """
        with self.lock:
            if query_signature:
                # Invalidate specific query
                cache_key = self._generate_cache_key(query_signature)
                if cache_key in self.lookup_table:
                    del self.lookup_table[cache_key]
                    self.invalidations += 1
            
            elif table_name:
                # Invalidate all plans using the table
                to_remove = []
                for key, entry in self.lookup_table.items():
                    if table_name in entry.metadata.get('tables_used', []):
                        to_remove.append(key)
                
                for key in to_remove:
                    del self.lookup_table[key]
                    self.invalidations += len(to_remove)
                
                # Update table version
                self.table_versions[table_name] = self.table_versions.get(table_name, 0) + 1
            
            else:
                # Invalidate all
                self.lookup_table.clear()
                self.buffer.clear()
                self.schema_version += 1
                self.invalidations += 1
    
    def _generate_cache_key(self, query_signature: str, tables_used: List[str] = None) -> str:
        """Generate a cache key from query signature and tables."""
        key_components = [query_signature]
        
        if tables_used:
            key_components.extend(sorted(tables_used))
        
        key_components.append(str(self.schema_version))
        
        key_string = "|".join(key_components)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def _estimate_size(self, obj: Any) -> int:
        """Estimate the size of an object in bytes."""
        try:
            return len(pickle.dumps(obj))
        except Exception:
            # Fallback estimation
            return 1024  # Default 1KB
    
    def cleanup_expired(self):
        """Remove expired entries from the cache."""
        with self.lock:
            current_time = time.time()
            expired_keys = []
            
            for key, entry in self.lookup_table.items():
                if entry.is_expired(current_time):
                    expired_keys.append(key)
            
            for key in expired_keys:
                del self.lookup_table[key]
                self.invalidations += len(expired_keys)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get cache performance statistics."""
        with self.lock:
            total_requests = self.hits + self.misses
            hit_rate = self.hits / max(1, total_requests)
            
            # Calculate memory usage
            total_size = sum(entry.size_bytes for entry in self.lookup_table.values())
            
            return {
                'hits': self.hits,
                'misses': self.misses,
                'hit_rate': hit_rate,
                'evictions': self.evictions,
                'invalidations': self.invalidations,
                'current_size': len(self.lookup_table),
                'capacity': self.capacity,
                'fill_rate': len(self.lookup_table) / self.capacity,
                'memory_usage_bytes': total_size,
                'schema_version': self.schema_version
            }


class CircularResultCache:
    """
    High-performance circular cache for query results.
    
    Caches deterministic query results with automatic eviction.
    Supports result compression and size-based eviction policies.
    """
    
    def __init__(self, capacity: int = 500, max_result_size: int = 10 * 1024 * 1024,
                 default_ttl: float = 1800.0, enable_compression: bool = True):
        """
        Initialize the result cache.
        
        Args:
            capacity: Maximum number of cached results
            max_result_size: Maximum size of a single result in bytes
            default_ttl: Default time-to-live in seconds
            enable_compression: Whether to compress large results
        """
        self.capacity = capacity
        self.max_result_size = max_result_size
        self.default_ttl = default_ttl
        self.enable_compression = enable_compression
        
        # Circular buffer for FIFO eviction
        self.buffer = CircularBuffer(capacity)
        
        # Hash table for O(1) lookups
        self.lookup_table: Dict[str, CacheEntry] = {}
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Statistics
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.compression_saves = 0
        self.total_memory_saved = 0
        
        logging.info(f"Initialized CircularResultCache with capacity {capacity}")
    
    def put(self, query_hash: str, result: Any, ttl: Optional[float] = None,
            metadata: Dict[str, Any] = None) -> bool:
        """
        Cache a query result.
        
        Args:
            query_hash: Hash of the query
            result: Query result to cache
            ttl: Time-to-live in seconds
            metadata: Additional metadata
            
        Returns:
            True if successfully cached
        """
        with self.lock:
            try:
                # Estimate result size
                result_size = self._estimate_size(result)
                
                # Skip if result is too large
                if result_size > self.max_result_size:
                    logging.debug(f"Result too large to cache: {result_size} bytes")
                    return False
                
                # Compress if enabled and beneficial
                compressed_result = result
                compressed_size = result_size
                
                if self.enable_compression and result_size > 1024:  # Compress if > 1KB
                    compressed_result, compressed_size = self._compress_result(result)
                    if compressed_size < result_size:
                        self.compression_saves += 1
                        self.total_memory_saved += (result_size - compressed_size)
                
                # Create cache entry
                entry = CacheEntry(
                    key=query_hash,
                    value=compressed_result,
                    created_at=time.time(),
                    last_accessed=time.time(),
                    ttl=ttl or self.default_ttl,
                    size_bytes=compressed_size,
                    metadata=metadata or {}
                )
                
                entry.metadata['original_size'] = result_size
                entry.metadata['compressed'] = compressed_size < result_size
                
                # Check if we need to evict
                evicted_entry = self.buffer.put(entry)
                if evicted_entry:
                    # Remove evicted entry from lookup table
                    if evicted_entry.key in self.lookup_table:
                        del self.lookup_table[evicted_entry.key]
                    self.evictions += 1
                
                # Add to lookup table
                self.lookup_table[query_hash] = entry
                
                return True
                
            except Exception as e:
                logging.error(f"Error caching result: {e}")
                return False
    
    def get(self, query_hash: str) -> Optional[Any]:
        """
        Retrieve a cached query result.
        
        Args:
            query_hash: Hash of the query
            
        Returns:
            Cached result if found and valid, None otherwise
        """
        with self.lock:
            if query_hash not in self.lookup_table:
                self.misses += 1
                return None
            
            entry = self.lookup_table[query_hash]
            
            # Check if expired
            if entry.is_expired():
                del self.lookup_table[query_hash]
                self.misses += 1
                return None
            
            # Update access statistics
            entry.update_access()
            self.hits += 1
            
            # Decompress if necessary
            result = entry.value
            if entry.metadata.get('compressed', False):
                result = self._decompress_result(result)
            
            return result
    
    def invalidate(self, query_hash: str = None):
        """
        Invalidate cached results.
        
        Args:
            query_hash: Specific query result to invalidate (optional)
        """
        with self.lock:
            if query_hash:
                if query_hash in self.lookup_table:
                    del self.lookup_table[query_hash]
            else:
                # Invalidate all
                self.lookup_table.clear()
                self.buffer.clear()
    
    def _compress_result(self, result: Any) -> Tuple[Any, int]:
        """
        Compress a result if beneficial.
        
        Args:
            result: Result to compress
            
        Returns:
            Tuple of (compressed_result, compressed_size)
        """
        try:
            import zlib
            
            # Serialize and compress
            serialized = pickle.dumps(result)
            compressed = zlib.compress(serialized, level=6)
            
            return compressed, len(compressed)
            
        except Exception as e:
            logging.debug(f"Compression failed: {e}")
            return result, self._estimate_size(result)
    
    def _decompress_result(self, compressed_result: Any) -> Any:
        """
        Decompress a compressed result.
        
        Args:
            compressed_result: Compressed result data
            
        Returns:
            Decompressed result
        """
        try:
            import zlib
            
            decompressed = zlib.decompress(compressed_result)
            return pickle.loads(decompressed)
            
        except Exception as e:
            logging.error(f"Decompression failed: {e}")
            return compressed_result
    
    def _estimate_size(self, obj: Any) -> int:
        """Estimate the size of an object in bytes."""
        try:
            return len(pickle.dumps(obj))
        except Exception:
            return 1024  # Default 1KB
    
    def cleanup_expired(self):
        """Remove expired entries from the cache."""
        with self.lock:
            current_time = time.time()
            expired_keys = []
            
            for key, entry in self.lookup_table.items():
                if entry.is_expired(current_time):
                    expired_keys.append(key)
            
            for key in expired_keys:
                del self.lookup_table[key]
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get cache performance statistics."""
        with self.lock:
            total_requests = self.hits + self.misses
            hit_rate = self.hits / max(1, total_requests)
            
            # Calculate memory usage
            total_size = sum(entry.size_bytes for entry in self.lookup_table.values())
            original_size = sum(entry.metadata.get('original_size', entry.size_bytes) 
                              for entry in self.lookup_table.values())
            
            compression_ratio = original_size / max(1, total_size)
            
            return {
                'hits': self.hits,
                'misses': self.misses,
                'hit_rate': hit_rate,
                'evictions': self.evictions,
                'current_size': len(self.lookup_table),
                'capacity': self.capacity,
                'fill_rate': len(self.lookup_table) / self.capacity,
                'memory_usage_bytes': total_size,
                'original_memory_bytes': original_size,
                'compression_ratio': compression_ratio,
                'compression_saves': self.compression_saves,
                'total_memory_saved': self.total_memory_saved
            }


class CacheManager:
    """
    Manages multiple cache instances and provides unified interface.
    
    Coordinates between plan cache and result cache, handles global
    invalidation events, and provides cache warming capabilities.
    """
    
    def __init__(self, plan_cache_capacity: int = 1000, 
                 result_cache_capacity: int = 500):
        """
        Initialize the cache manager.
        
        Args:
            plan_cache_capacity: Capacity of the plan cache
            result_cache_capacity: Capacity of the result cache
        """
        self.plan_cache = CircularPlanCache(capacity=plan_cache_capacity)
        self.result_cache = CircularResultCache(capacity=result_cache_capacity)
        
        # Cleanup thread
        self.cleanup_interval = 300  # 5 minutes
        self.cleanup_thread = None
        self.shutdown_flag = False
        
        self.start_cleanup_thread()
        
        logging.info("Initialized CacheManager")
    
    def start_cleanup_thread(self):
        """Start the background cleanup thread."""
        def cleanup_worker():
            while not self.shutdown_flag:
                try:
                    time.sleep(self.cleanup_interval)
                    self.cleanup_expired()
                except Exception as e:
                    logging.error(f"Cache cleanup error: {e}")
        
        self.cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        self.cleanup_thread.start()
    
    def cleanup_expired(self):
        """Clean up expired entries from both caches."""
        self.plan_cache.cleanup_expired()
        self.result_cache.cleanup_expired()
        
        # Force garbage collection
        gc.collect()
    
    def invalidate_table(self, table_name: str):
        """Invalidate all cache entries related to a table."""
        self.plan_cache.invalidate(table_name=table_name)
        # Result cache doesn't track table dependencies directly
        # In practice, would need more sophisticated invalidation
    
    def invalidate_all(self):
        """Invalidate all cache entries."""
        self.plan_cache.invalidate()
        self.result_cache.invalidate()
    
    def get_combined_statistics(self) -> Dict[str, Any]:
        """Get combined statistics from both caches."""
        plan_stats = self.plan_cache.get_statistics()
        result_stats = self.result_cache.get_statistics()
        
        return {
            'plan_cache': plan_stats,
            'result_cache': result_stats,
            'combined_hit_rate': (plan_stats['hits'] + result_stats['hits']) / 
                               max(1, plan_stats['hits'] + plan_stats['misses'] + 
                                   result_stats['hits'] + result_stats['misses']),
            'total_memory_usage': plan_stats['memory_usage_bytes'] + result_stats['memory_usage_bytes']
        }
    
    def shutdown(self):
        """Shutdown the cache manager."""
        self.shutdown_flag = True
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5.0)
        
        logging.info("CacheManager shutdown complete")


# Alias for compatibility with unified optimizer
CircularCacheSystem = CacheManager
