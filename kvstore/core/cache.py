"""
Cache Management System with Multiple Eviction Policies
Handles memory management and key eviction strategies.
"""

import time
import random
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Set, Tuple
from dataclasses import dataclass
from collections import OrderedDict, defaultdict
from threading import RLock
from enum import Enum

logger = logging.getLogger(__name__)

class EvictionPolicy(Enum):
    """Available eviction policies"""
    LRU = "lru"                    # Least Recently Used
    LFU = "lfu"                    # Least Frequently Used
    ARC = "arc"                    # Adaptive Replacement Cache
    RANDOM = "random"              # Random eviction
    VOLATILE_LRU = "volatile-lru"  # LRU among keys with TTL
    VOLATILE_LFU = "volatile-lfu"  # LFU among keys with TTL

@dataclass
class CacheEntry:
    """Represents a cache entry with metadata"""
    key: str
    value: Any
    created_at: float
    last_accessed: float
    access_count: int
    has_ttl: bool
    
    def update_access(self):
        """Update access metadata"""
        self.last_accessed = time.time()
        self.access_count += 1

class EvictionStrategy(ABC):
    """Abstract base class for eviction strategies"""
    
    @abstractmethod
    def on_access(self, key: str, entry: CacheEntry):
        """Called when a key is accessed"""
        pass
    
    @abstractmethod
    def on_insert(self, key: str, entry: CacheEntry):
        """Called when a key is inserted"""
        pass
    
    @abstractmethod
    def on_delete(self, key: str):
        """Called when a key is deleted"""
        pass
    
    @abstractmethod
    def select_victims(self, count: int, volatile_only: bool = False) -> List[str]:
        """Select keys for eviction"""
        pass
    
    @abstractmethod
    def clear(self):
        """Clear all tracking data"""
        pass

class LRUStrategy(EvictionStrategy):
    """Least Recently Used eviction strategy"""
    
    def __init__(self):
        self._access_order = OrderedDict()  # key -> timestamp
        self._lock = RLock()
    
    def on_access(self, key: str, entry: CacheEntry):
        with self._lock:
            # Move to end (most recent)
            if key in self._access_order:
                del self._access_order[key]
            self._access_order[key] = entry.last_accessed
    
    def on_insert(self, key: str, entry: CacheEntry):
        with self._lock:
            self._access_order[key] = entry.last_accessed
    
    def on_delete(self, key: str):
        with self._lock:
            self._access_order.pop(key, None)
    
    def select_victims(self, count: int, volatile_only: bool = False) -> List[str]:
        with self._lock:
            victims = []
            for key in list(self._access_order.keys()):
                if len(victims) >= count:
                    break
                victims.append(key)
            return victims
    
    def clear(self):
        with self._lock:
            self._access_order.clear()

class LFUStrategy(EvictionStrategy):
    """Least Frequently Used eviction strategy"""
    
    def __init__(self):
        self._frequency = defaultdict(int)  # key -> access count
        self._freq_to_keys = defaultdict(set)  # frequency -> set of keys
        self._min_freq = 0
        self._lock = RLock()
    
    def on_access(self, key: str, entry: CacheEntry):
        with self._lock:
            old_freq = self._frequency[key]
            new_freq = entry.access_count
            
            # Update frequency tracking
            if old_freq > 0:
                self._freq_to_keys[old_freq].discard(key)
                if not self._freq_to_keys[old_freq] and old_freq == self._min_freq:
                    self._min_freq += 1
            
            self._frequency[key] = new_freq
            self._freq_to_keys[new_freq].add(key)
            
            if old_freq == 0:
                self._min_freq = 1
    
    def on_insert(self, key: str, entry: CacheEntry):
        with self._lock:
            self._frequency[key] = entry.access_count
            self._freq_to_keys[entry.access_count].add(key)
            if entry.access_count == 1:
                self._min_freq = 1
    
    def on_delete(self, key: str):
        with self._lock:
            freq = self._frequency.pop(key, 0)
            if freq > 0:
                self._freq_to_keys[freq].discard(key)
    
    def select_victims(self, count: int, volatile_only: bool = False) -> List[str]:
        with self._lock:
            victims = []
            
            # Start from minimum frequency
            freq = self._min_freq
            while len(victims) < count and freq < max(self._freq_to_keys.keys(), default=0) + 1:
                if freq in self._freq_to_keys:
                    keys_at_freq = list(self._freq_to_keys[freq])
                    for key in keys_at_freq:
                        if len(victims) >= count:
                            break
                        victims.append(key)
                freq += 1
            
            return victims
    
    def clear(self):
        with self._lock:
            self._frequency.clear()
            self._freq_to_keys.clear()
            self._min_freq = 0

class ARCStrategy(EvictionStrategy):
    """Adaptive Replacement Cache eviction strategy"""
    
    def __init__(self, cache_size: int = 1000):
        self.cache_size = cache_size
        self.p = 0  # Target size for T1
        
        # T1: recently used once
        self.t1 = OrderedDict()
        # T2: recently used at least twice  
        self.t2 = OrderedDict()
        # B1: ghost entries for T1
        self.b1 = OrderedDict()
        # B2: ghost entries for T2
        self.b2 = OrderedDict()
        
        self._lock = RLock()
    
    def on_access(self, key: str, entry: CacheEntry):
        with self._lock:
            if key in self.t1:
                # Move from T1 to T2
                del self.t1[key]
                self.t2[key] = entry.last_accessed
            elif key in self.t2:
                # Move to end of T2
                del self.t2[key]
                self.t2[key] = entry.last_accessed
            elif key in self.b1:
                # Cache hit in B1 - adapt
                self.p = min(self.cache_size, self.p + max(1, len(self.b2) // len(self.b1)))
                del self.b1[key]
                self.t2[key] = entry.last_accessed
            elif key in self.b2:
                # Cache hit in B2 - adapt
                self.p = max(0, self.p - max(1, len(self.b1) // len(self.b2)))
                del self.b2[key]
                self.t2[key] = entry.last_accessed
    
    def on_insert(self, key: str, entry: CacheEntry):
        with self._lock:
            self.t1[key] = entry.last_accessed
    
    def on_delete(self, key: str):
        with self._lock:
            self.t1.pop(key, None)
            self.t2.pop(key, None)
            self.b1.pop(key, None)
            self.b2.pop(key, None)
    
    def select_victims(self, count: int, volatile_only: bool = False) -> List[str]:
        with self._lock:
            victims = []
            
            # Select from T1 first if it's larger than target
            if len(self.t1) > self.p:
                victims.extend(list(self.t1.keys())[:count])
            else:
                # Select from T2
                victims.extend(list(self.t2.keys())[:count])
            
            return victims[:count]
    
    def clear(self):
        with self._lock:
            self.t1.clear()
            self.t2.clear()
            self.b1.clear()
            self.b2.clear()
            self.p = 0

class RandomStrategy(EvictionStrategy):
    """Random eviction strategy"""
    
    def __init__(self):
        self._keys = set()
        self._lock = RLock()
    
    def on_access(self, key: str, entry: CacheEntry):
        pass  # No tracking needed
    
    def on_insert(self, key: str, entry: CacheEntry):
        with self._lock:
            self._keys.add(key)
    
    def on_delete(self, key: str):
        with self._lock:
            self._keys.discard(key)
    
    def select_victims(self, count: int, volatile_only: bool = False) -> List[str]:
        with self._lock:
            available_keys = list(self._keys)
            if len(available_keys) <= count:
                return available_keys
            return random.sample(available_keys, count)
    
    def clear(self):
        with self._lock:
            self._keys.clear()

class CacheManager:
    """
    High-performance cache manager with pluggable eviction policies.
    
    Features:
    - Multiple eviction strategies (LRU, LFU, ARC, Random)
    - Memory usage monitoring
    - Volatile key support (keys with TTL)
    - Batch eviction for efficiency
    - Comprehensive statistics
    """
    
    def __init__(self,
                 eviction_policy: EvictionPolicy = EvictionPolicy.LRU,
                 max_memory: Optional[int] = None,
                 memory_threshold: float = 0.85,
                 eviction_batch_size: int = 10,
                 sample_size: int = 5):
        
        self.eviction_policy = eviction_policy
        self.max_memory = max_memory
        self.memory_threshold = memory_threshold
        self.eviction_batch_size = eviction_batch_size
        self.sample_size = sample_size
        
        # Cache storage
        self._cache: Dict[str, CacheEntry] = {}
        self._volatile_keys: Set[str] = set()  # Keys with TTL
        
        # Eviction strategy
        self._strategy = self._create_strategy(eviction_policy)
        
        # Thread safety
        self._lock = RLock()
        
        # Statistics
        self.stats = {
            'total_entries': 0,
            'memory_usage': 0,
            'evictions': 0,
            'hits': 0,
            'misses': 0,
            'memory_pressure_evictions': 0,
            'volatile_evictions': 0
        }
        
        logger.info(f"Cache Manager initialized with {eviction_policy.value} policy")
    
    def _create_strategy(self, policy: EvictionPolicy) -> EvictionStrategy:
        """Create eviction strategy based on policy"""
        if policy == EvictionPolicy.LRU or policy == EvictionPolicy.VOLATILE_LRU:
            return LRUStrategy()
        elif policy == EvictionPolicy.LFU or policy == EvictionPolicy.VOLATILE_LFU:
            return LFUStrategy()
        elif policy == EvictionPolicy.ARC:
            return ARCStrategy()
        elif policy == EvictionPolicy.RANDOM:
            return RandomStrategy()
        else:
            raise ValueError(f"Unsupported eviction policy: {policy}")
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get a value from cache.
        
        Args:
            key: The key to retrieve
            
        Returns:
            The cached value or None if not found
        """
        with self._lock:
            if key not in self._cache:
                self.stats['misses'] += 1
                return None
            
            entry = self._cache[key]
            entry.update_access()
            
            # Update eviction strategy
            self._strategy.on_access(key, entry)
            
            self.stats['hits'] += 1
            return entry.value
    
    def put(self, key: str, value: Any, has_ttl: bool = False) -> bool:
        """
        Put a value in cache.
        
        Args:
            key: The key to store
            value: The value to store
            has_ttl: Whether the key has TTL set
            
        Returns:
            True if stored successfully
        """
        current_time = time.time()
        
        with self._lock:
            # Check if we need to evict
            if self._should_evict():
                self._evict_entries()
            
            # Create cache entry
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=current_time,
                last_accessed=current_time,
                access_count=1,
                has_ttl=has_ttl
            )
            
            # Update existing entry
            if key in self._cache:
                old_entry = self._cache[key]
                # Preserve access count for existing keys
                entry.access_count = old_entry.access_count + 1
                
                # Update volatile tracking
                if has_ttl:
                    self._volatile_keys.add(key)
                else:
                    self._volatile_keys.discard(key)
            else:
                # New entry
                if has_ttl:
                    self._volatile_keys.add(key)
                self.stats['total_entries'] += 1
            
            # Store in cache
            self._cache[key] = entry
            
            # Update eviction strategy
            self._strategy.on_insert(key, entry)
            
            # Update memory usage (rough estimate)
            self._update_memory_usage()
            
        logger.debug(f"Cached key '{key}' (TTL: {has_ttl})")
        return True
    
    def delete(self, key: str) -> bool:
        """
        Delete a key from cache.
        
        Args:
            key: The key to delete
            
        Returns:
            True if key was deleted
        """
        with self._lock:
            if key not in self._cache:
                return False
            
            # Remove from cache
            del self._cache[key]
            self._volatile_keys.discard(key)
            
            # Update eviction strategy
            self._strategy.on_delete(key)
            
            # Update statistics
            self.stats['total_entries'] -= 1
            self._update_memory_usage()
            
        logger.debug(f"Deleted key '{key}' from cache")
        return True
    
    def _should_evict(self) -> bool:
        """Check if we should trigger eviction"""
        if self.max_memory is None:
            return False
        
        current_usage = self.stats['memory_usage']
        threshold = self.max_memory * self.memory_threshold
        
        return current_usage > threshold
    
    def _evict_entries(self):
        """Evict entries based on the configured policy"""
        if not self._cache:
            return
        
        # Determine if we should only evict volatile keys
        volatile_only = self.eviction_policy in [
            EvictionPolicy.VOLATILE_LRU,
            EvictionPolicy.VOLATILE_LFU
        ]
        
        # Select victims
        victims = self._strategy.select_victims(
            self.eviction_batch_size, 
            volatile_only
        )
        
        # If volatile_only but no volatile keys, fall back to all keys
        if volatile_only and not victims and self._volatile_keys:
            victims = list(self._volatile_keys)[:self.eviction_batch_size]
        
        # Evict selected keys
        evicted_count = 0
        for key in victims:
            if key in self._cache:
                self.delete(key)
                evicted_count += 1
                
                if volatile_only:
                    self.stats['volatile_evictions'] += 1
                else:
                    self.stats['memory_pressure_evictions'] += 1
        
        self.stats['evictions'] += evicted_count
        
        if evicted_count > 0:
            logger.debug(f"Evicted {evicted_count} keys "
                        f"({'volatile only' if volatile_only else 'all keys'})")
    
    def _update_memory_usage(self):
        """Update memory usage statistics (rough estimate)"""
        # This is a simplified memory estimation
        # In production, you might want to use more sophisticated methods
        total_size = 0
        
        for entry in self._cache.values():
            # Estimate size of key + value + metadata
            key_size = len(entry.key.encode('utf-8'))
            value_size = self._estimate_value_size(entry.value)
            metadata_size = 64  # Rough estimate for CacheEntry metadata
            
            total_size += key_size + value_size + metadata_size
        
        self.stats['memory_usage'] = total_size
    
    def _estimate_value_size(self, value: Any) -> int:
        """Estimate the memory size of a value"""
        try:
            if isinstance(value, str):
                return len(value.encode('utf-8'))
            elif isinstance(value, (int, float)):
                return 8
            elif isinstance(value, bytes):
                return len(value)
            elif isinstance(value, (list, tuple)):
                return sum(self._estimate_value_size(item) for item in value)
            elif isinstance(value, dict):
                return sum(
                    self._estimate_value_size(k) + self._estimate_value_size(v)
                    for k, v in value.items()
                )
            else:
                # Fallback estimation
                return len(str(value)) * 2
        except Exception:
            return 100  # Default fallback
    
    def force_eviction(self, count: int = None) -> int:
        """
        Force eviction of entries.
        
        Args:
            count: Number of entries to evict (default: eviction_batch_size)
            
        Returns:
            Number of entries actually evicted
        """
        if count is None:
            count = self.eviction_batch_size
        
        with self._lock:
            initial_size = len(self._cache)
            
            victims = self._strategy.select_victims(count, False)
            
            evicted_count = 0
            for key in victims:
                if key in self._cache:
                    self.delete(key)
                    evicted_count += 1
            
            self.stats['evictions'] += evicted_count
            
        logger.info(f"Force evicted {evicted_count} entries")
        return evicted_count
    
    def get_memory_info(self) -> Dict[str, Any]:
        """Get memory usage information"""
        with self._lock:
            return {
                'current_usage': self.stats['memory_usage'],
                'max_memory': self.max_memory,
                'usage_percentage': (
                    self.stats['memory_usage'] / self.max_memory * 100
                    if self.max_memory else 0
                ),
                'threshold': self.memory_threshold,
                'should_evict': self._should_evict(),
                'total_entries': len(self._cache),
                'volatile_entries': len(self._volatile_keys)
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self._lock:
            hit_rate = (
                self.stats['hits'] / (self.stats['hits'] + self.stats['misses'])
                if (self.stats['hits'] + self.stats['misses']) > 0 else 0
            )
            
            return {
                **self.stats,
                'hit_rate': hit_rate,
                'current_entries': len(self._cache),
                'volatile_entries': len(self._volatile_keys),
                'eviction_policy': self.eviction_policy.value
            }
    
    def clear(self):
        """Clear all cache entries"""
        with self._lock:
            self._cache.clear()
            self._volatile_keys.clear()
            self._strategy.clear()
            
            # Reset statistics
            self.stats.update({
                'total_entries': 0,
                'memory_usage': 0,
                'hits': 0,
                'misses': 0
            })
        
        logger.info("Cleared all cache entries")
    
    def resize(self, new_max_memory: Optional[int]):
        """Resize the cache memory limit"""
        with self._lock:
            old_max = self.max_memory
            self.max_memory = new_max_memory
            
            # Trigger eviction if necessary
            if self._should_evict():
                self._evict_entries()
        
        logger.info(f"Resized cache memory limit: {old_max} -> {new_max_memory}")
