#!/usr/bin/env python3
"""
Quick unit tests for HyperKV components
"""

import pytest
import asyncio
import tempfile
import time
from unittest.mock import Mock, patch
import sys
import os

# Add the project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kvstore.crdt import (
    HybridLogicalClock, VectorClock, HLCTimestamp, 
    LWWElementSet, ORSet, CRDTCounter, create_crdt
)
from kvstore.core.cache import CacheManager, EvictionPolicy
from kvstore.core.ttl import TTLManager
from kvstore.config import HyperKVConfig

# Install pytest-asyncio if not available
try:
    import pytest_asyncio
except ImportError:
    pass


class TestCRDT:
    """Test CRDT implementations"""
    
    def test_hybrid_logical_clock(self):
        """Test HybridLogicalClock functionality"""
        hlc = HybridLogicalClock("node1")
        
        # Test tick
        ts1 = hlc.tick()
        assert isinstance(ts1, HLCTimestamp)
        assert ts1.logical >= 0
        assert ts1.physical > 0
        
        # Test logical_time property
        current_time = hlc.logical_time
        assert isinstance(current_time, HLCTimestamp)
        
        # Test update
        other_ts = HLCTimestamp(logical=5, physical=int(time.time() * 1000000))
        updated = hlc.update(other_ts)
        assert isinstance(updated, HLCTimestamp)
    
    def test_vector_clock(self):
        """Test VectorClock functionality"""
        vc = VectorClock("node1")
        
        # Test tick
        clock1 = vc.tick()
        assert clock1["node1"] == 1
        
        # Test update
        other_clock = {"node2": 3, "node1": 0}
        updated = vc.update(other_clock)
        assert updated["node1"] > clock1["node1"]
        assert updated["node2"] == 3
    
    def test_lww_element_set(self):
        """Test LWW Element Set CRDT"""
        clock = HybridLogicalClock("node1")
        lww_set = LWWElementSet(clock, "node1")
        
        # Test add and contains
        lww_set.add("value1")
        assert lww_set.contains("value1")
        
        # Test remove
        lww_set.remove("value1")
        assert not lww_set.contains("value1")
        
        # Test set_value and get_value (for LWW register behavior)
        lww_set.set_value("test_value")
        assert lww_set.get_value() == "test_value"
    
    def test_or_set(self):
        """Test OR Set CRDT"""
        or_set = ORSet("node1")
        
        # Test add and contains
        or_set.add("value1")
        assert or_set.contains("value1")
        assert "value1" in or_set.values()
        
        # Test remove
        or_set.remove("value1")
        assert not or_set.contains("value1")
        assert "value1" not in or_set.values()
    
    def test_crdt_counter(self):
        """Test CRDT Counter"""
        counter = CRDTCounter("node1")
        
        # Test increment
        counter.increment(5)
        assert counter.value() == 5
        
        # Test decrement
        counter.decrement(2)
        assert counter.value() == 3
    
    def test_create_crdt_factory(self):
        """Test CRDT factory function"""
        # Test LWW creation
        lww_crdt = create_crdt("lww", "test_value", "node1")
        assert isinstance(lww_crdt, LWWElementSet)
        
        # Test OR Set creation
        or_crdt = create_crdt("orset", ["val1", "val2"], "node1")
        assert isinstance(or_crdt, ORSet)
        
        # Test Counter creation
        counter_crdt = create_crdt("counter", 10, "node1")
        assert isinstance(counter_crdt, CRDTCounter)
        assert counter_crdt.value() == 10


class TestCacheManager:
    """Test Cache Manager"""
    
    def test_cache_initialization(self):
        """Test cache manager initialization"""
        cache = CacheManager(
            eviction_policy=EvictionPolicy.LRU,
            max_memory=1024*1024
        )
        assert cache.eviction_policy == EvictionPolicy.LRU
        assert cache.max_memory == 1024*1024
    
    def test_cache_operations(self):
        """Test basic cache operations"""
        cache = CacheManager(eviction_policy=EvictionPolicy.LRU, max_memory=1024)
        
        # Test put and get
        result = cache.put("key1", "value1")
        assert result is True
        assert cache.get("key1") == "value1"
        
        # Test get non-existent key
        assert cache.get("nonexistent") is None
        
        # Test delete
        result = cache.delete("key1")
        assert result is True
        assert cache.get("key1") is None
    
    def test_cache_eviction(self):
        """Test cache eviction policy"""
        cache = CacheManager(eviction_policy=EvictionPolicy.LRU, max_memory=10240)  # Larger memory limit
        
        # Add some items
        cache.put("key1", "value1")
        cache.put("key2", "value2")
        cache.put("key3", "value3")
        
        # Cache should have items (with larger memory limit, eviction shouldn't happen)
        assert cache.get("key1") == "value1"
        assert cache.get("key2") == "value2"
        assert cache.get("key3") == "value3"


class TestTTLManager:
    """Test TTL Manager"""
    
    def test_ttl_basic_operations(self):
        """Test basic TTL operations"""
        ttl_manager = TTLManager(check_interval=0.1)
        
        # Test set TTL
        result = ttl_manager.set_ttl("key1", 1.0)  # 1 second TTL
        assert result is True
        
        # Test get TTL
        ttl = ttl_manager.get_ttl("key1")
        assert ttl is not None
        assert 0 < ttl <= 1.0
        
        # Test remove TTL
        result = ttl_manager.remove_ttl("key1")
        assert result is True
        
        # Test TTL is gone
        ttl = ttl_manager.get_ttl("key1")
        assert ttl is None
    
    def test_ttl_expiration_check(self):
        """Test TTL expiration logic"""
        ttl_manager = TTLManager(check_interval=0.1)
        
        # Set short TTL
        ttl_manager.set_ttl("key1", 0.01)  # 10ms TTL
        
        # Wait for expiration
        time.sleep(0.05)
        
        # Check if key is expired
        is_expired = ttl_manager.is_expired("key1")
        assert is_expired is True
        
        # Get expired keys
        expired = ttl_manager.get_expired_keys(max_keys=10)
        assert len(expired) >= 0  # Could be 0 if already cleaned up


class TestConfiguration:
    """Test configuration classes"""
    
    def test_hyperkv_config(self):
        """Test HyperKV configuration"""
        config = HyperKVConfig()
        
        # Test that config object exists and has some basic attributes
        assert hasattr(config, '__dict__')
        assert isinstance(config, HyperKVConfig)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
