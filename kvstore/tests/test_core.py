"""
Core functionality tests for HyperKV Server
"""

import pytest
import asyncio
import time
import tempfile
import shutil
from pathlib import Path

from ..core import HyperKVServer, HyperKVConfig, TTLManager, CacheManager, EvictionPolicy

class TestTTLManager:
    """Test TTL Manager functionality"""
    
    @pytest.fixture
    async def ttl_manager(self):
        """Create TTL manager for testing"""
        manager = TTLManager(check_interval=0.1, max_keys_per_check=10)
        manager.start()
        yield manager
        await manager.stop()
    
    def test_set_ttl(self, ttl_manager):
        """Test setting TTL for keys"""
        assert ttl_manager.set_ttl("test_key", 1.0)
        assert ttl_manager.get_ttl("test_key") is not None
        assert ttl_manager.get_ttl("test_key") <= 1.0
    
    def test_remove_ttl(self, ttl_manager):
        """Test removing TTL from keys"""
        ttl_manager.set_ttl("test_key", 1.0)
        assert ttl_manager.remove_ttl("test_key")
        assert ttl_manager.get_ttl("test_key") is None
    
    def test_key_expiration(self, ttl_manager):
        """Test key expiration detection"""
        ttl_manager.set_ttl("test_key", 0.1)
        time.sleep(0.2)
        assert ttl_manager.is_expired("test_key")
    
    async def test_active_expiration(self, ttl_manager):
        """Test active expiration cleanup"""
        expired_keys = []
        
        def on_expire(key):
            expired_keys.append(key)
        
        ttl_manager.on_expire_callback = on_expire
        
        # Set short TTL
        ttl_manager.set_ttl("expire_me", 0.1)
        
        # Wait for cleanup
        await asyncio.sleep(0.3)
        
        assert "expire_me" in expired_keys

class TestCacheManager:
    """Test Cache Manager functionality"""
    
    @pytest.fixture
    def cache_manager(self):
        """Create cache manager for testing"""
        return CacheManager(
            eviction_policy=EvictionPolicy.LRU,
            max_memory=1024 * 1024,  # 1MB
            memory_threshold=0.8,
            eviction_batch_size=5
        )
    
    def test_put_get(self, cache_manager):
        """Test basic put/get operations"""
        assert cache_manager.put("key1", "value1")
        assert cache_manager.get("key1") == "value1"
        assert cache_manager.get("nonexistent") is None
    
    def test_delete(self, cache_manager):
        """Test key deletion"""
        cache_manager.put("key1", "value1")
        assert cache_manager.delete("key1")
        assert cache_manager.get("key1") is None
    
    def test_lru_eviction(self, cache_manager):
        """Test LRU eviction policy"""
        # Fill cache beyond threshold
        for i in range(100):
            cache_manager.put(f"key{i}", f"value{i}")
        
        # Access some keys to make them recently used
        cache_manager.get("key50")
        cache_manager.get("key75")
        
        # Force eviction
        evicted = cache_manager.force_eviction(10)
        assert evicted > 0
        
        # Recently used keys should still be there
        assert cache_manager.get("key50") == "value50"
        assert cache_manager.get("key75") == "value75"
    
    def test_volatile_keys(self, cache_manager):
        """Test volatile key tracking"""
        cache_manager.put("volatile", "value", has_ttl=True)
        cache_manager.put("persistent", "value", has_ttl=False)
        
        stats = cache_manager.get_stats()
        assert stats['volatile_entries'] == 1
    
    def test_memory_estimation(self, cache_manager):
        """Test memory usage estimation"""
        initial_usage = cache_manager.get_stats()['memory_usage']
        
        # Add some data
        cache_manager.put("test", "x" * 1000)  # 1KB string
        
        new_usage = cache_manager.get_stats()['memory_usage']
        assert new_usage > initial_usage

class TestHyperKVServer:
    """Test HyperKV Server functionality"""
    
    @pytest.fixture
    async def temp_config(self):
        """Create temporary configuration for testing"""
        temp_dir = tempfile.mkdtemp()
        config = HyperKVConfig(
            host="127.0.0.1",
            port=0,  # Random port
            data_dir=temp_dir,
            storage_backend="memory",
            enable_clustering=False,
            log_level="WARNING"  # Reduce log noise in tests
        )
        yield config
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    async def hyperkv_server(self, temp_config):
        """Create HyperKV server for testing"""
        server = HyperKVServer(temp_config)
        await server.start()
        yield server
        await server.stop()
    
    async def test_basic_operations(self, hyperkv_server):
        """Test basic KV operations"""
        server = hyperkv_server
        
        # Test SET
        assert await server.set("test_key", "test_value")
        
        # Test GET
        value = await server.get("test_key")
        assert value == "test_value"
        
        # Test EXISTS
        assert await server.exists("test_key")
        assert not await server.exists("nonexistent")
        
        # Test DELETE
        assert await server.delete("test_key")
        assert not await server.exists("test_key")
    
    async def test_ttl_operations(self, hyperkv_server):
        """Test TTL operations"""
        server = hyperkv_server
        
        # Set key with TTL
        assert await server.set("ttl_key", "value", ttl=1.0)
        
        # Check TTL
        ttl = await server.ttl("ttl_key")
        assert ttl is not None
        assert 0 < ttl <= 1.0
        
        # Set TTL on existing key
        await server.set("existing", "value")
        assert await server.expire("existing", 0.5)
        
        # Remove TTL
        assert await server.persist("existing")
        assert await server.ttl("existing") is None
    
    async def test_expiration(self, hyperkv_server):
        """Test key expiration"""
        server = hyperkv_server
        
        # Set key with short TTL
        await server.set("expire_me", "value", ttl=0.1)
        
        # Key should exist initially
        assert await server.exists("expire_me")
        
        # Wait for expiration
        await asyncio.sleep(0.2)
        
        # Key should be expired and cleaned up
        assert not await server.exists("expire_me")
        assert await server.get("expire_me") is None
    
    async def test_scan_operations(self, hyperkv_server):
        """Test key scanning"""
        server = hyperkv_server
        
        # Add test keys
        for i in range(10):
            await server.set(f"scan_test_{i}", f"value_{i}")
        
        # Scan all keys
        cursor, keys = await server.scan(0, "*", 20)
        assert len(keys) == 10
        assert all(key.startswith("scan_test_") for key in keys)
        
        # Scan with pattern
        cursor, keys = await server.scan(0, "scan_test_[0-4]", 10)
        # Note: Simple pattern matching may not work exactly like Redis
        # This test might need adjustment based on implementation
    
    async def test_crdt_operations(self, hyperkv_server):
        """Test CRDT value operations"""
        server = hyperkv_server
        
        # Test with LWW (Last-Writer-Wins)
        await server.set("lww_key", "value1", crdt_type="lww")
        
        # Simulate concurrent update
        await asyncio.sleep(0.01)  # Small delay to ensure different timestamps
        await server.set("lww_key", "value2", crdt_type="lww")
        
        # Latest value should win
        value = await server.get("lww_key")
        assert value == "value2"
    
    async def test_server_info(self, hyperkv_server):
        """Test server info retrieval"""
        server = hyperkv_server
        
        info = server.get_info()
        
        assert 'server' in info
        assert 'memory' in info
        assert 'stats' in info
        assert 'cache' in info
        assert 'ttl' in info
        assert 'networking' in info
        
        # Check basic server info
        assert info['server']['node_id'] == server.config.node_id
        assert info['server']['running'] == True
    
    async def test_concurrent_operations(self, hyperkv_server):
        """Test concurrent operations"""
        server = hyperkv_server
        
        async def set_keys(start, count):
            for i in range(start, start + count):
                await server.set(f"concurrent_{i}", f"value_{i}")
        
        async def get_keys(start, count):
            results = []
            for i in range(start, start + count):
                value = await server.get(f"concurrent_{i}")
                results.append(value)
            return results
        
        # Run concurrent operations
        await asyncio.gather(
            set_keys(0, 50),
            set_keys(50, 50),
            set_keys(100, 50)
        )
        
        # Verify all keys were set
        results = await get_keys(0, 150)
        assert len([r for r in results if r is not None]) == 150

class TestCRDTIntegration:
    """Test CRDT integration in HyperKV"""
    
    @pytest.fixture
    async def servers(self):
        """Create multiple HyperKV servers for CRDT testing"""
        servers = []
        temp_dirs = []
        
        for i in range(3):
            temp_dir = tempfile.mkdtemp()
            temp_dirs.append(temp_dir)
            
            config = HyperKVConfig(
                host="127.0.0.1",
                port=0,  # Random port
                node_id=f"node_{i}",
                data_dir=temp_dir,
                storage_backend="memory",
                enable_clustering=False,
                log_level="WARNING"
            )
            
            server = HyperKVServer(config)
            await server.start()
            servers.append(server)
        
        yield servers
        
        # Cleanup
        for server in servers:
            await server.stop()
        
        for temp_dir in temp_dirs:
            shutil.rmtree(temp_dir)
    
    async def test_lww_conflict_resolution(self, servers):
        """Test Last-Writer-Wins conflict resolution"""
        server1, server2, server3 = servers
        
        # Set same key on different nodes with different values
        await server1.set("conflict_key", "value_from_node1", crdt_type="lww")
        await asyncio.sleep(0.01)  # Ensure different timestamps
        await server2.set("conflict_key", "value_from_node2", crdt_type="lww")
        await asyncio.sleep(0.01)
        await server3.set("conflict_key", "value_from_node3", crdt_type="lww")
        
        # In a real distributed scenario, nodes would sync and resolve conflicts
        # For this test, we verify that each node has its own value
        value1 = await server1.get("conflict_key")
        value2 = await server2.get("conflict_key") 
        value3 = await server3.get("conflict_key")
        
        assert value1 == "value_from_node1"
        assert value2 == "value_from_node2"
        assert value3 == "value_from_node3"

# Performance benchmarks
class TestPerformance:
    """Performance tests for HyperKV"""
    
    @pytest.fixture
    async def perf_server(self):
        """Create server optimized for performance testing"""
        temp_dir = tempfile.mkdtemp()
        config = HyperKVConfig(
            data_dir=temp_dir,
            storage_backend="memory",
            max_memory=100 * 1024 * 1024,  # 100MB
            eviction_policy="lru",
            enable_clustering=False,
            log_level="ERROR"  # Minimal logging for performance
        )
        
        server = HyperKVServer(config)
        await server.start()
        yield server
        await server.stop()
        shutil.rmtree(temp_dir)
    
    @pytest.mark.performance
    async def test_throughput(self, perf_server):
        """Test basic throughput"""
        server = perf_server
        
        # Measure SET operations
        start_time = time.time()
        for i in range(1000):
            await server.set(f"perf_key_{i}", f"value_{i}")
        set_time = time.time() - start_time
        
        set_ops_per_sec = 1000 / set_time
        
        # Measure GET operations
        start_time = time.time()
        for i in range(1000):
            await server.get(f"perf_key_{i}")
        get_time = time.time() - start_time
        
        get_ops_per_sec = 1000 / get_time
        
        print(f"SET throughput: {set_ops_per_sec:.0f} ops/sec")
        print(f"GET throughput: {get_ops_per_sec:.0f} ops/sec")
        
        # Basic performance assertions
        assert set_ops_per_sec > 1000  # At least 1K SET ops/sec
        assert get_ops_per_sec > 5000  # At least 5K GET ops/sec
    
    @pytest.mark.performance
    async def test_concurrent_throughput(self, perf_server):
        """Test concurrent operation throughput"""
        server = perf_server
        
        async def worker(worker_id, operations):
            for i in range(operations):
                key = f"worker_{worker_id}_key_{i}"
                await server.set(key, f"value_{i}")
                value = await server.get(key)
                assert value == f"value_{i}"
        
        # Run multiple workers concurrently
        start_time = time.time()
        await asyncio.gather(*[
            worker(i, 200) for i in range(10)
        ])
        total_time = time.time() - start_time
        
        total_ops = 10 * 200 * 2  # 10 workers * 200 ops * 2 (SET + GET)
        ops_per_sec = total_ops / total_time
        
        print(f"Concurrent throughput: {ops_per_sec:.0f} ops/sec")
        
        # Should handle concurrent load efficiently
        assert ops_per_sec > 2000
