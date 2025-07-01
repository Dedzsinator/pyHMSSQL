#!/usr/bin/env python3
"""
HyperKV Core Components Tests
Pytest version of core components testing without network layer
"""

import asyncio
import time
import pytest
from kvstore.config import StorageConfig, CacheConfig, NetworkConfig
from kvstore.core.config import HyperKVServerConfig
from kvstore.core.server import HyperKVServer
from kvstore.core.cache import CacheManager, EvictionPolicy
from kvstore.core.ttl import TTLManager
from kvstore.crdt import create_crdt, HybridLogicalClock


class TestHyperKVCoreComponents:
    """Test core components without network layer"""

    def test_basic_imports(self):
        """Test that all basic imports work"""
        # These imports should not raise any exceptions
        from kvstore.core.server import HyperKVServer
        from kvstore.core.cache import CacheManager, EvictionPolicy
        from kvstore.core.ttl import TTLManager
        from kvstore.crdt import create_crdt, HybridLogicalClock

        # Verify classes exist
        assert HyperKVServer is not None
        assert CacheManager is not None
        assert TTLManager is not None
        assert create_crdt is not None
        assert HybridLogicalClock is not None

    def test_ttl_manager_functionality(self):
        """Test TTL manager basic functionality"""
        ttl_manager = TTLManager()
        current_time = time.time()

        # Set TTL for a key
        ttl_manager.set_ttl("test_key", 10)  # 10 seconds from now
        ttl_value = ttl_manager.get_ttl("test_key")

        # Verify TTL was set correctly
        assert ttl_value is not None
        assert ttl_value > 9  # Should be close to 10 seconds
        assert ttl_value <= 10

        # Test TTL removal
        ttl_manager.remove_ttl("test_key")
        assert ttl_manager.get_ttl("test_key") is None

        # Test TTL checking with future expiration
        ttl_manager.set_ttl("valid_key", 100)  # Not expired
        assert not ttl_manager.is_expired("valid_key")

        # Test expired TTL by setting it to a very small value and waiting
        ttl_manager.set_ttl("expire_key", 0.001)  # Very short TTL
        time.sleep(0.002)  # Wait for it to expire
        assert ttl_manager.is_expired("expire_key")

    def test_cache_manager_functionality(self):
        """Test cache manager basic functionality"""
        cache_manager = CacheManager(
            max_memory=1024 * 1024, eviction_policy=EvictionPolicy.LRU  # 1MB
        )

        # Test basic put/get
        cache_manager.put("cache_key", "cache_value")
        cache_value = cache_manager.get("cache_key")
        assert cache_value == "cache_value"

        # Test key existence (using get method since exists() doesn't exist)
        assert cache_manager.get("cache_key") is not None
        assert cache_manager.get("nonexistent_key") is None

        # Test cache deletion
        cache_manager.delete("cache_key")
        assert cache_manager.get("cache_key") is None

        # Test cache statistics
        stats = cache_manager.get_stats()
        assert isinstance(stats, dict)
        assert "hit_rate" in stats

    def test_hybrid_logical_clock(self):
        """Test Hybrid Logical Clock functionality"""
        hlc = HybridLogicalClock("test_node")

        # Test basic tick functionality
        ts1 = hlc.tick()
        ts2 = hlc.tick()

        assert ts2.logical > ts1.logical
        assert ts1.node_id == "test_node"
        assert ts2.node_id == "test_node"

        # Test comparison
        assert ts2 > ts1
        assert ts1 < ts2

        # Test string representation
        ts_str = str(ts1)
        assert "test_node" in ts_str

    def test_lww_set_crdt(self):
        """Test LWW Set CRDT functionality"""
        lww_set = create_crdt("lww", None, "test_node")

        # Test add operation
        lww_set.add("item1")
        assert lww_set.contains("item1")

        # Test multiple adds
        lww_set.add("item2")
        lww_set.add("item3")
        assert lww_set.contains("item2")
        assert lww_set.contains("item3")

        # Test remove operation
        lww_set.remove("item1")
        assert not lww_set.contains("item1")

        # Test value retrieval
        value = lww_set.value()
        assert isinstance(value, set)
        assert "item2" in value
        assert "item3" in value
        assert "item1" not in value

    def test_counter_crdt(self):
        """Test Counter CRDT functionality"""
        counter = create_crdt("counter", 0, "test_node")

        # Test initial value
        assert counter.value() == 0

        # Test increment
        counter.increment(1)
        assert counter.value() == 1

        counter.increment(5)
        assert counter.value() == 6

        # Test decrement
        counter.decrement(2)
        assert counter.value() == 4

    def test_server_creation(self):
        """Test server creation without starting"""
        config = HyperKVServerConfig(
            storage=StorageConfig(
                data_dir="/tmp/hyperkv_core_test",
                aof_enabled=False,
                snapshot_enabled=False,
                backend="memory",
            ),
            cache=CacheConfig(
                max_memory=100 * 1024 * 1024, eviction_policy="lru"  # 100MB
            ),
            network=NetworkConfig(host="127.0.0.1", port=6379, max_connections=1000),
        )

        # Server creation should not raise an exception
        server = HyperKVServer(config)
        assert server is not None
        assert server.config == config

        # Test basic server properties
        assert hasattr(server, "_data")
        assert hasattr(server, "stats")
        assert hasattr(server, "ttl_manager")

        # Test stats retrieval
        stats = server.get_stats()
        assert isinstance(stats, dict)

    @pytest.mark.asyncio
    async def test_server_data_operations(self):
        """Test direct server data operations"""
        config = HyperKVServerConfig(
            storage=StorageConfig(
                data_dir="/tmp/hyperkv_test_data_ops",
                aof_enabled=False,
                snapshot_enabled=False,
                backend="memory",
            ),
            cache=CacheConfig(max_memory=50 * 1024 * 1024),
            network=NetworkConfig(host="127.0.0.1", port=6379),
        )

        server = HyperKVServer(config)

        # Test direct data operations
        server._data["direct_key"] = "direct_value"
        value = server._data.get("direct_key")
        assert value == "direct_value"

        # Test server initialization without network
        await server._load_persisted_data()
        assert server._data is not None

        # Test stats after initialization
        stats = server.get_stats()
        assert isinstance(stats, dict)
        assert len(stats) > 0


class TestHyperKVServerNoNetwork:
    """Test server components without network layer"""

    @pytest.fixture
    async def server_config(self):
        """Server configuration for testing"""
        return HyperKVServerConfig(
            storage=StorageConfig(
                data_dir="/tmp/hyperkv_no_network_test",
                aof_enabled=False,
                snapshot_enabled=False,
                backend="memory",
            ),
            cache=CacheConfig(max_memory=50 * 1024 * 1024, eviction_policy="lru"),
            network=NetworkConfig(host="127.0.0.1", port=6380, max_connections=100),
        )

    @pytest.mark.asyncio
    async def test_server_component_initialization(self, server_config):
        """Test server component initialization without network"""
        server = HyperKVServer(server_config)

        # Test individual component initialization
        await server._load_persisted_data()
        assert server._data is not None

        # Test TTL manager startup
        server.ttl_manager.start()
        assert server.ttl_manager._running

        # Test background tasks startup
        server._start_background_tasks()
        assert len(server._background_tasks) > 0

        # Cleanup
        for task in server._background_tasks:
            task.cancel()
        await asyncio.gather(*server._background_tasks, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_server_basic_operations(self, server_config):
        """Test basic server operations without network"""
        server = HyperKVServer(server_config)

        # Initialize server components
        await server._load_persisted_data()
        server.ttl_manager.start()
        server._start_background_tasks()
        server._running = True

        try:
            # Test SET operation
            await server.set("test_key", "test_value")

            # Test GET operation
            value = await server.get("test_key")
            assert value == "test_value"

            # Test EXISTS operation
            exists = await server.exists("test_key")
            assert exists is True

            # Test DELETE operation
            await server.delete("test_key")
            exists_after_delete = await server.exists("test_key")
            assert exists_after_delete is False

            # Test SET with TTL
            await server.set("ttl_key", "ttl_value", ttl=1)
            value = await server.get("ttl_key")
            assert value == "ttl_value"

            # Wait for TTL expiration
            await asyncio.sleep(1.1)
            expired_value = await server.get("ttl_key")
            assert expired_value is None

        finally:
            # Cleanup
            server._running = False
            for task in server._background_tasks:
                task.cancel()
            await asyncio.gather(*server._background_tasks, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_server_crdt_operations(self, server_config):
        """Test server CRDT operations without network"""
        server = HyperKVServer(server_config)

        # Initialize server
        await server._load_persisted_data()
        server._running = True

        try:
            # Test LWW Set operations
            await server.crdt_add("lww_set", "item1")
            contains = await server.crdt_contains("lww_set", "item1")
            assert contains is True

            # Test Counter operations
            await server.crdt_increment("counter", 5)
            value = await server.crdt_value("counter")
            assert value == 5

            await server.crdt_increment("counter", 3)
            value = await server.crdt_value("counter")
            assert value == 8

        finally:
            server._running = False

    @pytest.mark.asyncio
    async def test_server_stats_and_monitoring(self, server_config):
        """Test server statistics and monitoring without network"""
        server = HyperKVServer(server_config)

        # Initialize server
        await server._load_persisted_data()
        server._running = True
        server.stats["start_time"] = time.time()

        try:
            # Perform some operations to generate stats
            for i in range(10):
                await server.set(f"stats_key_{i}", f"value_{i}")
                await server.get(f"stats_key_{i}")

            # Check statistics
            stats = server.get_stats()
            assert isinstance(stats, dict)
            assert "start_time" in stats

            # Test memory usage stats
            if "memory_usage" in stats:
                assert stats["memory_usage"] >= 0

            # Test operation counts
            if "operations" in stats:
                operations = stats["operations"]
                assert isinstance(operations, dict)

        finally:
            server._running = False


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
