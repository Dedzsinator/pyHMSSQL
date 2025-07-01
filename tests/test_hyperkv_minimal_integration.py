#!/usr/bin/env python3
"""
HyperKV Minimal Integration Tests
Pytest version of minimal integration testing
"""

import asyncio
import time
import pytest
from kvstore.config import StorageConfig, CacheConfig, NetworkConfig
from kvstore.core.config import HyperKVServerConfig
from kvstore.core.server import HyperKVServer


class TestHyperKVMinimalIntegration:
    """Minimal integration tests for HyperKV server"""

    @pytest.fixture
    async def minimal_server(self):
        """Create a minimal server for testing"""
        config = HyperKVServerConfig(
            storage=StorageConfig(
                data_dir="/tmp/hyperkv_minimal_test",
                aof_enabled=False,
                snapshot_enabled=False,
                backend="memory",  # Use memory backend for faster startup
            ),
            cache=CacheConfig(
                max_memory=50 * 1024 * 1024, eviction_policy="lru"  # 50MB
            ),
            network=NetworkConfig(
                host="127.0.0.1",
                port=6380,  # Use different port to avoid conflicts
                max_connections=100,
            ),
        )

        server = HyperKVServer(config)

        # Initialize server components without full network startup
        await server._load_persisted_data()
        server.ttl_manager.start()
        server._start_background_tasks()
        server._running = True
        server.stats["start_time"] = time.time()

        yield server

        # Cleanup
        if server._running:
            server._running = False
            for task in server._background_tasks:
                task.cancel()
            if server._background_tasks:
                await asyncio.gather(*server._background_tasks, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_server_creation_and_config(self):
        """Test server creation and configuration"""
        config = HyperKVServerConfig(
            storage=StorageConfig(
                data_dir="/tmp/hyperkv_config_test",
                aof_enabled=False,
                snapshot_enabled=False,
                backend="memory",
            ),
            cache=CacheConfig(max_memory=50 * 1024 * 1024, eviction_policy="lru"),
            network=NetworkConfig(host="127.0.0.1", port=6380, max_connections=100),
        )

        server = HyperKVServer(config)

        # Verify server was created correctly
        assert server is not None
        assert server.config == config
        assert hasattr(server, "_data")
        assert hasattr(server, "stats")
        assert hasattr(server, "ttl_manager")

    @pytest.mark.asyncio
    async def test_basic_set_get_operations(self, minimal_server):
        """Test basic SET and GET operations"""
        # Test SET operation
        result = await minimal_server.set("test_key", "test_value")
        assert result is True or result == "OK"

        # Test GET operation
        value = await minimal_server.get("test_key")
        assert value == "test_value"

        # Test GET for non-existent key
        non_existent = await minimal_server.get("non_existent_key")
        assert non_existent is None

    @pytest.mark.asyncio
    async def test_exists_operation(self, minimal_server):
        """Test EXISTS operation"""
        # Test exists for non-existent key
        exists_before = await minimal_server.exists("exists_key")
        assert exists_before is False

        # Set a key
        await minimal_server.set("exists_key", "exists_value")

        # Test exists for existing key
        exists_after = await minimal_server.exists("exists_key")
        assert exists_after is True

    @pytest.mark.asyncio
    async def test_delete_operation(self, minimal_server):
        """Test DELETE operation"""
        # Set a key first
        await minimal_server.set("delete_key", "delete_value")

        # Verify it exists
        assert await minimal_server.exists("delete_key") is True

        # Delete the key
        result = await minimal_server.delete("delete_key")
        assert result is True or result == 1

        # Verify it no longer exists
        assert await minimal_server.exists("delete_key") is False
        assert await minimal_server.get("delete_key") is None

    @pytest.mark.asyncio
    async def test_ttl_operations(self, minimal_server):
        """Test TTL operations"""
        # Set a key with TTL
        await minimal_server.set("ttl_key", "ttl_value", ttl=1)  # 1 second TTL

        # Verify key exists immediately
        assert await minimal_server.exists("ttl_key") is True
        assert await minimal_server.get("ttl_key") == "ttl_value"

        # Wait for TTL to expire
        await asyncio.sleep(1.1)

        # Verify key has expired
        assert await minimal_server.exists("ttl_key") is False
        assert await minimal_server.get("ttl_key") is None

    @pytest.mark.asyncio
    async def test_multiple_operations_sequence(self, minimal_server):
        """Test a sequence of multiple operations"""
        operations = [
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
        ]

        # Set multiple keys
        for key, value in operations:
            result = await minimal_server.set(key, value)
            assert result is True or result == "OK"

        # Get all keys and verify values
        for key, expected_value in operations:
            actual_value = await minimal_server.get(key)
            assert actual_value == expected_value

        # Check all keys exist
        for key, _ in operations:
            exists = await minimal_server.exists(key)
            assert exists is True

        # Delete all keys
        for key, _ in operations:
            result = await minimal_server.delete(key)
            assert result is True or result == 1

        # Verify all keys are gone
        for key, _ in operations:
            exists = await minimal_server.exists(key)
            assert exists is False

    @pytest.mark.asyncio
    async def test_server_stats(self, minimal_server):
        """Test server statistics"""
        # Perform some operations to generate stats
        await minimal_server.set("stats_key1", "stats_value1")
        await minimal_server.get("stats_key1")
        await minimal_server.exists("stats_key1")

        # Get server stats
        stats = minimal_server.get_stats()

        # Verify stats structure
        assert isinstance(stats, dict)
        assert len(stats) > 0

        # Check for common stat fields
        if "start_time" in stats:
            assert isinstance(stats["start_time"], (int, float))
            assert stats["start_time"] > 0

    @pytest.mark.asyncio
    async def test_concurrent_operations(self, minimal_server):
        """Test concurrent operations"""
        num_concurrent = 10

        async def worker(worker_id: int):
            """Worker function for concurrent operations"""
            key = f"concurrent_key_{worker_id}"
            value = f"concurrent_value_{worker_id}"

            # Set the key
            await minimal_server.set(key, value)

            # Get the key
            retrieved_value = await minimal_server.get(key)
            assert retrieved_value == value

            # Check existence
            exists = await minimal_server.exists(key)
            assert exists is True

            return worker_id

        # Run concurrent workers
        tasks = [asyncio.create_task(worker(i)) for i in range(num_concurrent)]
        results = await asyncio.gather(*tasks)

        # Verify all workers completed successfully
        assert len(results) == num_concurrent
        assert set(results) == set(range(num_concurrent))

        # Verify all keys exist
        for i in range(num_concurrent):
            key = f"concurrent_key_{i}"
            exists = await minimal_server.exists(key)
            assert exists is True

    @pytest.mark.asyncio
    async def test_crdt_basic_operations(self, minimal_server):
        """Test basic CRDT operations"""
        # Test LWW Set operations
        await minimal_server.crdt_add("test_lww_set", "item1")
        contains = await minimal_server.crdt_contains("test_lww_set", "item1")
        assert contains is True

        # Add more items
        await minimal_server.crdt_add("test_lww_set", "item2")
        await minimal_server.crdt_add("test_lww_set", "item3")

        # Check all items
        for item in ["item1", "item2", "item3"]:
            contains = await minimal_server.crdt_contains("test_lww_set", item)
            assert contains is True

        # Test Counter operations
        await minimal_server.crdt_increment("test_counter", 5)
        value = await minimal_server.crdt_value("test_counter")
        assert value == 5

        await minimal_server.crdt_increment("test_counter", 3)
        value = await minimal_server.crdt_value("test_counter")
        assert value == 8

        await minimal_server.crdt_decrement("test_counter", 2)
        value = await minimal_server.crdt_value("test_counter")
        assert value == 6

    @pytest.mark.asyncio
    async def test_error_handling(self, minimal_server):
        """Test error handling for edge cases"""
        # Test operations on empty keys
        empty_get = await minimal_server.get("")
        assert empty_get is None

        empty_exists = await minimal_server.exists("")
        assert empty_exists is False

        # Test delete on non-existent key
        delete_result = await minimal_server.delete("non_existent_key")
        # Should not raise an exception, might return False or 0
        assert delete_result is not None

        # Test setting empty value
        await minimal_server.set("empty_value_key", "")
        empty_value = await minimal_server.get("empty_value_key")
        assert empty_value == ""

    @pytest.mark.asyncio
    async def test_large_value_handling(self, minimal_server):
        """Test handling of large values"""
        # Create a large value (1KB)
        large_value = "x" * 1024

        # Set large value
        await minimal_server.set("large_key", large_value)

        # Get large value
        retrieved_value = await minimal_server.get("large_key")
        assert retrieved_value == large_value
        assert len(retrieved_value) == 1024

        # Verify operations work with large values
        exists = await minimal_server.exists("large_key")
        assert exists is True

        # Delete large value
        delete_result = await minimal_server.delete("large_key")
        assert delete_result is True or delete_result == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
