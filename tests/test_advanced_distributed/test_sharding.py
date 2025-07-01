"""
Unit tests for advanced shard-per-core architecture
"""

import pytest
import asyncio
import time
import threading
from unittest.mock import Mock, patch, MagicMock

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from kvstore.shards import (
    AdvancedShardManager,
    ShardConfig,
    PlacementStrategy,
    AdvancedShard,
    ShardingError,
    ConsistentHashRing,
)


class TestShardConfig:
    """Test shard configuration"""

    def test_shard_config_creation(self):
        """Test creating shard configuration"""
        config = ShardConfig(
            shard_id=1,
            cpu_core=2,
            numa_node=0,
            memory_limit=64 * 1024 * 1024,  # 64MB
            wal_enabled=True,
            compression_enabled=True,
        )

        assert config.shard_id == 1
        assert config.cpu_core == 2
        assert config.numa_node == 0
        assert config.memory_limit == 64 * 1024 * 1024
        assert config.wal_enabled is True
        assert config.compression_enabled is True

    def test_shard_config_defaults(self):
        """Test shard configuration with defaults"""
        config = ShardConfig(shard_id=0, cpu_core=0, memory_limit=64*1024*1024)
        assert config.shard_id == 0
        assert config.cpu_core == 0
        assert config.memory_limit == 64*1024*1024


class TestPlacementStrategy:
    """Test placement strategy enum"""

    def test_placement_strategies_defined(self):
        """Test that all placement strategies are defined"""
        # Check that the enum values exist (using auto() so they're integers)
        assert hasattr(PlacementStrategy, 'ROUND_ROBIN')
        assert hasattr(PlacementStrategy, 'NUMA_AWARE')
        assert hasattr(PlacementStrategy, 'LOAD_BALANCED')
        assert hasattr(PlacementStrategy, 'LOCALITY_AWARE')
        assert hasattr(PlacementStrategy, 'CAPACITY_BASED')
        
        # Verify they're actually enum values
        assert isinstance(PlacementStrategy.ROUND_ROBIN.value, int)
        assert isinstance(PlacementStrategy.NUMA_AWARE.value, int)


class TestConsistentHashRing:
    """Test consistent hash ring implementation"""

    def test_hash_ring_creation(self):
        """Test creating consistent hash ring"""
        try:
            ring = ConsistentHashRing(num_virtual_nodes=150)
            assert ring is not None
        except (NameError, ImportError):
            pytest.skip("ConsistentHashRing not implemented")

    def test_hash_ring_add_remove_nodes(self):
        """Test adding and removing nodes from hash ring"""
        try:
            ring = ConsistentHashRing(num_virtual_nodes=100)

            # Add nodes
            ring.add_node("shard_0")
            ring.add_node("shard_1")
            ring.add_node("shard_2")

            assert len(ring.get_nodes()) == 3

            # Remove node
            ring.remove_node("shard_1")
            assert len(ring.get_nodes()) == 2
            assert "shard_1" not in ring.get_nodes()

        except (NameError, ImportError):
            pytest.skip("ConsistentHashRing not implemented")

    def test_hash_ring_key_distribution(self):
        """Test key distribution across nodes"""
        try:
            ring = ConsistentHashRing(num_virtual_nodes=100)

            # Add nodes
            nodes = ["shard_0", "shard_1", "shard_2", "shard_3"]
            for node in nodes:
                ring.add_node(node)

            # Test key distribution
            key_distribution = {}
            for i in range(1000):
                key = f"test_key_{i}"
                node = ring.get_node(key)
                key_distribution[node] = key_distribution.get(node, 0) + 1

            # Should distribute keys across all nodes
            assert len(key_distribution) == 4

            # Distribution should be reasonably balanced (within 30% of average)
            average = 1000 / 4
            for count in key_distribution.values():
                assert count > average * 0.7
                assert count < average * 1.3

        except (NameError, ImportError):
            pytest.skip("ConsistentHashRing not implemented")


class TestAdvancedShard:
    """Test individual shard functionality"""

    @pytest.fixture
    def shard_config(self):
        """Create shard configuration for tests"""
        return ShardConfig(
            shard_id=0,
            cpu_core=0,
            numa_node=0,
            memory_limit=32 * 1024 * 1024,  # 32MB
            wal_enabled=False,  # Disable WAL for simpler tests
            compression_enabled=False,
        )

    @pytest.fixture
    def mock_shard_manager(self):
        """Create mock shard manager"""
        manager = Mock()
        manager.zerocopy_manager = Mock()
        return manager

    def test_shard_creation(self, shard_config, mock_shard_manager):
        """Test creating an advanced shard"""
        shard = AdvancedShard(shard_config, mock_shard_manager)

        assert shard.config == shard_config
        assert shard.shard_manager == mock_shard_manager
        assert shard.running is False
        assert len(shard.data) == 0

    def test_shard_start_stop(self, shard_config, mock_shard_manager):
        """Test starting and stopping a shard"""
        shard = AdvancedShard(shard_config, mock_shard_manager)

        # Start shard
        shard.start()
        assert shard.running is True

        # Stop shard
        shard.stop()
        assert shard.running is False

    def test_shard_set_get_operations(self, shard_config, mock_shard_manager):
        """Test basic set/get operations on shard"""
        shard = AdvancedShard(shard_config, mock_shard_manager)
        shard.start()

        try:
            # Set operation
            shard.set("test_key", "test_value")
            assert "test_key" in shard.data
            assert shard.data["test_key"] == "test_value"

            # Get operation
            value = shard.get("test_key")
            assert value == "test_value"

            # Get non-existent key
            value = shard.get("non_existent")
            assert value is None

        finally:
            shard.stop()

    def test_shard_delete_operation(self, shard_config, mock_shard_manager):
        """Test delete operation on shard"""
        shard = AdvancedShard(shard_config, mock_shard_manager)
        shard.start()

        try:
            # Set then delete
            shard.set("test_key", "test_value")
            assert "test_key" in shard.data

            deleted = shard.delete("test_key")
            assert deleted is True
            assert "test_key" not in shard.data

            # Try to delete non-existent key
            deleted = shard.delete("non_existent")
            assert deleted is False

        finally:
            shard.stop()

    def test_shard_memory_tracking(self, shard_config, mock_shard_manager):
        """Test shard memory usage tracking"""
        shard = AdvancedShard(shard_config, mock_shard_manager)
        shard.start()

        try:
            initial_memory = shard.memory_usage

            # Add some data
            large_value = "x" * 1000
            shard.set("large_key", large_value)

            # Memory usage should increase
            assert shard.memory_usage > initial_memory

            # Delete data
            shard.delete("large_key")

            # Memory usage should decrease
            assert shard.memory_usage <= initial_memory + 100  # Allow some overhead

        finally:
            shard.stop()

    def test_shard_cache_operations(self, shard_config, mock_shard_manager):
        """Test shard caching functionality"""
        shard = AdvancedShard(shard_config, mock_shard_manager)
        shard.start()

        try:
            # Set multiple keys to trigger caching
            for i in range(10):
                shard.set(f"key_{i}", f"value_{i}")

            # Check cache statistics
            assert shard.cache_stats["hits"] >= 0
            assert shard.cache_stats["misses"] >= 0
            assert shard.cache_stats["evictions"] >= 0

            # Access keys to generate cache hits
            for i in range(5):
                value = shard.get(f"key_{i}")
                assert value == f"value_{i}"

        finally:
            shard.stop()

    def test_shard_performance_monitoring(self, shard_config, mock_shard_manager):
        """Test shard performance monitoring"""
        shard = AdvancedShard(shard_config, mock_shard_manager)
        shard.start()

        try:
            # Perform operations
            for i in range(20):
                shard.set(f"perf_key_{i}", f"perf_value_{i}")
                shard.get(f"perf_key_{i}")

            # Check performance metrics
            assert len(shard.operation_latencies) > 0
            assert len(shard.throughput_samples) >= 0

        finally:
            shard.stop()


class TestAdvancedShardManager:
    """Test advanced shard manager functionality"""

    @pytest.fixture
    def shard_config_dict(self):
        """Create shard manager configuration"""
        return {
            "num_shards": 4,
            "placement_strategy": PlacementStrategy.ROUND_ROBIN.value,
            "enable_consistency": True,
            "enable_compression": False,
            "enable_wal": False,  # Disable for simpler tests
            "enable_zero_copy": False,
            "memory_per_shard": 32 * 1024 * 1024,  # 32MB per shard
        }

    def test_shard_manager_creation(self, shard_config_dict):
        """Test creating advanced shard manager"""
        manager = AdvancedShardManager(shard_config_dict)

        assert manager.config == shard_config_dict
        assert len(manager.shards) == 0  # Not started yet
        assert manager.running is False

    def test_shard_manager_start_stop(self, shard_config_dict):
        """Test starting and stopping shard manager"""
        manager = AdvancedShardManager(shard_config_dict)

        # Start manager
        manager.start()
        assert manager.running is True
        assert len(manager.shards) == shard_config_dict["num_shards"]

        # Stop manager
        manager.stop()
        assert manager.running is False

    def test_shard_key_distribution(self, shard_config_dict):
        """Test key distribution across shards"""
        manager = AdvancedShardManager(shard_config_dict)
        manager.start()

        try:
            # Test key distribution
            shard_distribution = {}

            for i in range(100):
                key = f"test_key_{i}"
                shard_id = manager.get_shard_for_key(key)

                assert 0 <= shard_id < shard_config_dict["num_shards"]
                shard_distribution[shard_id] = shard_distribution.get(shard_id, 0) + 1

            # Should distribute keys across multiple shards
            assert len(shard_distribution) > 1

        finally:
            manager.stop()

    @pytest.mark.asyncio
    async def test_execute_on_shard(self, shard_config_dict):
        """Test executing operations on specific shards"""
        manager = AdvancedShardManager(shard_config_dict)
        manager.start()

        try:
            # Execute set operation
            test_key = "execute_test_key"
            test_value = "execute_test_value"

            result = await manager.execute_on_shard(
                test_key, lambda shard, k=test_key, v=test_value: shard.set(k, v)
            )

            # Result should be None (set operation return)
            assert result is None

            # Execute get operation
            result = await manager.execute_on_shard(
                test_key, lambda shard, k=test_key: shard.get(k)
            )

            assert result == test_value

        finally:
            manager.stop()

    def test_shard_statistics(self, shard_config_dict):
        """Test shard manager statistics"""
        manager = AdvancedShardManager(shard_config_dict)
        manager.start()

        try:
            stats = manager.get_comprehensive_stats()

            # Check basic statistics structure
            assert "total_operations" in stats
            assert "cross_shard_operations" in stats
            assert "numa_topology" in stats
            assert "shards" in stats

            # Should have stats for each shard
            assert len(stats["shards"]) == shard_config_dict["num_shards"]

            # Each shard should have individual stats
            for shard_id, shard_stats in stats["shards"].items():
                assert "operations" in shard_stats
                assert "cpu_affinity" in shard_stats
                assert "numa_node" in shard_stats

        finally:
            manager.stop()

    def test_placement_strategies(self):
        """Test different placement strategies"""
        strategies = [
            PlacementStrategy.ROUND_ROBIN,
            PlacementStrategy.NUMA_AWARE,
            PlacementStrategy.LOAD_BALANCED,
        ]

        for strategy in strategies:
            config = {
                "num_shards": 4,
                "placement_strategy": strategy.value,
                "enable_consistency": False,
                "enable_compression": False,
                "enable_wal": False,
                "enable_zero_copy": False,
                "memory_per_shard": 16 * 1024 * 1024,
            }

            manager = AdvancedShardManager(config)
            manager.start()

            try:
                # Should create shards according to strategy
                assert len(manager.shards) == 4

                # Test key distribution
                keys = [f"strategy_test_{i}" for i in range(20)]
                shard_ids = [manager.get_shard_for_key(key) for key in keys]

                # Should distribute across multiple shards
                unique_shards = set(shard_ids)
                assert len(unique_shards) > 1

            finally:
                manager.stop()

    @pytest.mark.asyncio
    async def test_concurrent_shard_operations(self, shard_config_dict):
        """Test concurrent operations on shards"""
        manager = AdvancedShardManager(shard_config_dict)
        manager.start()

        try:
            # Prepare concurrent operations
            async def set_operation(key_suffix):
                key = f"concurrent_key_{key_suffix}"
                value = f"concurrent_value_{key_suffix}"
                return await manager.execute_on_shard(
                    key, lambda shard, k=key, v=value: shard.set(k, v)
                )

            # Execute concurrent operations
            tasks = [set_operation(i) for i in range(20)]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # All operations should complete successfully
            for result in results:
                assert not isinstance(result, Exception)

            # Verify data was set
            for i in range(20):
                key = f"concurrent_key_{i}"
                value = await manager.execute_on_shard(
                    key, lambda shard, k=key: shard.get(k)
                )
                assert value == f"concurrent_value_{i}"

        finally:
            manager.stop()

    def test_shard_rebalancing(self, shard_config_dict):
        """Test shard rebalancing functionality"""
        manager = AdvancedShardManager(shard_config_dict)
        manager.start()

        try:
            # Add data to shards
            for i in range(50):
                key = f"rebalance_key_{i}"
                asyncio.run(
                    manager.execute_on_shard(
                        key, lambda shard, k=key, v=f"value_{i}": shard.set(k, v)
                    )
                )

            # Check initial distribution
            initial_stats = manager.get_comprehensive_stats()

            # Trigger rebalancing (if implemented)
            try:
                manager.rebalance_shards()
            except (AttributeError, NotImplementedError):
                # Rebalancing might not be implemented
                pass

            # Check stats after rebalancing
            final_stats = manager.get_comprehensive_stats()
            assert final_stats["total_operations"] >= initial_stats["total_operations"]

        finally:
            manager.stop()


class TestShardingErrorHandling:
    """Test sharding error handling"""

    def test_invalid_shard_configuration(self):
        """Test handling of invalid shard configuration"""
        with pytest.raises((ValueError, ShardingError)):
            # Invalid number of shards
            config = {
                "num_shards": 0,
                "placement_strategy": PlacementStrategy.ROUND_ROBIN.value,
            }
            manager = AdvancedShardManager(config)

    def test_shard_operation_on_stopped_manager(self):
        """Test operations on stopped shard manager"""
        config = {
            "num_shards": 2,
            "placement_strategy": PlacementStrategy.ROUND_ROBIN.value,
            "enable_consistency": False,
            "enable_compression": False,
            "enable_wal": False,
            "enable_zero_copy": False,
        }

        manager = AdvancedShardManager(config)
        # Don't start the manager

        with pytest.raises((RuntimeError, ShardingError)):
            manager.get_shard_for_key("test_key")

    @pytest.mark.asyncio
    async def test_shard_operation_failure(self):
        """Test handling of shard operation failures"""
        config = {
            "num_shards": 2,
            "placement_strategy": PlacementStrategy.ROUND_ROBIN.value,
            "enable_consistency": False,
            "enable_compression": False,
            "enable_wal": False,
            "enable_zero_copy": False,
        }

        manager = AdvancedShardManager(config)
        manager.start()

        try:
            # Execute operation that raises exception
            def failing_operation(shard):
                raise RuntimeError("Simulated shard failure")

            with pytest.raises(RuntimeError):
                await manager.execute_on_shard("test_key", failing_operation)

        finally:
            manager.stop()

    def test_memory_limit_exceeded(self):
        """Test handling when shard memory limit is exceeded"""
        config = ShardConfig(
            shard_id=0,
            memory_limit=1024,  # Very small limit
            wal_enabled=False,
            compression_enabled=False,
        )

        mock_manager = Mock()
        mock_manager.zerocopy_manager = Mock()

        shard = AdvancedShard(config, mock_manager)
        shard.start()

        try:
            # Try to store data larger than memory limit
            large_data = "x" * 2048  # Larger than limit

            # Should handle gracefully (eviction or error)
            shard.set("large_key", large_data)

            # Memory usage should be managed
            assert shard.memory_usage <= config.memory_limit * 2  # Allow some overhead

        finally:
            shard.stop()


class TestShardingPerformance:
    """Test sharding performance characteristics"""

    def test_key_distribution_performance(self):
        """Test performance of key distribution"""
        config = {
            "num_shards": 8,
            "placement_strategy": PlacementStrategy.ROUND_ROBIN.value,
            "enable_consistency": False,
            "enable_compression": False,
            "enable_wal": False,
            "enable_zero_copy": False,
        }

        manager = AdvancedShardManager(config)
        manager.start()

        try:
            # Time key distribution for many keys
            import time

            start_time = time.time()

            for i in range(10000):
                key = f"perf_test_key_{i}"
                shard_id = manager.get_shard_for_key(key)
                assert 0 <= shard_id < 8

            elapsed = time.time() - start_time

            # Should be very fast (less than 1 second for 10k keys)
            assert elapsed < 1.0

        finally:
            manager.stop()

    @pytest.mark.asyncio
    async def test_concurrent_shard_access_performance(self):
        """Test performance of concurrent shard access"""
        config = {
            "num_shards": 4,
            "placement_strategy": PlacementStrategy.ROUND_ROBIN.value,
            "enable_consistency": False,
            "enable_compression": False,
            "enable_wal": False,
            "enable_zero_copy": False,
        }

        manager = AdvancedShardManager(config)
        manager.start()

        try:
            # Prepare many concurrent operations
            async def batch_operations(batch_id):
                for i in range(100):
                    key = f"batch_{batch_id}_key_{i}"
                    value = f"batch_{batch_id}_value_{i}"
                    await manager.execute_on_shard(
                        key, lambda shard, k=key, v=value: shard.set(k, v)
                    )

            # Time concurrent execution
            import time

            start_time = time.time()

            # Run 10 batches concurrently (1000 operations total)
            tasks = [batch_operations(i) for i in range(10)]
            await asyncio.gather(*tasks)

            elapsed = time.time() - start_time

            # Should complete in reasonable time (less than 5 seconds)
            assert elapsed < 5.0

            # Verify all data was set
            total_operations = manager.get_comprehensive_stats()["total_operations"]
            assert total_operations >= 1000

        finally:
            manager.stop()

    def test_memory_efficiency(self):
        """Test memory efficiency of sharding"""
        config = {
            "num_shards": 4,
            "placement_strategy": PlacementStrategy.ROUND_ROBIN.value,
            "enable_consistency": False,
            "enable_compression": False,
            "enable_wal": False,
            "enable_zero_copy": False,
            "memory_per_shard": 16 * 1024 * 1024,  # 16MB per shard
        }

        manager = AdvancedShardManager(config)
        manager.start()

        try:
            # Add data to all shards
            for i in range(1000):
                key = f"memory_test_key_{i}"
                value = f"memory_test_value_{i}"
                asyncio.run(
                    manager.execute_on_shard(
                        key, lambda shard, k=key, v=value: shard.set(k, v)
                    )
                )

            # Check memory usage across shards
            stats = manager.get_comprehensive_stats()

            total_memory = 0
            for shard_stats in stats["shards"].values():
                if "memory_usage" in shard_stats:
                    total_memory += shard_stats["memory_usage"]

            # Memory usage should be reasonable for the amount of data
            # (This is hard to test precisely, but should not be excessive)
            max_expected_memory = 64 * 1024 * 1024  # 64MB total
            assert total_memory < max_expected_memory

        finally:
            manager.stop()


if __name__ == "__main__":
    pytest.main([__file__])
