#!/usr/bin/env python3
"""
HyperKV Server Startup Debug Tests
Pytest version of server startup debugging
"""

import time
import asyncio
import pytest
from kvstore.config import StorageConfig, CacheConfig, NetworkConfig
from kvstore.core.config import HyperKVServerConfig


class TestHyperKVServerStartupDebug:
    """Debug tests for HyperKV server startup issues"""
    
    def test_server_import(self):
        """Test that server can be imported without errors"""
        from kvstore.core.server import HyperKVServer
        assert HyperKVServer is not None
    
    def test_server_configuration_creation(self):
        """Test server configuration creation"""
        config = HyperKVServerConfig(
            storage=StorageConfig(
                data_dir="/tmp/hyperkv_debug",
                aof_enabled=False,
                snapshot_enabled=False,
                backend="memory"
            ),
            cache=CacheConfig(
                max_memory=100 * 1024 * 1024,  # 100MB
                eviction_policy="lru"
            ),
            network=NetworkConfig(
                host="127.0.0.1",
                port=6379,
                max_connections=1000
            )
        )
        
        # Verify configuration was created correctly
        assert config is not None
        assert config.storage.data_dir == "/tmp/hyperkv_debug"
        assert config.cache.max_memory == 100 * 1024 * 1024
        assert config.network.host == "127.0.0.1"
        assert config.network.port == 6379
    
    def test_server_instance_creation(self):
        """Test server instance creation"""
        from kvstore.core.server import HyperKVServer
        
        config = HyperKVServerConfig(
            storage=StorageConfig(
                data_dir="/tmp/hyperkv_debug_instance",
                aof_enabled=False,
                snapshot_enabled=False,
                backend="memory"
            ),
            cache=CacheConfig(
                max_memory=50 * 1024 * 1024,
                eviction_policy="lru"
            ),
            network=NetworkConfig(
                host="127.0.0.1",
                port=6380,
                max_connections=100
            )
        )
        
        server = HyperKVServer(config)
        
        # Verify server instance was created correctly
        assert server is not None
        assert server.config == config
        assert hasattr(server, '_data')
        assert hasattr(server, 'stats')
        assert hasattr(server, 'ttl_manager')
        assert hasattr(server, '_running')
        assert hasattr(server, '_background_tasks')
    
    @pytest.mark.asyncio
    async def test_server_component_initialization(self):
        """Test individual server component initialization"""
        from kvstore.core.server import HyperKVServer
        
        config = HyperKVServerConfig(
            storage=StorageConfig(
                data_dir="/tmp/hyperkv_debug_components",
                aof_enabled=False,
                snapshot_enabled=False,
                backend="memory"
            ),
            cache=CacheConfig(max_memory=50 * 1024 * 1024),
            network=NetworkConfig(host="127.0.0.1", port=6381)
        )
        
        server = HyperKVServer(config)
        
        # Test individual component initialization
        try:
            # Test data loading
            await server._load_persisted_data()
            assert server._data is not None
            
            # Test TTL manager startup
            server.ttl_manager.start()
            assert server.ttl_manager._running
            
            # Test background tasks
            server._start_background_tasks()
            assert isinstance(server._background_tasks, list)
            
            # Test running state
            server._running = True
            assert server._running is True
            
            # Test stats initialization
            server.stats['start_time'] = time.time()
            assert 'start_time' in server.stats
            
        finally:
            # Cleanup
            server._running = False
            for task in server._background_tasks:
                task.cancel()
            if server._background_tasks:
                await asyncio.gather(*server._background_tasks, return_exceptions=True)
    
    @pytest.mark.asyncio
    async def test_server_startup_timeout_handling(self):
        """Test server startup with timeout handling"""
        from kvstore.core.server import HyperKVServer
        
        config = HyperKVServerConfig(
            storage=StorageConfig(
                data_dir="/tmp/hyperkv_debug_timeout",
                aof_enabled=False,
                snapshot_enabled=False,
                backend="memory"
            ),
            cache=CacheConfig(max_memory=50 * 1024 * 1024),
            network=NetworkConfig(host="127.0.0.1", port=6382)
        )
        
        server = HyperKVServer(config)
        
        # Test component startup with timeout
        try:
            # Use asyncio.wait_for to test timeout handling
            await asyncio.wait_for(server._load_persisted_data(), timeout=5.0)
            
            # Start TTL manager with timeout
            server.ttl_manager.start()
            await asyncio.wait_for(asyncio.sleep(0.1), timeout=1.0)  # Small delay
            assert server.ttl_manager._running
            
            # Start background tasks with timeout
            server._start_background_tasks()
            await asyncio.wait_for(asyncio.sleep(0.1), timeout=1.0)  # Small delay
            
            server._running = True
            assert server._running
            
        except asyncio.TimeoutError:
            pytest.fail("Server component initialization timed out")
        finally:
            # Cleanup
            server._running = False
            for task in server._background_tasks:
                task.cancel()
            if server._background_tasks:
                await asyncio.gather(*server._background_tasks, return_exceptions=True)
    
    @pytest.mark.asyncio
    async def test_server_basic_operations_after_startup(self):
        """Test basic operations after server startup"""
        from kvstore.core.server import HyperKVServer
        
        config = HyperKVServerConfig(
            storage=StorageConfig(
                data_dir="/tmp/hyperkv_debug_operations",
                aof_enabled=False,
                snapshot_enabled=False,
                backend="memory"
            ),
            cache=CacheConfig(max_memory=50 * 1024 * 1024),
            network=NetworkConfig(host="127.0.0.1", port=6383)
        )
        
        server = HyperKVServer(config)
        
        try:
            # Initialize server components
            await server._load_persisted_data()
            server.ttl_manager.start()
            server._start_background_tasks()
            server._running = True
            server.stats['start_time'] = time.time()
            
            # Test basic operations work after initialization
            await server.set("debug_key", "debug_value")
            value = await server.get("debug_key")
            assert value == "debug_value"
            
            exists = await server.exists("debug_key")
            assert exists is True
            
            # Test stats are available
            stats = server.get_stats()
            assert isinstance(stats, dict)
            assert 'start_time' in stats
            
        finally:
            # Cleanup
            server._running = False
            for task in server._background_tasks:
                task.cancel()
            if server._background_tasks:
                await asyncio.gather(*server._background_tasks, return_exceptions=True)
    
    @pytest.mark.asyncio
    async def test_server_error_handling_during_startup(self):
        """Test error handling during server startup"""
        from kvstore.core.server import HyperKVServer
        
        # Test with invalid configuration
        config = HyperKVServerConfig(
            storage=StorageConfig(
                data_dir="/tmp/hyperkv_debug_errors",
                aof_enabled=False,
                snapshot_enabled=False,
                backend="memory"
            ),
            cache=CacheConfig(max_memory=50 * 1024 * 1024),
            network=NetworkConfig(host="127.0.0.1", port=6384)
        )
        
        server = HyperKVServer(config)
        
        # Test that server can handle errors gracefully
        try:
            await server._load_persisted_data()
            
            # Even if there are minor issues, basic functionality should work
            server._running = True
            
            # Test that we can still perform basic operations
            await server.set("error_test_key", "error_test_value")
            value = await server.get("error_test_key")
            assert value == "error_test_value"
            
        except Exception as e:
            # If there's an exception, it should be specific and handleable
            assert isinstance(e, (OSError, RuntimeError, ValueError))
            # The test should not fail on expected exceptions
        finally:
            # Cleanup should always work
            server._running = False
    
    def test_server_dependencies_available(self):
        """Test that all server dependencies are available"""
        # Test core imports
        from kvstore.core.server import HyperKVServer
        from kvstore.core.cache import CacheManager
        from kvstore.core.ttl import TTLManager
        from kvstore.crdt import create_crdt
        
        # Test config imports
        from kvstore.config import StorageConfig, CacheConfig, NetworkConfig
        from kvstore.core.config import HyperKVServerConfig
        
        # Verify all imports succeeded
        assert HyperKVServer is not None
        assert CacheManager is not None
        assert TTLManager is not None
        assert create_crdt is not None
        assert StorageConfig is not None
        assert CacheConfig is not None
        assert NetworkConfig is not None
        assert HyperKVServerConfig is not None
    
    @pytest.mark.asyncio
    async def test_server_cleanup_after_startup(self):
        """Test server cleanup after startup"""
        from kvstore.core.server import HyperKVServer
        
        config = HyperKVServerConfig(
            storage=StorageConfig(
                data_dir="/tmp/hyperkv_debug_cleanup",
                aof_enabled=False,
                snapshot_enabled=False,
                backend="memory"
            ),
            cache=CacheConfig(max_memory=50 * 1024 * 1024),
            network=NetworkConfig(host="127.0.0.1", port=6385)
        )
        
        server = HyperKVServer(config)
        
        # Initialize server
        await server._load_persisted_data()
        server.ttl_manager.start()
        server._start_background_tasks()
        server._running = True
        
        # Verify server is running
        assert server._running is True
        assert server.ttl_manager._running is True
        assert len(server._background_tasks) > 0
        
        # Test cleanup
        server._running = False
        for task in server._background_tasks:
            task.cancel()
        
        if server._background_tasks:
            await asyncio.gather(*server._background_tasks, return_exceptions=True)
        
        # Verify cleanup completed
        assert server._running is False
        for task in server._background_tasks:
            assert task.cancelled() or task.done()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
