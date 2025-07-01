#!/usr/bin/env python3
"""
Test script to verify HyperKV server startup and basic functionality.
"""
import asyncio
import tempfile
import shutil
import sys
import os
import json
import time
import logging

# Add the project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kvstore.core.server import HyperKVServer
from kvstore.core.config import HyperKVServerConfig

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def test_server_startup():
    """Test basic server startup and operations"""
    logger.info("Testing HyperKV server startup...")

    # Create temporary directory for testing
    temp_dir = tempfile.mkdtemp(prefix="hyperkv_test_")
    logger.info(f"Using temporary directory: {temp_dir}")

    try:
        # Create configuration
        config = HyperKVServerConfig()
        config.storage.data_dir = temp_dir
        config.network.host = "127.0.0.1"
        config.network.port = 6380  # Non-standard port for testing
        config.cache.max_memory = 100 * 1024 * 1024  # 100MB
        config.crdt.ttl_check_interval = 1.0
        config.storage.snapshot_enabled = True
        config.storage.aof_enabled = True
        config.storage.backend = "memory"

        # Initialize server
        logger.info("Initializing HyperKV server...")
        server = HyperKVServer(config)

        # Start server
        logger.info("Starting server...")
        await server.start()
        logger.info("Server started successfully!")

        # Test basic operations
        logger.info("Testing basic operations...")

        # Test SET operation
        result = await server.set("test_key", "test_value")
        assert result, "Failed to set key"
        logger.info("✓ SET operation successful")

        # Test GET operation
        value = await server.get("test_key")
        assert value == "test_value", f"Expected 'test_value', got {value}"
        logger.info("✓ GET operation successful")

        # Test SET with TTL
        result = await server.set("ttl_key", "ttl_value", ttl=2.0)
        assert result, "Failed to set key with TTL"
        logger.info("✓ SET with TTL successful")

        # Test TTL checking
        ttl = await server.ttl("ttl_key")
        assert ttl is not None and ttl > 0, f"Expected positive TTL, got {ttl}"
        logger.info(f"✓ TTL check successful: {ttl:.2f}s remaining")

        # Test EXISTS operation
        exists = await server.exists("test_key")
        assert exists, "Key should exist"
        logger.info("✓ EXISTS operation successful")

        # Test DELETE operation
        deleted = await server.delete("test_key")
        assert deleted, "Failed to delete key"
        logger.info("✓ DELETE operation successful")

        # Verify key was deleted
        value = await server.get("test_key")
        assert value is None, f"Expected None after delete, got {value}"
        logger.info("✓ Key deletion verified")

        # Test SCAN operation
        await server.set("scan1", "value1")
        await server.set("scan2", "value2")
        await server.set("other", "value3")

        cursor, keys = await server.scan(0, "scan*", 10)
        assert len(keys) == 2, f"Expected 2 keys matching 'scan*', got {len(keys)}"
        assert all(
            k.startswith("scan") for k in keys
        ), f"All keys should start with 'scan': {keys}"
        logger.info(f"✓ SCAN operation successful: {keys}")

        # Test statistics
        stats = server.get_stats()
        assert isinstance(stats, dict), "Stats should be a dictionary"
        assert "stats" in stats, "Response should include stats section"
        assert (
            "total_operations" in stats["stats"]
        ), "Stats should include total_operations"
        assert (
            stats["stats"]["total_operations"] > 0
        ), "Should have some operations recorded"
        logger.info(
            f"✓ Statistics: {stats['stats']['total_operations']} operations, {stats['stats'].get('memory_usage', 0)} bytes memory"
        )

        # Test persistence (create a snapshot)
        snapshot_id = await server.storage.create_snapshot()
        assert snapshot_id is not None, "Should be able to create snapshot"
        logger.info(f"✓ Snapshot created: {snapshot_id}")

        # Test server shutdown
        logger.info("Stopping server...")
        await server.stop()
        logger.info("✓ Server stopped successfully!")

        logger.info("🎉 All tests passed!")
        return True

    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        # Cleanup
        try:
            shutil.rmtree(temp_dir)
            logger.info(f"Cleaned up temporary directory: {temp_dir}")
        except Exception as e:
            logger.warning(f"Failed to cleanup {temp_dir}: {e}")


async def test_persistence():
    """Test data persistence across server restarts"""
    logger.info("Testing data persistence...")

    temp_dir = tempfile.mkdtemp(prefix="hyperkv_persist_")
    logger.info(f"Using temporary directory: {temp_dir}")

    try:
        config = HyperKVServerConfig()
        config.storage.data_dir = temp_dir
        config.network.host = "127.0.0.1"
        config.network.port = 6381
        config.storage.backend = "memory"
        config.storage.snapshot_enabled = True
        config.storage.aof_enabled = True

        # First server instance
        logger.info("Starting first server instance...")
        server1 = HyperKVServer(config)
        await server1.start()

        # Add some data
        await server1.set("persist1", "value1")
        await server1.set("persist2", "value2")
        await server1.set("persist3", {"nested": "object"})

        # Create snapshot
        await server1.storage.create_snapshot()

        # Stop server
        await server1.stop()
        logger.info("First server stopped")

        # Second server instance (should load persisted data)
        logger.info("Starting second server instance...")
        server2 = HyperKVServer(config)
        await server2.start()

        # Check if data was restored
        value1 = await server2.get("persist1")
        value2 = await server2.get("persist2")
        value3 = await server2.get("persist3")

        assert value1 == "value1", f"Expected 'value1', got {value1}"
        assert value2 == "value2", f"Expected 'value2', got {value2}"
        logger.info("✓ Data persistence verified")

        await server2.stop()
        logger.info("✓ Persistence test passed!")
        return True

    except Exception as e:
        logger.error(f"❌ Persistence test failed: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        try:
            shutil.rmtree(temp_dir)
        except Exception as e:
            logger.warning(f"Failed to cleanup {temp_dir}: {e}")


async def main():
    """Run all tests"""
    logger.info("Starting HyperKV server tests...")

    # Test basic startup and operations
    startup_success = await test_server_startup()

    # Test persistence
    persistence_success = await test_persistence()

    if startup_success and persistence_success:
        logger.info("🎉 All tests completed successfully!")
        return 0
    else:
        logger.error("❌ Some tests failed!")
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Tests interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
