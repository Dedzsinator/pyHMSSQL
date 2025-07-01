#!/usr/bin/env python3
"""
Minimal test to debug HyperKV server issues
"""

import asyncio
import tempfile
import logging
from config import HyperKVConfig
from core.config import HyperKVServerConfig
from core.server import HyperKVServer

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_minimal():
    """Minimal test of server startup and basic operations"""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create configuration
        config = HyperKVServerConfig(
            host="127.0.0.1",
            port=6380,
            storage=HyperKVConfig(
                data_dir=temp_dir,
                enable_persistence=True,
                aof_filename="test.aof",
                snapshot_filename="test.rdb",
            ),
            node_id="test-node",
            max_memory=100 * 1024 * 1024,  # 100MB
        )

        logger.info("Creating server...")
        server = HyperKVServer(config)

        try:
            logger.info("Starting server...")
            await server.start()
            logger.info("Server started!")

            logger.info("Testing basic set operation...")
            result = await server.set("test", "value")
            logger.info(f"Set result: {result}")

            logger.info("Testing basic get operation...")
            value = await server.get("test")
            logger.info(f"Get result: {value}")

            logger.info("Testing get_stats...")
            stats = server.get_stats()
            logger.info(f"Stats keys: {list(stats.keys())}")
            logger.info(f"Stats structure: {stats}")

        finally:
            logger.info("Stopping server...")
            await server.stop()
            logger.info("Server stopped!")


if __name__ == "__main__":
    asyncio.run(test_minimal())
