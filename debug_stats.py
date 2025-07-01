#!/usr/bin/env python3
"""Debug script to test statistics counting"""

import asyncio
import tempfile
from kvstore.core.server import HyperKVServer
from kvstore.core.config import HyperKVServerConfig


async def main():
    """Test statistics counting"""
    # Use default configuration
    server = HyperKVServer()

    # Initialize core components without network
    await server._load_persisted_data()
    server.ttl_manager.start()
    server._start_background_tasks()
    server._running = True
    server.stats["start_time"] = 0  # Reset stats

    print("Initial stats:", server.stats)

    # Perform operations exactly like the test
    print("\n1. SET stats_test_1")
    await server.set("stats_test_1", "value1")
    print("After SET 1:", server.stats)

    print("\n2. GET stats_test_1")
    await server.get("stats_test_1")
    print("After GET:", server.stats)

    print("\n3. SET stats_test_2")
    await server.set("stats_test_2", "value2")
    print("After SET 2:", server.stats)

    print("\n4. DELETE stats_test_2")
    result = await server.delete("stats_test_2")
    print(f"DELETE result: {result}")
    print("After DELETE:", server.stats)

    print(f"\nFinal total_operations: {server.stats['total_operations']}")
    print(f"Expected: 4, Actual: {server.stats['total_operations']}")

    # Cleanup
    server._running = False
    for task in server._background_tasks:
        task.cancel()
    if server._background_tasks:
        await asyncio.gather(*server._background_tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
