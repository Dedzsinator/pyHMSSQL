"""
Basic usage examples for HyperKV
Demonstrates key features and capabilities.
"""

import asyncio
import time
import logging
from pathlib import Path

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kvstore import HyperKVServer, HyperKVConfig, EvictionPolicy

# Set up logging to see what's happening
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def basic_operations_demo():
    """Demonstrate basic key-value operations"""
    print("\n" + "="*50)
    print("Basic Operations Demo")
    print("="*50)
    
    # Create configuration
    config = HyperKVConfig(
        host="127.0.0.1",
        port=6379,
        data_dir="./demo_data",
        storage_backend="memory",
        max_memory=100 * 1024 * 1024,  # 100MB
        log_level="INFO"
    )
    
    # Create and start server
    server = HyperKVServer(config)
    await server.start()
    
    try:
        # Basic SET and GET
        print("1. Basic SET/GET operations:")
        await server.set("user:1001", "{'name': 'Alice', 'age': 30}")
        value = await server.get("user:1001")
        print(f"   SET user:1001 -> GET: {value}")
        
        # Check existence
        exists = await server.exists("user:1001")
        print(f"   EXISTS user:1001: {exists}")
        
        # DELETE
        deleted = await server.delete("user:1001")
        print(f"   DELETE user:1001: {deleted}")
        
        exists_after = await server.exists("user:1001")
        print(f"   EXISTS user:1001 after delete: {exists_after}")
        
    finally:
        await server.stop()

async def ttl_demo():
    """Demonstrate TTL (Time-To-Live) functionality"""
    print("\n" + "="*50)
    print("TTL (Time-To-Live) Demo")
    print("="*50)
    
    config = HyperKVConfig(
        host="127.0.0.1",
        port=6380,
        data_dir="./demo_data_ttl",
        storage_backend="memory",
        ttl_check_interval=0.5  # Check for expiration every 0.5 seconds
    )
    
    server = HyperKVServer(config)
    await server.start()
    
    try:
        # Set key with TTL
        print("1. Setting key with 2-second TTL:")
        await server.set("temp_key", "This will expire soon", ttl=2.0)
        
        # Check TTL
        ttl = await server.ttl("temp_key")
        print(f"   TTL for temp_key: {ttl:.2f} seconds")
        
        # Wait and check again
        await asyncio.sleep(1)
        ttl = await server.ttl("temp_key")
        print(f"   TTL after 1 second: {ttl:.2f} seconds")
        
        # Wait for expiration
        await asyncio.sleep(1.5)
        value = await server.get("temp_key")
        print(f"   Value after expiration: {value}")
        
        # Set TTL on existing key
        print("\n2. Setting TTL on existing key:")
        await server.set("persistent_key", "This key will get TTL later")
        await server.expire("persistent_key", 1.0)
        
        ttl = await server.ttl("persistent_key")
        print(f"   TTL set to: {ttl:.2f} seconds")
        
        # Remove TTL (make persistent)
        await server.persist("persistent_key")
        ttl = await server.ttl("persistent_key")
        print(f"   TTL after PERSIST: {ttl}")
        
    finally:
        await server.stop()

async def crdt_demo():
    """Demonstrate CRDT (Conflict-free Replicated Data Type) functionality"""
    print("\n" + "="*50)
    print("CRDT Demo")
    print("="*50)
    
    config = HyperKVConfig(
        host="127.0.0.1",
        port=6381,
        data_dir="./demo_data_crdt",
        storage_backend="memory",
        node_id="demo_node_1"
    )
    
    server = HyperKVServer(config)
    await server.start()
    
    try:
        # Last-Writer-Wins (LWW) CRDT
        print("1. Last-Writer-Wins CRDT:")
        await server.set("lww_key", "First value", crdt_type="lww")
        await asyncio.sleep(0.01)  # Small delay to ensure different timestamps
        await server.set("lww_key", "Second value", crdt_type="lww")
        
        value = await server.get("lww_key")
        print(f"   Final value (latest wins): {value}")
        
        # OR-Set CRDT (for sets)
        print("\n2. OR-Set CRDT (for set operations):")
        # Note: This would require extending the server to support set operations
        # For now, we'll just demonstrate the concept
        await server.set("user_tags", "['python', 'databases']", crdt_type="orset")
        value = await server.get("user_tags")
        print(f"   Set value: {value}")
        
    finally:
        await server.stop()

async def pubsub_demo():
    """Demonstrate Pub/Sub functionality"""
    print("\n" + "="*50)
    print("Pub/Sub Demo")
    print("="*50)
    
    config = HyperKVConfig(
        host="127.0.0.1",
        port=6382,
        data_dir="./demo_data_pubsub",
        storage_backend="memory"
    )
    
    server = HyperKVServer(config)
    await server.start()
    
    try:
        # Message handler
        received_messages = []
        
        async def message_handler(channel, message):
            received_messages.append((channel, message))
            print(f"   Received on {channel}: {message}")
        
        # Subscribe to channel
        print("1. Subscribing to 'news' channel:")
        await server.subscribe("client_1", ["news"], message_handler)
        
        # Publish messages
        print("\n2. Publishing messages:")
        await server.publish("news", "Breaking: HyperKV is awesome!")
        await server.publish("news", "Update: Performance benchmarks completed")
        
        # Give some time for message delivery
        await asyncio.sleep(0.1)
        
        print(f"\n3. Messages received: {len(received_messages)}")
        
        # Unsubscribe
        await server.unsubscribe("client_1", ["news"])
        
        # Publish after unsubscribe (should not be received)
        await server.publish("news", "This message won't be received")
        await asyncio.sleep(0.1)
        
        print(f"   Messages after unsubscribe: {len(received_messages)}")
        
    finally:
        await server.stop()

async def cache_eviction_demo():
    """Demonstrate cache eviction policies"""
    print("\n" + "="*50)
    print("Cache Eviction Demo")
    print("="*50)
    
    config = HyperKVConfig(
        host="127.0.0.1",
        port=6383,
        data_dir="./demo_data_cache",
        storage_backend="memory",
        max_memory=1024 * 1024,  # 1MB to trigger eviction
        memory_usage_threshold=0.7,
        eviction_policy="lru",
        eviction_batch_size=3
    )
    
    server = HyperKVServer(config)
    await server.start()
    
    try:
        # Fill cache with data
        print("1. Filling cache with 100 keys:")
        for i in range(100):
            # Create 10KB values to quickly fill memory
            large_value = f"Large value {i}: " + "x" * 10000
            await server.set(f"cache_key_{i}", large_value)
        
        # Check cache stats
        cache_stats = server.cache_manager.get_stats()
        print(f"   Cache entries: {cache_stats['current_entries']}")
        print(f"   Evictions: {cache_stats['evictions']}")
        print(f"   Hit rate: {cache_stats['hit_rate']:.2%}")
        
        # Access some keys to make them "recently used"
        print("\n2. Accessing some keys to demonstrate LRU:")
        for i in [10, 20, 30]:
            value = await server.get(f"cache_key_{i}")
            if value:
                print(f"   Found cache_key_{i}")
        
        # Force more eviction
        for i in range(100, 120):
            large_value = f"New large value {i}: " + "x" * 10000
            await server.set(f"new_cache_key_{i}", large_value)
        
        # Check which keys survived (recently accessed ones should be more likely to survive)
        survivors = []
        for i in [10, 20, 30, 95, 96, 97]:
            if await server.exists(f"cache_key_{i}"):
                survivors.append(i)
        
        print(f"\n3. Keys that survived eviction: {survivors}")
        
        final_stats = server.cache_manager.get_stats()
        print(f"   Final cache entries: {final_stats['current_entries']}")
        print(f"   Total evictions: {final_stats['evictions']}")
        
    finally:
        await server.stop()

async def performance_demo():
    """Demonstrate performance characteristics"""
    print("\n" + "="*50)
    print("Performance Demo")
    print("="*50)
    
    config = HyperKVConfig(
        host="127.0.0.1",
        port=6384,
        data_dir="./demo_data_perf",
        storage_backend="memory",
        max_memory=100 * 1024 * 1024,  # 100MB
        log_level="WARNING"  # Reduce logging for performance test
    )
    
    server = HyperKVServer(config)
    await server.start()
    
    try:
        # Measure SET performance
        print("1. Measuring SET performance (10,000 operations):")
        start_time = time.time()
        
        for i in range(10000):
            await server.set(f"perf_key_{i}", f"value_{i}")
        
        set_duration = time.time() - start_time
        set_ops_per_sec = 10000 / set_duration
        print(f"   SET operations: {set_ops_per_sec:.0f} ops/sec")
        
        # Measure GET performance
        print("\n2. Measuring GET performance (10,000 operations):")
        start_time = time.time()
        
        for i in range(10000):
            await server.get(f"perf_key_{i}")
        
        get_duration = time.time() - start_time
        get_ops_per_sec = 10000 / get_duration
        print(f"   GET operations: {get_ops_per_sec:.0f} ops/sec")
        
        # Measure concurrent performance
        print("\n3. Measuring concurrent performance (5 workers, 2,000 ops each):")
        
        async def worker(worker_id, operations):
            for i in range(operations):
                await server.set(f"concurrent_{worker_id}_{i}", f"value_{i}")
                await server.get(f"concurrent_{worker_id}_{i}")
        
        start_time = time.time()
        await asyncio.gather(*[worker(i, 2000) for i in range(5)])
        concurrent_duration = time.time() - start_time
        
        total_ops = 5 * 2000 * 2  # 5 workers * 2000 ops * 2 (SET + GET)
        concurrent_ops_per_sec = total_ops / concurrent_duration
        print(f"   Concurrent operations: {concurrent_ops_per_sec:.0f} ops/sec")
        
        # Display server info
        print("\n4. Server information:")
        info = server.get_info()
        print(f"   Node ID: {info['server']['node_id']}")
        print(f"   Uptime: {info['server']['uptime_seconds']:.1f}s")
        print(f"   Total operations: {info['stats']['total_operations']}")
        print(f"   Memory usage: {info['memory']['used_memory'] / 1024 / 1024:.1f}MB")
        
    finally:
        await server.stop()

async def main():
    """Run all demos"""
    print("HyperKV Feature Demonstration")
    print("============================")
    
    try:
        await basic_operations_demo()
        await ttl_demo()
        await crdt_demo()
        await pubsub_demo()
        await cache_eviction_demo()
        await performance_demo()
        
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        print(f"\nDemo failed with error: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*50)
    print("Demo completed!")
    print("="*50)

if __name__ == "__main__":
    asyncio.run(main())
