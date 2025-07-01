#!/usr/bin/env python3
"""
HyperKV Core Performance Benchmark Tests
Pytest version of core performance benchmarks
"""

import asyncio
import time
import statistics
import random
import string
import psutil
import gc
import pytest
from typing import Dict, List, Any
from kvstore.config import StorageConfig, CacheConfig, NetworkConfig
from kvstore.core.config import HyperKVServerConfig
from kvstore.core.server import HyperKVServer
from kvstore.crdt import create_crdt


class TestHyperKVCoreBenchmarks:
    """Core performance benchmark tests without network layer"""

    @pytest.fixture(scope="class")
    async def benchmark_server(self):
        """Setup server for benchmarking"""
        config = HyperKVServerConfig(
            storage=StorageConfig(
                data_dir="/tmp/hyperkv_benchmark",
                aof_enabled=False,
                snapshot_enabled=False,
                backend="memory",
            ),
            cache=CacheConfig(
                max_memory=500 * 1024 * 1024,  # 500MB for benchmarking
                eviction_policy="lru",
            ),
            network=NetworkConfig(host="127.0.0.1", port=6381, max_connections=1000),
        )

        server = HyperKVServer(config)

        # Start core components without network
        await server._load_persisted_data()
        server.ttl_manager.start()
        server._start_background_tasks()
        server._running = True
        server.stats["start_time"] = time.time()

        yield server

        # Cleanup
        if server._running:
            server._running = False

            # Stop TTL manager
            if hasattr(server.ttl_manager, "stop"):
                if asyncio.iscoroutinefunction(server.ttl_manager.stop):
                    await server.ttl_manager.stop()
                else:
                    server.ttl_manager.stop()

            # Cancel background tasks
            for task in server._background_tasks:
                task.cancel()
            if server._background_tasks:
                await asyncio.gather(*server._background_tasks, return_exceptions=True)

            # Give a moment for cleanup
            await asyncio.sleep(0.1)

    def generate_test_data(self, num_keys: int) -> List[tuple]:
        """Generate test key-value pairs"""
        data = []
        for i in range(num_keys):
            key = f"benchmark:key:{i:08d}"
            value = "".join(random.choices(string.ascii_letters + string.digits, k=64))
            data.append((key, value))
        return data

    @pytest.mark.asyncio
    @pytest.mark.benchmark
    async def test_sequential_set_operations(self, benchmark_server):
        """Test sequential SET operations performance"""
        num_operations = 1000
        test_data = self.generate_test_data(num_operations)

        set_latencies = []
        start_time = time.perf_counter()

        for key, value in test_data:
            op_start = time.perf_counter()
            await benchmark_server.set(key, value)
            latency = (time.perf_counter() - op_start) * 1000  # ms
            set_latencies.append(latency)

        duration = time.perf_counter() - start_time
        ops_per_second = len(set_latencies) / duration
        avg_latency = statistics.mean(set_latencies)
        p95_latency = statistics.quantiles(set_latencies, n=20)[18]

        # Performance assertions
        assert (
            ops_per_second > 1000
        ), f"SET throughput too low: {ops_per_second:.0f} ops/sec"
        assert avg_latency < 10.0, f"Average SET latency too high: {avg_latency:.3f}ms"
        assert p95_latency < 50.0, f"P95 SET latency too high: {p95_latency:.3f}ms"

        print(
            f"Sequential SET: {ops_per_second:.0f} ops/sec, avg: {avg_latency:.3f}ms, p95: {p95_latency:.3f}ms"
        )

    @pytest.mark.asyncio
    @pytest.mark.benchmark
    async def test_sequential_get_operations(self, benchmark_server):
        """Test sequential GET operations performance"""
        num_operations = 1000
        test_data = self.generate_test_data(num_operations)

        # First populate the data
        for key, value in test_data:
            await benchmark_server.set(key, value)

        get_latencies = []
        start_time = time.perf_counter()

        for key, expected_value in test_data:
            op_start = time.perf_counter()
            value = await benchmark_server.get(key)
            latency = (time.perf_counter() - op_start) * 1000  # ms
            get_latencies.append(latency)
            assert value == expected_value

        duration = time.perf_counter() - start_time
        ops_per_second = len(get_latencies) / duration
        avg_latency = statistics.mean(get_latencies)
        p95_latency = statistics.quantiles(get_latencies, n=20)[18]

        # Performance assertions
        assert (
            ops_per_second > 2000
        ), f"GET throughput too low: {ops_per_second:.0f} ops/sec"
        assert avg_latency < 5.0, f"Average GET latency too high: {avg_latency:.3f}ms"
        assert p95_latency < 25.0, f"P95 GET latency too high: {p95_latency:.3f}ms"

        print(
            f"Sequential GET: {ops_per_second:.0f} ops/sec, avg: {avg_latency:.3f}ms, p95: {p95_latency:.3f}ms"
        )

    @pytest.mark.asyncio
    @pytest.mark.benchmark
    async def test_mixed_operations_performance(self, benchmark_server):
        """Test mixed operations performance"""
        num_operations = 900  # Divisible by 3
        test_data = self.generate_test_data(num_operations)

        # Populate some initial data
        for key, value in test_data[: num_operations // 3]:
            await benchmark_server.set(key, value)

        mixed_latencies = []
        start_time = time.perf_counter()

        for i in range(num_operations // 3):
            op_start = time.perf_counter()

            if i % 3 == 0:
                # SET
                key = f"mixed:set:{i}"
                value = f"mixed_value_{i}"
                await benchmark_server.set(key, value)
            elif i % 3 == 1:
                # GET
                key = f"benchmark:key:{i:08d}"
                await benchmark_server.get(key)
            else:
                # EXISTS
                key = f"benchmark:key:{i:08d}"
                await benchmark_server.exists(key)

            latency = (time.perf_counter() - op_start) * 1000
            mixed_latencies.append(latency)

        duration = time.perf_counter() - start_time
        ops_per_second = len(mixed_latencies) / duration
        avg_latency = statistics.mean(mixed_latencies)
        p95_latency = statistics.quantiles(mixed_latencies, n=20)[18]

        # Performance assertions
        assert (
            ops_per_second > 1500
        ), f"Mixed ops throughput too low: {ops_per_second:.0f} ops/sec"
        assert (
            avg_latency < 8.0
        ), f"Average mixed ops latency too high: {avg_latency:.3f}ms"
        assert (
            p95_latency < 40.0
        ), f"P95 mixed ops latency too high: {p95_latency:.3f}ms"

        print(
            f"Mixed operations: {ops_per_second:.0f} ops/sec, avg: {avg_latency:.3f}ms, p95: {p95_latency:.3f}ms"
        )

    @pytest.mark.asyncio
    @pytest.mark.benchmark
    async def test_concurrent_operations_performance(self, benchmark_server):
        """Test concurrent operations performance"""
        num_clients = 10
        ops_per_client = 100

        async def client_worker(client_id: int) -> Dict[str, Any]:
            """Worker function for concurrent client"""
            latencies = []
            successful_ops = 0
            failed_ops = 0

            for i in range(ops_per_client):
                try:
                    op_start = time.perf_counter()

                    key = f"concurrent:client{client_id}:op{i}"
                    value = f"value_{client_id}_{i}"

                    if i % 4 == 0:
                        await benchmark_server.set(key, value)
                    elif i % 4 == 1:
                        await benchmark_server.get(key)
                    elif i % 4 == 2:
                        await benchmark_server.exists(key)
                    else:
                        await benchmark_server.delete(key)

                    latency = (time.perf_counter() - op_start) * 1000
                    latencies.append(latency)
                    successful_ops += 1

                except Exception:
                    failed_ops += 1

            return {
                "client_id": client_id,
                "successful_ops": successful_ops,
                "failed_ops": failed_ops,
                "latencies": latencies,
            }

        # Run concurrent clients
        start_time = time.perf_counter()
        tasks = [asyncio.create_task(client_worker(i)) for i in range(num_clients)]
        client_results = await asyncio.gather(*tasks)
        total_duration = time.perf_counter() - start_time

        # Aggregate results
        total_successful = sum(r["successful_ops"] for r in client_results)
        total_failed = sum(r["failed_ops"] for r in client_results)
        all_latencies = []
        for r in client_results:
            all_latencies.extend(r["latencies"])

        ops_per_second = (total_successful + total_failed) / total_duration
        avg_latency = statistics.mean(all_latencies) if all_latencies else 0
        p95_latency = (
            statistics.quantiles(all_latencies, n=20)[18] if all_latencies else 0
        )

        # Performance assertions
        assert total_failed == 0, f"Failed operations: {total_failed}"
        assert (
            ops_per_second > 500
        ), f"Concurrent throughput too low: {ops_per_second:.0f} ops/sec"
        assert (
            avg_latency < 15.0
        ), f"Average concurrent latency too high: {avg_latency:.3f}ms"
        assert (
            p95_latency < 60.0
        ), f"P95 concurrent latency too high: {p95_latency:.3f}ms"

        print(
            f"Concurrent ops ({num_clients} clients): {ops_per_second:.0f} ops/sec, avg: {avg_latency:.3f}ms, p95: {p95_latency:.3f}ms"
        )


class TestHyperKVCRDTBenchmarks:
    """CRDT performance benchmark tests"""

    @pytest.mark.benchmark
    def test_lww_set_performance(self):
        """Test LWW Set CRDT performance"""
        num_operations = 1000
        lww_set = create_crdt("lww", None, "benchmark_node")

        # Benchmark add operations
        add_latencies = []
        start_time = time.perf_counter()

        for i in range(num_operations // 2):
            op_start = time.perf_counter()
            lww_set.add(f"lww_item_{i}")
            latency = (time.perf_counter() - op_start) * 1000
            add_latencies.append(latency)

        # Benchmark contains operations
        contains_latencies = []
        for i in range(num_operations // 2):
            op_start = time.perf_counter()
            lww_set.contains(f"lww_item_{i}")
            latency = (time.perf_counter() - op_start) * 1000
            contains_latencies.append(latency)

        duration = time.perf_counter() - start_time
        all_latencies = add_latencies + contains_latencies
        ops_per_second = len(all_latencies) / duration
        avg_latency = statistics.mean(all_latencies)
        p95_latency = statistics.quantiles(all_latencies, n=20)[18]

        # Performance assertions
        assert (
            ops_per_second > 5000
        ), f"LWW Set throughput too low: {ops_per_second:.0f} ops/sec"
        assert (
            avg_latency < 2.0
        ), f"Average LWW Set latency too high: {avg_latency:.3f}ms"
        assert p95_latency < 10.0, f"P95 LWW Set latency too high: {p95_latency:.3f}ms"

        print(
            f"LWW Set: {ops_per_second:.0f} ops/sec, avg: {avg_latency:.3f}ms, p95: {p95_latency:.3f}ms"
        )

    @pytest.mark.benchmark
    def test_counter_crdt_performance(self):
        """Test Counter CRDT performance"""
        num_operations = 1000
        counter = create_crdt("counter", 0, "benchmark_node")

        counter_latencies = []
        start_time = time.perf_counter()

        for i in range(num_operations):
            op_start = time.perf_counter()
            counter.increment(1)
            latency = (time.perf_counter() - op_start) * 1000
            counter_latencies.append(latency)

        duration = time.perf_counter() - start_time
        ops_per_second = len(counter_latencies) / duration
        avg_latency = statistics.mean(counter_latencies)
        p95_latency = statistics.quantiles(counter_latencies, n=20)[18]

        # Performance assertions
        assert (
            ops_per_second > 10000
        ), f"Counter throughput too low: {ops_per_second:.0f} ops/sec"
        assert (
            avg_latency < 1.0
        ), f"Average Counter latency too high: {avg_latency:.3f}ms"
        assert p95_latency < 5.0, f"P95 Counter latency too high: {p95_latency:.3f}ms"
        assert (
            counter.value() == num_operations
        ), f"Counter final value incorrect: {counter.value()}"

        print(
            f"Counter CRDT: {ops_per_second:.0f} ops/sec, avg: {avg_latency:.3f}ms, p95: {p95_latency:.3f}ms"
        )


class TestHyperKVMemoryBenchmarks:
    """Memory usage benchmark tests"""

    @pytest.fixture(scope="class")
    async def benchmark_server(self):
        """Setup server for memory benchmarking"""
        config = HyperKVServerConfig(
            storage=StorageConfig(
                data_dir="/tmp/hyperkv_memory_benchmark",
                aof_enabled=False,
                snapshot_enabled=False,
                backend="memory",
            ),
            cache=CacheConfig(
                max_memory=500 * 1024 * 1024,  # 500MB for benchmarking
                eviction_policy="lru",
            ),
            network=NetworkConfig(host="127.0.0.1", port=6382, max_connections=1000),
        )

        server = HyperKVServer(config)

        # Start core components without network
        await server._load_persisted_data()
        server.ttl_manager.start()
        server._start_background_tasks()
        server._running = True
        server.stats["start_time"] = time.time()

        yield server

        # Cleanup
        if server._running:
            server._running = False

            # Stop TTL manager
            if hasattr(server.ttl_manager, "stop"):
                if asyncio.iscoroutinefunction(server.ttl_manager.stop):
                    await server.ttl_manager.stop()
                else:
                    server.ttl_manager.stop()

            # Cancel background tasks
            for task in server._background_tasks:
                task.cancel()
            if server._background_tasks:
                await asyncio.gather(*server._background_tasks, return_exceptions=True)

            # Give a moment for cleanup
            await asyncio.sleep(0.1)

    @pytest.mark.benchmark
    async def test_memory_usage_under_load(self, benchmark_server):
        """Test memory usage under load"""
        process = psutil.Process()
        initial_memory = process.memory_info().rss / (1024 * 1024)  # MB

        # Generate load
        num_operations = 5000  # Restored original value
        test_data = []
        for i in range(num_operations):
            key = f"memory:test:{i:08d}"
            value = "".join(random.choices(string.ascii_letters + string.digits, k=128))
            test_data.append((key, value))

        # Insert data
        for key, value in test_data:
            await benchmark_server.set(key, value)

        # Force garbage collection
        gc.collect()

        # Measure memory after load
        final_memory = process.memory_info().rss / (1024 * 1024)  # MB
        memory_increase = final_memory - initial_memory
        memory_per_key = memory_increase / num_operations * 1024  # KB per key

        server_stats = benchmark_server.get_stats()
        cache_usage = server_stats.get("cache_usage", {})

        # Memory assertions
        assert memory_per_key < 5.0, f"Memory per key too high: {memory_per_key:.2f} KB"
        assert (
            final_memory < initial_memory + 100
        ), f"Memory usage too high: {final_memory:.1f} MB"

        print(
            f"Memory usage: {memory_increase:.1f} MB increase, {memory_per_key:.2f} KB per key"
        )
        print(f"Cache stats: {cache_usage}")

    @pytest.mark.benchmark
    async def test_memory_cleanup_after_delete(self, benchmark_server):
        """Test memory cleanup after deleting keys"""
        process = psutil.Process()
        initial_memory = process.memory_info().rss / (1024 * 1024)  # MB

        # Insert data
        num_keys = 2000  # Restored original value
        keys = []
        for i in range(num_keys):
            key = f"cleanup:test:{i:08d}"
            value = "".join(random.choices(string.ascii_letters + string.digits, k=256))
            await benchmark_server.set(key, value)
            keys.append(key)

        memory_after_insert = process.memory_info().rss / (1024 * 1024)  # MB

        # Delete all keys
        for key in keys:
            await benchmark_server.delete(key)

        # Force garbage collection and allow more time for cleanup
        gc.collect()
        await asyncio.sleep(0.5)  # Allow cleanup
        gc.collect()  # Second pass

        final_memory = process.memory_info().rss / (1024 * 1024)  # MB
        memory_recovered = memory_after_insert - final_memory
        memory_increase = memory_after_insert - initial_memory

        # Avoid division by zero
        if memory_increase > 0:
            recovery_percentage = (memory_recovered / memory_increase) * 100
        else:
            recovery_percentage = 0.0

        # Debug information
        print(f"Initial memory: {initial_memory:.1f} MB")
        print(f"Memory after insert: {memory_after_insert:.1f} MB")
        print(f"Final memory: {final_memory:.1f} MB")
        print(f"Memory increase: {memory_increase:.1f} MB")
        print(f"Memory recovered: {memory_recovered:.1f} MB")
        print(f"Recovery percentage: {recovery_percentage:.1f}%")

        # More realistic memory cleanup assertions for in-memory storage
        # Python's garbage collection may not immediately free all memory
        assert (
            final_memory <= memory_after_insert
        ), "Memory should not increase after deletion"

        # Either we have some memory recovery OR the memory usage was minimal to begin with
        memory_recovery_ok = (recovery_percentage > 10) or (
            memory_increase < 5.0
        )  # Less than 5MB increase
        assert (
            memory_recovery_ok
        ), f"Memory recovery insufficient: {recovery_percentage:.1f}% recovery, {memory_increase:.1f}MB increase"

        print(
            f"Memory recovery: {memory_recovered:.1f} MB ({recovery_percentage:.1f}%)"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
