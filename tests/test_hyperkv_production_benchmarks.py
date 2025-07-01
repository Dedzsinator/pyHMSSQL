#!/usr/bin/env python3
"""
HyperKV Production Performance Benchmark Tests
Pytest version of production performance validation
"""

import asyncio
import time
import json
import statistics
import random
import string
import concurrent.futures
import pytest
from typing import Dict, List, Any, Tuple
from datetime import datetime, timedelta
import logging
import psutil
import gc

from kvstore.core.server import HyperKVServer
from kvstore.core.config import HyperKVServerConfig
from kvstore.config import StorageConfig, CacheConfig, NetworkConfig
from kvstore.crdt import create_crdt, HybridLogicalClock


class TestHyperKVProductionBenchmarks:
    """Production performance validation test suite"""

    @pytest.fixture(scope="class")
    async def production_server(self):
        """Setup production-like server configuration"""
        # Try RocksDB first, fallback to memory if not available
        try:
            from kvstore.storage import ROCKSDB_AVAILABLE

            backend = "rocksdb" if ROCKSDB_AVAILABLE else "memory"
        except ImportError:
            backend = "memory"

        config = HyperKVServerConfig(
            storage=StorageConfig(
                data_dir="/tmp/hyperkv_production_test",
                aof_enabled=True,
                snapshot_enabled=True,
                backend=backend,
            ),
            cache=CacheConfig(
                max_memory=500 * 1024 * 1024, eviction_policy="lru"  # 500MB
            ),
            network=NetworkConfig(host="127.0.0.1", port=6382, max_connections=1000),
        )

        server = HyperKVServer(config)

        # Start core components without network for testing
        await server._load_persisted_data()
        server.ttl_manager.start()
        server._start_background_tasks()
        server._running = True
        server.stats["start_time"] = time.time()

        yield server

        # Cleanup
        if server._running:
            for task in server._background_tasks:
                task.cancel()
            if server._background_tasks:
                await asyncio.gather(*server._background_tasks, return_exceptions=True)
            server._running = False

    @pytest.mark.asyncio
    @pytest.mark.benchmark
    @pytest.mark.production
    async def test_sustained_throughput_performance(self, production_server):
        """Test sustained throughput under production-like conditions"""
        duration_seconds = 10  # Reduced for testing

        # Test SET throughput
        start_time = time.time()
        set_count = 0

        while time.time() - start_time < duration_seconds:
            batch_tasks = []
            for i in range(50):  # Smaller batches for testing
                key = f"throughput:set:{set_count + i}"
                value = f"value_{set_count + i}_{random.randint(1000, 9999)}"
                batch_tasks.append(production_server.set(key, value))

            await asyncio.gather(*batch_tasks)
            set_count += 50

            # Small pause to prevent overwhelming in tests
            await asyncio.sleep(0.001)

        actual_duration = time.time() - start_time
        set_throughput = set_count / actual_duration

        # Test GET throughput
        start_time = time.time()
        get_count = 0

        while time.time() - start_time < duration_seconds:
            batch_tasks = []
            for i in range(50):
                key = f"throughput:set:{random.randint(0, set_count-1)}"
                batch_tasks.append(production_server.get(key))

            await asyncio.gather(*batch_tasks)
            get_count += 50
            await asyncio.sleep(0.001)

        actual_duration = time.time() - start_time
        get_throughput = get_count / actual_duration

        # Performance assertions
        assert (
            set_throughput > 1000
        ), f"SET throughput too low: {set_throughput:.0f} ops/sec"
        assert (
            get_throughput > 2000
        ), f"GET throughput too low: {get_throughput:.0f} ops/sec"

        print(
            f"Production throughput - SET: {set_throughput:.0f} ops/sec, GET: {get_throughput:.0f} ops/sec"
        )

    @pytest.mark.asyncio
    @pytest.mark.benchmark
    @pytest.mark.production
    async def test_latency_distribution_under_load(self, production_server):
        """Test latency distribution under production load"""
        num_operations = 1000

        # Populate initial data
        for i in range(num_operations // 2):
            key = f"latency:baseline:{i}"
            value = f"baseline_value_{i}"
            await production_server.set(key, value)

        # Measure SET latencies
        set_latencies = []
        for i in range(num_operations // 3):
            start_time = time.perf_counter()
            key = f"latency:set:{i}"
            value = f"latency_value_{i}"
            await production_server.set(key, value)
            latency = (time.perf_counter() - start_time) * 1000  # ms
            set_latencies.append(latency)

        # Measure GET latencies
        get_latencies = []
        for i in range(num_operations // 3):
            start_time = time.perf_counter()
            key = f"latency:baseline:{i}"
            value = await production_server.get(key)
            latency = (time.perf_counter() - start_time) * 1000
            get_latencies.append(latency)

        # Calculate percentiles
        set_p95 = statistics.quantiles(set_latencies, n=20)[18]
        set_p99 = statistics.quantiles(set_latencies, n=100)[98]
        get_p95 = statistics.quantiles(get_latencies, n=20)[18]
        get_p99 = statistics.quantiles(get_latencies, n=100)[98]

        # Production latency assertions
        assert set_p95 < 20.0, f"SET P95 latency too high: {set_p95:.3f}ms"
        assert set_p99 < 50.0, f"SET P99 latency too high: {set_p99:.3f}ms"
        assert get_p95 < 10.0, f"GET P95 latency too high: {get_p95:.3f}ms"
        assert get_p99 < 25.0, f"GET P99 latency too high: {get_p99:.3f}ms"

        print(f"Production latency - SET P95: {set_p95:.3f}ms, P99: {set_p99:.3f}ms")
        print(f"Production latency - GET P95: {get_p95:.3f}ms, P99: {get_p99:.3f}ms")

    @pytest.mark.asyncio
    @pytest.mark.benchmark
    @pytest.mark.production
    async def test_concurrent_client_simulation(self, production_server):
        """Simulate multiple concurrent clients"""
        num_clients = 20
        ops_per_client = 100

        async def simulate_client(client_id: int) -> Dict[str, Any]:
            """Simulate a single client workload"""
            latencies = []
            successful_ops = 0
            errors = 0

            for i in range(ops_per_client):
                try:
                    start_time = time.perf_counter()

                    # Mixed workload simulation
                    if i % 4 == 0:
                        # Heavy write workload
                        key = f"client{client_id}:write:{i}"
                        value = f"client_data_{client_id}_{i}_" + "x" * 100
                        await production_server.set(key, value)
                    elif i % 4 == 1:
                        # Read existing data
                        key = f"client{client_id}:write:{max(0, i-10)}"
                        await production_server.get(key)
                    elif i % 4 == 2:
                        # Check existence
                        key = f"client{client_id}:write:{max(0, i-5)}"
                        await production_server.exists(key)
                    else:
                        # TTL operations
                        key = f"client{client_id}:ttl:{i}"
                        value = f"ttl_data_{client_id}_{i}"
                        await production_server.set(key, value, ttl=60)

                    latency = (time.perf_counter() - start_time) * 1000
                    latencies.append(latency)
                    successful_ops += 1

                except Exception as e:
                    errors += 1

                # Simulate realistic client behavior
                if i % 10 == 0:
                    await asyncio.sleep(0.001)

            return {
                "client_id": client_id,
                "successful_ops": successful_ops,
                "errors": errors,
                "latencies": latencies,
                "avg_latency": statistics.mean(latencies) if latencies else 0,
            }

        # Run concurrent clients
        start_time = time.perf_counter()
        tasks = [asyncio.create_task(simulate_client(i)) for i in range(num_clients)]
        client_results = await asyncio.gather(*tasks)
        total_duration = time.perf_counter() - start_time

        # Aggregate results
        total_successful = sum(r["successful_ops"] for r in client_results)
        total_errors = sum(r["errors"] for r in client_results)
        all_latencies = []
        for r in client_results:
            all_latencies.extend(r["latencies"])

        overall_throughput = total_successful / total_duration
        overall_avg_latency = statistics.mean(all_latencies) if all_latencies else 0
        overall_p95 = (
            statistics.quantiles(all_latencies, n=20)[18]
            if len(all_latencies) > 20
            else 0
        )

        # Production concurrency assertions
        assert total_errors == 0, f"Errors occurred: {total_errors}"
        assert (
            overall_throughput > 500
        ), f"Concurrent throughput too low: {overall_throughput:.0f} ops/sec"
        assert (
            overall_avg_latency < 30.0
        ), f"Concurrent avg latency too high: {overall_avg_latency:.3f}ms"
        assert (
            overall_p95 < 100.0
        ), f"Concurrent P95 latency too high: {overall_p95:.3f}ms"

        print(
            f"Concurrent simulation ({num_clients} clients): {overall_throughput:.0f} ops/sec, avg: {overall_avg_latency:.3f}ms"
        )

    @pytest.mark.asyncio
    @pytest.mark.benchmark
    @pytest.mark.production
    async def test_crdt_distributed_performance(self, production_server):
        """Test CRDT performance in distributed scenarios"""
        num_nodes = 5
        ops_per_node = 200

        # Create CRDT instances for different nodes
        lww_sets = []
        counters = []

        for i in range(num_nodes):
            lww_sets.append(create_crdt("lww", None, f"node_{i}"))
            counters.append(create_crdt("counter", 0, f"node_{i}"))

        async def node_worker(node_id: int) -> Dict[str, Any]:
            """Simulate CRDT operations on a node"""
            lww_set = lww_sets[node_id]
            counter = counters[node_id]

            lww_latencies = []
            counter_latencies = []

            for i in range(ops_per_node):
                # LWW Set operations
                start_time = time.perf_counter()
                lww_set.add(f"node{node_id}_item_{i}")
                latency = (time.perf_counter() - start_time) * 1000
                lww_latencies.append(latency)

                # Counter operations
                start_time = time.perf_counter()
                counter.increment(1)
                latency = (time.perf_counter() - start_time) * 1000
                counter_latencies.append(latency)

                # Occasional merge simulation
                if i % 20 == 0 and node_id > 0:
                    # Simulate merging with another node
                    other_node = (node_id - 1) % num_nodes
                    lww_set.merge(lww_sets[other_node])
                    counter.merge(counters[other_node])

            return {
                "node_id": node_id,
                "lww_latencies": lww_latencies,
                "counter_latencies": counter_latencies,
                "lww_size": len(lww_set.value()),
                "counter_value": counter.value(),
            }

        # Run distributed simulation
        start_time = time.perf_counter()
        tasks = [asyncio.create_task(node_worker(i)) for i in range(num_nodes)]
        node_results = await asyncio.gather(*tasks)
        total_duration = time.perf_counter() - start_time

        # Aggregate CRDT performance metrics
        all_lww_latencies = []
        all_counter_latencies = []
        total_counter_value = 0

        for result in node_results:
            all_lww_latencies.extend(result["lww_latencies"])
            all_counter_latencies.extend(result["counter_latencies"])
            total_counter_value += result["counter_value"]

        lww_avg = statistics.mean(all_lww_latencies)
        counter_avg = statistics.mean(all_counter_latencies)
        total_ops = len(all_lww_latencies) + len(all_counter_latencies)
        crdt_throughput = total_ops / total_duration

        # CRDT performance assertions
        assert lww_avg < 5.0, f"LWW Set avg latency too high: {lww_avg:.3f}ms"
        assert counter_avg < 2.0, f"Counter avg latency too high: {counter_avg:.3f}ms"
        assert (
            crdt_throughput > 1000
        ), f"CRDT throughput too low: {crdt_throughput:.0f} ops/sec"
        assert (
            total_counter_value == num_nodes * ops_per_node
        ), f"Counter consistency error: {total_counter_value}"

        print(
            f"CRDT distributed performance: {crdt_throughput:.0f} ops/sec, LWW: {lww_avg:.3f}ms, Counter: {counter_avg:.3f}ms"
        )

    @pytest.mark.asyncio
    @pytest.mark.benchmark
    @pytest.mark.production
    async def test_memory_efficiency_under_load(self, production_server):
        """Test memory efficiency under production load"""
        process = psutil.Process()
        initial_memory = process.memory_info().rss / (1024 * 1024)  # MB

        # Generate realistic production load
        num_sessions = 100
        keys_per_session = 50

        # Simulate user sessions with realistic data patterns
        session_data = {}
        for session_id in range(num_sessions):
            session_keys = []
            for key_id in range(keys_per_session):
                key = f"session:{session_id}:data:{key_id}"
                # Simulate realistic user data (JSON-like structures)
                value = json.dumps(
                    {
                        "user_id": session_id,
                        "action": f"action_{key_id}",
                        "timestamp": time.time(),
                        "metadata": {
                            "source": "production_test",
                            "version": "1.0",
                            "data": "x" * random.randint(50, 200),
                        },
                    }
                )
                await production_server.set(key, value)
                session_keys.append(key)
            session_data[session_id] = session_keys

        # Measure memory after load
        loaded_memory = process.memory_info().rss / (1024 * 1024)  # MB
        memory_increase = loaded_memory - initial_memory

        # Simulate realistic access patterns
        for _ in range(1000):
            session_id = random.randint(0, num_sessions - 1)
            key = random.choice(session_data[session_id])
            value = await production_server.get(key)
            assert value is not None

        # Test memory cleanup by deleting half the sessions
        deleted_keys = 0
        for session_id in range(0, num_sessions, 2):
            for key in session_data[session_id]:
                await production_server.delete(key)
                deleted_keys += 1

        # Force garbage collection
        gc.collect()
        await asyncio.sleep(0.1)

        final_memory = process.memory_info().rss / (1024 * 1024)  # MB
        memory_recovered = loaded_memory - final_memory

        # Memory efficiency assertions
        memory_per_key = (
            memory_increase / (num_sessions * keys_per_session) * 1024
        )  # KB per key
        recovery_rate = (
            (memory_recovered / memory_increase) * 100 if memory_increase > 0 else 0
        )

        assert (
            memory_per_key < 10.0
        ), f"Memory per key too high: {memory_per_key:.2f} KB"
        assert recovery_rate > 30, f"Memory recovery too low: {recovery_rate:.1f}%"
        assert final_memory < loaded_memory, "Memory not reduced after cleanup"

        print(
            f"Production memory efficiency: {memory_per_key:.2f} KB/key, {recovery_rate:.1f}% recovery"
        )

    @pytest.mark.asyncio
    @pytest.mark.benchmark
    @pytest.mark.production
    async def test_ttl_performance_at_scale(self, production_server):
        """Test TTL performance at production scale"""
        num_keys = 2000
        ttl_seconds = 5

        # Set keys with TTL
        start_time = time.perf_counter()
        ttl_keys = []

        for i in range(num_keys):
            key = f"ttl:scale:{i}"
            value = f"ttl_value_{i}"
            await production_server.set(key, value, ttl=ttl_seconds)
            ttl_keys.append(key)

        set_duration = time.perf_counter() - start_time
        set_throughput = num_keys / set_duration

        # Verify all keys exist
        existing_count = 0
        start_time = time.perf_counter()
        for key in ttl_keys:
            if await production_server.exists(key):
                existing_count += 1

        check_duration = time.perf_counter() - start_time
        check_throughput = num_keys / check_duration

        # Wait for TTL expiration
        await asyncio.sleep(ttl_seconds + 1)

        # Verify keys are expired
        expired_count = 0
        start_time = time.perf_counter()
        for key in ttl_keys:
            if not await production_server.exists(key):
                expired_count += 1

        expiry_check_duration = time.perf_counter() - start_time
        expiry_throughput = num_keys / expiry_check_duration

        # TTL performance assertions
        assert (
            set_throughput > 500
        ), f"TTL set throughput too low: {set_throughput:.0f} ops/sec"
        assert (
            check_throughput > 1000
        ), f"TTL check throughput too low: {check_throughput:.0f} ops/sec"
        assert (
            existing_count == num_keys
        ), f"Not all TTL keys were set: {existing_count}/{num_keys}"
        assert (
            expired_count > num_keys * 0.95
        ), f"TTL expiration failed: {expired_count}/{num_keys} expired"

        print(
            f"TTL at scale: set {set_throughput:.0f} ops/sec, check {check_throughput:.0f} ops/sec, {expired_count}/{num_keys} expired"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-m", "production"])
