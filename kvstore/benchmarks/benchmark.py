"""
Comprehensive benchmarking suite for HyperKV
Measures performance across various scenarios and configurations.
"""

import asyncio
import time
import statistics
import gc
import psutil
import json
import argparse
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor
import tempfile
import shutil

from ..core import HyperKVServer, HyperKVConfig, EvictionPolicy


@dataclass
class BenchmarkResult:
    """Result of a benchmark run"""

    name: str
    operations: int
    duration: float
    ops_per_second: float
    latency_avg: float
    latency_p50: float
    latency_p95: float
    latency_p99: float
    memory_usage: int
    cpu_usage: float
    errors: int


class BenchmarkSuite:
    """Comprehensive benchmark suite for HyperKV"""

    def __init__(self, config: Optional[HyperKVConfig] = None):
        self.temp_dir = tempfile.mkdtemp()
        self.config = config or self._create_default_config()
        self.server: Optional[HyperKVServer] = None
        self.results: List[BenchmarkResult] = []

    def _create_default_config(self) -> HyperKVConfig:
        """Create default configuration for benchmarking"""
        return HyperKVConfig(
            host="127.0.0.1",
            port=0,  # Random port
            data_dir=self.temp_dir,
            storage_backend="memory",
            max_memory=1024 * 1024 * 1024,  # 1GB
            max_connections=10000,
            enable_clustering=False,
            enable_metrics=False,
            log_level="ERROR",  # Minimal logging for performance
        )

    async def setup(self):
        """Set up the benchmark environment"""
        self.server = HyperKVServer(self.config)
        await self.server.start()

        # Warm up
        for i in range(100):
            await self.server.set(f"warmup_{i}", f"value_{i}")

        # Clear stats
        self.server.stats = {
            "start_time": time.time(),
            "total_operations": 0,
            "get_operations": 0,
            "set_operations": 0,
            "del_operations": 0,
            "errors": 0,
            "memory_usage": 0,
            "active_connections": 0,
        }

    async def teardown(self):
        """Clean up the benchmark environment"""
        if self.server:
            await self.server.stop()
        shutil.rmtree(self.temp_dir)

    async def run_all_benchmarks(self) -> List[BenchmarkResult]:
        """Run all benchmarks and return results"""
        await self.setup()

        try:
            # Basic operations
            await self.benchmark_set_operations(10000)
            await self.benchmark_get_operations(10000)
            await self.benchmark_mixed_operations(10000)

            # TTL operations
            await self.benchmark_ttl_operations(5000)

            # Concurrent operations
            await self.benchmark_concurrent_sets(1000, 10)
            await self.benchmark_concurrent_gets(1000, 10)
            await self.benchmark_concurrent_mixed(1000, 10)

            # Memory and cache benchmarks
            await self.benchmark_cache_eviction(20000)
            await self.benchmark_large_values(1000)

            # Stress tests
            await self.benchmark_memory_pressure(50000)
            await self.benchmark_high_concurrency(500, 50)

        finally:
            await self.teardown()

        return self.results

    async def benchmark_set_operations(self, count: int):
        """Benchmark SET operations"""
        latencies = []
        start_time = time.time()

        for i in range(count):
            op_start = time.perf_counter()
            await self.server.set(f"set_bench_{i}", f"value_{i}")
            latencies.append(time.perf_counter() - op_start)

        duration = time.time() - start_time
        await self._record_result("SET Operations", count, duration, latencies)

    async def benchmark_get_operations(self, count: int):
        """Benchmark GET operations"""
        # Pre-populate data
        for i in range(count):
            await self.server.set(f"get_bench_{i}", f"value_{i}")

        latencies = []
        start_time = time.time()

        for i in range(count):
            op_start = time.perf_counter()
            await self.server.get(f"get_bench_{i}")
            latencies.append(time.perf_counter() - op_start)

        duration = time.time() - start_time
        await self._record_result("GET Operations", count, duration, latencies)

    async def benchmark_mixed_operations(self, count: int):
        """Benchmark mixed SET/GET operations (70% GET, 30% SET)"""
        # Pre-populate some data
        for i in range(count // 2):
            await self.server.set(f"mixed_bench_{i}", f"value_{i}")

        latencies = []
        start_time = time.time()

        for i in range(count):
            op_start = time.perf_counter()

            if i % 10 < 7:  # 70% GET operations
                await self.server.get(f"mixed_bench_{i % (count // 2)}")
            else:  # 30% SET operations
                await self.server.set(f"mixed_bench_{count // 2 + i}", f"new_value_{i}")

            latencies.append(time.perf_counter() - op_start)

        duration = time.time() - start_time
        await self._record_result(
            "Mixed Operations (70% GET, 30% SET)", count, duration, latencies
        )

    async def benchmark_ttl_operations(self, count: int):
        """Benchmark TTL operations"""
        latencies = []
        start_time = time.time()

        for i in range(count):
            op_start = time.perf_counter()

            # Set with TTL
            await self.server.set(f"ttl_bench_{i}", f"value_{i}", ttl=10.0)

            # Check TTL
            await self.server.ttl(f"ttl_bench_{i}")

            latencies.append(time.perf_counter() - op_start)

        duration = time.time() - start_time
        await self._record_result("TTL Operations", count, duration, latencies)

    async def benchmark_concurrent_sets(self, ops_per_worker: int, workers: int):
        """Benchmark concurrent SET operations"""

        async def worker(worker_id: int):
            latencies = []
            for i in range(ops_per_worker):
                op_start = time.perf_counter()
                await self.server.set(f"concurrent_set_{worker_id}_{i}", f"value_{i}")
                latencies.append(time.perf_counter() - op_start)
            return latencies

        start_time = time.time()

        worker_results = await asyncio.gather(*[worker(i) for i in range(workers)])

        duration = time.time() - start_time

        # Combine all latencies
        all_latencies = []
        for latencies in worker_results:
            all_latencies.extend(latencies)

        total_ops = ops_per_worker * workers
        await self._record_result(
            f"Concurrent SET ({workers} workers)", total_ops, duration, all_latencies
        )

    async def benchmark_concurrent_gets(self, ops_per_worker: int, workers: int):
        """Benchmark concurrent GET operations"""
        # Pre-populate data
        for i in range(ops_per_worker * workers):
            await self.server.set(f"concurrent_get_{i}", f"value_{i}")

        async def worker(worker_id: int):
            latencies = []
            for i in range(ops_per_worker):
                op_start = time.perf_counter()
                await self.server.get(
                    f"concurrent_get_{worker_id * ops_per_worker + i}"
                )
                latencies.append(time.perf_counter() - op_start)
            return latencies

        start_time = time.time()

        worker_results = await asyncio.gather(*[worker(i) for i in range(workers)])

        duration = time.time() - start_time

        all_latencies = []
        for latencies in worker_results:
            all_latencies.extend(latencies)

        total_ops = ops_per_worker * workers
        await self._record_result(
            f"Concurrent GET ({workers} workers)", total_ops, duration, all_latencies
        )

    async def benchmark_concurrent_mixed(self, ops_per_worker: int, workers: int):
        """Benchmark concurrent mixed operations"""
        # Pre-populate some data
        for i in range(ops_per_worker * workers // 2):
            await self.server.set(f"concurrent_mixed_{i}", f"value_{i}")

        async def worker(worker_id: int):
            latencies = []
            for i in range(ops_per_worker):
                op_start = time.perf_counter()

                if i % 3 == 0:  # SET operation
                    await self.server.set(
                        f"concurrent_mixed_new_{worker_id}_{i}", f"value_{i}"
                    )
                else:  # GET operation
                    key_idx = (worker_id * ops_per_worker + i) % (
                        ops_per_worker * workers // 2
                    )
                    await self.server.get(f"concurrent_mixed_{key_idx}")

                latencies.append(time.perf_counter() - op_start)
            return latencies

        start_time = time.time()

        worker_results = await asyncio.gather(*[worker(i) for i in range(workers)])

        duration = time.time() - start_time

        all_latencies = []
        for latencies in worker_results:
            all_latencies.extend(latencies)

        total_ops = ops_per_worker * workers
        await self._record_result(
            f"Concurrent Mixed ({workers} workers)", total_ops, duration, all_latencies
        )

    async def benchmark_cache_eviction(self, count: int):
        """Benchmark cache eviction performance"""
        # Configure smaller memory limit to trigger eviction
        original_max_memory = self.server.cache_manager.max_memory
        self.server.cache_manager.max_memory = 1024 * 1024  # 1MB

        try:
            latencies = []
            start_time = time.time()

            for i in range(count):
                op_start = time.perf_counter()
                # Create large enough values to trigger eviction
                await self.server.set(f"evict_bench_{i}", "x" * 1000)  # 1KB values
                latencies.append(time.perf_counter() - op_start)

            duration = time.time() - start_time
            await self._record_result("Cache Eviction", count, duration, latencies)

        finally:
            self.server.cache_manager.max_memory = original_max_memory

    async def benchmark_large_values(self, count: int):
        """Benchmark operations with large values"""
        large_value = "x" * 10240  # 10KB values

        latencies = []
        start_time = time.time()

        for i in range(count):
            op_start = time.perf_counter()
            await self.server.set(f"large_value_{i}", large_value)
            await self.server.get(f"large_value_{i}")
            latencies.append(time.perf_counter() - op_start)

        duration = time.time() - start_time
        await self._record_result("Large Values (10KB)", count * 2, duration, latencies)

    async def benchmark_memory_pressure(self, count: int):
        """Benchmark under memory pressure"""
        # Fill memory close to limit
        for i in range(count // 2):
            await self.server.set(f"pressure_prep_{i}", "x" * 1000)

        latencies = []
        start_time = time.time()

        for i in range(count):
            op_start = time.perf_counter()
            await self.server.set(f"pressure_bench_{i}", f"value_{i}")
            latencies.append(time.perf_counter() - op_start)

        duration = time.time() - start_time
        await self._record_result("Memory Pressure", count, duration, latencies)

    async def benchmark_high_concurrency(self, ops_per_worker: int, workers: int):
        """Benchmark with high concurrency"""

        async def worker(worker_id: int):
            latencies = []
            for i in range(ops_per_worker):
                op_start = time.perf_counter()

                # Mix of operations
                if i % 4 == 0:
                    await self.server.set(
                        f"high_conc_set_{worker_id}_{i}", f"value_{i}"
                    )
                elif i % 4 == 1:
                    await self.server.get(f"high_conc_set_{worker_id}_{max(0, i-1)}")
                elif i % 4 == 2:
                    await self.server.set(
                        f"high_conc_ttl_{worker_id}_{i}", f"value_{i}", ttl=60.0
                    )
                else:
                    await self.server.exists(f"high_conc_set_{worker_id}_{max(0, i-2)}")

                latencies.append(time.perf_counter() - op_start)
            return latencies

        start_time = time.time()

        worker_results = await asyncio.gather(*[worker(i) for i in range(workers)])

        duration = time.time() - start_time

        all_latencies = []
        for latencies in worker_results:
            all_latencies.extend(latencies)

        total_ops = ops_per_worker * workers
        await self._record_result(
            f"High Concurrency ({workers} workers)", total_ops, duration, all_latencies
        )

    async def _record_result(
        self, name: str, operations: int, duration: float, latencies: List[float]
    ):
        """Record benchmark result"""
        ops_per_second = operations / duration

        # Calculate latency statistics
        latencies.sort()
        latency_avg = statistics.mean(latencies)
        latency_p50 = latencies[len(latencies) // 2]
        latency_p95 = latencies[int(len(latencies) * 0.95)]
        latency_p99 = latencies[int(len(latencies) * 0.99)]

        # Get system metrics
        process = psutil.Process()
        memory_usage = process.memory_info().rss
        cpu_usage = process.cpu_percent()

        # Get error count
        errors = self.server.stats.get("errors", 0)

        result = BenchmarkResult(
            name=name,
            operations=operations,
            duration=duration,
            ops_per_second=ops_per_second,
            latency_avg=latency_avg * 1000,  # Convert to milliseconds
            latency_p50=latency_p50 * 1000,
            latency_p95=latency_p95 * 1000,
            latency_p99=latency_p99 * 1000,
            memory_usage=memory_usage,
            cpu_usage=cpu_usage,
            errors=errors,
        )

        self.results.append(result)

        # Force garbage collection
        gc.collect()

    def print_results(self):
        """Print benchmark results in a formatted table"""
        print("\n" + "=" * 120)
        print("HyperKV Performance Benchmark Results")
        print("=" * 120)
        print(
            f"{'Benchmark':<40} {'Ops':<8} {'Duration':<8} {'Ops/sec':<10} {'Avg(ms)':<8} {'P50(ms)':<8} {'P95(ms)':<8} {'P99(ms)':<8} {'Memory(MB)':<10} {'CPU%':<6}"
        )
        print("-" * 120)

        for result in self.results:
            print(
                f"{result.name:<40} "
                f"{result.operations:<8} "
                f"{result.duration:<8.2f} "
                f"{result.ops_per_second:<10.0f} "
                f"{result.latency_avg:<8.3f} "
                f"{result.latency_p50:<8.3f} "
                f"{result.latency_p95:<8.3f} "
                f"{result.latency_p99:<8.3f} "
                f"{result.memory_usage / 1024 / 1024:<10.1f} "
                f"{result.cpu_usage:<6.1f}"
            )

        print("-" * 120)

        # Summary statistics
        total_ops = sum(r.operations for r in self.results)
        total_duration = sum(r.duration for r in self.results)
        avg_throughput = statistics.mean([r.ops_per_second for r in self.results])

        print(f"\nSummary:")
        print(f"Total Operations: {total_ops:,}")
        print(f"Total Duration: {total_duration:.2f}s")
        print(f"Average Throughput: {avg_throughput:.0f} ops/sec")
        print(
            f"Peak Throughput: {max(r.ops_per_second for r in self.results):.0f} ops/sec"
        )

    def save_results(self, filename: str):
        """Save results to JSON file"""
        data = {
            "config": asdict(self.config),
            "results": [asdict(result) for result in self.results],
            "summary": {
                "total_operations": sum(r.operations for r in self.results),
                "total_duration": sum(r.duration for r in self.results),
                "average_throughput": statistics.mean(
                    [r.ops_per_second for r in self.results]
                ),
                "peak_throughput": max(r.ops_per_second for r in self.results),
            },
        }

        with open(filename, "w") as f:
            json.dump(data, f, indent=2)

        print(f"\nResults saved to {filename}")


async def main():
    """Main benchmark entry point"""
    parser = argparse.ArgumentParser(description="HyperKV Performance Benchmark")

    parser.add_argument(
        "--storage-backend",
        choices=["memory", "rocksdb", "lmdb"],
        default="memory",
        help="Storage backend to test",
    )
    parser.add_argument(
        "--eviction-policy",
        choices=["lru", "lfu", "arc", "random"],
        default="lru",
        help="Cache eviction policy",
    )
    parser.add_argument(
        "--max-memory",
        type=str,
        default="1GB",
        help="Maximum memory (e.g., 1GB, 512MB)",
    )
    parser.add_argument("--output", type=str, help="Output file for results")

    args = parser.parse_args()

    # Parse memory size
    def parse_memory(size_str):
        size_str = size_str.upper()
        if size_str.endswith("GB"):
            return int(float(size_str[:-2]) * 1024 * 1024 * 1024)
        elif size_str.endswith("MB"):
            return int(float(size_str[:-2]) * 1024 * 1024)
        else:
            return int(size_str)

    # Create configuration
    config = HyperKVConfig(
        storage_backend=args.storage_backend,
        eviction_policy=args.eviction_policy,
        max_memory=parse_memory(args.max_memory),
    )

    print(f"Starting HyperKV benchmark with configuration:")
    print(f"  Storage Backend: {config.storage_backend}")
    print(f"  Eviction Policy: {config.eviction_policy}")
    print(f"  Max Memory: {config.max_memory / 1024 / 1024:.0f}MB")

    # Run benchmarks
    suite = BenchmarkSuite(config)
    await suite.run_all_benchmarks()

    # Display results
    suite.print_results()

    # Save results if requested
    if args.output:
        suite.save_results(args.output)


if __name__ == "__main__":
    asyncio.run(main())
