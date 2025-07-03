"""
Test configuration and utilities for advanced distributed database tests.

This module provides:
- Test configuration and fixtures
- Performance test markers
- Benchmark utilities
- Test data generators
- Common test utilities

Created for comprehensive testing of production-ready distributed DBMS core service.
"""

import pytest
import asyncio
import time
import random
import string
import tempfile
import os
from pathlib import Path
from typing import Dict, Any, List, Generator, Optional
import logging

# Configure test logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "benchmark: mark test as a performance benchmark"
    )
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "endtoend: mark test as an end-to-end test")
    config.addinivalue_line("markers", "slow: mark test as slow running")
    config.addinivalue_line("markers", "network: mark test as requiring network")

    # Enable asyncio mode
    config.option.asyncio_mode = "auto"


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers automatically"""
    for item in items:
        # Add slow marker to tests that take more than 5 seconds
        if "test_" in item.name and any(
            marker in item.name.lower()
            for marker in ["performance", "benchmark", "load", "stress", "large"]
        ):
            item.add_marker(pytest.mark.slow)

        # Add integration marker to integration tests
        if "integration" in item.nodeid:
            item.add_marker(pytest.mark.integration)

        # Add endtoend marker to end-to-end tests
        if "end_to_end" in item.nodeid:
            item.add_marker(pytest.mark.endtoend)


# Common fixtures
@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for async tests"""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def temp_directory():
    """Provide a temporary directory for tests"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def test_config():
    """Provide test configuration"""
    return {
        "timeouts": {"short": 1.0, "medium": 5.0, "long": 30.0},
        "performance": {
            "min_ops_per_sec": 100,
            "max_latency_ms": 100,
            "min_success_rate": 0.95,
        },
        "cluster": {
            "default_nodes": 3,
            "election_timeout": 2.0,
            "heartbeat_interval": 0.5,
        },
    }


# Test data generators
class TestDataGenerator:
    """Generate test data for various scenarios"""

    @staticmethod
    def generate_random_string(length: int = 10) -> str:
        """Generate random string of specified length"""
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))

    @staticmethod
    def generate_key_value_pairs(count: int, key_prefix: str = "key") -> Dict[str, str]:
        """Generate key-value pairs for testing"""
        return {
            f"{key_prefix}_{i}": f"value_{TestDataGenerator.generate_random_string()}"
            for i in range(count)
        }

    @staticmethod
    def generate_large_text(size_kb: int = 10) -> str:
        """Generate large text data for compression testing"""
        # Generate compressible data (repeated patterns)
        base_text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
        repeat_count = (size_kb * 1024) // len(base_text)
        return base_text * repeat_count

    @staticmethod
    def generate_json_data(complexity: str = "simple") -> Dict[str, Any]:
        """Generate JSON data of varying complexity"""
        if complexity == "simple":
            return {
                "id": random.randint(1, 1000),
                "name": TestDataGenerator.generate_random_string(),
                "active": random.choice([True, False]),
            }
        elif complexity == "nested":
            return {
                "id": random.randint(1, 1000),
                "user": {
                    "name": TestDataGenerator.generate_random_string(),
                    "email": f"{TestDataGenerator.generate_random_string()}@example.com",
                    "profile": {
                        "age": random.randint(18, 80),
                        "preferences": [
                            TestDataGenerator.generate_random_string()
                            for _ in range(random.randint(1, 5))
                        ],
                    },
                },
                "metadata": {
                    "created_at": time.time(),
                    "tags": [
                        TestDataGenerator.generate_random_string()
                        for _ in range(random.randint(0, 3))
                    ],
                },
            }
        else:
            return {}

    @staticmethod
    def generate_time_series_data(points: int = 100) -> List[Dict[str, Any]]:
        """Generate time series data for testing"""
        base_time = time.time() - (points * 60)  # Points 1 minute apart
        data = []

        for i in range(points):
            data.append(
                {
                    "timestamp": base_time + (i * 60),
                    "value": random.uniform(0, 100),
                    "metric": f"metric_{i % 5}",  # 5 different metrics
                    "tags": {
                        "host": f"host_{i % 3}",  # 3 different hosts
                        "region": random.choice(["us-east", "us-west", "eu-west"]),
                    },
                }
            )

        return data


# Performance measurement utilities
class PerformanceMeasurement:
    """Utilities for measuring test performance"""

    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.operation_count = 0
        self.error_count = 0

    def start(self):
        """Start timing"""
        self.start_time = time.time()
        self.operation_count = 0
        self.error_count = 0

    def stop(self):
        """Stop timing"""
        self.end_time = time.time()

    def record_operation(self, success: bool = True):
        """Record an operation"""
        self.operation_count += 1
        if not success:
            self.error_count += 1

    def get_duration(self) -> float:
        """Get duration in seconds"""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0

    def get_ops_per_second(self) -> float:
        """Get operations per second"""
        duration = self.get_duration()
        if duration > 0:
            return self.operation_count / duration
        return 0.0

    def get_success_rate(self) -> float:
        """Get success rate"""
        if self.operation_count > 0:
            return (self.operation_count - self.error_count) / self.operation_count
        return 0.0

    def get_latency_ms(self) -> float:
        """Get average latency in milliseconds"""
        duration = self.get_duration()
        if self.operation_count > 0:
            return (duration / self.operation_count) * 1000
        return 0.0

    def assert_performance(
        self,
        min_ops_per_sec: float = 100,
        max_latency_ms: float = 100,
        min_success_rate: float = 0.95,
    ):
        """Assert performance meets requirements"""
        ops_per_sec = self.get_ops_per_second()
        latency_ms = self.get_latency_ms()
        success_rate = self.get_success_rate()

        assert (
            ops_per_sec >= min_ops_per_sec
        ), f"Performance too slow: {ops_per_sec:.1f} ops/sec < {min_ops_per_sec}"

        assert (
            latency_ms <= max_latency_ms
        ), f"Latency too high: {latency_ms:.1f} ms > {max_latency_ms} ms"

        assert (
            success_rate >= min_success_rate
        ), f"Success rate too low: {success_rate:.1%} < {min_success_rate:.1%}"


# Benchmark utilities
@pytest.fixture
def benchmark_runner():
    """Provide benchmark runner"""
    return PerformanceMeasurement()


# Async test utilities
class AsyncTestUtils:
    """Utilities for async testing"""

    @staticmethod
    async def wait_for_condition(
        condition_func, timeout: float = 10.0, check_interval: float = 0.1
    ) -> bool:
        """Wait for a condition to become true"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            if condition_func():
                return True
            await asyncio.sleep(check_interval)

        return False

    @staticmethod
    async def run_concurrent_tasks(tasks: List, max_concurrency: int = 10):
        """Run tasks with limited concurrency"""
        if len(tasks) <= max_concurrency:
            return await asyncio.gather(*tasks)

        results = []
        for i in range(0, len(tasks), max_concurrency):
            batch = tasks[i : i + max_concurrency]
            batch_results = await asyncio.gather(*batch)
            results.extend(batch_results)

        return results


# Mock utilities
class MockNetworkDelay:
    """Mock network delays for testing"""

    def __init__(self, min_delay: float = 0.01, max_delay: float = 0.1):
        self.min_delay = min_delay
        self.max_delay = max_delay

    async def delay(self):
        """Add random network delay"""
        delay = random.uniform(self.min_delay, self.max_delay)
        await asyncio.sleep(delay)


class MockFailureInjector:
    """Inject failures for testing fault tolerance"""

    def __init__(self, failure_rate: float = 0.1):
        self.failure_rate = failure_rate

    def should_fail(self) -> bool:
        """Determine if operation should fail"""
        return random.random() < self.failure_rate

    def maybe_raise_exception(
        self, exception_type: type = Exception, message: str = "Injected failure"
    ):
        """Raise exception based on failure rate"""
        if self.should_fail():
            raise exception_type(message)


# Test fixtures for common components
@pytest.fixture
def test_data_generator():
    """Provide test data generator"""
    return TestDataGenerator()


@pytest.fixture
def mock_network_delay():
    """Provide mock network delay"""
    return MockNetworkDelay()


@pytest.fixture
def failure_injector():
    """Provide failure injector"""
    return MockFailureInjector(failure_rate=0.05)  # 5% failure rate


@pytest.fixture
def async_utils():
    """Provide async utilities"""
    return AsyncTestUtils()


# Test report utilities
class TestReporter:
    """Generate test reports"""

    def __init__(self, report_dir: str = "test_reports"):
        self.report_dir = Path(report_dir)
        self.report_dir.mkdir(exist_ok=True)
        self.results = []

    def add_result(self, test_name: str, result: Dict[str, Any]):
        """Add test result"""
        self.results.append(
            {"test_name": test_name, "timestamp": time.time(), **result}
        )

    def generate_performance_report(self):
        """Generate performance test report"""
        report_file = self.report_dir / "performance_report.json"

        performance_results = [
            result
            for result in self.results
            if "ops_per_second" in result or "latency_ms" in result
        ]

        with open(report_file, "w") as f:
            import json

            json.dump(performance_results, f, indent=2)

        return report_file

    def generate_summary_report(self):
        """Generate summary report"""
        total_tests = len(self.results)
        passed_tests = len([r for r in self.results if r.get("status") == "passed"])
        failed_tests = total_tests - passed_tests

        summary = {
            "total_tests": total_tests,
            "passed_tests": passed_tests,
            "failed_tests": failed_tests,
            "success_rate": passed_tests / total_tests if total_tests > 0 else 0,
            "timestamp": time.time(),
        }

        report_file = self.report_dir / "summary_report.json"
        with open(report_file, "w") as f:
            import json

            json.dump(summary, f, indent=2)

        return summary


@pytest.fixture(scope="session")
def test_reporter():
    """Provide test reporter"""
    return TestReporter()


# Test data cleanup
@pytest.fixture(autouse=True)
def cleanup_test_data():
    """Automatically cleanup test data after tests"""
    yield
    # Cleanup logic would go here
    # For now, temporary directories handle most cleanup


# Integration test helpers
class IntegrationTestHelper:
    """Helper for integration tests"""

    @staticmethod
    def create_test_cluster_config(node_count: int = 3) -> Dict[str, Any]:
        """Create test cluster configuration"""
        base_port = 30000
        nodes = {}

        for i in range(node_count):
            node_id = f"test_node_{i}"
            nodes[node_id] = {
                "host": "127.0.0.1",
                "port": base_port + i,
                "role": "voter",
            }

        return {
            "cluster_id": "test_cluster",
            "nodes": nodes,
            "election_timeout_min": 1.0,
            "election_timeout_max": 2.0,
            "heartbeat_interval": 0.5,
        }

    @staticmethod
    def verify_cluster_health(nodes: List[Any]) -> bool:
        """Verify cluster health"""
        # Basic health check
        running_nodes = sum(1 for node in nodes if getattr(node, "running", False))
        return running_nodes >= len(nodes) // 2 + 1  # Majority


@pytest.fixture
def integration_helper():
    """Provide integration test helper"""
    return IntegrationTestHelper()


if __name__ == "__main__":
    # Run as test discovery
    print("Test configuration module loaded successfully")
    print("Available fixtures:")
    fixtures = [
        "temp_directory",
        "test_config",
        "benchmark_runner",
        "test_data_generator",
        "mock_network_delay",
        "failure_injector",
        "async_utils",
        "test_reporter",
        "integration_helper",
    ]
    for fixture in fixtures:
        print(f"  - {fixture}")
