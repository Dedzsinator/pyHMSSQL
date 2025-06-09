"""
Comprehensive profiling test for pyHMSSQL database operations.

This unified test module provides in-depth profiling of ALL database operations including:
- DDL operations (CREATE, DROP tables, indexes, databases)
- DML operations (INSERT, UPDATE, DELETE, SELECT)
- JOIN operations (different algorithms with 10k x 10k record joins)
- Index operations and performance comparisons
- Transaction operations
- Aggregation operations
- Complex queries with nested joins
- Memory usage patterns
- CPU utilization patterns
- I/O performance metrics
- Linux perf integration for detailed CPU profiling
- Flamegraph generation and hardware counter analysis
- Comprehensive reporting and data export

The test creates comprehensive reports of system resource usage for every operation
and integrates Linux perf for advanced CPU profiling and performance analysis.

To disable profiling during simple testing, set the environment variable:
DISABLE_PROFILING=1
"""

import pytest
import time
import json
import os
import logging
import statistics
from datetime import datetime
from collections import defaultdict
from pathlib import Path
import psutil

# Check if profiling should be disabled
DISABLE_PROFILING = os.environ.get("DISABLE_PROFILING", "0").lower() in (
    "1",
    "true",
    "yes",
    "on",
)

if DISABLE_PROFILING:
    # Skip all profiling tests when disabled
    pytestmark = pytest.mark.skip(
        reason="Profiling disabled via DISABLE_PROFILING environment variable"
    )

    # Create dummy functions to avoid import errors
    def test_dummy_profiling_disabled():
        """Dummy test when profiling is disabled."""
        pass

    # Exit early to avoid import errors
    import sys

    # Don't exit, just mark tests as skipped
    logging.info("Profiling tests disabled via DISABLE_PROFILING environment variable")

# Import the system modules
import sys

project_root = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
server_dir = os.path.join(project_root, "server")

# Add server directory to path so that imports from within server work correctly
if server_dir not in sys.path:
    sys.path.insert(0, server_dir)

# Add project root to path as well
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Only import profiling modules if profiling is enabled
if not DISABLE_PROFILING:
    try:
        from profiler import SystemProfiler, DBMSStatistics
        from catalog_manager import CatalogManager
        from execution_engine import ExecutionEngine
        from ddl_processor.index_manager import IndexManager
        from ddl_processor.schema_manager import SchemaManager
        from planner import Planner
    except ImportError as e:
        logging.warning(f"Could not import profiling modules: {e}")
        # Set profiling as disabled if imports fail
        DISABLE_PROFILING = True
        pytestmark = pytest.mark.skip(reason=f"Profiling modules not available: {e}")


class ComprehensiveProfiler:
    """
    Advanced profiler that captures detailed metrics for every database operation
    with integrated Linux perf support for CPU profiling and hardware counters.
    """

    def __init__(self, catalog_manager, enable_perf=True, output_dir=None):
        """Initialize the comprehensive profiler with perf integration."""
        self.catalog_manager = catalog_manager
        self.enable_perf = enable_perf
        self.output_dir = (
            Path(output_dir) if output_dir else Path("/tmp/pyhmssql_comprehensive_test")
        )
        self.output_dir.mkdir(exist_ok=True, parents=True)

        # Initialize system profiler
        self.system_profiler = SystemProfiler(
            catalog_manager, sample_interval=0.1
        )  # High frequency sampling
        self.dbms_stats = DBMSStatistics(catalog_manager, self.system_profiler)

        # Initialize perf integration if enabled
        if self.enable_perf:
            try:
                from perf_profiler import PerfProfiler, EnhancedSystemProfiler

                self.perf_profiler = PerfProfiler(output_dir=str(self.output_dir))
                self.enhanced_profiler = EnhancedSystemProfiler(
                    catalog_manager=catalog_manager,
                    sample_interval=0.1,
                    enable_perf=True,
                    perf_output_dir=str(self.output_dir),
                )
                logging.info("Perf integration enabled")
            except (ImportError, Exception) as e:
                logging.warning(f"Perf integration disabled: {e}")
                self.enable_perf = False
                self.perf_profiler = None
                self.enhanced_profiler = None
        else:
            self.perf_profiler = None
            self.enhanced_profiler = None

        # Detailed operation metrics
        self.operation_metrics = defaultdict(list)
        self.resource_snapshots = []
        self.current_operation = None
        self.operation_start_time = None
        self.operation_start_resources = None
        self.perf_active = False

        # Start system profiling
        self.system_profiler.start_profiling()
        if self.enhanced_profiler:
            self.enhanced_profiler.start_profiling()

        logging.info(
            f"ComprehensiveProfiler initialized with high-frequency monitoring (perf: {self.enable_perf})"
        )

    def start_operation(self, operation_name, operation_details=None):
        """Start profiling a specific operation with perf integration."""
        self.current_operation = operation_name
        self.operation_start_time = time.time()

        # Start perf profiling if enabled
        if self.enable_perf and self.perf_profiler:
            try:
                # Start CPU profiling for the operation
                self.perf_profiler.start_cpu_profiling(
                    output_file=f"{operation_name}_cpu_profile.data"
                )

                # Start hardware counter profiling
                self.perf_profiler.start_counter_profiling(
                    events=["cycles", "instructions", "cache-misses", "branch-misses"],
                    output_file=f"{operation_name}_counters.data",
                )

                # Start memory profiling if the operation might be memory intensive
                if any(
                    keyword in operation_name.lower()
                    for keyword in ["insert", "select", "join", "index"]
                ):
                    self.perf_profiler.start_memory_profiling(
                        output_file=f"{operation_name}_memory.data"
                    )

                self.perf_active = True
                logging.info(f"🔥 Started perf profiling for: {operation_name}")
            except Exception as e:
                logging.warning(
                    f"Failed to start perf profiling for {operation_name}: {e}"
                )
                self.perf_active = False

        # Capture baseline system metrics
        self.operation_start_resources = {
            "timestamp": self.operation_start_time,
            "cpu_percent": psutil.cpu_percent(),
            "memory_info": psutil.virtual_memory()._asdict(),
            "disk_io": (
                psutil.disk_io_counters()._asdict() if psutil.disk_io_counters() else {}
            ),
            "network_io": (
                psutil.net_io_counters()._asdict() if psutil.net_io_counters() else {}
            ),
            "process_info": psutil.Process().memory_info()._asdict(),
            "process_cpu": psutil.Process().cpu_percent(),
        }

        logging.info(f"🔍 Started profiling operation: {operation_name}")
        if operation_details:
            logging.info(f"   Details: {operation_details}")

    def end_operation(self, result=None, error=None):
        """End profiling the current operation and record metrics with perf data."""
        if not self.current_operation:
            return

        end_time = time.time()
        execution_time = end_time - self.operation_start_time

        # Stop perf profiling and collect data
        perf_data = {}
        if self.perf_active and self.perf_profiler:
            try:
                # Stop CPU profiling and get results
                cpu_profile_data = self.perf_profiler.stop_cpu_profiling()
                if cpu_profile_data:
                    perf_data["cpu_profile"] = cpu_profile_data

                # Stop counter profiling and get results
                counter_data = self.perf_profiler.stop_counter_profiling()
                if counter_data:
                    perf_data["hardware_counters"] = counter_data

                # Stop memory profiling if it was started
                memory_data = self.perf_profiler.stop_memory_profiling()
                if memory_data:
                    perf_data["memory_profile"] = memory_data

                # Generate flamegraph if CPU profiling was active
                if cpu_profile_data:
                    flamegraph_path = self.perf_profiler.generate_flamegraph(
                        f"{self.current_operation}_cpu_profile.data",
                        f"{self.current_operation}_flamegraph.svg",
                    )
                    if flamegraph_path:
                        perf_data["flamegraph_path"] = flamegraph_path

                logging.info(f"🔥 Collected perf data for: {self.current_operation}")
                self.perf_active = False

            except Exception as e:
                logging.warning(
                    f"Failed to collect perf data for {self.current_operation}: {e}"
                )

        # Capture end system metrics
        end_resources = {
            "timestamp": end_time,
            "cpu_percent": psutil.cpu_percent(),
            "memory_info": psutil.virtual_memory()._asdict(),
            "disk_io": (
                psutil.disk_io_counters()._asdict() if psutil.disk_io_counters() else {}
            ),
            "network_io": (
                psutil.net_io_counters()._asdict() if psutil.net_io_counters() else {}
            ),
            "process_info": psutil.Process().memory_info()._asdict(),
            "process_cpu": psutil.Process().cpu_percent(),
        }

        # Calculate resource deltas
        resource_deltas = self._calculate_resource_deltas(
            self.operation_start_resources, end_resources
        )

        # Record comprehensive metrics including perf data
        operation_metric = {
            "operation": self.current_operation,
            "execution_time_seconds": execution_time,
            "start_time": self.operation_start_time,
            "end_time": end_time,
            "success": error is None,
            "error": str(error) if error else None,
            "result_summary": self._summarize_result(result),
            "resource_usage": resource_deltas,
            "start_resources": self.operation_start_resources,
            "end_resources": end_resources,
            "perf_data": perf_data,  # Include perf profiling data
        }

        self.operation_metrics[self.current_operation].append(operation_metric)

        # Log operation completion with perf info
        logging.info(f"✅ Completed profiling operation: {self.current_operation}")
        logging.info(f"   Execution time: {execution_time:.4f} seconds")
        logging.info(
            f"   Memory delta: {resource_deltas.get('memory_rss_mb', 0):.2f} MB"
        )
        logging.info(f"   CPU usage: {resource_deltas.get('cpu_percent_avg', 0):.2f}%")
        if perf_data:
            logging.info(f"   Perf data collected: {list(perf_data.keys())}")

        # Profile the query execution
        self.system_profiler.profile_query(
            self.current_operation,
            execution_time,
            result_rows=self._count_result_rows(result),
            error=error,
        )

        self.current_operation = None
        self.operation_start_time = None
        self.operation_start_resources = None

    def _calculate_resource_deltas(self, start_resources, end_resources):
        """Calculate the resource usage delta between start and end."""
        deltas = {}

        # Memory deltas
        start_memory = start_resources["memory_info"]
        end_memory = end_resources["memory_info"]
        deltas["memory_used_mb"] = (end_memory["used"] - start_memory["used"]) / (
            1024**2
        )
        deltas["memory_available_mb"] = (
            end_memory["available"] - start_memory["available"]
        ) / (1024**2)

        # Process memory deltas
        start_proc = start_resources["process_info"]
        end_proc = end_resources["process_info"]
        deltas["memory_rss_mb"] = (end_proc["rss"] - start_proc["rss"]) / (1024**2)
        deltas["memory_vms_mb"] = (end_proc["vms"] - start_proc["vms"]) / (1024**2)

        # CPU usage (average)
        deltas["cpu_percent_avg"] = (
            start_resources["cpu_percent"] + end_resources["cpu_percent"]
        ) / 2
        deltas["process_cpu_avg"] = (
            start_resources["process_cpu"] + end_resources["process_cpu"]
        ) / 2

        # I/O deltas
        if start_resources["disk_io"] and end_resources["disk_io"]:
            start_disk = start_resources["disk_io"]
            end_disk = end_resources["disk_io"]
            deltas["disk_read_mb"] = (
                end_disk["read_bytes"] - start_disk["read_bytes"]
            ) / (1024**2)
            deltas["disk_write_mb"] = (
                end_disk["write_bytes"] - start_disk["write_bytes"]
            ) / (1024**2)
            deltas["disk_read_count"] = (
                end_disk["read_count"] - start_disk["read_count"]
            )
            deltas["disk_write_count"] = (
                end_disk["write_count"] - start_disk["write_count"]
            )

        if start_resources["network_io"] and end_resources["network_io"]:
            start_net = start_resources["network_io"]
            end_net = end_resources["network_io"]
            deltas["network_recv_mb"] = (
                end_net["bytes_recv"] - start_net["bytes_recv"]
            ) / (1024**2)
            deltas["network_sent_mb"] = (
                end_net["bytes_sent"] - start_net["bytes_sent"]
            ) / (1024**2)

        return deltas

    def _summarize_result(self, result):
        """Create a summary of the operation result."""
        if not result:
            return None

        if isinstance(result, dict):
            summary = {
                "status": result.get("status"),
                "type": type(result).__name__,
            }

            if "rows" in result:
                summary["row_count"] = len(result["rows"]) if result["rows"] else 0
            if "columns" in result:
                summary["column_count"] = (
                    len(result["columns"]) if result["columns"] else 0
                )
            if "affected_rows" in result:
                summary["affected_rows"] = result["affected_rows"]

            return summary

        return {
            "type": type(result).__name__,
            "length": len(result) if hasattr(result, "__len__") else None,
        }

    def _count_result_rows(self, result):
        """Count the number of rows in a result."""
        if not result:
            return 0

        if isinstance(result, dict):
            if "rows" in result:
                return len(result["rows"]) if result["rows"] else 0
            if "affected_rows" in result:
                return result["affected_rows"]

        if isinstance(result, list):
            return len(result)

        return 0

    def get_operation_summary(self):
        """Get a comprehensive summary of all profiled operations."""
        summary = {
            "total_operations": sum(
                len(ops) for ops in self.operation_metrics.values()
            ),
            "operation_types": len(self.operation_metrics),
            "total_execution_time": 0,
            "operations_by_type": {},
            "performance_statistics": {},
            "resource_usage_summary": {},
        }

        all_execution_times = []
        all_memory_usage = []
        all_cpu_usage = []

        for operation_type, operations in self.operation_metrics.items():
            execution_times = [op["execution_time_seconds"] for op in operations]
            memory_usage = [
                op["resource_usage"].get("memory_rss_mb", 0) for op in operations
            ]
            cpu_usage = [
                op["resource_usage"].get("cpu_percent_avg", 0) for op in operations
            ]

            all_execution_times.extend(execution_times)
            all_memory_usage.extend(memory_usage)
            all_cpu_usage.extend(cpu_usage)

            summary["operations_by_type"][operation_type] = {
                "count": len(operations),
                "total_time": sum(execution_times),
                "average_time": (
                    statistics.mean(execution_times) if execution_times else 0
                ),
                "min_time": min(execution_times) if execution_times else 0,
                "max_time": max(execution_times) if execution_times else 0,
                "median_time": (
                    statistics.median(execution_times) if execution_times else 0
                ),
                "total_memory_mb": sum(memory_usage),
                "average_memory_mb": (
                    statistics.mean(memory_usage) if memory_usage else 0
                ),
                "average_cpu_percent": statistics.mean(cpu_usage) if cpu_usage else 0,
                "success_rate": (
                    len([op for op in operations if op["success"]]) / len(operations)
                    if operations
                    else 0
                ),
            }

        summary["total_execution_time"] = sum(all_execution_times)

        if all_execution_times:
            summary["performance_statistics"] = {
                "total_execution_time": sum(all_execution_times),
                "average_execution_time": statistics.mean(all_execution_times),
                "median_execution_time": statistics.median(all_execution_times),
                "min_execution_time": min(all_execution_times),
                "max_execution_time": max(all_execution_times),
                "execution_time_std_dev": (
                    statistics.stdev(all_execution_times)
                    if len(all_execution_times) > 1
                    else 0
                ),
            }

        if all_memory_usage:
            summary["resource_usage_summary"] = {
                "total_memory_mb": sum(all_memory_usage),
                "average_memory_mb": statistics.mean(all_memory_usage),
                "max_memory_mb": max(all_memory_usage),
                "average_cpu_percent": (
                    statistics.mean(all_cpu_usage) if all_cpu_usage else 0
                ),
                "max_cpu_percent": max(all_cpu_usage) if all_cpu_usage else 0,
            }

        return summary

    def generate_detailed_report(self, output_file=None):
        """Generate a detailed profiling report with perf data."""
        # Collect perf system information if available
        perf_system_info = {}
        if self.enable_perf and self.perf_profiler:
            try:
                perf_system_info = self.perf_profiler.get_system_info()
            except Exception as e:
                logging.warning(f"Failed to collect perf system info: {e}")

        report = {
            "profiling_session": {
                "start_time": datetime.now().isoformat(),
                "perf_enabled": self.enable_perf,
                "output_directory": str(self.output_dir) if self.output_dir else None,
                "system_info": {
                    "cpu_count": psutil.cpu_count(),
                    "memory_total_gb": psutil.virtual_memory().total / (1024**3),
                    "disk_total_gb": psutil.disk_usage("/").total / (1024**3),
                },
                "perf_system_info": perf_system_info,
            },
            "summary": self.get_operation_summary(),
            "detailed_metrics": dict(self.operation_metrics),
            "system_metrics": self.system_profiler.get_metrics_summary(),
        }

        # Add enhanced profiler metrics if available
        if self.enhanced_profiler:
            try:
                enhanced_metrics = self.enhanced_profiler.get_metrics_summary()
                report["enhanced_system_metrics"] = enhanced_metrics
            except Exception as e:
                logging.warning(f"Failed to collect enhanced metrics: {e}")

        if output_file:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, default=str)
            logging.info(
                f"📊 Detailed profiling report with perf data saved to: {output_file}"
            )

        return report

    def stop_profiling(self):
        """Stop the profiling session and clean up perf processes."""
        self.system_profiler.stop_profiling()

        if self.enhanced_profiler:
            try:
                self.enhanced_profiler.stop_profiling()
            except Exception as e:
                logging.warning(f"Failed to stop enhanced profiler: {e}")

        if self.perf_profiler:
            try:
                # Cleanup any remaining perf processes
                self.perf_profiler.cleanup()
            except Exception as e:
                logging.warning(f"Failed to cleanup perf profiler: {e}")

        logging.info("🛑 Comprehensive profiling session stopped")


class TestComprehensiveProfiling:
    """
    Comprehensive test suite for profiling all database operations.
    """

    @pytest.fixture(scope="class")
    def profiling_setup(self):
        """Set up comprehensive profiling environment."""
        # Initialize components
        catalog_manager = CatalogManager()
        index_manager = IndexManager(catalog_manager)
        planner = Planner(catalog_manager, index_manager)
        execution_engine = ExecutionEngine(catalog_manager, index_manager, planner)
        schema_manager = SchemaManager(catalog_manager)

        # Initialize comprehensive profiler
        profiler = ComprehensiveProfiler(catalog_manager)

        # Create test database
        profiler.start_operation("CREATE_TEST_DATABASE")
        try:
            catalog_manager.create_database("profiling_test_db")
            catalog_manager.set_current_database("profiling_test_db")
            profiler.end_operation({"status": "success"})
        except Exception as e:
            profiler.end_operation(error=e)
            raise

        yield {
            "catalog_manager": catalog_manager,
            "index_manager": index_manager,
            "execution_engine": execution_engine,
            "schema_manager": schema_manager,
            "planner": planner,
            "profiler": profiler,
        }

        # Generate final report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"/home/deginandor/Documents/Programming/pyHMSSQL/logs/comprehensive_profiling_report_{timestamp}.json"
        profiler.generate_detailed_report(report_file)

        # Stop profiling
        profiler.stop_profiling()

        # Print summary
        summary = profiler.get_operation_summary()
        print("\n" + "=" * 80)
        print("COMPREHENSIVE PROFILING SUMMARY")
        print("=" * 80)
        print(f"Total Operations: {summary['total_operations']}")
        print(f"Operation Types: {summary['operation_types']}")
        print(f"Total Execution Time: {summary['total_execution_time']:.4f} seconds")

        if summary["performance_statistics"]:
            stats = summary["performance_statistics"]
            print(
                f"Average Execution Time: {stats['average_execution_time']:.4f} seconds"
            )
            print(
                f"Median Execution Time: {stats['median_execution_time']:.4f} seconds"
            )
            print(f"Min Execution Time: {stats['min_execution_time']:.4f} seconds")
            print(f"Max Execution Time: {stats['max_execution_time']:.4f} seconds")

        if summary["resource_usage_summary"]:
            resources = summary["resource_usage_summary"]
            print(f"Total Memory Usage: {resources['total_memory_mb']:.2f} MB")
            print(f"Average Memory Usage: {resources['average_memory_mb']:.2f} MB")
            print(f"Max Memory Usage: {resources['max_memory_mb']:.2f} MB")
            print(f"Average CPU Usage: {resources['average_cpu_percent']:.2f}%")
            print(f"Max CPU Usage: {resources['max_cpu_percent']:.2f}%")

        print("\nOperations by Type:")
        for op_type, stats in summary["operations_by_type"].items():
            print(
                f"  {op_type}: {stats['count']} operations, {stats['total_time']:.4f}s total, {stats['average_time']:.4f}s avg"
            )

        print(f"\nDetailed report saved to: {report_file}")
        print("=" * 80)

    def test_01_ddl_operations(self, profiling_setup):
        """Test and profile DDL operations (CREATE/DROP tables, indexes)."""
        components = profiling_setup
        profiler = components["profiler"]
        schema_manager = components["schema_manager"]

        # Test CREATE TABLE operations
        tables_to_create = [
            ("small_table", ["id INT PRIMARY KEY", "name TEXT", "value INT"]),
            (
                "medium_table",
                ["id INT PRIMARY KEY", "data TEXT", "timestamp DATETIME", "status INT"],
            ),
            (
                "large_table",
                [
                    "id INT PRIMARY KEY",
                    "field1 TEXT",
                    "field2 INT",
                    "field3 DECIMAL",
                    "field4 DATETIME",
                    "field5 TEXT",
                ],
            ),
        ]

        for table_name, columns in tables_to_create:
            profiler.start_operation(
                f"CREATE_TABLE_{table_name.upper()}",
                {"table": table_name, "columns": columns},
            )
            try:
                result = schema_manager.execute_create_table(
                    {"type": "CREATE_TABLE", "table": table_name, "columns": columns}
                )
                profiler.end_operation(result)
            except Exception as e:
                profiler.end_operation(error=e)
                raise

        # Test CREATE INDEX operations
        indexes_to_create = [
            ("idx_small_name", "small_table", "name"),
            ("idx_medium_status", "medium_table", "status"),
            ("idx_large_field2", "large_table", "field2"),
        ]

        for index_name, table_name, column in indexes_to_create:
            profiler.start_operation(
                f"CREATE_INDEX_{index_name.upper()}",
                {"index": index_name, "table": table_name, "column": column},
            )
            try:
                result = schema_manager.execute_create_index(
                    {
                        "type": "CREATE_INDEX",
                        "index_name": index_name,
                        "table": table_name,
                        "column": column,
                    }
                )
                profiler.end_operation(result)
            except Exception as e:
                profiler.end_operation(error=e)
                raise

    def test_02_create_large_dataset(self, profiling_setup):
        """Create large datasets for comprehensive join testing."""
        components = profiling_setup
        profiler = components["profiler"]
        catalog_manager = components["catalog_manager"]
        schema_manager = components["schema_manager"]

        # Create two large tables for join testing (10k records each)
        profiler.start_operation("CREATE_LARGE_TABLE_PRODUCTS")
        try:
            result = schema_manager.execute_create_table(
                {
                    "type": "CREATE_TABLE",
                    "table": "products",
                    "columns": [
                        "product_id INT PRIMARY KEY",
                        "product_name TEXT NOT NULL",
                        "category TEXT",
                        "price DECIMAL(10,2)",
                        "stock INT DEFAULT 0",
                    ],
                }
            )
            profiler.end_operation(result)
        except Exception as e:
            profiler.end_operation(error=e)
            raise

        profiler.start_operation("CREATE_LARGE_TABLE_ORDER_DETAILS")
        try:
            result = schema_manager.execute_create_table(
                {
                    "type": "CREATE_TABLE",
                    "table": "order_details",
                    "columns": [
                        "order_id INT PRIMARY KEY",
                        "product_id INT",
                        "quantity INT",
                        "order_date DATETIME",
                        "status TEXT",
                    ],
                }
            )
            profiler.end_operation(result)
        except Exception as e:
            profiler.end_operation(error=e)
            raise

        # Insert 10k records into products table
        profiler.start_operation("INSERT_10K_PRODUCTS", {"target_records": 10000})
        try:
            for i in range(1, 10001):
                catalog_manager.insert_record(
                    "products",
                    {
                        "product_id": i,
                        "product_name": f"Product {i}",
                        "category": [
                            "Electronics",
                            "Appliances",
                            "Furniture",
                            "Clothing",
                            "Books",
                        ][i % 5],
                        "price": round(10.0 + (i % 1000), 2),
                        "stock": i % 100,
                    },
                )
            profiler.end_operation({"status": "success", "affected_rows": 10000})
        except Exception as e:
            profiler.end_operation(error=e)
            raise

        # Insert 10k records into order_details table
        profiler.start_operation("INSERT_10K_ORDER_DETAILS", {"target_records": 10000})
        try:
            for i in range(1, 10001):
                catalog_manager.insert_record(
                    "order_details",
                    {
                        "order_id": i,
                        "product_id": (i % 10000)
                        + 1,  # Ensure foreign key relationships
                        "quantity": (i % 10) + 1,
                        "order_date": f"2023-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
                        "status": [
                            "pending",
                            "processing",
                            "shipped",
                            "delivered",
                            "cancelled",
                        ][i % 5],
                    },
                )
            profiler.end_operation({"status": "success", "affected_rows": 10000})
        except Exception as e:
            profiler.end_operation(error=e)
            raise

    def test_03_basic_query_operations(self, profiling_setup):
        """Test and profile basic SELECT operations."""
        components = profiling_setup
        profiler = components["profiler"]
        execution_engine = components["execution_engine"]

        # Simple SELECT operations
        select_queries = [
            (
                "SELECT_ALL_SMALL",
                {"type": "SELECT", "table": "small_table", "columns": ["*"]},
            ),
            (
                "SELECT_FILTERED_MEDIUM",
                {
                    "type": "SELECT",
                    "table": "medium_table",
                    "columns": ["*"],
                    "condition": "status = 1",
                },
            ),
            (
                "SELECT_COUNT_PRODUCTS",
                {
                    "type": "SELECT",
                    "table": "products",
                    "columns": ["COUNT(*)"],
                    "operation": "AGGREGATE",
                },
            ),
            (
                "SELECT_DISTINCT_CATEGORIES",
                {
                    "type": "SELECT",
                    "table": "products",
                    "columns": ["DISTINCT category"],
                },
            ),
        ]

        for query_name, plan in select_queries:
            profiler.start_operation(query_name, plan)
            try:
                result = execution_engine.execute(plan)
                profiler.end_operation(result)
            except Exception as e:
                profiler.end_operation(error=e)
                # Don't raise for optional operations

    def test_04_join_operations_without_indexes(self, profiling_setup):
        """Test and profile JOIN operations without indexes (full table scans)."""
        components = profiling_setup
        profiler = components["profiler"]
        execution_engine = components["execution_engine"]

        # Different types of JOINs without indexes
        join_queries = [
            (
                "INNER_JOIN_10K_NO_INDEX",
                {
                    "type": "JOIN",
                    "table1": "products",
                    "table2": "order_details",
                    "condition": "products.product_id = order_details.product_id",
                    "join_type": "INNER",
                    "columns": ["*"],
                },
            ),
            (
                "LEFT_JOIN_10K_NO_INDEX",
                {
                    "type": "JOIN",
                    "table1": "products",
                    "table2": "order_details",
                    "condition": "products.product_id = order_details.product_id",
                    "join_type": "LEFT",
                    "columns": [
                        "products.product_name",
                        "order_details.quantity",
                        "order_details.status",
                    ],
                },
            ),
            (
                "COUNT_JOIN_10K_NO_INDEX",
                {
                    "type": "JOIN",
                    "table1": "products",
                    "table2": "order_details",
                    "condition": "products.product_id = order_details.product_id",
                    "join_type": "INNER",
                    "columns": ["COUNT(*)"],
                    "operation": "AGGREGATE",
                },
            ),
        ]

        for query_name, plan in join_queries:
            profiler.start_operation(
                query_name, {"description": "10k x 10k JOIN without indexes", **plan}
            )
            try:
                result = execution_engine.execute(plan)
                profiler.end_operation(result)
            except Exception as e:
                profiler.end_operation(error=e)

    def test_05_create_join_indexes(self, profiling_setup):
        """Create indexes on join columns."""
        components = profiling_setup
        profiler = components["profiler"]
        schema_manager = components["schema_manager"]

        # Create indexes on join columns
        join_indexes = [
            ("idx_products_id", "products", "product_id"),
            ("idx_order_details_product_id", "order_details", "product_id"),
            ("idx_order_details_status", "order_details", "status"),
            ("idx_products_category", "products", "category"),
        ]

        for index_name, table_name, column in join_indexes:
            profiler.start_operation(
                f"CREATE_JOIN_INDEX_{index_name.upper()}",
                {
                    "index": index_name,
                    "table": table_name,
                    "column": column,
                    "purpose": "optimize_joins",
                },
            )
            try:
                result = schema_manager.execute_create_index(
                    {
                        "type": "CREATE_INDEX",
                        "index_name": index_name,
                        "table": table_name,
                        "column": column,
                    }
                )
                profiler.end_operation(result)
            except Exception as e:
                profiler.end_operation(error=e)

    def test_06_join_operations_with_indexes(self, profiling_setup):
        """Test and profile JOIN operations with indexes (should be faster)."""
        components = profiling_setup
        profiler = components["profiler"]
        execution_engine = components["execution_engine"]

        # Same JOINs as before, but now with indexes
        join_queries = [
            (
                "INNER_JOIN_10K_WITH_INDEX",
                {
                    "type": "JOIN",
                    "table1": "products",
                    "table2": "order_details",
                    "condition": "products.product_id = order_details.product_id",
                    "join_type": "INNER",
                    "columns": ["*"],
                },
            ),
            (
                "LEFT_JOIN_10K_WITH_INDEX",
                {
                    "type": "JOIN",
                    "table1": "products",
                    "table2": "order_details",
                    "condition": "products.product_id = order_details.product_id",
                    "join_type": "LEFT",
                    "columns": [
                        "products.product_name",
                        "order_details.quantity",
                        "order_details.status",
                    ],
                },
            ),
            (
                "FILTERED_JOIN_10K_WITH_INDEX",
                {
                    "type": "JOIN",
                    "table1": "products",
                    "table2": "order_details",
                    "condition": "products.product_id = order_details.product_id",
                    "join_type": "INNER",
                    "columns": ["*"],
                    "where_conditions": [
                        {
                            "column": "order_details.status",
                            "operator": "=",
                            "value": "delivered",
                        }
                    ],
                },
            ),
        ]

        for query_name, plan in join_queries:
            profiler.start_operation(
                query_name, {"description": "10k x 10k JOIN with indexes", **plan}
            )
            try:
                result = execution_engine.execute(plan)
                profiler.end_operation(result)
            except Exception as e:
                profiler.end_operation(error=e)

    def test_07_complex_aggregation_operations(self, profiling_setup):
        """Test and profile complex aggregation operations."""
        components = profiling_setup
        profiler = components["profiler"]
        execution_engine = components["execution_engine"]

        # Complex aggregation queries
        aggregation_queries = [
            (
                "GROUP_BY_CATEGORY_COUNT",
                {
                    "type": "SELECT",
                    "table": "products",
                    "columns": ["category", "COUNT(*)"],
                    "group_by": ["category"],
                    "operation": "GROUP_BY",
                },
            ),
            (
                "SUM_BY_STATUS",
                {
                    "type": "SELECT",
                    "table": "order_details",
                    "columns": ["status", "SUM(quantity)"],
                    "group_by": ["status"],
                    "operation": "GROUP_BY",
                },
            ),
            (
                "AVG_PRICE_BY_CATEGORY",
                {
                    "type": "SELECT",
                    "table": "products",
                    "columns": ["category", "AVG(price)", "COUNT(*)"],
                    "group_by": ["category"],
                    "operation": "GROUP_BY",
                },
            ),
        ]

        for query_name, plan in aggregation_queries:
            profiler.start_operation(query_name, plan)
            try:
                result = execution_engine.execute(plan)
                profiler.end_operation(result)
            except Exception as e:
                profiler.end_operation(error=e)

    def test_08_update_operations(self, profiling_setup):
        """Test and profile UPDATE operations."""
        components = profiling_setup
        profiler = components["profiler"]
        execution_engine = components["execution_engine"]

        # Various UPDATE operations
        update_queries = [
            (
                "UPDATE_SINGLE_RECORD",
                {
                    "type": "UPDATE",
                    "table": "products",
                    "set_clause": {"price": 999.99},
                    "condition": "product_id = 1",
                },
            ),
            (
                "UPDATE_BULK_CATEGORY",
                {
                    "type": "UPDATE",
                    "table": "products",
                    "set_clause": {"category": "Premium Electronics"},
                    "condition": "category = 'Electronics' AND price > 500",
                },
            ),
            (
                "UPDATE_ORDER_STATUS",
                {
                    "type": "UPDATE",
                    "table": "order_details",
                    "set_clause": {"status": "completed"},
                    "condition": "status = 'delivered' AND order_date < '2023-06-01'",
                },
            ),
        ]

        for query_name, plan in update_queries:
            profiler.start_operation(query_name, plan)
            try:
                result = execution_engine.execute(plan)
                profiler.end_operation(result)
            except Exception as e:
                profiler.end_operation(error=e)

    def test_09_delete_operations(self, profiling_setup):
        """Test and profile DELETE operations."""
        components = profiling_setup
        profiler = components["profiler"]
        execution_engine = components["execution_engine"]

        # Various DELETE operations
        delete_queries = [
            (
                "DELETE_CANCELLED_ORDERS",
                {
                    "type": "DELETE",
                    "table": "order_details",
                    "condition": "status = 'cancelled'",
                },
            ),
            (
                "DELETE_OLD_PRODUCTS",
                {
                    "type": "DELETE",
                    "table": "products",
                    "condition": "stock = 0 AND price < 20",
                },
            ),
        ]

        for query_name, plan in delete_queries:
            profiler.start_operation(query_name, plan)
            try:
                result = execution_engine.execute(plan)
                profiler.end_operation(result)
            except Exception as e:
                profiler.end_operation(error=e)

    def test_10_complex_join_scenarios(self, profiling_setup):
        """Test complex JOIN scenarios with multiple conditions."""
        components = profiling_setup
        profiler = components["profiler"]
        execution_engine = components["execution_engine"]

        # Test different join algorithms explicitly
        complex_joins = [
            (
                "HASH_JOIN_10K",
                {
                    "type": "JOIN",
                    "table1": "products",
                    "table2": "order_details",
                    "condition": "products.product_id = order_details.product_id",
                    "join_type": "INNER",
                    "join_algorithm": "HASH",
                    "columns": ["products.product_name", "order_details.quantity"],
                },
            ),
            (
                "NESTED_LOOP_JOIN_10K",
                {
                    "type": "JOIN",
                    "table1": "products",
                    "table2": "order_details",
                    "condition": "products.product_id = order_details.product_id",
                    "join_type": "INNER",
                    "join_algorithm": "NESTED_LOOP",
                    "columns": ["products.category", "order_details.status"],
                },
            ),
            (
                "INDEX_JOIN_10K",
                {
                    "type": "JOIN",
                    "table1": "products",
                    "table2": "order_details",
                    "condition": "products.product_id = order_details.product_id",
                    "join_type": "INNER",
                    "join_algorithm": "INDEX",
                    "columns": ["*"],
                },
            ),
        ]

        for query_name, plan in complex_joins:
            profiler.start_operation(
                query_name,
                {
                    "description": f"10k x 10k JOIN using {plan['join_algorithm']} algorithm",
                    **plan,
                },
            )
            try:
                result = execution_engine.execute(plan)
                profiler.end_operation(result)
            except Exception as e:
                profiler.end_operation(error=e)

    def test_11_transaction_operations(self, profiling_setup):
        """Test and profile transaction operations."""
        components = profiling_setup
        profiler = components["profiler"]
        execution_engine = components["execution_engine"]

        # Transaction operations
        transaction_ops = [
            ("BEGIN_TRANSACTION", {"type": "BEGIN_TRANSACTION"}),
            ("COMMIT_TRANSACTION", {"type": "COMMIT_TRANSACTION"}),
            ("ROLLBACK_TRANSACTION", {"type": "ROLLBACK_TRANSACTION"}),
        ]

        for op_name, plan in transaction_ops:
            profiler.start_operation(op_name, plan)
            try:
                result = execution_engine.execute(plan)
                profiler.end_operation(result)
            except Exception as e:
                profiler.end_operation(error=e)

    def test_12_drop_operations(self, profiling_setup):
        """Test and profile DROP operations."""
        components = profiling_setup
        profiler = components["profiler"]
        schema_manager = components["schema_manager"]

        # Drop indexes first
        indexes_to_drop = [
            "idx_products_id",
            "idx_order_details_product_id",
            "idx_order_details_status",
            "idx_products_category",
        ]

        for index_name in indexes_to_drop:
            profiler.start_operation(
                f"DROP_INDEX_{index_name.upper()}", {"index": index_name}
            )
            try:
                result = schema_manager.execute_drop_index(
                    {"type": "DROP_INDEX", "index_name": index_name}
                )
                profiler.end_operation(result)
            except Exception as e:
                profiler.end_operation(error=e)

        # Drop tables
        tables_to_drop = [
            "order_details",
            "products",
            "large_table",
            "medium_table",
            "small_table",
        ]

        for table_name in tables_to_drop:
            profiler.start_operation(
                f"DROP_TABLE_{table_name.upper()}", {"table": table_name}
            )
            try:
                result = schema_manager.execute_drop_table(
                    {"type": "DROP_TABLE", "table": table_name}
                )
                profiler.end_operation(result)
            except Exception as e:
                profiler.end_operation(error=e)

    def test_13_cpu_intensive_operations(self, profiling_setup):
        """Test CPU-intensive operations with perf profiling."""
        components = profiling_setup
        profiler = components["profiler"]
        schema_manager = components["schema_manager"]

        # Create table with many columns for CPU-intensive operations
        profiler.start_operation(
            "CREATE_COMPLEX_TABLE",
            {"type": "DDL", "operation": "CREATE_TABLE", "complexity": "high_cpu"},
        )

        try:
            result = schema_manager.execute_create_table(
                {
                    "type": "CREATE_TABLE",
                    "table": "complex_table",
                    "columns": [
                        "id INT PRIMARY KEY",
                        "data1 TEXT",
                        "data2 INT",
                        "data3 DECIMAL(10,2)",
                        "data4 DATETIME",
                        "data5 TEXT",
                        "data6 INT",
                        "data7 DECIMAL(15,4)",
                        "data8 TEXT",
                        "data9 DATETIME",
                        "data10 BIGINT",
                    ],
                }
            )
            profiler.end_operation(result)
        except Exception as e:
            profiler.end_operation(error=e)

        # CPU-intensive bulk insert with computed values
        profiler.start_operation(
            "BULK_INSERT_CPU_INTENSIVE",
            {
                "type": "DML",
                "operation": "INSERT",
                "target_records": 5000,
                "complexity": "high_cpu",
            },
        )

        try:
            catalog_manager = components["catalog_manager"]
            for i in range(5000):
                # Simulate CPU-intensive data processing
                data_value = f"processed_data_{i}_{'x' * (i % 100)}"
                computed_value = sum(ord(c) for c in data_value) % 1000000

                catalog_manager.insert_record(
                    "complex_table",
                    {
                        "id": i,
                        "data1": data_value,
                        "data2": computed_value,
                        "data3": round(computed_value * 0.123, 2),
                        "data4": f"2023-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
                        "data5": f"computed_{computed_value}",
                        "data6": i * 7 % 10000,
                        "data7": round(i * 0.456789, 4),
                        "data8": f"bulk_data_{i}",
                        "data9": f"2023-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
                        "data10": i * i,
                    },
                )

            profiler.end_operation({"status": "success", "records_inserted": 5000})
        except Exception as e:
            profiler.end_operation(error=e)

    def test_14_memory_intensive_operations(self, profiling_setup):
        """Test memory-intensive operations with perf profiling."""
        components = profiling_setup
        profiler = components["profiler"]
        execution_engine = components["execution_engine"]

        # Large data aggregation that consumes significant memory
        profiler.start_operation(
            "MEMORY_INTENSIVE_AGGREGATION",
            {
                "type": "QUERY",
                "operation": "AGGREGATION",
                "data_size": "large",
                "memory_pattern": "allocation_heavy",
            },
        )

        try:
            # Create large temporary data structures in memory
            large_data = []
            for i in range(10000):
                large_data.append(
                    {
                        "id": i,
                        "data": "x" * 1000,  # 1KB per record = ~10MB total
                        "computed": sum(range(i % 100)),
                    }
                )

            # Simulate complex aggregation operations
            result = {
                "total_records": len(large_data),
                "total_data_size_mb": len(large_data) / 1024,
                "sum_computed": sum(item["computed"] for item in large_data),
                "avg_computed": sum(item["computed"] for item in large_data)
                / len(large_data),
                "max_computed": max(item["computed"] for item in large_data),
                "min_computed": min(item["computed"] for item in large_data),
            }

            profiler.end_operation(result)
        except Exception as e:
            profiler.end_operation(error=e)

    def test_15_io_intensive_operations(self, profiling_setup):
        """Test I/O intensive operations with perf profiling."""
        components = profiling_setup
        profiler = components["profiler"]
        execution_engine = components["execution_engine"]

        # Complex SELECT operations that trigger disk I/O
        profiler.start_operation(
            "IO_INTENSIVE_QUERIES",
            {
                "type": "QUERY",
                "operation": "COMPLEX_SELECT",
                "io_pattern": "sequential_scan",
                "target_operations": 5,
            },
        )

        try:
            # Full table scan with complex conditions
            results = []
            for i in range(5):
                result = execution_engine.execute(
                    {
                        "type": "SELECT",
                        "table": "complex_table",
                        "columns": ["*"],
                        "condition": f"data2 > {i * 1000} AND data6 < {(i + 1) * 2000}",
                    }
                )
                results.append(result)

            profiler.end_operation(
                {
                    "status": "success",
                    "queries_executed": len(results),
                    "total_results": sum(len(r) if r else 0 for r in results),
                }
            )
        except Exception as e:
            profiler.end_operation(error=e)

    def test_16_mixed_workload_profiling(self, profiling_setup):
        """Test mixed workload with various operation types and perf profiling."""
        components = profiling_setup
        profiler = components["profiler"]
        catalog_manager = components["catalog_manager"]
        execution_engine = components["execution_engine"]

        profiler.start_operation(
            "MIXED_WORKLOAD_STRESS_TEST",
            {
                "type": "MIXED",
                "operations": ["INSERT", "SELECT", "UPDATE", "DELETE"],
                "duration": "30_seconds",
                "complexity": "high",
            },
        )

        try:
            start_time = time.time()
            operations_performed = {
                "inserts": 0,
                "selects": 0,
                "updates": 0,
                "deletes": 0,
            }

            # Run mixed workload for 30 seconds
            while time.time() - start_time < 30:
                operation_type = int(time.time() * 1000) % 4

                if operation_type == 0:  # INSERT
                    record_id = int(time.time() * 1000) % 100000
                    try:
                        catalog_manager.insert_record(
                            "complex_table",
                            {
                                "id": record_id + 10000,  # Avoid conflicts
                                "data1": f"mixed_workload_{record_id}",
                                "data2": record_id,
                                "data3": round(record_id * 0.123, 2),
                                "data4": "2023-12-01",
                                "data5": f"test_{record_id}",
                                "data6": record_id % 1000,
                                "data7": round(record_id * 0.456, 4),
                                "data8": f"workload_{record_id}",
                                "data9": "2023-12-01",
                                "data10": record_id * record_id,
                            },
                        )
                        operations_performed["inserts"] += 1
                    except Exception:
                        pass  # Ignore conflicts in stress test

                elif operation_type == 1:  # SELECT
                    try:
                        # Execute complex query for I/O stress
                        execution_engine.execute(
                            {
                                "type": "SELECT",
                                "table": "complex_table",
                                "columns": ["id", "data1", "data2"],
                                "condition": f"data2 < {int(time.time()) % 1000}",
                            }
                        )
                        operations_performed["selects"] += 1
                    except Exception:
                        pass  # Continue on errors in stress test

                # Add small delay to prevent overwhelming the system
                time.sleep(0.01)

            profiler.end_operation(
                {
                    "status": "success",
                    "duration_seconds": 30,
                    "operations_performed": operations_performed,
                    "total_operations": sum(operations_performed.values()),
                }
            )

        except Exception as e:
            profiler.end_operation(error=e)


def run_comprehensive_perf_demo():
    """
    Run a comprehensive demonstration of the unified perf profiling system.
    This demonstrates both the comprehensive profiling and visualization capabilities.
    """
    print("🚀 Starting Comprehensive Linux Perf Profiling Demo...")
    print("=" * 80)

    # Initialize components (assume they're already imported at module level)
    catalog_manager = CatalogManager()
    index_manager = IndexManager(catalog_manager)
    planner = Planner(catalog_manager, index_manager)
    execution_engine = ExecutionEngine(catalog_manager, index_manager, planner)
    schema_manager = SchemaManager(catalog_manager)

    # Initialize comprehensive profiler with perf integration
    profiler = ComprehensiveProfiler(catalog_manager, enable_perf=True)

    print("📋 System Information:")
    try:
        if profiler.perf_profiler:
            perf_info = profiler.perf_profiler.get_system_info()
            print(f"   Perf Version: {perf_info.get('perf_version', 'unknown')}")
            print(
                f"   CPU Model: {perf_info.get('cpu_info', {}).get('model name', 'unknown')}"
            )
            print(
                f"   Available Events: {len(perf_info.get('available_events', []))} events"
            )
            capabilities = perf_info.get("perf_capabilities", {})
            print(f"   Perf Record: {'✅' if capabilities.get('record') else '❌'}")
            print(
                f"   Hardware Events: {'✅' if capabilities.get('hardware_events') else '❌'}"
            )
        else:
            print("   Perf integration not available")
    except Exception as e:
        print(f"   Error checking perf: {e}")

    print("\n🔧 Setting up test environment...")

    # Create test database
    profiler.start_operation("DEMO_CREATE_DATABASE")
    try:
        catalog_manager.create_database("perf_demo_db")
        catalog_manager.set_current_database("perf_demo_db")
        profiler.end_operation({"status": "success"})
    except Exception as e:
        profiler.end_operation(error=e)
        raise

    print("\n🔍 Running CPU-intensive demo...")

    # CPU-intensive operation demo
    profiler.start_operation(
        "DEMO_CPU_INTENSIVE",
        {
            "type": "CPU_DEMO",
            "description": "CPU-intensive computation with perf profiling",
        },
    )

    try:
        # Create a table
        schema_manager.execute_create_table(
            {
                "type": "CREATE_TABLE",
                "table": "demo_table",
                "columns": ["id INT PRIMARY KEY", "data TEXT", "computed INT"],
            }
        )

        # CPU-intensive data insertion
        for i in range(1000):
            # Simulate CPU-intensive computation
            computed_value = sum(j * j for j in range(i % 100))
            catalog_manager.insert_record(
                "demo_table",
                {
                    "id": i,
                    "data": f"data_{i}_{'x' * (i % 50)}",
                    "computed": computed_value,
                },
            )

        profiler.end_operation({"status": "success", "records": 1000})
    except Exception as e:
        profiler.end_operation(error=e)

    print("\n💾 Running memory-intensive demo...")

    # Memory-intensive operation demo
    profiler.start_operation(
        "DEMO_MEMORY_INTENSIVE",
        {"type": "MEMORY_DEMO", "description": "Memory allocation with perf profiling"},
    )

    try:
        # Create large data structures
        large_data = []
        for i in range(5000):
            large_data.append(
                {
                    "id": i,
                    "data": "x" * 200,  # 200 bytes per record
                    "computed": sum(range(i % 50)),
                }
            )

        # Process the data
        result = {
            "total_records": len(large_data),
            "total_size_mb": len(large_data) * 200 / (1024 * 1024),
            "sum_computed": sum(item["computed"] for item in large_data),
        }

        profiler.end_operation(result)
    except Exception as e:
        profiler.end_operation(error=e)

    print("\n🔄 Running I/O-intensive demo...")

    # I/O-intensive operation demo
    profiler.start_operation(
        "DEMO_IO_INTENSIVE",
        {"type": "IO_DEMO", "description": "Database queries with perf profiling"},
    )

    try:
        results = []
        for i in range(10):
            result = execution_engine.execute(
                {
                    "type": "SELECT",
                    "table": "demo_table",
                    "columns": ["*"],
                    "condition": f"computed > {i * 100}",
                }
            )
            results.append(result)

        profiler.end_operation(
            {
                "status": "success",
                "queries": len(results),
                "total_results": sum(len(r) if r else 0 for r in results),
            }
        )
    except Exception as e:
        profiler.end_operation(error=e)

    print("\n📊 Generating comprehensive report...")

    # Generate detailed report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"/tmp/perf_demo_report_{timestamp}.json"
    profiler.generate_detailed_report(report_file)

    # Stop profiling
    profiler.stop_profiling()

    print("\n📈 Visualizations available...")

    # Note: Visualizations can be generated separately using generate_profiling_visualizations.py
    try:
        output_dir = f"/tmp/perf_demo_visualizations_{timestamp}"
        os.makedirs(output_dir, exist_ok=True)

        print(f"   Visualization directory created: {output_dir}")
        print("   To generate visualizations, run:")
        print("   python generate_profiling_visualizations.py")
        print("   (This will include the new perf-specific visualizations)")
    except Exception as e:
        print(f"   Error creating visualization directory: {e}")

    # Print summary
    summary = profiler.get_operation_summary()

    print("\n" + "=" * 80)
    print("COMPREHENSIVE PERF DEMO SUMMARY")
    print("=" * 80)
    print(f"Total Operations: {summary['total_operations']}")
    print(f"Operation Types: {summary['operation_types']}")
    print(f"Total Execution Time: {summary['total_execution_time']:.4f} seconds")

    if summary["performance_statistics"]:
        stats = summary["performance_statistics"]
        print(f"Average Execution Time: {stats['average_execution_time']:.4f} seconds")
        print(f"Max Execution Time: {stats['max_execution_time']:.4f} seconds")

    if summary["resource_usage_summary"]:
        resources = summary["resource_usage_summary"]
        print(f"Total Memory Usage: {resources['total_memory_mb']:.2f} MB")
        print(f"Average CPU Usage: {resources['average_cpu_percent']:.2f}%")

    print(f"\nDetailed report saved to: {report_file}")

    # Show operations by type
    print("\nOperations by Type:")
    for op_type, stats in summary["operations_by_type"].items():
        print(
            f"  {op_type}: {stats['count']} operations, {stats['average_time']:.4f}s avg"
        )

    print("\n✅ Comprehensive perf profiling demo completed!")
    print("=" * 80)

    return report_file, output_dir if "output_dir" in locals() else None


if __name__ == "__main__":
    # Allow running the comprehensive perf demo standalone
    if len(sys.argv) > 1 and sys.argv[1] == "demo":
        run_comprehensive_perf_demo()
    else:
        # Run the normal pytest tests
        pytest.main([__file__])
