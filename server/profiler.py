"""
Comprehensive profiling module for DBMS performance monitoring.
Tracks CPU, memory, I/O, and query execution metrics.
"""

import psutil
import time
import threading
import logging
import json
import os
from collections import defaultdict, deque
from datetime import datetime, timedelta
import statistics


class SystemProfiler:
    """
    System-level profiler for monitoring CPU, memory, disk I/O, and network usage.
    """

    def __init__(self, catalog_manager, sample_interval=1.0):
        """
        Initialize the system profiler.

        Args:
            catalog_manager: Reference to catalog manager for storing metrics
            sample_interval: How often to sample system metrics (seconds)
        """
        self.catalog_manager = catalog_manager
        self.sample_interval = sample_interval
        self.is_profiling = False
        self.profiling_thread = None

        # Metrics storage (in-memory circular buffers)
        self.max_samples = 3600  # Keep 1 hour of data at 1-second intervals
        self.cpu_samples = deque(maxlen=self.max_samples)
        self.memory_samples = deque(maxlen=self.max_samples)
        self.disk_samples = deque(maxlen=self.max_samples)
        self.network_samples = deque(maxlen=self.max_samples)

        # Query-level metrics
        self.query_metrics = deque(maxlen=1000)  # Keep last 1000 queries

        # Lock for thread safety
        self.metrics_lock = threading.RLock()

        # Process reference for detailed monitoring
        self.process = psutil.Process()

        logging.info(
            "SystemProfiler initialized with sample interval: %s seconds",
            sample_interval,
        )

    def start_profiling(self):
        """Start continuous system profiling."""
        if self.is_profiling:
            return

        self.is_profiling = True
        self.profiling_thread = threading.Thread(
            target=self._profiling_loop, daemon=True
        )
        self.profiling_thread.start()
        logging.info("System profiling started")

    def stop_profiling(self):
        """Stop system profiling."""
        self.is_profiling = False
        if self.profiling_thread:
            self.profiling_thread.join(timeout=2.0)
        logging.info("System profiling stopped")

    def _profiling_loop(self):
        """Main profiling loop that collects system metrics."""
        last_disk_io = None
        last_network_io = None

        while self.is_profiling:
            try:
                timestamp = time.time()

                # CPU metrics
                cpu_percent = psutil.cpu_percent(interval=None)
                cpu_count = psutil.cpu_count()
                load_avg = os.getloadavg() if hasattr(os, "getloadavg") else [0, 0, 0]

                # Memory metrics
                memory = psutil.virtual_memory()
                swap = psutil.swap_memory()

                # Process-specific metrics
                process_memory = self.process.memory_info()
                process_cpu = self.process.cpu_percent()

                # Disk I/O metrics
                disk_io = psutil.disk_io_counters()
                disk_usage = psutil.disk_usage("/")

                # Calculate disk I/O rates
                disk_read_rate = 0
                disk_write_rate = 0
                if last_disk_io and disk_io:
                    time_delta = self.sample_interval
                    disk_read_rate = (
                        disk_io.read_bytes - last_disk_io.read_bytes
                    ) / time_delta
                    disk_write_rate = (
                        disk_io.write_bytes - last_disk_io.write_bytes
                    ) / time_delta
                last_disk_io = disk_io

                # Network I/O metrics
                network_io = psutil.net_io_counters()
                network_recv_rate = 0
                network_sent_rate = 0
                if last_network_io and network_io:
                    time_delta = self.sample_interval
                    network_recv_rate = (
                        network_io.bytes_recv - last_network_io.bytes_recv
                    ) / time_delta
                    network_sent_rate = (
                        network_io.bytes_sent - last_network_io.bytes_sent
                    ) / time_delta
                last_network_io = network_io

                with self.metrics_lock:
                    # Store CPU metrics
                    self.cpu_samples.append(
                        {
                            "timestamp": timestamp,
                            "cpu_percent": cpu_percent,
                            "cpu_count": cpu_count,
                            "load_avg_1m": load_avg[0],
                            "load_avg_5m": load_avg[1],
                            "load_avg_15m": load_avg[2],
                            "process_cpu_percent": process_cpu,
                        }
                    )

                    # Store memory metrics
                    self.memory_samples.append(
                        {
                            "timestamp": timestamp,
                            "total_memory_gb": memory.total / (1024**3),
                            "available_memory_gb": memory.available / (1024**3),
                            "used_memory_gb": memory.used / (1024**3),
                            "memory_percent": memory.percent,
                            "swap_total_gb": swap.total / (1024**3),
                            "swap_used_gb": swap.used / (1024**3),
                            "swap_percent": swap.percent,
                            "process_memory_mb": process_memory.rss / (1024**2),
                            "process_memory_vms_mb": process_memory.vms / (1024**2),
                        }
                    )

                    # Store disk metrics
                    self.disk_samples.append(
                        {
                            "timestamp": timestamp,
                            "disk_total_gb": disk_usage.total / (1024**3),
                            "disk_used_gb": disk_usage.used / (1024**3),
                            "disk_free_gb": disk_usage.free / (1024**3),
                            "disk_percent": (disk_usage.used / disk_usage.total) * 100,
                            "disk_read_rate_mbps": disk_read_rate / (1024**2),
                            "disk_write_rate_mbps": disk_write_rate / (1024**2),
                        }
                    )

                    # Store network metrics
                    self.network_samples.append(
                        {
                            "timestamp": timestamp,
                            "network_recv_rate_mbps": network_recv_rate / (1024**2),
                            "network_sent_rate_mbps": network_sent_rate / (1024**2),
                        }
                    )

                time.sleep(self.sample_interval)

            except Exception as e:
                logging.error(f"Error in profiling loop: {str(e)}")
                time.sleep(self.sample_interval)

    def profile_query(self, query_text, execution_time, result_rows=0, error=None):
        """
        Profile a specific query execution.

        Args:
            query_text: SQL query text
            execution_time: Time taken to execute (seconds)
            result_rows: Number of rows returned
            error: Error message if query failed
        """
        with self.metrics_lock:
            # Get current system state
            cpu_percent = psutil.cpu_percent(interval=None)
            memory = psutil.virtual_memory()
            process_memory = self.process.memory_info()

            query_metric = {
                "timestamp": time.time(),
                "query_text": query_text[:500],  # Truncate long queries
                "execution_time_ms": execution_time * 1000,
                "result_rows": result_rows,
                "error": error,
                "cpu_percent_during": cpu_percent,
                "memory_used_mb": process_memory.rss / (1024**2),
                "system_memory_percent": memory.percent,
            }

            self.query_metrics.append(query_metric)

            # Also store in system database if available
            self._store_query_metric(query_metric)

    def _store_query_metric(self, metric):
        """Store query metric in system database."""
        try:
            # Store in system database
            self._ensure_system_database()

            # Switch to system database temporarily
            current_db = self.catalog_manager.get_current_database()
            self.catalog_manager.set_current_database("_system")

            # Insert the metric
            self.catalog_manager.insert_record("query_metrics", metric)

            # Restore original database
            if current_db:
                self.catalog_manager.set_current_database(current_db)

        except Exception as e:
            logging.error(f"Error storing query metric: {str(e)}")

    def _ensure_system_database(self):
        """Ensure system database and tables exist."""
        try:
            # Create system database if it doesn't exist
            if "_system" not in self.catalog_manager.list_databases():
                self.catalog_manager.create_database("_system")

            # Switch to system database
            original_db = self.catalog_manager.get_current_database()
            self.catalog_manager.set_current_database("_system")

            # Create tables if they don't exist
            tables = self.catalog_manager.list_tables("_system")

            if "query_metrics" not in tables:
                self.catalog_manager.create_table(
                    "query_metrics",
                    [
                        "id INTEGER IDENTITY(1,1) PRIMARY KEY",
                        "timestamp FLOAT",
                        "query_text TEXT",
                        "execution_time_ms FLOAT",
                        "result_rows INTEGER",
                        "error TEXT",
                        "cpu_percent_during FLOAT",
                        "memory_used_mb FLOAT",
                        "system_memory_percent FLOAT",
                    ],
                )

            if "system_metrics" not in tables:
                self.catalog_manager.create_table(
                    "system_metrics",
                    [
                        "id INTEGER IDENTITY(1,1) PRIMARY KEY",
                        "timestamp FLOAT",
                        "metric_type VARCHAR(50)",
                        "cpu_percent FLOAT",
                        "memory_percent FLOAT",
                        "disk_percent FLOAT",
                        "process_memory_mb FLOAT",
                        "disk_read_rate_mbps FLOAT",
                        "disk_write_rate_mbps FLOAT",
                    ],
                )

            if "dbms_statistics" not in tables:
                self.catalog_manager.create_table(
                    "dbms_statistics",
                    [
                        "id INTEGER IDENTITY(1,1) PRIMARY KEY",
                        "timestamp FLOAT",
                        "total_queries INTEGER",
                        "successful_queries INTEGER",
                        "failed_queries INTEGER",
                        "avg_query_time_ms FLOAT",
                        "total_databases INTEGER",
                        "total_tables INTEGER",
                        "total_indexes INTEGER",
                        "cache_hit_ratio FLOAT",
                        "uptime_seconds FLOAT",
                    ],
                )

            # Restore original database
            if original_db:
                self.catalog_manager.set_current_database(original_db)

        except Exception as e:
            logging.error(f"Error ensuring system database: {str(e)}")

    def get_current_metrics(self):
        """Get current system metrics snapshot."""
        try:
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage("/")
            process_memory = self.process.memory_info()

            return {
                "timestamp": time.time(),
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "memory_used_gb": memory.used / (1024**3),
                "memory_available_gb": memory.available / (1024**3),
                "disk_percent": (disk.used / disk.total) * 100,
                "disk_free_gb": disk.free / (1024**3),
                "process_memory_mb": process_memory.rss / (1024**2),
                "process_cpu_percent": self.process.cpu_percent(),
            }
        except Exception as e:
            logging.error(f"Error getting current metrics: {str(e)}")
            return {}

    def get_metrics_summary(self, duration_minutes=60):
        """
        Get summary of metrics over the specified duration.

        Args:
            duration_minutes: Duration to summarize (default 60 minutes)

        Returns:
            dict: Summary statistics
        """
        cutoff_time = time.time() - (duration_minutes * 60)

        with self.metrics_lock:
            # Filter samples by time
            recent_cpu = [s for s in self.cpu_samples if s["timestamp"] >= cutoff_time]
            recent_memory = [
                s for s in self.memory_samples if s["timestamp"] >= cutoff_time
            ]
            recent_queries = [
                q for q in self.query_metrics if q["timestamp"] >= cutoff_time
            ]

            if not recent_cpu or not recent_memory:
                return {"error": "Insufficient data for summary"}

            # Calculate CPU statistics
            cpu_values = [s["cpu_percent"] for s in recent_cpu]
            cpu_stats = {
                "avg": statistics.mean(cpu_values),
                "min": min(cpu_values),
                "max": max(cpu_values),
                "median": statistics.median(cpu_values),
            }

            # Calculate memory statistics
            memory_values = [s["memory_percent"] for s in recent_memory]
            memory_stats = {
                "avg": statistics.mean(memory_values),
                "min": min(memory_values),
                "max": max(memory_values),
                "median": statistics.median(memory_values),
            }

            # Calculate query statistics
            query_stats = {
                "total_queries": len(recent_queries),
                "successful_queries": len(
                    [q for q in recent_queries if not q.get("error")]
                ),
                "failed_queries": len([q for q in recent_queries if q.get("error")]),
                "avg_execution_time_ms": 0,
                "min_execution_time_ms": 0,
                "max_execution_time_ms": 0,
            }

            if recent_queries:
                exec_times = [q["execution_time_ms"] for q in recent_queries]
                query_stats.update(
                    {
                        "avg_execution_time_ms": statistics.mean(exec_times),
                        "min_execution_time_ms": min(exec_times),
                        "max_execution_time_ms": max(exec_times),
                    }
                )

            return {
                "period_minutes": duration_minutes,
                "sample_count": len(recent_cpu),
                "cpu_stats": cpu_stats,
                "memory_stats": memory_stats,
                "query_stats": query_stats,
                "current_metrics": self.get_current_metrics(),
            }

    def save_metrics_to_database(self):
        """Save current metrics to the system database."""
        try:
            current_metrics = self.get_current_metrics()
            if not current_metrics:
                return

            self._ensure_system_database()

            # Switch to system database
            original_db = self.catalog_manager.get_current_database()
            self.catalog_manager.set_current_database("_system")

            # Insert system metrics
            metric_record = {
                "timestamp": current_metrics["timestamp"],
                "metric_type": "system_snapshot",
                "cpu_percent": current_metrics["cpu_percent"],
                "memory_percent": current_metrics["memory_percent"],
                "disk_percent": current_metrics["disk_percent"],
                "process_memory_mb": current_metrics["process_memory_mb"],
                "disk_read_rate_mbps": 0.0,  # Would need to calculate
                "disk_write_rate_mbps": 0.0,  # Would need to calculate
            }

            self.catalog_manager.insert_record("system_metrics", metric_record)

            # Restore original database
            if original_db:
                self.catalog_manager.set_current_database(original_db)

            logging.info("Saved metrics to system database")

        except Exception as e:
            logging.error(f"Error saving metrics to database: {str(e)}")


class DBMSStatistics:
    """
    Collects and manages DBMS-level statistics.
    """

    def __init__(self, catalog_manager, profiler):
        """
        Initialize DBMS statistics collector.

        Args:
            catalog_manager: Reference to catalog manager
            profiler: Reference to system profiler
        """
        self.catalog_manager = catalog_manager
        self.profiler = profiler
        self.start_time = time.time()

        # Counters
        self.total_queries = 0
        self.successful_queries = 0
        self.failed_queries = 0
        self.total_execution_time = 0.0

        # Lock for thread safety
        self.stats_lock = threading.RLock()

        logging.info("DBMS Statistics collector initialized")

    def record_query(self, execution_time, success=True):
        """
        Record a query execution.

        Args:
            execution_time: Time taken to execute the query
            success: Whether the query was successful
        """
        with self.stats_lock:
            self.total_queries += 1
            self.total_execution_time += execution_time

            if success:
                self.successful_queries += 1
            else:
                self.failed_queries += 1

    def get_statistics(self):
        """Get current DBMS statistics."""
        with self.stats_lock:
            uptime = time.time() - self.start_time

            stats = {
                "uptime_seconds": uptime,
                "uptime_hours": uptime / 3600,
                "total_queries": self.total_queries,
                "successful_queries": self.successful_queries,
                "failed_queries": self.failed_queries,
                "success_rate": (self.successful_queries / max(1, self.total_queries))
                * 100,
                "avg_query_time_ms": (
                    self.total_execution_time / max(1, self.total_queries)
                )
                * 1000,
                "queries_per_minute": (self.total_queries / max(1, uptime / 60)),
                "total_databases": len(self.catalog_manager.list_databases()),
                "current_database": self.catalog_manager.get_current_database(),
            }

            # Add table and index counts
            try:
                current_db = self.catalog_manager.get_current_database()
                if current_db:
                    stats["total_tables"] = len(
                        self.catalog_manager.list_tables(current_db)
                    )
                else:
                    stats["total_tables"] = 0

                # Count indexes across all databases
                total_indexes = 0
                for db_name in self.catalog_manager.list_databases():
                    for table_name in self.catalog_manager.list_tables(db_name):
                        indexes = self.catalog_manager.get_indexes_for_table(table_name)
                        total_indexes += len(indexes)

                stats["total_indexes"] = total_indexes
            except Exception as e:
                logging.error(f"Error counting tables/indexes: {str(e)}")
                stats["total_tables"] = 0
                stats["total_indexes"] = 0

            # Add system metrics if available
            if self.profiler:
                current_metrics = self.profiler.get_current_metrics()
                stats.update(
                    {
                        "current_cpu_percent": current_metrics.get("cpu_percent", 0),
                        "current_memory_percent": current_metrics.get(
                            "memory_percent", 0
                        ),
                        "current_disk_percent": current_metrics.get("disk_percent", 0),
                        "process_memory_mb": current_metrics.get(
                            "process_memory_mb", 0
                        ),
                    }
                )

            return stats

    def save_statistics(self):
        """Save current statistics to the system database."""
        try:
            stats = self.get_statistics()

            self.profiler._ensure_system_database()

            # Switch to system database
            original_db = self.catalog_manager.get_current_database()
            self.catalog_manager.set_current_database("_system")

            # Insert DBMS statistics
            stats_record = {
                "timestamp": time.time(),
                "total_queries": stats["total_queries"],
                "successful_queries": stats["successful_queries"],
                "failed_queries": stats["failed_queries"],
                "avg_query_time_ms": stats["avg_query_time_ms"],
                "total_databases": stats["total_databases"],
                "total_tables": stats["total_tables"],
                "total_indexes": stats["total_indexes"],
                "cache_hit_ratio": 0.0,  # Could implement cache hit tracking
                "uptime_seconds": stats["uptime_seconds"],
            }

            self.catalog_manager.insert_record("dbms_statistics", stats_record)

            # Restore original database
            if original_db:
                self.catalog_manager.set_current_database(original_db)

            logging.info("Saved DBMS statistics to system database")

        except Exception as e:
            logging.error(f"Error saving DBMS statistics: {str(e)}")
