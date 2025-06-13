"""
Advanced Cost Estimation Module for Query Optimization.

This module provides sophisticated cost estimation capabilities that rival
enterprise-grade optimizers like Oracle CBO, PostgreSQL, and SQL Server.

Features:
- Multi-dimensional cost models (CPU, I/O, Memory, Network)
- Operator-specific cost functions with calibration
- Memory-aware cost estimation
- Parallel execution cost modeling
- Adaptive cost model parameters
- Hardware-aware cost calibration
"""

import logging
import math
import time
import psutil
from typing import Dict, List, Tuple, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum
import json
import numpy as np


class CostComponent(Enum):
    """Different components of query execution cost."""

    CPU = "cpu"
    IO_SEQUENTIAL = "io_sequential"
    IO_RANDOM = "io_random"
    MEMORY = "memory"
    NETWORK = "network"
    STARTUP = "startup"


class OperatorType(Enum):
    """Types of query operators."""

    TABLE_SCAN = "table_scan"
    INDEX_SCAN = "index_scan"
    INDEX_ONLY_SCAN = "index_only_scan"
    BITMAP_SCAN = "bitmap_scan"
    NESTED_LOOP_JOIN = "nested_loop_join"
    HASH_JOIN = "hash_join"
    MERGE_JOIN = "merge_join"
    SORT = "sort"
    AGGREGATE = "aggregate"
    FILTER = "filter"
    PROJECT = "project"
    UNION = "union"
    INTERSECT = "intersect"
    EXCEPT = "except"


@dataclass
class CostFactors:
    """Hardware and system-specific cost factors."""

    # CPU costs (per operation)
    cpu_tuple_cost: float = 0.01  # Cost per tuple processed
    cpu_index_tuple_cost: float = 0.005  # Cost per index tuple
    cpu_operator_cost: float = 0.0025  # Cost per operator invocation

    # I/O costs (per page)
    seq_page_cost: float = 1.0  # Sequential page access
    random_page_cost: float = 4.0  # Random page access
    page_size: int = 8192  # Page size in bytes

    # Memory costs
    work_mem: int = 4 * 1024 * 1024  # Working memory in bytes (4MB)
    shared_buffers: int = 128 * 1024 * 1024  # Shared buffer size (128MB)
    effective_cache_size: int = 4 * 1024 * 1024 * 1024  # Effective cache (4GB)

    # Join costs
    hash_mem_multiplier: float = 1.0  # Hash table memory multiplier
    sort_mem_multiplier: float = 1.0  # Sort memory multiplier

    # Parallel execution
    parallel_tuple_cost: float = 0.1  # Parallel coordination overhead
    parallel_setup_cost: float = 1000.0  # Parallel setup cost
    max_parallel_workers: int = 4  # Maximum parallel workers

    # Network costs (for distributed queries)
    network_byte_cost: float = 0.0001  # Cost per byte transferred


@dataclass
class OperatorCost:
    """Detailed cost breakdown for an operator."""

    total_cost: float
    startup_cost: float
    cpu_cost: float
    io_cost: float
    memory_cost: float
    network_cost: float
    estimated_rows: int
    estimated_width: int  # Average row width in bytes
    metadata: Dict[str, Any]


class CostCalibrator:
    """
    Calibrates cost model parameters based on system characteristics.

    Performs micro-benchmarks to determine accurate cost factors
    for the current hardware and system configuration.
    """

    def __init__(self):
        self.calibration_cache = {}
        self.last_calibration = 0
        self.calibration_interval = 3600  # Recalibrate every hour

    def calibrate_cost_factors(self) -> CostFactors:
        """
        Calibrate cost factors based on system performance.

        Returns:
            CostFactors with calibrated values
        """
        current_time = time.time()

        if (
            current_time - self.last_calibration < self.calibration_interval
            and "cost_factors" in self.calibration_cache
        ):
            return self.calibration_cache["cost_factors"]

        logging.info("Calibrating cost model parameters...")

        factors = CostFactors()

        # Get system information
        cpu_count = psutil.cpu_count()
        memory_info = psutil.virtual_memory()

        # Calibrate CPU costs
        factors.cpu_tuple_cost = self._calibrate_cpu_cost()
        factors.cpu_operator_cost = factors.cpu_tuple_cost * 0.25
        factors.cpu_index_tuple_cost = factors.cpu_tuple_cost * 0.5

        # Calibrate I/O costs
        factors.seq_page_cost, factors.random_page_cost = self._calibrate_io_costs()

        # Adjust memory parameters
        factors.work_mem = min(
            memory_info.available // 16, 64 * 1024 * 1024
        )  # Max 64MB
        factors.shared_buffers = min(
            memory_info.total // 4, 1024 * 1024 * 1024
        )  # Max 1GB
        factors.effective_cache_size = memory_info.available

        # Adjust parallel parameters
        factors.max_parallel_workers = max(1, cpu_count // 2)

        self.calibration_cache["cost_factors"] = factors
        self.last_calibration = current_time

        logging.info(
            f"Cost model calibrated: CPU={factors.cpu_tuple_cost:.6f}, "
            f"SeqIO={factors.seq_page_cost:.2f}, RandomIO={factors.random_page_cost:.2f}"
        )

        return factors

    def _calibrate_cpu_cost(self) -> float:
        """Calibrate CPU cost by running micro-benchmarks."""
        try:
            # Simple CPU-intensive operation benchmark
            start_time = time.time()
            iterations = 100000

            # Simulate tuple processing
            total = 0
            for i in range(iterations):
                total += i * 2 + 1  # Simple arithmetic operations

            elapsed = time.time() - start_time

            # Calculate cost per operation
            cpu_cost = elapsed / iterations if iterations > 0 else 0.01

            # Clamp to reasonable bounds
            return max(0.001, min(cpu_cost, 0.1))

        except Exception as e:
            logging.warning(f"CPU calibration failed: {e}")
            return 0.01  # Default value

    def _calibrate_io_costs(self) -> Tuple[float, float]:
        """Calibrate I/O costs by testing disk performance."""
        try:
            # This would involve actual I/O benchmarking
            # For now, use system-specific defaults

            # Get disk information (simplified)
            disk_usage = psutil.disk_usage("/")

            # Estimate based on available space and type
            # SSD vs HDD heuristics (simplified)
            if disk_usage.total > 1024 * 1024 * 1024 * 1024:  # > 1TB suggests HDD
                seq_cost = 1.0
                random_cost = 4.0
            else:  # Assume SSD
                seq_cost = 0.5
                random_cost = 1.5

            return seq_cost, random_cost

        except Exception as e:
            logging.warning(f"I/O calibration failed: {e}")
            return 1.0, 4.0  # Default values


class AdvancedCostEstimator:
    """
    Advanced cost estimator with multi-dimensional cost modeling.

    Provides accurate cost estimates for query operators based on:
    - Statistical information about data
    - System hardware characteristics
    - Memory availability and usage patterns
    - Parallel execution capabilities
    - Network topology (for distributed queries)
    """

    def __init__(self, statistics_collector, catalog_manager):
        """
        Initialize the advanced cost estimator.

        Args:
            statistics_collector: Statistics collector for data characteristics
            catalog_manager: Catalog manager for schema information
        """
        self.statistics_collector = statistics_collector
        self.catalog_manager = catalog_manager
        self.calibrator = CostCalibrator()

        # Initialize cost factors
        self.cost_factors = self.calibrator.calibrate_cost_factors()

        # Cost estimation cache
        self.cost_cache = {}
        self.cache_hits = 0
        self.cache_misses = 0

        logging.info("Initialized AdvancedCostEstimator")

    def estimate_table_scan_cost(
        self, table_name: str, table_stats: Any = None
    ) -> OperatorCost:
        """
        Estimate the cost of a table scan operation.

        Args:
            table_name: Name of the table to scan
            table_stats: Pre-computed table statistics (optional)

        Returns:
            OperatorCost with detailed breakdown
        """
        cache_key = f"table_scan:{table_name}"

        if cache_key in self.cost_cache:
            self.cache_hits += 1
            return self.cost_cache[cache_key]

        self.cache_misses += 1

        # Get table statistics
        if table_stats is None:
            table_stats = self.statistics_collector.collect_table_statistics(table_name)

        # Calculate I/O cost
        pages_to_read = table_stats.page_count
        io_cost = pages_to_read * self.cost_factors.seq_page_cost

        # Calculate CPU cost
        rows_to_process = table_stats.row_count
        cpu_cost = rows_to_process * self.cost_factors.cpu_tuple_cost

        # Startup cost (opening file, initializing scan)
        startup_cost = 10.0

        # Memory cost (minimal for table scan)
        memory_cost = 0.0

        # Network cost (zero for local scan)
        network_cost = 0.0

        total_cost = startup_cost + cpu_cost + io_cost + memory_cost + network_cost

        result = OperatorCost(
            total_cost=total_cost,
            startup_cost=startup_cost,
            cpu_cost=cpu_cost,
            io_cost=io_cost,
            memory_cost=memory_cost,
            network_cost=network_cost,
            estimated_rows=table_stats.row_count,
            estimated_width=int(table_stats.avg_row_length),
            metadata={
                "operator_type": OperatorType.TABLE_SCAN,
                "table_name": table_name,
                "pages_read": pages_to_read,
            },
        )

        self.cost_cache[cache_key] = result
        return result

    def estimate_index_scan_cost(
        self,
        table_name: str,
        index_name: str,
        selectivity: float = 0.1,
        table_stats: Any = None,
    ) -> OperatorCost:
        """
        Estimate the cost of an index scan operation.

        Args:
            table_name: Name of the table
            index_name: Name of the index
            selectivity: Estimated selectivity of the scan
            table_stats: Pre-computed table statistics

        Returns:
            OperatorCost with detailed breakdown
        """
        cache_key = f"index_scan:{table_name}:{index_name}:{selectivity}"

        if cache_key in self.cost_cache:
            self.cache_hits += 1
            return self.cost_cache[cache_key]

        self.cache_misses += 1

        # Get table statistics
        if table_stats is None:
            table_stats = self.statistics_collector.collect_table_statistics(table_name)

        # Get index statistics
        index_stats = table_stats.index_stats.get(index_name)
        if not index_stats:
            # Fallback to table scan cost
            return self.estimate_table_scan_cost(table_name, table_stats)

        # Estimate rows to be returned
        estimated_rows = int(table_stats.row_count * selectivity)

        # Index traversal cost (logarithmic in index size)
        index_pages = max(1, index_stats.leaf_pages)
        index_cpu_cost = math.log2(index_pages) * self.cost_factors.cpu_index_tuple_cost

        # Index I/O cost (depends on clustering factor)
        if index_stats.clustering_factor < 0.1:  # Well-clustered index
            # Sequential access pattern
            data_pages_to_read = max(
                1,
                int(
                    estimated_rows
                    * table_stats.avg_row_length
                    / self.cost_factors.page_size
                ),
            )
            io_cost = (
                index_stats.height
                * self.cost_factors.random_page_cost  # Index traversal
                + data_pages_to_read * self.cost_factors.seq_page_cost
            )  # Data access
        else:
            # Random access pattern
            io_cost = (
                index_stats.height
                * self.cost_factors.random_page_cost  # Index traversal
                + estimated_rows * self.cost_factors.random_page_cost
            )  # Random data access

        # CPU cost for processing returned rows
        cpu_cost = index_cpu_cost + (estimated_rows * self.cost_factors.cpu_tuple_cost)

        # Startup cost
        startup_cost = 5.0

        # Memory cost (minimal)
        memory_cost = 0.0

        # Network cost (zero for local scan)
        network_cost = 0.0

        total_cost = startup_cost + cpu_cost + io_cost + memory_cost + network_cost

        result = OperatorCost(
            total_cost=total_cost,
            startup_cost=startup_cost,
            cpu_cost=cpu_cost,
            io_cost=io_cost,
            memory_cost=memory_cost,
            network_cost=network_cost,
            estimated_rows=estimated_rows,
            estimated_width=int(table_stats.avg_row_length),
            metadata={
                "operator_type": OperatorType.INDEX_SCAN,
                "table_name": table_name,
                "index_name": index_name,
                "selectivity": selectivity,
                "clustering_factor": index_stats.clustering_factor,
            },
        )

        self.cost_cache[cache_key] = result
        return result

    def estimate_join_cost(
        self,
        left_plan: Any,
        right_plan: Any,
        join_algorithm: Any,
        join_condition: Any = None,
    ) -> Tuple[float, int]:
        """
        Estimate the cost and cardinality of a join operation.

        Args:
            left_plan: Left input plan
            right_plan: Right input plan
            join_algorithm: Join algorithm to use
            join_condition: Join condition (optional)

        Returns:
            Tuple of (estimated_cost, estimated_rows)
        """
        from .join_order_enumerator import (
            JoinAlgorithm,
        )  # Import here to avoid circular imports

        left_rows = left_plan.estimated_rows
        right_rows = right_plan.estimated_rows
        left_cost = left_plan.estimated_cost
        right_cost = right_plan.estimated_cost

        # Estimate join selectivity
        if join_condition:
            selectivity = join_condition.selectivity
        else:
            selectivity = 0.1  # Default for cross joins

        # Estimate output cardinality
        if join_algorithm == JoinAlgorithm.NESTED_LOOP:
            estimated_rows = int(left_rows * right_rows * selectivity)
        else:
            estimated_rows = int(left_rows * right_rows * selectivity)

        # Estimate join cost based on algorithm
        if join_algorithm == JoinAlgorithm.NESTED_LOOP:
            # Nested loop: O(n * m)
            join_cost = left_rows * right_rows * self.cost_factors.cpu_tuple_cost

        elif join_algorithm == JoinAlgorithm.HASH_JOIN:
            # Hash join: O(n + m) with memory considerations
            build_cost = (
                right_rows * self.cost_factors.cpu_tuple_cost * 2
            )  # Build hash table
            probe_cost = left_rows * self.cost_factors.cpu_tuple_cost  # Probe phase

            # Memory cost for hash table
            hash_table_size = right_rows * 50  # Estimate 50 bytes per hash entry
            if hash_table_size > self.cost_factors.work_mem:
                # Hash table doesn't fit in memory - add I/O cost
                spill_cost = hash_table_size * 0.1  # Cost of spilling to disk
                build_cost += spill_cost

            join_cost = build_cost + probe_cost

        elif join_algorithm == JoinAlgorithm.MERGE_JOIN:
            # Merge join: O(n log n + m log m) for sorting + O(n + m) for merging
            left_sort_cost = (
                left_rows
                * math.log2(max(2, left_rows))
                * self.cost_factors.cpu_tuple_cost
            )
            right_sort_cost = (
                right_rows
                * math.log2(max(2, right_rows))
                * self.cost_factors.cpu_tuple_cost
            )
            merge_cost = (left_rows + right_rows) * self.cost_factors.cpu_tuple_cost

            join_cost = left_sort_cost + right_sort_cost + merge_cost

        else:
            # Default to nested loop cost
            join_cost = left_rows * right_rows * self.cost_factors.cpu_tuple_cost

        # Total cost includes input costs
        total_cost = left_cost + right_cost + join_cost

        return total_cost, estimated_rows

    def estimate_sort_cost(
        self, input_rows: int, avg_row_width: int, sort_columns: List[str] = None
    ) -> OperatorCost:
        """
        Estimate the cost of a sort operation.

        Args:
            input_rows: Number of input rows
            avg_row_width: Average row width in bytes
            sort_columns: List of columns to sort by

        Returns:
            OperatorCost with detailed breakdown
        """
        # Sort complexity: O(n log n)
        if input_rows <= 1:
            cpu_cost = 0.0
        else:
            cpu_cost = (
                input_rows * math.log2(input_rows) * self.cost_factors.cpu_tuple_cost
            )

        # Memory and I/O cost for external sorting
        total_data_size = input_rows * avg_row_width

        if total_data_size <= self.cost_factors.work_mem:
            # In-memory sort
            io_cost = 0.0
            memory_cost = total_data_size * 0.001  # Small memory management cost
        else:
            # External sort - multiple passes required
            passes = math.ceil(math.log2(total_data_size / self.cost_factors.work_mem))
            io_cost = (
                passes
                * total_data_size
                * 2
                / self.cost_factors.page_size
                * self.cost_factors.seq_page_cost
            )
            memory_cost = self.cost_factors.work_mem * 0.001

        startup_cost = 10.0
        network_cost = 0.0

        total_cost = startup_cost + cpu_cost + io_cost + memory_cost + network_cost

        return OperatorCost(
            total_cost=total_cost,
            startup_cost=startup_cost,
            cpu_cost=cpu_cost,
            io_cost=io_cost,
            memory_cost=memory_cost,
            network_cost=network_cost,
            estimated_rows=input_rows,
            estimated_width=avg_row_width,
            metadata={
                "operator_type": OperatorType.SORT,
                "sort_columns": sort_columns or [],
                "external_sort": total_data_size > self.cost_factors.work_mem,
                "sort_passes": math.ceil(
                    math.log2(max(1, total_data_size / self.cost_factors.work_mem))
                ),
            },
        )

    def estimate_aggregate_cost(
        self,
        input_rows: int,
        avg_row_width: int,
        group_by_columns: List[str] = None,
        aggregate_functions: List[str] = None,
    ) -> OperatorCost:
        """
        Estimate the cost of an aggregation operation.

        Args:
            input_rows: Number of input rows
            avg_row_width: Average row width in bytes
            group_by_columns: List of GROUP BY columns
            aggregate_functions: List of aggregate functions

        Returns:
            OperatorCost with detailed breakdown
        """
        if not group_by_columns:
            # Simple aggregation (no grouping) - single pass
            cpu_cost = input_rows * self.cost_factors.cpu_tuple_cost
            estimated_rows = 1
        else:
            # Hash-based grouping
            # Estimate number of groups (simplified)
            estimated_groups = min(input_rows, max(1, input_rows // 10))
            estimated_rows = estimated_groups

            # Cost includes hashing and aggregation
            cpu_cost = input_rows * self.cost_factors.cpu_tuple_cost * 1.5

            # Memory cost for hash table
            hash_table_size = estimated_groups * 100  # Estimate 100 bytes per group
            if hash_table_size > self.cost_factors.work_mem:
                # Spill to disk if doesn't fit
                io_cost = hash_table_size * 0.1
            else:
                io_cost = 0.0

        startup_cost = 5.0
        memory_cost = min(input_rows * 50, self.cost_factors.work_mem) * 0.001
        network_cost = 0.0

        total_cost = startup_cost + cpu_cost + io_cost + memory_cost + network_cost

        return OperatorCost(
            total_cost=total_cost,
            startup_cost=startup_cost,
            cpu_cost=cpu_cost,
            io_cost=io_cost,
            memory_cost=memory_cost,
            network_cost=network_cost,
            estimated_rows=estimated_rows,
            estimated_width=avg_row_width,
            metadata={
                "operator_type": OperatorType.AGGREGATE,
                "group_by_columns": group_by_columns or [],
                "aggregate_functions": aggregate_functions or [],
                "estimated_groups": estimated_rows,
            },
        )

    def estimate_filter_cost(
        self,
        input_rows: int,
        avg_row_width: int,
        filter_conditions: List[str],
        selectivity: float = 0.33,
    ) -> OperatorCost:
        """
        Estimate the cost of a filter operation.

        Args:
            input_rows: Number of input rows
            avg_row_width: Average row width in bytes
            filter_conditions: List of filter conditions
            selectivity: Estimated selectivity of the filter

        Returns:
            OperatorCost with detailed breakdown
        """
        # CPU cost for evaluating conditions
        condition_cost = len(filter_conditions) * self.cost_factors.cpu_operator_cost
        cpu_cost = input_rows * (self.cost_factors.cpu_tuple_cost + condition_cost)

        # Output rows
        estimated_rows = int(input_rows * selectivity)

        startup_cost = 1.0
        io_cost = 0.0  # Filter doesn't do I/O
        memory_cost = 0.0  # Minimal memory usage
        network_cost = 0.0

        total_cost = startup_cost + cpu_cost + io_cost + memory_cost + network_cost

        return OperatorCost(
            total_cost=total_cost,
            startup_cost=startup_cost,
            cpu_cost=cpu_cost,
            io_cost=io_cost,
            memory_cost=memory_cost,
            network_cost=network_cost,
            estimated_rows=estimated_rows,
            estimated_width=avg_row_width,
            metadata={
                "operator_type": OperatorType.FILTER,
                "filter_conditions": filter_conditions,
                "selectivity": selectivity,
            },
        )

    def estimate_parallel_cost(
        self, base_cost: OperatorCost, parallel_workers: int
    ) -> OperatorCost:
        """
        Estimate the cost of parallel execution.

        Args:
            base_cost: Base cost for sequential execution
            parallel_workers: Number of parallel workers

        Returns:
            OperatorCost adjusted for parallel execution
        """
        if parallel_workers <= 1:
            return base_cost

        # Parallel efficiency (Amdahl's law approximation)
        parallel_fraction = 0.9  # Assume 90% of work can be parallelized
        serial_fraction = 1.0 - parallel_fraction

        # Speedup calculation
        speedup = 1.0 / (serial_fraction + parallel_fraction / parallel_workers)

        # Adjust costs
        cpu_cost = base_cost.cpu_cost / speedup
        io_cost = base_cost.io_cost / min(speedup, 2.0)  # I/O doesn't scale linearly

        # Add parallel coordination overhead
        coordination_cost = (
            parallel_workers
            * self.cost_factors.parallel_tuple_cost
            * base_cost.estimated_rows
        )
        setup_cost = self.cost_factors.parallel_setup_cost

        total_cost = (
            base_cost.startup_cost
            + setup_cost
            + cpu_cost
            + base_cost.io_cost
            + base_cost.memory_cost
            + base_cost.network_cost
            + coordination_cost
        )

        metadata = base_cost.metadata.copy()
        metadata.update(
            {
                "parallel_workers": parallel_workers,
                "parallel_speedup": speedup,
                "coordination_cost": coordination_cost,
            }
        )

        return OperatorCost(
            total_cost=total_cost,
            startup_cost=base_cost.startup_cost + setup_cost,
            cpu_cost=cpu_cost + coordination_cost,
            io_cost=base_cost.io_cost,
            memory_cost=base_cost.memory_cost,
            network_cost=base_cost.network_cost,
            estimated_rows=base_cost.estimated_rows,
            estimated_width=base_cost.estimated_width,
            metadata=metadata,
        )

    def recalibrate_if_needed(self):
        """Recalibrate cost factors if needed."""
        self.cost_factors = self.calibrator.calibrate_cost_factors()

    def clear_cache(self):
        """Clear the cost estimation cache."""
        self.cost_cache.clear()
        self.cache_hits = 0
        self.cache_misses = 0
        logging.info("Cost estimation cache cleared")

    def get_cache_statistics(self) -> Dict[str, Any]:
        """Get cache performance statistics."""
        total_requests = self.cache_hits + self.cache_misses
        hit_rate = self.cache_hits / max(1, total_requests)

        return {
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "hit_rate": hit_rate,
            "cache_size": len(self.cost_cache),
        }

    def explain_cost_breakdown(self, cost: OperatorCost) -> str:
        """Generate a human-readable cost breakdown explanation."""
        lines = [
            f"Operator: {cost.metadata.get('operator_type', 'Unknown')}",
            f"Total Cost: {cost.total_cost:.2f}",
            f"  Startup: {cost.startup_cost:.2f}",
            f"  CPU: {cost.cpu_cost:.2f}",
            f"  I/O: {cost.io_cost:.2f}",
            f"  Memory: {cost.memory_cost:.2f}",
            f"  Network: {cost.network_cost:.2f}",
            f"Estimated Rows: {cost.estimated_rows:,}",
            f"Average Row Width: {cost.estimated_width} bytes",
        ]

        # Add operator-specific details
        if "selectivity" in cost.metadata:
            lines.append(f"Selectivity: {cost.metadata['selectivity']:.3f}")

        if "parallel_workers" in cost.metadata:
            lines.append(f"Parallel Workers: {cost.metadata['parallel_workers']}")
            lines.append(f"Parallel Speedup: {cost.metadata['parallel_speedup']:.2f}x")

        return "\n".join(lines)
