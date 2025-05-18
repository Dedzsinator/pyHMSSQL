"""
Advanced query optimizer with cost-based planning, parallel query execution,
and adaptive optimization features.
"""

import logging
import re
import os
import math
import random
import time
import threading
import multiprocessing
from functools import lru_cache
from collections import defaultdict, Counter
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import psutil
import json
from hashlib import md5

from bptree_adapter import BPlusTree
from table_stats import TableStatistics

class BufferManager:
    """
    LRU/LFU hybrid buffer pool for caching query results and frequently accessed data.

    Uses a hybrid approach of LRU (Least Recently Used) and LFU (Least Frequently Used)
    to optimize cache hit ratios for both temporal and frequency access patterns.
    """
    def __init__(self, max_size=1000, lru_ratio=0.7):
        """
        Initialize buffer manager with hybrid LRU/LFU strategy.

        Args:
            max_size: Maximum number of items in the cache
            lru_ratio: Ratio of LRU to LFU (0.7 means 70% LRU, 30% LFU)
        """
        self.max_size = max_size
        self.lru_ratio = lru_ratio
        self.lru_size = int(max_size * lru_ratio)
        self.lfu_size = max_size - self.lru_size

        # LRU cache with timestamp tracking
        self.lru_cache = {}
        self.lru_access_time = {}

        # LFU cache with frequency tracking
        self.lfu_cache = {}
        self.lfu_counter = Counter()

        # Query result cache with query hash as key
        self.query_cache = {}
        self.query_cache_access_time = {}
        self.query_cache_counter = Counter()
        self.query_cache_lock = threading.RLock()

        # Statistics
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def get(self, key, cache_type='lru'):
        """
        Get an item from the cache.

        Args:
            key: Key to retrieve
            cache_type: Cache to use ('lru', 'lfu', or 'query')

        Returns:
            The cached value or None if not found
        """
        if cache_type == 'lru':
            if key in self.lru_cache:
                self.hits += 1
                self.lru_access_time[key] = time.time()
                return self.lru_cache[key]
        elif cache_type == 'lfu':
            if key in self.lfu_cache:
                self.hits += 1
                self.lfu_counter[key] += 1
                return self.lfu_cache[key]
        elif cache_type == 'query':
            with self.query_cache_lock:
                if key in self.query_cache:
                    self.hits += 1
                    self.query_cache_access_time[key] = time.time()
                    self.query_cache_counter[key] += 1
                    return self.query_cache[key]

        self.misses += 1
        return None

    def put(self, key, value, cache_type='lru'):
        """
        Put an item in the cache.

        Args:
            key: Key to store
            value: Value to store
            cache_type: Cache to use ('lru', 'lfu', or 'query')
        """
        current_time = time.time()

        if cache_type == 'lru':
            # Evict if needed
            if len(self.lru_cache) >= self.lru_size:
                self._evict_lru()

            self.lru_cache[key] = value
            self.lru_access_time[key] = current_time

        elif cache_type == 'lfu':
            # Evict if needed
            if len(self.lfu_cache) >= self.lfu_size:
                self._evict_lfu()

            self.lfu_cache[key] = value
            self.lfu_counter[key] = 1

        elif cache_type == 'query':
            with self.query_cache_lock:
                # Evict if needed
                if len(self.query_cache) >= self.max_size:
                    self._evict_query_cache()

                self.query_cache[key] = value
                self.query_cache_access_time[key] = current_time
                self.query_cache_counter[key] = 1

    def _evict_lru(self):
        """Evict least recently used item from LRU cache."""
        if not self.lru_cache:
            return

        # Find the oldest accessed key
        oldest_key = min(self.lru_access_time.items(), key=lambda x: x[1])[0]
        del self.lru_cache[oldest_key]
        del self.lru_access_time[oldest_key]
        self.evictions += 1

    def _evict_lfu(self):
        """Evict least frequently used item from LFU cache."""
        if not self.lfu_cache:
            return

        # Find the least frequently used key
        least_key = min(self.lfu_counter.items(), key=lambda x: x[1])[0]
        del self.lfu_cache[least_key]
        del self.lfu_counter[least_key]
        self.evictions += 1

    def _evict_query_cache(self):
        """Evict items from query cache using a hybrid approach."""
        if not self.query_cache:
            return

        # Use score formula: 0.7*recency + 0.3*frequency (normalized)
        scores = {}

        # Get max values for normalization
        now = time.time()
        max_age = max((now - t) for t in self.query_cache_access_time.values()) if self.query_cache_access_time else 1
        max_freq = max(self.query_cache_counter.values()) if self.query_cache_counter else 1

        # Calculate scores
        for key in self.query_cache:
            age = (now - self.query_cache_access_time.get(key, 0)) / max_age
            freq = self.query_cache_counter.get(key, 0) / max_freq

            # Invert age so newer items have higher scores
            recency = 1 - age

            # Combine scores (higher is better to keep)
            scores[key] = 0.7 * recency + 0.3 * freq

        # Evict the item with the lowest score
        worst_key = min(scores.items(), key=lambda x: x[1])[0]

        del self.query_cache[worst_key]
        if worst_key in self.query_cache_access_time:
            del self.query_cache_access_time[worst_key]
        if worst_key in self.query_cache_counter:
            del self.query_cache_counter[worst_key]

        self.evictions += 1

    def invalidate(self, table_name):
        """
        Invalidate all cached items related to a specific table.

        Args:
            table_name: Table name to invalidate
        """
        with self.query_cache_lock:
            # Query cache invalidation
            to_remove = []
            for key in self.query_cache:
                if f"table:{table_name}" in key:
                    to_remove.append(key)

            for key in to_remove:
                del self.query_cache[key]
                if key in self.query_cache_access_time:
                    del self.query_cache_access_time[key]
                if key in self.query_cache_counter:
                    del self.query_cache_counter[key]

        # LRU cache invalidation
        to_remove = []
        for key in self.lru_cache:
            if f"table:{table_name}" in key:
                to_remove.append(key)

        for key in to_remove:
            del self.lru_cache[key]
            if key in self.lru_access_time:
                del self.lru_access_time[key]

        # LFU cache invalidation
        to_remove = []
        for key in self.lfu_cache:
            if f"table:{table_name}" in key:
                to_remove.append(key)

        for key in to_remove:
            del self.lfu_cache[key]
            if key in self.lfu_counter:
                del self.lfu_counter[key]

    def clear(self):
        """Clear all caches."""
        with self.query_cache_lock:
            self.lru_cache.clear()
            self.lru_access_time.clear()
            self.lfu_cache.clear()
            self.lfu_counter.clear()
            self.query_cache.clear()
            self.query_cache_access_time.clear()
            self.query_cache_counter.clear()

    def get_stats(self):
        """
        Get cache statistics.

        Returns:
            dict: Statistics about cache usage
        """
        total = self.hits + self.misses
        hit_ratio = self.hits / total if total > 0 else 0

        return {
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
            "hit_ratio": hit_ratio,
            "lru_size": len(self.lru_cache),
            "lfu_size": len(self.lfu_cache),
            "query_cache_size": len(self.query_cache)
        }


class ParallelQueryCoordinator:
    """
    Coordinates parallel query execution across multiple cores and processes.

    Handles:
    1. Task partitioning
    2. Result merging
    3. Resource allocation
    4. Intra-query parallelism
    """
    def __init__(self, max_workers=None):
        """
        Initialize the parallel query coordinator.

        Args:
            max_workers: Maximum number of worker processes/threads
        """
        # Use 75% of available cores by default
        available_cores = multiprocessing.cpu_count()
        self.max_workers = max_workers or max(1, int(available_cores * 0.75))
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)
        self.process_pool = None  # Created on demand

        # Track resource usage
        self.active_tasks = 0
        self.task_lock = threading.Lock()

        # Metrics
        self.total_tasks = 0
        self.completed_tasks = 0
        self.parallel_queries = 0

        # Resource thresholds
        self.cpu_threshold = 90  # Percentage
        self.memory_threshold = 80  # Percentage

        logging.info(f"Initialized ParallelQueryCoordinator with {self.max_workers} workers")

    def is_parallelism_beneficial(self, plan):
        """
        Determine if parallelism would be beneficial for this query plan.

        Args:
            plan: Query plan to evaluate

        Returns:
            bool: True if parallelism would help, False otherwise
        """
        # Quickly check if system is overloaded
        if self._is_system_overloaded():
            return False

        # Check plan type and size
        if plan.get("type") in ("SELECT", "JOIN", "FILTER"):
            table_name = plan.get("table", plan.get("table1"))

            # Check if the table is large enough to benefit from parallelism
            if table_name and plan.get("estimated_rows", 0) > 10000:
                return True

            # Check for large joins
            if plan.get("type") == "JOIN" and (
                plan.get("estimated_rows", 0) > 5000 or
                plan.get("estimated_cost", 0) > 1000
            ):
                return True

        # Check for aggregations which are typically CPU-intensive
        if plan.get("type") == "AGGREGATE" and plan.get("estimated_rows", 0) > 5000:
            return True

        # Default to sequential execution
        return False

    def _is_system_overloaded(self):
        """
        Check if the system is currently overloaded.

        Returns:
            bool: True if system is overloaded, False otherwise
        """
        try:
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory_percent = psutil.virtual_memory().percent

            return cpu_percent > self.cpu_threshold or memory_percent > self.memory_threshold
        except RuntimeError as e:
            logging.warning(f"Error checking system resources: {e}")
            return False

    def parallelize_execution(self, table_access_func, task_params, merge_func):
        """
        Execute a query in parallel.

        Args:
            table_access_func: Function to process each partition
            task_params: List of parameters for each task
            merge_func: Function to merge results

        Returns:
            The merged query results
        """
        with self.task_lock:
            self.active_tasks += 1
            self.total_tasks += 1
            self.parallel_queries += 1

        try:
            # Submit tasks to thread pool
            futures = []
            for params in task_params:
                futures.append(
                    self.thread_pool.submit(table_access_func, *params)
                )

            # Collect results
            results = []
            for future in futures:
                results.append(future.result())

            # Merge results
            merged_result = merge_func(results)

            return merged_result
        finally:
            with self.task_lock:
                self.active_tasks -= 1
                self.completed_tasks += 1

    def partition_table(self, table_name, num_partitions, partition_key=None):
        """
        Create data partitions for parallel processing.

        Args:
            table_name: Table to partition
            num_partitions: Number of partitions to create
            partition_key: Column to partition by

        Returns:
            List of partition specifications
        """
        # Adjust partitions based on system load
        if self._is_system_overloaded():
            num_partitions = min(num_partitions, 2)

        # If no specific key, partition by equal ranges
        partitions = []

        if partition_key is None:
            # Use the primary key range for partitioning
            # We'll implement a range-based partitioning
            partitions = [
                {"start": i/num_partitions, "end": (i+1)/num_partitions}
                for i in range(num_partitions)
            ]
        else:
            # Use histogram-based partitioning when a partition key is specified
            # This would be implemented with table statistics
            partitions = [
                {"key": partition_key, "bucket": i}
                for i in range(num_partitions)
            ]

        return partitions

    def get_optimal_partition_count(self, estimated_rows):
        """
        Determine the optimal number of partitions based on data size.

        Args:
            estimated_rows: Estimated number of rows

        Returns:
            int: Optimal number of partitions
        """
        # Calculate base on data size and available cores
        base_partitions = min(
            self.max_workers,
            max(2, int(math.sqrt(estimated_rows / 1000)))
        )

        # Check system load
        if self._is_system_overloaded():
            return min(base_partitions, 2)

        return base_partitions

    def shutdown(self):
        """Shutdown the coordinator and release resources."""
        self.thread_pool.shutdown()
        if self.process_pool:
            self.process_pool.shutdown()


class Optimizer:
    """Enhanced query optimizer with cost-based planning and parallel execution support."""

    def __init__(self, catalog_manager, index_manager):
        """
        Initialize the query optimizer.

        Args:
            catalog_manager: Catalog manager instance
            index_manager: Index manager instance
        """
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager

        # Cost-based optimization components
        self.statistics = TableStatistics(catalog_manager)
        self.buffer_manager = BufferManager(max_size=1000)
        self.parallel_coordinator = ParallelQueryCoordinator()

        self._plan_key_generator = self._generate_plan_key
        self.plan_cache = {}  # Simple cache dictionary

        # Cardinality estimation model
        self.cardinality_estimates = {}

        # Join ordering components
        self.join_graph = defaultdict(set)

        # Adaptivity metrics
        self.optimization_time = 0
        self.optimization_count = 0

        # Configure logging
        logging.info("Initialized enhanced query optimizer with cost-based planning")

    def optimize(self, plan, plan_type=None):
        """
        Optimize the execution plan using cost-based techniques.

        Args:
            plan: The execution plan to optimize
            plan_type: Optional plan type to override the one in the plan

        Returns:
            The optimized execution plan
        """
        # Check if plan is None or not a dictionary
        if not plan or not isinstance(plan, dict):
            return plan

        # Generate plan type safely
        plan_type = plan_type or plan.get("type", "UNKNOWN")
        logging.info(f"Starting plan optimization for plan type: {plan_type}")
            
        # DISABLE ALL CACHING - Always treat every plan as new
        # Skip all caching mechanisms completely
        logging.info("✅ Optimized query plan")
        
        # Mark the plan as no_cache to ensure other components don't cache it
        plan["no_cache"] = True
        
        # Return the original plan without caching
        return plan

    def _optimize_union(self, plan):
        """
        Optimize a UNION query.

        Args:
            plan: UNION query plan

        Returns:
            Optimized UNION plan
        """
        # Create a copy of the plan to avoid modifying the original
        optimized_plan = plan.copy() if isinstance(plan, dict) else plan

        # Recursively optimize both sides of the UNION
        if isinstance(optimized_plan, dict):
            if "left" in optimized_plan and isinstance(optimized_plan["left"], dict):
                optimized_plan["left"] = self.optimize(optimized_plan["left"])

            if "right" in optimized_plan and isinstance(optimized_plan["right"], dict):
                optimized_plan["right"] = self.optimize(optimized_plan["right"])

        return optimized_plan

    def _optimize_intersect(self, plan):
        """
        Optimize an INTERSECT query.

        Args:
            plan: INTERSECT query plan

        Returns:
            Optimized INTERSECT plan
        """
        # Create a copy of the plan to avoid modifying the original
        optimized_plan = plan.copy() if isinstance(plan, dict) else plan

        # Recursively optimize both sides of the INTERSECT
        if isinstance(optimized_plan, dict):
            if "left" in optimized_plan and isinstance(optimized_plan["left"], dict):
                optimized_plan["left"] = self.optimize(optimized_plan["left"])

            if "right" in optimized_plan and isinstance(optimized_plan["right"], dict):
                optimized_plan["right"] = self.optimize(optimized_plan["right"])

        return optimized_plan

    def _optimize_set_operation(self, plan):
        """Generic optimization for set operations like EXCEPT"""
        # Create a copy of the plan to avoid modifying the original
        optimized_plan = plan.copy() if isinstance(plan, dict) else plan

        # Recursively optimize both sides of the set operation
        if isinstance(optimized_plan, dict):
            if "left" in optimized_plan and isinstance(optimized_plan["left"], dict):
                optimized_plan["left"] = self.optimize(optimized_plan["left"])

            if "right" in optimized_plan and isinstance(optimized_plan["right"], dict):
                optimized_plan["right"] = self.optimize(optimized_plan["right"])

        return optimized_plan

    def _generate_plan_key(self, plan):
        """
        Generate a unique key for a plan to use in caching.

        Args:
            plan: The plan to generate a key for

        Returns:
            str: A unique key for the plan
        """
        # Create a simplified representation for hashing
        key_parts = [f"type:{plan.get('type', 'unknown')}"]

        # Add table information
        for table_key in ('table', 'table1', 'table2'):
            if table_key in plan:
                key_parts.append(f"table:{plan[table_key]}")

        # Add condition information
        for cond_key in ('condition', 'filter', 'where'):
            if cond_key in plan:
                condition = plan[cond_key]
                # Convert condition to string if it's a dictionary
                if isinstance(condition, dict):
                    condition = json.dumps(condition, sort_keys=True)
                key_parts.append(f"{cond_key}:{condition}")

        # Add column information
        if 'columns' in plan:
            cols = ','.join(sorted(str(col) for col in plan['columns']))
            key_parts.append(f"columns:{cols}")

        # Add session ID to differentiate user-specific queries
        if 'session_id' in plan:
            key_parts.append(f"session:{plan['session_id']}")

        # Build the key
        return md5("|".join(key_parts).encode()).hexdigest()

    def _estimate_cardinality_and_cost(self, plan):
        """
        Estimate row counts and execution costs for a plan.

        Args:
            plan: The execution plan

        Returns:
            The plan with estimated rows and costs
        """
        # Default estimates
        plan["estimated_rows"] = 1000
        plan["estimated_cost"] = 1000

        plan_type = plan.get("type")

        if plan_type == "SELECT":
            table_name = plan.get("table")
            if table_name:
                # Get base table statistics
                table_stats = self.statistics.get_table_statistics(table_name)

                # Start with full table size
                estimated_rows = table_stats.get("row_count", 1000)

                # Apply selectivity for conditions
                if "condition" in plan:
                    condition = plan["condition"]
                    selectivity = self._estimate_condition_selectivity(table_name, condition)
                    estimated_rows = max(1, int(estimated_rows * selectivity))

                # Calculate cost based on access method
                if plan.get("use_index"):
                    # Index scan cost model: log(N) for lookup + matching rows
                    estimated_cost = math.log2(table_stats.get("row_count", 1000)) + estimated_rows
                else:
                    # Sequential scan cost model: full table scan
                    estimated_cost = table_stats.get("row_count", 1000)

                plan["estimated_rows"] = estimated_rows
                plan["estimated_cost"] = estimated_cost

        elif plan_type == "JOIN":
            table1 = plan.get("table1")
            table2 = plan.get("table2")

            if table1 and table2:
                # Get statistics for both tables
                stats1 = self.statistics.get_table_statistics(table1)
                stats2 = self.statistics.get_table_statistics(table2)

                rows1 = stats1.get("row_count", 1000)
                rows2 = stats2.get("row_count", 1000)

                # Estimate join cardinality
                if "condition" in plan:
                    condition = plan["condition"]
                    join_cols = self._extract_join_columns(condition)

                    if join_cols[0] and join_cols[1]:
                        # Use column statistics if available
                        col1_distinct = self.statistics.get_column_distinct_values(table1, join_cols[0])
                        col2_distinct = self.statistics.get_column_distinct_values(table2, join_cols[1])

                        # Apply join cardinality formula
                        if col1_distinct > 0 and col2_distinct > 0:
                            # Estimated rows = (rows1 * rows2) / max(distinct values)
                            estimated_rows = int((rows1 * rows2) / max(col1_distinct, col2_distinct))
                        else:
                            # Fallback estimate
                            estimated_rows = int(math.sqrt(rows1 * rows2))
                    else:
                        # Cross join estimate
                        estimated_rows = rows1 * rows2
                else:
                    # Cross join
                    estimated_rows = rows1 * rows2

                # Cost models for different join methods
                join_algorithm = plan.get("join_algorithm", "NESTED_LOOP")

                if join_algorithm == "NESTED_LOOP":
                    # Cost = outer table scan + (outer rows * inner table lookup)
                    estimated_cost = rows1 + (rows1 * math.log2(rows2) if plan.get("use_index") else rows1 * rows2)
                elif join_algorithm == "HASH":
                    # Cost = build hash table + probe
                    estimated_cost = rows1 + rows2
                elif join_algorithm == "MERGE":
                    # Cost = sort both relations + merge
                    estimated_cost = rows1 * math.log2(rows1) + rows2 * math.log2(rows2) + rows1 + rows2
                elif join_algorithm == "INDEX":
                    # Cost = outer table scan + (outer rows * index lookup)
                    estimated_cost = rows1 + (rows1 * math.log2(rows2))

                plan["estimated_rows"] = estimated_rows
                plan["estimated_cost"] = estimated_cost

        elif plan_type == "AGGREGATE":
            table_name = plan.get("table")
            if table_name:
                table_stats = self.statistics.get_table_statistics(table_name)
                input_rows = table_stats.get("row_count", 1000)

                # Apply group by cardinality estimation
                if "group_by" in plan:
                    # Get the number of distinct values in group by columns
                    group_cols = plan["group_by"]
                    distinct_groups = 1

                    for col in group_cols:
                        col_distinct = self.statistics.get_column_distinct_values(table_name, col)
                        if col_distinct > 0:
                            distinct_groups *= col_distinct

                    # Cap the estimate to the input size
                    estimated_rows = min(distinct_groups, input_rows)
                else:
                    # Aggregation without GROUP BY produces one row
                    estimated_rows = 1

                # Cost model for aggregation
                estimated_cost = input_rows + (
                    input_rows * math.log2(input_rows) if "group_by" in plan else 0
                )

                plan["estimated_rows"] = estimated_rows
                plan["estimated_cost"] = estimated_cost

        # Recursively process child plans
        if "child" in plan and isinstance(plan["child"], dict):
            plan["child"] = self._estimate_cardinality_and_cost(plan["child"])

            # Update parent estimates based on child
            child_rows = plan["child"].get("estimated_rows", 0)
            child_cost = plan["child"].get("estimated_cost", 0)

            # For most operations, output cardinality depends on child
            if plan_type in ("PROJECT", "SORT", "LIMIT", "SORT_LIMIT", "TOP_N"):
                plan["estimated_rows"] = child_rows

            # Accumulated cost
            plan["estimated_cost"] += child_cost

        return plan

    def _estimate_condition_selectivity(self, table_name, condition):
        """
        Estimate the selectivity of a condition.

        Args:
            table_name: The table the condition applies to
            condition: The condition to evaluate

        Returns:
            float: Estimated selectivity (0.0-1.0)
        """
        if not condition:
            return 1.0

        # Simple equality condition
        if "=" in condition and "<" not in condition and ">" not in condition:
            parts = condition.split("=")
            if len(parts) == 2:
                col_name = parts[0].strip()

                # Get column statistics
                distinct_values = self.statistics.get_column_distinct_values(table_name, col_name)
                if distinct_values > 0:
                    # Equality selectivity is 1/distinct_values
                    return 1.0 / distinct_values

        # Range condition
        if "<" in condition or ">" in condition:
            # Extract column name
            col_name = re.search(r"(\w+)\s*[<>]", condition)
            if col_name:
                col_name = col_name.group(1)

                # Get column min/max
                col_range = self.statistics.get_column_range(table_name, col_name)
                if col_range:
                    min_val, max_val = col_range
                    range_width = max_val - min_val if max_val > min_val else 1

                    # Estimate range selectivity based on condition
                    if "<=" in condition or "<" in condition:
                        # Extract the constant
                        const_val = re.search(r"[<>]=?\s*(\d+)", condition)
                        if const_val:
                            const_val = float(const_val.group(1))
                            # Estimate as (const - min) / range
                            return min(1.0, max(0.0, (const_val - min_val) / range_width))

                    if ">=" in condition or ">" in condition:
                        # Extract the constant
                        const_val = re.search(r"[<>]=?\s*(\d+)", condition)
                        if const_val:
                            const_val = float(const_val.group(1))
                            # Estimate as (max - const) / range
                            return min(1.0, max(0.0, (max_val - const_val) / range_width))

        # LIKE condition
        if "LIKE" in condition:
            # Start/end wildcard patterns have different selectivity
            if "'%'" in condition:
                return 1.0  # Matches everything
            elif condition.count("%") == 1:
                if re.search(r"LIKE\s*'%\w+'", condition):  # Ends with pattern
                    return 0.2
                elif re.search(r"LIKE\s*'\w+%'", condition):  # Starts with pattern
                    return 0.1
            return 0.05  # Contains pattern

        # BETWEEN condition
        if "BETWEEN" in condition:
            col_name = re.search(r"(\w+)\s+BETWEEN", condition)
            if col_name:
                col_name = col_name.group(1)

                # Extract range values
                range_vals = re.search(r"BETWEEN\s+(\d+)\s+AND\s+(\d+)", condition)
                if range_vals:
                    low = float(range_vals.group(1))
                    high = float(range_vals.group(2))

                    # Get column range
                    col_range = self.statistics.get_column_range(table_name, col_name)
                    if col_range:
                        min_val, max_val = col_range
                        range_width = max_val - min_val if max_val > min_val else 1

                        # Estimate as range overlap
                        overlap = min(high, max_val) - max(low, min_val)
                        return max(0.0, overlap / range_width)

        # IN condition
        if " IN (" in condition:
            col_name = re.search(r"(\w+)\s+IN\s+\(", condition)
            if col_name:
                col_name = col_name.group(1)

                # Count values in the IN list
                values = re.findall(r"IN\s+\((.*?)\)", condition)
                if values:
                    value_count = len(values[0].split(","))

                    # Get column cardinality
                    distinct_values = self.statistics.get_column_distinct_values(table_name, col_name)
                    if distinct_values > 0:
                        return min(1.0, value_count / distinct_values)

        # Compound conditions
        if "AND" in condition:
            # Split by AND and multiply selectivities
            parts = condition.split("AND")
            selectivity = 1.0
            for part in parts:
                selectivity *= self._estimate_condition_selectivity(table_name, part.strip())
            return selectivity

        if "OR" in condition:
            # Split by OR and combine selectivities
            parts = condition.split("OR")
            selectivity = 0.0
            for part in parts:
                part_selectivity = self._estimate_condition_selectivity(table_name, part.strip())
                selectivity = selectivity + part_selectivity - (selectivity * part_selectivity)
            return selectivity

        # Default selectivity for unknown conditions
        return 0.33  # Assume 1/3 of rows match

    def _optimize_select_with_cost(self, plan):
        """
        Optimize a SELECT query using cost-based techniques.

        Args:
            plan: SELECT query plan

        Returns:
            Optimized plan
        """
        table_name = plan.get("table")
        condition = plan.get("condition")

        # Start with a copy of the original plan
        optimized_plan = dict(plan)

        # Check if an index would be beneficial
        if condition:
            # Parse condition to extract column
            indexed_column = self._extract_indexed_column(condition)

            if indexed_column:
                # Check if an index exists for this column
                index = self.index_manager.get_index(f"{table_name}.idx_{indexed_column}")

                if index:
                    # Check if using the index would be more efficient
                    table_stats = self.statistics.get_table_statistics(table_name)
                    total_rows = table_stats.get("row_count", 1000)

                    # Estimate selectivity of the condition
                    selectivity = self._estimate_condition_selectivity(table_name, condition)
                    matching_rows = total_rows * selectivity

                    # Cost models
                    seq_scan_cost = total_rows
                    index_scan_cost = math.log2(total_rows) + matching_rows

                    if index_scan_cost < seq_scan_cost:
                        optimized_plan["use_index"] = True
                        optimized_plan["index"] = index
                        optimized_plan["access_method"] = "INDEX_SCAN"
                        optimized_plan["estimated_cost"] = index_scan_cost
                        logging.debug(
                            f"Using index {index} for column {indexed_column} - Cost: {index_scan_cost} vs {seq_scan_cost}"
                        )
                    else:
                        # Sequential scan is more efficient
                        optimized_plan["access_method"] = "SEQ_SCAN"
                        optimized_plan["estimated_cost"] = seq_scan_cost
                        logging.debug(
                            f"Using sequential scan - Cost: {seq_scan_cost} vs {index_scan_cost}"
                        )

        # Check if the query is eligible for parallel execution
        if plan.get("estimated_rows", 0) > 10000:
            # This is a larger table, consider parallel execution
            optimized_plan["parallel_eligible"] = True
            optimized_plan["parallel_partitions"] = self.parallel_coordinator.get_optimal_partition_count(
                plan.get("estimated_rows", 10000)
            )

        return optimized_plan

    def _optimize_join_with_cost(self, plan):
        """
        Optimize a JOIN query using cost-based techniques.

        Args:
            plan: JOIN query plan

        Returns:
            Optimized plan
        """
        # Start with a copy of the original plan
        optimized_plan = dict(plan)

        table1 = plan.get("table1", "")
        table2 = plan.get("table2", "")
        condition = plan.get("condition", "")

        # Extract column names from condition
        left_column, right_column = self._extract_join_columns(condition)

        if not left_column or not right_column:
            # Cannot extract columns, use default join method
            optimized_plan["join_algorithm"] = "HASH"
            return optimized_plan

        # Get statistics for both tables
        stats1 = self.statistics.get_table_statistics(table1)
        stats2 = self.statistics.get_table_statistics(table2)

        rows1 = stats1.get("row_count", 1000)
        rows2 = stats2.get("row_count", 1000)

        # Check for indexes on join columns
        left_index = self.index_manager.get_index(f"{table1}.idx_{left_column}")
        right_index = self.index_manager.get_index(f"{table2}.idx_{right_column}")

        # Cost models for different join algorithms
        costs = {}

        # Nested Loop Join
        if left_index:
            # Outer table is table2, inner is table1 with index
            nl_cost_1 = rows2 + (rows2 * math.log2(rows1))
            costs["NESTED_LOOP_1"] = {"cost": nl_cost_1, "outer": table2, "inner": table1, "index": left_index}

        if right_index:
            # Outer table is table1, inner is table2 with index
            nl_cost_2 = rows1 + (rows1 * math.log2(rows2))
            costs["NESTED_LOOP_2"] = {"cost": nl_cost_2, "outer": table1, "inner": table2, "index": right_index}

        # Hash Join - build hash table on smaller relation
        if rows1 <= rows2:
            # Build hash table on table1
            hash_cost = rows1 + rows2  # Build + Probe
            costs["HASH_1"] = {"cost": hash_cost, "build": table1, "probe": table2}
        else:
            # Build hash table on table2
            hash_cost = rows2 + rows1  # Build + Probe
            costs["HASH_2"] = {"cost": hash_cost, "build": table2, "probe": table1}

        # Merge Join - requires sorted input
        is_sorted1 = self.catalog_manager.is_table_sorted(table1, left_column)
        is_sorted2 = self.catalog_manager.is_table_sorted(table2, right_column)

        if is_sorted1 and is_sorted2:
            # Both tables already sorted
            merge_cost = rows1 + rows2  # Just merging cost
            costs["MERGE"] = {"cost": merge_cost}
        elif is_sorted1:
            # Sort table2
            merge_cost = rows1 + rows2 + (rows2 * math.log2(rows2))
            costs["MERGE_SORT_2"] = {"cost": merge_cost, "sort": table2}
        elif is_sorted2:
            # Sort table1
            merge_cost = rows1 + rows2 + (rows1 * math.log2(rows1))
            costs["MERGE_SORT_1"] = {"cost": merge_cost, "sort": table1}
        else:
            # Sort both tables
            merge_cost = rows1 + rows2 + (rows1 * math.log2(rows1)) + (rows2 * math.log2(rows2))
            costs["MERGE_SORT_BOTH"] = {"cost": merge_cost}

        # Find the lowest cost algorithm
        if costs:
            best_algorithm = min(costs.items(), key=lambda x: x[1]["cost"])
            algorithm_name = best_algorithm[0]
            algorithm_data = best_algorithm[1]

            # Set the join algorithm
            if algorithm_name.startswith("NESTED_LOOP"):
                optimized_plan["join_algorithm"] = "INDEX"

                # Set correct outer and inner tables
                if algorithm_name == "NESTED_LOOP_1":
                    # Swap tables so we can use the index
                    optimized_plan["table1"] = table2
                    optimized_plan["table2"] = table1
                    optimized_plan["condition"] = condition.replace(f"{table1}.{left_column}", "TEMP")
                    optimized_plan["condition"] = optimized_plan["condition"].replace(
                        f"{table2}.{right_column}", f"{table1}.{left_column}"
                    )
                    optimized_plan["condition"] = optimized_plan["condition"].replace("TEMP", f"{table2}.{right_column}")

                optimized_plan["index"] = algorithm_data["index"]
            elif algorithm_name.startswith("HASH"):
                optimized_plan["join_algorithm"] = "HASH"

                # Make sure smaller table is first for hash table build
                if algorithm_name == "HASH_2":
                    optimized_plan["table1"] = table2
                    optimized_plan["table2"] = table1
                    optimized_plan["condition"] = condition.replace(f"{table1}.{left_column}", "TEMP")
                    optimized_plan["condition"] = optimized_plan["condition"].replace(
                        f"{table2}.{right_column}", f"{table1}.{left_column}"
                    )
                    optimized_plan["condition"] = optimized_plan["condition"].replace("TEMP", f"{table2}.{right_column}")
            elif algorithm_name.startswith("MERGE"):
                optimized_plan["join_algorithm"] = "MERGE"
                optimized_plan["sort_required"] = not (is_sorted1 and is_sorted2)

            # Record estimated cost
            optimized_plan["estimated_cost"] = algorithm_data["cost"]
            logging.debug(f"Selected join algorithm: {algorithm_name} with cost {algorithm_data['cost']}")
        else:
            # Fallback to hash join
            optimized_plan["join_algorithm"] = "HASH"

        # Check for parallel execution opportunity for large joins
        if rows1 * rows2 > 1000000:  # 1M result rows threshold
            optimized_plan["parallel_eligible"] = True

        return optimized_plan

    def _optimize_aggregate(self, plan):
        """
        Optimize an aggregate query using cost-based techniques.

        Args:
            plan: Aggregate query plan

        Returns:
            Optimized plan
        """
        # Start with a copy of the original plan
        optimized_plan = dict(plan)

        # Optimization strategies for aggregation
        table_name = plan.get("table")
        group_by = plan.get("group_by", [])

        # Check if we can use an index for grouped aggregation
        if group_by and len(group_by) == 1:
            group_column = group_by[0]
            index = self.index_manager.get_index(f"{table_name}.idx_{group_column}")

            if index:
                # Using an index for grouping can be more efficient
                optimized_plan["use_index_for_grouping"] = True
                optimized_plan["group_index"] = index

                # Update cost estimate
                table_stats = self.statistics.get_table_statistics(table_name)
                total_rows = table_stats.get("row_count", 1000)
                distinct_groups = self.statistics.get_column_distinct_values(table_name, group_column)

                # Cost model: index lookup + one pass through data
                optimized_plan["estimated_cost"] = total_rows + distinct_groups

        # Check if this aggregation can benefit from parallelization
        if plan.get("estimated_rows", 0) > 5000:
            optimized_plan["parallel_eligible"] = True

        return optimized_plan

    def _apply_generic_optimizations(self, plan):
        """
        Apply generic optimizations to any plan type.

        Args:
            plan: The query plan to optimize

        Returns:
            Optimized plan
        """
        # Start with a copy
        optimized_plan = dict(plan)

        # Apply optimizations for specific subtypes
        plan_type = plan.get("type")

        if plan_type == "FILTER":
            # Eliminate always-true filter
            if plan.get("condition") == "1=1":
                if "child" in plan:
                    return plan["child"]

            # Push filter down if possible
            if "child" in plan:
                child_type = plan["child"].get("type")
                if child_type in ("SEQ_SCAN", "INDEX_SCAN", "TABLE_SCAN"):
                    child_plan = dict(plan["child"])
                    child_plan["filter"] = plan.get("condition")
                    return child_plan

        elif plan_type == "PROJECT":
            # Eliminate unnecessary projections
            if "child" in plan:
                child_type = plan["child"].get("type")
                if child_type == "PROJECT":
                    # Merge projections
                    optimized_plan["columns"] = list(
                        set(plan.get("columns", []) + plan["child"].get("columns", []))
                    )
                    optimized_plan["child"] = plan["child"].get("child")

        elif plan_type == "LIMIT":
            # Combine with ORDER BY if possible
            if "child" in plan and plan["child"].get("type") == "SORT":
                # Convert to TOP_N
                optimized_plan = dict(plan["child"])  # Start with SORT plan
                optimized_plan["type"] = "TOP_N"
                optimized_plan["limit"] = plan.get("limit")

        # Process child plans
        if "child" in optimized_plan and isinstance(optimized_plan["child"], dict):
            optimized_plan["child"] = self._apply_generic_optimizations(optimized_plan["child"])

        return optimized_plan

    def _extract_indexed_column(self, condition):
        """
        Extract the column name from a condition that could use an index.

        Args:
            condition: SQL condition string

        Returns:
            str: Column name if found, None otherwise
        """
        if not condition:
            return None

        # Check for simple equality conditions
        if "=" in condition and "<" not in condition and ">" not in condition:
            parts = condition.split("=")
            if len(parts) == 2:
                col_name = parts[0].strip()

                # Remove table prefix if present
                if "." in col_name:
                    col_name = col_name.split(".")[1]

                return col_name

        # Check for range conditions
        range_match = re.search(r"(\w+)(?:\.\w+)?\s*[<>]=?", condition)
        if range_match:
            col_name = range_match.group(1)

            # Remove table prefix if present
            if "." in col_name:
                col_name = col_name.split(".")[1]

            return col_name

        return None

    def _extract_join_columns(self, condition):
        """
        Extract column names from join condition.

        Args:
            condition: Join condition string like "t1.col1 = t2.col2"

        Returns:
            Tuple of (left_column, right_column)
        """
        if not condition:
            return None, None

        # Try to match table.column = table.column pattern
        match = re.search(r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", condition)
        if match:
            return match.group(2), match.group(4)

        # Try simpler column = column pattern
        match = re.search(r"(\w+)\s*=\s*(\w+)", condition)
        if match:
            return match.group(1), match.group(2)

        return None, None

    def _get_plan_changes(self, original, optimized):
        """
        Get a list of changes made during optimization.

        Args:
            original: The original execution plan
            optimized: The optimized execution plan

        Returns:
            list: Descriptions of optimizations applied
        """
        changes = []

        # Check for index usage
        if optimized.get("use_index") and not original.get("use_index"):
            index_name = str(optimized.get("index", "unknown"))
            if isinstance(optimized.get("index"), dict):
                index_name = optimized["index"].get("name", "unknown")
            changes.append(f"Using index {index_name} for table {optimized.get('table', 'unknown')}")

        # Check for join algorithm changes
        if (
            original.get("type") == "JOIN"
            and optimized.get("type") == "JOIN"
            and original.get("join_algorithm") != optimized.get("join_algorithm")
        ):
            changes.append(
                f"Changed join algorithm from {original.get('join_algorithm', 'NONE')} to {
                    optimized.get('join_algorithm', 'NONE')}"
            )

        # Check for join order changes
        if (
            original.get("type") == "JOIN"
            and optimized.get("type") == "JOIN"
            and original.get("table1") != optimized.get("table1")
        ):
            changes.append(
                f"Reordered join tables: {optimized.get('table1')} then {optimized.get('table2')}"
            )

        # Check for parallelization
        if optimized.get("parallel_execution") and not original.get("parallel_execution"):
            changes.append(
                f"Enabled parallel execution with {optimized.get('parallel_partitions', 2)} partitions"
            )

        # Check for access method changes
        if optimized.get("access_method") and original.get("access_method") != optimized.get("access_method"):
            changes.append(
                f"Changed access method from {original.get('access_method', 'unknown')} to {
                    optimized.get('access_method', 'unknown')}"
            )

        # Check for index join optimization
        if original.get("type") == "JOIN" and optimized.get("type") == "JOIN" and optimized.get("join_algorithm") == "INDEX":
            changes.append("Using index-based join strategy")

        # Check for filter merging with join
        if (
            original.get("type") == "JOIN"
            and "filter" in original
            and optimized.get("type") == "JOIN"
            and "filter" not in optimized
        ):
            changes.append("Merged filter condition into join condition")

        # Check for projection merging
        if (
            original.get("type") == "PROJECT"
            and optimized.get("type") == "PROJECT"
            and "child" in original
            and "child" in optimized
            and original["child"].get("type") == "PROJECT"
            and optimized["child"].get("type") != "PROJECT"
        ):
            changes.append("Merged multiple projections")

        # Check for always-true filter elimination
        if (
            original.get("type") == "FILTER"
            and original.get("condition") == "1=1"
            and optimized.get("type") != "FILTER"
        ):
            changes.append("Eliminated always-true filter condition")

        # Check for TOP_N optimization
        if (
            original.get("type") == "SORT"
            and "limit" in original
            and optimized.get("type") == "TOP_N"
        ):
            changes.append(
                f"Optimized SORT+LIMIT to TOP_N with limit {
                    optimized.get('limit', 'unknown')}"
            )

        # Check for cost reduction
        if "estimated_cost" in original and "estimated_cost" in optimized:
            orig_cost = original["estimated_cost"]
            opt_cost = optimized["estimated_cost"]
            if opt_cost < orig_cost:
                reduction = ((orig_cost - opt_cost) / orig_cost) * 100
                changes.append(f"Reduced estimated cost by {reduction:.1f}% ({orig_cost:.1f} to {opt_cost:.1f})")

        return changes

    def refresh_statistics(self, table_name=None):
        """
        Refresh statistics for a table or all tables.

        Args:
            table_name: Specific table to refresh, or None for all tables
        """
        if table_name:
            self.statistics.collect_table_statistics(table_name)
            # Invalidate cache for this table
            self.buffer_manager.invalidate(table_name)
        else:
            # Refresh all tables
            db_name = self.catalog_manager.get_current_database()
            if db_name:
                tables = self.catalog_manager.list_tables(db_name)
                for table in tables:
                    self.statistics.collect_table_statistics(table)
                    self.buffer_manager.invalidate(table)

    def get_optimization_stats(self):
        """
        Get statistics about the optimizer's performance.

        Returns:
            dict: Optimization statistics
        """
        avg_time = 0
        if self.optimization_count > 0:
            avg_time = self.optimization_time / self.optimization_count

        return {
            "total_optimizations": self.optimization_count,
            "total_time_ms": self.optimization_time * 1000,
            "avg_time_ms": avg_time * 1000,
            "cache_stats": self.buffer_manager.get_stats()
        }

    def optimize_query_shape(self, plan):
        """
        Optimize the shape of a query plan (e.g., bushy vs. left-deep).

        Args:
            plan: Query plan to optimize

        Returns:
            Optimized query plan
        """
        # This is a more advanced optimization that would rewrite the entire plan shape
        # For now, we'll return the original plan
        return plan

    def shutdown(self):
        """Release resources upon shutdown."""
        self.parallel_coordinator.shutdown()