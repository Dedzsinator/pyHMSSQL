"""
Table statistics collection and management module for cost-based query optimization.
"""

import os
import json
import logging
import time
import math
import threading
import numpy as np
from collections import defaultdict

class TableStatistics:
    """
    Manages statistics about tables and columns for cost-based query optimization.
    Uses sampling and histogram-based approaches for accuracy and efficiency.
    """
    def __init__(self, catalog_manager, stats_dir="data/statistics"):
        """
        Initialize with a reference to the catalog manager.

        Args:
            catalog_manager: Catalog manager for accessing tables
            stats_dir: Directory to store statistics files
        """
        self.catalog_manager = catalog_manager
        self.stats_dir = stats_dir
        self.statistics = {}  # Cache of table statistics
        self.histograms = {}  # Cache of column histograms
        self.sample_size = 1000  # Default sample size for large tables
        self.min_distinct_values = 10  # Threshold for histogram creation
        self.refresh_interval = 3600  # Refresh stats every hour
        self.last_refresh = {}  # Track when stats were last refreshed
        self.stats_lock = threading.RLock()  # Thread safety

        # Ensure stats directory exists
        os.makedirs(self.stats_dir, exist_ok=True)

        # Load existing statistics
        self._load_all_statistics()

        logging.info(f"Initialized TableStatistics with stats directory: {stats_dir}")

    def _load_all_statistics(self):
        """Load all existing statistics files."""
        if not os.path.exists(self.stats_dir):
            return

        with self.stats_lock:
            try:
                files = os.listdir(self.stats_dir)
                loaded_count = 0

                for filename in files:
                    if filename.endswith(".stats.json"):
                        # Extract database and table names from filename
                        parts = filename.replace(".stats.json", "").split("_")
                        if len(parts) >= 2:
                            db_name = parts[0]
                            table_name = "_".join(parts[1:])

                            # Load the statistics
                            filepath = os.path.join(self.stats_dir, filename)
                            try:
                                with open(filepath, 'r') as f:
                                    stats_data = json.load(f)

                                self.statistics[f"{db_name}.{table_name}"] = stats_data
                                self.last_refresh[f"{db_name}.{table_name}"] = stats_data.get("timestamp", 0)
                                loaded_count += 1

                                # Load histograms if they exist
                                hist_file = os.path.join(self.stats_dir, f"{db_name}_{table_name}.hist.json")
                                if os.path.exists(hist_file):
                                    with open(hist_file, 'r') as f:
                                        self.histograms[f"{db_name}.{table_name}"] = json.load(f)
                            except RuntimeError as e:
                                logging.error(f"Error loading statistics for {db_name}.{table_name}: {e}")

                logging.info(f"Loaded statistics for {loaded_count} tables")
            except RuntimeError as e:
                logging.error(f"Error loading statistics: {e}")

    def invalidate_stats(self):
        """Invalidate all cached statistics."""
        with self.stats_lock:
            self.statistics.clear()
            self.histograms.clear()
            self.last_refresh.clear()
            logging.info("Invalidated all cached statistics")


    def get_table_statistics(self, table_name):
        """
        Get statistics for a table. If statistics are not available or outdated,
        collect them first.

        Args:
            table_name: Name of the table

        Returns:
            dict: Table statistics including row count, size, etc.
        """
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"row_count": 1000}  # Default fallback

        table_key = f"{db_name}.{table_name}"

        with self.stats_lock:
            # Check if we need to refresh the statistics
            current_time = time.time()
            if (
                table_key not in self.statistics or
                table_key not in self.last_refresh or
                current_time - self.last_refresh[table_key] > self.refresh_interval
            ):
                # Statistics are missing or outdated, collect them
                self.collect_table_statistics(table_name)

            # Return the statistics (or defaults if collection failed)
            return self.statistics.get(
                table_key,
                {"row_count": 1000, "avg_row_size": 100, "approx_size_kb": 100}
            )

    def collect_table_statistics(self, table_name):
        """
        Collect statistics for a table and its columns.

        Args:
            table_name: Name of the table to analyze

        Returns:
            bool: True if statistics were successfully collected
        """
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return False

        table_key = f"{db_name}.{table_name}"

        with self.stats_lock:
            try:
                # Get table schema to identify columns
                schema = self.catalog_manager.get_table_schema(table_name)
                if not schema:
                    return False

                # Extract column names from schema
                columns = []
                for column in schema:
                    if isinstance(column, dict):
                        columns.append(column.get("name"))
                    elif isinstance(column, str):
                        # Handle string column definitions
                        col_name = column.split()[0]
                        columns.append(col_name)

                # Query the table to get all records
                # For very large tables, we might want to use sampling instead
                all_records = self.catalog_manager.query_with_condition(table_name, None, columns)

                # Count rows
                row_count = len(all_records)

                # Initialize statistics dictionary
                stats = {
                    "row_count": row_count,
                    "timestamp": time.time(),
                    "columns": {}
                }

                # If table is empty, save minimal statistics
                if row_count == 0:
                    self.statistics[table_key] = stats
                    self._save_statistics(db_name, table_name, stats)
                    return True

                # Calculate average row size
                sample_size = min(row_count, self.sample_size)
                total_size = 0

                for i in range(sample_size):
                    record = all_records[i]
                    # Estimate row size (this is a rough approximation)
                    record_size = sum(
                        len(str(value)) for value in record.values()
                    )
                    total_size += record_size

                avg_row_size = total_size / sample_size
                approx_size_kb = (row_count * avg_row_size) / 1024

                stats["avg_row_size"] = avg_row_size
                stats["approx_size_kb"] = approx_size_kb

                # Collect column-level statistics
                column_stats = {}
                histograms = {}

                for column in columns:
                    if column not in all_records[0]:
                        continue

                    # Extract values for this column
                    values = [record.get(column) for record in all_records]

                    # Remove None values
                    values = [v for v in values if v is not None]

                    if not values:
                        continue

                    # Determine column type
                    sample_value = values[0]
                    col_type = "string"

                    if isinstance(sample_value, (int, float)):
                        col_type = "numeric"

                        # Calculate min/max/avg for numeric columns
                        min_val = min(values)
                        max_val = max(values)
                        avg_val = sum(values) / len(values)

                        column_stats[column] = {
                            "type": col_type,
                            "min": min_val,
                            "max": max_val,
                            "avg": avg_val,
                            "null_count": row_count - len(values)
                        }

                        # Create histogram for numeric columns
                        if len(set(values)) > self.min_distinct_values:
                            try:
                                hist, bin_edges = np.histogram(values, bins='auto')
                                histograms[column] = {
                                    "counts": hist.tolist(),
                                    "bins": bin_edges.tolist()
                                }
                            except RuntimeError as e:
                                logging.error(f"Error creating histogram for {column}: {e}")
                    else:
                        # For string columns, collect distinct values and top values
                        distinct_values = set(values)

                        # Count frequencies
                        freq = defaultdict(int)
                        for v in values:
                            freq[v] += 1

                        # Get top 10 most common values
                        top_values = sorted(freq.items(), key=lambda x: x[1], reverse=True)[:10]

                        column_stats[column] = {
                            "type": col_type,
                            "distinct_count": len(distinct_values),
                            "null_count": row_count - len(values),
                            "top_values": [{"value": str(v), "count": c} for v, c in top_values]
                        }

                        # For low-cardinality string columns, create a value distribution
                        if len(distinct_values) <= 100:  # Only for reasonable number of distinct values
                            value_dist = {str(k): v for k, v in freq.items()}
                            histograms[column] = {
                                "type": "categorical",
                                "distribution": value_dist
                            }

                # Update the statistics
                stats["columns"] = column_stats
                self.statistics[table_key] = stats
                self.histograms[table_key] = histograms
                self.last_refresh[table_key] = time.time()

                # Save statistics to disk
                self._save_statistics(db_name, table_name, stats)
                self._save_histograms(db_name, table_name, histograms)

                logging.info(f"Collected statistics for {table_name}: {row_count} rows")
                return True
            except RuntimeError as e:
                logging.error(f"Error collecting statistics for {table_name}: {e}")
                return False

    def _save_statistics(self, db_name, table_name, stats):
        """Save statistics to a file."""
        filename = f"{db_name}_{table_name}.stats.json"
        filepath = os.path.join(self.stats_dir, filename)

        try:
            with open(filepath, 'w') as f:
                json.dump(stats, f, indent=2)
        except RuntimeError as e:
            logging.error(f"Error saving statistics for {table_name}: {e}")

    def _save_histograms(self, db_name, table_name, histograms):
        """Save histograms to a file."""
        filename = f"{db_name}_{table_name}.hist.json"
        filepath = os.path.join(self.stats_dir, filename)

        try:
            with open(filepath, 'w') as f:
                json.dump(histograms, f, indent=2)
        except RuntimeError as e:
            logging.error(f"Error saving histograms for {table_name}: {e}")

    def get_column_statistics(self, table_name, column_name):
        """
        Get statistics for a specific column.

        Args:
            table_name: Table name
            column_name: Column name

        Returns:
            dict: Column statistics
        """
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {}

        table_key = f"{db_name}.{table_name}"

        with self.stats_lock:
            # Ensure we have the statistics
            if table_key not in self.statistics:
                self.collect_table_statistics(table_name)

            # Return column statistics if available
            if table_key in self.statistics:
                return self.statistics[table_key].get("columns", {}).get(column_name, {})

            return {}

    def get_column_histogram(self, table_name, column_name):
        """
        Get histogram for a specific column.

        Args:
            table_name: Table name
            column_name: Column name

        Returns:
            dict: Histogram data
        """
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {}

        table_key = f"{db_name}.{table_name}"

        with self.stats_lock:
            # Ensure we have the histograms
            if table_key not in self.histograms:
                self.collect_table_statistics(table_name)

            # Return histogram if available
            if table_key in self.histograms:
                return self.histograms[table_key].get(column_name, {})

            return {}

    def get_column_distinct_values(self, table_name, column_name):
        """
        Get the number of distinct values for a column.

        Args:
            table_name: Table name
            column_name: Column name

        Returns:
            int: Number of distinct values
        """
        column_stats = self.get_column_statistics(table_name, column_name)

        if "distinct_count" in column_stats:
            return column_stats["distinct_count"]

        # Try to infer from histogram
        histogram = self.get_column_histogram(table_name, column_name)

        if histogram:
            if histogram.get("type") == "categorical":
                return len(histogram.get("distribution", {}))
            else:
                # For numeric histograms, use number of bins as rough approximation
                return len(histogram.get("counts", []))

        # Default fallback based on table size
        table_stats = self.get_table_statistics(table_name)
        row_count = table_stats.get("row_count", 1000)

        # Heuristic: distinct values are typically around sqrt(N) for good selectivity columns
        return max(10, int(math.sqrt(row_count)))

    def get_column_range(self, table_name, column_name):
        """
        Get the range (min, max) for a numeric column.

        Args:
            table_name: Table name
            column_name: Column name

        Returns:
            tuple: (min_value, max_value) or None if not numeric or not available
        """
        column_stats = self.get_column_statistics(table_name, column_name)

        if column_stats.get("type") == "numeric":
            min_val = column_stats.get("min")
            max_val = column_stats.get("max")

            if min_val is not None and max_val is not None:
                return (min_val, max_val)

        return None

    def estimate_join_cardinality(self, table1, table2, column1, column2):
        """
        Estimate the cardinality of a join operation.

        Args:
            table1: First table name
            table2: Second table name
            column1: Join column from first table
            column2: Join column from second table

        Returns:
            int: Estimated number of rows in join result
        """
        # Get table sizes
        stats1 = self.get_table_statistics(table1)
        stats2 = self.get_table_statistics(table2)

        rows1 = stats1.get("row_count", 1000)
        rows2 = stats2.get("row_count", 1000)

        # Get column distinct values
        distinct1 = self.get_column_distinct_values(table1, column1)
        distinct2 = self.get_column_distinct_values(table2, column2)

        # Apply standard join size estimation formula
        if distinct1 > 0 and distinct2 > 0:
            selectivity = 1 / max(distinct1, distinct2)
            return int(rows1 * rows2 * selectivity)
        else:
            # Fallback estimate
            return int(min(rows1 * rows2, rows1 + rows2))

    def estimate_filter_cardinality(self, table_name, condition):
        """
        Estimate the cardinality after applying a filter condition.

        Args:
            table_name: Table name
            condition: Filter condition

        Returns:
            int: Estimated number of rows after filter
        """
        table_stats = self.get_table_statistics(table_name)
        row_count = table_stats.get("row_count", 1000)

        # Default selectivity is 0.1 (assumes 10% of rows match)
        selectivity = 0.1

        # TODO: Implement more sophisticated condition analysis
        # This would parse the condition and use column statistics

        return max(1, int(row_count * selectivity))

    def invalidate_statistics(self, table_name=None):
        """
        Invalidate statistics for a table or all tables.

        Args:
            table_name: Table to invalidate, or None for all tables
        """
        with self.stats_lock:
            db_name = self.catalog_manager.get_current_database()
            if not db_name:
                return

            if table_name:
                # Invalidate specific table
                table_key = f"{db_name}.{table_name}"
                if table_key in self.statistics:
                    del self.statistics[table_key]
                if table_key in self.histograms:
                    del self.histograms[table_key]
                if table_key in self.last_refresh:
                    del self.last_refresh[table_key]
            else:
                # Invalidate all tables
                self.statistics.clear()
                self.histograms.clear()
                self.last_refresh.clear()

    def get_schema_statistics(self):
        """
        Get statistics for the entire schema.

        Returns:
            dict: Schema statistics
        """
        result = {
            "tables": {},
            "total_rows": 0,
            "total_size_kb": 0
        }

        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return result

        with self.stats_lock:
            for key, stats in self.statistics.items():
                if key.startswith(f"{db_name}."):
                    table_name = key.split(".", 1)[1]
                    result["tables"][table_name] = {
                        "row_count": stats.get("row_count", 0),
                        "size_kb": stats.get("approx_size_kb", 0)
                    }
                    result["total_rows"] += stats.get("row_count", 0)
                    result["total_size_kb"] += stats.get("approx_size_kb", 0)

        return result
