#!/usr/bin/env python3
"""
Generate comprehensive visualizations from profiling benchmark JSON files.
Saves all generated graphics to the docs folder.
"""

import json
import os
import glob
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

# Set up matplotlib and seaborn
plt.style.use("seaborn-v0_8")
sns.set_palette("husl")
plt.rcParams["figure.figsize"] = (12, 8)
plt.rcParams["font.size"] = 10


def load_profiling_data(logs_dir):
    """Load all profiling JSON files from the logs directory."""
    json_files = glob.glob(
        os.path.join(logs_dir, "comprehensive_profiling_report_*.json")
    )
    json_files.sort()  # Sort chronologically

    profiling_data = []
    for file_path in json_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Extract timestamp from filename or use start_time
                timestamp_str = data.get("profiling_session", {}).get("start_time", "")
                data["filename"] = os.path.basename(file_path)
                data["timestamp"] = timestamp_str
                profiling_data.append(data)
        except Exception as e:
            print(f"Error loading {file_path}: {e}")

    return profiling_data


def create_operation_performance_comparison(profiling_data, output_dir):
    """Create a comparison chart of operation performance across different runs."""

    # Prepare data for comparison
    all_operations = set()
    for data in profiling_data:
        all_operations.update(data["summary"]["operations_by_type"].keys())

    # Create DataFrame for analysis
    comparison_data = []
    for i, data in enumerate(profiling_data):
        run_id = f"Run {i+1}"
        timestamp = data.get("timestamp", "")

        for op_name, op_stats in data["summary"]["operations_by_type"].items():
            comparison_data.append(
                {
                    "run_id": run_id,
                    "timestamp": timestamp,
                    "operation": op_name,
                    "avg_time": op_stats["average_time"],
                    "max_time": op_stats["max_time"],
                    "min_time": op_stats["min_time"],
                    "count": op_stats["count"],
                    "success_rate": op_stats["success_rate"],
                    "avg_memory_mb": op_stats.get("average_memory_mb", 0),
                    "avg_cpu_percent": op_stats.get("average_cpu_percent", 0),
                }
            )

    df = pd.DataFrame(comparison_data)

    # 1. Average Execution Time Comparison
    plt.figure(figsize=(16, 10))

    # Filter to show only major operations (avoid cluttering)
    major_ops = df.groupby("operation")["avg_time"].mean().nlargest(15).index
    major_df = df[df["operation"].isin(major_ops)]

    pivot_data = major_df.pivot(index="operation", columns="run_id", values="avg_time")

    pivot_data.plot(kind="bar", width=0.8)
    plt.title(
        "Average Execution Time by Operation Across Different Runs",
        fontsize=16,
        fontweight="bold",
    )
    plt.xlabel("Operations", fontsize=12)
    plt.ylabel("Average Time (seconds)", fontsize=12)
    plt.xticks(rotation=45, ha="right")
    plt.legend(title="Profiling Runs", bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "operation_performance_comparison.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()

    # 2. Memory Usage Comparison
    plt.figure(figsize=(14, 8))
    memory_df = df[df["avg_memory_mb"] > 0]  # Only operations that use memory
    if not memory_df.empty:
        memory_pivot = memory_df.pivot(
            index="operation", columns="run_id", values="avg_memory_mb"
        )
        memory_pivot.plot(kind="bar", width=0.8)
        plt.title(
            "Memory Usage by Operation Across Different Runs",
            fontsize=16,
            fontweight="bold",
        )
        plt.xlabel("Operations", fontsize=12)
        plt.ylabel("Average Memory (MB)", fontsize=12)
        plt.xticks(rotation=45, ha="right")
        plt.legend(title="Profiling Runs", bbox_to_anchor=(1.05, 1), loc="upper left")
        plt.tight_layout()
        plt.savefig(
            os.path.join(output_dir, "memory_usage_comparison.png"),
            dpi=300,
            bbox_inches="tight",
        )
    plt.close()


def create_performance_heatmap(profiling_data, output_dir):
    """Create heatmaps showing performance patterns."""

    # Use the most recent profiling data
    latest_data = profiling_data[-1] if profiling_data else None
    if not latest_data:
        return

    operations_data = latest_data["summary"]["operations_by_type"]

    # Prepare data for heatmap
    operations = list(operations_data.keys())
    metrics = [
        "average_time",
        "average_memory_mb",
        "average_cpu_percent",
        "success_rate",
    ]

    heatmap_data = []
    for op in operations:
        row = []
        for metric in metrics:
            value = operations_data[op].get(metric, 0)
            # Normalize success_rate to be on a comparable scale
            if metric == "success_rate":
                value = value * 100
            row.append(value)
        heatmap_data.append(row)

    # Create heatmap
    plt.figure(figsize=(10, 12))

    # Normalize data for better visualization
    heatmap_array = np.array(heatmap_data)
    normalized_data = np.zeros_like(heatmap_array)

    for i in range(heatmap_array.shape[1]):
        col_data = heatmap_array[:, i]
        if col_data.max() > 0:
            normalized_data[:, i] = (col_data / col_data.max()) * 100

    sns.heatmap(
        normalized_data,
        xticklabels=["Avg Time", "Avg Memory (MB)", "Avg CPU %", "Success Rate %"],
        yticklabels=operations,
        annot=False,
        cmap="YlOrRd",
        cbar_kws={"label": "Normalized Score (0-100)"},
    )

    plt.title(
        "Performance Metrics Heatmap (Latest Run)", fontsize=16, fontweight="bold"
    )
    plt.xlabel("Performance Metrics", fontsize=12)
    plt.ylabel("Operations", fontsize=12)
    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "performance_heatmap.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def create_execution_time_trends(profiling_data, output_dir):
    """Create trends showing how execution times change over different runs."""

    # Track specific operations across runs
    key_operations = [
        "INSERT_10K_PRODUCTS",
        "INSERT_10K_ORDER_DETAILS",
        "SELECT_ALL_LARGE",
        "SELECT_JOIN_COMPLEX",
        "UPDATE_BULK",
        "DELETE_BULK",
    ]

    trends_data = []

    for i, data in enumerate(profiling_data):
        run_number = i + 1
        timestamp = data.get("timestamp", "")

        for op_name in key_operations:
            if op_name in data["summary"]["operations_by_type"]:
                op_stats = data["summary"]["operations_by_type"][op_name]
                trends_data.append(
                    {
                        "run": run_number,
                        "timestamp": timestamp,
                        "operation": op_name,
                        "avg_time": op_stats["average_time"],
                        "memory_mb": op_stats.get("average_memory_mb", 0),
                    }
                )

    if not trends_data:
        return

    trends_df = pd.DataFrame(trends_data)

    # Create execution time trends
    plt.figure(figsize=(14, 8))

    for operation in key_operations:
        op_data = trends_df[trends_df["operation"] == operation]
        if not op_data.empty:
            plt.plot(
                op_data["run"],
                op_data["avg_time"],
                marker="o",
                linewidth=2,
                label=operation,
            )

    plt.title(
        "Execution Time Trends Across Profiling Runs", fontsize=16, fontweight="bold"
    )
    plt.xlabel("Profiling Run Number", fontsize=12)
    plt.ylabel("Average Execution Time (seconds)", fontsize=12)
    plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "execution_time_trends.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def create_operation_category_analysis(profiling_data, output_dir):
    """Analyze operations by category (CREATE, INSERT, SELECT, etc.)."""

    # Use the most recent profiling data
    latest_data = profiling_data[-1] if profiling_data else None
    if not latest_data:
        return

    operations_data = latest_data["summary"]["operations_by_type"]

    # Categorize operations
    categories = {
        "CREATE": [],
        "INSERT": [],
        "SELECT": [],
        "UPDATE": [],
        "DELETE": [],
        "INDEX": [],
        "OTHER": [],
    }

    for op_name, op_stats in operations_data.items():
        if op_name.startswith("CREATE"):
            categories["CREATE"].append((op_name, op_stats))
        elif op_name.startswith("INSERT"):
            categories["INSERT"].append((op_name, op_stats))
        elif op_name.startswith("SELECT"):
            categories["SELECT"].append((op_name, op_stats))
        elif op_name.startswith("UPDATE"):
            categories["UPDATE"].append((op_name, op_stats))
        elif op_name.startswith("DELETE"):
            categories["DELETE"].append((op_name, op_stats))
        elif "INDEX" in op_name:
            categories["INDEX"].append((op_name, op_stats))
        else:
            categories["OTHER"].append((op_name, op_stats))

    # Create category performance summary
    category_summary = []
    for category, operations in categories.items():
        if operations:
            total_time = sum(op[1]["total_time"] for op in operations)
            avg_time = np.mean([op[1]["average_time"] for op in operations])
            total_memory = sum(op[1].get("total_memory_mb", 0) for op in operations)
            operation_count = len(operations)

            category_summary.append(
                {
                    "category": category,
                    "total_time": total_time,
                    "avg_time": avg_time,
                    "total_memory": total_memory,
                    "operation_count": operation_count,
                }
            )

    # Create visualizations
    if category_summary:
        df_categories = pd.DataFrame(category_summary)

        # 1. Total execution time by category
        plt.figure(figsize=(12, 6))
        plt.subplot(1, 2, 1)
        plt.pie(
            df_categories["total_time"],
            labels=df_categories["category"],
            autopct="%1.1f%%",
        )
        plt.title("Total Execution Time by Operation Category", fontweight="bold")

        # 2. Average execution time by category
        plt.subplot(1, 2, 2)
        bars = plt.bar(df_categories["category"], df_categories["avg_time"])
        plt.title("Average Execution Time by Operation Category", fontweight="bold")
        plt.xlabel("Operation Category")
        plt.ylabel("Average Time (seconds)")
        plt.xticks(rotation=45)

        # Color bars based on performance
        for i, bar in enumerate(bars):
            if df_categories.iloc[i]["avg_time"] > 0.1:
                bar.set_color("red")
            elif df_categories.iloc[i]["avg_time"] > 0.01:
                bar.set_color("orange")
            else:
                bar.set_color("green")

        plt.tight_layout()
        plt.savefig(
            os.path.join(output_dir, "operation_category_analysis.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.close()


def create_system_resource_analysis(profiling_data, output_dir):
    """Analyze system resource usage patterns."""

    # Combine data from all runs
    all_resource_data = []

    for i, data in enumerate(profiling_data):
        run_id = f"Run {i+1}"
        system_info = data.get("profiling_session", {}).get("system_info", {})

        for op_name, op_stats in data["summary"]["operations_by_type"].items():
            all_resource_data.append(
                {
                    "run_id": run_id,
                    "operation": op_name,
                    "execution_time": op_stats["average_time"],
                    "memory_mb": op_stats.get("average_memory_mb", 0),
                    "cpu_percent": op_stats.get("average_cpu_percent", 0),
                    "system_cpu_count": system_info.get("cpu_count", 0),
                    "system_memory_gb": system_info.get("memory_total_gb", 0),
                }
            )

    if not all_resource_data:
        return

    resource_df = pd.DataFrame(all_resource_data)

    # Create resource usage scatter plots
    _, axes = plt.subplots(2, 2, figsize=(16, 12))

    # 1. Memory vs Execution Time
    axes[0, 0].scatter(
        resource_df["memory_mb"], resource_df["execution_time"], alpha=0.6
    )
    axes[0, 0].set_xlabel("Memory Usage (MB)")
    axes[0, 0].set_ylabel("Execution Time (seconds)")
    axes[0, 0].set_title("Memory Usage vs Execution Time")
    axes[0, 0].grid(True, alpha=0.3)

    # 2. CPU vs Execution Time
    axes[0, 1].scatter(
        resource_df["cpu_percent"],
        resource_df["execution_time"],
        alpha=0.6,
        color="orange",
    )
    axes[0, 1].set_xlabel("CPU Usage (%)")
    axes[0, 1].set_ylabel("Execution Time (seconds)")
    axes[0, 1].set_title("CPU Usage vs Execution Time")
    axes[0, 1].grid(True, alpha=0.3)

    # 3. Memory vs CPU
    axes[1, 0].scatter(
        resource_df["memory_mb"], resource_df["cpu_percent"], alpha=0.6, color="green"
    )
    axes[1, 0].set_xlabel("Memory Usage (MB)")
    axes[1, 0].set_ylabel("CPU Usage (%)")
    axes[1, 0].set_title("Memory Usage vs CPU Usage")
    axes[1, 0].grid(True, alpha=0.3)

    # 4. Resource efficiency (operations per unit resource)
    # Calculate efficiency metric
    resource_df["efficiency"] = 1 / (
        resource_df["execution_time"] * (resource_df["memory_mb"] + 1)
    )
    top_operations = resource_df.nlargest(10, "efficiency")

    axes[1, 1].barh(range(len(top_operations)), top_operations["efficiency"])
    axes[1, 1].set_yticks(range(len(top_operations)))
    axes[1, 1].set_yticklabels(top_operations["operation"], fontsize=8)
    axes[1, 1].set_xlabel("Efficiency Score")
    axes[1, 1].set_title("Top 10 Most Efficient Operations")

    plt.suptitle("System Resource Analysis", fontsize=16, fontweight="bold")
    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "system_resource_analysis.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def create_summary_dashboard(profiling_data, output_dir):
    """Create a summary dashboard with key metrics."""

    if not profiling_data:
        return

    latest_data = profiling_data[-1]
    summary = latest_data["summary"]

    _, axes = plt.subplots(2, 3, figsize=(18, 12))

    # 1. Total operations and execution time
    axes[0, 0].bar(
        ["Total Operations", "Total Time (s)"],
        [summary["total_operations"], summary["total_execution_time"]],
    )
    axes[0, 0].set_title("Overall Performance Summary")
    axes[0, 0].set_ylabel("Count / Time")

    # 2. Top 10 slowest operations
    ops_data = summary["operations_by_type"]
    slowest_ops = sorted(
        ops_data.items(), key=lambda x: x[1]["average_time"], reverse=True
    )[:10]
    op_names = [op[0][:20] + "..." if len(op[0]) > 20 else op[0] for op in slowest_ops]
    op_times = [op[1]["average_time"] for op in slowest_ops]

    axes[0, 1].barh(range(len(op_names)), op_times)
    axes[0, 1].set_yticks(range(len(op_names)))
    axes[0, 1].set_yticklabels(op_names, fontsize=8)
    axes[0, 1].set_xlabel("Average Time (seconds)")
    axes[0, 1].set_title("Top 10 Slowest Operations")

    # 3. Memory usage distribution
    memory_usage = [
        ops_data[op]["average_memory_mb"]
        for op in ops_data
        if ops_data[op].get("average_memory_mb", 0) > 0
    ]
    if memory_usage:
        axes[0, 2].hist(memory_usage, bins=15, alpha=0.7, color="skyblue")
        axes[0, 2].set_xlabel("Memory Usage (MB)")
        axes[0, 2].set_ylabel("Number of Operations")
        axes[0, 2].set_title("Memory Usage Distribution")

    # 4. Success rate overview
    success_rates = [ops_data[op]["success_rate"] for op in ops_data]
    success_counts = [
        sum(1 for rate in success_rates if rate == 1.0),
        sum(1 for rate in success_rates if rate < 1.0),
    ]

    axes[1, 0].pie(
        success_counts, labels=["100% Success", "Partial Success"], autopct="%1.1f%%"
    )
    axes[1, 0].set_title("Operation Success Rate Distribution")

    # 5. Execution time trends across runs
    if len(profiling_data) > 1:
        run_times = [data["summary"]["total_execution_time"] for data in profiling_data]
        axes[1, 1].plot(
            range(1, len(run_times) + 1), run_times, marker="o", linewidth=2
        )
        axes[1, 1].set_xlabel("Profiling Run")
        axes[1, 1].set_ylabel("Total Execution Time (s)")
        axes[1, 1].set_title("Performance Trend Across Runs")
        axes[1, 1].grid(True, alpha=0.3)

    # 6. Operation count by category
    category_counts = {}
    for op_name in ops_data.keys():
        category = op_name.split("_")[0]
        category_counts[category] = category_counts.get(category, 0) + 1

    if category_counts:
        axes[1, 2].pie(
            category_counts.values(), labels=category_counts.keys(), autopct="%1.0f"
        )
        axes[1, 2].set_title("Operations by Category")

    plt.suptitle("pyHMSSQL Performance Dashboard", fontsize=20, fontweight="bold")
    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "performance_dashboard.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def create_perf_flamegraph_analysis(profiling_data, output_dir):
    """Create analysis of perf flamegraph data and hardware counters."""
    perf_analysis_data = []

    for data in profiling_data:
        detailed_metrics = data.get("detailed_metrics", {})
        for _, operations in detailed_metrics.items():
            for operation in operations:
                perf_data = operation.get("perf_data", {})
                if perf_data:
                    analysis_entry = {
                        "operation": operation["operation"],
                        "execution_time": operation["execution_time_seconds"],
                        "memory_mb": operation["resource_usage"].get(
                            "memory_rss_mb", 0
                        ),
                        "cpu_percent": operation["resource_usage"].get(
                            "cpu_percent_avg", 0
                        ),
                    }

                    # Add hardware counter data if available
                    if "hardware_counters" in perf_data:
                        counters = perf_data["hardware_counters"]
                        analysis_entry.update(
                            {
                                "cpu_cycles": counters.get("cycles", 0),
                                "instructions": counters.get("instructions", 0),
                                "cache_misses": counters.get("cache-misses", 0),
                                "branch_misses": counters.get("branch-misses", 0),
                            }
                        )

                        # Calculate derived metrics
                        if (
                            counters.get("cycles", 0) > 0
                            and counters.get("instructions", 0) > 0
                        ):
                            analysis_entry["ipc"] = (
                                counters["instructions"] / counters["cycles"]
                            )
                        if (
                            counters.get("instructions", 0) > 0
                            and counters.get("cache-misses", 0) > 0
                        ):
                            analysis_entry["cache_miss_rate"] = (
                                counters["cache-misses"] / counters["instructions"]
                            )

                    # Add CPU profiling info if available
                    if "cpu_profile" in perf_data:
                        analysis_entry["has_cpu_profile"] = True
                        analysis_entry["flamegraph_available"] = (
                            "flamegraph_path" in perf_data
                        )

                    perf_analysis_data.append(analysis_entry)

    if not perf_analysis_data:
        print("No perf data found for analysis")
        return

    df = pd.DataFrame(perf_analysis_data)

    # Create perf-specific visualizations
    _, axes = plt.subplots(2, 2, figsize=(16, 12))

    # 1. Instructions Per Cycle (IPC) vs Execution Time
    if "ipc" in df.columns:
        valid_ipc = df[df["ipc"] > 0]
        if not valid_ipc.empty:
            axes[0, 0].scatter(
                valid_ipc["execution_time"],
                valid_ipc["ipc"],
                alpha=0.7,
                c=valid_ipc["memory_mb"],
                cmap="viridis",
                s=60,
            )
            axes[0, 0].set_xlabel("Execution Time (seconds)")
            axes[0, 0].set_ylabel("Instructions Per Cycle (IPC)")
            axes[0, 0].set_title("CPU Efficiency: IPC vs Execution Time")
            axes[0, 0].grid(True, alpha=0.3)
            cbar = plt.colorbar(axes[0, 0].collections[0], ax=axes[0, 0])
            cbar.set_label("Memory Usage (MB)")

    # 2. Cache Miss Rate vs Performance
    if "cache_miss_rate" in df.columns:
        valid_cache = df[df["cache_miss_rate"] > 0]
        if not valid_cache.empty:
            axes[0, 1].scatter(
                valid_cache["cache_miss_rate"],
                valid_cache["execution_time"],
                alpha=0.7,
                s=60,
                color="red",
            )
            axes[0, 1].set_xlabel("Cache Miss Rate")
            axes[0, 1].set_ylabel("Execution Time (seconds)")
            axes[0, 1].set_title("Memory Performance: Cache Misses vs Time")
            axes[0, 1].grid(True, alpha=0.3)

    # 3. Hardware Counter Summary
    counter_columns = ["cpu_cycles", "instructions", "cache_misses", "branch_misses"]
    available_counters = [
        col for col in counter_columns if col in df.columns and df[col].sum() > 0
    ]

    if available_counters:
        counter_data = df[available_counters].sum()
        axes[1, 0].bar(range(len(counter_data)), counter_data.values)
        axes[1, 0].set_xticks(range(len(counter_data)))
        axes[1, 0].set_xticklabels(
            [col.replace("_", " ").title() for col in counter_data.index], rotation=45
        )
        axes[1, 0].set_ylabel("Total Count")
        axes[1, 0].set_title("Hardware Performance Counters Summary")
        axes[1, 0].grid(True, alpha=0.3)

    # 4. Perf Coverage Analysis
    perf_coverage = {
        "With CPU Profile": len(df[df.get("has_cpu_profile", False) == True]),
        "With Hardware Counters": (
            len(df[df[available_counters].sum(axis=1) > 0]) if available_counters else 0
        ),
        "With Flamegraphs": len(df[df.get("flamegraph_available", False) == True]),
        "Total Operations": len(df),
    }

    coverage_values = list(perf_coverage.values())[:-1]  # Exclude total
    coverage_labels = list(perf_coverage.keys())[:-1]

    axes[1, 1].pie(
        coverage_values, labels=coverage_labels, autopct="%1.1f%%", startangle=90
    )
    axes[1, 1].set_title(
        f'Perf Data Coverage ({perf_coverage["Total Operations"]} operations)'
    )

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "perf_analysis.png"), dpi=300, bbox_inches="tight"
    )
    plt.close()


def create_perf_hardware_counter_trends(profiling_data, output_dir):
    """Create trends analysis for hardware performance counters."""
    counter_data = []

    for data in profiling_data:
        detailed_metrics = data.get("detailed_metrics", {})
        run_timestamp = data.get("timestamp", "")

        for operation_type, operations in detailed_metrics.items():
            for operation in operations:
                perf_data = operation.get("perf_data", {})
                if perf_data and "hardware_counters" in perf_data:
                    counters = perf_data["hardware_counters"]

                    counter_data.append(
                        {
                            "timestamp": run_timestamp,
                            "operation": operation["operation"],
                            "operation_type": operation_type,
                            "execution_time": operation["execution_time_seconds"],
                            "cycles": counters.get("cycles", 0),
                            "instructions": counters.get("instructions", 0),
                            "cache_misses": counters.get("cache-misses", 0),
                            "branch_misses": counters.get("branch-misses", 0),
                            "ipc": (
                                counters.get("instructions", 0)
                                / counters.get("cycles", 1)
                                if counters.get("cycles", 0) > 0
                                else 0
                            ),
                        }
                    )

    if not counter_data:
        print("No hardware counter data found for trend analysis")
        return

    df = pd.DataFrame(counter_data)

    # Create hardware counter trend visualizations
    _, axes = plt.subplots(2, 2, figsize=(16, 12))

    # 1. IPC trends by operation type
    if df["ipc"].sum() > 0:
        ipc_by_type = (
            df.groupby("operation_type")["ipc"].mean().sort_values(ascending=False)
        )
        axes[0, 0].bar(range(len(ipc_by_type)), ipc_by_type.values)
        axes[0, 0].set_xticks(range(len(ipc_by_type)))
        axes[0, 0].set_xticklabels(ipc_by_type.index, rotation=45, ha="right")
        axes[0, 0].set_ylabel("Average IPC")
        axes[0, 0].set_title("Average Instructions Per Cycle by Operation Type")
        axes[0, 0].grid(True, alpha=0.3)

    # 2. Cache miss trends
    if df["cache_misses"].sum() > 0:
        cache_by_type = df.groupby("operation_type")["cache_misses"].mean()
        axes[0, 1].bar(range(len(cache_by_type)), cache_by_type.values, color="orange")
        axes[0, 1].set_xticks(range(len(cache_by_type)))
        axes[0, 1].set_xticklabels(cache_by_type.index, rotation=45, ha="right")
        axes[0, 1].set_ylabel("Average Cache Misses")
        axes[0, 1].set_title("Average Cache Misses by Operation Type")
        axes[0, 1].grid(True, alpha=0.3)

    # 3. Execution time vs CPU efficiency
    if df["cycles"].sum() > 0:
        # Calculate CPU efficiency (instructions per second)
        df["instructions_per_second"] = df["instructions"] / df["execution_time"]
        valid_data = df[df["instructions_per_second"] > 0]

        if not valid_data.empty:
            axes[1, 0].scatter(
                valid_data["execution_time"],
                valid_data["instructions_per_second"],
                alpha=0.7,
                s=60,
                c=valid_data["cache_misses"],
                cmap="plasma",
            )
            axes[1, 0].set_xlabel("Execution Time (seconds)")
            axes[1, 0].set_ylabel("Instructions per Second")
            axes[1, 0].set_title("CPU Throughput vs Execution Time")
            axes[1, 0].grid(True, alpha=0.3)
            axes[1, 0].set_yscale("log")

    # 4. Performance correlation matrix
    numeric_cols = [
        "execution_time",
        "cycles",
        "instructions",
        "cache_misses",
        "branch_misses",
        "ipc",
    ]
    available_cols = [
        col for col in numeric_cols if col in df.columns and df[col].sum() > 0
    ]

    if len(available_cols) > 2:
        correlation_matrix = df[available_cols].corr()
        im = axes[1, 1].imshow(
            correlation_matrix, cmap="RdBu_r", aspect="auto", vmin=-1, vmax=1
        )
        axes[1, 1].set_xticks(range(len(available_cols)))
        axes[1, 1].set_yticks(range(len(available_cols)))
        axes[1, 1].set_xticklabels(
            [col.replace("_", " ").title() for col in available_cols],
            rotation=45,
            ha="right",
        )
        axes[1, 1].set_yticklabels(
            [col.replace("_", " ").title() for col in available_cols]
        )
        axes[1, 1].set_title("Performance Metrics Correlation Matrix")

        # Add correlation values to the heatmap
        for i in range(len(available_cols)):
            for j in range(len(available_cols)):
                axes[1, 1].text(
                    j,
                    i,
                    f"{correlation_matrix.iloc[i, j]:.2f}",
                    ha="center",
                    va="center",
                    color="black",
                    fontsize=8,
                )

        plt.colorbar(im, ax=axes[1, 1], label="Correlation")

    plt.tight_layout()
    plt.savefig(
        os.path.join(output_dir, "perf_hardware_counter_trends.png"),
        dpi=300,
        bbox_inches="tight",
    )
    plt.close()


def main():
    """Main function to generate all visualizations."""
    # Paths
    logs_dir = "/home/deginandor/Documents/Programming/pyHMSSQL/logs"
    output_dir = "/home/deginandor/Documents/Programming/pyHMSSQL/docs"

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    print("Loading profiling data...")
    profiling_data = load_profiling_data(logs_dir)

    if not profiling_data:
        print("No profiling data found!")
        return

    print(f"Found {len(profiling_data)} profiling reports")

    print("Generating visualizations...")

    # Generate all visualizations
    create_operation_performance_comparison(profiling_data, output_dir)
    print("✓ Operation performance comparison")

    create_performance_heatmap(profiling_data, output_dir)
    print("✓ Performance heatmap")

    create_execution_time_trends(profiling_data, output_dir)
    print("✓ Execution time trends")

    create_operation_category_analysis(profiling_data, output_dir)
    print("✓ Operation category analysis")

    create_system_resource_analysis(profiling_data, output_dir)
    print("✓ System resource analysis")

    create_summary_dashboard(profiling_data, output_dir)
    print("✓ Performance dashboard")

    # New perf-specific visualizations
    create_perf_flamegraph_analysis(profiling_data, output_dir)
    print("✓ Perf flamegraph analysis")

    create_perf_hardware_counter_trends(profiling_data, output_dir)
    print("✓ Perf hardware counter trends")

    print(f"\nAll visualizations saved to: {output_dir}")
    print("\nGenerated files:")
    generated_files = [
        "operation_performance_comparison.png",
        "memory_usage_comparison.png",
        "performance_heatmap.png",
        "execution_time_trends.png",
        "operation_category_analysis.png",
        "system_resource_analysis.png",
        "performance_dashboard.png",
        "perf_analysis.png",
        "perf_hardware_counter_trends.png",
    ]

    for file in generated_files:
        file_path = os.path.join(output_dir, file)
        if os.path.exists(file_path):
            print(f"  ✓ {file}")
        else:
            print(f"  ✗ {file} (not generated)")


if __name__ == "__main__":
    main()
