"""
Focused benchmark script for the optimized B+ tree implementation only
"""

import time
import random
import logging
import sys
import matplotlib.pyplot as plt

# Configure minimal logging for benchmarks
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("benchmark")


def run_benchmark(num_keys_param=100000, batch_size_param=10000, order_param=50):
    """Run a benchmark for the optimized B+ tree implementation"""
    try:
        # Try to import the optimized B+ tree
        from bptree import BPlusTreeOptimized
    except ImportError:
        logger.error("BPlusTreeOptimized module not found. Please build it first.")
        return None

    num_keys = num_keys_param
    batch_size = batch_size_param
    order = order_param

    logger.info(
        "Starting benchmark with %d keys, batch_size %d, order %d",
        num_keys,
        batch_size,
        order,
    )

    # Setup tree
    try:
        optimized_tree = BPlusTreeOptimized(order=order, name="optimized")
    except Exception as exc:
        logger.error("Error setting up tree: %s", exc)
        logger.exception("Failed to setup tree")
        return None

    # Generate random keys
    logger.info("Generating random keys...")
    random.seed(42)  # For reproducibility
    keys = [random.randint(1, 1000000) for _ in range(num_keys)]

    # Benchmark insertion
    logger.info("Benchmarking insertion of %d keys...", num_keys)
    insert_times = []
    insertion_rates = []

    for i in range(0, num_keys, batch_size):
        batch = keys[i : i + batch_size]
        batch_size_actual = len(batch)

        # Time insertion
        try:
            start = time.time()
            for key in batch:
                optimized_tree.insert(key, f"value_{key}")
            end = time.time()

            batch_time = end - start
            insert_times.append(batch_time)

            # Calculate insertion rate (ops/sec)
            if batch_time > 0:
                rate = batch_size_actual / batch_time
                insertion_rates.append(rate)
                logger.debug(
                    "Batch %d: %.3fs (%.0f ops/s)",
                    i // batch_size + 1,
                    batch_time,
                    rate,
                )
            else:
                insertion_rates.append(0)  # Avoid division by zero
        except Exception as exc:
            logger.error("Error during insertion: %s", exc)
            logger.exception("Error in insertion")
            return None

    # Verify some keys
    logger.info("Verifying key lookups...")
    validation_keys = random.sample(keys, min(100, num_keys))
    all_valid = True
    for key in validation_keys:
        value = optimized_tree.search(key)
        expected = f"value_{key}"
        if value != expected:
            logger.warning(
                "Invalid result for key %d: got %s, expected %s", key, value, expected
            )
            all_valid = False

    if all_valid:
        logger.info("All validation keys returned correct values!")

    # Benchmark search
    logger.info("Benchmarking searches...")
    search_times = []
    search_keys = random.sample(keys, min(10000, num_keys // 10))

    # Run search in smaller batches for more detailed timing
    search_batch_size = 1000
    for i in range(0, len(search_keys), search_batch_size):
        batch = search_keys[i : i + search_batch_size]
        start = time.time()
        for key in batch:
            result = optimized_tree.search(key)
            if result is None:
                logger.warning("Key %d not found", key)
        end = time.time()
        batch_time = end - start
        search_times.append(batch_time)
        if batch_time > 0:
            rate = len(batch) / batch_time
            logger.debug(
                "Search batch %d: %.3fs (%.0f ops/s)",
                i // search_batch_size + 1,
                batch_time,
                rate,
            )

    total_search_time = sum(search_times)
    avg_search_rate = (
        len(search_keys) / total_search_time if total_search_time > 0 else 0
    )

    # Benchmark range queries
    logger.info("Benchmarking range queries...")
    range_times = []
    ranges = []
    for _ in range(100):
        start_key = random.randint(1, 900000)
        end_key = start_key + random.randint(1000, 100000)
        ranges.append((start_key, end_key))

    # Run range queries in smaller batches
    range_batch_size = 10
    for i in range(0, len(ranges), range_batch_size):
        batch = ranges[i : i + range_batch_size]
        start = time.time()
        total_results = 0
        for start_key, end_key in batch:
            results = optimized_tree.range_query(start_key, end_key)
            total_results += len(results)
        end = time.time()
        batch_time = end - start
        range_times.append(batch_time)
        if batch_time > 0:
            rate = len(batch) / batch_time
            avg_results = total_results / len(batch)
            logger.debug(
                "Range query batch %d: %.3fs (%.0f queries/s)",
                i // range_batch_size + 1,
                batch_time,
                rate,
            )
            logger.debug("Average results per query: %.1f", avg_results)

    total_range_time = sum(range_times)
    avg_range_rate = len(ranges) / total_range_time if total_range_time > 0 else 0

    # Log results
    logger.info("=== BENCHMARK RESULTS ===")
    logger.info("Tree order: %d, Keys: %d", order, num_keys)

    logger.info("Insertion Performance:")
    logger.info("Total time: %.3f seconds", sum(insert_times))
    logger.info("Average rate: %.0f insertions/second", num_keys / sum(insert_times))

    logger.info("Search Performance:")
    logger.info("Total time: %.3f seconds", total_search_time)
    logger.info("Average rate: %.0f searches/second", avg_search_rate)

    logger.info("Range Query Performance:")
    logger.info("Total time: %.3f seconds", total_range_time)
    logger.info("Average rate: %.0f range queries/second", avg_range_rate)

    logger.info("=== END OF BENCHMARK ===")

    # Generate plots
    _generate_plots(
        insert_times, insertion_rates, num_keys, avg_search_rate, avg_range_rate
    )

    logger.info("Benchmark completed successfully!")
    return {
        "insertion_rate": num_keys / sum(insert_times) if insert_times else 0,
        "search_rate": avg_search_rate,
        "range_rate": avg_range_rate,
    }


def _generate_plots(
    insert_times, insertion_rates, num_keys, avg_search_rate, avg_range_rate
):
    """Generate benchmark visualization plots"""
    try:
        # Plot insertion performance over time (batch number)
        plt.figure(figsize=(10, 6))
        plt.plot(
            range(1, len(insert_times) + 1), insert_times, marker="o", linestyle="-"
        )
        plt.title("Insertion Time per Batch")
        plt.xlabel("Batch Number")
        plt.ylabel("Time (seconds)")
        plt.grid(True)
        plt.savefig("insertion_time_by_batch.png")
        plt.close()

        # Plot insertion rates
        plt.figure(figsize=(10, 6))
        plt.plot(
            range(1, len(insertion_rates) + 1),
            insertion_rates,
            marker="o",
            linestyle="-",
            color="green",
        )
        plt.title("Insertion Rate by Batch")
        plt.xlabel("Batch Number")
        plt.ylabel("Rate (operations/second)")
        plt.grid(True)
        plt.savefig("insertion_rate_by_batch.png")
        plt.close()

        # Plot operation rates comparison
        plt.figure(figsize=(8, 6))
        operation_types = ["Insertion", "Search", "Range Query"]
        rates = [
            num_keys / sum(insert_times) if sum(insert_times) > 0 else 0,
            avg_search_rate,
            avg_range_rate,
        ]

        plt.bar(operation_types, rates, color=["blue", "green", "red"])
        plt.title("Performance by Operation Type")
        plt.ylabel("Rate (operations/second)")
        plt.grid(axis="y", alpha=0.3)

        # Add rate values on top of bars
        for i, val in enumerate(rates):
            plt.text(i, val + 0.1, f"{val:.0f}/s", ha="center")

        plt.tight_layout()
        plt.savefig("operation_rates.png")
        plt.close()
        logger.info("Benchmark visualization plots saved successfully")
    except Exception as exc:
        logger.error("Error generating plot: %s", exc)
        logger.exception("Error in plotting")


def main():
    """Main function for benchmark script"""
    try:
        if len(sys.argv) != 4:
            logger.error(
                "Usage: python optimized_benchmark.py "
                "<num_keys> <batch_size> <order>"
            )
            num_keys = int(
                input("Enter number of keys (default: 100000): ") or "100000"
            )
            batch_size = int(input("Enter batch size (default: 10000): ") or "10000")
            order = int(input("Enter tree order (default: 50): ") or "50")
        else:
            num_keys = int(sys.argv[1])
            batch_size = int(sys.argv[2])
            order = int(sys.argv[3])

        result = run_benchmark(
            num_keys_param=num_keys, batch_size_param=batch_size, order_param=order
        )
        if result:
            logger.info("Benchmark script executed successfully!")
        else:
            logger.error("Benchmark failed to complete")
            sys.exit(1)
    except KeyboardInterrupt:
        logger.warning("Benchmark interrupted by user!")
    except Exception as exc:
        logger.error("Unexpected error: %s", exc)
        logger.exception("Traceback for unexpected error")
        sys.exit(1)


if __name__ == "__main__":
    main()
