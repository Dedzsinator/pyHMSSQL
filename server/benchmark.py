"""
Focused benchmark script for the optimized B+ tree implementation only
"""
import time
import random
import logging
import sys
import traceback
import matplotlib.pyplot as plt
from bptree_optimized import BPlusTreeOptimized

# Configure minimal logging for benchmarks
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('benchmark')

def run_benchmark(num_keys=100000, batch_size=10000, order=50):
    """Run a benchmark for the optimized B+ tree implementation"""
    logger.info(f"Starting benchmark with {num_keys} keys, batch_size {batch_size}, order {order}")
    print("Setting up benchmark environment...")
    
    # Setup tree
    try:
        optimized_tree = BPlusTreeOptimized(order=order, name="optimized")
    except RuntimeError as e:
        print(f"Error setting up tree: {e}")
        logger.exception("Failed to setup tree")
        return
    
    # Generate random keys
    print("Generating random keys...")
    random.seed(42)  # For reproducibility
    keys = [random.randint(1, 1000000) for _ in range(num_keys)]
    
    # Benchmark insertion
    print(f"Benchmarking insertion of {num_keys} keys...")
    insert_times = []
    insertion_rates = []
    
    for i in range(0, num_keys, batch_size):
        batch = keys[i:i+batch_size]
        batch_size_actual = len(batch)
        
        # Time insertion
        try:
            print(f"Batch {i//batch_size + 1}/{(num_keys-1)//batch_size + 1}...")
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
                print(f"  - Time: {batch_time:.3f}s ({rate:.0f} ops/s)")
            else:
                print(f"  - Time: {batch_time:.3f}s (too fast to measure rate)")
                insertion_rates.append(0)  # Avoid division by zero
        except RuntimeError as e:
            print(f"Error during insertion: {e}")
            logger.exception("Error in insertion")
            traceback.print_exc()
            return
            
        print(f"Inserted {i + len(batch)}/{num_keys} keys")
    
    # Verify some keys
    print("\nVerifying key lookups...")
    validation_keys = random.sample(keys, min(100, num_keys))
    all_valid = True
    for key in validation_keys:
        value = optimized_tree.search(key)
        expected = f"value_{key}"
        if value != expected:
            print(f"WARNING: Invalid result for key {key}: got {value}, expected {expected}")
            all_valid = False
    
    if all_valid:
        print("All validation keys returned correct values!")
    
    # Benchmark search
    print(f"\nBenchmarking searches...")
    search_times = []
    search_keys = random.sample(keys, min(10000, num_keys // 10))
    
    # Run search in smaller batches for more detailed timing
    search_batch_size = 1000
    for i in range(0, len(search_keys), search_batch_size):
        batch = search_keys[i:i+search_batch_size]
        start = time.time()
        for key in batch:
            result = optimized_tree.search(key)
            if result is None:
                print(f"Warning: Key {key} not found")
        end = time.time()
        batch_time = end - start
        search_times.append(batch_time)
        if batch_time > 0:
            rate = len(batch) / batch_time
            print(f"Search batch {i//search_batch_size + 1}: {batch_time:.3f}s ({rate:.0f} ops/s)")
    
    total_search_time = sum(search_times)
    avg_search_rate = len(search_keys) / total_search_time if total_search_time > 0 else 0
    
    # Benchmark range queries
    print(f"\nBenchmarking range queries...")
    range_times = []
    ranges = []
    for _ in range(100):
        start_key = random.randint(1, 900000)
        end_key = start_key + random.randint(1000, 100000)
        ranges.append((start_key, end_key))
    
    # Run range queries in smaller batches
    range_batch_size = 10
    for i in range(0, len(ranges), range_batch_size):
        batch = ranges[i:i+range_batch_size]
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
            print(f"Range query batch {i//range_batch_size + 1}: {batch_time:.3f}s ({rate:.0f} queries/s)")
            print(f"  - Average results per query: {total_results/len(batch):.1f}")
    
    total_range_time = sum(range_times)
    avg_range_rate = len(ranges) / total_range_time if total_range_time > 0 else 0
    
    # Print results
    print("\n--- BENCHMARK RESULTS ---")
    print(f"Tree order: {order}, Keys: {num_keys}")
    
    print("\nInsertion Performance:")
    print(f"Total time: {sum(insert_times):.3f} seconds")
    print(f"Average rate: {num_keys/sum(insert_times):.0f} insertions/second")
    
    print("\nSearch Performance:")
    print(f"Total time: {total_search_time:.3f} seconds")
    print(f"Average rate: {avg_search_rate:.0f} searches/second")
    
    print("\nRange Query Performance:")
    print(f"Total time: {total_range_time:.3f} seconds")
    print(f"Average rate: {avg_range_rate:.0f} range queries/second")
    
    print("\n--- END OF BENCHMARK ---")
    
    # Plot insertion performance over time (batch number)
    try:
        plt.figure(figsize=(10, 6))
        plt.plot(range(1, len(insert_times) + 1), insert_times, marker='o', linestyle='-')
        plt.title('Insertion Time per Batch')
        plt.xlabel('Batch Number')
        plt.ylabel('Time (seconds)')
        plt.grid(True)
        plt.savefig('insertion_time_by_batch.png')
        plt.close()
        
        # Plot insertion rates
        plt.figure(figsize=(10, 6))
        plt.plot(range(1, len(insertion_rates) + 1), insertion_rates, marker='o', linestyle='-', color='green')
        plt.title('Insertion Rate by Batch')
        plt.xlabel('Batch Number')
        plt.ylabel('Rate (operations/second)')
        plt.grid(True)
        plt.savefig('insertion_rate_by_batch.png')
        plt.close()
        
        # Plot operation rates comparison
        plt.figure(figsize=(8, 6))
        operation_types = ['Insertion', 'Search', 'Range Query']
        rates = [
            num_keys/sum(insert_times) if sum(insert_times) > 0 else 0,
            avg_search_rate,
            avg_range_rate
        ]
        
        plt.bar(operation_types, rates, color=['blue', 'green', 'red'])
        plt.title('Performance by Operation Type')
        plt.ylabel('Rate (operations/second)')
        plt.grid(axis='y', alpha=0.3)
        
        # Add rate values on top of bars
        for i, v in enumerate(rates):
            plt.text(i, v + 0.1, f"{v:.0f}/s", ha='center')
            
        plt.tight_layout()
        plt.savefig('operation_rates.png')
        plt.close()
    except RuntimeError as e:
        print(f"Error generating plot: {e}")
        logger.exception("Error in plotting")
        
    print("Benchmark completed successfully!")
    return

if __name__ == "__main__":
    try:
        if len(sys.argv) != 4:
            print("Usage: python optimized_benchmark.py <num_keys> <batch_size> <order>")
            num_keys = int(input("Enter number of keys (default: 100000): ") or "100000")
            batch_size = int(input("Enter batch size (default: 10000): ") or "10000")
            order = int(input("Enter tree order (default: 50): ") or "50")
        else:
            num_keys = int(sys.argv[1])
            batch_size = int(sys.argv[2])
            order = int(sys.argv[3])
        
        run_benchmark(num_keys=num_keys, batch_size=batch_size, order=order)
        print("Benchmark script executed successfully!")
    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user!")
    except RuntimeError as e:
        print(f"Unexpected error: {e}")
        traceback.print_exc()
    sys.exit(0)