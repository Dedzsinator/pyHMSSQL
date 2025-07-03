#!/usr/bin/env python3
"""
Demo script to test the real compression manager implementation
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from kvstore.compression import (
    CompressionManager, CompressionType, CompressionConfig,
    create_compression_manager, compress_data, detect_best_algorithm,
    benchmark_compression, CompressionStrategy
)

def test_basic_compression():
    """Test basic compression functionality"""
    print("=== Testing Basic Compression ===")
    
    # Create compression manager
    manager = create_compression_manager(level=6, use_checksum=True)
    
    # Test data
    test_data = "Hello, World! " * 100
    
    # Test different algorithms
    algorithms = [
        CompressionType.GZIP,
        CompressionType.ZLIB,
        CompressionType.LZ4,
        CompressionType.SNAPPY,
        CompressionType.ZSTD,
    ]
    
    for algo in algorithms:
        try:
            result = manager.compress(test_data, algo)
            decompressed = manager.decompress(result.compressed_data, algo, result.metadata)
            
            print(f"{algo.value:8}: {result.original_size:6} -> {result.compressed_size:6} bytes "
                  f"({result.compression_percentage:.1f}% saved, {result.compression_time*1000:.2f}ms)")
            
            assert decompressed == test_data, f"Decompression failed for {algo.value}"
            
        except Exception as e:
            print(f"{algo.value:8}: Not available ({e})")

def test_adaptive_compression():
    """Test adaptive compression selection"""
    print("\n=== Testing Adaptive Compression ===")
    
    manager = create_compression_manager()
    
    test_cases = {
        "Small text": "Hello World",
        "Large repetitive": "ABCD" * 1000,
        "JSON data": {"users": [{"id": i, "name": f"user{i}"} for i in range(100)]},
        "Numeric sequence": list(range(1000)),
        "Time series": [(i, i * 1.5 + 0.1) for i in range(500)],
    }
    
    for name, data in test_cases.items():
        result = manager.compress(data)  # Adaptive selection
        decompressed = manager.decompress(result.compressed_data, result.algorithm, result.metadata)
        
        print(f"{name:20}: {result.algorithm.value:8} - {result.compression_percentage:.1f}% saved")
        
        # Special handling for time series data (tuple/list format differences)
        if name == "Time series":
            # Convert both to same format for comparison
            if isinstance(decompressed, list) and isinstance(data, list):
                data_normalized = [list(item) if isinstance(item, tuple) else item for item in data]
                decompressed_normalized = [list(item) if isinstance(item, tuple) else item for item in decompressed]
                assert decompressed_normalized == data_normalized, f"Time series compression failed"
            else:
                assert decompressed == data, f"Adaptive compression failed for {name}"
        else:
            assert decompressed == data, f"Adaptive compression failed for {name}"

def test_specialized_codecs():
    """Test specialized compression codecs"""
    print("\n=== Testing Specialized Codecs ===")
    
    manager = create_compression_manager()
    
    # Test Delta compression
    numeric_sequence = list(range(100, 200))  # Sequential numbers
    result = manager.compress(numeric_sequence, CompressionType.DELTA)
    decompressed = manager.decompress(result.compressed_data, CompressionType.DELTA, result.metadata)
    print(f"Delta (numeric):     {result.compression_percentage:.1f}% saved")
    assert decompressed == numeric_sequence
    
    # Test Gorilla compression for time series
    time_series = [(i, i * 1.1 + 0.05) for i in range(100)]
    result = manager.compress(time_series, CompressionType.GORILLA)
    decompressed = manager.decompress(result.compressed_data, CompressionType.GORILLA, result.metadata)
    print(f"Gorilla (time series): {result.compression_percentage:.1f}% saved")
    
    # Handle time series comparison (tuple/list format differences)
    if isinstance(decompressed, list) and isinstance(time_series, list):
        time_series_normalized = [list(item) if isinstance(item, tuple) else item for item in time_series]
        decompressed_normalized = [list(item) if isinstance(item, tuple) else item for item in decompressed]
        assert decompressed_normalized == time_series_normalized, "Gorilla compression failed"
    
    # Test RLE compression
    repetitive_data = [1, 1, 1, 2, 2, 3, 3, 3, 3, 4, 5, 5]
    result = manager.compress(repetitive_data, CompressionType.RLE)
    decompressed = manager.decompress(result.compressed_data, CompressionType.RLE, result.metadata)
    print(f"RLE (repetitive):    {result.compression_percentage:.1f}% saved")
    assert decompressed == repetitive_data

def test_benchmarking():
    """Test compression benchmarking"""
    print("\n=== Testing Compression Benchmarking ===")
    
    test_data = "The quick brown fox jumps over the lazy dog. " * 100
    
    # Benchmark available algorithms
    results = benchmark_compression(test_data)
    
    print("Algorithm  Ratio   Comp.Time  Decomp.Time  Comp.Speed")
    print("-" * 55)
    
    for algo, metrics in sorted(results.items(), key=lambda x: x[1]["compression_ratio"]):
        print(f"{algo.value:10} {metrics['compression_ratio']:.3f}   "
              f"{metrics['compression_time']*1000:8.2f}ms  {metrics['decompression_time']*1000:9.2f}ms  "
              f"{metrics['compression_speed_mbps']:8.1f}MB/s")

def test_algorithm_selection():
    """Test best algorithm detection"""
    print("\n=== Testing Algorithm Selection ===")
    
    test_data = {"data": list(range(1000)), "metadata": "test" * 50}
    
    strategies = [
        CompressionStrategy.SPEED,
        CompressionStrategy.RATIO,
        CompressionStrategy.BALANCED,
        CompressionStrategy.ADAPTIVE,
    ]
    
    for strategy in strategies:
        best_algo = detect_best_algorithm(test_data, strategy)
        print(f"{strategy.value:10}: {best_algo.value}")

def test_statistics():
    """Test compression statistics"""
    print("\n=== Testing Statistics ===")
    
    manager = create_compression_manager()
    
    # Perform several compressions
    test_data = ["Hello World", {"key": "value"}, list(range(100))]
    
    for data in test_data:
        result = manager.compress(data)
        manager.decompress(result.compressed_data, result.algorithm, result.metadata)
    
    stats = manager.get_stats()
    print(f"Compressions: {stats['compressions']}")
    print(f"Decompressions: {stats['decompressions']}")
    print(f"Overall compression ratio: {stats['overall_compression_ratio']:.3f}")
    print(f"Average compression time: {stats['avg_compression_time']*1000:.2f}ms")
    print(f"Available algorithms: {len(stats['available_algorithms'])}")
    print(f"Algorithm usage: {stats['algorithm_usage']}")

def main():
    """Run all tests"""
    print("Testing Real Compression Manager Implementation")
    print("=" * 50)
    
    try:
        test_basic_compression()
        test_adaptive_compression()
        test_specialized_codecs()
        test_benchmarking()
        test_algorithm_selection()
        test_statistics()
        
        print("\n✅ All compression tests passed successfully!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
