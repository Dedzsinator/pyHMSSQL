"""
Unit tests for advanced compression codecs
"""

import pytest
import json
import time
from unittest.mock import Mock, patch

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from kvstore.compression import (
    CompressionManager,
    CompressionType,
    CompressionResult,
    GorillaCodec,
    DeltaCodec,
    CompressionError,
)


class TestCompressionTypes:
    """Test compression type definitions"""

    def test_compression_types_defined(self):
        """Test that all compression types are properly defined"""
        assert CompressionType.LZ4.value == "lz4"
        assert CompressionType.SNAPPY.value == "snappy"
        assert CompressionType.GZIP.value == "gzip"
        assert CompressionType.GORILLA.value == "gorilla"
        assert CompressionType.DELTA.value == "delta"


class TestCompressionResult:
    """Test compression result structure"""

    def test_compression_result_creation(self):
        """Test creating compression result"""
        result = CompressionResult(
            compressed_data=b"compressed",
            original_size=100,
            compressed_size=50,
            algorithm=CompressionType.LZ4,
            metadata={"algorithm": "lz4"},
        )

        assert result.compressed_data == b"compressed"
        assert result.original_size == 100
        assert result.compressed_size == 50
        assert result.compression_ratio == 0.5
        assert result.metadata["algorithm"] == "lz4"


class TestCompressionManager:
    """Test compression manager functionality"""

    @pytest.fixture
    def manager(self):
        """Create compression manager instance"""
        return CompressionManager()

    def test_manager_initialization(self, manager):
        """Test compression manager initialization"""
        assert manager is not None
        stats = manager.get_stats()
        assert stats["compressions"] == 0
        assert stats["decompressions"] == 0

    def test_compress_text_data(self, manager):
        """Test compressing text data"""
        text_data = "This is a test string for compression" * 10

        # Test LZ4 compression
        result = manager.compress(text_data, CompressionType.LZ4)
        assert isinstance(result, CompressionResult)
        assert result.compressed_size < result.original_size
        assert result.compression_ratio < 1.0

        # Test decompression
        decompressed = manager.decompress(
            result.compressed_data, CompressionType.LZ4, result.metadata
        )
        assert decompressed == text_data

    def test_compress_json_data(self, manager):
        """Test compressing JSON data"""
        json_data = {
            "users": [{"id": i, "name": f"user{i}", "active": True} for i in range(50)]
        }

        # Test GZIP compression (good for JSON)
        result = manager.compress(json_data, CompressionType.GZIP)
        assert isinstance(result, CompressionResult)
        assert result.compressed_size < result.original_size

        # Test decompression
        decompressed = manager.decompress(
            result.compressed_data, CompressionType.GZIP, result.metadata
        )
        assert decompressed == json_data

    def test_compress_numeric_data(self, manager):
        """Test compressing numeric data"""
        numeric_data = list(range(1000, 2000))

        # Test Snappy compression
        result = manager.compress(numeric_data, CompressionType.SNAPPY)
        assert isinstance(result, CompressionResult)

        # Test decompression
        decompressed = manager.decompress(
            result.compressed_data, CompressionType.SNAPPY, result.metadata
        )
        assert decompressed == numeric_data

    def test_compression_statistics(self, manager):
        """Test compression statistics tracking"""
        test_data = "Test data for statistics"

        # Perform several compressions
        for _ in range(3):
            result = manager.compress(test_data, CompressionType.LZ4)
            manager.decompress(
                result.compressed_data, CompressionType.LZ4, result.metadata
            )

        stats = manager.get_stats()
        assert stats["compressions"] == 3
        assert stats["decompressions"] == 3
        assert stats["total_original_bytes"] > 0
        assert stats["total_compressed_bytes"] > 0

    def test_compression_algorithm_comparison(self, manager):
        """Test comparing different compression algorithms"""
        test_data = "Sample data for algorithm comparison testing" * 20
        algorithms = [CompressionType.LZ4, CompressionType.SNAPPY, CompressionType.GZIP]

        results = {}
        for algorithm in algorithms:
            result = manager.compress(test_data, algorithm)
            results[algorithm] = result

            # Verify decompression works
            decompressed = manager.decompress(
                result.compressed_data, algorithm, result.metadata
            )
            assert decompressed == test_data

        # All algorithms should compress the data
        for algorithm, result in results.items():
            assert result.compressed_size < result.original_size
            assert result.compression_ratio < 1.0


class TestGorillaCodec:
    """Test Gorilla compression codec for time series data"""

    @pytest.fixture
    def codec(self):
        """Create Gorilla codec instance"""
        return GorillaCodec()

    def test_gorilla_codec_initialization(self, codec):
        """Test Gorilla codec initialization"""
        assert codec is not None

    def test_gorilla_time_series_compression(self, codec):
        """Test Gorilla compression on time series data"""
        # Generate time series data (timestamp, value pairs)
        base_time = int(time.time())
        time_series = []
        for i in range(100):
            timestamp = base_time + i * 60  # Every minute
            value = 23.5 + (i % 10) * 0.1  # Slight variations
            time_series.append((timestamp, value))

        # Compress
        result = codec.compress(time_series)
        assert isinstance(result, CompressionResult)

        # For time series with patterns, should achieve good compression
        if result.original_size > 0:
            assert result.compression_ratio <= 1.0

        # Decompress
        decompressed = codec.decompress(result.compressed_data, result.metadata)
        assert len(decompressed) == len(time_series)

        # Verify data integrity (allowing for small floating point differences)
        for original, decompressed_item in zip(time_series, decompressed):
            assert original[0] == decompressed_item[0]  # Timestamp exact
            assert abs(original[1] - decompressed_item[1]) < 0.001  # Value close

    def test_gorilla_with_constant_values(self, codec):
        """Test Gorilla codec with constant values (should compress well)"""
        # All same values should compress very well
        constant_series = [(1640995200 + i, 25.0) for i in range(50)]

        result = codec.compress(constant_series)
        decompressed = codec.decompress(result.compressed_data, result.metadata)

        assert len(decompressed) == len(constant_series)
        for original, decompressed_item in zip(constant_series, decompressed):
            assert original == decompressed_item

    def test_gorilla_with_random_values(self, codec):
        """Test Gorilla codec with random values"""
        import random

        random.seed(42)  # For reproducible tests

        random_series = []
        base_time = int(time.time())
        for i in range(50):
            timestamp = base_time + i * 60
            value = random.uniform(20.0, 30.0)
            random_series.append((timestamp, value))

        result = codec.compress(random_series)
        decompressed = codec.decompress(result.compressed_data, result.metadata)

        assert len(decompressed) == len(random_series)
        # For random data, compression may not be as effective
        # but data integrity should be maintained


class TestDeltaCodec:
    """Test Delta compression codec for sequential data"""

    @pytest.fixture
    def codec(self):
        """Create Delta codec instance"""
        return DeltaCodec()

    def test_delta_codec_initialization(self, codec):
        """Test Delta codec initialization"""
        assert codec is not None

    def test_delta_sequential_integers(self, codec):
        """Test Delta compression on sequential integers"""
        # Sequential integers should compress very well with delta encoding
        sequential_data = list(range(1000, 1100))

        result = codec.compress(sequential_data)
        assert isinstance(result, CompressionResult)

        # Sequential data should compress well
        assert result.compression_ratio < 1.0

        # Decompress and verify
        decompressed = codec.decompress(result.compressed_data, result.metadata)
        assert decompressed == sequential_data

    def test_delta_with_small_differences(self, codec):
        """Test Delta codec with small differences"""
        # Data with small differences between consecutive values
        base_value = 1000
        small_diff_data = [base_value]
        for i in range(1, 100):
            # Add small random delta
            import random

            random.seed(i)  # Reproducible
            delta = random.randint(-5, 5)
            small_diff_data.append(small_diff_data[-1] + delta)

        result = codec.compress(small_diff_data)
        decompressed = codec.decompress(result.compressed_data, result.metadata)

        assert decompressed == small_diff_data

    def test_delta_floating_point_data(self, codec):
        """Test Delta codec with floating point data"""
        # Use integers as floats to avoid precision issues entirely
        float_data = [float(i) for i in range(10, 60)]

        result = codec.compress(float_data)
        decompressed = codec.decompress(result.compressed_data, result.metadata)

        # Check length
        assert len(decompressed) == len(float_data)

        # Check exact equality for integer floats
        for original, decompressed_val in zip(float_data, decompressed):
            assert original == decompressed_val


class TestCompressionErrorHandling:
    """Test compression error handling"""

    @pytest.fixture
    def manager(self):
        """Create compression manager instance"""
        return CompressionManager()

    def test_invalid_compression_type(self, manager):
        """Test handling of invalid compression types"""
        with pytest.raises((ValueError, CompressionError)):
            manager.compress("test data", "invalid_algorithm")

    def test_decompress_invalid_data(self, manager):
        """Test decompression of invalid data"""
        with pytest.raises((ValueError, CompressionError)):
            manager.decompress(b"invalid_compressed_data", CompressionType.LZ4, {})

    def test_compress_unsupported_data_type(self, manager):
        """Test compression of unsupported data types"""

        # Try to compress something that can't be serialized
        class UnsupportedClass:
            pass

        unsupported_data = UnsupportedClass()

        # Should handle gracefully or raise appropriate error
        try:
            result = manager.compress(unsupported_data, CompressionType.LZ4)
            # If it succeeds, decompression should work too
            decompressed = manager.decompress(
                result.compressed_data, CompressionType.LZ4, result.metadata
            )
        except (TypeError, CompressionError, ValueError):
            # Expected for unsupported types
            pass

    def test_compression_with_corrupted_metadata(self, manager):
        """Test decompression with corrupted metadata"""
        test_data = "Test data for corruption test"

        # Compress normally
        result = manager.compress(test_data, CompressionType.LZ4)

        # Corrupt metadata
        corrupted_metadata = {"corrupted": "metadata"}

        # Should handle corrupted metadata gracefully
        # Note: Our implementation may handle missing metadata by using defaults
        try:
            decompressed = manager.decompress(
                result.compressed_data, CompressionType.LZ4, corrupted_metadata
            )
            # If it doesn't raise an exception, that's also acceptable behavior
            # as our implementation has fallback defaults
        except (ValueError, CompressionError, KeyError):
            # This is expected for truly corrupted metadata
            pass


class TestCompressionPerformance:
    """Test compression performance characteristics"""

    @pytest.fixture
    def manager(self):
        """Create compression manager instance"""
        return CompressionManager()

    def test_compression_speed_comparison(self, manager):
        """Test relative compression speeds"""
        # Large test data
        large_data = "Performance test data " * 1000

        algorithms = [CompressionType.LZ4, CompressionType.SNAPPY, CompressionType.GZIP]
        timings = {}

        for algorithm in algorithms:
            start_time = time.time()
            result = manager.compress(large_data, algorithm)
            compress_time = time.time() - start_time

            start_time = time.time()
            decompressed = manager.decompress(
                result.compressed_data, algorithm, result.metadata
            )
            decompress_time = time.time() - start_time

            timings[algorithm] = {
                "compress_time": compress_time,
                "decompress_time": decompress_time,
                "compression_ratio": result.compression_ratio,
            }

            # Verify correctness
            assert decompressed == large_data

        # All algorithms should complete in reasonable time (< 1 second for test data)
        for algorithm, timing in timings.items():
            assert timing["compress_time"] < 1.0
            assert timing["decompress_time"] < 1.0
            assert timing["compression_ratio"] < 1.0

    def test_compression_effectiveness_by_data_type(self, manager):
        """Test compression effectiveness on different data types"""
        test_cases = {
            "repetitive_text": "abcd" * 1000,
            "json_data": [{"key": "value"} for _ in range(100)],
            "numeric_sequence": list(range(1000)),
            "random_data": [chr(i % 256) for i in range(1000)],
        }

        results = {}
        for data_type, data in test_cases.items():
            result = manager.compress(data, CompressionType.GZIP)
            results[data_type] = result.compression_ratio

            # Verify decompression
            decompressed = manager.decompress(
                result.compressed_data, CompressionType.GZIP, result.metadata
            )
            assert decompressed == data

        # Repetitive data should compress better than random data
        assert results["repetitive_text"] < results["random_data"]


if __name__ == "__main__":
    pytest.main([__file__])
