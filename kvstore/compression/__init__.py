"""
Advanced compression codecs for HyperKV
Supports multiple compression algorithms including specialized time-series compression

This module provides a comprehensive compression framework with:
- Multiple compression algorithms (LZ4, Snappy, GZIP, ZLIB, BZIP2)
- Specialized codecs for time-series and sequential data
- Adaptive compression selection
- Compression level control
- Thread-safe operations
- Performance monitoring and statistics
- Streaming compression for large datasets
"""

import json
import pickle
import zlib
import gzip
import bz2
import time
import threading
import struct
import logging
from enum import Enum
from typing import Any, Dict, Optional, Union, List, Iterator, Tuple, Callable
from dataclasses import dataclass, field
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
import hashlib

# Try to import optional compression libraries
try:
    import lz4.frame
    import lz4.block

    LZ4_AVAILABLE = True
except ImportError:
    LZ4_AVAILABLE = False

try:
    import snappy

    SNAPPY_AVAILABLE = True
except ImportError:
    SNAPPY_AVAILABLE = False

# Try to import additional compression libraries
try:
    import zstandard as zstd

    ZSTD_AVAILABLE = True
except ImportError:
    ZSTD_AVAILABLE = False

try:
    import blosc

    BLOSC_AVAILABLE = True
except ImportError:
    BLOSC_AVAILABLE = False

logger = logging.getLogger(__name__)


class CompressionType(Enum):
    """Available compression algorithms"""

    LZ4 = "lz4"
    LZ4_BLOCK = "lz4_block"
    SNAPPY = "snappy"
    GZIP = "gzip"
    ZLIB = "zlib"
    BZIP2 = "bzip2"
    ZSTD = "zstd"
    BLOSC = "blosc"
    GORILLA = "gorilla"
    DELTA = "delta"
    RLE = "rle"  # Run-Length Encoding
    ADAPTIVE = "adaptive"  # Adaptive algorithm selection


@dataclass
class CompressionResult:
    """Result of compression operation"""

    compressed_data: bytes
    original_size: int
    compressed_size: int
    algorithm: CompressionType
    metadata: Dict[str, Any] = field(default_factory=dict)
    compression_time: float = 0.0
    checksum: Optional[str] = None

    @property
    def compression_ratio(self) -> float:
        """Calculate compression ratio (compressed/original)"""
        if self.original_size == 0:
            return 0.0
        return self.compressed_size / self.original_size

    @property
    def compression_percentage(self) -> float:
        """Calculate compression percentage (space saved)"""
        return (1.0 - self.compression_ratio) * 100.0

    @property
    def space_saved(self) -> int:
        """Calculate bytes saved through compression"""
        return self.original_size - self.compressed_size


@dataclass
class CompressionConfig:
    """Configuration for compression operations"""

    level: int = 1  # Compression level (1-9, algorithm dependent)
    use_checksum: bool = True
    enable_streaming: bool = False
    chunk_size: int = 64 * 1024  # 64KB chunks for streaming
    max_threads: int = 1
    adaptive_threshold: int = 1024  # Size threshold for adaptive compression
    prefer_speed: bool = True  # Prefer speed over compression ratio


class CompressionError(Exception):
    """Compression-related error"""

    pass


class CompressionStrategy(Enum):
    """Compression strategy selection"""

    SPEED = "speed"  # Prioritize compression/decompression speed
    RATIO = "ratio"  # Prioritize compression ratio
    BALANCED = "balanced"  # Balance between speed and ratio
    ADAPTIVE = "adaptive"  # Adapt based on data characteristics


class CompressionManager:
    """
    Advanced compression manager with multi-algorithm support,
    adaptive selection, and performance monitoring.
    """

    def __init__(self, config: Optional[CompressionConfig] = None):
        self.config = config or CompressionConfig()
        self.stats = {
            "compressions": 0,
            "decompressions": 0,
            "total_original_bytes": 0,
            "total_compressed_bytes": 0,
            "compression_times": [],
            "decompression_times": [],
            "algorithm_usage": {},
            "errors": 0,
        }
        self._lock = threading.RLock()

        # Thread pool for parallel operations
        if self.config.max_threads > 1:
            self.executor = ThreadPoolExecutor(max_workers=self.config.max_threads)
        else:
            self.executor = None

        # Algorithm performance cache
        self._algorithm_performance = {}

        # Initialize algorithm availability
        self._available_algorithms = self._detect_available_algorithms()

    def _detect_available_algorithms(self) -> Dict[CompressionType, bool]:
        """Detect which compression algorithms are available"""
        algorithms = {
            CompressionType.GZIP: True,  # Always available (stdlib)
            CompressionType.ZLIB: True,  # Always available (stdlib)
            CompressionType.BZIP2: True,  # Always available (stdlib)
            CompressionType.LZ4: LZ4_AVAILABLE,
            CompressionType.LZ4_BLOCK: LZ4_AVAILABLE,
            CompressionType.SNAPPY: SNAPPY_AVAILABLE,
            CompressionType.ZSTD: ZSTD_AVAILABLE,
            CompressionType.BLOSC: BLOSC_AVAILABLE,
            CompressionType.GORILLA: True,  # Custom implementation
            CompressionType.DELTA: True,  # Custom implementation
            CompressionType.RLE: True,  # Custom implementation
        }

        logger.info(
            f"Available compression algorithms: {[k.value for k, v in algorithms.items() if v]}"
        )
        return algorithms

    def _calculate_checksum(self, data: bytes) -> str:
        """Calculate MD5 checksum for data integrity"""
        return hashlib.md5(data).hexdigest()

    def _serialize_data(self, data: Any) -> Tuple[bytes, Dict[str, Any]]:
        """Serialize data with metadata about the serialization method"""
        metadata = {}

        if isinstance(data, bytes):
            serialized_data = data
            metadata["type"] = "bytes"
        elif isinstance(data, str):
            serialized_data = data.encode("utf-8")
            metadata["type"] = "string"
        elif isinstance(data, (int, float)):
            serialized_data = struct.pack(
                f"{'d' if isinstance(data, float) else 'q'}", data
            )
            metadata["type"] = "numeric"
            metadata["numeric_type"] = "float" if isinstance(data, float) else "int"
        elif isinstance(data, (list, tuple)):
            # Check if it's a time series (list of tuples with 2 elements)
            if data and all(
                isinstance(item, (list, tuple)) and len(item) == 2 for item in data
            ):
                # This looks like time series data, use JSON if possible
                try:
                    json_str = json.dumps(data)
                    serialized_data = json_str.encode("utf-8")
                    metadata["type"] = "json"
                    metadata["subtype"] = "time_series"
                except (TypeError, ValueError):
                    # Fallback to pickle
                    serialized_data = pickle.dumps(data)
                    metadata["type"] = "pickle"
                    metadata["subtype"] = "time_series"
            else:
                # Try JSON first for simple structures
                try:
                    json_str = json.dumps(data)
                    serialized_data = json_str.encode("utf-8")
                    metadata["type"] = "json"
                except (TypeError, ValueError):
                    # Fallback to pickle
                    serialized_data = pickle.dumps(data)
                    metadata["type"] = "pickle"
        elif isinstance(data, dict):
            # Try JSON first
            try:
                json_str = json.dumps(data)
                serialized_data = json_str.encode("utf-8")
                metadata["type"] = "json"
            except (TypeError, ValueError):
                # Fallback to pickle
                serialized_data = pickle.dumps(data)
                metadata["type"] = "pickle"
        else:
            # Use pickle for complex objects
            serialized_data = pickle.dumps(data)
            metadata["type"] = "pickle"

        return serialized_data, metadata

    def _deserialize_data(self, data: bytes, metadata: Dict[str, Any]) -> Any:
        """Deserialize data based on metadata"""
        data_type = metadata.get("type", "bytes")

        if data_type == "bytes":
            return data
        elif data_type == "string":
            return data.decode("utf-8")
        elif data_type == "numeric":
            numeric_type = metadata.get("numeric_type", "int")
            if numeric_type == "float":
                return struct.unpack("d", data)[0]
            else:
                return struct.unpack("q", data)[0]
        elif data_type == "json":
            json_str = data.decode("utf-8")
            return json.loads(json_str)
        elif data_type == "pickle":
            return pickle.loads(data)
        else:
            # Default fallback
            try:
                return data.decode("utf-8")
            except UnicodeDecodeError:
                return data

    def _select_adaptive_algorithm(
        self, data_size: int, data_type: str, metadata: Dict[str, Any] = None
    ) -> CompressionType:
        """Select the best compression algorithm based on data characteristics"""
        metadata = metadata or {}

        # Check for special data patterns
        if metadata.get("subtype") == "time_series":
            # Time series data works well with Gorilla compression
            return CompressionType.GORILLA

        # For small data, use fast algorithms
        if data_size < self.config.adaptive_threshold:
            if self._available_algorithms.get(CompressionType.LZ4, False):
                return CompressionType.LZ4
            elif self._available_algorithms.get(CompressionType.SNAPPY, False):
                return CompressionType.SNAPPY
            else:
                return CompressionType.ZLIB

        # For larger data, consider data type
        if data_type == "json" or data_type == "string":
            # Text data compresses well with GZIP/ZSTD
            if self._available_algorithms.get(CompressionType.ZSTD, False):
                return CompressionType.ZSTD
            else:
                return CompressionType.GZIP
        elif data_type == "numeric":
            # Numeric data might benefit from specialized compression
            if self._available_algorithms.get(CompressionType.BLOSC, False):
                return CompressionType.BLOSC
            elif self._available_algorithms.get(CompressionType.LZ4, False):
                return CompressionType.LZ4
            else:
                return CompressionType.ZLIB
        else:
            # General purpose compression
            if self.config.prefer_speed:
                if self._available_algorithms.get(CompressionType.LZ4, False):
                    return CompressionType.LZ4
                elif self._available_algorithms.get(CompressionType.SNAPPY, False):
                    return CompressionType.SNAPPY
                else:
                    return CompressionType.ZLIB
            else:
                if self._available_algorithms.get(CompressionType.ZSTD, False):
                    return CompressionType.ZSTD
                else:
                    return CompressionType.GZIP

    def _compress_with_algorithm(
        self, data: bytes, algorithm: CompressionType
    ) -> bytes:
        """Compress data with specific algorithm"""
        level = self.config.level

        if algorithm == CompressionType.LZ4:
            if not self._available_algorithms[CompressionType.LZ4]:
                raise CompressionError("LZ4 not available")
            return lz4.frame.compress(data, compression_level=min(level, 12))

        elif algorithm == CompressionType.LZ4_BLOCK:
            if not self._available_algorithms[CompressionType.LZ4_BLOCK]:
                raise CompressionError("LZ4 block compression not available")
            return lz4.block.compress(data)

        elif algorithm == CompressionType.SNAPPY:
            if not self._available_algorithms[CompressionType.SNAPPY]:
                raise CompressionError("Snappy not available")
            return snappy.compress(data)

        elif algorithm == CompressionType.GZIP:
            return gzip.compress(data, compresslevel=min(level, 9))

        elif algorithm == CompressionType.ZLIB:
            return zlib.compress(data, level=min(level, 9))

        elif algorithm == CompressionType.BZIP2:
            return bz2.compress(data, compresslevel=min(level, 9))

        elif algorithm == CompressionType.ZSTD:
            if not self._available_algorithms[CompressionType.ZSTD]:
                raise CompressionError("Zstandard not available")
            cctx = zstd.ZstdCompressor(level=min(level, 22))
            return cctx.compress(data)

        elif algorithm == CompressionType.BLOSC:
            if not self._available_algorithms[CompressionType.BLOSC]:
                raise CompressionError("Blosc not available")
            return blosc.compress(data, typesize=8, clevel=min(level, 9))

        else:
            raise CompressionError(f"Unsupported compression algorithm: {algorithm}")

    def _decompress_with_algorithm(
        self, data: bytes, algorithm: CompressionType
    ) -> bytes:
        """Decompress data with specific algorithm"""
        if algorithm == CompressionType.LZ4:
            if not self._available_algorithms[CompressionType.LZ4]:
                raise CompressionError("LZ4 not available")
            return lz4.frame.decompress(data)

        elif algorithm == CompressionType.LZ4_BLOCK:
            if not self._available_algorithms[CompressionType.LZ4_BLOCK]:
                raise CompressionError("LZ4 block compression not available")
            return lz4.block.decompress(data)

        elif algorithm == CompressionType.SNAPPY:
            if not self._available_algorithms[CompressionType.SNAPPY]:
                raise CompressionError("Snappy not available")
            return snappy.decompress(data)

        elif algorithm == CompressionType.GZIP:
            return gzip.decompress(data)

        elif algorithm == CompressionType.ZLIB:
            return zlib.decompress(data)

        elif algorithm == CompressionType.BZIP2:
            return bz2.decompress(data)

        elif algorithm == CompressionType.ZSTD:
            if not self._available_algorithms[CompressionType.ZSTD]:
                raise CompressionError("Zstandard not available")
            dctx = zstd.ZstdDecompressor()
            return dctx.decompress(data)

        elif algorithm == CompressionType.BLOSC:
            if not self._available_algorithms[CompressionType.BLOSC]:
                raise CompressionError("Blosc not available")
            return blosc.decompress(data)

        else:
            raise CompressionError(f"Unsupported decompression algorithm: {algorithm}")

    def compress(
        self, data: Any, algorithm: Optional[CompressionType] = None
    ) -> CompressionResult:
        """
        Compress data using specified or adaptive algorithm selection.

        Args:
            data: Data to compress (any serializable type)
            algorithm: Compression algorithm to use (None for adaptive selection)

        Returns:
            CompressionResult with compressed data and metadata
        """
        start_time = time.time()

        try:
            with self._lock:
                # Serialize data
                serialized_data, serialization_metadata = self._serialize_data(data)
                original_size = len(serialized_data)

                # Select algorithm if not specified
                if algorithm is None or algorithm == CompressionType.ADAPTIVE:
                    algorithm = self._select_adaptive_algorithm(
                        original_size,
                        serialization_metadata.get("type", "unknown"),
                        serialization_metadata,
                    )

                # Handle special algorithms
                if algorithm == CompressionType.GORILLA:
                    codec = GorillaCodec()
                    return codec.compress(data)
                elif algorithm == CompressionType.DELTA:
                    codec = DeltaCodec()
                    return codec.compress(data)
                elif algorithm == CompressionType.RLE:
                    codec = RLECodec()
                    return codec.compress(data)

                # Check algorithm availability
                if not self._available_algorithms.get(algorithm, False):
                    # Fallback to available algorithm
                    fallback_algorithm = self._select_adaptive_algorithm(
                        original_size, serialization_metadata.get("type", "unknown")
                    )
                    logger.warning(
                        f"Algorithm {algorithm.value} not available, using {fallback_algorithm.value}"
                    )
                    algorithm = fallback_algorithm

                # Compress data
                compressed_data = self._compress_with_algorithm(
                    serialized_data, algorithm
                )
                compression_time = time.time() - start_time

                # Calculate checksum if enabled
                checksum = None
                if self.config.use_checksum:
                    checksum = self._calculate_checksum(serialized_data)

                # Create metadata
                metadata = serialization_metadata.copy()
                metadata.update(
                    {
                        "algorithm": algorithm.value,
                        "compression_level": self.config.level,
                        "compressed_at": time.time(),
                    }
                )

                if checksum:
                    metadata["checksum"] = checksum

                # Update statistics
                self.stats["compressions"] += 1
                self.stats["total_original_bytes"] += original_size
                self.stats["total_compressed_bytes"] += len(compressed_data)
                self.stats["compression_times"].append(compression_time)
                self.stats["algorithm_usage"][algorithm.value] = (
                    self.stats["algorithm_usage"].get(algorithm.value, 0) + 1
                )

                return CompressionResult(
                    compressed_data=compressed_data,
                    original_size=original_size,
                    compressed_size=len(compressed_data),
                    algorithm=algorithm,
                    metadata=metadata,
                    compression_time=compression_time,
                    checksum=checksum,
                )

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Compression failed: {e}")
            raise CompressionError(f"Compression failed: {e}") from e

    def decompress(
        self,
        compressed_data: bytes,
        algorithm: CompressionType,
        metadata: Dict[str, Any],
    ) -> Any:
        """
        Decompress data using specified algorithm.

        Args:
            compressed_data: Compressed data bytes
            algorithm: Algorithm used for compression
            metadata: Compression metadata

        Returns:
            Original decompressed data
        """
        start_time = time.time()

        try:
            with self._lock:
                # Handle special algorithms
                if algorithm == CompressionType.GORILLA:
                    codec = GorillaCodec()
                    return codec.decompress(compressed_data, metadata)
                elif algorithm == CompressionType.DELTA:
                    codec = DeltaCodec()
                    return codec.decompress(compressed_data, metadata)
                elif algorithm == CompressionType.RLE:
                    codec = RLECodec()
                    return codec.decompress(compressed_data, metadata)

                # Handle fallback algorithms
                actual_algorithm = algorithm
                if metadata.get("fallback"):
                    fallback_algo = metadata["fallback"]
                    actual_algorithm = CompressionType(fallback_algo)

                # Decompress data
                decompressed_data = self._decompress_with_algorithm(
                    compressed_data, actual_algorithm
                )

                # Verify checksum if present
                if self.config.use_checksum and "checksum" in metadata:
                    calculated_checksum = self._calculate_checksum(decompressed_data)
                    if calculated_checksum != metadata["checksum"]:
                        raise CompressionError("Checksum verification failed")

                # Deserialize data
                result = self._deserialize_data(decompressed_data, metadata)

                decompression_time = time.time() - start_time

                # Update statistics
                self.stats["decompressions"] += 1
                self.stats["decompression_times"].append(decompression_time)

                return result

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Decompression failed: {e}")
            raise CompressionError(f"Decompression failed: {e}") from e

    def compress_stream(
        self, data_iterator: Iterator[Any], algorithm: Optional[CompressionType] = None
    ) -> Iterator[CompressionResult]:
        """
        Compress data from an iterator in streaming fashion.

        Args:
            data_iterator: Iterator yielding data chunks
            algorithm: Compression algorithm to use

        Yields:
            CompressionResult for each chunk
        """
        for chunk in data_iterator:
            yield self.compress(chunk, algorithm)

    def decompress_stream(
        self, compressed_iterator: Iterator[Tuple[bytes, CompressionType, Dict]]
    ) -> Iterator[Any]:
        """
        Decompress data from an iterator in streaming fashion.

        Args:
            compressed_iterator: Iterator yielding (compressed_data, algorithm, metadata) tuples

        Yields:
            Decompressed data chunks
        """
        for compressed_data, algorithm, metadata in compressed_iterator:
            yield self.decompress(compressed_data, algorithm, metadata)

    def benchmark_algorithms(
        self, test_data: Any, algorithms: Optional[List[CompressionType]] = None
    ) -> Dict[CompressionType, Dict[str, float]]:
        """
        Benchmark different compression algorithms on test data.

        Args:
            test_data: Data to use for benchmarking
            algorithms: List of algorithms to test (None for all available)

        Returns:
            Dictionary mapping algorithms to performance metrics
        """
        if algorithms is None:
            algorithms = [
                algo
                for algo, available in self._available_algorithms.items()
                if available
            ]

        results = {}

        for algorithm in algorithms:
            try:
                # Skip special algorithms for now
                if algorithm in [
                    CompressionType.GORILLA,
                    CompressionType.DELTA,
                    CompressionType.RLE,
                ]:
                    continue

                # Compress
                start_time = time.time()
                result = self.compress(test_data, algorithm)
                compress_time = time.time() - start_time

                # Decompress
                start_time = time.time()
                decompressed = self.decompress(
                    result.compressed_data, algorithm, result.metadata
                )
                decompress_time = time.time() - start_time

                # Verify correctness
                if decompressed != test_data:
                    logger.warning(
                        f"Algorithm {algorithm.value} failed correctness test"
                    )
                    continue

                results[algorithm] = {
                    "compression_ratio": result.compression_ratio,
                    "compression_time": compress_time,
                    "decompression_time": decompress_time,
                    "total_time": compress_time + decompress_time,
                    "compression_speed_mbps": (result.original_size / compress_time)
                    / (1024 * 1024),
                    "decompression_speed_mbps": (result.original_size / decompress_time)
                    / (1024 * 1024),
                }

            except Exception as e:
                logger.warning(f"Benchmark failed for {algorithm.value}: {e}")

        return results

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive compression statistics"""
        with self._lock:
            stats = self.stats.copy()

            # Calculate derived statistics
            if stats["total_original_bytes"] > 0:
                stats["overall_compression_ratio"] = (
                    stats["total_compressed_bytes"] / stats["total_original_bytes"]
                )
            else:
                stats["overall_compression_ratio"] = 0.0

            if stats["compression_times"]:
                stats["avg_compression_time"] = sum(stats["compression_times"]) / len(
                    stats["compression_times"]
                )
                stats["max_compression_time"] = max(stats["compression_times"])
                stats["min_compression_time"] = min(stats["compression_times"])
            else:
                stats["avg_compression_time"] = 0.0
                stats["max_compression_time"] = 0.0
                stats["min_compression_time"] = 0.0

            if stats["decompression_times"]:
                stats["avg_decompression_time"] = sum(
                    stats["decompression_times"]
                ) / len(stats["decompression_times"])
                stats["max_decompression_time"] = max(stats["decompression_times"])
                stats["min_decompression_time"] = min(stats["decompression_times"])
            else:
                stats["avg_decompression_time"] = 0.0
                stats["max_decompression_time"] = 0.0
                stats["min_decompression_time"] = 0.0

            stats["available_algorithms"] = [
                algo.value
                for algo, available in self._available_algorithms.items()
                if available
            ]

            return stats

    def reset_stats(self):
        """Reset all statistics"""
        with self._lock:
            self.stats = {
                "compressions": 0,
                "decompressions": 0,
                "total_original_bytes": 0,
                "total_compressed_bytes": 0,
                "compression_times": [],
                "decompression_times": [],
                "algorithm_usage": {},
                "errors": 0,
            }

    def __del__(self):
        """Cleanup resources"""
        if hasattr(self, "executor") and self.executor:
            self.executor.shutdown(wait=False)


class RLECodec:
    """Run-Length Encoding codec for repetitive data"""

    def __init__(self):
        pass

    def compress(self, data: Any) -> CompressionResult:
        """Compress data using Run-Length Encoding"""
        if isinstance(data, str):
            data_bytes = data.encode("utf-8")
            metadata = {"type": "string"}
        elif isinstance(data, (list, tuple)):
            # RLE for sequences
            return self._compress_sequence(data)
        else:
            # Convert to bytes for RLE
            data_bytes = pickle.dumps(data)
            metadata = {"type": "pickle"}

        # Perform RLE on bytes
        compressed = self._rle_compress_bytes(data_bytes)

        return CompressionResult(
            compressed_data=compressed,
            original_size=len(data_bytes),
            compressed_size=len(compressed),
            algorithm=CompressionType.RLE,
            metadata=metadata,
        )

    def _rle_compress_bytes(self, data: bytes) -> bytes:
        """Compress bytes using RLE"""
        if not data:
            return b""

        result = BytesIO()
        count = 1
        prev_byte = data[0]

        for byte in data[1:]:
            if byte == prev_byte and count < 255:
                count += 1
            else:
                # Write count and byte
                result.write(bytes([count, prev_byte]))
                count = 1
                prev_byte = byte

        # Write final run
        result.write(bytes([count, prev_byte]))
        return result.getvalue()

    def _compress_sequence(self, sequence: Union[List, Tuple]) -> CompressionResult:
        """Compress sequence using RLE"""
        if not sequence:
            return CompressionResult(
                compressed_data=b"",
                original_size=0,
                compressed_size=0,
                algorithm=CompressionType.RLE,
                metadata={"type": "sequence", "count": 0},
            )

        # RLE compress the sequence
        compressed_items = []
        count = 1
        prev_item = sequence[0]

        for item in sequence[1:]:
            if item == prev_item:
                count += 1
            else:
                compressed_items.append((count, prev_item))
                count = 1
                prev_item = item

        # Add final run
        compressed_items.append((count, prev_item))

        # Serialize compressed data
        serialized = pickle.dumps(compressed_items)

        return CompressionResult(
            compressed_data=serialized,
            original_size=len(pickle.dumps(sequence)),
            compressed_size=len(serialized),
            algorithm=CompressionType.RLE,
            metadata={"type": "sequence", "count": len(sequence)},
        )

    def decompress(self, compressed_data: bytes, metadata: Dict[str, Any]) -> Any:
        """Decompress RLE-compressed data"""
        if not compressed_data:
            return [] if metadata.get("type") == "sequence" else b""

        data_type = metadata.get("type", "bytes")

        if data_type == "sequence":
            # Decompress sequence
            compressed_items = pickle.loads(compressed_data)
            result = []
            for count, item in compressed_items:
                result.extend([item] * count)
            return result
        else:
            # Decompress bytes
            decompressed_bytes = self._rle_decompress_bytes(compressed_data)

            if data_type == "string":
                return decompressed_bytes.decode("utf-8")
            elif data_type == "pickle":
                return pickle.loads(decompressed_bytes)
            else:
                return decompressed_bytes

    def _rle_decompress_bytes(self, compressed_data: bytes) -> bytes:
        """Decompress RLE-compressed bytes"""
        if len(compressed_data) % 2 != 0:
            raise CompressionError("Invalid RLE compressed data")

        result = BytesIO()
        for i in range(0, len(compressed_data), 2):
            count = compressed_data[i]
            byte_value = compressed_data[i + 1]
            result.write(bytes([byte_value] * count))

        return result.getvalue()


class GorillaCodec:
    """
    Gorilla compression for time series data.
    Optimized for floating-point time series with temporal locality.
    """

    def __init__(self):
        pass

    def compress(
        self, time_series_data: Union[List[float], List[Tuple]]
    ) -> CompressionResult:
        """
        Compress time series data using Gorilla algorithm.

        Args:
            time_series_data: List of values or list of (timestamp, value) tuples
        """
        if not time_series_data:
            return CompressionResult(
                compressed_data=b"",
                original_size=0,
                compressed_size=0,
                algorithm=CompressionType.GORILLA,
                metadata={"count": 0, "type": "time_series"},
            )

        # Determine data format
        if (
            isinstance(time_series_data[0], (tuple, list))
            and len(time_series_data[0]) == 2
        ):
            # (timestamp, value) tuples
            timestamps = [item[0] for item in time_series_data]
            values = [item[1] for item in time_series_data]
            has_timestamps = True
        else:
            # Just values
            values = time_series_data
            timestamps = list(range(len(values)))
            has_timestamps = False

        # Compress timestamps and values separately
        compressed_timestamps = (
            self._compress_timestamps(timestamps) if has_timestamps else b""
        )
        compressed_values = self._compress_values(values)

        # Combine compressed data
        metadata = {
            "count": len(time_series_data),
            "type": "time_series",
            "has_timestamps": has_timestamps,
        }

        combined_data = (
            struct.pack("<I", len(compressed_timestamps))
            + compressed_timestamps
            + compressed_values
        )

        return CompressionResult(
            compressed_data=combined_data,
            original_size=len(pickle.dumps(time_series_data)),
            compressed_size=len(combined_data),
            algorithm=CompressionType.GORILLA,
            metadata=metadata,
        )

    def _compress_timestamps(self, timestamps: List[Union[int, float]]) -> bytes:
        """Compress timestamps using delta-of-delta encoding"""
        if len(timestamps) < 2:
            return pickle.dumps(timestamps)

        # Delta-of-delta compression
        deltas = []
        prev_delta = timestamps[1] - timestamps[0]

        for i in range(2, len(timestamps)):
            current_delta = timestamps[i] - timestamps[i - 1]
            delta_of_delta = current_delta - prev_delta
            deltas.append(delta_of_delta)
            prev_delta = current_delta

        compressed_data = {
            "first": timestamps[0],
            "first_delta": prev_delta,
            "deltas": deltas,
        }

        return gzip.compress(pickle.dumps(compressed_data))

    def _compress_values(self, values: List[float]) -> bytes:
        """Compress values using XOR-based compression"""
        if not values:
            return b""

        if len(values) == 1:
            return struct.pack("<d", values[0])

        # Convert to binary representation and use XOR compression
        result = BytesIO()

        # Store first value
        prev_bits = struct.unpack("<Q", struct.pack("<d", values[0]))[0]
        result.write(struct.pack("<Q", prev_bits))

        for value in values[1:]:
            # Convert to binary
            current_bits = struct.unpack("<Q", struct.pack("<d", value))[0]

            # XOR with previous value
            xor_result = prev_bits ^ current_bits

            if xor_result == 0:
                # Same value, write 0
                result.write(b"\x00")
            else:
                # Different value, write 1 followed by XOR result
                result.write(b"\x01")
                result.write(struct.pack("<Q", xor_result))

            prev_bits = current_bits

        return gzip.compress(result.getvalue())

    def decompress(
        self, compressed_data: bytes, metadata: Dict[str, Any]
    ) -> Union[List[float], List[Tuple]]:
        """Decompress Gorilla-compressed time series data"""
        if not compressed_data:
            return []

        has_timestamps = metadata.get("has_timestamps", False)

        # Extract timestamps and values
        timestamp_len = struct.unpack("<I", compressed_data[:4])[0]
        compressed_timestamps = compressed_data[4 : 4 + timestamp_len]
        compressed_values = compressed_data[4 + timestamp_len :]

        # Decompress
        timestamps = (
            self._decompress_timestamps(compressed_timestamps)
            if has_timestamps
            else None
        )
        values = self._decompress_values(compressed_values)

        # Combine results
        if has_timestamps and timestamps:
            result = list(zip(timestamps, values))
            # Convert to list of lists for consistent comparison
            return [list(item) for item in result]
        else:
            return values

    def _decompress_timestamps(self, compressed_data: bytes) -> List[Union[int, float]]:
        """Decompress delta-of-delta encoded timestamps"""
        if not compressed_data:
            return []

        try:
            data = pickle.loads(gzip.decompress(compressed_data))
            if not isinstance(data, dict):
                return pickle.loads(compressed_data)  # Fallback for simple format

            first = data["first"]
            first_delta = data["first_delta"]
            deltas = data["deltas"]

            # Reconstruct timestamps
            timestamps = [first, first + first_delta]
            current_delta = first_delta

            for delta_of_delta in deltas:
                current_delta += delta_of_delta
                next_timestamp = timestamps[-1] + current_delta
                timestamps.append(next_timestamp)

            return timestamps
        except:
            # Fallback to simple decompression
            return pickle.loads(compressed_data)

    def _decompress_values(self, compressed_data: bytes) -> List[float]:
        """Decompress XOR-compressed values"""
        if not compressed_data:
            return []

        try:
            data = gzip.decompress(compressed_data)
        except:
            # Single value case
            if len(compressed_data) == 8:
                return [struct.unpack("<d", compressed_data)[0]]
            return []

        if len(data) < 8:
            return []

        values = []
        offset = 0

        # Read first value
        prev_bits = struct.unpack("<Q", data[offset : offset + 8])[0]
        values.append(struct.unpack("<d", struct.pack("<Q", prev_bits))[0])
        offset += 8

        # Read subsequent values
        while offset < len(data):
            flag = data[offset]
            offset += 1

            if flag == 0:
                # Same value as previous
                values.append(values[-1])
            elif flag == 1 and offset + 8 <= len(data):
                # XOR-ed value
                xor_result = struct.unpack("<Q", data[offset : offset + 8])[0]
                offset += 8

                current_bits = prev_bits ^ xor_result
                value = struct.unpack("<d", struct.pack("<Q", current_bits))[0]
                values.append(value)
                prev_bits = current_bits
            else:
                break

        return values


class DeltaCodec:
    """
    Delta compression for sequential data.
    Optimized for data with predictable patterns and incremental changes.
    """

    def __init__(self):
        pass

    def compress(self, sequential_data: Union[List, Tuple]) -> CompressionResult:
        """Compress sequential data using delta encoding"""
        if not sequential_data:
            return CompressionResult(
                compressed_data=b"",
                original_size=0,
                compressed_size=0,
                algorithm=CompressionType.DELTA,
                metadata={"count": 0, "type": "sequence"},
            )

        # Analyze data type
        data_type = "mixed"
        if all(isinstance(x, (int, float)) for x in sequential_data):
            data_type = "numeric"
        elif all(isinstance(x, str) for x in sequential_data):
            data_type = "string"

        if data_type == "numeric":
            return self._compress_numeric_sequence(sequential_data)
        elif data_type == "string":
            return self._compress_string_sequence(sequential_data)
        else:
            return self._compress_mixed_sequence(sequential_data)

    def _compress_numeric_sequence(
        self, data: List[Union[int, float]]
    ) -> CompressionResult:
        """Compress numeric sequence using delta encoding"""
        if len(data) <= 1:
            serialized = pickle.dumps(data)
            return CompressionResult(
                compressed_data=gzip.compress(serialized),
                original_size=len(serialized),
                compressed_size=len(gzip.compress(serialized)),
                algorithm=CompressionType.DELTA,
                metadata={"count": len(data), "type": "numeric_simple"},
            )

        # Multi-level delta encoding
        base_value = data[0]
        first_deltas = []

        # First level deltas
        for i in range(1, len(data)):
            first_deltas.append(data[i] - data[i - 1])

        # Second level deltas (delta of deltas)
        second_deltas = []
        if len(first_deltas) > 1:
            for i in range(1, len(first_deltas)):
                second_deltas.append(first_deltas[i] - first_deltas[i - 1])

        compressed_data = {
            "base": base_value,
            "first_delta": first_deltas[0] if first_deltas else 0,
            "second_deltas": second_deltas,
            "type": "numeric_delta",
        }

        serialized = pickle.dumps(compressed_data)
        compressed = gzip.compress(serialized)

        return CompressionResult(
            compressed_data=compressed,
            original_size=len(pickle.dumps(data)),
            compressed_size=len(compressed),
            algorithm=CompressionType.DELTA,
            metadata={"count": len(data), "type": "numeric_delta"},
        )

    def _compress_string_sequence(self, data: List[str]) -> CompressionResult:
        """Compress string sequence using common prefix/suffix extraction"""
        if len(data) <= 1:
            serialized = pickle.dumps(data)
            return CompressionResult(
                compressed_data=gzip.compress(serialized),
                original_size=len(serialized),
                compressed_size=len(gzip.compress(serialized)),
                algorithm=CompressionType.DELTA,
                metadata={"count": len(data), "type": "string_simple"},
            )

        # Find common prefixes and suffixes
        common_prefix = ""
        common_suffix = ""

        # Find longest common prefix
        if data:
            common_prefix = data[0]
            for string in data[1:]:
                new_prefix = ""
                for i, (c1, c2) in enumerate(zip(common_prefix, string)):
                    if c1 == c2:
                        new_prefix += c1
                    else:
                        break
                common_prefix = new_prefix
                if not common_prefix:
                    break

        # Extract middle parts after removing common prefix
        middle_parts = []
        for string in data:
            if string.startswith(common_prefix):
                middle_parts.append(string[len(common_prefix) :])
            else:
                middle_parts.append(string)

        compressed_data = {
            "common_prefix": common_prefix,
            "middle_parts": middle_parts,
            "type": "string_delta",
        }

        serialized = pickle.dumps(compressed_data)
        compressed = gzip.compress(serialized)

        return CompressionResult(
            compressed_data=compressed,
            original_size=len(pickle.dumps(data)),
            compressed_size=len(compressed),
            algorithm=CompressionType.DELTA,
            metadata={"count": len(data), "type": "string_delta"},
        )

    def _compress_mixed_sequence(self, data: List[Any]) -> CompressionResult:
        """Compress mixed-type sequence using general delta encoding"""
        # For mixed types, use a simpler approach
        compressed_items = []
        prev_item = None

        for item in data:
            if prev_item is None:
                compressed_items.append(("full", item))
            else:
                # Try to find delta
                if isinstance(item, type(prev_item)):
                    if isinstance(item, (int, float)) and isinstance(
                        prev_item, (int, float)
                    ):
                        delta = item - prev_item
                        compressed_items.append(("delta", delta))
                    elif isinstance(item, str) and isinstance(prev_item, str):
                        # Simple string delta (not very effective, but consistent)
                        compressed_items.append(("full", item))
                    else:
                        compressed_items.append(("full", item))
                else:
                    compressed_items.append(("full", item))
            prev_item = item

        compressed_data = {"items": compressed_items, "type": "mixed_delta"}

        serialized = pickle.dumps(compressed_data)
        compressed = gzip.compress(serialized)

        return CompressionResult(
            compressed_data=compressed,
            original_size=len(pickle.dumps(data)),
            compressed_size=len(compressed),
            algorithm=CompressionType.DELTA,
            metadata={"count": len(data), "type": "mixed_delta"},
        )

    def decompress(self, compressed_data: bytes, metadata: Dict[str, Any]) -> List[Any]:
        """Decompress delta-compressed data"""
        if not compressed_data:
            return []

        delta_type = metadata.get("type", "unknown")

        try:
            # Decompress and deserialize
            decompressed = gzip.decompress(compressed_data)
            data = pickle.loads(decompressed)

            if delta_type == "numeric_simple" or delta_type == "string_simple":
                return data
            elif delta_type == "numeric_delta":
                return self._decompress_numeric_delta(data)
            elif delta_type == "string_delta":
                return self._decompress_string_delta(data)
            elif delta_type == "mixed_delta":
                return self._decompress_mixed_delta(data)
            else:
                # Fallback
                return data if isinstance(data, list) else []

        except Exception as e:
            logger.error(f"Delta decompression failed: {e}")
            raise CompressionError(f"Delta decompression failed: {e}")

    def _decompress_numeric_delta(
        self, data: Dict[str, Any]
    ) -> List[Union[int, float]]:
        """Decompress numeric delta-encoded data"""
        base = data["base"]
        first_delta = data["first_delta"]
        second_deltas = data["second_deltas"]

        result = [base]

        if first_delta is not None:
            result.append(base + first_delta)
            current_delta = first_delta

            # Reconstruct from second deltas
            for second_delta in second_deltas:
                current_delta += second_delta
                next_value = result[-1] + current_delta
                result.append(next_value)

        return result

    def _decompress_string_delta(self, data: Dict[str, Any]) -> List[str]:
        """Decompress string delta-encoded data"""
        common_prefix = data["common_prefix"]
        middle_parts = data["middle_parts"]

        result = []
        for middle in middle_parts:
            result.append(common_prefix + middle)

        return result

    def _decompress_mixed_delta(self, data: Dict[str, Any]) -> List[Any]:
        """Decompress mixed-type delta-encoded data"""
        items = data["items"]
        result = []

        for item_type, value in items:
            if item_type == "full":
                result.append(value)
            elif item_type == "delta":
                if (
                    result
                    and isinstance(result[-1], (int, float))
                    and isinstance(value, (int, float))
                ):
                    result.append(result[-1] + value)
                else:
                    result.append(value)  # Fallback

        return result


# Utility functions for easy usage


def create_compression_manager(
    level: int = 1,
    use_checksum: bool = True,
    prefer_speed: bool = True,
    max_threads: int = 1,
) -> CompressionManager:
    """Create a compression manager with specified configuration"""
    config = CompressionConfig(
        level=level,
        use_checksum=use_checksum,
        prefer_speed=prefer_speed,
        max_threads=max_threads,
    )
    return CompressionManager(config)


def compress_data(
    data: Any, algorithm: Optional[CompressionType] = None
) -> CompressionResult:
    """Compress data using a default compression manager"""
    manager = create_compression_manager()
    return manager.compress(data, algorithm)


def decompress_data(
    compressed_data: bytes, algorithm: CompressionType, metadata: Dict[str, Any]
) -> Any:
    """Decompress data using a default compression manager"""
    manager = create_compression_manager()
    return manager.decompress(compressed_data, algorithm, metadata)


def benchmark_compression(
    test_data: Any, algorithms: Optional[List[CompressionType]] = None
) -> Dict[CompressionType, Dict[str, float]]:
    """Benchmark compression algorithms on test data"""
    manager = create_compression_manager()
    return manager.benchmark_algorithms(test_data, algorithms)


def detect_best_algorithm(
    test_data: Any, strategy: CompressionStrategy = CompressionStrategy.BALANCED
) -> CompressionType:
    """
    Detect the best compression algorithm for given data based on strategy.

    Args:
        test_data: Sample data to test
        strategy: Optimization strategy (speed, ratio, balanced, adaptive)

    Returns:
        Best compression algorithm for the data
    """
    manager = create_compression_manager()
    results = manager.benchmark_algorithms(test_data)

    if not results:
        return CompressionType.GZIP  # Fallback

    if strategy == CompressionStrategy.SPEED:
        # Prefer fastest compression
        best_algo = min(results.keys(), key=lambda x: results[x]["compression_time"])
    elif strategy == CompressionStrategy.RATIO:
        # Prefer best compression ratio
        best_algo = min(results.keys(), key=lambda x: results[x]["compression_ratio"])
    elif strategy == CompressionStrategy.BALANCED:
        # Balance compression ratio and speed
        def score(algo):
            metrics = results[algo]
            # Normalize scores (lower is better for both ratio and time)
            ratio_score = metrics["compression_ratio"]
            time_score = metrics["total_time"]
            return ratio_score + (time_score * 0.1)  # Weight time less than ratio

        best_algo = min(results.keys(), key=score)
    else:  # ADAPTIVE
        # Use adaptive selection based on data characteristics
        data_size = len(pickle.dumps(test_data))
        if data_size < 1024:
            best_algo = min(
                results.keys(), key=lambda x: results[x]["compression_time"]
            )
        else:
            best_algo = min(
                results.keys(), key=lambda x: results[x]["compression_ratio"]
            )

    return best_algo


# Export all public classes and functions
__all__ = [
    # Enums
    "CompressionType",
    "CompressionStrategy",
    # Classes
    "CompressionResult",
    "CompressionConfig",
    "CompressionManager",
    "CompressionError",
    "GorillaCodec",
    "DeltaCodec",
    "RLECodec",
    # Utility functions
    "create_compression_manager",
    "compress_data",
    "decompress_data",
    "benchmark_compression",
    "detect_best_algorithm",
]
