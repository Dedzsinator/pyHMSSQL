"""
Advanced Compression Codecs for HyperKV
Implements Gorilla compression for time series and delta compression
Optimized for different data types and usage patterns
"""

import struct
import time
import numpy as np
from typing import List, Tuple, Optional, Any, Union, Dict
from dataclasses import dataclass
from abc import ABC, abstractmethod
import zlib
import lz4.frame
import snappy
import logging
from enum import Enum

logger = logging.getLogger(__name__)


class CompressionType(Enum):
    """Available compression algorithms"""
    NONE = "none"
    GZIP = "gzip"
    LZ4 = "lz4"
    SNAPPY = "snappy"
    GORILLA = "gorilla"
    DELTA = "delta"
    DELTA_GORILLA = "delta_gorilla"
    RLE = "rle"  # Run-Length Encoding
    DICTIONARY = "dictionary"


@dataclass
class CompressionResult:
    """Result of compression operation"""
    compressed_data: bytes
    original_size: int
    compressed_size: int
    algorithm: CompressionType
    metadata: Dict[str, Any]
    
    @property
    def compression_ratio(self) -> float:
        """Calculate compression ratio"""
        if self.original_size == 0:
            return 0.0
        return self.compressed_size / self.original_size
    
    @property
    def space_saved(self) -> float:
        """Calculate space saved percentage"""
        if self.original_size == 0:
            return 0.0
        return (1.0 - self.compression_ratio) * 100.0


class CompressionCodec(ABC):
    """Abstract base class for compression codecs"""
    
    @abstractmethod
    def compress(self, data: Union[bytes, List[Any]]) -> CompressionResult:
        """Compress data"""
        pass
    
    @abstractmethod
    def decompress(self, compressed_data: bytes, metadata: Dict[str, Any]) -> Union[bytes, List[Any]]:
        """Decompress data"""
        pass
    
    @property
    @abstractmethod
    def compression_type(self) -> CompressionType:
        """Get compression type"""
        pass


class GZipCodec(CompressionCodec):
    """GZIP compression codec"""
    
    def __init__(self, level: int = 6):
        self.level = level
    
    def compress(self, data: Union[bytes, List[Any]]) -> CompressionResult:
        if isinstance(data, list):
            data = str(data).encode('utf-8')
        elif not isinstance(data, bytes):
            data = str(data).encode('utf-8')
        
        compressed = zlib.compress(data, self.level)
        
        return CompressionResult(
            compressed_data=compressed,
            original_size=len(data),
            compressed_size=len(compressed),
            algorithm=CompressionType.GZIP,
            metadata={'level': self.level}
        )
    
    def decompress(self, compressed_data: bytes, metadata: Dict[str, Any]) -> bytes:
        return zlib.decompress(compressed_data)
    
    @property
    def compression_type(self) -> CompressionType:
        return CompressionType.GZIP


class LZ4Codec(CompressionCodec):
    """LZ4 compression codec"""
    
    def __init__(self, compression_level: int = 0):
        self.compression_level = compression_level
    
    def compress(self, data: Union[bytes, List[Any]]) -> CompressionResult:
        if isinstance(data, list):
            data = str(data).encode('utf-8')
        elif not isinstance(data, bytes):
            data = str(data).encode('utf-8')
        
        compressed = lz4.frame.compress(
            data, 
            compression_level=self.compression_level
        )
        
        return CompressionResult(
            compressed_data=compressed,
            original_size=len(data),
            compressed_size=len(compressed),
            algorithm=CompressionType.LZ4,
            metadata={'level': self.compression_level}
        )
    
    def decompress(self, compressed_data: bytes, metadata: Dict[str, Any]) -> bytes:
        return lz4.frame.decompress(compressed_data)
    
    @property
    def compression_type(self) -> CompressionType:
        return CompressionType.LZ4


class SnappyCodec(CompressionCodec):
    """Snappy compression codec"""
    
    def compress(self, data: Union[bytes, List[Any]]) -> CompressionResult:
        if isinstance(data, list):
            data = str(data).encode('utf-8')
        elif not isinstance(data, bytes):
            data = str(data).encode('utf-8')
        
        compressed = snappy.compress(data)
        
        return CompressionResult(
            compressed_data=compressed,
            original_size=len(data),
            compressed_size=len(compressed),
            algorithm=CompressionType.SNAPPY,
            metadata={}
        )
    
    def decompress(self, compressed_data: bytes, metadata: Dict[str, Any]) -> bytes:
        return snappy.decompress(compressed_data)
    
    @property
    def compression_type(self) -> CompressionType:
        return CompressionType.SNAPPY


class GorillaCodec(CompressionCodec):
    """
    Gorilla compression codec for time series data
    Optimized for timestamp-value pairs with temporal locality
    """
    
    def __init__(self):
        self.LEADING_ZEROS_BITS = 5
        self.BLOCK_SIZE_BITS = 6
    
    def compress(self, data: Union[bytes, List[Any]]) -> CompressionResult:
        """Compress time series data using Gorilla algorithm"""
        if isinstance(data, bytes):
            # Assume it's a list of (timestamp, value) pairs
            import pickle
            data = pickle.loads(data)
        
        if not isinstance(data, list) or len(data) == 0:
            return CompressionResult(
                compressed_data=b'',
                original_size=0,
                compressed_size=0,
                algorithm=CompressionType.GORILLA,
                metadata={'count': 0}
            )
        
        # Separate timestamps and values
        if isinstance(data[0], (tuple, list)) and len(data[0]) == 2:
            timestamps = [item[0] for item in data]
            values = [item[1] for item in data]
        else:
            # Assume it's a list of values with implicit timestamps
            timestamps = list(range(len(data)))
            values = data
        
        # Compress timestamps and values separately
        compressed_timestamps = self._compress_timestamps(timestamps)
        compressed_values = self._compress_values(values)
        
        # Combine results
        metadata = {
            'count': len(data),
            'timestamp_size': len(compressed_timestamps),
            'values_size': len(compressed_values),
            'has_timestamps': len(timestamps) > 0
        }
        
        # Pack: metadata_len(4) + metadata + timestamps + values
        import json
        metadata_bytes = json.dumps(metadata).encode('utf-8')
        
        result_data = (
            struct.pack('>I', len(metadata_bytes)) +
            metadata_bytes +
            compressed_timestamps +
            compressed_values
        )
        
        original_size = len(str(data).encode('utf-8'))
        
        return CompressionResult(
            compressed_data=result_data,
            original_size=original_size,
            compressed_size=len(result_data),
            algorithm=CompressionType.GORILLA,
            metadata=metadata
        )
    
    def decompress(self, compressed_data: bytes, metadata: Dict[str, Any]) -> List[Tuple[int, float]]:
        """Decompress Gorilla-compressed data"""
        if len(compressed_data) == 0:
            return []
        
        # Unpack metadata
        metadata_len = struct.unpack('>I', compressed_data[:4])[0]
        import json
        stored_metadata = json.loads(compressed_data[4:4+metadata_len].decode('utf-8'))
        
        offset = 4 + metadata_len
        
        # Extract compressed parts
        timestamp_size = stored_metadata['timestamp_size']
        values_size = stored_metadata['values_size']
        
        compressed_timestamps = compressed_data[offset:offset+timestamp_size]
        compressed_values = compressed_data[offset+timestamp_size:offset+timestamp_size+values_size]
        
        # Decompress
        timestamps = self._decompress_timestamps(compressed_timestamps, stored_metadata['count'])
        values = self._decompress_values(compressed_values, stored_metadata['count'])
        
        return list(zip(timestamps, values))
    
    def _compress_timestamps(self, timestamps: List[int]) -> bytes:
        """Compress timestamps using delta-of-delta encoding"""
        if len(timestamps) == 0:
            return b''
        
        if len(timestamps) == 1:
            return struct.pack('>Q', timestamps[0])
        
        result = bytearray()
        
        # Store first timestamp as-is
        result.extend(struct.pack('>Q', timestamps[0]))
        
        if len(timestamps) == 1:
            return bytes(result)
        
        # Store first delta
        first_delta = timestamps[1] - timestamps[0]
        result.extend(struct.pack('>q', first_delta))
        
        # Delta-of-delta encoding for remaining timestamps
        prev_delta = first_delta
        
        for i in range(2, len(timestamps)):
            current_delta = timestamps[i] - timestamps[i-1]
            delta_of_delta = current_delta - prev_delta
            
            # Simple encoding - in practice, use bit packing
            result.extend(struct.pack('>q', delta_of_delta))
            prev_delta = current_delta
        
        return bytes(result)
    
    def _decompress_timestamps(self, compressed: bytes, count: int) -> List[int]:
        """Decompress delta-of-delta encoded timestamps"""
        if len(compressed) == 0:
            return []
        
        timestamps = []
        offset = 0
        
        # Read first timestamp
        first_timestamp = struct.unpack('>Q', compressed[offset:offset+8])[0]
        timestamps.append(first_timestamp)
        offset += 8
        
        if count == 1:
            return timestamps
        
        # Read first delta
        first_delta = struct.unpack('>q', compressed[offset:offset+8])[0]
        timestamps.append(first_timestamp + first_delta)
        offset += 8
        
        # Reconstruct remaining timestamps
        prev_delta = first_delta
        
        for i in range(2, count):
            delta_of_delta = struct.unpack('>q', compressed[offset:offset+8])[0]
            current_delta = prev_delta + delta_of_delta
            
            next_timestamp = timestamps[-1] + current_delta
            timestamps.append(next_timestamp)
            
            prev_delta = current_delta
            offset += 8
        
        return timestamps
    
    def _compress_values(self, values: List[float]) -> bytes:
        """Compress values using XOR-based encoding"""
        if len(values) == 0:
            return b''
        
        result = bytearray()
        
        # Convert to float64 and get bit representation
        float_values = [float(v) for v in values]
        
        # Store first value as-is
        result.extend(struct.pack('>d', float_values[0]))
        
        if len(float_values) == 1:
            return bytes(result)
        
        # XOR encoding for subsequent values
        prev_bits = struct.unpack('>Q', struct.pack('>d', float_values[0]))[0]
        
        for value in float_values[1:]:
            current_bits = struct.unpack('>Q', struct.pack('>d', value))[0]
            xor_result = prev_bits ^ current_bits
            
            # Simple storage - in practice, use leading zero compression
            result.extend(struct.pack('>Q', xor_result))
            prev_bits = current_bits
        
        return bytes(result)
    
    def _decompress_values(self, compressed: bytes, count: int) -> List[float]:
        """Decompress XOR-encoded values"""
        if len(compressed) == 0:
            return []
        
        values = []
        offset = 0
        
        # Read first value
        first_value = struct.unpack('>d', compressed[offset:offset+8])[0]
        values.append(first_value)
        offset += 8
        
        if count == 1:
            return values
        
        # Reconstruct subsequent values
        prev_bits = struct.unpack('>Q', struct.pack('>d', first_value))[0]
        
        for i in range(1, count):
            xor_result = struct.unpack('>Q', compressed[offset:offset+8])[0]
            current_bits = prev_bits ^ xor_result
            
            current_value = struct.unpack('>d', struct.pack('>Q', current_bits))[0]
            values.append(current_value)
            
            prev_bits = current_bits
            offset += 8
        
        return values
    
    @property
    def compression_type(self) -> CompressionType:
        return CompressionType.GORILLA


class DeltaCodec(CompressionCodec):
    """Delta compression codec for numeric sequences"""
    
    def __init__(self, data_type: str = 'int64'):
        self.data_type = data_type
    
    def compress(self, data: Union[bytes, List[Any]]) -> CompressionResult:
        """Compress numeric data using delta encoding"""
        if isinstance(data, bytes):
            import pickle
            data = pickle.loads(data)
        
        if not isinstance(data, list) or len(data) == 0:
            return CompressionResult(
                compressed_data=b'',
                original_size=0,
                compressed_size=0,
                algorithm=CompressionType.DELTA,
                metadata={'count': 0, 'data_type': self.data_type}
            )
        
        # Convert to numbers
        numbers = []
        for item in data:
            if isinstance(item, (int, float)):
                numbers.append(item)
            else:
                try:
                    numbers.append(float(item))
                except (ValueError, TypeError):
                    numbers.append(0.0)
        
        if len(numbers) == 0:
            return CompressionResult(
                compressed_data=b'',
                original_size=0,
                compressed_size=0,
                algorithm=CompressionType.DELTA,
                metadata={'count': 0, 'data_type': self.data_type}
            )
        
        result = bytearray()
        
        # Store first value
        if self.data_type == 'int64':
            result.extend(struct.pack('>q', int(numbers[0])))
            prev_value = int(numbers[0])
        else:
            result.extend(struct.pack('>d', float(numbers[0])))
            prev_value = float(numbers[0])
        
        # Store deltas
        for value in numbers[1:]:
            if self.data_type == 'int64':
                delta = int(value) - prev_value
                result.extend(struct.pack('>q', delta))
                prev_value = int(value)
            else:
                delta = float(value) - prev_value
                result.extend(struct.pack('>d', delta))
                prev_value = float(value)
        
        original_size = len(str(data).encode('utf-8'))
        
        return CompressionResult(
            compressed_data=bytes(result),
            original_size=original_size,
            compressed_size=len(result),
            algorithm=CompressionType.DELTA,
            metadata={'count': len(numbers), 'data_type': self.data_type}
        )
    
    def decompress(self, compressed_data: bytes, metadata: Dict[str, Any]) -> List[Union[int, float]]:
        """Decompress delta-encoded data"""
        count = metadata.get('count', 0)
        data_type = metadata.get('data_type', self.data_type)
        
        if count == 0 or len(compressed_data) == 0:
            return []
        
        values = []
        offset = 0
        
        # Read first value
        if data_type == 'int64':
            first_value = struct.unpack('>q', compressed_data[offset:offset+8])[0]
            values.append(first_value)
            offset += 8
            prev_value = first_value
            
            # Reconstruct subsequent values
            for i in range(1, count):
                delta = struct.unpack('>q', compressed_data[offset:offset+8])[0]
                current_value = prev_value + delta
                values.append(current_value)
                prev_value = current_value
                offset += 8
        else:
            first_value = struct.unpack('>d', compressed_data[offset:offset+8])[0]
            values.append(first_value)
            offset += 8
            prev_value = first_value
            
            # Reconstruct subsequent values
            for i in range(1, count):
                delta = struct.unpack('>d', compressed_data[offset:offset+8])[0]
                current_value = prev_value + delta
                values.append(current_value)
                prev_value = current_value
                offset += 8
        
        return values
    
    @property
    def compression_type(self) -> CompressionType:
        return CompressionType.DELTA


class RLECodec(CompressionCodec):
    """Run-Length Encoding codec"""
    
    def compress(self, data: Union[bytes, List[Any]]) -> CompressionResult:
        """Compress data using Run-Length Encoding"""
        if isinstance(data, bytes):
            data = list(data)
        elif not isinstance(data, list):
            data = list(str(data))
        
        if len(data) == 0:
            return CompressionResult(
                compressed_data=b'',
                original_size=0,
                compressed_size=0,
                algorithm=CompressionType.RLE,
                metadata={'count': 0}
            )
        
        compressed = []
        current_value = data[0]
        count = 1
        
        for item in data[1:]:
            if item == current_value:
                count += 1
            else:
                compressed.append((current_value, count))
                current_value = item
                count = 1
        
        # Add the last run
        compressed.append((current_value, count))
        
        # Serialize compressed data
        import pickle
        compressed_data = pickle.dumps(compressed)
        original_size = len(pickle.dumps(data))
        
        return CompressionResult(
            compressed_data=compressed_data,
            original_size=original_size,
            compressed_size=len(compressed_data),
            algorithm=CompressionType.RLE,
            metadata={'runs': len(compressed)}
        )
    
    def decompress(self, compressed_data: bytes, metadata: Dict[str, Any]) -> List[Any]:
        """Decompress RLE data"""
        if len(compressed_data) == 0:
            return []
        
        import pickle
        compressed = pickle.loads(compressed_data)
        
        result = []
        for value, count in compressed:
            result.extend([value] * count)
        
        return result
    
    @property
    def compression_type(self) -> CompressionType:
        return CompressionType.RLE


class DictionaryCodec(CompressionCodec):
    """Dictionary compression codec"""
    
    def __init__(self, max_dict_size: int = 65536):
        self.max_dict_size = max_dict_size
    
    def compress(self, data: Union[bytes, List[Any]]) -> CompressionResult:
        """Compress data using dictionary encoding"""
        if isinstance(data, bytes):
            data = data.decode('utf-8', errors='ignore').split()
        elif not isinstance(data, list):
            data = str(data).split()
        
        if len(data) == 0:
            return CompressionResult(
                compressed_data=b'',
                original_size=0,
                compressed_size=0,
                algorithm=CompressionType.DICTIONARY,
                metadata={'dict_size': 0}
            )
        
        # Build dictionary
        unique_values = list(set(data))[:self.max_dict_size]
        value_to_index = {value: i for i, value in enumerate(unique_values)}
        
        # Encode data as indices
        encoded = []
        for item in data:
            if item in value_to_index:
                encoded.append(value_to_index[item])
            else:
                # Use a special marker for unknown values
                encoded.append(len(unique_values))
        
        # Serialize
        import pickle
        result_data = pickle.dumps({
            'dictionary': unique_values,
            'encoded': encoded
        })
        
        original_size = len(str(data).encode('utf-8'))
        
        return CompressionResult(
            compressed_data=result_data,
            original_size=original_size,
            compressed_size=len(result_data),
            algorithm=CompressionType.DICTIONARY,
            metadata={'dict_size': len(unique_values)}
        )
    
    def decompress(self, compressed_data: bytes, metadata: Dict[str, Any]) -> List[str]:
        """Decompress dictionary-encoded data"""
        if len(compressed_data) == 0:
            return []
        
        import pickle
        data = pickle.loads(compressed_data)
        
        dictionary = data['dictionary']
        encoded = data['encoded']
        
        result = []
        for index in encoded:
            if index < len(dictionary):
                result.append(dictionary[index])
            else:
                result.append('<UNKNOWN>')
        
        return result
    
    @property
    def compression_type(self) -> CompressionType:
        return CompressionType.DICTIONARY


class CompressionManager:
    """Manages multiple compression codecs"""
    
    def __init__(self):
        self.codecs = {
            CompressionType.GZIP: GZipCodec(),
            CompressionType.LZ4: LZ4Codec(),
            CompressionType.SNAPPY: SnappyCodec(),
            CompressionType.GORILLA: GorillaCodec(),
            CompressionType.DELTA: DeltaCodec(),
            CompressionType.RLE: RLECodec(),
            CompressionType.DICTIONARY: DictionaryCodec()
        }
        
        self.stats = {
            'compressions': 0,
            'decompressions': 0,
            'total_original_bytes': 0,
            'total_compressed_bytes': 0,
            'algorithm_usage': {alg.value: 0 for alg in CompressionType}
        }
    
    def register_codec(self, codec: CompressionCodec):
        """Register a custom codec"""
        self.codecs[codec.compression_type] = codec
    
    def compress(self, data: Union[bytes, List[Any]], 
                algorithm: CompressionType = CompressionType.LZ4) -> CompressionResult:
        """Compress data using specified algorithm"""
        if algorithm not in self.codecs:
            raise ValueError(f"Unsupported compression algorithm: {algorithm}")
        
        codec = self.codecs[algorithm]
        result = codec.compress(data)
        
        # Update stats
        self.stats['compressions'] += 1
        self.stats['total_original_bytes'] += result.original_size
        self.stats['total_compressed_bytes'] += result.compressed_size
        self.stats['algorithm_usage'][algorithm.value] += 1
        
        return result
    
    def decompress(self, compressed_data: bytes, algorithm: CompressionType,
                  metadata: Dict[str, Any] = None) -> Union[bytes, List[Any]]:
        """Decompress data using specified algorithm"""
        if algorithm not in self.codecs:
            raise ValueError(f"Unsupported compression algorithm: {algorithm}")
        
        codec = self.codecs[algorithm]
        result = codec.decompress(compressed_data, metadata or {})
        
        # Update stats
        self.stats['decompressions'] += 1
        
        return result
    
    def auto_compress(self, data: Union[bytes, List[Any]]) -> CompressionResult:
        """Automatically choose best compression algorithm"""
        # Try different algorithms and pick the best one
        algorithms_to_try = [
            CompressionType.LZ4,
            CompressionType.SNAPPY,
            CompressionType.GZIP
        ]
        
        # For numeric data, try specialized algorithms
        if isinstance(data, list) and len(data) > 0:
            if all(isinstance(x, (int, float)) for x in data):
                algorithms_to_try.insert(0, CompressionType.DELTA)
            elif all(isinstance(x, (tuple, list)) and len(x) == 2 for x in data):
                algorithms_to_try.insert(0, CompressionType.GORILLA)
        
        best_result = None
        best_ratio = float('inf')
        
        for algorithm in algorithms_to_try:
            try:
                result = self.compress(data, algorithm)
                if result.compression_ratio < best_ratio:
                    best_ratio = result.compression_ratio
                    best_result = result
            except Exception as e:
                logger.warning(f"Compression failed with {algorithm}: {e}")
                continue
        
        if best_result is None:
            # Fallback to no compression
            if isinstance(data, bytes):
                data_bytes = data
            else:
                data_bytes = str(data).encode('utf-8')
            
            best_result = CompressionResult(
                compressed_data=data_bytes,
                original_size=len(data_bytes),
                compressed_size=len(data_bytes),
                algorithm=CompressionType.NONE,
                metadata={}
            )
        
        return best_result
    
    def get_stats(self) -> Dict[str, Any]:
        """Get compression statistics"""
        stats = self.stats.copy()
        
        if stats['total_original_bytes'] > 0:
            stats['overall_compression_ratio'] = (
                stats['total_compressed_bytes'] / stats['total_original_bytes']
            )
            stats['space_saved_percent'] = (
                1.0 - stats['overall_compression_ratio']
            ) * 100.0
        else:
            stats['overall_compression_ratio'] = 0.0
            stats['space_saved_percent'] = 0.0
        
        return stats


# Factory functions
def create_compression_manager() -> CompressionManager:
    """Create a compression manager with default codecs"""
    return CompressionManager()


def compress_time_series(timestamps: List[int], values: List[float]) -> CompressionResult:
    """Compress time series data using Gorilla algorithm"""
    codec = GorillaCodec()
    data = list(zip(timestamps, values))
    return codec.compress(data)


def compress_numeric_sequence(values: List[Union[int, float]], 
                            use_integers: bool = True) -> CompressionResult:
    """Compress numeric sequence using delta encoding"""
    codec = DeltaCodec('int64' if use_integers else 'float64')
    return codec.compress(values)
