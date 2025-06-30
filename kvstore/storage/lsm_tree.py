"""
LSM Tree (Log-Structured Merge Tree) Implementation
Inspired by ScyllaDB, Cassandra, and RocksDB
"""

import asyncio
import heapq
import json
import os
import pickle
import struct
import time
import threading
import zlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Iterator, Any
import logging

logger = logging.getLogger(__name__)


@dataclass
class SSTableMetadata:
    """Metadata for an SSTable"""
    level: int
    size: int
    min_key: str
    max_key: str
    created_at: float
    bloom_filter_fp_rate: float = 0.01


class BloomFilter:
    """Simple Bloom filter implementation for SSTable key lookups"""
    
    def __init__(self, capacity: int, fp_rate: float = 0.01):
        self.capacity = capacity
        self.fp_rate = fp_rate
        self.size = self._optimal_size(capacity, fp_rate)
        self.hash_count = self._optimal_hash_count(self.size, capacity)
        self.bit_array = bytearray(self.size // 8 + 1)
        
    def _optimal_size(self, n: int, p: float) -> int:
        """Calculate optimal bit array size"""
        import math
        return int(-n * math.log(p) / (math.log(2) ** 2))
    
    def _optimal_hash_count(self, m: int, n: int) -> int:
        """Calculate optimal number of hash functions"""
        import math
        return int((m / n) * math.log(2))
    
    def _hash(self, key: str, seed: int) -> int:
        """Simple hash function with seed"""
        hash_val = hash(key + str(seed))
        return abs(hash_val) % len(self.bit_array) * 8
    
    def add(self, key: str):
        """Add key to bloom filter"""
        for i in range(self.hash_count):
            bit_pos = self._hash(key, i)
            byte_pos = bit_pos // 8
            bit_offset = bit_pos % 8
            self.bit_array[byte_pos] |= (1 << bit_offset)
    
    def might_contain(self, key: str) -> bool:
        """Check if key might be in the set (no false negatives)"""
        for i in range(self.hash_count):
            bit_pos = self._hash(key, i)
            byte_pos = bit_pos // 8
            bit_offset = bit_pos % 8
            if not (self.bit_array[byte_pos] & (1 << bit_offset)):
                return False
        return True


class Memtable:
    """In-memory sorted table using skip list for O(log n) operations"""
    
    def __init__(self, max_size: int = 64 * 1024 * 1024):  # 64MB
        self.data = {}  # Using dict for simplicity - could use skip list
        self.max_size = max_size
        self.size = 0
        self.lock = threading.RWLock()
        
    def put(self, key: str, value: bytes, timestamp: float = None) -> bool:
        """Put key-value pair. Returns True if memtable is full after insert"""
        if timestamp is None:
            timestamp = time.time()
            
        with self.lock.writer():
            old_size = len(self.data.get(key, b''))
            self.data[key] = (value, timestamp)
            self.size += len(value) - old_size
            
            return self.size >= self.max_size
    
    def get(self, key: str) -> Optional[Tuple[bytes, float]]:
        """Get value and timestamp for key"""
        with self.lock.reader():
            return self.data.get(key)
    
    def delete(self, key: str, timestamp: float = None):
        """Mark key as deleted (tombstone)"""
        if timestamp is None:
            timestamp = time.time()
        with self.lock.writer():
            self.data[key] = (b'__TOMBSTONE__', timestamp)
    
    def scan(self, start_key: str = None, end_key: str = None) -> Iterator[Tuple[str, bytes, float]]:
        """Scan range of keys in sorted order"""
        with self.lock.reader():
            keys = sorted(self.data.keys())
            for key in keys:
                if start_key and key < start_key:
                    continue
                if end_key and key > end_key:
                    break
                value, timestamp = self.data[key]
                yield key, value, timestamp
    
    def flush_to_sstable(self, sstable_path: str) -> SSTableMetadata:
        """Flush memtable to SSTable file"""
        with self.lock.reader():
            if not self.data:
                return None
                
            keys = sorted(self.data.keys())
            min_key = keys[0]
            max_key = keys[-1]
            
            # Create bloom filter
            bloom_filter = BloomFilter(len(keys))
            
            with open(sstable_path, 'wb') as f:
                # Write header
                header = {
                    'version': 1,
                    'key_count': len(keys),
                    'min_key': min_key,
                    'max_key': max_key,
                    'created_at': time.time()
                }
                header_data = json.dumps(header).encode('utf-8')
                f.write(struct.pack('<I', len(header_data)))
                f.write(header_data)
                
                # Write bloom filter
                bloom_data = pickle.dumps(bloom_filter)
                f.write(struct.pack('<I', len(bloom_data)))
                f.write(bloom_data)
                
                # Write index and data
                index_offset = f.tell()
                index_entries = []
                
                for key in keys:
                    value, timestamp = self.data[key]
                    bloom_filter.add(key)
                    
                    # Compress value if beneficial
                    compressed_value = zlib.compress(value) if len(value) > 100 else value
                    is_compressed = len(compressed_value) < len(value)
                    
                    key_bytes = key.encode('utf-8')
                    entry = struct.pack('<I', len(key_bytes)) + key_bytes
                    entry += struct.pack('<d', timestamp)
                    entry += struct.pack('<B', 1 if is_compressed else 0)
                    entry += struct.pack('<I', len(compressed_value))
                    entry += compressed_value
                    
                    index_entries.append((key, f.tell()))
                    f.write(entry)
                
                # Write index at end
                index_data = []
                for key, offset in index_entries:
                    key_bytes = key.encode('utf-8')
                    index_data.append(struct.pack('<I', len(key_bytes)) + key_bytes)
                    index_data.append(struct.pack('<Q', offset))
                
                index_content = b''.join(index_data)
                f.write(struct.pack('<Q', index_offset))
                f.write(index_content)
            
            return SSTableMetadata(
                level=0,
                size=os.path.getsize(sstable_path),
                min_key=min_key,
                max_key=max_key,
                created_at=time.time()
            )


class SSTable:
    """Sorted String Table for persistent storage"""
    
    def __init__(self, path: str, metadata: SSTableMetadata):
        self.path = path
        self.metadata = metadata
        self.bloom_filter = None
        self.index = {}
        self._load_metadata()
    
    def _load_metadata(self):
        """Load SSTable metadata and bloom filter"""
        try:
            with open(self.path, 'rb') as f:
                # Read header
                header_size = struct.unpack('<I', f.read(4))[0]
                header_data = f.read(header_size)
                header = json.loads(header_data.decode('utf-8'))
                
                # Read bloom filter
                bloom_size = struct.unpack('<I', f.read(4))[0]
                bloom_data = f.read(bloom_size)
                self.bloom_filter = pickle.loads(bloom_data)
                
        except Exception as e:
            logger.error(f"Failed to load SSTable metadata: {e}")
    
    def get(self, key: str) -> Optional[Tuple[bytes, float]]:
        """Get value for key from SSTable"""
        if self.bloom_filter and not self.bloom_filter.might_contain(key):
            return None
            
        try:
            with open(self.path, 'rb') as f:
                # Read header to skip it
                header_size = struct.unpack('<I', f.read(4))[0]
                f.seek(header_size, 1)
                
                # Read bloom filter to skip it
                bloom_size = struct.unpack('<I', f.read(4))[0]
                f.seek(bloom_size, 1)
                
                # Binary search through entries
                # For simplicity, we'll do a linear scan
                # In production, we'd build and cache an index
                while True:
                    try:
                        key_len = struct.unpack('<I', f.read(4))[0]
                        entry_key = f.read(key_len).decode('utf-8')
                        timestamp = struct.unpack('<d', f.read(8))[0]
                        is_compressed = struct.unpack('<B', f.read(1))[0]
                        value_len = struct.unpack('<I', f.read(4))[0]
                        value = f.read(value_len)
                        
                        if entry_key == key:
                            if is_compressed:
                                value = zlib.decompress(value)
                            return value, timestamp
                        elif entry_key > key:
                            break
                            
                    except struct.error:
                        break
                        
        except Exception as e:
            logger.error(f"Error reading SSTable: {e}")
            
        return None
    
    def scan(self, start_key: str = None, end_key: str = None) -> Iterator[Tuple[str, bytes, float]]:
        """Scan range of keys from SSTable"""
        try:
            with open(self.path, 'rb') as f:
                # Skip header and bloom filter
                header_size = struct.unpack('<I', f.read(4))[0]
                f.seek(header_size, 1)
                bloom_size = struct.unpack('<I', f.read(4))[0]
                f.seek(bloom_size, 1)
                
                while True:
                    try:
                        key_len = struct.unpack('<I', f.read(4))[0]
                        key = f.read(key_len).decode('utf-8')
                        timestamp = struct.unpack('<d', f.read(8))[0]
                        is_compressed = struct.unpack('<B', f.read(1))[0]
                        value_len = struct.unpack('<I', f.read(4))[0]
                        value = f.read(value_len)
                        
                        if start_key and key < start_key:
                            continue
                        if end_key and key > end_key:
                            break
                            
                        if is_compressed:
                            value = zlib.decompress(value)
                        yield key, value, timestamp
                        
                    except struct.error:
                        break
                        
        except Exception as e:
            logger.error(f"Error scanning SSTable: {e}")


class CompactionStrategy:
    """LSM Tree compaction strategy"""
    
    def __init__(self, max_level: int = 7, size_ratio: int = 10):
        self.max_level = max_level
        self.size_ratio = size_ratio
    
    def should_compact(self, level: int, sstables: List[SSTable]) -> bool:
        """Determine if level should be compacted"""
        if level == 0:
            return len(sstables) >= 4  # L0 has overlapping ranges
        
        # Check if level exceeds size threshold
        total_size = sum(st.metadata.size for st in sstables)
        max_size = (10 ** level) * 1024 * 1024  # 10MB, 100MB, 1GB, etc.
        
        return total_size > max_size
    
    def select_sstables_for_compaction(self, level: int, sstables: List[SSTable]) -> List[SSTable]:
        """Select SSTables for compaction"""
        if level == 0:
            # Compact all L0 tables
            return sstables[:]
        
        # For other levels, select oldest tables that exceed threshold
        sorted_tables = sorted(sstables, key=lambda st: st.metadata.created_at)
        total_size = 0
        selected = []
        
        for table in sorted_tables:
            selected.append(table)
            total_size += table.metadata.size
            if total_size > (10 ** level) * 1024 * 1024:
                break
                
        return selected


class LSMTree:
    """Log-Structured Merge Tree implementation"""
    
    def __init__(self, data_dir: str, memtable_size: int = 64 * 1024 * 1024):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.memtable_size = memtable_size
        self.memtable = Memtable(memtable_size)
        self.immutable_memtables = []
        
        # SSTable management
        self.sstables = {}  # level -> List[SSTable]
        self.compaction_strategy = CompactionStrategy()
        
        # Background tasks
        self.flush_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="lsm-flush")
        self.compaction_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="lsm-compact")
        
        self.lock = threading.RLock()
        
        # Load existing SSTables
        self._load_existing_sstables()
        
        # Start background tasks
        self._start_background_tasks()
    
    def _load_existing_sstables(self):
        """Load existing SSTable files on startup"""
        for sstable_file in self.data_dir.glob("*.sst"):
            try:
                # Parse filename: level_timestamp.sst
                name_parts = sstable_file.stem.split('_')
                if len(name_parts) >= 2:
                    level = int(name_parts[0])
                    
                    # Create metadata (simplified - normally stored separately)
                    metadata = SSTableMetadata(
                        level=level,
                        size=sstable_file.stat().st_size,
                        min_key="",  # Would be loaded from file
                        max_key="",
                        created_at=sstable_file.stat().st_mtime
                    )
                    
                    sstable = SSTable(str(sstable_file), metadata)
                    
                    if level not in self.sstables:
                        self.sstables[level] = []
                    self.sstables[level].append(sstable)
                    
            except Exception as e:
                logger.error(f"Failed to load SSTable {sstable_file}: {e}")
    
    def _start_background_tasks(self):
        """Start background flush and compaction tasks"""
        def background_flush():
            while True:
                time.sleep(1)
                if self.immutable_memtables:
                    self._flush_immutable_memtable()
        
        def background_compaction():
            while True:
                time.sleep(10)
                self._trigger_compaction()
        
        self.flush_executor.submit(background_flush)
        self.compaction_executor.submit(background_compaction)
    
    def put(self, key: str, value: bytes) -> None:
        """Put key-value pair"""
        with self.lock:
            is_full = self.memtable.put(key, value)
            
            if is_full:
                # Move current memtable to immutable and create new one
                self.immutable_memtables.append(self.memtable)
                self.memtable = Memtable(self.memtable_size)
                
                # Trigger async flush
                self.flush_executor.submit(self._flush_immutable_memtable)
    
    def get(self, key: str) -> Optional[bytes]:
        """Get value for key"""
        # Check memtable first
        result = self.memtable.get(key)
        if result:
            value, timestamp = result
            if value == b'__TOMBSTONE__':
                return None
            return value
        
        # Check immutable memtables
        for memtable in reversed(self.immutable_memtables):
            result = memtable.get(key)
            if result:
                value, timestamp = result
                if value == b'__TOMBSTONE__':
                    return None
                return value
        
        # Check SSTables from newest to oldest
        with self.lock:
            for level in sorted(self.sstables.keys()):
                for sstable in reversed(sorted(self.sstables[level], 
                                             key=lambda st: st.metadata.created_at)):
                    result = sstable.get(key)
                    if result:
                        value, timestamp = result
                        if value == b'__TOMBSTONE__':
                            return None
                        return value
        
        return None
    
    def delete(self, key: str) -> None:
        """Delete key (add tombstone)"""
        with self.lock:
            is_full = self.memtable.put(key, b'__TOMBSTONE__')
            
            if is_full:
                self.immutable_memtables.append(self.memtable)
                self.memtable = Memtable(self.memtable_size)
                self.flush_executor.submit(self._flush_immutable_memtable)
    
    def scan(self, start_key: str = None, end_key: str = None) -> Iterator[Tuple[str, bytes]]:
        """Scan range of keys"""
        # Merge iterators from all sources (memtable, immutable memtables, SSTables)
        iterators = []
        
        # Add memtable iterator
        iterators.append(self.memtable.scan(start_key, end_key))
        
        # Add immutable memtable iterators
        for memtable in self.immutable_memtables:
            iterators.append(memtable.scan(start_key, end_key))
        
        # Add SSTable iterators
        with self.lock:
            for level in sorted(self.sstables.keys()):
                for sstable in self.sstables[level]:
                    iterators.append(sstable.scan(start_key, end_key))
        
        # Merge all iterators with deduplication (latest value wins)
        seen_keys = set()
        for key, value, timestamp in heapq.merge(*iterators, key=lambda x: x[0]):
            if key not in seen_keys:
                seen_keys.add(key)
                if value != b'__TOMBSTONE__':
                    yield key, value
    
    def _flush_immutable_memtable(self):
        """Flush oldest immutable memtable to SSTable"""
        with self.lock:
            if not self.immutable_memtables:
                return
                
            memtable = self.immutable_memtables.pop(0)
        
        # Generate SSTable filename
        timestamp = int(time.time() * 1000000)  # microseconds for uniqueness
        sstable_path = self.data_dir / f"0_{timestamp}.sst"
        
        try:
            metadata = memtable.flush_to_sstable(str(sstable_path))
            if metadata:
                sstable = SSTable(str(sstable_path), metadata)
                
                with self.lock:
                    if 0 not in self.sstables:
                        self.sstables[0] = []
                    self.sstables[0].append(sstable)
                
                logger.info(f"Flushed memtable to SSTable: {sstable_path}")
                
                # Trigger compaction if needed
                self._trigger_compaction()
                
        except Exception as e:
            logger.error(f"Failed to flush memtable: {e}")
    
    def _trigger_compaction(self):
        """Check if compaction is needed and trigger it"""
        with self.lock:
            for level in sorted(self.sstables.keys()):
                if level not in self.sstables:
                    continue
                    
                sstables = self.sstables[level]
                if self.compaction_strategy.should_compact(level, sstables):
                    logger.info(f"Triggering compaction for level {level}")
                    self.compaction_executor.submit(self._compact_level, level)
                    break
    
    def _compact_level(self, level: int):
        """Compact SSTables at given level"""
        with self.lock:
            if level not in self.sstables:
                return
                
            sstables = self.sstables[level]
            selected_sstables = self.compaction_strategy.select_sstables_for_compaction(
                level, sstables
            )
            
            if not selected_sstables:
                return
        
        try:
            # Merge selected SSTables into new SSTable at next level
            timestamp = int(time.time() * 1000000)
            next_level = level + 1
            output_path = self.data_dir / f"{next_level}_{timestamp}.sst"
            
            # Collect all entries from selected SSTables
            all_entries = []
            for sstable in selected_sstables:
                for key, value, ts in sstable.scan():
                    all_entries.append((key, value, ts))
            
            # Sort by key and deduplicate (keep latest timestamp)
            all_entries.sort(key=lambda x: (x[0], -x[2]))  # Sort by key, then by timestamp desc
            
            deduplicated = {}
            for key, value, ts in all_entries:
                if key not in deduplicated:
                    deduplicated[key] = (value, ts)
            
            # Write new SSTable
            if deduplicated:
                keys = sorted(deduplicated.keys())
                min_key = keys[0]
                max_key = keys[-1]
                
                bloom_filter = BloomFilter(len(keys))
                
                with open(output_path, 'wb') as f:
                    # Write header
                    header = {
                        'version': 1,
                        'key_count': len(keys),
                        'min_key': min_key,
                        'max_key': max_key,
                        'created_at': time.time()
                    }
                    header_data = json.dumps(header).encode('utf-8')
                    f.write(struct.pack('<I', len(header_data)))
                    f.write(header_data)
                    
                    # Write bloom filter
                    bloom_data = pickle.dumps(bloom_filter)
                    f.write(struct.pack('<I', len(bloom_data)))
                    f.write(bloom_data)
                    
                    # Write entries
                    for key in keys:
                        value, timestamp = deduplicated[key]
                        bloom_filter.add(key)
                        
                        compressed_value = zlib.compress(value) if len(value) > 100 else value
                        is_compressed = len(compressed_value) < len(value)
                        
                        key_bytes = key.encode('utf-8')
                        entry = struct.pack('<I', len(key_bytes)) + key_bytes
                        entry += struct.pack('<d', timestamp)
                        entry += struct.pack('<B', 1 if is_compressed else 0)
                        entry += struct.pack('<I', len(compressed_value))
                        entry += compressed_value
                        
                        f.write(entry)
                
                # Create new SSTable object
                metadata = SSTableMetadata(
                    level=next_level,
                    size=os.path.getsize(output_path),
                    min_key=min_key,
                    max_key=max_key,
                    created_at=time.time()
                )
                
                new_sstable = SSTable(str(output_path), metadata)
                
                # Update SSTable registry
                with self.lock:
                    # Remove old SSTables
                    for sstable in selected_sstables:
                        self.sstables[level].remove(sstable)
                        try:
                            os.remove(sstable.path)
                        except Exception as e:
                            logger.error(f"Failed to remove old SSTable: {e}")
                    
                    # Add new SSTable
                    if next_level not in self.sstables:
                        self.sstables[next_level] = []
                    self.sstables[next_level].append(new_sstable)
                
                logger.info(f"Compacted {len(selected_sstables)} SSTables from level {level} to level {next_level}")
                
        except Exception as e:
            logger.error(f"Compaction failed for level {level}: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get LSM Tree statistics"""
        with self.lock:
            stats = {
                'memtable_size': self.memtable.size,
                'immutable_memtables': len(self.immutable_memtables),
                'levels': {}
            }
            
            for level, sstables in self.sstables.items():
                stats['levels'][level] = {
                    'sstable_count': len(sstables),
                    'total_size': sum(st.metadata.size for st in sstables)
                }
            
            return stats
    
    def close(self):
        """Close LSM Tree and cleanup resources"""
        self.flush_executor.shutdown(wait=True)
        self.compaction_executor.shutdown(wait=True)
