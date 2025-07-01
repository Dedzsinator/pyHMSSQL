"""
Write-Ahead Log (WAL) Implementation for HyperKV
Separate from AOF, provides durability guarantees and recovery capabilities
Supports concurrent writes, log rotation, and crash recovery
"""

import os
import asyncio
import time
import struct
import json
import threading
import logging
import zlib
from typing import Dict, List, Optional, Any, BinaryIO, AsyncIterator
from dataclasses import dataclass, field
from enum import Enum
import aiofiles
import aiofiles.os
from pathlib import Path
import hashlib
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class WALEntryType(Enum):
    """Types of WAL entries"""

    SET = 1
    DELETE = 2
    EXPIRE = 3
    CLEAR = 4
    BEGIN_TX = 5
    COMMIT_TX = 6
    ROLLBACK_TX = 7
    CHECKPOINT = 8
    CRDT_MERGE = 9
    BATCH_START = 10
    BATCH_END = 11


@dataclass
class WALEntry:
    """Write-Ahead Log entry"""

    entry_type: WALEntryType
    sequence_number: int
    timestamp: int
    key: Optional[str] = None
    value: Optional[Any] = None
    transaction_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    checksum: Optional[int] = None

    def to_bytes(self) -> bytes:
        """Serialize entry to bytes"""
        data = {
            "type": self.entry_type.value,
            "seq": self.sequence_number,
            "ts": self.timestamp,
            "key": self.key,
            "value": self.value,
            "tx_id": self.transaction_id,
            "meta": self.metadata,
        }

        json_data = json.dumps(data, separators=(",", ":")).encode("utf-8")
        compressed_data = zlib.compress(json_data)

        # Calculate checksum using zlib.crc32 instead of hashlib.crc32
        checksum = zlib.crc32(compressed_data) & 0xFFFFFFFF

        # Pack: magic(4) + checksum(4) + length(4) + data
        magic = b"WAL1"
        header = struct.pack(
            ">III", int.from_bytes(magic, "big"), checksum, len(compressed_data)
        )

        return header + compressed_data

    @classmethod
    def from_bytes(cls, data: bytes) -> "WALEntry":
        """Deserialize entry from bytes"""
        if len(data) < 12:
            raise ValueError("Invalid WAL entry: too short")

        # Unpack header
        magic_int, checksum, length = struct.unpack(">III", data[:12])
        magic = magic_int.to_bytes(4, "big")

        if magic != b"WAL1":
            raise ValueError(f"Invalid WAL magic: {magic}")

        if len(data) < 12 + length:
            raise ValueError("Invalid WAL entry: data truncated")

        compressed_data = data[12 : 12 + length]

        # Verify checksum using zlib.crc32
        calc_checksum = zlib.crc32(compressed_data) & 0xFFFFFFFF
        if calc_checksum != checksum:
            raise ValueError(
                f"WAL entry checksum mismatch: {calc_checksum} != {checksum}"
            )

        # Decompress and parse
        json_data = zlib.decompress(compressed_data)
        parsed = json.loads(json_data.decode("utf-8"))

        return cls(
            entry_type=WALEntryType(parsed["type"]),
            sequence_number=parsed["seq"],
            timestamp=parsed["ts"],
            key=parsed.get("key"),
            value=parsed.get("value"),
            transaction_id=parsed.get("tx_id"),
            metadata=parsed.get("meta", {}),
            checksum=checksum,
        )


@dataclass
class WALConfig:
    """WAL configuration"""

    wal_dir: str = "wal"
    segment_size_mb: int = 64
    max_segments: int = 100
    sync_interval_ms: int = 1000
    sync_on_write: bool = False
    compression_enabled: bool = True
    rotation_check_interval: int = 1000  # entries
    recovery_batch_size: int = 1000


class WALSegment:
    """Individual WAL segment file"""

    def __init__(self, segment_id: int, file_path: str, config: WALConfig):
        self.segment_id = segment_id
        self.file_path = file_path
        self.config = config
        self.file_handle: Optional[BinaryIO] = None
        self.current_size = 0
        self.entry_count = 0
        self.first_sequence = None
        self.last_sequence = None
        self.is_closed = False
        self.lock = threading.Lock()

    async def open(self, mode: str = "ab"):
        """Open the segment file"""
        with self.lock:
            if self.file_handle is None:
                # Ensure directory exists
                os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
                self.file_handle = open(self.file_path, mode)

                # Get current size if file exists
                if os.path.exists(self.file_path):
                    self.current_size = os.path.getsize(self.file_path)

    async def close(self):
        """Close the segment file"""
        with self.lock:
            if self.file_handle:
                self.file_handle.flush()
                os.fsync(self.file_handle.fileno())
                self.file_handle.close()
                self.file_handle = None
                self.is_closed = True

    async def write_entry(self, entry: WALEntry) -> bool:
        """Write an entry to the segment"""
        if self.is_closed:
            return False

        try:
            entry_bytes = entry.to_bytes()

            with self.lock:
                if self.file_handle is None:
                    await self.open()

                self.file_handle.write(entry_bytes)
                self.current_size += len(entry_bytes)
                self.entry_count += 1

                if self.first_sequence is None:
                    self.first_sequence = entry.sequence_number
                self.last_sequence = entry.sequence_number

                if self.config.sync_on_write:
                    self.file_handle.flush()
                    os.fsync(self.file_handle.fileno())

            return True

        except Exception as e:
            logger.error(f"Failed to write WAL entry: {e}")
            return False

    def is_full(self) -> bool:
        """Check if segment is full"""
        max_size = self.config.segment_size_mb * 1024 * 1024
        return self.current_size >= max_size

    async def read_entries(
        self, from_sequence: Optional[int] = None
    ) -> AsyncIterator[WALEntry]:
        """Read entries from segment"""
        if not os.path.exists(self.file_path):
            return

        async with aiofiles.open(self.file_path, "rb") as f:
            while True:
                # Read header
                header_data = await f.read(12)
                if len(header_data) < 12:
                    break

                try:
                    magic_int, checksum, length = struct.unpack(">III", header_data)

                    # Read data
                    data = await f.read(length)
                    if len(data) < length:
                        break

                    # Parse entry
                    full_data = header_data + data
                    entry = WALEntry.from_bytes(full_data)

                    if from_sequence is None or entry.sequence_number >= from_sequence:
                        yield entry

                except Exception as e:
                    logger.error(f"Error reading WAL entry: {e}")
                    break


class WriteAheadLog:
    """Write-Ahead Log implementation"""

    def __init__(self, config: WALConfig = None):
        self.config = config or WALConfig()
        self.segments: Dict[int, WALSegment] = {}
        self.active_segment: Optional[WALSegment] = None
        self.sequence_number = 0
        self.segment_counter = 0
        self.lock = threading.RLock()
        self.sync_task: Optional[asyncio.Task] = None
        self.running = False
        self.stats = {
            "entries_written": 0,
            "bytes_written": 0,
            "segments_created": 0,
            "sync_operations": 0,
            "write_errors": 0,
        }

        # Thread pool for I/O operations
        self.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="wal-")

    async def start(self):
        """Start the WAL"""
        self.running = True

        # Create WAL directory
        os.makedirs(self.config.wal_dir, exist_ok=True)

        # Load existing segments
        await self._load_existing_segments()

        # Create initial segment if needed
        if not self.active_segment:
            await self._create_new_segment()

        # Start periodic sync task
        if not self.config.sync_on_write:
            self.sync_task = asyncio.create_task(self._periodic_sync())

        logger.info(f"WAL started with {len(self.segments)} segments")

    async def stop(self):
        """Stop the WAL"""
        self.running = False

        if self.sync_task:
            try:
                self.sync_task.cancel()
                try:
                    await self.sync_task
                except asyncio.CancelledError:
                    pass
            except RuntimeError as e:
                # Handle closed event loop gracefully
                if "Event loop is closed" in str(e):
                    logger.warning("Event loop already closed during WAL stop")
                else:
                    raise

        # Close all segments
        for segment in self.segments.values():
            await segment.close()

        self.executor.shutdown(wait=True)
        logger.info("WAL stopped")

    async def write_entry(
        self,
        entry_type: WALEntryType,
        key: Optional[str] = None,
        value: Optional[Any] = None,
        transaction_id: Optional[str] = None,
        metadata: Dict[str, Any] = None,
    ) -> int:
        """Write an entry to the WAL"""
        if not self.running:
            raise RuntimeError("WAL is not running")

        with self.lock:
            self.sequence_number += 1
            entry = WALEntry(
                entry_type=entry_type,
                sequence_number=self.sequence_number,
                timestamp=int(time.time() * 1000000),
                key=key,
                value=value,
                transaction_id=transaction_id,
                metadata=metadata or {},
            )

        try:
            # Check if we need to rotate the active segment
            if self.active_segment and self.active_segment.is_full():
                await self._rotate_segment()

            if not self.active_segment:
                await self._create_new_segment()

            # Write the entry
            success = await self.active_segment.write_entry(entry)

            if success:
                with self.lock:
                    self.stats["entries_written"] += 1
                    self.stats["bytes_written"] += len(entry.to_bytes())

                # Check for rotation based on entry count
                if (
                    self.active_segment.entry_count
                    % self.config.rotation_check_interval
                    == 0
                ):
                    if self.active_segment.is_full():
                        await self._rotate_segment()

                return entry.sequence_number
            else:
                with self.lock:
                    self.stats["write_errors"] += 1
                raise RuntimeError("Failed to write WAL entry")

        except Exception as e:
            with self.lock:
                self.stats["write_errors"] += 1
            logger.error(f"WAL write error: {e}")
            raise

    async def _load_existing_segments(self):
        """Load existing WAL segments"""
        wal_path = Path(self.config.wal_dir)
        if not wal_path.exists():
            return

        segment_files = sorted(wal_path.glob("wal-*.log"))

        for segment_file in segment_files:
            try:
                # Extract segment ID from filename
                segment_id = int(segment_file.stem.split("-")[1])

                segment = WALSegment(segment_id, str(segment_file), self.config)
                self.segments[segment_id] = segment

                # Update counters
                if segment_id >= self.segment_counter:
                    self.segment_counter = segment_id + 1

                # Find the latest sequence number
                if os.path.getsize(str(segment_file)) > 0:
                    try:
                        # Read entries to get the highest sequence number
                        last_sequence = 0
                        async for entry in segment.read_entries():
                            last_sequence = max(last_sequence, entry.sequence_number)

                        if last_sequence > self.sequence_number:
                            self.sequence_number = last_sequence

                    except Exception as e:
                        logger.warning(f"Error reading segment {segment_id}: {e}")

            except Exception as e:
                logger.warning(f"Error loading segment {segment_file}: {e}")

        # Set active segment to the latest one
        if self.segments:
            latest_id = max(self.segments.keys())
            self.active_segment = self.segments[latest_id]

            # Reopen active segment for writing
            await self.active_segment.close()
            await self.active_segment.open("ab")

    async def _create_new_segment(self):
        """Create a new segment"""
        segment_id = self.segment_counter
        self.segment_counter += 1

        segment_path = os.path.join(self.config.wal_dir, f"wal-{segment_id:06d}.log")

        segment = WALSegment(segment_id, segment_path, self.config)
        await segment.open("ab")

        with self.lock:
            self.segments[segment_id] = segment
            self.active_segment = segment
            self.stats["segments_created"] += 1

        logger.debug(f"Created new WAL segment: {segment_id}")

    async def _rotate_segment(self):
        """Rotate to a new segment"""
        if self.active_segment:
            await self.active_segment.close()

        await self._create_new_segment()

        # Clean up old segments if needed
        await self._cleanup_old_segments()

    async def _cleanup_old_segments(self):
        """Clean up old segments beyond max_segments limit"""
        if len(self.segments) <= self.config.max_segments:
            return

        # Sort segments by ID and remove oldest
        sorted_segments = sorted(self.segments.keys())
        segments_to_remove = sorted_segments[: -self.config.max_segments]

        for segment_id in segments_to_remove:
            segment = self.segments.pop(segment_id)
            await segment.close()

            try:
                os.remove(segment.file_path)
                logger.debug(f"Removed old WAL segment: {segment_id}")
            except Exception as e:
                logger.warning(f"Failed to remove segment {segment_id}: {e}")

    async def _periodic_sync(self):
        """Periodic sync task"""
        while self.running:
            try:
                await asyncio.sleep(self.config.sync_interval_ms / 1000)
                await self.sync()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Periodic sync error: {e}")

    async def sync(self):
        """Force sync all pending writes"""
        if self.active_segment and self.active_segment.file_handle:
            with self.active_segment.lock:
                if self.active_segment.file_handle:
                    self.active_segment.file_handle.flush()
                    os.fsync(self.active_segment.file_handle.fileno())

            with self.lock:
                self.stats["sync_operations"] += 1

    async def read_entries(
        self, from_sequence: Optional[int] = None, to_sequence: Optional[int] = None
    ) -> AsyncIterator[WALEntry]:
        """Read entries from WAL"""
        sorted_segments = sorted(self.segments.keys())

        for segment_id in sorted_segments:
            segment = self.segments[segment_id]

            async for entry in segment.read_entries(from_sequence):
                if to_sequence is not None and entry.sequence_number > to_sequence:
                    return

                yield entry

    async def recover(self, callback) -> int:
        """Recover from WAL by replaying entries"""
        if not callable(callback):
            raise ValueError("Recovery callback must be callable")

        recovered_entries = 0
        batch = []

        async for entry in self.read_entries():
            batch.append(entry)

            if len(batch) >= self.config.recovery_batch_size:
                try:
                    await callback(batch)
                    recovered_entries += len(batch)
                    batch = []
                except Exception as e:
                    logger.error(f"Recovery callback failed: {e}")
                    raise

        # Process remaining entries
        if batch:
            try:
                await callback(batch)
                recovered_entries += len(batch)
            except Exception as e:
                logger.error(f"Recovery callback failed: {e}")
                raise

        logger.info(f"WAL recovery completed: {recovered_entries} entries")
        return recovered_entries

    async def checkpoint(self, sequence_number: int):
        """Create a checkpoint entry"""
        await self.write_entry(
            WALEntryType.CHECKPOINT, metadata={"checkpoint_sequence": sequence_number}
        )

    async def truncate_before(self, sequence_number: int):
        """Truncate WAL before a sequence number (after checkpoint)"""
        # Close and remove segments that only contain old entries
        segments_to_remove = []

        for segment_id, segment in self.segments.items():
            if (
                segment.last_sequence is not None
                and segment.last_sequence < sequence_number
                and segment != self.active_segment
            ):
                segments_to_remove.append(segment_id)

        for segment_id in segments_to_remove:
            segment = self.segments.pop(segment_id)
            await segment.close()

            try:
                os.remove(segment.file_path)
                logger.debug(f"Truncated WAL segment: {segment_id}")
            except Exception as e:
                logger.warning(f"Failed to remove segment {segment_id}: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Get WAL statistics"""
        with self.lock:
            stats = self.stats.copy()

        stats.update(
            {
                "active_segments": len(self.segments),
                "current_sequence": self.sequence_number,
                "active_segment_id": (
                    self.active_segment.segment_id if self.active_segment else None
                ),
                "active_segment_size": (
                    self.active_segment.current_size if self.active_segment else 0
                ),
                "config": {
                    "segment_size_mb": self.config.segment_size_mb,
                    "max_segments": self.config.max_segments,
                    "sync_interval_ms": self.config.sync_interval_ms,
                    "sync_on_write": self.config.sync_on_write,
                },
            }
        )

        return stats


# Convenience functions for common operations
async def create_wal(wal_dir: str = "wal", segment_size_mb: int = 64) -> WriteAheadLog:
    """Create and start a WAL instance"""
    config = WALConfig(wal_dir=wal_dir, segment_size_mb=segment_size_mb)
    wal = WriteAheadLog(config)
    await wal.start()
    return wal


async def write_set_operation(wal: WriteAheadLog, key: str, value: Any) -> int:
    """Write a SET operation to WAL"""
    return await wal.write_entry(WALEntryType.SET, key=key, value=value)


async def write_delete_operation(wal: WriteAheadLog, key: str) -> int:
    """Write a DELETE operation to WAL"""
    return await wal.write_entry(WALEntryType.DELETE, key=key)
