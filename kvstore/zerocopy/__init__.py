"""
Zero-copy optimizations for HyperKV
Provides memory-mapped files and buffer pools for efficient data transfer
"""

import os
import mmap
import threading
import time
from typing import Dict, Any, Optional, Union
from dataclasses import dataclass
from pathlib import Path
import logging
from io import BytesIO


# Custom exceptions
class ZeroCopyError(Exception):
    """Base exception for zero-copy operations"""

    pass


class BufferPoolExhausted(ZeroCopyError):
    """Exception raised when buffer pool is exhausted"""

    pass


class MemoryMappingError(ZeroCopyError):
    """Exception raised when memory mapping fails"""

    pass


logger = logging.getLogger(__name__)


@dataclass
class BufferInfo:
    """Information about a buffer"""

    size: int
    created_at: float
    last_used: float
    ref_count: int = 0


class BufferPool:
    """Memory buffer pool for zero-copy operations"""

    def __init__(
        self,
        min_buffers: int = 2,
        max_buffers: int = 100,
        default_size: int = 1024 * 1024,
    ):
        self.min_buffers = min_buffers
        self.max_buffers = max_buffers
        self.default_size = default_size
        self.buffers: Dict[int, BytesIO] = {}
        self.buffer_info: Dict[int, BufferInfo] = {}
        self.free_buffers = []
        self.next_id = 0
        self.lock = threading.Lock()
        self.deallocations = 0
        self.cache_hits = 0
        self.allocations = 0

        # Pre-allocate minimum buffers
        for _ in range(min_buffers):
            self._create_buffer(default_size)

    def _create_buffer(self, size: int) -> Optional[int]:
        """Create a new buffer and return its ID"""
        if len(self.buffers) >= self.max_buffers:
            return None

        buffer_id = self.next_id
        self.next_id += 1

        buffer = BytesIO()
        buffer.write(b"\x00" * size)
        buffer.seek(0)

        self.buffers[buffer_id] = buffer
        self.buffer_info[buffer_id] = BufferInfo(
            size=size, created_at=time.time(), last_used=time.time(), ref_count=0
        )
        self.free_buffers.append(buffer_id)
        self.allocations += 1
        return buffer_id

    def get_buffer(self, size: int = None) -> Optional[BytesIO]:
        """Get a buffer from the pool"""
        if size is None:
            size = self.default_size

        with self.lock:
            # Try to reuse existing buffer
            for buffer_id in self.free_buffers[:]:
                if buffer_id in self.buffers:
                    buffer_size = self.buffer_info[buffer_id].size
                    if buffer_size >= size:
                        self.free_buffers.remove(buffer_id)
                        self.buffer_info[buffer_id].last_used = time.time()
                        self.cache_hits += 1  # Track cache hit
                        self.buffer_info[buffer_id].ref_count += 1
                        self.buffers[buffer_id].seek(0)
                        self.buffers[buffer_id].truncate(0)
                        return self.buffers[buffer_id]

            # Create new buffer if under limit
            buffer_id = self._create_buffer(size)
            if buffer_id is not None:
                self.free_buffers.remove(buffer_id)
                self.buffer_info[buffer_id].ref_count += 1
                self.buffers[buffer_id].seek(0)
                self.buffers[buffer_id].truncate(0)
                return self.buffers[buffer_id]

        # If we can't create or reuse, create a temporary buffer
        temp_buffer = BytesIO()
        temp_buffer.write(b"\x00" * size)
        temp_buffer.seek(0)
        temp_buffer.truncate(0)
        return temp_buffer

    def return_buffer(self, buffer: BytesIO) -> bool:
        """Return a buffer to the pool"""
        with self.lock:
            for buffer_id, pool_buffer in self.buffers.items():
                if pool_buffer is buffer:
                    self.buffer_info[buffer_id].ref_count -= 1
                    if self.buffer_info[buffer_id].ref_count <= 0:
                        self.deallocations += 1  # Track deallocation
                        self.buffer_info[buffer_id].ref_count = 0
                        if buffer_id not in self.free_buffers:
                            self.free_buffers.append(buffer_id)
                    return True
        return False

    def get_stats(self) -> Dict[str, Any]:
        """Get buffer pool statistics"""
        with self.lock:
            buffer_sizes = [
                self.buffer_info[bid].size for bid in self.buffer_info.keys()
            ]
            # Active buffers = total allocated buffers when none are in use, otherwise in-use count
            in_use_count = sum(
                1 for info in self.buffer_info.values() if info.ref_count > 0
            )
            active_buffers = len(self.buffers) if in_use_count == 0 else in_use_count

            return {
                "total_buffers": len(self.buffers),
                "active_buffers": active_buffers,
                "free_buffers": len(self.free_buffers),
                "available_buffers": len(self.free_buffers),
                "max_buffers": self.max_buffers,
                "deallocations": self.deallocations,
                "cache_hits": self.cache_hits,
                "total_bytes_allocated": sum(buffer_sizes),
                "memory_efficiency": (
                    (self.cache_hits / max(self.allocations, 1)) * 100
                    if self.allocations > 0
                    else 0
                ),
                "min_buffers": self.min_buffers,
                "allocations": self.allocations,
                "buffer_sizes": buffer_sizes,
            }

    def get_buffer_context(self):
        """Get a buffer context manager for automatic buffer management"""
        from contextlib import contextmanager

        @contextmanager
        def buffer_context():
            buffer = self.get_buffer()
            try:
                yield buffer
            finally:
                self.return_buffer(buffer)

        return buffer_context()


class MemoryMappedBuffer:
    """Memory-mapped file buffer"""

    def __init__(self, file_path: str, mode: str = "r", size: int = None):
        self.file_path = file_path
        self.mode = mode
        self.requested_size = size
        self.file_handle = None
        self.mmap_obj = None
        self._size = None

        # Auto-open the file
        self.open()

    @property
    def size(self) -> int:
        """Get the size of the mapped buffer"""
        if self._size is not None:
            return self._size
        elif self.mmap_obj:
            return len(self.mmap_obj)
        elif self.file_handle:
            return os.path.getsize(self.file_path)
        return 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def open(self):
        """Open the memory-mapped file"""
        try:
            if not os.path.exists(self.file_path):
                raise MemoryMappingError(f"File not found: {self.file_path}")

            # Determine file mode
            if "w" in self.mode or "+" in self.mode:
                file_mode = "r+b"
                access_mode = mmap.ACCESS_WRITE
            else:
                file_mode = "rb"
                access_mode = mmap.ACCESS_READ

            self.file_handle = open(self.file_path, file_mode)

            # Get file size
            file_size = os.path.getsize(self.file_path)
            if file_size == 0:
                raise MemoryMappingError(f"Cannot map empty file: {self.file_path}")

            map_size = (
                self.requested_size
                if self.requested_size and self.requested_size <= file_size
                else file_size
            )
            self._size = map_size

            self.mmap_obj = mmap.mmap(
                self.file_handle.fileno(), map_size, access=access_mode
            )

        except Exception as e:
            self.close()
            raise MemoryMappingError(f"Failed to map file {self.file_path}: {e}")

    def close(self):
        """Close the memory-mapped file"""
        if self.mmap_obj:
            self.mmap_obj.close()
            self.mmap_obj = None
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None

    def read(self, length: int = None, offset: int = 0) -> bytes:
        """Read data from the mapped file"""
        if not self.mmap_obj:
            raise RuntimeError("Memory-mapped file not opened")

        if length is None:
            length = len(self.mmap_obj) - offset

        self.mmap_obj.seek(offset)
        return self.mmap_obj.read(length)

    def write(self, data: bytes, offset: int = 0) -> int:
        """Write data to the mapped file"""
        if not self.mmap_obj:
            raise RuntimeError("Memory-mapped file not opened")

        if "r" in self.mode and "+" not in self.mode:
            raise RuntimeError("Cannot write to read-only mapped file")

        self.mmap_obj.seek(offset)
        return self.mmap_obj.write(data)

    def __len__(self):
        """Get the size of the mapped file"""
        return self.size

    def __getitem__(self, key):
        """Get data using slice notation"""
        if not self.mmap_obj:
            raise RuntimeError("Memory-mapped file not opened")
        if isinstance(key, slice):
            start, stop, step = key.indices(len(self.mmap_obj))
            return self.mmap_obj[start:stop:step]
        else:
            return self.mmap_obj[key]

    def __setitem__(self, key, value):
        """Set data using slice notation"""
        if not self.mmap_obj:
            raise RuntimeError("Memory-mapped file not opened")
        if "r" in self.mode and "+" not in self.mode:
            raise ValueError("Memory-mapped file not opened for writing")
        self.mmap_obj[key] = value


class ZeroCopyManager:
    """Manager for zero-copy operations"""

    def __init__(self, buffer_pool_config: Dict[str, Any] = None):
        self.buffer_pool_config = buffer_pool_config or {}
        self.buffer_pool = BufferPool(**self.buffer_pool_config)
        self.mapped_files: Dict[str, MemoryMappedBuffer] = {}
        self.lock = threading.Lock()
        self.total_operations = 0
        self.bytes_transferred = 0

    def map_file(
        self, file_path: str, mode: str = "r", size: int = None
    ) -> MemoryMappedBuffer:
        """Memory map a file"""
        with self.lock:
            self.total_operations += 1

        if file_path in self.mapped_files:
            return self.mapped_files[file_path]

        try:
            mapped_buffer = MemoryMappedBuffer(file_path, mode, size)
            self.mapped_files[file_path] = mapped_buffer
            return mapped_buffer
        except Exception as e:
            raise MemoryMappingError(f"Failed to map file {file_path}: {e}")

    def unmap_file(self, file_path: str) -> bool:
        """Unmap a file"""
        with self.lock:
            if file_path in self.mapped_files:
                self.mapped_files[file_path].close()
                del self.mapped_files[file_path]
                return True
        return False

    def zero_copy_transfer(self, data: bytes, size: int) -> int:
        """Simulate zero-copy transfer using buffer pool"""
        with self.lock:
            self.total_operations += 1

        # Get buffer from pool
        buffer = self.buffer_pool.get_buffer(size)

        # Copy data to buffer (simulation)
        data_len = min(len(data), size)
        buffer.write(data[:data_len])

        # Update transfer statistics
        with self.lock:
            self.bytes_transferred += data_len

        # Return buffer to pool
        self.buffer_pool.return_buffer(buffer)

        return data_len

    def get_stats(self) -> Dict[str, Any]:
        """Get zero-copy manager statistics"""
        with self.lock:
            return {
                "buffer_pool": self.buffer_pool.get_stats(),
                "mapped_files": len(self.mapped_files),
                "total_operations": self.total_operations,
                "bytes_transferred": self.bytes_transferred,
            }

    def get_buffer_pool(self) -> "BufferPool":
        """Get the buffer pool instance"""
        return self.buffer_pool


# Export public API
__all__ = [
    "ZeroCopyError",
    "BufferPoolExhausted",
    "MemoryMappingError",
    "BufferInfo",
    "BufferPool",
    "MemoryMappedBuffer",
    "ZeroCopyManager",
]
