"""
Zero-Copy Optimizations for HyperKV
Implements memory-efficient operations that avoid unnecessary data copying
Includes memory mapping, buffer pooling, and direct I/O operations
"""

import os
import mmap
import asyncio
import threading
import time
from typing import Optional, Dict, List, Any, Union, ByteString, Protocol
from dataclasses import dataclass
from abc import ABC, abstractmethod
import weakref
from collections import deque
import logging
from contextlib import contextmanager
import struct

logger = logging.getLogger(__name__)


class BufferProtocol(Protocol):
    """Protocol for buffer-like objects"""
    def __buffer__(self, flags: int) -> memoryview: ...


@dataclass
class BufferInfo:
    """Information about a buffer"""
    size: int
    allocated_at: float
    last_used: float
    ref_count: int
    buffer_id: str
    is_pinned: bool = False


class ZeroCopyBuffer:
    """Zero-copy buffer with memory view support"""
    
    def __init__(self, size: int, buffer_id: str = None):
        self.size = size
        self.buffer_id = buffer_id or f"buf_{id(self)}"
        self._data = bytearray(size)
        self._memoryview = memoryview(self._data)
        self._ref_count = 0
        self._is_pinned = False
        self.allocated_at = time.time()
        self.last_used = time.time()
        self._position = 0  # Current write position for file-like operations
    
    def get_view(self, offset: int = 0, size: Optional[int] = None) -> memoryview:
        """Get a memory view of the buffer"""
        self.last_used = time.time()
        self._ref_count += 1
        
        if size is None:
            size = self.size - offset
        
        if offset + size > self.size:
            raise ValueError("View exceeds buffer size")
        
        return self._memoryview[offset:offset + size]
    
    def release_view(self):
        """Release a reference to the buffer"""
        if self._ref_count > 0:
            self._ref_count -= 1
    
    def pin(self):
        """Pin buffer in memory (prevent from being recycled)"""
        self._is_pinned = True
    
    def unpin(self):
        """Unpin buffer (allow recycling)"""
        self._is_pinned = False
    
    @property
    def is_available(self) -> bool:
        """Check if buffer can be recycled"""
        return self._ref_count == 0 and not self._is_pinned
    
    @property
    def info(self) -> BufferInfo:
        """Get buffer information"""
        return BufferInfo(
            size=self.size,
            allocated_at=self.allocated_at,
            last_used=self.last_used,
            ref_count=self._ref_count,
            buffer_id=self.buffer_id,
            is_pinned=self._is_pinned
        )
    
    def write(self, data: Union[bytes, bytearray, memoryview]) -> int:
        """Write data to buffer at current position (file-like interface)"""
        self.last_used = time.time()
        
        if isinstance(data, (bytes, bytearray)):
            data_view = memoryview(data)
        else:
            data_view = data
        
        data_len = len(data_view)
        
        if self._position + data_len > self.size:
            # Truncate to fit buffer
            data_len = self.size - self._position
            data_view = data_view[:data_len]
        
        if data_len > 0:
            self._memoryview[self._position:self._position + data_len] = data_view
            self._position += data_len
        
        return data_len
    
    def read(self, size: int = -1) -> bytes:
        """Read data from buffer at current position (file-like interface)"""
        self.last_used = time.time()
        
        if size == -1:
            # Read all remaining data
            size = self.size - self._position
        
        actual_size = min(size, self.size - self._position)
        
        if actual_size <= 0:
            return b''
        
        data = bytes(self._memoryview[self._position:self._position + actual_size])
        self._position += actual_size
        
        return data
    
    def seek(self, offset: int, whence: int = 0) -> int:
        """Seek to position (file-like interface)"""
        if whence == 0:  # SEEK_SET
            self._position = offset
        elif whence == 1:  # SEEK_CUR
            self._position += offset
        elif whence == 2:  # SEEK_END
            self._position = self.size + offset
        
        # Clamp position to valid range
        self._position = max(0, min(self._position, self.size))
        return self._position
    
    def tell(self) -> int:
        """Get current position (file-like interface)"""
        return self._position
    
    def getvalue(self) -> bytes:
        """Get buffer contents as bytes (BytesIO-like interface)"""
        self.last_used = time.time()
        return bytes(self._memoryview[:self._position])
    
    def truncate(self, size: Optional[int] = None):
        """Truncate buffer (BytesIO-like interface)"""
        if size is None:
            size = self._position
        
        if size < self.size:
            # Zero out the truncated portion
            self._memoryview[size:].tobytes() # Just to ensure it's accessed
            for i in range(size, self.size):
                self._data[i] = 0
        
        self._position = min(self._position, size)
    
    def write_at(self, offset: int, data: Union[bytes, bytearray, memoryview]):
        """Write data at specific offset without copying"""
        self.last_used = time.time()
        
        if isinstance(data, (bytes, bytearray)):
            data_view = memoryview(data)
        else:
            data_view = data
        
        if offset + len(data_view) > self.size:
            raise ValueError("Write exceeds buffer size")
        
        self._memoryview[offset:offset + len(data_view)] = data_view
    
    def read_at(self, offset: int, size: int) -> memoryview:
        """Read data at specific offset without copying"""
        self.last_used = time.time()
        
        if offset + size > self.size:
            raise ValueError("Read exceeds buffer size")
        
        return self._memoryview[offset:offset + size]
    
    def __del__(self):
        """Clean up buffer"""
        try:
            self._memoryview.release()
        except:
            pass


class BufferPool:
    """Pool of reusable zero-copy buffers"""
    
    def __init__(self, min_buffers: int = 10, max_buffers: int = 1000,
                 default_size: int = 64 * 1024):  # 64KB default
        self.min_buffers = min_buffers
        self.max_buffers = max_buffers
        self.default_size = default_size
        
        # Organize buffers by size for efficiency
        self.buffers_by_size: Dict[int, deque] = {}
        self.all_buffers: Dict[str, ZeroCopyBuffer] = {}
        self.lock = threading.RLock()
        
        self.stats = {
            'allocations': 0,
            'deallocations': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'total_bytes_allocated': 0,
            'peak_buffers': 0
        }
        
        # Pre-allocate minimum buffers
        self._preallocate_buffers()
    
    def _preallocate_buffers(self):
        """Pre-allocate minimum number of buffers"""
        with self.lock:
            if self.default_size not in self.buffers_by_size:
                self.buffers_by_size[self.default_size] = deque()
            
            for _ in range(self.min_buffers):
                buffer = ZeroCopyBuffer(self.default_size)
                self.buffers_by_size[self.default_size].append(buffer)
                self.all_buffers[buffer.buffer_id] = buffer
                self.stats['total_bytes_allocated'] += self.default_size
    
    def get_buffer(self, size: int = None) -> ZeroCopyBuffer:
        """Get a buffer from the pool"""
        if size is None:
            size = self.default_size
        
        with self.lock:
            # Try to find an available buffer of the right size
            if size in self.buffers_by_size:
                available_buffers = self.buffers_by_size[size]
                
                while available_buffers:
                    buffer = available_buffers.popleft()
                    if buffer.is_available:
                        self.stats['cache_hits'] += 1
                        self.stats['allocations'] += 1
                        return buffer
            
            # No available buffer found, create new one
            if len(self.all_buffers) < self.max_buffers:
                buffer = ZeroCopyBuffer(size)
                self.all_buffers[buffer.buffer_id] = buffer
                self.stats['cache_misses'] += 1
                self.stats['allocations'] += 1
                self.stats['total_bytes_allocated'] += size
                
                if len(self.all_buffers) > self.stats['peak_buffers']:
                    self.stats['peak_buffers'] = len(self.all_buffers)
                
                return buffer
            else:
                # Pool is full, force garbage collection
                self._cleanup_unused_buffers()
                
                # Try again after cleanup
                if size in self.buffers_by_size:
                    available_buffers = self.buffers_by_size[size]
                    if available_buffers:
                        buffer = available_buffers.popleft()
                        self.stats['cache_hits'] += 1
                        self.stats['allocations'] += 1
                        return buffer
                
                # Last resort: create new buffer anyway
                buffer = ZeroCopyBuffer(size)
                self.stats['cache_misses'] += 1
                self.stats['allocations'] += 1
                return buffer
    
    def return_buffer(self, buffer: ZeroCopyBuffer):
        """Return a buffer to the pool"""
        with self.lock:
            if buffer.is_available:
                size = buffer.size
                
                if size not in self.buffers_by_size:
                    self.buffers_by_size[size] = deque()
                
                self.buffers_by_size[size].append(buffer)
                self.stats['deallocations'] += 1
    
    def _cleanup_unused_buffers(self):
        """Clean up unused buffers"""
        current_time = time.time()
        cleanup_threshold = 60.0  # 1 minute
        
        buffers_to_remove = []
        
        for buffer_id, buffer in self.all_buffers.items():
            if (buffer.is_available and 
                current_time - buffer.last_used > cleanup_threshold):
                buffers_to_remove.append(buffer_id)
        
        for buffer_id in buffers_to_remove:
            buffer = self.all_buffers.pop(buffer_id)
            self.stats['total_bytes_allocated'] -= buffer.size
            
            # Remove from size-based index
            size = buffer.size
            if size in self.buffers_by_size:
                try:
                    self.buffers_by_size[size].remove(buffer)
                except ValueError:
                    pass
    
    @contextmanager
    def get_buffer_context(self, size: int = None):
        """Context manager for automatic buffer return"""
        buffer = self.get_buffer(size)
        try:
            yield buffer
        finally:
            self.return_buffer(buffer)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get buffer pool statistics"""
        with self.lock:
            stats = self.stats.copy()
            stats.update({
                'active_buffers': len(self.all_buffers),
                'available_buffers': sum(len(buffers) for buffers in self.buffers_by_size.values()),
                'buffer_sizes': list(self.buffers_by_size.keys()),
                'memory_efficiency': self._calculate_memory_efficiency()
            })
        
        return stats
    
    def _calculate_memory_efficiency(self) -> float:
        """Calculate memory efficiency ratio"""
        if self.stats['allocations'] == 0:
            return 0.0
        
        return self.stats['cache_hits'] / self.stats['allocations']


class MemoryMappedFile:
    """Memory-mapped file for zero-copy file operations"""
    
    def __init__(self, file_path: str, max_size: int = 1024 * 1024 * 1024):  # 1GB default
        self.file_path = file_path
        self.max_size = max_size
        self.file_handle = None
        self.mmap_handle = None
        self.current_size = 0
        self.lock = threading.RLock()
        self._is_open = False
    
    def open(self, mode: str = 'r+b'):
        """Open the memory-mapped file"""
        with self.lock:
            if self._is_open:
                return
            
            # Ensure file exists
            if not os.path.exists(self.file_path):
                with open(self.file_path, 'wb') as f:
                    f.write(b'\x00' * min(self.max_size, 1024))  # Initial size
            
            self.file_handle = open(self.file_path, mode)
            self.current_size = os.path.getsize(self.file_path)
            
            # Create memory map
            self.mmap_handle = mmap.mmap(
                self.file_handle.fileno(),
                0,  # Map entire file
                access=mmap.ACCESS_WRITE if 'w' in mode or '+' in mode else mmap.ACCESS_READ
            )
            
            self._is_open = True
    
    def close(self):
        """Close the memory-mapped file"""
        with self.lock:
            if self.mmap_handle:
                self.mmap_handle.flush()
                self.mmap_handle.close()
                self.mmap_handle = None
            
            if self.file_handle:
                self.file_handle.close()
                self.file_handle = None
            
            self._is_open = False
    
    def get_view(self, offset: int = 0, size: Optional[int] = None) -> memoryview:
        """Get a memory view of the mapped file"""
        if not self._is_open:
            raise RuntimeError("File is not open")
        
        with self.lock:
            if size is None:
                size = self.current_size - offset
            
            if offset + size > self.current_size:
                raise ValueError("View exceeds file size")
            
            return memoryview(self.mmap_handle)[offset:offset + size]
    
    def write_at(self, offset: int, data: Union[bytes, bytearray, memoryview]):
        """Write data at specific offset without copying"""
        if not self._is_open:
            raise RuntimeError("File is not open")
        
        with self.lock:
            if isinstance(data, (bytes, bytearray)):
                data_view = memoryview(data)
            else:
                data_view = data
            
            if offset + len(data_view) > self.current_size:
                # Expand file if necessary
                new_size = min(offset + len(data_view), self.max_size)
                self._expand_file(new_size)
            
            self.mmap_handle[offset:offset + len(data_view)] = data_view
    
    def read_at(self, offset: int, size: int) -> memoryview:
        """Read data at specific offset without copying"""
        if not self._is_open:
            raise RuntimeError("File is not open")
        
        with self.lock:
            if offset + size > self.current_size:
                raise ValueError("Read exceeds file size")
            
            return memoryview(self.mmap_handle)[offset:offset + size]
    
    def _expand_file(self, new_size: int):
        """Expand the file and remap"""
        if new_size <= self.current_size:
            return
        
        # Close current mapping
        if self.mmap_handle:
            self.mmap_handle.close()
        
        # Expand file
        self.file_handle.seek(new_size - 1)
        self.file_handle.write(b'\x00')
        self.file_handle.flush()
        
        # Create new mapping
        self.mmap_handle = mmap.mmap(
            self.file_handle.fileno(),
            0,
            access=mmap.ACCESS_WRITE
        )
        
        self.current_size = new_size
    
    def sync(self):
        """Synchronize changes to disk"""
        if self.mmap_handle:
            self.mmap_handle.flush()
    
    def __enter__(self):
        self.open()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class ZeroCopyQueue:
    """Lock-free queue for zero-copy message passing"""
    
    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self.queue = deque(maxlen=max_size)
        self.lock = threading.Lock()
        self.not_empty = threading.Condition(self.lock)
        self.stats = {
            'enqueued': 0,
            'dequeued': 0,
            'overflows': 0
        }
    
    def enqueue(self, item: memoryview) -> bool:
        """Enqueue a memory view without copying"""
        with self.not_empty:
            if len(self.queue) >= self.max_size:
                self.stats['overflows'] += 1
                return False
            
            # Store the memory view directly
            self.queue.append(item)
            self.stats['enqueued'] += 1
            self.not_empty.notify()
            return True
    
    def dequeue(self, timeout: Optional[float] = None) -> Optional[memoryview]:
        """Dequeue a memory view without copying"""
        with self.not_empty:
            deadline = None
            if timeout is not None:
                deadline = time.time() + timeout
            
            while len(self.queue) == 0:
                if timeout is not None:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        return None
                    self.not_empty.wait(remaining)
                else:
                    self.not_empty.wait()
            
            item = self.queue.popleft()
            self.stats['dequeued'] += 1
            return item
    
    def size(self) -> int:
        """Get queue size"""
        with self.lock:
            return len(self.queue)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        return self.stats.copy()


class ZeroCopyNetworkBuffer:
    """Network buffer optimized for zero-copy operations"""
    
    def __init__(self, buffer_pool: BufferPool):
        self.buffer_pool = buffer_pool
        self.read_buffer: Optional[ZeroCopyBuffer] = None
        self.write_buffer: Optional[ZeroCopyBuffer] = None
        self.read_offset = 0
        self.write_offset = 0
        self.lock = threading.Lock()
    
    def prepare_read(self, size: int) -> memoryview:
        """Prepare buffer for reading"""
        with self.lock:
            if self.read_buffer is None or self.read_buffer.size < size:
                if self.read_buffer:
                    self.buffer_pool.return_buffer(self.read_buffer)
                
                self.read_buffer = self.buffer_pool.get_buffer(size)
                self.read_offset = 0
            
            return self.read_buffer.get_view(self.read_offset, size)
    
    def prepare_write(self, size: int) -> memoryview:
        """Prepare buffer for writing"""
        with self.lock:
            if self.write_buffer is None or self.write_buffer.size < size:
                if self.write_buffer:
                    self.buffer_pool.return_buffer(self.write_buffer)
                
                self.write_buffer = self.buffer_pool.get_buffer(size)
                self.write_offset = 0
            
            return self.write_buffer.get_view(self.write_offset, size)
    
    def advance_read(self, bytes_read: int):
        """Advance read position"""
        with self.lock:
            self.read_offset += bytes_read
    
    def advance_write(self, bytes_written: int):
        """Advance write position"""
        with self.lock:
            self.write_offset += bytes_written
    
    def get_read_data(self, size: int) -> memoryview:
        """Get read data without copying"""
        with self.lock:
            if self.read_buffer is None:
                raise RuntimeError("No read buffer available")
            
            data = self.read_buffer.get_view(0, min(size, self.read_offset))
            return data
    
    def reset(self):
        """Reset buffers"""
        with self.lock:
            if self.read_buffer:
                self.buffer_pool.return_buffer(self.read_buffer)
                self.read_buffer = None
                self.read_offset = 0
            
            if self.write_buffer:
                self.buffer_pool.return_buffer(self.write_buffer)
                self.write_buffer = None
                self.write_offset = 0


class ZeroCopyManager:
    """Central manager for zero-copy operations"""
    
    def __init__(self):
        self.buffer_pool = BufferPool()
        self.mmap_files: Dict[str, MemoryMappedFile] = {}
        self.lock = threading.RLock()
        self.stats = {
            'total_operations': 0,
            'bytes_transferred': 0,
            'copy_operations_avoided': 0
        }
    
    def get_buffer_pool(self) -> BufferPool:
        """Get the buffer pool"""
        return self.buffer_pool
    
    def create_network_buffer(self) -> ZeroCopyNetworkBuffer:
        """Create a network buffer"""
        return ZeroCopyNetworkBuffer(self.buffer_pool)
    
    def create_queue(self, max_size: int = 10000) -> ZeroCopyQueue:
        """Create a zero-copy queue"""
        return ZeroCopyQueue(max_size)
    
    def open_mmap_file(self, file_path: str, max_size: int = None) -> MemoryMappedFile:
        """Open or get existing memory-mapped file"""
        with self.lock:
            if file_path in self.mmap_files:
                return self.mmap_files[file_path]
            
            mmap_file = MemoryMappedFile(
                file_path, 
                max_size or 1024 * 1024 * 1024
            )
            self.mmap_files[file_path] = mmap_file
            return mmap_file
    
    def close_mmap_file(self, file_path: str):
        """Close memory-mapped file"""
        with self.lock:
            if file_path in self.mmap_files:
                mmap_file = self.mmap_files.pop(file_path)
                mmap_file.close()
    
    def map_file(self, file_path: str, max_size: int = None) -> memoryview:
        """Map a file and return a memory view"""
        mmap_file = self.open_mmap_file(file_path, max_size)
        mmap_file.open()
        
        with self.lock:
            self.stats['total_operations'] += 1
        
        return mmap_file.get_view()
    
    def copy_between_views(self, src: memoryview, dst: memoryview) -> int:
        """Copy data between memory views efficiently"""
        size = min(len(src), len(dst))
        dst[:size] = src[:size]
        
        with self.lock:
            self.stats['total_operations'] += 1
            self.stats['bytes_transferred'] += size
            self.stats['copy_operations_avoided'] += 1
        
        return size
    
    def get_stats(self) -> Dict[str, Any]:
        """Get zero-copy manager statistics"""
        with self.lock:
            stats = self.stats.copy()
        
        stats.update({
            'buffer_pool_stats': self.buffer_pool.get_stats(),
            'active_mmap_files': len(self.mmap_files),
            'mmap_files': list(self.mmap_files.keys())
        })
        
        return stats
    
    def shutdown(self):
        """Shutdown zero-copy manager"""
        with self.lock:
            # Close all memory-mapped files
            for mmap_file in self.mmap_files.values():
                mmap_file.close()
            self.mmap_files.clear()


# Global zero-copy manager instance
_global_manager = None


def get_zero_copy_manager() -> ZeroCopyManager:
    """Get the global zero-copy manager"""
    global _global_manager
    if _global_manager is None:
        _global_manager = ZeroCopyManager()
    return _global_manager


# Convenience functions
def get_buffer(size: int = None) -> ZeroCopyBuffer:
    """Get a buffer from the global pool"""
    return get_zero_copy_manager().get_buffer_pool().get_buffer(size)


def return_buffer(buffer: ZeroCopyBuffer):
    """Return a buffer to the global pool"""
    get_zero_copy_manager().get_buffer_pool().return_buffer(buffer)


@contextmanager
def zero_copy_buffer(size: int = None):
    """Context manager for zero-copy buffer"""
    buffer = get_buffer(size)
    try:
        yield buffer
    finally:
        return_buffer(buffer)
