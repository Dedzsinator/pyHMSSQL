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
    
    def __init__(self, initial_size: int = 1024 * 1024, max_buffers: int = 100):
        self.initial_size = initial_size
        self.max_buffers = max_buffers
        self.buffers: Dict[int, bytearray] = {}
        self.buffer_info: Dict[int, BufferInfo] = {}
        self.free_buffers = []
        self.next_id = 0
        self.lock = threading.Lock()
        
    def get_buffer(self, size: int = None) -> Optional[bytearray]:
        """Get a buffer from the pool"""
        if size is None:
            size = self.initial_size
            
        with self.lock:
            # Try to reuse existing buffer
            for buffer_id in self.free_buffers:
                if buffer_id in self.buffers and len(self.buffers[buffer_id]) >= size:
                    self.free_buffers.remove(buffer_id)
                    self.buffer_info[buffer_id].last_used = time.time()
                    self.buffer_info[buffer_id].ref_count += 1
                    return self.buffers[buffer_id]
            
            # Create new buffer if under limit
            if len(self.buffers) < self.max_buffers:
                buffer_id = self.next_id
                self.next_id += 1
                
                buffer = bytearray(size)
                self.buffers[buffer_id] = buffer
                self.buffer_info[buffer_id] = BufferInfo(
                    size=size,
                    created_at=time.time(),
                    last_used=time.time(),
                    ref_count=1
                )
                return buffer
                
        return None
    
    def return_buffer(self, buffer: bytearray) -> bool:
        """Return a buffer to the pool"""
        with self.lock:
            for buffer_id, pool_buffer in self.buffers.items():
                if pool_buffer is buffer:
                    self.buffer_info[buffer_id].ref_count -= 1
                    if self.buffer_info[buffer_id].ref_count <= 0:
                        self.free_buffers.append(buffer_id)
                    return True
        return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get buffer pool statistics"""
        with self.lock:
            total_memory = sum(info.size for info in self.buffer_info.values())
            return {
                "total_buffers": len(self.buffers),
                "free_buffers": len(self.free_buffers),
                "active_buffers": len(self.buffers) - len(self.free_buffers),
                "total_memory": total_memory,
                "max_buffers": self.max_buffers
            }


class MemoryMappedBuffer:
    """Memory-mapped file buffer"""
    
    def __init__(self, file_path: str, size: int = None, read_only: bool = True):
        self.file_path = file_path
        self.read_only = read_only
        self.file_handle = None
        self.mmap_obj = None
        self.size = size
        
    def __enter__(self):
        self.open()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        
    def open(self):
        """Open the memory-mapped file"""
        if self.read_only:
            self.file_handle = open(self.file_path, 'rb')
            self.mmap_obj = mmap.mmap(self.file_handle.fileno(), 0, access=mmap.ACCESS_READ)
        else:
            # Ensure file exists and has correct size
            if not os.path.exists(self.file_path) and self.size:
                with open(self.file_path, 'wb') as f:
                    f.write(b'\x00' * self.size)
                    
            self.file_handle = open(self.file_path, 'r+b')
            self.mmap_obj = mmap.mmap(self.file_handle.fileno(), 0)
            
    def close(self):
        """Close the memory-mapped file"""
        if self.mmap_obj:
            self.mmap_obj.close()
            self.mmap_obj = None
            
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None
            
    def read(self, offset: int = 0, length: int = None) -> bytes:
        """Read data from the mapped file"""
        if not self.mmap_obj:
            raise RuntimeError("Memory-mapped file not opened")
            
        if length is None:
            length = len(self.mmap_obj) - offset
            
        return self.mmap_obj[offset:offset + length]
        
    def write(self, data: bytes, offset: int = 0):
        """Write data to the mapped file"""
        if not self.mmap_obj or self.read_only:
            raise RuntimeError("Memory-mapped file not opened for writing")
            
        self.mmap_obj[offset:offset + len(data)] = data
        
    def __len__(self):
        """Get the size of the mapped file"""
        return len(self.mmap_obj) if self.mmap_obj else 0
        
    def __getitem__(self, key):
        """Get data using slice notation"""
        if not self.mmap_obj:
            raise RuntimeError("Memory-mapped file not opened")
        return self.mmap_obj[key]
        
    def __setitem__(self, key, value):
        """Set data using slice notation"""
        if not self.mmap_obj or self.read_only:
            raise RuntimeError("Memory-mapped file not opened for writing")
        self.mmap_obj[key] = value


class ZeroCopyManager:
    """Manager for zero-copy operations"""
    
    def __init__(self):
        self.buffer_pool = BufferPool()
        self.mapped_files: Dict[str, MemoryMappedBuffer] = {}
        self.lock = threading.Lock()
        
    def map_file(self, file_path: str, read_only: bool = True) -> MemoryMappedBuffer:
        """Map a file into memory"""
        with self.lock:
            if file_path in self.mapped_files:
                return self.mapped_files[file_path]
                
            mapped_buffer = MemoryMappedBuffer(file_path, read_only=read_only)
            mapped_buffer.open()
            self.mapped_files[file_path] = mapped_buffer
            return mapped_buffer
            
    def unmap_file(self, file_path: str):
        """Unmap a file from memory"""
        with self.lock:
            if file_path in self.mapped_files:
                self.mapped_files[file_path].close()
                del self.mapped_files[file_path]
                
    def get_stats(self) -> Dict[str, Any]:
        """Get zero-copy manager statistics"""
        with self.lock:
            return {
                "mapped_files": len(self.mapped_files),
                "buffer_pool": self.buffer_pool.get_stats()
            }
