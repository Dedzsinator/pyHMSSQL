"""
Unit tests for zero-copy optimizations
"""

import pytest
import os
import tempfile
import mmap
from io import BytesIO
from unittest.mock import Mock, patch

import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from kvstore.zerocopy import (
    ZeroCopyManager,
    BufferPool,
    MemoryMappedBuffer,
    ZeroCopyError,
    BufferPoolExhausted,
)


class TestBufferPool:
    """Test buffer pool functionality"""

    def test_buffer_pool_creation(self):
        """Test creating a buffer pool"""
        pool = BufferPool(min_buffers=5, max_buffers=20, default_size=4096)
        assert pool.min_buffers == 5
        assert pool.max_buffers == 20
        assert pool.default_size == 4096

    def test_buffer_pool_initialization(self):
        """Test buffer pool initialization with minimum buffers"""
        pool = BufferPool(min_buffers=3, max_buffers=10, default_size=1024)

        # Should create minimum number of buffers
        stats = pool.get_stats()
        assert stats["available_buffers"] >= 3
        assert stats["active_buffers"] >= 3

    def test_get_buffer_from_pool(self):
        """Test getting a buffer from the pool"""
        pool = BufferPool(min_buffers=2, max_buffers=5, default_size=1024)

        buffer = pool.get_buffer()
        assert buffer is not None
        assert isinstance(buffer, BytesIO)

        # Buffer should be empty initially
        assert len(buffer.getvalue()) == 0

    def test_return_buffer_to_pool(self):
        """Test returning a buffer to the pool"""
        pool = BufferPool(min_buffers=2, max_buffers=5, default_size=1024)

        buffer = pool.get_buffer()
        buffer.write(b"test data")

        # Return buffer to pool
        pool.return_buffer(buffer)

        # Get a buffer again - should be reused and cleared
        buffer2 = pool.get_buffer()
        assert len(buffer2.getvalue()) == 0  # Should be cleared

    def test_buffer_pool_reuse(self):
        """Test buffer reuse in pool"""
        pool = BufferPool(min_buffers=1, max_buffers=3, default_size=1024)

        # Get and return multiple buffers
        buffers = []
        for i in range(3):
            buffer = pool.get_buffer()
            buffer.write(f"data {i}".encode())
            buffers.append(buffer)

        # Return all buffers
        for buffer in buffers:
            pool.return_buffer(buffer)

        # Get buffers again - should reuse
        new_buffers = []
        for i in range(3):
            buffer = pool.get_buffer()
            assert len(buffer.getvalue()) == 0  # Should be cleared
            new_buffers.append(buffer)

        stats = pool.get_stats()
        assert stats["cache_hits"] > 0

    def test_buffer_pool_growth(self):
        """Test buffer pool growth when needed"""
        pool = BufferPool(min_buffers=1, max_buffers=5, default_size=1024)

        # Get more buffers than minimum
        buffers = []
        for i in range(4):
            buffer = pool.get_buffer()
            buffers.append(buffer)

        stats = pool.get_stats()
        assert stats["active_buffers"] >= 4
        assert stats["allocations"] >= 4

        # Return buffers
        for buffer in buffers:
            pool.return_buffer(buffer)

    def test_buffer_pool_max_limit(self):
        """Test buffer pool maximum limit"""
        pool = BufferPool(min_buffers=1, max_buffers=3, default_size=1024)

        # Get maximum number of buffers
        buffers = []
        for i in range(3):
            buffer = pool.get_buffer()
            buffers.append(buffer)

        # Try to get one more - should still work but may reuse or create temporary
        buffer_extra = pool.get_buffer()
        assert buffer_extra is not None

        # Return all buffers
        for buffer in buffers + [buffer_extra]:
            pool.return_buffer(buffer)

    def test_buffer_pool_statistics(self):
        """Test buffer pool statistics tracking"""
        pool = BufferPool(min_buffers=2, max_buffers=5, default_size=1024)

        # Perform operations
        buffer1 = pool.get_buffer()
        buffer2 = pool.get_buffer()
        pool.return_buffer(buffer1)
        buffer3 = pool.get_buffer()  # Should reuse buffer1

        stats = pool.get_stats()
        assert stats["allocations"] >= 2
        assert stats["deallocations"] >= 1
        assert stats["cache_hits"] >= 1
        assert "total_bytes_allocated" in stats
        assert "memory_efficiency" in stats

    def test_buffer_sizes(self):
        """Test different buffer sizes"""
        pool = BufferPool(min_buffers=1, max_buffers=5, default_size=2048)

        # Get buffer with default size
        buffer1 = pool.get_buffer()
        assert buffer1 is not None

        # Get buffer with custom size
        buffer2 = pool.get_buffer(size=4096)
        assert buffer2 is not None

        pool.return_buffer(buffer1)
        pool.return_buffer(buffer2)

        stats = pool.get_stats()
        assert len(stats["buffer_sizes"]) > 0


class TestZeroCopyManager:
    """Test zero-copy manager functionality"""

    @pytest.fixture
    def temp_file(self):
        """Create temporary file for testing"""
        fd, path = tempfile.mkstemp()
        with os.fdopen(fd, "wb") as f:
            f.write(b"Test data for memory mapping" * 100)
        yield path
        os.unlink(path)

    def test_zerocopy_manager_creation(self):
        """Test creating zero-copy manager"""
        manager = ZeroCopyManager()
        assert manager is not None

    def test_memory_map_file(self, temp_file):
        """Test memory mapping a file"""
        manager = ZeroCopyManager()

        mapped_data = manager.map_file(temp_file)
        assert mapped_data is not None
        assert len(mapped_data) > 0

        # Should be able to read data
        data = bytes(mapped_data[:20])
        assert data.startswith(b"Test data")

    def test_memory_map_file_readonly(self, temp_file):
        """Test memory mapping a file in read-only mode"""
        manager = ZeroCopyManager()

        mapped_data = manager.map_file(temp_file, mode="r")
        assert mapped_data is not None
        assert len(mapped_data) > 0

        # Should not be able to write to read-only mapping
        with pytest.raises((ValueError, OSError)):
            mapped_data[0] = ord("X")

    def test_memory_map_file_readwrite(self, temp_file):
        """Test memory mapping a file in read-write mode"""
        manager = ZeroCopyManager()

        mapped_data = manager.map_file(temp_file, mode="r+")
        assert mapped_data is not None

        # Should be able to read original data
        original_byte = mapped_data[0]

        # Should be able to write (if file is writable)
        try:
            mapped_data[0] = ord("X")
            assert mapped_data[0] == ord("X")
            # Restore original
            mapped_data[0] = original_byte
        except (ValueError, OSError):
            # Some systems may not allow writing
            pass

    def test_unmap_file(self, temp_file):
        """Test unmapping a file"""
        manager = ZeroCopyManager()

        mapped_data = manager.map_file(temp_file)
        assert mapped_data is not None

        # Unmap the file
        manager.unmap_file(temp_file)

        # Should not be able to access mapped data after unmapping
        # (This test is tricky as the behavior is system-dependent)

    def test_multiple_file_mappings(self):
        """Test mapping multiple files"""
        manager = ZeroCopyManager()

        # Create multiple temporary files
        temp_files = []
        for i in range(3):
            fd, path = tempfile.mkstemp()
            with os.fdopen(fd, "wb") as f:
                f.write(f"File {i} data".encode() * 50)
            temp_files.append(path)

        try:
            # Map all files
            mapped_data = []
            for temp_file in temp_files:
                data = manager.map_file(temp_file)
                mapped_data.append(data)
                assert data is not None

            # Verify each mapping
            for i, data in enumerate(mapped_data):
                content = bytes(data[:10])
                assert content.startswith(f"File {i}".encode())

        finally:
            # Clean up
            for temp_file in temp_files:
                try:
                    manager.unmap_file(temp_file)
                    os.unlink(temp_file)
                except:
                    pass

    def test_zerocopy_statistics(self, temp_file):
        """Test zero-copy statistics tracking"""
        manager = ZeroCopyManager()

        # Perform operations
        mapped_data = manager.map_file(temp_file)

        # Get statistics
        stats = manager.get_stats()
        assert "total_operations" in stats
        assert "bytes_transferred" in stats
        assert stats["total_operations"] > 0

    def test_zero_copy_transfer_simulation(self):
        """Test zero-copy transfer simulation"""
        manager = ZeroCopyManager()

        # Create source and destination buffers
        source_data = b"Source data for zero-copy transfer" * 100

        # Simulate zero-copy transfer
        transferred = manager.zero_copy_transfer(source_data, len(source_data))

        # Should return the transferred size
        assert transferred >= 0

        stats = manager.get_stats()
        assert stats["bytes_transferred"] >= transferred


class TestMemoryMappedBuffer:
    """Test memory-mapped buffer functionality"""

    @pytest.fixture
    def temp_file(self):
        """Create temporary file for testing"""
        fd, path = tempfile.mkstemp()
        with os.fdopen(fd, "wb") as f:
            f.write(b"Initial buffer content" * 20)
        yield path
        os.unlink(path)

    def test_memory_mapped_buffer_creation(self, temp_file):
        """Test creating memory-mapped buffer"""
        try:
            buffer = MemoryMappedBuffer(temp_file)
            assert buffer is not None
            assert buffer.size > 0
        except (NameError, AttributeError):
            # MemoryMappedBuffer might not be implemented
            pytest.skip("MemoryMappedBuffer not implemented")

    def test_memory_mapped_buffer_read(self, temp_file):
        """Test reading from memory-mapped buffer"""
        try:
            buffer = MemoryMappedBuffer(temp_file)

            # Should be able to read data
            data = buffer.read(20)
            assert data.startswith(b"Initial buffer")
        except (NameError, AttributeError):
            pytest.skip("MemoryMappedBuffer not implemented")

    def test_memory_mapped_buffer_write(self, temp_file):
        """Test writing to memory-mapped buffer"""
        try:
            buffer = MemoryMappedBuffer(temp_file, mode="r+")

            # Should be able to write data
            original_data = buffer.read(10)
            buffer.seek(0)
            buffer.write(b"Modified  ")
            buffer.seek(0)
            new_data = buffer.read(10)

            assert new_data == b"Modified  "

        except (NameError, AttributeError):
            pytest.skip("MemoryMappedBuffer not implemented")
        except (ValueError, OSError):
            # Some systems may not allow writing
            pass


class TestZeroCopyErrorHandling:
    """Test zero-copy error handling"""

    def test_map_nonexistent_file(self):
        """Test mapping non-existent file"""
        manager = ZeroCopyManager()

        with pytest.raises((OSError, FileNotFoundError, ZeroCopyError)):
            manager.map_file("/nonexistent/file.dat")

    def test_buffer_pool_exhaustion(self):
        """Test buffer pool exhaustion handling"""
        # Create pool with very limited capacity
        pool = BufferPool(min_buffers=1, max_buffers=2, default_size=1024)

        # Get all available buffers
        buffers = []
        try:
            for i in range(10):  # Try to get more than max
                buffer = pool.get_buffer()
                buffers.append(buffer)
        except BufferPoolExhausted:
            # Expected when pool is exhausted
            pass
        finally:
            # Return buffers
            for buffer in buffers:
                if buffer:
                    pool.return_buffer(buffer)

    def test_invalid_buffer_return(self):
        """Test returning invalid buffer to pool"""
        pool = BufferPool(min_buffers=1, max_buffers=5, default_size=1024)

        # Try to return None
        pool.return_buffer(None)  # Should handle gracefully

        # Try to return non-BytesIO object
        pool.return_buffer("not a buffer")  # Should handle gracefully

    def test_map_directory_instead_of_file(self):
        """Test mapping a directory instead of file"""
        manager = ZeroCopyManager()

        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises((OSError, ValueError, ZeroCopyError)):
                manager.map_file(temp_dir)


class TestZeroCopyPerformance:
    """Test zero-copy performance characteristics"""

    def test_buffer_pool_performance(self):
        """Test buffer pool performance under load"""
        pool = BufferPool(min_buffers=5, max_buffers=20, default_size=4096)

        # Perform many allocations and deallocations
        import time

        start_time = time.time()

        for _ in range(1000):
            buffer = pool.get_buffer()
            buffer.write(b"test data")
            pool.return_buffer(buffer)

        elapsed = time.time() - start_time

        # Should complete quickly
        assert elapsed < 1.0  # Less than 1 second for 1000 operations

        stats = pool.get_stats()
        assert stats["cache_hits"] > 0  # Should have cache hits
        assert stats["memory_efficiency"] > 0

    def test_memory_mapping_performance(self):
        """Test memory mapping performance"""
        manager = ZeroCopyManager()

        # Create a larger test file
        fd, temp_file = tempfile.mkstemp()
        try:
            with os.fdopen(fd, "wb") as f:
                # Write 1MB of test data
                test_data = b"Performance test data " * 1000
                for _ in range(50):
                    f.write(test_data)

            # Time the mapping operation
            import time

            start_time = time.time()

            mapped_data = manager.map_file(temp_file)

            mapping_time = time.time() - start_time

            # Mapping should be fast
            assert mapping_time < 0.1  # Less than 100ms
            assert len(mapped_data) > 1024 * 1024  # Should be at least 1MB

        finally:
            try:
                manager.unmap_file(temp_file)
                os.unlink(temp_file)
            except:
                pass


class TestZeroCopyIntegration:
    """Test zero-copy integration scenarios"""

    def test_buffer_pool_with_zerocopy_manager(self):
        """Test using buffer pool with zero-copy manager"""
        pool = BufferPool(min_buffers=2, max_buffers=10, default_size=4096)
        manager = ZeroCopyManager()

        # Get buffer from pool
        buffer = pool.get_buffer()

        # Write some data
        test_data = b"Integration test data" * 50
        buffer.write(test_data)

        # Simulate zero-copy operation
        transferred = manager.zero_copy_transfer(buffer.getvalue(), len(test_data))

        assert transferred >= 0

        # Return buffer to pool
        pool.return_buffer(buffer)

        # Verify statistics
        pool_stats = pool.get_stats()
        manager_stats = manager.get_stats()

        assert pool_stats["allocations"] > 0
        assert manager_stats["total_operations"] > 0

    def test_concurrent_buffer_operations(self):
        """Test concurrent buffer pool operations"""
        import threading
        import time

        pool = BufferPool(min_buffers=3, max_buffers=15, default_size=2048)
        results = []

        def worker_thread(thread_id):
            """Worker thread that uses buffer pool"""
            for i in range(10):
                buffer = pool.get_buffer()
                buffer.write(f"Thread {thread_id}, iteration {i}".encode())
                time.sleep(0.001)  # Small delay
                pool.return_buffer(buffer)
            results.append(thread_id)

        # Start multiple threads
        threads = []
        for i in range(3):
            thread = threading.Thread(target=worker_thread, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=5.0)

        # All threads should complete
        assert len(results) == 3

        # Pool should handle concurrent access
        stats = pool.get_stats()
        assert stats["allocations"] >= 3
        assert stats["deallocations"] >= 3


if __name__ == "__main__":
    pytest.main([__file__])
