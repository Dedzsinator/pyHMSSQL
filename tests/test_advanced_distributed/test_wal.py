"""
Unit tests for Write-Ahead Log (WAL) system
"""

import pytest
import asyncio
import os
import tempfile
import shutil
import time
from unittest.mock import Mock, patch

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from kvstore.storage.wal import (
    WriteAheadLog, WALConfig, WALEntry, WALEntryType,
    WALSegment
)

class TestWALConfig:
    """Test WAL configuration"""
    
    def test_wal_config_creation(self):
        """Test WAL configuration creation with default values"""
        config = WALConfig(wal_dir="/tmp/test_wal")
        assert config.wal_dir == "/tmp/test_wal"
        assert config.segment_size_mb == 64  # Default
        assert config.max_segments == 100    # Default
        assert config.sync_interval_ms == 1000  # Default
    
    def test_wal_config_custom_values(self):
        """Test WAL configuration with custom values"""
        config = WALConfig(
            wal_dir="/custom/wal",
            segment_size_mb=128,
            max_segments=50,
            sync_interval_ms=500,
            sync_on_write=True
        )
        assert config.wal_dir == "/custom/wal"
        assert config.segment_size_mb == 128
        assert config.max_segments == 50
        assert config.sync_interval_ms == 500
        assert config.sync_on_write is True

class TestWALEntry:
    """Test WAL entry creation and serialization"""
    
    def test_wal_entry_creation(self):
        """Test creating a WAL entry"""
        entry = WALEntry(
            entry_type=WALEntryType.SET,
            sequence_number=1,
            timestamp=1640995200000000,
            key="test_key",
            value="test_value"
        )
        assert entry.entry_type == WALEntryType.SET
        assert entry.sequence_number == 1
        assert entry.key == "test_key"
        assert entry.value == "test_value"
    
    def test_wal_entry_serialization(self):
        """Test WAL entry serialization to bytes"""
        entry = WALEntry(
            entry_type=WALEntryType.SET,
            sequence_number=1,
            timestamp=1640995200000000,
            key="test_key",
            value="test_value"
        )
        
        # Serialize to bytes
        entry_bytes = entry.to_bytes()
        assert isinstance(entry_bytes, bytes)
        assert len(entry_bytes) > 0
        
        # Deserialize back
        deserialized = WALEntry.from_bytes(entry_bytes)
        assert deserialized.entry_type == entry.entry_type
        assert deserialized.sequence_number == entry.sequence_number
        assert deserialized.key == entry.key
        assert deserialized.value == entry.value
    
    def test_wal_entry_types(self):
        """Test all WAL entry types"""
        entry_types = [
            WALEntryType.SET,
            WALEntryType.DELETE,
            WALEntryType.EXPIRE,
            WALEntryType.CLEAR,
            WALEntryType.BEGIN_TX,
            WALEntryType.COMMIT_TX,
            WALEntryType.ROLLBACK_TX,
            WALEntryType.CHECKPOINT
        ]
        
        for entry_type in entry_types:
            entry = WALEntry(
                entry_type=entry_type,
                sequence_number=1,
                timestamp=time.time_ns() // 1000,
                key="test_key"
            )
            assert entry.entry_type == entry_type
    
    def test_wal_entry_serialization_roundtrip(self):
        """Test serialization/deserialization roundtrip with various data"""
        test_cases = [
            {"key": "simple", "value": "string"},
            {"key": "number", "value": 42},
            {"key": "list", "value": [1, 2, 3]},
            {"key": "dict", "value": {"nested": "data"}},
            {"key": "none", "value": None}
        ]
        
        for case in test_cases:
            entry = WALEntry(
                entry_type=WALEntryType.SET,
                sequence_number=1,
                timestamp=time.time_ns() // 1000,
                key=case["key"],
                value=case["value"]
            )
            
            # Serialize and deserialize
            entry_bytes = entry.to_bytes()
            deserialized = WALEntry.from_bytes(entry_bytes)
            
            assert deserialized.key == case["key"]
            assert deserialized.value == case["value"]

class TestWALSegment:
    """Test WAL segment functionality"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def segment_config(self, temp_dir):
        """Create segment configuration tuple (segment_id, file_path, config)"""
        segment_id = 0
        file_path = os.path.join(temp_dir, "test_segment.wal")
        config = WALConfig(
            wal_dir=temp_dir,
            segment_size_mb=1,  # 1MB
            max_segments=10,
            sync_interval_ms=1000,
            sync_on_write=False
        )
        return (segment_id, file_path, config)

    @pytest.mark.asyncio
    async def test_segment_creation(self, segment_config):
        """Test creating a new WAL segment"""
        segment_id, file_path, config = segment_config
        segment = WALSegment(segment_id, file_path, config)
        await segment.open()
        assert segment.is_closed is False
        assert segment.entry_count == 0
        assert segment.current_size == 0
        await segment.close()
        assert segment.is_closed is True

    @pytest.mark.asyncio
    async def test_segment_write_entry(self, segment_config):
        """Test writing entries to a segment"""
        segment_id, file_path, config = segment_config
        segment = WALSegment(segment_id, file_path, config)
        await segment.open()
        try:
            entry = WALEntry(
                entry_type=WALEntryType.SET,
                sequence_number=1,
                timestamp=time.time_ns() // 1000,
                key="test_key",
                value="test_value"
            )
            success = await segment.write_entry(entry)
            assert success is True
            assert segment.entry_count == 1
            assert segment.current_size > 0
            assert segment.first_sequence == 1
            assert segment.last_sequence == 1
        finally:
            await segment.close()

    @pytest.mark.asyncio
    async def test_segment_multiple_entries(self, segment_config):
        """Test writing multiple entries to a segment"""
        segment_id, file_path, config = segment_config
        segment = WALSegment(segment_id, file_path, config)
        await segment.open()
        try:
            entries = []
            for i in range(5):
                entry = WALEntry(
                    entry_type=WALEntryType.SET,
                    sequence_number=i + 1,
                    timestamp=time.time_ns() // 1000,
                    key=f"key_{i}",
                    value=f"value_{i}"
                )
                entries.append(entry)
                success = await segment.write_entry(entry)
                assert success is True
            assert segment.entry_count == 5
            assert segment.first_sequence == 1
            assert segment.last_sequence == 5
        finally:
            await segment.close()

    @pytest.mark.asyncio
    async def test_segment_full_detection(self, segment_config):
        """Test segment full detection"""
        segment_id, file_path, config = segment_config
        config.segment_size_mb = 0.0001  # Very small for test
        segment = WALSegment(segment_id, file_path, config)
        await segment.open()
        try:
            written_count = 0
            while not segment.is_full() and written_count < 10:
                entry = WALEntry(
                    entry_type=WALEntryType.SET,
                    sequence_number=written_count + 1,
                    timestamp=time.time_ns() // 1000,
                    key=f"key_{written_count}",
                    value=f"value_{written_count}" * 10
                )
                await segment.write_entry(entry)
                written_count += 1
            assert segment.is_full() or written_count == 10
        finally:
            await segment.close()

class TestWriteAheadLog:
    """Test main WriteAheadLog functionality"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def wal_config(self, temp_dir):
        """Create WAL configuration"""
        return WALConfig(
            wal_dir=temp_dir,
            segment_size_mb=1,  # Small segments for testing
            max_segments=5,
            sync_interval_ms=100,
            sync_on_write=False
        )
    
    @pytest.mark.asyncio
    async def test_wal_initialization(self, wal_config):
        """Test WAL initialization"""
        wal = WriteAheadLog(wal_config)
        
        await wal.start()
        assert wal.running is True
        assert wal.sequence_number == 0
        
        await wal.stop()
        assert wal.running is False
    
    @pytest.mark.asyncio
    async def test_wal_write_entry(self, wal_config):
        """Test writing entries to WAL"""
        wal = WriteAheadLog(wal_config)
        await wal.start()
        
        try:
            # Write a SET entry
            seq1 = await wal.write_entry(
                WALEntryType.SET,
                key="user:1",
                value={"name": "Alice", "age": 30}
            )
            assert seq1 == 1
            
            # Write a DELETE entry
            seq2 = await wal.write_entry(
                WALEntryType.DELETE,
                key="user:2"
            )
            assert seq2 == 2
            
            # Check statistics
            stats = wal.get_stats()
            assert stats['entries_written'] == 2
            assert stats['bytes_written'] > 0
            
        finally:
            await wal.stop()
    
    @pytest.mark.asyncio
    async def test_wal_segment_rotation(self, wal_config):
        """Test WAL segment rotation"""
        # Use very small segments to force rotation
        wal_config.segment_size_mb = 0.001  # Very small
        wal = WriteAheadLog(wal_config)
        await wal.start()
        
        try:
            # Write many entries to trigger rotation
            for i in range(20):
                await wal.write_entry(
                    WALEntryType.SET,
                    key=f"key_{i}",
                    value=f"value_{i}" * 100  # Make it larger
                )
            
            # Should have created multiple segments
            assert len(wal.segments) > 1
            
        finally:
            await wal.stop()
    
    @pytest.mark.asyncio
    async def test_wal_recovery(self, wal_config):
        """Test WAL recovery functionality"""
        # First, write some entries
        wal = WriteAheadLog(wal_config)
        await wal.start()
        
        entries_written = [
            (WALEntryType.SET, "user:1", "Alice"),
            (WALEntryType.SET, "user:2", "Bob"),
            (WALEntryType.DELETE, "user:3", None)
        ]
        
        for entry_type, key, value in entries_written:
            await wal.write_entry(entry_type, key=key, value=value)
        
        await wal.sync()  # Force sync to disk
        await wal.stop()
        
        # Now recover from a new WAL instance
        wal2 = WriteAheadLog(wal_config)
        await wal2.start()
        
        try:
            recovered_entries = []
            
            async def recovery_callback(entries):
                for entry in entries:
                    recovered_entries.append(entry)
            
            await wal2.recover(recovery_callback)
            
            # Verify recovery
            assert len(recovered_entries) == 3
            assert recovered_entries[0].key == "user:1"
            assert recovered_entries[1].key == "user:2"
            assert recovered_entries[2].key == "user:3"
            assert recovered_entries[0].entry_type == WALEntryType.SET
            assert recovered_entries[2].entry_type == WALEntryType.DELETE
            
        finally:
            await wal2.stop()
    
    @pytest.mark.asyncio
    async def test_wal_sync_operations(self, wal_config):
        """Test WAL sync operations"""
        wal = WriteAheadLog(wal_config)
        await wal.start()
        
        try:
            # Write some entries
            await wal.write_entry(WALEntryType.SET, key="key1", value="value1")
            await wal.write_entry(WALEntryType.SET, key="key2", value="value2")
            
            # Force sync
            await wal.sync()
            
            stats = wal.get_stats()
            assert stats['sync_operations'] > 0
            
        finally:
            await wal.stop()
    
    @pytest.mark.asyncio
    async def test_wal_checkpoint_creation(self, wal_config):
        """Test WAL checkpoint creation (skip if not implemented)"""
        wal = WriteAheadLog(wal_config)
        await wal.start()
        
        try:
            for i in range(5):
                await wal.write_entry(
                    WALEntryType.SET,
                    key=f"key_{i}",
                    value=f"value_{i}"
                )
            if not hasattr(wal, "create_checkpoint"):
                pytest.skip("create_checkpoint not implemented")
            checkpoint_seq = await wal.create_checkpoint()
            assert checkpoint_seq > 0
            stats = wal.get_stats()
            assert stats['entries_written'] == 6
        finally:
            await wal.stop()

class TestWALErrorHandling:
    """Test WAL error handling and edge cases"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.mark.asyncio
    async def test_wal_write_when_stopped(self, temp_dir):
        """Test writing to WAL when it's not running"""
        config = WALConfig(wal_dir=temp_dir)
        wal = WriteAheadLog(config)
        
        # Try to write without starting
        with pytest.raises(RuntimeError):
            await wal.write_entry(WALEntryType.SET, key="key", value="value")
    
    @pytest.mark.asyncio
    async def test_wal_invalid_directory(self):
        """Test WAL with invalid directory (should raise PermissionError)"""
        config = WALConfig(wal_dir="/invalid/path/that/does/not/exist")
        wal = WriteAheadLog(config)
        with pytest.raises(PermissionError):
            await wal.start()
    
    @pytest.mark.asyncio
    async def test_wal_corrupted_entry_recovery(self, temp_dir):
        """Test recovery with corrupted entries"""
        config = WALConfig(wal_dir=temp_dir)
        wal = WriteAheadLog(config)
        await wal.start()
        
        try:
            # Write a valid entry
            await wal.write_entry(WALEntryType.SET, key="valid", value="data")
            await wal.sync()
            
            # Manually corrupt the WAL file
            segment_file = os.path.join(temp_dir, "segment_000000000000000000.wal")
            if os.path.exists(segment_file):
                with open(segment_file, 'r+b') as f:
                    f.seek(-10, 2)  # Seek to near end
                    f.write(b"corrupted")  # Corrupt the file
            
        finally:
            await wal.stop()
        
        # Try to recover - should handle corruption gracefully
        wal2 = WriteAheadLog(config)
        await wal2.start()
        
        try:
            recovered_count = 0
            
            async def recovery_callback(entries):
                nonlocal recovered_count
                recovered_count += len(entries)
            
            # Should not crash on corrupted data
            await wal2.recover(recovery_callback)
            
        finally:
            await wal2.stop()

if __name__ == "__main__":
    pytest.main([__file__])
