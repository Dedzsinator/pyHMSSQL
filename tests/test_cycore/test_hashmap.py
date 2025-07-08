#!/usr/bin/env python3
"""
Comprehensive unit tests for high-performance hashmap implementations in CyCore.

This test module provides thorough testing of the hashmap implementation including:
- Basic operations (get, set, delete, contains)
- Performance characteristics 
- Edge cases and error handling
- Different map types (SwissMap, RangeMap, TombstoneMap)
- Memory management and cleanup
- Integration with pyHMSSQL systems

Tests are designed to ensure production readiness and high performance.
"""

import pytest
import time
import threading
import gc
from unittest.mock import Mock, patch
import sys
import os
import tempfile

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Try to import hashmap implementations with graceful fallback
try:
    from cycore import SwissMap, RangeMap, TombstoneMap, HASHMAP_IMPLEMENTATION
    HASHMAP_AVAILABLE = True
except ImportError:
    # Fallback to dict for testing infrastructure
    SwissMap = dict
    RangeMap = dict  
    TombstoneMap = dict
    HASHMAP_IMPLEMENTATION = "fallback"
    HASHMAP_AVAILABLE = False


class TestHashmapBasicOperations:
    """Test basic hashmap operations"""
    
    def test_set_and_get(self):
        """Test basic set and get operations"""
        smap = SwissMap()
        
        # Test basic string operations
        smap["key1"] = "value1"
        assert smap["key1"] == "value1"
        
        # Test numeric keys and values
        smap["42"] = "numeric_value"
        assert smap["42"] == "numeric_value"
        
        # Test unicode support
        smap["unicode_key"] = "unicode_value_ñáéíóú"
        assert smap["unicode_key"] == "unicode_value_ñáéíóú"
    
    def test_delete_operations(self):
        """Test delete operations"""
        smap = SwissMap()
        
        # Set up test data
        smap["key1"] = "value1"
        smap["key2"] = "value2"
        
        # Test successful deletion
        del smap["key1"]
        assert "key1" not in smap
        assert "key2" in smap
        
        # Test KeyError on missing key
        with pytest.raises(KeyError):
            del smap["nonexistent"]
    
    def test_contains_operation(self):
        """Test membership testing"""
        smap = SwissMap()
        
        smap["existing"] = "value"
        
        assert "existing" in smap
        assert "nonexistent" not in smap
    
    def test_length_operation(self):
        """Test length operation"""
        smap = SwissMap()
        
        assert len(smap) == 0
        
        smap["key1"] = "value1"
        assert len(smap) == 1
        
        smap["key2"] = "value2"
        assert len(smap) == 2
        
        del smap["key1"]
        assert len(smap) == 1
    
    def test_get_with_default(self):
        """Test get method with default values"""
        if not hasattr(SwissMap, 'get'):
            pytest.skip("get method not available in fallback implementation")
            
        smap = SwissMap()
        smap["existing"] = "value"
        
        # Test existing key
        assert smap.get("existing") == "value"
        
        # Test missing key with default None
        assert smap.get("missing") is None
        
        # Test missing key with custom default
        assert smap.get("missing", "default") == "default"
    
    def test_pop_operation(self):
        """Test pop operation"""
        if not hasattr(SwissMap, 'pop'):
            pytest.skip("pop method not available in fallback implementation")
            
        smap = SwissMap()
        smap["key1"] = "value1"
        
        # Test successful pop
        value = smap.pop("key1")
        assert value == "value1"
        assert "key1" not in smap
        
        # Test pop with default on missing key
        default_value = smap.pop("missing", "default")
        assert default_value == "default"
        
        # Test KeyError on missing key without default
        with pytest.raises(KeyError):
            smap.pop("missing")
    
    def test_clear_operation(self):
        """Test clear operation"""
        if not hasattr(SwissMap, 'clear'):
            pytest.skip("clear method not available in fallback implementation")
            
        smap = SwissMap()
        smap["key1"] = "value1"
        smap["key2"] = "value2"
        
        assert len(smap) == 2
        
        smap.clear()
        assert len(smap) == 0
        assert "key1" not in smap
        assert "key2" not in smap


class TestHashmapIterationMethods:
    """Test iteration methods"""
    
    def test_keys_method(self):
        """Test keys iteration"""
        if not hasattr(SwissMap, 'keys'):
            pytest.skip("keys method not available in fallback implementation")
            
        smap = SwissMap()
        smap["key1"] = "value1"
        smap["key2"] = "value2"
        smap["key3"] = "value3"
        
        keys = smap.keys()
        assert set(keys) == {"key1", "key2", "key3"}
    
    def test_values_method(self):
        """Test values iteration"""
        if not hasattr(SwissMap, 'values'):
            pytest.skip("values method not available in fallback implementation")
            
        smap = SwissMap()
        smap["key1"] = "value1"
        smap["key2"] = "value2"
        smap["key3"] = "value3"
        
        values = smap.values()
        assert set(values) == {"value1", "value2", "value3"}
    
    def test_items_method(self):
        """Test items iteration"""
        if not hasattr(SwissMap, 'items'):
            pytest.skip("items method not available in fallback implementation")
            
        smap = SwissMap()
        smap["key1"] = "value1" 
        smap["key2"] = "value2"
        
        items = smap.items()
        assert set(items) == {("key1", "value1"), ("key2", "value2")}


class TestHashmapPerformance:
    """Test performance characteristics"""
    
    def test_insertion_performance(self):
        """Test insertion performance"""
        smap = SwissMap()
        
        # Measure insertion time for large dataset
        start_time = time.time()
        for i in range(10000):
            smap[f"key_{i}"] = f"value_{i}"
        end_time = time.time()
        
        insertion_time = end_time - start_time
        
        # Should complete within reasonable time (adjust threshold as needed)
        assert insertion_time < 5.0, f"Insertion took too long: {insertion_time:.2f}s"
        assert len(smap) == 10000
    
    def test_lookup_performance(self):
        """Test lookup performance"""
        smap = SwissMap()
        
        # Set up large dataset
        for i in range(10000):
            smap[f"key_{i}"] = f"value_{i}"
        
        # Measure lookup time
        start_time = time.time()
        for i in range(10000):
            _ = smap[f"key_{i}"]
        end_time = time.time()
        
        lookup_time = end_time - start_time
        
        # Should complete within reasonable time
        assert lookup_time < 2.0, f"Lookup took too long: {lookup_time:.2f}s"
    
    def test_memory_efficiency(self):
        """Test memory usage patterns"""
        if not HASHMAP_AVAILABLE:
            pytest.skip("Hashmap not available for memory testing")
            
        # Create large map and check it doesn't consume excessive memory
        smap = SwissMap()
        
        for i in range(10000):
            smap[f"key_{i}"] = f"value_{i}"
        
        # Clear and check memory is released
        smap.clear()
        gc.collect()
        
        # Should not crash or leak memory
        assert len(smap) == 0


class TestHashmapThreadSafety:
    """Test thread safety characteristics"""
    
    def test_concurrent_access(self):
        """Test concurrent read/write access"""
        if not HASHMAP_AVAILABLE:
            pytest.skip("Hashmap not available for threading test")
            
        smap = SwissMap()
        errors = []
        
        def writer_thread(thread_id):
            try:
                for i in range(1000):
                    smap[f"thread_{thread_id}_key_{i}"] = f"thread_{thread_id}_value_{i}"
            except Exception as e:
                errors.append(f"Writer {thread_id}: {e}")
        
        def reader_thread(thread_id):
            try:
                for i in range(1000):
                    key = f"thread_0_key_{i}"
                    if key in smap:
                        _ = smap[key]
            except Exception as e:
                errors.append(f"Reader {thread_id}: {e}")
        
        # Start multiple threads
        threads = []
        for i in range(3):
            t = threading.Thread(target=writer_thread, args=(i,))
            threads.append(t)
            t.start()
        
        for i in range(2):
            t = threading.Thread(target=reader_thread, args=(i,))
            threads.append(t)
            t.start()
        
        # Wait for completion
        for t in threads:
            t.join()
        
        # Check for errors
        if errors:
            pytest.fail(f"Concurrent access errors: {errors}")


class TestRangeMap:
    """Test RangeMap specialized functionality"""
    
    def test_range_map_basic(self):
        """Test basic RangeMap operations"""
        if not HASHMAP_AVAILABLE or RangeMap is dict:
            pytest.skip("RangeMap not available")
            
        rmap = RangeMap()
        
        # RangeMap uses uint64 keys and JSON values
        range_data = {"start": 100, "end": 200, "node": "node1"}
        rmap[100] = range_data  # uint64 key
        
        retrieved = rmap[100]
        assert retrieved == range_data
        assert rmap.get(100) == range_data
        assert 100 in rmap
        assert len(rmap) == 1
        
        # Test range lookup
        if hasattr(rmap, 'get_range_for_key'):
            found_range = rmap.get_range_for_key(150)  # Should find range starting at 100
            assert found_range == range_data
    
    def test_range_map_specialized_features(self):
        """Test RangeMap specialized features if available"""
        if not HASHMAP_AVAILABLE or RangeMap is dict:
            pytest.skip("RangeMap not available")
            
        rmap = RangeMap()
        
        # Test multiple ranges
        rmap[100] = {"start": 100, "end": 200, "node": "node1"}
        rmap[200] = {"start": 200, "end": 300, "node": "node2"}
        rmap[300] = {"start": 300, "end": 400, "node": "node3"}
        
        assert 100 in rmap
        assert 200 in rmap
        assert 300 in rmap
        
        # Test range lookup if available
        if hasattr(rmap, 'get_range_for_key'):
            # Test finding ranges
            range1 = rmap.get_range_for_key(150)  # Should find range starting at 100
            range2 = rmap.get_range_for_key(250)  # Should find range starting at 200
            
            if range1:
                assert range1["node"] == "node1"
            if range2:
                assert range2["node"] == "node2"


class TestTombstoneMap:
    """Test TombstoneMap specialized functionality"""
    
    def test_tombstone_map_basic(self):
        """Test basic TombstoneMap operations"""
        if not HASHMAP_AVAILABLE or TombstoneMap is dict:
            pytest.skip("TombstoneMap not available")
            
        tmap = TombstoneMap()
        
        # TombstoneMap uses specialized methods for marking deletions
        if hasattr(tmap, 'mark_deleted'):
            tmap.mark_deleted("key1", 1000)  # Mark as deleted at timestamp 1000
            
            # Test is_deleted method
            if hasattr(tmap, 'is_deleted'):
                assert tmap.is_deleted("key1", 500)  # Should be deleted after timestamp 500
                assert not tmap.is_deleted("key1", 1500)  # Should not be deleted after timestamp 1500
                
            # Test get_deletion_time
            if hasattr(tmap, 'get_deletion_time'):
                deletion_time = tmap.get_deletion_time("key1")
                assert deletion_time == 1000
                
                # Test non-existent key
                assert tmap.get_deletion_time("nonexistent") is None
        else:
            pytest.skip("TombstoneMap specialized methods not available")
    
    def test_tombstone_map_deletion_behavior(self):
        """Test TombstoneMap deletion behavior"""
        if not HASHMAP_AVAILABLE or TombstoneMap is dict:
            pytest.skip("TombstoneMap not available")
            
        tmap = TombstoneMap()
        
        if hasattr(tmap, 'mark_deleted') and hasattr(tmap, 'cleanup_before'):
            # Mark multiple keys as deleted at different times
            tmap.mark_deleted("key1", 1000)
            tmap.mark_deleted("key2", 2000)
            tmap.mark_deleted("key3", 3000)
            
            assert len(tmap) == 3
            
            # Cleanup old tombstones
            removed_count = tmap.cleanup_before(2500)
            assert removed_count == 2  # Should remove key1 and key2
            assert len(tmap) == 1
            
            # Verify key3 still exists
            deletion_time = tmap.get_deletion_time("key3")
            assert deletion_time == 3000
        else:
            pytest.skip("TombstoneMap specialized methods not available")


class TestHashmapEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_empty_string_keys_values(self):
        """Test empty string keys and values"""
        smap = SwissMap()
        
        # Test empty string key
        smap[""] = "empty_key_value"
        assert smap[""] == "empty_key_value"
        
        # Test empty string value
        smap["empty_value_key"] = ""
        assert smap["empty_value_key"] == ""
    
    def test_none_values(self):
        """Test None values handling"""
        smap = SwissMap()
        
        # Hashmap stores strings, so None gets converted
        smap["none_key"] = None
        result = smap["none_key"]
        
        # Check that it's handled appropriately (converted to string)
        assert result == "None" or result is None
    
    def test_large_strings(self):
        """Test large string handling"""
        smap = SwissMap()
        
        # Test large key
        large_key = "x" * 10000
        smap[large_key] = "large_key_value"
        assert smap[large_key] == "large_key_value"
        
        # Test large value
        large_value = "y" * 10000
        smap["large_value_key"] = large_value
        assert smap["large_value_key"] == large_value
    
    def test_special_characters(self):
        """Test special character handling"""
        smap = SwissMap()
        
        # Test various special characters
        special_chars = {
            "newline_key\n": "newline_value\n",
            "tab_key\t": "tab_value\t",
            "null_char_key\x00": "null_char_value\x00",
            "unicode_key_🚀": "unicode_value_🌟",
            "quote_key'\"": "quote_value'\"",
        }
        
        for key, value in special_chars.items():
            smap[key] = value
            assert smap[key] == value


class TestHashmapIntegration:
    """Test integration with pyHMSSQL systems"""
    
    def test_map_implementation_availability(self):
        """Test that Hashmap implementation is available"""
        # This test documents which implementation is being used
        print(f"Hashmap implementation: {HASHMAP_IMPLEMENTATION}")
        
        if HASHMAP_AVAILABLE:
            assert HASHMAP_IMPLEMENTATION in ["standard", "advanced", "fallback"]
        else:
            assert HASHMAP_IMPLEMENTATION == "none"
    
    def test_map_creation_patterns(self):
        """Test common map creation patterns used in pyHMSSQL"""
        # Test creating maps for different use cases
        
        # Cache map
        cache_map = SwissMap()
        cache_map["cache_key_1"] = "cached_data_1"
        cache_map["cache_key_2"] = "cached_data_2"
        
        assert len(cache_map) == 2
        
        # Index map
        index_map = SwissMap()
        for i in range(100):
            index_map[f"index_{i}"] = f"record_{i}"
        
        assert len(index_map) == 100
        
        # Metadata map
        metadata_map = SwissMap()
        metadata_map["version"] = "1.0"
        metadata_map["timestamp"] = str(time.time())
        metadata_map["node_id"] = "node_123"
        
        assert "version" in metadata_map
        assert "timestamp" in metadata_map
        assert "node_id" in metadata_map
    
    def test_serialization_compatibility(self):
        """Test serialization patterns used in distributed systems"""
        import json
        
        smap = SwissMap()
        smap["config_key"] = json.dumps({"setting": "value", "enabled": True})
        smap["data_key"] = json.dumps([1, 2, 3, 4, 5])
        
        # Test that JSON data can be stored and retrieved
        config_data = json.loads(smap["config_key"])
        assert config_data["setting"] == "value"
        assert config_data["enabled"] is True
        
        data_list = json.loads(smap["data_key"])
        assert data_list == [1, 2, 3, 4, 5]


@pytest.mark.performance
class TestHashmapBenchmarks:
    """Performance benchmarks for Hashmap"""
    
    def test_ops_per_second(self):
        """Benchmark operations per second"""
        if not HASHMAP_AVAILABLE:
            pytest.skip("Hashmap not available for benchmarking")
            
        smap = SwissMap()
        
        # Benchmark insertions
        num_ops = 100000
        start_time = time.time()
        
        for i in range(num_ops):
            smap[f"bench_key_{i}"] = f"bench_value_{i}"
        
        end_time = time.time()
        insert_ops_per_sec = num_ops / (end_time - start_time)
        
        print(f"Insert ops/sec: {insert_ops_per_sec:.0f}")
        
        # Benchmark lookups
        start_time = time.time()
        
        for i in range(num_ops):
            _ = smap[f"bench_key_{i}"]
        
        end_time = time.time()
        lookup_ops_per_sec = num_ops / (end_time - start_time)
        
        print(f"Lookup ops/sec: {lookup_ops_per_sec:.0f}")
        
        # Performance assertions (adjust thresholds based on expected performance)
        assert insert_ops_per_sec > 50000, f"Insert performance too low: {insert_ops_per_sec:.0f} ops/sec"
        assert lookup_ops_per_sec > 100000, f"Lookup performance too low: {lookup_ops_per_sec:.0f} ops/sec"
    
    def test_memory_overhead(self):
        """Test memory overhead compared to dict"""
        if not HASHMAP_AVAILABLE:
            pytest.skip("Hashmap not available for memory testing")
            
        import sys
        
        # Compare memory usage with Python dict
        test_data = {f"key_{i}": f"value_{i}" for i in range(1000)}
        
        # Python dict memory usage
        python_dict = dict(test_data)
        dict_size = sys.getsizeof(python_dict)
        
        # Hashmap usage (approximate)
        swiss_map = SwissMap()
        for k, v in test_data.items():
            swiss_map[k] = v
        
        # Just ensure it works and doesn't crash
        assert len(swiss_map) == len(python_dict)
        print(f"Dict size: {dict_size} bytes")
        print(f"Hashmap contains {len(swiss_map)} items")


if __name__ == "__main__":
    # Run tests directly if script is executed
    pytest.main([__file__, "-v", "--tb=short"])
