# distutils: language = c++
# cython: language_level=3

"""
Swiss Table (flat_hash_map) Cython bindings for high-performance hash maps.

This module provides Python bindings to Google's Swiss Table implementation,
offering significant performance improvements over Python's built-in dict
for hot paths in the database engine.
"""

from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.pair cimport pair
from libcpp cimport bool
from libc.stdint cimport uint64_t
from cython.operator cimport dereference, preincrement
import json

# Include Google's flat_hash_map
cdef extern from "absl/container/flat_hash_map.h" namespace "absl":
    cdef cppclass flat_hash_map[K, V]:
        flat_hash_map() except +
        flat_hash_map(flat_hash_map&) except +
        
        # Core operations
        V& operator[](const K& key) except +
        bool contains(const K& key) const
        size_t size() const
        bool empty() const
        void clear()
        
        # Insertion/removal
        bool insert(const pair[K, V]& value) except +
        bool erase(const K& key)
        
        # Iteration
        cppclass iterator:
            pair[K, V] operator*()
            iterator operator++()
            bool operator==(iterator)
            bool operator!=(iterator)
        
        iterator begin()
        iterator end()
        iterator find(const K& key)

# String-to-string map for general purpose use
cdef class SwissMap:
    """
    High-performance hash map using Google's Swiss Table implementation.
    
    Provides significantly better performance than Python dict for large maps
    and frequent access patterns common in database systems.
    """
    cdef flat_hash_map[string, string] _map
    
    def __init__(self, dict initial_data=None):
        """Initialize SwissMap, optionally with initial data"""
        if initial_data:
            for key, value in initial_data.items():
                self._map[key.encode('utf-8')] = str(value).encode('utf-8')
    
    def __setitem__(self, key, value):
        """Set map[key] = value"""
        cdef string ckey = str(key).encode('utf-8')
        cdef string cvalue = str(value).encode('utf-8')
        self._map[ckey] = cvalue
    
    def __getitem__(self, key):
        """Get map[key]"""
        cdef string ckey = str(key).encode('utf-8')
        if not self._map.contains(ckey):
            raise KeyError(key)
        return self._map[ckey].decode('utf-8')
    
    def __delitem__(self, key):
        """Delete map[key]"""
        cdef string ckey = str(key).encode('utf-8')
        if not self._map.erase(ckey):
            raise KeyError(key)
    
    def __contains__(self, key):
        """Check if key in map"""
        cdef string ckey = str(key).encode('utf-8')
        return self._map.contains(ckey)
    
    def __len__(self):
        """Get map size"""
        return self._map.size()
    
    def __bool__(self):
        """Check if map is non-empty"""
        return not self._map.empty()
    
    def get(self, key, default=None):
        """Get value with default"""
        cdef string ckey = str(key).encode('utf-8')
        if self._map.contains(ckey):
            return self._map[ckey].decode('utf-8')
        return default
    
    def pop(self, key, default=None):
        """Remove and return value"""
        cdef string ckey = str(key).encode('utf-8')
        if self._map.contains(ckey):
            value = self._map[ckey].decode('utf-8')
            self._map.erase(ckey)
            return value
        if default is None:
            raise KeyError(key)
        return default
    
    def clear(self):
        """Clear all entries"""
        self._map.clear()
    
    def keys(self):
        """Get all keys"""
        cdef vector[string] result
        cdef flat_hash_map[string, string].iterator it = self._map.begin()
        cdef flat_hash_map[string, string].iterator end_it = self._map.end()
        while it != end_it:
            result.push_back(dereference(it).first)
            preincrement(it)
        
        return [key.decode('utf-8') for key in result]
    
    def values(self):
        """Get all values"""
        cdef vector[string] result
        cdef flat_hash_map[string, string].iterator it = self._map.begin()
        cdef flat_hash_map[string, string].iterator end_it = self._map.end()
        while it != end_it:
            result.push_back(dereference(it).second)
            preincrement(it)
        
        return [value.decode('utf-8') for value in result]
    
    def items(self):
        """Get all key-value pairs"""
        cdef vector[pair[string, string]] result
        cdef flat_hash_map[string, string].iterator it = self._map.begin()
        cdef flat_hash_map[string, string].iterator end_it = self._map.end()
        while it != end_it:
            result.push_back(dereference(it))
            preincrement(it)
        
        return [(item.first.decode('utf-8'), item.second.decode('utf-8')) for item in result]
    
    def update(self, other):
        """Update with another mapping"""
        if hasattr(other, 'items'):
            for key, value in other.items():
                self[key] = value
        else:
            for key, value in other:
                self[key] = value
    
    def to_dict(self):
        """Convert to Python dict"""
        return dict(self.items())
    
    def __repr__(self):
        """String representation"""
        return f"SwissMap({self.to_dict()!r})"

# Specialized uint64 -> string map for range metadata
cdef class RangeMap:
    """
    Specialized high-performance map for range metadata (uint64 -> string).
    
    Optimized for range key operations common in distributed database systems.
    """
    cdef flat_hash_map[uint64_t, string] _map
    
    def __init__(self):
        pass
    
    def __setitem__(self, uint64_t key, value):
        """Set map[key] = value"""
        cdef string cvalue = str(value).encode('utf-8')
        self._map[key] = cvalue
    
    def __getitem__(self, uint64_t key):
        """Get map[key]"""
        if not self._map.contains(key):
            raise KeyError(key)
        return self._map[key].decode('utf-8')
    
    def __delitem__(self, uint64_t key):
        """Delete map[key]"""
        if not self._map.erase(key):
            raise KeyError(key)
    
    def __contains__(self, uint64_t key):
        """Check if key in map"""
        return self._map.contains(key)
    
    def __len__(self):
        """Get map size"""
        return self._map.size()
    
    def get(self, uint64_t key, default=None):
        """Get value with default"""
        if self._map.contains(key):
            return self._map[key].decode('utf-8')
        return default
    
    def clear(self):
        """Clear all entries"""
        self._map.clear()
    
    def get_range_for_key(self, uint64_t search_key):
        """
        Find the range that contains the given key.
        
        Assumes ranges are stored with their start keys.
        Returns the range metadata for the range containing search_key.
        """
        cdef uint64_t best_start = 0
        cdef string best_range = b""
        cdef bool found = False
        
        cdef flat_hash_map[uint64_t, string].iterator it = self._map.begin()
        cdef flat_hash_map[uint64_t, string].iterator end_it = self._map.end()
        cdef uint64_t range_start
        while it != end_it:
            range_start = dereference(it).first
            if range_start <= search_key and range_start >= best_start:
                best_start = range_start
                best_range = dereference(it).second
                found = True
            preincrement(it)
        
        if found:
            return json.loads(best_range.decode('utf-8'))
        return None
    
    def items(self):
        """Get all key-value pairs"""
        cdef vector[pair[uint64_t, string]] result
        cdef flat_hash_map[uint64_t, string].iterator it = self._map.begin()
        cdef flat_hash_map[uint64_t, string].iterator end_it = self._map.end()
        while it != end_it:
            result.push_back(dereference(it))
            preincrement(it)
        
        return [(item.first, json.loads(item.second.decode('utf-8'))) for item in result]

# Specialized map for tombstone tracking (string -> uint64)
cdef class TombstoneMap:
    """
    High-performance map for tracking deleted keys and their deletion timestamps.
    
    Used for garbage collection and conflict resolution in distributed systems.
    """
    cdef flat_hash_map[string, uint64_t] _map
    
    def __init__(self):
        pass
    
    def mark_deleted(self, key, uint64_t timestamp):
        """Mark a key as deleted at the given timestamp"""
        cdef string ckey = str(key).encode('utf-8')
        self._map[ckey] = timestamp
    
    def is_deleted(self, key, uint64_t after_timestamp=0):
        """Check if key is deleted after the given timestamp"""
        cdef string ckey = str(key).encode('utf-8')
        if not self._map.contains(ckey):
            return False
        return self._map[ckey] > after_timestamp
    
    def get_deletion_time(self, key):
        """Get deletion timestamp for a key"""
        cdef string ckey = str(key).encode('utf-8')
        if not self._map.contains(ckey):
            return None
        return self._map[ckey]
    
    def cleanup_before(self, uint64_t timestamp):
        """Remove tombstones older than timestamp"""
        cdef vector[string] to_remove
        cdef flat_hash_map[string, uint64_t].iterator it = self._map.begin()
        cdef flat_hash_map[string, uint64_t].iterator end_it = self._map.end()
        
        while it != end_it:
            if dereference(it).second < timestamp:
                to_remove.push_back(dereference(it).first)
            preincrement(it)
        
        for key in to_remove:
            self._map.erase(key)
        
        return len(to_remove)
    
    def __len__(self):
        """Get number of tombstones"""
        return self._map.size()
    
    def clear(self):
        """Clear all tombstones"""
        self._map.clear()
