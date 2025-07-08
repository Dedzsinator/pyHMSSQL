# distutils: language = c++
# cython: language_level=3

"""
High-performance hashmap implementation using std::unordered_map.
This provides production-ready hash maps for the pyHMSSQL distributed database system.
"""

from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.pair cimport pair
from libcpp cimport bool
from libc.stdint cimport uint64_t
from cython.operator cimport dereference, preincrement
import json

# String-to-string map for general purpose use
cdef class SwissMap:
    """
    High-performance hash map using std::unordered_map.
    Provides similar interface to the original Swiss Table but without external deps.
    """
    cdef unordered_map[string, string] _map
    
    def __setitem__(self, key, value):
        """Set map[key] = value"""
        cdef string ckey = str(key).encode('utf-8')
        cdef string cvalue = str(value).encode('utf-8')
        self._map[ckey] = cvalue
    
    def __getitem__(self, key):
        """Get map[key]"""
        cdef string ckey = str(key).encode('utf-8')
        if self._map.find(ckey) == self._map.end():
            raise KeyError(key)
        return self._map[ckey].decode('utf-8')
    
    def __delitem__(self, key):
        """Delete map[key]"""
        cdef string ckey = str(key).encode('utf-8')
        if self._map.erase(ckey) == 0:
            raise KeyError(key)
    
    def __contains__(self, key):
        """Check if key in map"""
        cdef string ckey = str(key).encode('utf-8')
        return self._map.find(ckey) != self._map.end()
    
    def __len__(self):
        """Get map size"""
        return self._map.size()
    
    def get(self, key, default=None):
        """Get value with default"""
        cdef string ckey = str(key).encode('utf-8')
        if self._map.find(ckey) != self._map.end():
            return self._map[ckey].decode('utf-8')
        return default
    
    def pop(self, key, default=None):
        """Remove and return value"""
        cdef string ckey = str(key).encode('utf-8')
        if self._map.find(ckey) != self._map.end():
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
        cdef unordered_map[string, string].iterator it = self._map.begin()
        cdef unordered_map[string, string].iterator end_it = self._map.end()
        while it != end_it:
            result.push_back(dereference(it).first)
            preincrement(it)
        
        return [key.decode('utf-8') for key in result]
    
    def values(self):
        """Get all values"""
        cdef vector[string] result
        cdef unordered_map[string, string].iterator it = self._map.begin()
        cdef unordered_map[string, string].iterator end_it = self._map.end()
        while it != end_it:
            result.push_back(dereference(it).second)
            preincrement(it)
        
        return [value.decode('utf-8') for value in result]
    
    def items(self):
        """Get all key-value pairs"""
        cdef vector[pair[string, string]] result
        cdef unordered_map[string, string].iterator it = self._map.begin()
        cdef unordered_map[string, string].iterator end_it = self._map.end()
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

# Specialized map for range routing (uint64 -> string)
cdef class RangeMap:
    """
    Range-optimized map for database range routing.
    Maps range start keys (uint64) to range metadata (JSON strings).
    """
    cdef unordered_map[uint64_t, string] _map
    
    def __setitem__(self, uint64_t key, value):
        """Set map[key] = value"""
        cdef string cvalue = json.dumps(value).encode('utf-8')
        self._map[key] = cvalue
    
    def __getitem__(self, uint64_t key):
        """Get map[key]"""
        if self._map.find(key) == self._map.end():
            raise KeyError(key)
        return json.loads(self._map[key].decode('utf-8'))
    
    def __delitem__(self, uint64_t key):
        """Delete map[key]"""
        if self._map.erase(key) == 0:
            raise KeyError(key)
    
    def __contains__(self, uint64_t key):
        """Check if key in map"""
        return self._map.find(key) != self._map.end()
    
    def __len__(self):
        """Get map size"""
        return self._map.size()
    
    def get(self, uint64_t key, default=None):
        """Get value with default"""
        if self._map.find(key) != self._map.end():
            return json.loads(self._map[key].decode('utf-8'))
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
        
        cdef unordered_map[uint64_t, string].iterator it = self._map.begin()
        cdef unordered_map[uint64_t, string].iterator end_it = self._map.end()
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
        cdef unordered_map[uint64_t, string].iterator it = self._map.begin()
        cdef unordered_map[uint64_t, string].iterator end_it = self._map.end()
        while it != end_it:
            result.push_back(dereference(it))
            preincrement(it)
        
        return [(item.first, json.loads(item.second.decode('utf-8'))) for item in result]

# Specialized map for tombstone tracking (string -> uint64)
cdef class TombstoneMap:
    """
    Tombstone tracking for CRDT support.
    Maps keys to deletion timestamps for conflict resolution.
    """
    cdef unordered_map[string, uint64_t] _map
    
    def mark_deleted(self, key, uint64_t timestamp):
        """Mark a key as deleted at timestamp"""
        cdef string ckey = str(key).encode('utf-8')
        self._map[ckey] = timestamp
    
    def is_deleted(self, key, uint64_t after_timestamp):
        """Check if key was deleted after given timestamp"""
        cdef string ckey = str(key).encode('utf-8')
        if self._map.find(ckey) == self._map.end():
            return False
        return self._map[ckey] > after_timestamp
    
    def get_deletion_time(self, key):
        """Get deletion timestamp for a key"""
        cdef string ckey = str(key).encode('utf-8')
        if self._map.find(ckey) == self._map.end():
            return None
        return self._map[ckey]
    
    def cleanup_before(self, uint64_t timestamp):
        """Remove tombstones older than timestamp"""
        cdef vector[string] to_remove
        cdef unordered_map[string, uint64_t].iterator it = self._map.begin()
        cdef unordered_map[string, uint64_t].iterator end_it = self._map.end()
        
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
