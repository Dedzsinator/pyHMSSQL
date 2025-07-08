# distutils: language = c++
# cython: language_level=3

"""
Fallback Swiss Table implementation using std::unordered_map.
Used when Google Abseil is not available.
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
    High-performance hash map fallback using std::unordered_map.
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

# Simplified range map
cdef class RangeMap:
    """Range-optimized map for uint64 -> string mappings"""
    cdef unordered_map[uint64_t, string] _map
    
    def __setitem__(self, uint64_t key, value):
        cdef string cvalue = str(value).encode('utf-8')
        self._map[key] = cvalue
    
    def __getitem__(self, uint64_t key):
        if self._map.find(key) == self._map.end():
            raise KeyError(key)
        return self._map[key].decode('utf-8')
    
    def __contains__(self, uint64_t key):
        return self._map.find(key) != self._map.end()
    
    def __len__(self):
        return self._map.size()
    
    def clear(self):
        self._map.clear()

# Simplified tombstone map
cdef class TombstoneMap:
    """Tombstone tracking map"""
    cdef unordered_map[string, uint64_t] _map
    
    def mark_deleted(self, key, uint64_t timestamp):
        cdef string ckey = str(key).encode('utf-8')
        self._map[ckey] = timestamp
    
    def is_deleted(self, key, uint64_t after_timestamp):
        cdef string ckey = str(key).encode('utf-8')
        if self._map.find(ckey) == self._map.end():
            return False
        return self._map[ckey] > after_timestamp
    
    def __len__(self):
        return self._map.size()
    
    def clear(self):
        self._map.clear()
