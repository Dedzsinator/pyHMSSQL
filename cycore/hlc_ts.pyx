# distutils: language = c++
# cython: language_level=3

"""
High-performance Hybrid Logical Clock (HLC) implementation for pyHMSSQL.

This module provides Cython bindings to a Rust-based HLC implementation,
offering thread-safe, high-performance timestamps for distributed systems.
"""

from libc.stdint cimport uint64_t, uint8_t, int8_t
from libc.stdlib cimport malloc, free
import time

# External C API from Rust
cdef extern from "hlc.h":
    ctypedef struct CTimestamp:  # Renamed to avoid conflict
        uint64_t physical
        uint64_t logical
    
    ctypedef struct CHybridLogicalClock:  # Renamed to avoid conflict
        pass
    
    CHybridLogicalClock* hlc_new()
    void hlc_free(CHybridLogicalClock* hlc)
    CTimestamp hlc_now(const CHybridLogicalClock* hlc)
    CTimestamp hlc_update(const CHybridLogicalClock* hlc, CTimestamp remote_ts)
    int8_t hlc_timestamp_compare(const CTimestamp* ts1, const CTimestamp* ts2)
    void hlc_timestamp_to_bytes(const CTimestamp* ts, uint8_t* output)
    CTimestamp hlc_timestamp_from_bytes(const uint8_t* bytes)

cdef class HLCTimestamp:
    """
    Hybrid Logical Clock Timestamp.
    
    Combines physical and logical time for distributed system ordering.
    """
    cdef CTimestamp _ts
    
    def __init__(self, physical=None, logical=None):
        if physical is not None and logical is not None:
            self._ts.physical = physical
            self._ts.logical = logical
        else:
            # Initialize with current time
            self._ts.physical = <uint64_t>(time.time_ns())
            self._ts.logical = 0
    
    @property
    def physical(self):
        """Physical timestamp in nanoseconds since epoch"""
        return self._ts.physical
    
    @property 
    def logical(self):
        """Logical counter"""
        return self._ts.logical
    
    def __lt__(self, other):
        """Less than comparison"""
        if not isinstance(other, HLCTimestamp):
            return NotImplemented
        cdef HLCTimestamp other_ts = other
        return hlc_timestamp_compare(&self._ts, &other_ts._ts) < 0
    
    def __le__(self, other):
        """Less than or equal comparison"""
        if not isinstance(other, HLCTimestamp):
            return NotImplemented
        cdef HLCTimestamp other_ts = other
        return hlc_timestamp_compare(&self._ts, &other_ts._ts) <= 0
    
    def __gt__(self, other):
        """Greater than comparison"""
        if not isinstance(other, HLCTimestamp):
            return NotImplemented
        cdef HLCTimestamp other_ts = other
        return hlc_timestamp_compare(&self._ts, &other_ts._ts) > 0
    
    def __ge__(self, other):
        """Greater than or equal comparison"""
        if not isinstance(other, HLCTimestamp):
            return NotImplemented  
        cdef HLCTimestamp other_ts = other
        return hlc_timestamp_compare(&self._ts, &other_ts._ts) >= 0
    
    def __eq__(self, other):
        """Equality comparison"""
        if not isinstance(other, HLCTimestamp):
            return NotImplemented
        cdef HLCTimestamp other_ts = other
        return hlc_timestamp_compare(&self._ts, &other_ts._ts) == 0
    
    def __ne__(self, other):
        """Inequality comparison"""
        return not self.__eq__(other)
    
    def __hash__(self):
        """Hash for use in sets/dicts"""
        return hash((self._ts.physical, self._ts.logical))
    
    def __repr__(self):
        """String representation"""
        return f"HLCTimestamp(physical={self._ts.physical}, logical={self._ts.logical})"
    
    def to_bytes(self):
        """Serialize to bytes"""
        cdef uint8_t[16] output
        hlc_timestamp_to_bytes(&self._ts, output)
        return bytes(output)
    
    @classmethod
    def from_bytes(cls, bytes data):
        """Deserialize from bytes"""
        if len(data) != 16:
            raise ValueError("HLCTimestamp bytes must be exactly 16 bytes")
        
        cdef const uint8_t* bytes_ptr = data
        cdef HLCTimestamp result = hlc_timestamp_from_bytes(bytes_ptr)
        
        new_ts = cls.__new__(cls)
        new_ts._ts = result
        return new_ts
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return {
            'physical': self._ts.physical,
            'logical': self._ts.logical
        }
    
    @classmethod
    def from_dict(cls, dict data):
        """Create from dictionary"""
        return cls(data['physical'], data['logical'])

cdef class HybridLogicalClock:
    """
    High-performance Hybrid Logical Clock implementation.
    
    Thread-safe HLC that provides globally ordered timestamps for distributed systems.
    Based on CockroachDB's HLC design.
    """
    cdef CHybridLogicalClock* _hlc
    
    def __cinit__(self):
        self._hlc = hlc_new()
        if self._hlc is NULL:
            raise MemoryError("Failed to allocate HLC")
    
    def __dealloc__(self):
        if self._hlc is not NULL:
            hlc_free(self._hlc)
            self._hlc = NULL
    
    def now(self):
        """
        Get current HLC timestamp.
        
        Returns:
            HLCTimestamp: Current timestamp with physical and logical components
        """
        cdef CTimestamp raw_ts = hlc_now(self._hlc)
        
        result = HLCTimestamp.__new__(HLCTimestamp)
        result._ts = raw_ts
        return result
    
    def update(self, HLCTimestamp remote_ts):
        """
        Update HLC with remote timestamp and get new timestamp.
        
        Args:
            remote_ts (HLCTimestamp): Remote timestamp to incorporate
            
        Returns:
            HLCTimestamp: New timestamp after update
        """
        cdef CTimestamp raw_ts = hlc_update(self._hlc, remote_ts._ts)
        
        result = HLCTimestamp.__new__(HLCTimestamp)
        result._ts = raw_ts
        return result
    
    def tick(self):
        """
        Convenience method to get a new timestamp (alias for now()).
        
        Returns:
            HLCTimestamp: Current timestamp
        """
        return self.now()

# Module-level convenience functions
cdef HybridLogicalClock _global_hlc = HybridLogicalClock()

def now():
    """Get current HLC timestamp from global clock"""
    return _global_hlc.now()

def update(HLCTimestamp remote_ts):
    """Update global HLC with remote timestamp"""
    return _global_hlc.update(remote_ts)

def compare(HLCTimestamp ts1, HLCTimestamp ts2):
    """
    Compare two HLC timestamps.
    
    Returns:
        int: -1 if ts1 < ts2, 0 if equal, 1 if ts1 > ts2
    """
    return hlc_timestamp_compare(&ts1._ts, &ts2._ts)
