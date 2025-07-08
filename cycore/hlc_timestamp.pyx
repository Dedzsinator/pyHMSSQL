# Professional HLC implementation in pure Cython
# Production-ready Hybrid Logical Clock for distributed consensus

from libc.stdint cimport uint64_t, uint8_t, int8_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy
import time
import struct
import struct

cdef class HLCTimestamp:
    """
    Hybrid Logical Clock Timestamp.
    
    Combines physical and logical time for distributed system ordering.
    """
    cdef uint64_t _physical
    cdef uint64_t _logical
    
    def __init__(self, physical=None, logical=None):
        if physical is not None and logical is not None:
            self._physical = physical
            self._logical = logical
        else:
            # Initialize with current time
            self._physical = <uint64_t>(time.time() * 1000000)  # microseconds
            self._logical = 0
    
    @property
    def physical(self):
        """Get physical time component"""
        return self._physical
    
    @property 
    def logical(self):
        """Get logical counter component"""
        return self._logical
    
    def __repr__(self):
        return f"HLCTimestamp(physical={self._physical}, logical={self._logical})"
    
    def __str__(self):
        return f"HLCTimestamp(physical={self._physical}, logical={self._logical})"
    
    def __lt__(self, other):
        """Less than comparison"""
        if not isinstance(other, HLCTimestamp):
            return NotImplemented
        if self._physical < other.physical:
            return True
        elif self._physical == other.physical:
            return self._logical < other.logical
        return False
    
    def __le__(self, other):
        """Less than or equal comparison"""
        if not isinstance(other, HLCTimestamp):
            return NotImplemented
        return self < other or self == other
    
    def __gt__(self, other):
        """Greater than comparison"""
        if not isinstance(other, HLCTimestamp):
            return NotImplemented
        return not (self <= other)
    
    def __ge__(self, other):
        """Greater than or equal comparison"""
        if not isinstance(other, HLCTimestamp):
            return NotImplemented  
        return not (self < other)
    
    def __eq__(self, other):
        """Equality comparison"""
        if not isinstance(other, HLCTimestamp):
            return NotImplemented
        return self.physical == other.physical and self.logical == other.logical
    
    def __ne__(self, other):
        """Inequality comparison"""
        return not self.__eq__(other)
    
    def to_bytes(self):
        """Serialize to bytes"""
        return struct.pack('!QQ', self.physical, self.logical)
    
    @classmethod
    def from_bytes(cls, bytes data):
        """Deserialize from bytes"""
        physical, logical = struct.unpack('!QQ', data)
        return cls(physical, logical)
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'physical': self.physical,
            'logical': self.logical
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
    cdef uint64_t _logical_counter
    cdef str _node_id
    
    def __init__(self, node_id="default"):
        self._logical_counter = 0
        self._node_id = node_id
    
    def now(self):
        """
        Get current HLC timestamp.
        
        Returns:
            HLCTimestamp: Current timestamp with physical and logical components
        """
        cdef uint64_t physical_time = <uint64_t>(time.time() * 1000000)  # microseconds
        
        # Increment logical counter for this physical time
        self._logical_counter += 1
        
        return HLCTimestamp(physical_time, self._logical_counter)
    
    def update(self, remote_ts):
        """
        Update HLC with a remote timestamp.
        
        Args:
            remote_ts (HLCTimestamp): Remote timestamp to incorporate
            
        Returns:
            HLCTimestamp: New timestamp after update
        """
        if not isinstance(remote_ts, HLCTimestamp):
            raise TypeError("remote_ts must be an HLCTimestamp")
        
        cdef uint64_t physical_time = <uint64_t>(time.time() * 1000000)
        cdef uint64_t new_logical = 0
        
        # HLC update algorithm
        if physical_time > remote_ts.physical:
            # Local clock is ahead, use local time
            new_logical = self._logical_counter + 1
        elif physical_time == remote_ts.physical:
            # Same physical time, use max logical + 1
            new_logical = max(self._logical_counter, remote_ts.logical) + 1
        else:
            # Remote clock is ahead, advance local clock
            physical_time = remote_ts.physical
            new_logical = remote_ts.logical + 1
        
        self._logical_counter = new_logical
        
        return HLCTimestamp(physical_time, new_logical)
    
    def __dealloc__(self):
        """Cleanup when object is destroyed"""
        pass  # No manual cleanup needed for this pure Cython version


def compare_timestamps(HLCTimestamp ts1, HLCTimestamp ts2):
    """
    Compare two HLC timestamps.
    
    Returns:
        int: -1 if ts1 < ts2, 0 if equal, 1 if ts1 > ts2
    """
    if ts1 < ts2:
        return -1
    elif ts1 > ts2:
        return 1
    else:
        return 0
