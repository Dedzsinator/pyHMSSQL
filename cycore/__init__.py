"""
CyCore - High-performance Cython modules for pyHMSSQL
=====================================================

This package contains Cython-optimized modules for critical performance paths:
- HLC (Hybrid Logical Clock) implementation
- Swiss Table hash map bindings  
- Range management and routing
- Hot-path optimizations for consensus and replication
"""

__version__ = "1.0.0"

"""
CyCore - High-performance Cython modules for pyHMSSQL
=====================================================

This package contains Cython-optimized modules for critical performance paths:
- HLC (Hybrid Logical Clock) implementation
- Swiss Table hash map bindings  
- Range management and routing
- Hot-path optimizations for consensus and replication
"""

__version__ = "1.0.0"

# Try to import the Rust-based HLC first, fall back to simple version
try:
    from .hlc_ts import HLCTimestamp
    HLC_IMPLEMENTATION = "rust"
except ImportError:
    try:
        from .hlc_ts_simple import HLCTimestamp, HybridLogicalClock
        HLC_IMPLEMENTATION = "simple"
    except ImportError:
        HLCTimestamp = HybridLogicalClock = None
        HLC_IMPLEMENTATION = "none"

# Import Swiss Table implementations
try:
    from .swiss_map_simple import SwissMap, RangeMap, TombstoneMap
    SWISS_IMPLEMENTATION = "std"
except ImportError:
    try:
        from .swiss_map import SwissMap, RangeMap, TombstoneMap
        SWISS_IMPLEMENTATION = "abseil"
    except ImportError:
        SwissMap = RangeMap = TombstoneMap = None
        SWISS_IMPLEMENTATION = "none"

# Build __all__ list based on what's available
__all__ = []
if HLCTimestamp:
    __all__.extend(["HLCTimestamp", "HybridLogicalClock"])
if SwissMap:
    __all__.extend(["SwissMap", "RangeMap", "TombstoneMap"])

def get_info():
    """Get information about available implementations"""
    return {
        "hlc_implementation": HLC_IMPLEMENTATION,
        "swiss_implementation": SWISS_IMPLEMENTATION,
        "version": __version__
    }
