"""
CyCore - High-performance Cython modules for pyHMSSQL
=====================================================

This package contains Cython-optimized modules for critical performance paths:
- HLC (Hybrid Logical Clock) implementation
- High-performance hash map implementations
- Range management and routing
- Hot-path optimizations for consensus and replication
"""

__version__ = "1.0.0"

# Try to import HLC implementations with professional naming
try:
    from .hlc_timestamp import HLCTimestamp, HybridLogicalClock
    HLC_IMPLEMENTATION = "standard"
except ImportError:
    try:
        # Try advanced HLC implementation
        from .hlc_advanced import HLCTimestamp, HybridLogicalClock
        HLC_IMPLEMENTATION = "advanced"
    except ImportError:
        HLCTimestamp = HybridLogicalClock = None
        HLC_IMPLEMENTATION = "none"

# Import hash map implementations - professional naming with backward compatibility
try:
    # Try new professional hashmap implementation first
    from .hashmap import SwissMap, RangeMap, TombstoneMap
    HASHMAP_IMPLEMENTATION = "standard"
except ImportError:
    try:
        # Fallback to existing built versions for backward compatibility
        from .swiss_map_simple import SwissMap, RangeMap, TombstoneMap
        HASHMAP_IMPLEMENTATION = "standard"
    except ImportError:
        try:
            # Try advanced hashmap implementation
            from .hashmap_advanced import SwissMap, RangeMap, TombstoneMap
            HASHMAP_IMPLEMENTATION = "advanced"
        except ImportError:
            try:
                # Try fallback implementation
                from .hashmap_fallback import SwissMap, RangeMap, TombstoneMap
                HASHMAP_IMPLEMENTATION = "fallback"
            except ImportError:
                SwissMap = RangeMap = TombstoneMap = None
                HASHMAP_IMPLEMENTATION = "none"

# Backward compatibility for legacy swiss_map names
SWISS_IMPLEMENTATION = HASHMAP_IMPLEMENTATION

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
        "hashmap_implementation": HASHMAP_IMPLEMENTATION,
        "version": __version__
    }

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
        "hashmap_implementation": HASHMAP_IMPLEMENTATION,
        "swiss_implementation": SWISS_IMPLEMENTATION,  # backward compatibility
        "version": __version__
    }
