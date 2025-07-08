#!/usr/bin/env python3
"""
Check CyCore component availability
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

def check_availability():
    """Check if CyCore components are available and working"""
    
    try:
        import cycore
        info = cycore.get_info()
        print(f"CyCore version: {info['version']}")
        print(f"Swiss maps available: {info['swiss_implementation']}")
        print(f"HLC available: {info['hlc_implementation']}")
        
        # Test Swiss map functionality if available
        if cycore.SwissMap is not None:
            smap = cycore.SwissMap()
            smap["test"] = "value"  # Use dict-like interface
            assert smap["test"] == "value"
            assert smap.get("test") == "value"
            print("Swiss map basic test: PASS")
        else:
            print("Swiss map basic test: SKIP (not available)")
        
        # Test HLC functionality if available  
        if cycore.HLCTimestamp is not None:
            try:
                # Test HLCTimestamp creation
                ts = cycore.HLCTimestamp()
                print(f"HLC timestamp created: {ts}")
                
                if cycore.HybridLogicalClock is not None:
                    clock = cycore.HybridLogicalClock()
                    if hasattr(clock, 'now'):
                        ts1 = clock.now()
                        ts2 = clock.now()
                        assert ts2 > ts1
                        print("HLC clock test: PASS")
                    else:
                        print("HLC clock test: SKIP (now method not available)")
                else:
                    print("HLC clock test: SKIP (HybridLogicalClock not available)")
            except Exception as e:
                print(f"HLC test: FAIL ({e})")
        else:
            print("HLC test: SKIP (not available)")
        
        print("Availability check complete")
        return True
        
    except ImportError as e:
        print(f"CyCore not available: {e}")
        return False
    except Exception as e:
        print(f"CyCore test failed: {e}")
        return False

if __name__ == "__main__":
    success = check_availability()
    sys.exit(0 if success else 1)
