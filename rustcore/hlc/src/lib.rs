//! Hybrid Logical Clock (HLC) Implementation for pyHMSSQL
//!
//! This module provides a high-performance HLC implementation compatible with CockroachDB's approach.
//! HLC combines physical and logical time to provide a globally consistent ordering of events
//! in distributed systems.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Hybrid Logical Clock structure
#[repr(C)]
pub struct HybridLogicalClock {
    logical_counter: AtomicU64,
    last_physical: AtomicU64,
}

/// HLC Timestamp structure - compatible with Cython
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct HLCTimestamp {
    pub physical: u64, // Physical time in nanoseconds since epoch
    pub logical: u64,  // Logical counter
}

impl HybridLogicalClock {
    /// Create a new HLC instance
    pub fn new() -> Self {
        Self {
            logical_counter: AtomicU64::new(0),
            last_physical: AtomicU64::new(0),
        }
    }

    /// Get current timestamp - thread-safe
    pub fn now(&self) -> HLCTimestamp {
        let physical_now = Self::get_physical_time();
        let last_physical = self.last_physical.load(Ordering::SeqCst);

        if physical_now > last_physical {
            // Physical time advanced, reset logical counter
            self.last_physical.store(physical_now, Ordering::SeqCst);
            self.logical_counter.store(0, Ordering::SeqCst);
            HLCTimestamp {
                physical: physical_now,
                logical: 0,
            }
        } else {
            // Same or earlier physical time, increment logical counter
            let logical = self.logical_counter.fetch_add(1, Ordering::SeqCst) + 1;
            HLCTimestamp {
                physical: last_physical,
                logical,
            }
        }
    }

    /// Update HLC with remote timestamp
    pub fn update(&self, remote_ts: HLCTimestamp) -> HLCTimestamp {
        let physical_now = Self::get_physical_time();
        let max_physical = physical_now.max(remote_ts.physical);

        let last_physical = self.last_physical.load(Ordering::SeqCst);

        if max_physical > last_physical {
            // Physical time advanced
            self.last_physical.store(max_physical, Ordering::SeqCst);
            let logical = if max_physical == remote_ts.physical {
                remote_ts.logical + 1
            } else {
                0
            };
            self.logical_counter.store(logical, Ordering::SeqCst);
            HLCTimestamp {
                physical: max_physical,
                logical,
            }
        } else {
            // Same physical time, advance logical
            let logical = if last_physical == remote_ts.physical {
                let current_logical = self.logical_counter.load(Ordering::SeqCst);
                let new_logical = current_logical.max(remote_ts.logical) + 1;
                self.logical_counter.store(new_logical, Ordering::SeqCst);
                new_logical
            } else {
                self.logical_counter.fetch_add(1, Ordering::SeqCst) + 1
            };

            HLCTimestamp {
                physical: last_physical,
                logical,
            }
        }
    }

    /// Get physical time in nanoseconds
    fn get_physical_time() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }
}

impl HLCTimestamp {
    /// Compare timestamps for ordering
    pub fn compare(&self, other: &HLCTimestamp) -> std::cmp::Ordering {
        match self.physical.cmp(&other.physical) {
            std::cmp::Ordering::Equal => self.logical.cmp(&other.logical),
            other => other,
        }
    }

    /// Check if this timestamp is less than another
    pub fn is_less_than(&self, other: &HLCTimestamp) -> bool {
        self.compare(other) == std::cmp::Ordering::Less
    }

    /// Check if this timestamp is greater than another  
    pub fn is_greater_than(&self, other: &HLCTimestamp) -> bool {
        self.compare(other) == std::cmp::Ordering::Greater
    }

    /// Convert to bytes for serialization
    pub fn to_bytes(&self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&self.physical.to_le_bytes());
        bytes[8..].copy_from_slice(&self.logical.to_le_bytes());
        bytes
    }

    /// Create from bytes
    pub fn from_bytes(bytes: &[u8; 16]) -> Self {
        let physical = u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        let logical = u64::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        Self { physical, logical }
    }
}

// C-compatible API for Cython binding
#[no_mangle]
pub extern "C" fn hlc_new() -> *mut HybridLogicalClock {
    Box::into_raw(Box::new(HybridLogicalClock::new()))
}

#[no_mangle]
pub extern "C" fn hlc_free(hlc: *mut HybridLogicalClock) {
    if !hlc.is_null() {
        unsafe { drop(Box::from_raw(hlc)) };
    }
}

#[no_mangle]
pub extern "C" fn hlc_now(hlc: *const HybridLogicalClock) -> HLCTimestamp {
    unsafe { (*hlc).now() }
}

#[no_mangle]
pub extern "C" fn hlc_update(
    hlc: *const HybridLogicalClock,
    remote_ts: HLCTimestamp,
) -> HLCTimestamp {
    unsafe { (*hlc).update(remote_ts) }
}

#[no_mangle]
pub extern "C" fn hlc_timestamp_compare(ts1: *const HLCTimestamp, ts2: *const HLCTimestamp) -> i8 {
    unsafe {
        match (*ts1).compare(&*ts2) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }
    }
}

#[no_mangle]
pub extern "C" fn hlc_timestamp_to_bytes(ts: *const HLCTimestamp, output: *mut u8) {
    unsafe {
        let bytes = (*ts).to_bytes();
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), output, 16);
    }
}

#[no_mangle]
pub extern "C" fn hlc_timestamp_from_bytes(bytes: *const u8) -> HLCTimestamp {
    unsafe {
        let mut array = [0u8; 16];
        std::ptr::copy_nonoverlapping(bytes, array.as_mut_ptr(), 16);
        HLCTimestamp::from_bytes(&array)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_hlc_now() {
        let hlc = HybridLogicalClock::new();
        let ts1 = hlc.now();
        let ts2 = hlc.now();

        // Second timestamp should be greater
        assert!(ts2.is_greater_than(&ts1));
    }

    #[test]
    fn test_hlc_update() {
        let hlc = HybridLogicalClock::new();
        let ts1 = hlc.now();

        // Simulate remote timestamp from future
        let remote_ts = HLCTimestamp {
            physical: ts1.physical + 1_000_000, // 1ms in future
            logical: 5,
        };

        let ts2 = hlc.update(remote_ts);
        assert!(ts2.is_greater_than(&ts1));
        assert!(ts2.is_greater_than(&remote_ts));
    }

    #[test]
    fn test_timestamp_serialization() {
        let ts = HLCTimestamp {
            physical: 1234567890,
            logical: 42,
        };

        let bytes = ts.to_bytes();
        let restored = HLCTimestamp::from_bytes(&bytes);

        assert_eq!(ts.physical, restored.physical);
        assert_eq!(ts.logical, restored.logical);
    }
}
