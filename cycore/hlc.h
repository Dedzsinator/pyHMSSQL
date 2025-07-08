// C header for HLC Rust library - used by Cython

#ifndef HLC_H
#define HLC_H

#include <stdint.h>

typedef struct
{
    uint64_t physical;
    uint64_t logical;
} CTimestamp;

typedef struct CHybridLogicalClock CHybridLogicalClock;

// Function declarations
CHybridLogicalClock *hlc_new(void);
void hlc_free(CHybridLogicalClock *hlc);
CTimestamp hlc_now(const CHybridLogicalClock *hlc);
CTimestamp hlc_update(const CHybridLogicalClock *hlc, CTimestamp remote_ts);
int8_t hlc_timestamp_compare(const CTimestamp *ts1, const CTimestamp *ts2);
void hlc_timestamp_to_bytes(const CTimestamp *ts, uint8_t *output);
CTimestamp hlc_timestamp_from_bytes(const uint8_t *bytes);

#endif // HLC_H
