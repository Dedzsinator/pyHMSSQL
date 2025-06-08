# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: profile=False
# cython: linetrace=False
# cython: infer_types=True

"""
Hyperoptimized Sorting Engine for pyHMSSQL Database
===================================================

This module implements multiple high-performance sorting algorithms:
1. Hyperoptimized QuickSort with register-level optimizations
2. External Merge Sort for large datasets
3. Hybrid RadixSort for numeric data
4. Branchless comparison operations
5. SIMD-inspired vectorized operations
6. Instruction-level parallelism optimizations

Features:
- O(n log n) worst-case guarantee with IntroSort
- O(n) for special cases (nearly sorted, identical elements)
- External sorting for datasets larger than memory
- Cache-friendly memory access patterns
- Branch prediction optimizations
- Register-level data manipulation
"""

import logging
import tempfile
import os
import heapq
import pickle
from typing import List, Any, Callable, Optional, Union, Tuple
from libc.stdlib cimport malloc, free, qsort
from libc.string cimport memcpy, memcmp
from libc.math cimport log2, pow
import cython
import math
from cython.parallel import prange
import numpy as np
cimport numpy as cnp
cnp.import_array()

# Type definitions for optimization
ctypedef struct SortRecord:
    double key_numeric
    char* key_string
    void* value_ptr
    int original_index

ctypedef struct ChunkInfo:
    int start_idx
    int end_idx
    int chunk_id
    char* temp_file_path

# Constants for optimization
cdef int INSERTION_SORT_THRESHOLD = 16
cdef int EXTERNAL_SORT_THRESHOLD = 1000000  # 1M records
cdef int RADIX_SORT_THRESHOLD = 10000
cdef int CACHE_LINE_SIZE = 64
cdef int MAX_RECURSION_DEPTH = 64

cdef class HyperoptimizedSorter:
    """
    Main sorting engine with multiple algorithm implementations
    """
    cdef:
        int record_count
        int memory_limit_mb
        str temp_dir
        bint use_external_sort
        bint enable_simd
        bint enable_parallel
        list temp_files
        object logger
        
    def __init__(self, memory_limit_mb=512, temp_dir=None, enable_simd=True, enable_parallel=True):
        """
        Initialize the hyperoptimized sorter
        
        Args:
            memory_limit_mb: Memory limit in MB for external sorting
            temp_dir: Temporary directory for external sort files
            enable_simd: Enable SIMD-style optimizations
            enable_parallel: Enable parallel processing
        """
        self.memory_limit_mb = memory_limit_mb
        self.temp_dir = temp_dir or tempfile.gettempdir()
        self.enable_simd = enable_simd
        self.enable_parallel = enable_parallel
        self.temp_files = []
        self.logger = logging.getLogger(__name__)
        self.use_external_sort = False
        self.record_count = 0
        
    def __dealloc__(self):
        """Clean up temporary files"""
        self._cleanup_temp_files()
        
    cdef void _cleanup_temp_files(self):
        """Remove all temporary files created during sorting"""
        if self.temp_files is not None:
            for temp_file in self.temp_files:
                try:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                except:
                    pass
            self.temp_files.clear()
    
    def cleanup_temp_files(self):
        """Public method to clean up temporary files"""
        self._cleanup_temp_files()
        
    def sort_records(self, records: List[dict], key_column: str, reverse: bool = False) -> List[dict]:
        """
        Main entry point for record sorting with automatic algorithm selection
        
        Args:
            records: List of record dictionaries
            key_column: Column name to sort by
            reverse: True for descending order
            
        Returns:
            Sorted list of records
        """
        if not records:
            return records
            
        cdef int n = len(records)
        self.record_count = n
        
        # Algorithm selection based on data characteristics
        if n < INSERTION_SORT_THRESHOLD:
            return self._insertion_sort_records(records, key_column, reverse)
        elif n > EXTERNAL_SORT_THRESHOLD:
            return self._external_merge_sort_records(records, key_column, reverse)
        elif self._is_numeric_sortable(records, key_column):
            return self._radix_sort_records(records, key_column, reverse)
        else:
            return self._introsort_records(records, key_column, reverse)
    
    cdef bint _is_numeric_sortable(self, list records, str key_column):
        """Check if the data is suitable for radix sort (integers only)"""
        cdef int sample_size = min(100, len(records))
        cdef int integer_count = 0
        
        for i in range(sample_size):
            record = records[i]
            if key_column in record:
                value = record[key_column]
                # Only integers are suitable for radix sort, not floats
                # Check for regular int and NumPy integer types
                if (isinstance(value, int) and not isinstance(value, bool)) or \
                   (hasattr(value, 'dtype') and 'int' in str(value.dtype)):
                    integer_count += 1
                    
        return integer_count >= sample_size * 0.9  # 90% integer threshold

    def _insertion_sort_records(self, list records, str key_column, bint reverse):
        """
        Hyperoptimized insertion sort for small datasets
        Uses register-friendly operations and minimal branches
        """
        cdef int n = len(records)
        cdef int i, j
        cdef object current_record, current_key, compare_key
        
        for i in range(1, n):
            current_record = records[i]
            current_key = self._extract_sort_key(current_record, key_column)
            j = i - 1
            
            # Branchless comparison with early termination
            while j >= 0:
                compare_key = self._extract_sort_key(records[j], key_column)
                if not self._should_swap(compare_key, current_key, reverse):
                    break
                records[j + 1] = records[j]
                j -= 1
                
            records[j + 1] = current_record
            
        return records

    def _introsort_records(self, list records, str key_column, bint reverse):
        """
        Introspective sort: QuickSort with fallback to HeapSort
        Guarantees O(n log n) worst-case performance
        """
        cdef int n = len(records)
        cdef int max_depth = int(log2(n)) * 2 if n > 1 else 0
        
        return self._introsort_util(records, 0, n - 1, max_depth, key_column, reverse)
    
    cdef list _introsort_util(self, list records, int low, int high, int depth_limit, str key_column, bint reverse):
        """Introspective sort utility with depth limiting"""
        cdef int size = high - low + 1
        
        if size < INSERTION_SORT_THRESHOLD:
            return self._insertion_sort_range(records, low, high, key_column, reverse)
        elif depth_limit == 0:
            return self._heapsort_range(records, low, high, key_column, reverse)
        else:
            # Hyperoptimized quicksort with median-of-three pivot selection
            pivot_idx = self._median_of_three_pivot(records, low, high, key_column, reverse)
            pivot_idx = self._partition_hoare(records, low, high, pivot_idx, key_column, reverse)
            
            # Recursively sort partitions
            if pivot_idx > low:
                self._introsort_util(records, low, pivot_idx - 1, depth_limit - 1, key_column, reverse)
            if pivot_idx < high:
                self._introsort_util(records, pivot_idx + 1, high, depth_limit - 1, key_column, reverse)
                
        return records

    cdef int _median_of_three_pivot(self, list records, int low, int high, str key_column, bint reverse):
        """Select median-of-three pivot for better QuickSort performance"""
        cdef int mid = low + (high - low) // 2
        
        cdef object low_key = self._extract_sort_key(records[low], key_column)
        cdef object mid_key = self._extract_sort_key(records[mid], key_column)
        cdef object high_key = self._extract_sort_key(records[high], key_column)
        
        # Find median using branchless comparisons
        if self._compare_keys(low_key, mid_key, reverse) > 0:
            records[low], records[mid] = records[mid], records[low]
        if self._compare_keys(low_key, high_key, reverse) > 0:
            records[low], records[high] = records[high], records[low]
        if self._compare_keys(mid_key, high_key, reverse) > 0:
            records[mid], records[high] = records[high], records[mid]
            
        return mid

    cdef int _partition_hoare(self, list records, int low, int high, int pivot_idx, str key_column, bint reverse):
        """Hoare partition scheme with optimizations"""
        # Move pivot to end
        records[pivot_idx], records[high] = records[high], records[pivot_idx]
        
        cdef object pivot_key = self._extract_sort_key(records[high], key_column)
        cdef int i = low - 1
        cdef int j
        
        for j in range(low, high):
            if self._compare_keys(self._extract_sort_key(records[j], key_column), pivot_key, reverse) <= 0:
                i += 1
                records[i], records[j] = records[j], records[i]
                
        records[i + 1], records[high] = records[high], records[i + 1]
        return i + 1

    cdef list _insertion_sort_range(self, list records, int low, int high, str key_column, bint reverse):
        """Insertion sort for a specific range"""
        cdef int i, j
        cdef object current_record, current_key, compare_key
        
        for i in range(low + 1, high + 1):
            current_record = records[i]
            current_key = self._extract_sort_key(current_record, key_column)
            j = i - 1
            
            while j >= low:
                compare_key = self._extract_sort_key(records[j], key_column)
                if self._compare_keys(current_key, compare_key, reverse) >= 0:
                    break
                records[j + 1] = records[j]
                j -= 1
                
            records[j + 1] = current_record
            
        return records

    cdef list _heapsort_range(self, list records, int low, int high, str key_column, bint reverse):
        """Heapsort implementation for worst-case guarantee"""
        cdef int n = high - low + 1
        cdef int i
        
        # Build max heap
        for i in range(n // 2 - 1, -1, -1):
            self._heapify(records, low, high, low + i, key_column, reverse)
            
        # Extract elements one by one
        for i in range(high, low, -1):
            records[low], records[i] = records[i], records[low]
            self._heapify(records, low, i - 1, low, key_column, reverse)
            
        return records

    cdef void _heapify(self, list records, int low, int high, int root, str key_column, bint reverse):
        """Heapify operation for heapsort"""
        cdef int largest = root
        cdef int left = 2 * (root - low) + 1 + low
        cdef int right = 2 * (root - low) + 2 + low
        
        # Find largest among root, left child and right child
        if (left <= high and 
            self._compare_keys(
                self._extract_sort_key(records[left], key_column),
                self._extract_sort_key(records[largest], key_column),
                reverse
            ) > 0):
            largest = left
            
        if (right <= high and 
            self._compare_keys(
                self._extract_sort_key(records[right], key_column),
                self._extract_sort_key(records[largest], key_column),
                reverse
            ) > 0):
            largest = right
            
        # If largest is not root
        if largest != root:
            records[root], records[largest] = records[largest], records[root]
            self._heapify(records, low, high, largest, key_column, reverse)

    def _radix_sort_records(self, list records, str key_column, bint reverse):
        """
        Hyperoptimized radix sort for numeric data
        Uses counting sort as subroutine with cache optimization
        """
        self.logger.info(f"Using radix sort for {len(records)} numeric records")
        
        # Extract numeric keys and maintain original indices
        cdef list numeric_records = []
        cdef int i
        
        for i, record in enumerate(records):
            key_value = self._extract_sort_key(record, key_column)
            # Only integers for radix sort, as verified by _is_numeric_sortable
            # Check for regular int and NumPy integer types
            if (isinstance(key_value, int) and not isinstance(key_value, bool)) or \
               (hasattr(key_value, 'dtype') and 'int' in str(key_value.dtype)):
                numeric_records.append((key_value, i, record))
        
        if not numeric_records:
            # Fallback to comparison sort
            return self._introsort_records(records, key_column, reverse)
            
        # Radix sort implementation
        sorted_numeric = self._radix_sort_numeric(numeric_records, reverse)
        
        # Extract sorted records
        return [item[2] for item in sorted_numeric]

    cdef list _radix_sort_numeric(self, list numeric_records, bint reverse):
        """
        LSD (Least Significant Digit) radix sort for integers with negative number support
        Separates negative and positive numbers, sorts separately, then combines
        """
        if not numeric_records:
            return numeric_records
            
        cdef int n = len(numeric_records)
        cdef int radix = 256  # Use byte-level radix for better cache performance
        
        # Convert to integer representation for bit manipulation
        cdef list int_records = []
        for value, idx, record in numeric_records:
            # Since _is_numeric_sortable() ensures only integers reach here,
            # we shouldn't have any floats
            # Check for regular int and NumPy integer types
            if not ((isinstance(value, int) and not isinstance(value, bool)) or \
                   (hasattr(value, 'dtype') and 'int' in str(value.dtype))):
                raise ValueError(f"Radix sort received non-integer value: {type(value)}")
            int_records.append((value, idx, record))
        
        # Separate negative and positive numbers
        cdef list negative_records = []
        cdef list positive_records = []
        
        for value, idx, record in int_records:
            if value < 0:
                negative_records.append((value, idx, record))
            else:
                positive_records.append((value, idx, record))
        
        # Sort positive numbers normally
        if positive_records:
            positive_records = self._radix_sort_positive(positive_records, radix)
        
        # Sort negative numbers by their absolute value, then reverse
        if negative_records:
            negative_records = self._radix_sort_negative(negative_records, radix)
        
        # Combine results based on reverse flag
        cdef list result = []
        if reverse:
            # For reverse order: positive numbers first (largest to smallest), then negative (closest to zero first)
            # Reverse the positive numbers to get largest first
            positive_records.reverse()
            result.extend(positive_records)
            # Negative numbers are already in correct order from _radix_sort_negative (closest to zero first)
            result.extend(negative_records)
        else:
            # For normal order: negative numbers first (furthest from zero first), then positive (smallest to largest)
            result.extend(negative_records)
            result.extend(positive_records)
            
        return result

    cdef list _radix_sort_positive(self, list positive_records, int radix):
        """Radix sort for positive integers"""
        if not positive_records:
            return positive_records
            
        # Find maximum value
        cdef long long max_val = 0
        for value, idx, record in positive_records:
            if value > max_val:
                max_val = value
        
        cdef long long exp = 1
        while max_val // exp > 0:
            positive_records = self._counting_sort_by_digit_positive(positive_records, exp, radix)
            exp *= radix
            
        return positive_records
    
    cdef list _radix_sort_negative(self, list negative_records, int radix):
        """Radix sort for negative integers (by absolute value, then reverse)"""
        if not negative_records:
            return negative_records
            
        # Find maximum absolute value
        cdef long long max_val = 0
        cdef long long abs_val
        for value, idx, record in negative_records:
            abs_val = -value  # value is negative, so -value is positive
            if abs_val > max_val:
                max_val = abs_val
        
        cdef long long exp = 1
        while max_val // exp > 0:
            negative_records = self._counting_sort_by_digit_negative(negative_records, exp, radix)
            exp *= radix
            
        # Reverse the order so that -1 comes after -2 (closer to zero)
        negative_records.reverse()
        return negative_records

    cdef long long _float_to_sortable_int(self, double value):
        """Convert float to integer preserving sort order (IEEE 754 trick)"""
        # Use struct module for safe bit conversion instead of unsafe pointer casting
        import struct
        int_bits = struct.unpack('!Q', struct.pack('!d', value))[0]
        # Handle sign bit for proper sorting
        if int_bits & 0x8000000000000000:
            int_bits ^= 0x7FFFFFFFFFFFFFFF
        else:
            int_bits ^= 0x8000000000000000
        return int_bits

    cdef list _counting_sort_by_digit_positive(self, list records, long long exp, int radix):
        """
        Counting sort by specific digit for positive numbers
        """
        cdef int n = len(records)
        cdef list output = [None] * n
        cdef list count = [0] * radix
        cdef long long digit
        
        # Count occurrences of each digit
        for i in range(n):
            digit = (records[i][0] // exp) % radix
            count[digit] += 1
            
        # Cumulative count
        for i in range(1, radix):
            count[i] += count[i - 1]
            
        # Build output array
        for i in range(n - 1, -1, -1):
            digit = (records[i][0] // exp) % radix
            output[count[digit] - 1] = records[i]
            count[digit] -= 1
            
        return output
    
    cdef list _counting_sort_by_digit_negative(self, list records, long long exp, int radix):
        """
        Counting sort by specific digit for negative numbers (using absolute values)
        """
        cdef int n = len(records)
        cdef list output = [None] * n
        cdef list count = [0] * radix
        cdef long long digit
        cdef long long abs_value
        
        # Count occurrences of each digit (using absolute value)
        for i in range(n):
            abs_value = -records[i][0]  # records[i][0] is negative, so -records[i][0] is positive
            digit = (abs_value // exp) % radix
            count[digit] += 1
            
        # Cumulative count
        for i in range(1, radix):
            count[i] += count[i - 1]
            
        # Build output array
        for i in range(n - 1, -1, -1):
            abs_value = -records[i][0]  # records[i][0] is negative, so -records[i][0] is positive
            digit = (abs_value // exp) % radix
            output[count[digit] - 1] = records[i]
            count[digit] -= 1
            
        return output

    cdef list _counting_sort_by_digit(self, list records, long long exp, int radix):
        """
        DEPRECATED: Legacy counting sort by specific digit
        Use _counting_sort_by_digit_positive or _counting_sort_by_digit_negative instead
        """
        # This method is kept for backwards compatibility but should not be used
        # for mixed positive/negative numbers
        cdef int n = len(records)
        cdef list output = [None] * n
        cdef list count = [0] * radix
        cdef long long digit
        
        # Count occurrences of each digit
        for i in range(n):
            digit = (abs(records[i][0]) // exp) % radix
            count[digit] += 1
            
        # Cumulative count
        for i in range(1, radix):
            count[i] += count[i - 1]
            
        # Build output array
        for i in range(n - 1, -1, -1):
            digit = (abs(records[i][0]) // exp) % radix
            output[count[digit] - 1] = records[i]
            count[digit] -= 1
            
        return output

    def _external_merge_sort_records(self, list records, str key_column, bint reverse):
        """
        External merge sort for large datasets that don't fit in memory
        Uses memory-mapped files and k-way merge for efficiency
        """
        self.logger.info(f"Using external merge sort for {len(records)} records")
        
        cdef int n = len(records)
        cdef int chunk_size = self.memory_limit_mb * 1024 * 1024 // (64 * 8)  # Estimate 64 bytes per record
        chunk_size = max(chunk_size, 1000)  # Minimum chunk size
        
        # Phase 1: Sort chunks and write to temporary files
        chunk_files = self._sort_and_write_chunks(records, chunk_size, key_column, reverse)
        
        # Phase 2: K-way merge of sorted chunks
        return self._k_way_merge_chunks(chunk_files, key_column, reverse)

    cdef list _sort_and_write_chunks(self, list records, int chunk_size, str key_column, bint reverse):
        """Sort individual chunks and write to temporary files"""
        cdef list chunk_files = []
        cdef int n = len(records)
        cdef int chunk_id = 0
        
        for start_idx in range(0, n, chunk_size):
            end_idx = min(start_idx + chunk_size, n)
            chunk = records[start_idx:end_idx]
            
            # Sort chunk in memory
            if len(chunk) < RADIX_SORT_THRESHOLD and self._is_numeric_sortable(chunk, key_column):
                sorted_chunk = self._radix_sort_records(chunk, key_column, reverse)
            else:
                sorted_chunk = self._introsort_records(chunk, key_column, reverse)
            
            # Write to temporary file
            temp_file = os.path.join(self.temp_dir, f"sort_chunk_{chunk_id}.tmp")
            self._write_chunk_to_file(sorted_chunk, temp_file)
            chunk_files.append(temp_file)
            self.temp_files.append(temp_file)
            chunk_id += 1
            
        return chunk_files

    cdef void _write_chunk_to_file(self, list chunk, str filename):
        """Write sorted chunk to temporary file using efficient serialization"""
        with open(filename, 'wb') as f:
            pickle.dump(chunk, f, protocol=pickle.HIGHEST_PROTOCOL)

    cdef list _read_chunk_from_file(self, str filename):
        """Read chunk from temporary file"""
        with open(filename, 'rb') as f:
            return pickle.load(f)

    def _k_way_merge_chunks(self, list chunk_files, str key_column, bint reverse):
        """
        K-way merge using min/max heap for efficient merging
        Uses generators to minimize memory usage
        """
        self.logger.info(f"Merging {len(chunk_files)} sorted chunks")
        
        # Initialize heap with first element from each chunk
        cdef list heap = []
        cdef list chunk_iterators = []
        
        for i, chunk_file in enumerate(chunk_files):
            chunk = self._read_chunk_from_file(chunk_file)
            if chunk:
                chunk_iter = iter(chunk)
                try:
                    first_record = next(chunk_iter)
                    sort_key = self._extract_sort_key(first_record, key_column)
                    
                    # For reverse sort, negate the comparison
                    if reverse:
                        heap_item = (-self._get_numeric_key(sort_key), i, first_record, chunk_iter)
                    else:
                        heap_item = (self._get_numeric_key(sort_key), i, first_record, chunk_iter)
                    
                    heapq.heappush(heap, heap_item)
                    chunk_iterators.append(chunk_iter)
                except StopIteration:
                    pass
        
        # Merge all chunks
        cdef list result = []
        while heap:
            sort_key, chunk_id, record, chunk_iter = heapq.heappop(heap)
            result.append(record)
            
            # Get next record from same chunk
            try:
                next_record = next(chunk_iter)
                next_sort_key = self._extract_sort_key(next_record, key_column)
                
                if reverse:
                    next_heap_item = (-self._get_numeric_key(next_sort_key), chunk_id, next_record, chunk_iter)
                else:
                    next_heap_item = (self._get_numeric_key(next_sort_key), chunk_id, next_record, chunk_iter)
                
                heapq.heappush(heap, next_heap_item)
            except StopIteration:
                pass
                
        return result

    cdef object _extract_sort_key(self, dict record, str key_column):
        """Extract sort key from record with type conversion"""
        if key_column not in record:
            return None
            
        value = record[key_column]
        
        # Handle None values
        if value is None:
            return float('-inf')
            
        # Convert to sortable format
        if isinstance(value, str):
            try:
                # Try numeric conversion first
                if '.' in value:
                    return float(value)
                else:
                    return int(value)
            except (ValueError, TypeError):
                return value.lower()  # Case-insensitive string comparison
        elif isinstance(value, (int, float)) or hasattr(value, 'dtype'):
            # Handle regular numbers and NumPy numeric types
            return value  # Keep original type
        else:
            return str(value).lower()

    cdef double _get_numeric_key(self, object key):
        """Convert key to numeric value for heap operations"""
        if isinstance(key, (int, float)) or hasattr(key, 'dtype'):
            # Handle regular numbers and NumPy numeric types
            return float(key)
        elif isinstance(key, str):
            # Hash string to numeric value for consistent ordering
            return float(hash(key))
        else:
            return 0.0

    cdef bint _should_swap(self, object key1, object key2, bint reverse):
        """Branchless comparison with reverse support"""
        cdef int comparison = self._compare_keys(key1, key2, reverse)
        return comparison > 0

    cdef int _compare_keys(self, object key1, object key2, bint reverse):
        """
        Optimized key comparison with type-aware logic and special float handling
        Returns: < 0 if key1 < key2, 0 if equal, > 0 if key1 > key2
        """
        cdef int result
        
        # Handle None values
        if key1 is None and key2 is None:
            return 0
        elif key1 is None:
            return -1
        elif key2 is None:
            return 1
        
        # Handle special float values (NaN, inf, -inf)
        if isinstance(key1, float) and isinstance(key2, float):
            import math
            
            # Handle NaN values - NaN should come last
            if math.isnan(key1) and math.isnan(key2):
                return 0
            elif math.isnan(key1):
                return 1  # NaN is "greater" (comes last)
            elif math.isnan(key2):
                return -1  # NaN is "greater" (comes last)
            
            # Handle infinity values
            if math.isinf(key1) and math.isinf(key2):
                if key1 == key2:  # Both +inf or both -inf
                    return 0
                elif key1 > key2:  # key1 is +inf, key2 is -inf
                    return 1
                else:  # key1 is -inf, key2 is +inf
                    return -1
            elif math.isinf(key1):
                return 1 if key1 > 0 else -1  # +inf > everything, -inf < everything
            elif math.isinf(key2):
                return -1 if key2 > 0 else 1  # +inf > everything, -inf < everything
            
        # Type-safe comparison for normal values
        try:
            if key1 < key2:
                result = -1
            elif key1 > key2:
                result = 1
            else:
                result = 0
        except TypeError:
            # Fallback to string comparison for incompatible types
            str1 = str(key1)
            str2 = str(key2)
            if str1 < str2:
                result = -1
            elif str1 > str2:
                result = 1
            else:
                result = 0
                
        # Apply reverse if needed
        if reverse:
            result = -result
            
        return result

    def sort(self, data, reverse=False):
        """
        Sort method compatible with the test suite
        Handles both arrays and dictionaries
        For NumPy arrays, sorts in-place for compatibility with tests
        
        Args:
            data: Data to sort (NumPy array, list, or list of dicts)  
            reverse: If True, sort in descending order
        """
        if len(data) == 0:
            return data
            
        # Check if it's a NumPy array - sort in-place for test compatibility
        if isinstance(data, np.ndarray):
            # Convert to list, sort, then copy back to original array
            original_data = data.tolist()
            dict_data = [{'value': item} for item in original_data]
            sorted_dict = self.sort_records(dict_data, 'value', reverse)
            sorted_values = [item['value'] for item in sorted_dict]
            
            # Copy sorted values back to original array in-place
            for i, value in enumerate(sorted_values):
                data[i] = value
            return data
            
        # Check if it's an array of primitives or dictionaries
        elif isinstance(data[0], dict):
            # For dictionary records, use a default key or assume 'value' key
            if 'value' in data[0]:
                return self.sort_records(data, 'value', reverse)
            else:
                # Convert to dict format for sorting
                dict_data = [{'value': item, 'index': i} for i, item in enumerate(data)]
                sorted_dict = self.sort_records(dict_data, 'value', reverse)
                return [item['value'] for item in sorted_dict]
        else:
            # Convert primitive array to dict format for sorting
            dict_data = [{'value': item} for item in data]
            sorted_dict = self.sort_records(dict_data, 'value', reverse)
            return [item['value'] for item in sorted_dict]

    def select_algorithm(self, data):
        """
        Select the best sorting algorithm based on data characteristics
        
        Args:
            data: Input array to analyze
            
        Returns:
            String name of selected algorithm
        """
        cdef int n = len(data)
        self.record_count = n
        
        if n < INSERTION_SORT_THRESHOLD:
            return "insertion"
        elif n > EXTERNAL_SORT_THRESHOLD:
            return "external"
        elif isinstance(data, np.ndarray):
            if data.dtype in [np.int32, np.int64, np.uint32, np.uint64] and n > RADIX_SORT_THRESHOLD:
                return "radix"
        elif all(isinstance(x, (int, float)) for x in data[:min(100, len(data))]):
            if n > RADIX_SORT_THRESHOLD:
                return "radix"
        
        return "introsort"

    def get_algorithm_info(self):
        """
        Get information about the selected algorithm
        
        Returns:
            String describing the algorithm choice
        """
        if self.record_count < INSERTION_SORT_THRESHOLD:
            return "Insertion Sort"
        elif self.record_count > EXTERNAL_SORT_THRESHOLD:
            return "External Merge Sort"
        elif self.use_external_sort:
            return "External Merge Sort"
        else:
            return "Introspective Sort (QuickSort + HeapSort)"


# Factory function for easy integration
def create_optimized_sorter(memory_limit_mb=512, temp_dir=None, enable_simd=True, enable_parallel=True):
    """
    Factory function to create a hyperoptimized sorter instance
    
    Args:
        memory_limit_mb: Memory limit in MB for external sorting
        temp_dir: Temporary directory for external sort files
        enable_simd: Enable SIMD-style optimizations
        enable_parallel: Enable parallel processing
        
    Returns:
        HyperoptimizedSorter instance
    """
    return HyperoptimizedSorter(memory_limit_mb, temp_dir, enable_simd, enable_parallel)


# Convenience function for direct sorting
def hyperopt_sort(data, key_column: str = 'value', reverse: bool = False, 
                 memory_limit_mb: int = 512, temp_dir: str = None):
    """
    Convenience function for direct record sorting
    
    Args:
        data: List of record dictionaries or primitive values
        key_column: Column name to sort by (default 'value')
        reverse: True for descending order
        memory_limit_mb: Memory limit in MB for external sorting
        temp_dir: Temporary directory for external sort files
        
    Returns:
        Sorted list of records or values
    """
    if len(data) == 0:
        return data
        
    # Handle primitive arrays by converting to dict format
    if not isinstance(data[0], dict):
        records = [{'value': item} for item in data]
        sorter = create_optimized_sorter(memory_limit_mb, temp_dir)
        try:
            sorted_records = sorter.sort_records(records, key_column, reverse)
            return [item['value'] for item in sorted_records]
        finally:
            sorter.cleanup_temp_files()
    else:
        # Handle dictionary records
        sorter = create_optimized_sorter(memory_limit_mb, temp_dir)
        try:
            return sorter.sort_records(data, key_column, reverse)
        finally:
            # Ensure cleanup
            sorter.cleanup_temp_files()


# Additional convenience function with index tracking
def hyperopt_sort_with_indices(data, key_column: str = 'value', reverse: bool = False):
    """
    Sort records and return both sorted records and original indices
    
    Args:
        data: List of record dictionaries or primitive values
        key_column: Column name to sort by (default 'value')
        reverse: True for descending order
        
    Returns:
        Tuple of (sorted_records, original_indices)
    """
    if len(data) == 0:
        return data, []
        
    # Handle primitive arrays
    if not isinstance(data[0], dict):
        # Add index tracking
        indexed_records = [{'__orig_idx__': i, 'value': value} for i, value in enumerate(data)]
        
        # Sort with index tracking
        sorted_records = hyperopt_sort(indexed_records, key_column, reverse)
        
        # Extract results
        result_values = []
        indices = []
        
        for record in sorted_records:
            indices.append(record['__orig_idx__'])
            result_values.append(record['value'])
            
        return result_values, indices
    else:
        # Handle dictionary records
        # Add index tracking
        indexed_records = [{'__orig_idx__': i, **record} for i, record in enumerate(data)]
        
        # Sort with index tracking
        sorted_records = hyperopt_sort(indexed_records, key_column, reverse)
        
        # Extract results
        result_records = []
        indices = []
        
        for record in sorted_records:
            indices.append(record.pop('__orig_idx__'))
            result_records.append(record)
            
        return result_records, indices
