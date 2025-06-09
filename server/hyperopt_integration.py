"""
Hyperoptimized Sort Integration Module for pyHMSSQL
==================================================

This module provides drop-in replacements for Python's built-in sorting
functions using the hyperoptimized sorting engine.
"""

import logging
from typing import List, Dict, Any, Callable, Optional

try:
    from hyperoptimized_sort import HyperoptimizedSorter, hyperopt_sort

    HYPEROPT_AVAILABLE = True
    logger = logging.getLogger(__name__)
    logger.info("Hyperoptimized sorting engine loaded successfully")
except ImportError as e:
    logger = logging.getLogger(__name__)
    logger.warning(f"Failed to load hyperoptimized sorting engine: {e}")
    logger.warning("Falling back to Python's built-in sorting")
    HYPEROPT_AVAILABLE = False


def hyperopt_list_sort(
    data: List[Dict[str, Any]],
    key_func: Optional[Callable] = None,
    reverse: bool = False,
    threshold: int = 50000,
) -> None:
    """
    Drop-in replacement for list.sort() that uses hyperoptimized sorting.

    Args:
        data: List of dictionaries to sort (modified in-place)
        key_func: Function to extract sort key from each record
        reverse: True for descending order
        threshold: Minimum size to use hyperoptimized sort (default: 50000)
    """
    if not HYPEROPT_AVAILABLE or not data:
        # Fallback to built-in sort
        data.sort(key=key_func, reverse=reverse)
        return

    # For smaller datasets, use built-in sort to avoid Python-Cython overhead
    if len(data) < threshold:
        logger.debug(
            f"Using built-in sort for small dataset ({len(data)} < {threshold})"
        )
        data.sort(key=key_func, reverse=reverse)
        return

    # Try hyperoptimized sorting
    try:
        # Use hyperoptimized sorting for better performance
        if key_func is None:
            # If no key function, assume we're sorting by the dictionary values directly
            sorter = HyperoptimizedSorter()
            sorted_data = sorter.sort_records(data, "__default__", reverse)
            data[:] = sorted_data
        else:
            # Check if the key function returns complex keys (tuples, etc.)
            # that the hyperoptimized sort engine can't handle efficiently
            if data:
                sample_key = key_func(data[0])
                if isinstance(sample_key, (tuple, list)):
                    # Complex keys - fall back to built-in sort for correctness
                    logger.debug("Using built-in sort for complex tuple/list sort keys")
                    data.sort(key=key_func, reverse=reverse)
                    return

            # Apply key function to create sortable records
            keyed_records = []
            for i, record in enumerate(data):
                sort_key = key_func(record)
                keyed_records.append(
                    {
                        "__sort_key__": sort_key,
                        "__original_record__": record,
                        "__index__": i,
                    }
                )

            # Sort by the extracted key
            sorter = HyperoptimizedSorter()
            sorted_records = sorter.sort_records(keyed_records, "__sort_key__", reverse)

            # Extract the original records in sorted order
            data[:] = [r["__original_record__"] for r in sorted_records]

        logger.debug(f"Hyperoptimized sorting completed for {len(data)} records")

    except Exception as e:
        logger.warning(
            f"Hyperoptimized sorting failed: {e}, falling back to built-in sort"
        )
        data.sort(key=key_func, reverse=reverse)


def hyperopt_sorted(
    data: List[Dict[str, Any]],
    key_func: Optional[Callable] = None,
    reverse: bool = False,
) -> List[Dict[str, Any]]:
    """
    Drop-in replacement for sorted() that uses hyperoptimized sorting.

    Args:
        data: List of dictionaries to sort
        key_func: Function to extract sort key from each record
        reverse: True for descending order

    Returns:
        New sorted list
    """
    if not HYPEROPT_AVAILABLE or not data:
        # Fallback to built-in sorted
        return sorted(data, key=key_func, reverse=reverse)

    try:
        # Make a copy and sort it
        data_copy = list(data)
        hyperopt_list_sort(data_copy, key_func, reverse, threshold=50000)
        return data_copy

    except Exception as e:
        logger.warning(
            f"Hyperoptimized sorting failed: {e}, falling back to built-in sorted"
        )
        return sorted(data, key=key_func, reverse=reverse)


def get_performance_stats() -> Dict[str, Any]:
    """
    Get performance statistics from the hyperoptimized sorting engine.

    Returns:
        Dictionary containing performance metrics
    """
    return {
        "hyperopt_available": HYPEROPT_AVAILABLE,
        "engine_version": "1.0.0",
        "algorithms_available": (
            ["radix_sort", "introsort", "external_merge_sort", "insertion_sort"]
            if HYPEROPT_AVAILABLE
            else []
        ),
    }
