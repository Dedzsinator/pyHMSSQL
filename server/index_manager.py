"""
Index manager to handle B+ tree indexing with proper caching support.

This module provides enhanced caching for optimized B+ tree indexes to improve performance.
"""

import logging
from collections import OrderedDict
import time
import threading

try:
    from bptree import BPlusTreeOptimized as BPlusTree
except ImportError:
    # Fallback to adapter if optimized is not available
    from bptree_adapter import BPlusTree


class IndexManager:
    """Manage indexes with caching support"""

    def __init__(self, buffer_pool=None):
        self.buffer_pool = buffer_pool
        self.indexes = {}  # Fallback local cache if buffer_pool not available
        self.last_access = OrderedDict()
        self.lock = threading.RLock()
        self.logger = logging.getLogger("index_manager")

    def get_index(self, index_id):
        """Get a cached index by ID"""
        # Try the buffer pool first if available
        if self.buffer_pool:
            index = self.buffer_pool.get_index(index_id)
            if index:
                return index

        # Fall back to local cache
        with self.lock:
            if index_id in self.indexes:
                self.last_access[index_id] = time.time()
                self.last_access.move_to_end(index_id)
                self.logger.info(f"Local index cache hit: {index_id}")
                return self.indexes[index_id]

        self.logger.info(f"Index cache miss for: {index_id}")
        return None

    def cache_index(self, index_id, index_obj):
        """Cache an index object"""
        # Try to use buffer pool first
        if self.buffer_pool:
            self.buffer_pool.cache_index(index_id, index_obj)

        # Always keep a local copy too
        with self.lock:
            self.indexes[index_id] = index_obj
            self.last_access[index_id] = time.time()
            self.last_access.move_to_end(index_id)
            self.logger.info(f"Cached index locally: {index_id}")

    def invalidate_index(self, table_name):
        """Invalidate all indexes for a table"""
        count = 0

        # Invalidate in buffer pool if available
        if self.buffer_pool:
            count += self.buffer_pool.invalidate_index(table_name)

        # Also invalidate in local cache
        with self.lock:
            for index_id in list(self.indexes.keys()):
                if table_name.lower() in index_id.lower():
                    del self.indexes[index_id]
                    if index_id in self.last_access:
                        del self.last_access[index_id]
                    count += 1

        self.logger.info(f"Invalidated {count} indexes for table {table_name}")
        return count
