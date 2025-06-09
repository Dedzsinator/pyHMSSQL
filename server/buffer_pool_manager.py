"""Buffer pool implementation with LRU/LFU caching strategies.

This module provides memory-efficient caching of B+ tree pages
and query results to improve performance.
"""

import logging
import threading
import time
from collections import OrderedDict, defaultdict
from enum import Enum


class CacheStrategy(Enum):
    """Cache eviction strategies"""

    LRU = 1  # Least Recently Used
    LFU = 2  # Least Frequently Used
    HYBRID = 3  # Hybrid approach (frequency and recency)


class BufferPool:
    """Buffer pool manager to cache frequently accessed data"""

    def __init__(self, capacity=1000, strategy=CacheStrategy.HYBRID):
        self.capacity = capacity
        self.strategy = strategy

        # Main page cache
        self.pages = OrderedDict()  # Maps page_id to page_data

        # Cache metadata
        self.last_access_time = {}  # Maps page_id to last access time
        self.access_counts = {}  # Maps page_id to access count
        self.dirty_pages = set()  # Set of dirty page_ids

        # Index cache - store loaded B+ tree indexes
        self.index_cache = {}  # Maps index_id to loaded index object
        self.index_last_access = {}  # Maps index_id to last access time

        # Thread synchronization
        self.global_lock = threading.RLock()
        self.logger = logging.getLogger("buffer_pool")

        # Statistics
        self.stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "writes": 0,
            "index_hits": 0,
            "index_misses": 0,
        }

    def get_index(self, index_id):
        """Get a cached index.

        Args:
            index_id: Unique ID of the index

        Returns:
            The cached index or None if not found
        """
        with self.global_lock:
            if index_id in self.index_cache:
                # Cache hit
                self.stats["index_hits"] += 1
                self.index_last_access[index_id] = time.time()
                logging.info(f"Index cache hit: {index_id}")
                return self.index_cache[index_id]

            # Cache miss
            self.stats["index_misses"] += 1
            logging.info(f"Index cache miss: {index_id}")
            return None

    def cache_index(self, index_id, index_obj):
        """Cache an index object.

        Args:
            index_id: Unique ID for the index
            index_obj: The index object to cache

        Returns:
            bool: True if successful
        """
        with self.global_lock:
            # If index already exists in cache, just update last access time
            if index_id in self.index_cache:
                self.index_last_access[index_id] = time.time()
                logging.info(f"Index already in cache: {index_id}")
                return True

            # If cache is full, evict least recently used index
            if (
                len(self.index_cache) >= self.capacity // 10
            ):  # Use 10% of capacity for indexes
                oldest_id = min(self.index_last_access.items(), key=lambda x: x[1])[0]
                del self.index_cache[oldest_id]
                del self.index_last_access[oldest_id]
                self.stats["evictions"] += 1

            # Cache the index
            self.index_cache[index_id] = index_obj
            self.index_last_access[index_id] = time.time()
            logging.info(f"Cached index: {index_id}")
            return True

    def invalidate_index(self, table_name):
        """Invalidate indexes for a table.

        Args:
            table_name: The name of the table

        Returns:
            int: Number of indexes invalidated
        """
        count = 0
        with self.global_lock:
            # Find indexes for this table
            for index_id in list(self.index_cache.keys()):
                if table_name in index_id:
                    del self.index_cache[index_id]
                    if index_id in self.index_last_access:
                        del self.index_last_access[index_id]
                    count += 1

        return count

    def get_page(self, page_id):
        """Get a page from the buffer pool.

        Args:
            page_id: Unique ID of the page (e.g., 'db_name.table_name.page_num')

        Returns:
            The page data if in pool, None otherwise
        """
        with self.global_lock:
            if page_id in self.pages:
                # Cache hit
                self.stats["hits"] += 1
                self._update_page_metadata(page_id)
                return self.pages[page_id]

            # Cache miss
            self.stats["misses"] += 1
            return None

    def put_page(self, page_id, page_data, is_dirty=False):
        """Add or update a page in the buffer pool.

        Args:
            page_id: Unique ID of the page
            page_data: The page data to store
            is_dirty: Whether the page has been modified

        Returns:
            True if successful, False if eviction failed
        """
        with self.global_lock:
            # If page already exists, update it
            if page_id in self.pages:
                self.pages[page_id] = page_data
                if is_dirty:
                    self.dirty_pages.add(page_id)
                self._update_page_metadata(page_id)
                return True

            # If pool is full, evict a page
            if len(self.pages) >= self.capacity:
                evicted = self._evict_page()
                if not evicted:
                    return False
                self.stats["evictions"] += 1

            # Add the new page
            self.pages[page_id] = page_data
            self.last_access_time[page_id] = time.time()
            self.access_counts[page_id] = 1

            if is_dirty:
                self.dirty_pages.add(page_id)

            return True

    def _update_page_metadata(self, page_id):
        """Update metadata for a page access."""
        # Move to end for LRU (most recently used)
        self.pages.move_to_end(page_id)

        # Update access count for LFU
        self.access_counts[page_id] += 1

        # Update access time
        self.last_access_time[page_id] = time.time()

    def _evict_page(self):
        """Evict a page based on the current strategy."""
        if not self.pages:
            return False

        if self.strategy == CacheStrategy.LRU:
            # Evict the least recently used page (first in OrderedDict)
            victim_id, victim_data = next(iter(self.pages.items()))
        elif self.strategy == CacheStrategy.LFU:
            # Evict the least frequently used page
            victim_id = min(self.pages.keys(), key=lambda k: self.access_counts[k])
            victim_data = self.pages[victim_id]
        else:  # HYBRID
            # Use a combination of frequency and recency
            current_time = time.time()
            victim_id = min(
                self.pages.keys(),
                key=lambda k: (
                    self.access_counts[k] * 0.7
                    + (current_time - self.last_access_time[k]) * 0.3
                ),
            )
            victim_data = self.pages[victim_id]

        # If dirty, write back before eviction
        if victim_id in self.dirty_pages:
            self._write_back(victim_id, victim_data)
            self.dirty_pages.remove(victim_id)

        # Remove the victim
        del self.pages[victim_id]
        del self.last_access_time[victim_id]
        del self.access_counts[victim_id]

        return True

    def _write_back(self, page_id, page_data):
        """Write a dirty page back to persistent storage.

        Args:
            page_id: ID of the page to write (e.g., 'db_name.table_name.page_num')
            page_data: The page data to write to disk

        Returns:
            bool: True if successful, False otherwise
        """
        self.stats["writes"] += 1
        self.logger.debug(f"Writing back dirty page: {page_id}")

        try:
            # Parse the page ID to determine file path and offset
            parts = page_id.split(".")
            if len(parts) < 2:
                self.logger.error(f"Invalid page ID format: {page_id}")
                return False

            # Extract components
            if len(parts) >= 3:
                db_name, table_name, page_num = parts[0], parts[1], int(parts[2])
            else:
                # Default to current database if not specified
                table_name, page_num = parts[0], int(parts[1])
                db_name = (
                    "default"  # This should come from the current database context
                )

            # Construct file path
            import os
            from pathlib import Path

            data_dir = os.environ.get("DATA_DIR", "data")
            table_path = Path(f"{data_dir}/{db_name}/{table_name}")
            page_file = table_path / f"page_{page_num}.dat"

            # Ensure directory exists
            os.makedirs(table_path, exist_ok=True)

            # Write the page data to file
            with open(page_file, "wb") as f:
                f.write(page_data)

            # Optionally update a page metadata file with timestamp
            metadata_file = table_path / "page_metadata.json"
            if os.path.exists(metadata_file):
                import json

                try:
                    with open(metadata_file, "r") as f:
                        metadata = json.load(f)
                except json.JSONDecodeError:
                    metadata = {}
            else:
                metadata = {}

            # Update metadata
            if "pages" not in metadata:
                metadata["pages"] = {}

            metadata["pages"][str(page_num)] = {
                "last_modified": time.time(),
                "size": len(page_data),
                "checksum": self._calculate_checksum(page_data),
            }

            # Write updated metadata
            with open(metadata_file, "w") as f:
                json.dump(metadata, f, indent=2)

            self.logger.info(f"Successfully wrote page {page_id} to {page_file}")
            return True

        except (IOError, ValueError, IndexError, OSError) as e:
            self.logger.error(f"Error writing page {page_id} to disk: {str(e)}")
            return False

    def _calculate_checksum(self, data):
        """Calculate a simple checksum for data integrity verification."""
        import hashlib

        return hashlib.md5(data).hexdigest()

    def get_stats(self):
        """Get buffer pool statistics."""
        hit_rate = 0
        if (self.stats["hits"] + self.stats["misses"]) > 0:
            hit_rate = self.stats["hits"] / (self.stats["hits"] + self.stats["misses"])

        return {
            **self.stats,
            "size": len(self.pages),
            "capacity": self.capacity,
            "dirty_pages": len(self.dirty_pages),
            "hit_rate": hit_rate,
        }

    def flush_all(self):
        """Flush all dirty pages to disk."""
        with self.global_lock:
            for page_id in list(self.dirty_pages):
                self._write_back(page_id, self.pages[page_id])
            self.dirty_pages.clear()
            return True

    def invalidate(self, table_name):
        """Invalidate all pages for a specific table.

        Args:
            table_name: Name of the table to invalidate

        Returns:
            int: Number of pages invalidated
        """
        count = 0
        with self.global_lock:
            # Find all page IDs that reference this table
            pages_to_invalidate = []
            for page_id in list(self.pages.keys()):
                # Check if page is from this table - page ID format is db.table.page_num
                parts = page_id.split(".")
                if len(parts) >= 2:
                    page_table = parts[1] if len(parts) >= 3 else parts[0]
                    if page_table.lower() == table_name.lower():
                        pages_to_invalidate.append(page_id)

            # Flush dirty pages before removing
            for page_id in pages_to_invalidate:
                if page_id in self.dirty_pages:
                    self._write_back(page_id, self.pages[page_id])
                    self.dirty_pages.remove(page_id)

                # Remove from cache
                del self.pages[page_id]
                if page_id in self.last_access_time:
                    del self.last_access_time[page_id]
                if page_id in self.access_counts:
                    del self.access_counts[page_id]
                count += 1

            self.logger.info(f"Invalidated {count} pages for table {table_name}")
            return count


class QueryResultCache:
    """Cache for query results to avoid redundant computation."""

    def __init__(self, capacity=100, ttl=300):
        """Initialize the query result cache.

        Args:
            capacity: Maximum number of query results to cache
            ttl: Time-to-live for cache entries in seconds
        """
        self.capacity = capacity
        self.ttl = ttl
        self.table_versions = {}
        self.version_lock = threading.Lock()
        self.logger = logging.getLogger(__name__)

        # Cache of query results
        self.cache = OrderedDict()
        # Maps tables to the queries that use them
        self.table_queries = defaultdict(set)
        self.lock = threading.Lock()

        # Statistics
        self.stats = {"hits": 0, "misses": 0, "invalidations": 0, "entries": 0}

    def get(self, query_hash):
        """Get cached result if still valid based on row versions."""
        with self.lock:
            # First check with the original hash
            if query_hash in self.cache:
                result = self._get_cached_result(query_hash)
                if result:
                    return result

            # Try looking for pattern matches (for queries with pagination/sorting variations)
            # This is a simple approach - in a real system, you'd use a more sophisticated method
            base_hash = (
                query_hash.split("_limit:")[0]
                if "_limit:" in query_hash
                else query_hash
            )
            base_hash = (
                base_hash.split("_orderby:")[0]
                if "_orderby:" in base_hash
                else base_hash
            )

            # Just to avoid full iteration in most cases
            if base_hash != query_hash:
                for cached_key in list(self.cache.keys()):
                    if cached_key.startswith(base_hash):
                        # Still need to verify this is a valid match
                        cached_result = self._get_cached_result(cached_key)
                        if cached_result:
                            return cached_result

            self.stats["misses"] += 1
            return None

    def _get_cached_result(self, query_hash):
        """Internal helper to get and validate a cached result."""
        if query_hash in self.cache:
            result, timestamp, affected_tables, versions = self.cache[query_hash]
            current_time = time.time()

            # Check if expired by time
            if current_time - timestamp > self.ttl:
                self._remove_entry(query_hash)
                return None

            # Check if table versions have changed
            for table in affected_tables:
                normalized_table = table.lower()
                current_version = self.table_versions.get(normalized_table, 0)
                cached_version = versions.get(normalized_table, 0)
                if current_version > cached_version:
                    # Table has been modified since last cache
                    logging.info(
                        f"Cache miss due to version change for table {table} ({cached_version} -> {current_version})"
                    )
                    self._remove_entry(query_hash)
                    return None

            # Move to end (most recently used)
            self.cache.move_to_end(query_hash)
            self.stats["hits"] += 1
            return result

        return None

    def update_table_version(self, table_name):
        """Increment the version number for a table."""
        with self.version_lock:
            # Normalize table name for consistency
            normalized_table = table_name.lower()
            current = self.table_versions.get(normalized_table, 0)
            new_version = current + 1
            self.table_versions[normalized_table] = new_version
            logging.info(
                f"Updated version for table {table_name}: {current} -> {new_version}"
            )
            return new_version

    def invalidate(self, table_name):
        """Invalidate all cache entries for a specific table."""
        count = 0
        with self.lock:
            # Normalize table name for case-insensitive matching
            normalized_table = table_name.lower()

            # Update the version to invalidate queries using this table
            self.update_table_version(normalized_table)

            # Find all queries that could be affected by this table
            queries_to_invalidate = set()

            # Add queries directly associated with this table
            for cached_table in list(self.table_queries.keys()):
                if cached_table.lower() == normalized_table:
                    queries_to_invalidate.update(self.table_queries[cached_table])

            # Also invalidate any query that might reference this table
            # This is more aggressive but ensures cache consistency
            for query_hash, (result, _, affected_tables, _) in list(self.cache.items()):
                # Include by explicit table reference
                if any(table.lower() == normalized_table for table in affected_tables):
                    queries_to_invalidate.add(query_hash)
                # Include by query text search
                elif isinstance(result, dict) and "query" in result:
                    raw_query = result["query"].lower()
                    if (
                        normalized_table in raw_query
                        or f"{normalized_table}." in raw_query
                        or f"from {normalized_table}" in raw_query
                    ):
                        queries_to_invalidate.add(query_hash)

            # Remove each invalid cache entry
            for query_hash in queries_to_invalidate:
                if query_hash in self.cache:
                    self._remove_entry(query_hash)
                    count += 1

            self.stats["invalidations"] += count
            logging.info(f"Invalidated {count} cache entries for table {table_name}")
            return count

    def put(self, query_hash, result, affected_tables=None):
        """Cache a query result with current row versions."""
        # Don't cache DML operation results
        if (
            result
            and isinstance(result, dict)
            and result.get("type")
            in ["insert_result", "update_result", "delete_result"]
        ):
            return

        # Check if this is a query with pagination or sorting
        # We should identify these in the query hash to avoid reusing incorrect results
        has_pagination = False
        has_sorting = False

        if isinstance(result, dict):
            # Check if result has pagination or sorting metadata
            if result.get("limit") is not None or result.get("offset") is not None:
                has_pagination = True

            if result.get("order_by") is not None:
                has_sorting = True

            # Enhance the hash with pagination/sorting info to avoid incorrect cache hits
            if has_pagination or has_sorting:
                pagination_info = (
                    f"_limit:{result.get('limit')}_offset:{result.get('offset')}"
                )
                sorting_info = (
                    f"_orderby:{result.get('order_by')}"
                    if result.get("order_by")
                    else ""
                )
                query_hash = f"{query_hash}{pagination_info}{sorting_info}"
                logging.info(
                    f"Enhanced query hash with pagination/sorting info: {query_hash}"
                )

        if not affected_tables:
            affected_tables = self._extract_tables_from_result(result)

        with self.lock:
            # If an existing entry has this hash, remove its table mappings first
            if query_hash in self.cache:
                _, _, old_tables, _ = self.cache[query_hash]
                for table in old_tables:
                    if query_hash in self.table_queries[table]:
                        self.table_queries[table].remove(query_hash)

            # If cache is full, remove oldest entry and its table mappings
            if len(self.cache) >= self.capacity and query_hash not in self.cache:
                oldest_hash, (_, _, old_tables, _) = self.cache.popitem(last=False)
                for table in old_tables:
                    if oldest_hash in self.table_queries[table]:
                        self.table_queries[table].remove(query_hash)

            # Capture current versions of affected tables
            versions = {}
            for table in affected_tables:
                versions[table] = self.table_versions.get(table, 0)

            # Add new entry with table information and version
            self.cache[query_hash] = (result, time.time(), affected_tables, versions)

            # Update table to query mappings
            for table in affected_tables:
                self.table_queries[table].add(query_hash)

            self.stats["entries"] = len(self.cache)

    def invalidate_for_table(self, table_name):
        """Alias for invalidate() method to maintain API compatibility"""
        return self.invalidate(table_name)

    def invalidate_all(self):
        """Invalidate all cache entries.

        Returns:
            int: Number of entries invalidated
        """
        count = len(self.cache)
        with self.lock:
            self.cache.clear()
            self.table_queries.clear()
            self.stats["entries"] = 0
            self.stats["invalidations"] += count
            self.logger.info(f"Invalidated all {count} cache entries")
            return count

    def _remove_entry(self, query_hash):
        """Remove a cache entry and its table mappings."""
        if query_hash in self.cache:
            _, _, tables, _ = self.cache[query_hash]
            # Remove query from all table mappings
            for table in tables:
                if query_hash in self.table_queries[table]:
                    self.table_queries[table].remove(query_hash)
                # Clean up empty table entries
                if not self.table_queries[table]:
                    del self.table_queries[table]
            # Remove from cache
            del self.cache[query_hash]

    def _extract_tables_from_result(self, result):
        """Extract table names from a query result."""
        tables = set()

        if isinstance(result, dict):
            # Direct table references
            for key in ["table", "table_name", "from_table"]:
                if key in result and isinstance(result[key], str):
                    tables.add(result[key])

            # Check for table lists
            for key in ["tables", "from_tables"]:
                if key in result and isinstance(result[key], list):
                    for table in result[key]:
                        if isinstance(table, str):
                            tables.add(table)
                        elif isinstance(table, dict) and "name" in table:
                            tables.add(table["name"])

        return list(tables)

    def clear(self):
        """Clear the entire cache."""
        with self.lock:
            self.cache.clear()
            self.table_queries.clear()
            self.stats["entries"] = 0

    def get_stats(self):
        """Get cache statistics."""
        with self.lock:
            hit_rate = 0
            if (self.stats["hits"] + self.stats["misses"]) > 0:
                hit_rate = self.stats["hits"] / (
                    self.stats["hits"] + self.stats["misses"]
                )

            return {
                **self.stats,
                "hit_rate": hit_rate,
                "table_count": len(self.table_queries),
                "memory_usage": self._estimate_memory_usage(),
            }

    def _estimate_memory_usage(self):
        """Estimate memory usage of the cache in bytes."""
        import sys

        total_size = 0

        # Sample a few entries to estimate average size
        sample_size = min(10, len(self.cache))
        if sample_size > 0:
            sample_keys = list(self.cache.keys())[:sample_size]
            sample_total = sum(sys.getsizeof(self.cache[k][0]) for k in sample_keys)
            avg_entry_size = sample_total / sample_size
            total_size = avg_entry_size * len(self.cache)

        return int(total_size)
