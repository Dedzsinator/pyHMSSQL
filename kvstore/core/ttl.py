"""
TTL (Time-To-Live) Management System
Handles key expiration with both active and passive cleanup.
"""

import asyncio
import time
import logging
import random
from typing import Dict, Set, Optional, List, Callable
from dataclasses import dataclass
from collections import defaultdict
from threading import RLock
import heapq

logger = logging.getLogger(__name__)

@dataclass
class TTLEntry:
    """Represents a key with TTL"""
    key: str
    expires_at: float
    created_at: float
    
    def __lt__(self, other):
        return self.expires_at < other.expires_at
    
    def is_expired(self, current_time: Optional[float] = None) -> bool:
        """Check if the entry is expired"""
        if current_time is None:
            current_time = time.time()
        return current_time >= self.expires_at

class TTLManager:
    """
    High-performance TTL manager with both active and passive expiration.
    
    Features:
    - Active expiration: Background task that periodically checks for expired keys
    - Passive expiration: Check expiration on key access
    - Efficient data structures: Min-heap for sorted expiration times
    - Configurable cleanup policies
    """
    
    def __init__(self, 
                 check_interval: float = 1.0,
                 max_keys_per_check: int = 100,
                 on_expire_callback: Optional[Callable[[str], None]] = None):
        self.check_interval = check_interval
        self.max_keys_per_check = max_keys_per_check
        self.on_expire_callback = on_expire_callback
        
        # TTL storage: key -> TTLEntry
        self._ttl_entries: Dict[str, TTLEntry] = {}
        
        # Min-heap of TTL entries sorted by expiration time
        self._expiration_heap: List[TTLEntry] = []
        
        # Thread safety
        self._lock = RLock()
        
        # Background task
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False
        
        # Statistics
        self.stats = {
            'total_keys_with_ttl': 0,
            'expired_keys': 0,
            'active_expirations': 0,
            'passive_expirations': 0,
            'cleanup_cycles': 0
        }
        
        logger.info(f"TTL Manager initialized with {check_interval}s interval")
    
    def start(self):
        """Start the background cleanup task"""
        if self._running:
            return
        
        self._running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("TTL Manager started")
    
    async def stop(self):
        """Stop the background cleanup task"""
        if not self._running:
            return
        
        self._running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        logger.info("TTL Manager stopped")
    
    def set_ttl(self, key: str, ttl_seconds: float) -> bool:
        """
        Set TTL for a key.
        
        Args:
            key: The key to set TTL for
            ttl_seconds: TTL in seconds
            
        Returns:
            True if TTL was set, False otherwise
        """
        if ttl_seconds <= 0:
            return False
        
        current_time = time.time()
        expires_at = current_time + ttl_seconds
        
        with self._lock:
            # Remove existing TTL if present
            if key in self._ttl_entries:
                self.remove_ttl(key)
            
            # Create new TTL entry
            entry = TTLEntry(key=key, expires_at=expires_at, created_at=current_time)
            
            # Add to storage
            self._ttl_entries[key] = entry
            heapq.heappush(self._expiration_heap, entry)
            
            self.stats['total_keys_with_ttl'] += 1
        
        logger.debug(f"Set TTL for key '{key}': {ttl_seconds}s (expires at {expires_at})")
        return True
    
    def get_ttl(self, key: str) -> Optional[float]:
        """
        Get remaining TTL for a key.
        
        Args:
            key: The key to check
            
        Returns:
            Remaining TTL in seconds, or None if no TTL set
        """
        with self._lock:
            if key not in self._ttl_entries:
                return None
            
            entry = self._ttl_entries[key]
            current_time = time.time()
            
            if entry.is_expired(current_time):
                # Key is expired, remove it
                self._expire_key(key, passive=True)
                return None
            
            return entry.expires_at - current_time
    
    def remove_ttl(self, key: str) -> bool:
        """
        Remove TTL for a key (make it persistent).
        
        Args:
            key: The key to remove TTL from
            
        Returns:
            True if TTL was removed, False if no TTL was set
        """
        with self._lock:
            if key not in self._ttl_entries:
                return False
            
            del self._ttl_entries[key]
            self.stats['total_keys_with_ttl'] -= 1
            
            # Note: We don't remove from heap immediately for performance
            # The cleanup loop will handle stale entries
            
        logger.debug(f"Removed TTL for key '{key}'")
        return True
    
    def is_expired(self, key: str) -> bool:
        """
        Check if a key is expired (passive expiration check).
        
        Args:
            key: The key to check
            
        Returns:
            True if the key is expired
        """
        with self._lock:
            if key not in self._ttl_entries:
                return False
            
            entry = self._ttl_entries[key]
            current_time = time.time()
            
            if entry.is_expired(current_time):
                self._expire_key(key, passive=True)
                return True
            
            return False
    
    def get_expired_keys(self, max_keys: Optional[int] = None) -> List[str]:
        """
        Get a list of expired keys.
        
        Args:
            max_keys: Maximum number of keys to return
            
        Returns:
            List of expired key names
        """
        expired_keys = []
        current_time = time.time()
        
        with self._lock:
            # Check keys in expiration order
            while self._expiration_heap and len(expired_keys) < (max_keys or float('inf')):
                entry = self._expiration_heap[0]
                
                # If entry is not expired, we're done (heap is sorted)
                if not entry.is_expired(current_time):
                    break
                
                # Remove from heap
                heapq.heappop(self._expiration_heap)
                
                # Check if key still exists and is expired
                if entry.key in self._ttl_entries:
                    current_entry = self._ttl_entries[entry.key]
                    if current_entry.expires_at == entry.expires_at:  # Same TTL entry
                        expired_keys.append(entry.key)
        
        return expired_keys
    
    def _expire_key(self, key: str, passive: bool = False):
        """
        Internal method to expire a key.
        
        Args:
            key: The key to expire
            passive: Whether this is a passive expiration
        """
        with self._lock:
            if key in self._ttl_entries:
                del self._ttl_entries[key]
                self.stats['total_keys_with_ttl'] -= 1
                self.stats['expired_keys'] += 1
                
                if passive:
                    self.stats['passive_expirations'] += 1
                else:
                    self.stats['active_expirations'] += 1
        
        # Call expiration callback
        if self.on_expire_callback:
            try:
                self.on_expire_callback(key)
            except Exception as e:
                logger.error(f"Error in expiration callback for key '{key}': {e}")
        
        logger.debug(f"Expired key '{key}' ({'passive' if passive else 'active'})")
    
    async def _cleanup_loop(self):
        """Background cleanup loop for active expiration"""
        logger.info("TTL cleanup loop started")
        
        while self._running:
            try:
                await asyncio.sleep(self.check_interval)
                
                if not self._running:
                    break
                
                await self._cleanup_expired_keys()
                self.stats['cleanup_cycles'] += 1
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in TTL cleanup loop: {e}")
                await asyncio.sleep(1)  # Brief pause before retrying
        
        logger.info("TTL cleanup loop stopped")
    
    async def _cleanup_expired_keys(self):
        """Clean up expired keys (active expiration)"""
        start_time = time.time()
        cleaned_count = 0
        
        try:
            # Get expired keys
            expired_keys = self.get_expired_keys(self.max_keys_per_check)
            
            # Expire them
            for key in expired_keys:
                self._expire_key(key, passive=False)
                cleaned_count += 1
            
            # Clean up stale heap entries
            self._cleanup_heap()
            
            cleanup_time = time.time() - start_time
            
            if cleaned_count > 0:
                logger.debug(f"Active cleanup: expired {cleaned_count} keys in {cleanup_time:.3f}s")
            
        except Exception as e:
            logger.error(f"Error during TTL cleanup: {e}")
    
    def _cleanup_heap(self):
        """Remove stale entries from the expiration heap"""
        with self._lock:
            # Rebuild heap with only valid entries
            valid_entries = []
            
            for entry in self._expiration_heap:
                if (entry.key in self._ttl_entries and 
                    self._ttl_entries[entry.key].expires_at == entry.expires_at):
                    valid_entries.append(entry)
            
            self._expiration_heap = valid_entries
            heapq.heapify(self._expiration_heap)
    
    def get_stats(self) -> Dict:
        """Get TTL manager statistics"""
        with self._lock:
            return {
                **self.stats,
                'current_keys_with_ttl': len(self._ttl_entries),
                'heap_size': len(self._expiration_heap)
            }
    
    def clear(self):
        """Clear all TTL entries"""
        with self._lock:
            self._ttl_entries.clear()
            self._expiration_heap.clear()
            self.stats['total_keys_with_ttl'] = 0
        
        logger.info("Cleared all TTL entries")
