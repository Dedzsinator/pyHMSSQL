"""Lock manager for concurrency control in database operations.

This module provides locking mechanisms for tables during DML operations
to prevent race conditions and ensure data consistency.
"""

import threading
import time
import logging
from enum import Enum
from collections import defaultdict


class LockType(Enum):
    """Types of locks that can be acquired on database objects."""

    SHARED = 1  # For read operations
    EXCLUSIVE = 2  # For write operations


class LockManager:
    """Manages locks on database objects to control concurrent access."""

    def __init__(self):
        """Initialize the lock manager."""
        self._locks = defaultdict(threading.RLock)  # Table locks
        self._lock_owners = defaultdict(set)  # Who holds locks
        self._lock_types = {}  # Current lock types
        self._manager_lock = threading.RLock()  # Protect manager state
        self._wait_time = 5.0  # Default timeout (seconds)

    def acquire_lock(self, session_id, table_name, lock_type, wait_time=None):
        """Acquire a lock on a table for a session.

        Args:
            session_id: ID of the session requesting the lock
            table_name: Name of the table to lock
            lock_type: Type of lock (SHARED or EXCLUSIVE)
            wait_time: Time to wait for lock (None = use default)

        Returns:
            bool: True if lock acquired, False if timeout

        Raises:
            ValueError: If invalid arguments are provided
        """
        if not session_id or not table_name:
            raise ValueError("Session ID and table name must be provided")

        if wait_time is None:
            wait_time = self._wait_time

        lock_key = f"{table_name}"
        end_time = time.time() + wait_time

        while time.time() < end_time:
            with self._manager_lock:
                # Check if lock can be granted
                if lock_key not in self._lock_types:
                    # No locks on this table yet
                    self._lock_types[lock_key] = lock_type
                    self._lock_owners[lock_key].add(session_id)
                    logging.debug(
                        "Lock acquired: %s on %s by %s",
                        lock_type,  # Remove .name since lock_type is a string
                        table_name,
                        session_id,
                    )
                    return True

                # Check if session already has this lock
                if session_id in self._lock_owners[lock_key]:
                    current_lock = self._lock_types[lock_key]
                    # If requesting exclusive but have shared, need to upgrade
                    if (
                        current_lock == LockType.SHARED
                        and lock_type == LockType.EXCLUSIVE
                    ):
                        # Only allow upgrade if this is the only session
                        if len(self._lock_owners[lock_key]) == 1:
                            self._lock_types[lock_key] = LockType.EXCLUSIVE
                            logging.debug(
                                "Lock upgraded to EXCLUSIVE on %s by %s",
                                table_name,
                                session_id,
                            )
                            return True
                    else:
                        # Already has required lock or stronger
                        return True

                # Check if lock is compatible
                current_lock = self._lock_types[lock_key]
                if current_lock == LockType.SHARED and lock_type == LockType.SHARED:
                    # Multiple shared locks are allowed
                    self._lock_owners[lock_key].add(session_id)
                    logging.debug(
                        "Shared lock acquired: %s on %s by %s",
                        lock_type.name,
                        table_name,
                        session_id,
                    )
                    return True

            # Lock not available, wait and retry
            time.sleep(0.1)

        # Timeout occurred
        logging.warning(
            "Lock acquisition timeout: %s on %s by %s",
            lock_type.name,
            table_name,
            session_id,
        )
        return False

    def release_lock(self, session_id, table_name):
        """Release a lock held by a session.

        Args:
            session_id: ID of the session releasing the lock
            table_name: Name of the table to unlock

        Returns:
            bool: True if lock was released, False if not held
        """
        lock_key = f"{table_name}"

        with self._manager_lock:
            if (
                lock_key not in self._lock_owners
                or session_id not in self._lock_owners[lock_key]
            ):
                return False

            # Remove this session from owners
            self._lock_owners[lock_key].remove(session_id)

            # If no more owners, remove the lock
            if not self._lock_owners[lock_key]:
                del self._lock_types[lock_key]
                del self._lock_owners[lock_key]

            logging.debug("Lock released on %s by %s", table_name, session_id)
            return True

    def release_all_locks(self, session_id):
        """Release all locks held by a session.

        Args:
            session_id: ID of the session releasing locks

        Returns:
            int: Number of locks released
        """
        released = 0

        with self._manager_lock:
            # Find all locks held by this session
            for lock_key in list(self._lock_owners.keys()):
                if session_id in self._lock_owners[lock_key]:
                    self._lock_owners[lock_key].remove(session_id)
                    released += 1

                    # If no more owners, remove the lock
                    if not self._lock_owners[lock_key]:
                        del self._lock_types[lock_key]
                        del self._lock_owners[lock_key]

        return released

