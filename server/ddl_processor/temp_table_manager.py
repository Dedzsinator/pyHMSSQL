"""Temporary Table Manager for handling session-scoped temporary tables.

This module manages the creation, storage, and lifecycle of temporary tables
in the pyHMSSQL database system.
"""

import logging
import threading
from typing import Dict, Any, List, Optional, Set
from shared.utils import get_current_database_or_error


class TemporaryTableManager:
    """Handles temporary table operations with session-scoped lifecycle."""

    def __init__(self, catalog_manager):
        self.catalog_manager = catalog_manager
        self.logger = logging.getLogger(__name__)

        # Track temporary tables per session
        self._session_temp_tables: Dict[str, Set[str]] = {}
        self._lock = threading.Lock()

        # Default session ID for single-user mode
        self._default_session = "default_session"

    def execute_temporary_table_operation(self, plan, session_id: str = None):
        """Execute temporary table operations."""
        plan_type = plan.get("type")
        session_id = session_id or self._default_session

        if plan_type == "CREATE_TEMPORARY_TABLE":
            return self.execute_create_temporary_table(plan, session_id)
        elif plan_type == "DROP_TEMPORARY_TABLE":
            return self.execute_drop_temporary_table(plan, session_id)
        else:
            return {
                "error": f"Unsupported temporary table operation: {plan_type}",
                "status": "error",
            }

    def execute_create_temporary_table(self, plan, session_id: str = None):
        """Execute CREATE TEMPORARY TABLE operation."""
        session_id = session_id or self._default_session
        table_name = plan.get("table")
        columns = plan.get("columns", [])
        constraints = plan.get("constraints", [])

        if not table_name:
            return {"error": "No table name specified", "status": "error"}

        db_name, error = get_current_database_or_error(self.catalog_manager)
        if error:
            return error

        # Generate unique temporary table name
        temp_table_name = f"_temp_{session_id}_{table_name}"

        # Check if temporary table already exists in this session
        with self._lock:
            if session_id in self._session_temp_tables:
                if temp_table_name in self._session_temp_tables[session_id]:
                    return {
                        "error": f"Temporary table '{table_name}' already exists in current session",
                        "status": "error",
                    }
            else:
                self._session_temp_tables[session_id] = set()

        try:
            # Create the temporary table using regular table creation
            result = self.catalog_manager.create_table(
                temp_table_name, columns, constraints
            )

            if result is True:
                # Track the temporary table for this session
                with self._lock:
                    self._session_temp_tables[session_id].add(temp_table_name)

                return {
                    "message": f"Temporary table '{table_name}' created successfully",
                    "status": "success",
                    "temp_table_name": temp_table_name,
                }
            else:
                return {"error": str(result), "status": "error"}

        except Exception as e:
            self.logger.error(f"Error creating temporary table: {str(e)}")
            return {
                "error": f"Error creating temporary table: {str(e)}",
                "status": "error",
            }

    def execute_drop_temporary_table(self, plan, session_id: str = None):
        """Execute DROP TEMPORARY TABLE operation."""
        session_id = session_id or self._default_session
        table_name = plan.get("table")

        if not table_name:
            return {"error": "No table name specified", "status": "error"}

        # Generate temporary table name
        temp_table_name = f"_temp_{session_id}_{table_name}"

        # Check if temporary table exists in this session
        with self._lock:
            if (
                session_id not in self._session_temp_tables
                or temp_table_name not in self._session_temp_tables[session_id]
            ):
                return {
                    "error": f"Temporary table '{table_name}' does not exist in current session",
                    "status": "error",
                }

        try:
            # Drop the temporary table
            result = self.catalog_manager.drop_table(temp_table_name)

            if "successfully" in str(result) or result is True:
                # Remove from session tracking
                with self._lock:
                    self._session_temp_tables[session_id].discard(temp_table_name)

                return {
                    "message": f"Temporary table '{table_name}' dropped successfully",
                    "status": "success",
                }
            else:
                return {"error": str(result), "status": "error"}

        except Exception as e:
            self.logger.error(f"Error dropping temporary table: {str(e)}")
            return {
                "error": f"Error dropping temporary table: {str(e)}",
                "status": "error",
            }

    def cleanup_session_temp_tables(self, session_id: str):
        """Clean up all temporary tables for a session."""
        with self._lock:
            if session_id not in self._session_temp_tables:
                return

            temp_tables = self._session_temp_tables[session_id].copy()

            for temp_table_name in temp_tables:
                try:
                    self.catalog_manager.drop_table(temp_table_name)
                    self.logger.info(f"Cleaned up temporary table: {temp_table_name}")
                except Exception as e:
                    self.logger.error(
                        f"Error cleaning up temporary table {temp_table_name}: {str(e)}"
                    )

            # Clear the session's temporary tables
            del self._session_temp_tables[session_id]

    def list_session_temp_tables(self, session_id: str = None) -> Dict[str, Any]:
        """List temporary tables for a specific session."""
        session_id = session_id or self._default_session

        with self._lock:
            if session_id not in self._session_temp_tables:
                return {"temp_tables": [], "status": "success"}

            temp_tables = list(self._session_temp_tables[session_id])

            # Convert internal names back to user-friendly names
            user_friendly_names = []
            prefix = f"_temp_{session_id}_"

            for temp_table in temp_tables:
                if temp_table.startswith(prefix):
                    user_name = temp_table[len(prefix) :]
                    user_friendly_names.append(user_name)

            return {"temp_tables": user_friendly_names, "status": "success"}

    def resolve_table_name(self, table_name: str, session_id: str = None) -> str:
        """Resolve table name, checking for temporary tables first."""
        session_id = session_id or self._default_session
        temp_table_name = f"_temp_{session_id}_{table_name}"

        with self._lock:
            if (
                session_id in self._session_temp_tables
                and temp_table_name in self._session_temp_tables[session_id]
            ):
                return temp_table_name

        return table_name

    def is_temporary_table(self, table_name: str, session_id: str = None) -> bool:
        """Check if a table is a temporary table in the given session."""
        session_id = session_id or self._default_session
        temp_table_name = f"_temp_{session_id}_{table_name}"

        with self._lock:
            return (
                session_id in self._session_temp_tables
                and temp_table_name in self._session_temp_tables[session_id]
            )

    def get_session_count(self) -> int:
        """Get the number of active sessions with temporary tables."""
        with self._lock:
            return len(self._session_temp_tables)

    def get_total_temp_table_count(self) -> int:
        """Get the total number of temporary tables across all sessions."""
        with self._lock:
            return sum(len(tables) for tables in self._session_temp_tables.values())

    # Convenience methods for ExecutionEngine compatibility
    def create_temp_table(self, plan):
        """Convenience method for creating temporary tables."""
        session_id = plan.get("session_id")
        return self.execute_create_temporary_table(plan, session_id)

    def drop_temp_table(self, plan):
        """Convenience method for dropping temporary tables."""
        session_id = plan.get("session_id")
        return self.execute_drop_temporary_table(plan, session_id)

    def cleanup_all_temp_tables(self):
        """Clean up all temporary tables across all sessions."""
        with self._lock:
            sessions_to_cleanup = list(self._session_temp_tables.keys())

        for session_id in sessions_to_cleanup:
            self.cleanup_session_temp_tables(session_id)
