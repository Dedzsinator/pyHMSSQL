"""
Document Store Module for pyHMSSQL Multimodel Extension

This module provides NoSQL/Document Store capabilities including:
- Schema-less JSON document storage
- Path-based indexing and querying
- Document collections with automatic ID generation
- JSONPath-style query support
"""

from .doc_adapter import (
    DocumentStoreAdapter,
    DocumentQuery,
    DocumentCollection,
    JSONPathParser,
)

__all__ = [
    "DocumentStoreAdapter",
    "DocumentQuery",
    "DocumentCollection",
    "JSONPathParser",
]
