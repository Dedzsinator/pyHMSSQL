"""
Unified Record Layout for pyHMSSQL multimodel support.

This module provides a unified binary layout that can efficiently store:
- Relational rows
- Object-relational composites
- Document key-value pairs
- Graph triplets (subject-predicate-object)

Optimizes for storage efficiency and fast access patterns.
"""

import json
import struct
import logging
from typing import Dict, Any, List, Optional, Union, Tuple
from enum import Enum


class RecordType(Enum):
    """Types of records in the unified layout."""

    RELATIONAL = 1
    OBJECT_RELATIONAL = 2
    DOCUMENT = 3
    GRAPH_VERTEX = 4
    GRAPH_EDGE = 5


class UnifiedRecordLayout:
    """
    Unified binary record layout supporting all model types.

    Layout format:
    [Header: 8 bytes][Data: variable]

    Header:
    - Record type (1 byte)
    - Flags (1 byte)
    - Data length (4 bytes)
    - Schema ID (2 bytes)

    Data section varies by record type.
    """

    def __init__(self):
        self.schema_cache = {}
        self.compression_enabled = True
        logging.info("UnifiedRecordLayout initialized")

    def encode_record(
        self, record_type: RecordType, data: Dict[str, Any], schema_id: int = 0
    ) -> bytes:
        """
        Encode a record of any type into unified binary format.

        Args:
            record_type: Type of record (RecordType enum)
            data: Record data as dictionary
            schema_id: Schema identifier

        Returns:
            Encoded binary data
        """
        if record_type == RecordType.RELATIONAL:
            return self.encode_relational_record(data, schema_id)
        elif record_type == RecordType.OBJECT_RELATIONAL:
            return self.encode_object_relational_record(data, [], schema_id)
        elif record_type == RecordType.DOCUMENT:
            return self.encode_document_record(data, schema_id)
        elif record_type == RecordType.GRAPH_VERTEX:
            return self.encode_graph_vertex(
                data.get("id", ""),
                data.get("label", ""),
                data.get("properties", {}),
                schema_id,
            )
        elif record_type == RecordType.GRAPH_EDGE:
            return self.encode_graph_edge(
                data.get("id", ""),
                data.get("from_vertex", ""),
                data.get("to_vertex", ""),
                data.get("label", ""),
                data.get("properties", {}),
                schema_id,
            )
        else:
            raise ValueError(f"Unknown record type: {record_type}")

    def encode_relational_record(
        self, record: Dict[str, Any], schema_id: int = 0
    ) -> bytes:
        """
        Encode a relational record into binary format.

        Args:
            record: Dictionary representing the record
            schema_id: Schema identifier for this record type

        Returns:
            Binary encoded record
        """
        try:
            # Serialize record data as JSON (can be optimized to binary later)
            data = json.dumps(record, separators=(",", ":")).encode("utf-8")

            # Compress if enabled and beneficial
            if self.compression_enabled and len(data) > 100:
                data = self._compress_data(data)
                flags = 0x01  # Compression flag
            else:
                flags = 0x00

            # Create header
            header = struct.pack(
                ">BBIB",  # Big-endian: byte, byte, int, byte
                RecordType.RELATIONAL.value,
                flags,
                len(data),
                schema_id & 0xFF,  # Truncate to 1 byte for simplicity
            )

            return header + data

        except Exception as e:
            logging.error(f"Error encoding relational record: {str(e)}")
            raise

    def encode_object_relational_record(
        self, record: Dict[str, Any], composite_attrs: List[str], schema_id: int = 0
    ) -> bytes:
        """
        Encode an object-relational record with composite attributes.

        Args:
            record: Record data
            composite_attrs: List of composite attribute names
            schema_id: Schema identifier

        Returns:
            Binary encoded record
        """
        try:
            # Separate flat and composite attributes
            flat_data = {}
            composite_data = {}

            for key, value in record.items():
                if key in composite_attrs:
                    # Handle composite attribute
                    if isinstance(value, str):
                        try:
                            composite_data[key] = json.loads(value)
                        except json.JSONDecodeError:
                            composite_data[key] = value
                    else:
                        composite_data[key] = value
                else:
                    flat_data[key] = value

            # Create structured layout
            structured_record = {"flat": flat_data, "composite": composite_data}

            data = json.dumps(structured_record, separators=(",", ":")).encode("utf-8")

            # Compress if beneficial
            if self.compression_enabled and len(data) > 100:
                data = self._compress_data(data)
                flags = 0x01
            else:
                flags = 0x00

            header = struct.pack(
                ">BBIB",
                RecordType.OBJECT_RELATIONAL.value,
                flags,
                len(data),
                schema_id & 0xFF,
            )

            return header + data

        except Exception as e:
            logging.error(f"Error encoding object-relational record: {str(e)}")
            raise

    def encode_document_record(
        self, document: Dict[str, Any], schema_id: int = 0
    ) -> bytes:
        """
        Encode a document (NoSQL) record.

        Args:
            document: Document data
            schema_id: Schema identifier

        Returns:
            Binary encoded record
        """
        try:
            # Documents are stored as-is but with metadata
            doc_record = {
                "document": document,
                "metadata": {
                    "doc_type": "json",
                    "indexed_paths": self._extract_indexable_paths(document),
                },
            }

            data = json.dumps(doc_record, separators=(",", ":")).encode("utf-8")

            if self.compression_enabled and len(data) > 100:
                data = self._compress_data(data)
                flags = 0x01
            else:
                flags = 0x00

            header = struct.pack(
                ">BBIB", RecordType.DOCUMENT.value, flags, len(data), schema_id & 0xFF
            )

            return header + data

        except Exception as e:
            logging.error(f"Error encoding document record: {str(e)}")
            raise

    def encode_graph_vertex(
        self, vertex_id: str, label: str, properties: Dict[str, Any], schema_id: int = 0
    ) -> bytes:
        """
        Encode a graph vertex record.

        Args:
            vertex_id: Unique vertex identifier
            label: Vertex label/type
            properties: Vertex properties
            schema_id: Schema identifier

        Returns:
            Binary encoded record
        """
        try:
            vertex_record = {
                "id": vertex_id,
                "label": label,
                "properties": properties,
                "type": "vertex",
            }

            data = json.dumps(vertex_record, separators=(",", ":")).encode("utf-8")

            if self.compression_enabled and len(data) > 100:
                data = self._compress_data(data)
                flags = 0x01
            else:
                flags = 0x00

            header = struct.pack(
                ">BBIB",
                RecordType.GRAPH_VERTEX.value,
                flags,
                len(data),
                schema_id & 0xFF,
            )

            return header + data

        except Exception as e:
            logging.error(f"Error encoding graph vertex: {str(e)}")
            raise

    def encode_graph_edge(
        self,
        edge_id: str,
        source_id: str,
        target_id: str,
        label: str,
        properties: Dict[str, Any],
        schema_id: int = 0,
    ) -> bytes:
        """
        Encode a graph edge record.

        Args:
            edge_id: Unique edge identifier
            source_id: Source vertex ID (maps to from_vertex)
            target_id: Target vertex ID (maps to to_vertex)
            label: Edge label/type
            properties: Edge properties
            schema_id: Schema identifier

        Returns:
            Binary encoded record
        """
        try:
            edge_record = {
                "id": edge_id,
                "from_vertex": source_id,  # Changed from 'source' to 'from_vertex'
                "to_vertex": target_id,  # Changed from 'target' to 'to_vertex'
                "label": label,
                "properties": properties,
                "type": "edge",
            }

            data = json.dumps(edge_record, separators=(",", ":")).encode("utf-8")

            if self.compression_enabled and len(data) > 100:
                data = self._compress_data(data)
                flags = 0x01
            else:
                flags = 0x00

            header = struct.pack(
                ">BBIB", RecordType.GRAPH_EDGE.value, flags, len(data), schema_id & 0xFF
            )

            return header + data

        except Exception as e:
            logging.error(f"Error encoding graph edge: {str(e)}")
            raise

    def decode_record(self, binary_data: bytes) -> Dict[str, Any]:
        """
        Decode a binary record back to dictionary format.

        Args:
            binary_data: Binary encoded record

        Returns:
            Decoded data dictionary
        """
        record_type, data = self.decode_record_with_type(binary_data)

        # Extract the actual data based on record type
        if record_type == RecordType.DOCUMENT:
            return data.get("document", data)
        elif record_type in [RecordType.GRAPH_VERTEX, RecordType.GRAPH_EDGE]:
            return data
        elif record_type == RecordType.OBJECT_RELATIONAL:
            # Merge flat and composite data
            result = data.get("flat", {}).copy()
            result.update(data.get("composite", {}))
            return result
        else:
            return data

    def decode_record_with_type(
        self, binary_data: bytes
    ) -> Tuple[RecordType, Dict[str, Any]]:
        """
        Decode a binary record back to its original format.

        Args:
            binary_data: Binary encoded record

        Returns:
            Tuple of (record_type, decoded_data)
        """
        try:
            if len(binary_data) < 8:
                raise ValueError("Invalid record: too short")

            # Parse header
            record_type_val, flags, data_length, schema_id = struct.unpack(
                ">BBIB", binary_data[:7]
            )

            record_type = RecordType(record_type_val)

            # Extract data section
            data_section = binary_data[7 : 7 + data_length]

            # Decompress if needed
            if flags & 0x01:
                data_section = self._decompress_data(data_section)

            # Parse JSON data
            json_data = json.loads(data_section.decode("utf-8"))

            return record_type, json_data

        except Exception as e:
            logging.error(f"Error decoding record: {str(e)}")
            raise

    def get_record_type(self, binary_data: bytes) -> RecordType:
        """Get the record type without full decoding."""
        if len(binary_data) < 1:
            raise ValueError("Invalid record")

        record_type_val = struct.unpack(">B", binary_data[:1])[0]
        return RecordType(record_type_val)

    def _compress_data(self, data: bytes) -> bytes:
        """Compress data using simple compression."""
        try:
            import zlib

            return zlib.compress(data, level=1)  # Fast compression
        except ImportError:
            # Fallback - no compression
            return data

    def _decompress_data(self, data: bytes) -> bytes:
        """Decompress data."""
        try:
            import zlib

            return zlib.decompress(data)
        except ImportError:
            # Fallback - assume no compression
            return data

    def _extract_indexable_paths(
        self, document: Dict[str, Any], prefix: str = ""
    ) -> List[str]:
        """Extract indexable paths from a document."""
        paths = []

        def extract_paths(obj, current_path):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_path = f"{current_path}.{key}" if current_path else key
                    paths.append(new_path)

                    if isinstance(value, (dict, list)):
                        extract_paths(value, new_path)
            elif isinstance(obj, list):
                # For arrays, we might want to index by position or content
                paths.append(f"{current_path}[]")

        extract_paths(document, prefix)
        return paths
