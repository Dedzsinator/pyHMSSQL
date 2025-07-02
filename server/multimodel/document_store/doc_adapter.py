"""
Document Store Adapter for pyHMSSQL Multimodel Extension

This adapter provides NoSQL/Document Store capabilities including:
- Schema-less JSON document storage
- Path-based indexing and querying
- Document collections with automatic ID generation
- JSONPath-style query support
- Integration with existing B+ tree infrastructure
"""

import json
import uuid
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime
import re
from dataclasses import dataclass, field
import logging

from ..unified.record_layout import UnifiedRecordLayout, RecordType
from catalog_manager import CatalogManager

# Use optimized B+ Tree for document storage with proper encoding
try:
    from bptree import BPlusTreeOptimized as BPTree
except ImportError:
    from bptree_adapter import BPlusTree as BPTree
from transaction.transaction_manager import TransactionManager


class JSONPathParser:
    """Parser for JSONPath expressions"""

    @staticmethod
    def extract_paths(
        document: Dict[str, Any], prefix: str = ""
    ) -> List[Tuple[str, Any]]:
        """Extract all paths and values from a JSON document"""
        paths = []

        if isinstance(document, dict):
            for key, value in document.items():
                current_path = f"{prefix}.{key}" if prefix else key
                paths.append((current_path, value))

                if isinstance(value, (dict, list)):
                    paths.extend(JSONPathParser.extract_paths(value, current_path))

        elif isinstance(document, list):
            # Add the array itself
            if prefix:
                paths.append((prefix, document))

            # Add individual array elements
            for i, item in enumerate(document):
                current_path = f"{prefix}.{i}" if prefix else str(i)
                paths.append((current_path, item))

                if isinstance(item, (dict, list)):
                    paths.extend(JSONPathParser.extract_paths(item, current_path))

        return paths

    @staticmethod
    def get_value_by_path(document: Dict[str, Any], path: str) -> Any:
        """Get value from document using JSONPath"""
        try:
            parts = JSONPathParser._parse_path(path)
            current = document

            for part in parts:
                if isinstance(part, str):
                    if isinstance(current, dict):
                        current = current.get(part)
                    else:
                        return None
                elif isinstance(part, int):
                    if isinstance(current, list) and 0 <= part < len(current):
                        current = current[part]
                    else:
                        return None

                if current is None:
                    return None

            return current
        except:
            return None

    @staticmethod
    def _parse_path(path: str) -> List[Union[str, int]]:
        """Parse JSONPath into components"""
        parts = []
        current = ""
        i = 0

        while i < len(path):
            if path[i] == ".":
                if current:
                    # Try to convert to integer if it's numeric
                    try:
                        parts.append(int(current))
                    except ValueError:
                        parts.append(current)
                    current = ""
            elif path[i] == "[":
                if current:
                    parts.append(current)
                    current = ""
                # Find closing bracket
                j = i + 1
                while j < len(path) and path[j] != "]":
                    j += 1
                if j < len(path):
                    index_str = path[i + 1 : j]
                    try:
                        parts.append(int(index_str))
                    except ValueError:
                        parts.append(index_str.strip("\"'"))
                    i = j
            else:
                current += path[i]
            i += 1

        if current:
            # Try to convert to integer if it's numeric
            try:
                parts.append(int(current))
            except ValueError:
                parts.append(current)

        return parts

    @staticmethod
    def evaluate_filter(
        document: Dict[str, Any], filter_conditions: Dict[str, Any]
    ) -> bool:
        """Evaluate filter conditions against a document"""
        for path, condition in filter_conditions.items():
            value = JSONPathParser.get_value_by_path(document, path)

            if isinstance(condition, dict):
                # Handle comparison operators
                for op, expected in condition.items():
                    if op == "$gt":
                        if not (value is not None and value > expected):
                            return False
                    elif op == "$gte":
                        if not (value is not None and value >= expected):
                            return False
                    elif op == "$lt":
                        if not (value is not None and value < expected):
                            return False
                    elif op == "$lte":
                        if not (value is not None and value <= expected):
                            return False
                    elif op == "$ne":
                        if not (value != expected):
                            return False
                    elif op == "$eq":
                        if not (value == expected):
                            return False
                    else:
                        # Unknown operator, treat as equality
                        if value != expected:
                            return False
            else:
                # Direct equality comparison
                if value != condition:
                    return False

        return True


@dataclass
class DocumentCollection:
    """Represents a document collection"""

    name: str
    schema_name: str = "public"
    created_at: datetime = field(default_factory=datetime.now)
    document_count: int = 0
    total_size: int = 0
    indexes: Dict[str, str] = field(default_factory=dict)  # path -> index_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "schema_name": self.schema_name,
            "created_at": self.created_at.isoformat(),
            "document_count": self.document_count,
            "total_size": self.total_size,
            "indexes": self.indexes,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DocumentCollection":
        return cls(
            name=data["name"],
            schema_name=data.get("schema_name", "public"),
            created_at=datetime.fromisoformat(
                data.get("created_at", datetime.now().isoformat())
            ),
            document_count=data.get("document_count", 0),
            total_size=data.get("total_size", 0),
            indexes=data.get("indexes", {}),
        )


@dataclass
class DocumentQuery:
    """Represents a document query"""

    collection: str
    filter_conditions: Dict[str, Any] = field(default_factory=dict)
    projection: Optional[List[str]] = None
    sort_by: Optional[List[Tuple[str, str]]] = None  # (field, direction)
    limit: Optional[int] = None
    skip: Optional[int] = None


class DocumentStoreAdapter:
    """Adapter for NoSQL/Document Store operations"""

    def __init__(
        self, catalog_manager: CatalogManager, transaction_manager: TransactionManager
    ):
        self.catalog_manager = catalog_manager
        self.transaction_manager = transaction_manager
        self.record_layout = UnifiedRecordLayout()
        self.collections: Dict[str, DocumentCollection] = {}
        self.path_indexes: Dict[str, BPTree] = {}  # path -> index

        # Load existing collections from catalog
        self._load_collections()

    def _load_collections(self):
        """Load existing document collections from catalog"""
        try:
            # Try to get multimodel metadata from catalog
            multimodel_meta = self.catalog_manager.get_table_schema(
                "__multimodel_collections__"
            )
            if multimodel_meta:
                # Load collection metadata from catalog
                try:
                    collections_data = self.catalog_manager.get_collections_metadata()
                    for collection_info in collections_data:
                        collection_name = collection_info.get("name")
                        schema_name = collection_info.get("schema", "public")
                        collection_key = f"{schema_name}.{collection_name}"

                        if collection_key not in self.collections:
                            self.collections[collection_key] = (
                                DocumentCollection.from_dict(collection_info)
                            )

                        # Restore primary index
                        primary_index = BPTree(order=100)
                        self.path_indexes[f"{collection_key}._id"] = primary_index
                except Exception as e:
                    logging.warning(f"Failed to load collection metadata: {e}")
        except:
            # Collections table doesn't exist yet
            pass

    def create_collection(self, name: str, schema: str = "public") -> bool:
        """Create a new document collection"""
        collection_key = f"{schema}.{name}"

        if collection_key in self.collections:
            raise ValueError(f"Collection '{collection_key}' already exists")

        collection = DocumentCollection(name=name, schema_name=schema)
        self.collections[collection_key] = collection

        # Create primary index for document IDs
        primary_index_name = f"idx_{schema}_{name}_id"
        primary_index = BPTree(order=100)  # Use same order as existing B+ trees
        self.path_indexes[f"{collection_key}._id"] = primary_index

        # Persist collection metadata to catalog
        try:
            collection_metadata = collection.to_dict()
            self.catalog_manager.register_collection(name, schema, collection_metadata)
        except Exception as e:
            logging.warning(f"Failed to persist collection metadata: {e}")

        return True

    def drop_collection(self, name: str, schema: str = "public") -> bool:
        """Drop a document collection"""
        collection_key = f"{schema}.{name}"

        if collection_key not in self.collections:
            return False

        # Drop all indexes for this collection
        indexes_to_drop = [
            path
            for path in self.path_indexes.keys()
            if path.startswith(f"{collection_key}.")
        ]

        for index_path in indexes_to_drop:
            del self.path_indexes[index_path]

        del self.collections[collection_key]

        # Remove from catalog
        try:
            self.catalog_manager.drop_collection(name, schema)
        except Exception as e:
            logging.warning(f"Failed to remove collection from catalog: {e}")

        return True

    def insert_document(
        self,
        collection: str,
        document: Dict[str, Any],
        schema: str = "public",
        doc_id: str = None,
    ) -> str:
        """Insert a document into a collection"""
        collection_key = f"{schema}.{collection}"

        if collection_key not in self.collections:
            raise ValueError(f"Collection '{collection_key}' does not exist")

        # Generate document ID if not provided
        if doc_id is None:
            doc_id = str(uuid.uuid4())

        # Add metadata to document
        document_with_meta = {
            "_id": doc_id,
            "_collection": collection,
            "_schema": schema,
            "_created_at": datetime.now().isoformat(),
            "_updated_at": datetime.now().isoformat(),
            **document,
        }

        # Serialize document using UnifiedRecordLayout
        record_data = self.record_layout.encode_record(
            record_type=RecordType.DOCUMENT, data=document_with_meta
        )

        # Store in primary index - convert string doc_id to numeric key
        primary_index = self.path_indexes[f"{collection_key}._id"]
        numeric_key = self._string_to_numeric_key(doc_id)
        primary_index.insert(numeric_key, record_data)

        # Update path indexes
        self._update_path_indexes(collection_key, doc_id, document_with_meta)

        # Update collection statistics
        collection_obj = self.collections[collection_key]
        collection_obj.document_count += 1
        collection_obj.total_size += len(record_data)

        return doc_id

    def find_documents(self, query: DocumentQuery) -> List[Dict[str, Any]]:
        """Find documents matching query criteria"""
        collection_key = f"{query.collection}"
        if "." not in collection_key:
            collection_key = f"public.{query.collection}"

        if collection_key not in self.collections:
            raise ValueError(f"Collection '{collection_key}' does not exist")

        # Get all documents from primary index
        primary_index = self.path_indexes[f"{collection_key}._id"]

        # Use range_query to get all documents from the optimized B+ tree
        try:
            all_records = primary_index.range_query(float("-inf"), float("inf"))

            # Filter documents based on query conditions
            matching_docs = []
            for record_item in all_records:
                try:
                    # The optimized B+ tree returns tuples (key, value)
                    if isinstance(record_item, tuple) and len(record_item) == 2:
                        numeric_key, record_data = record_item

                        # Skip if record_data is not bytes (could be float due to B+ tree issues)
                        if not isinstance(record_data, bytes):
                            # Try to get the document using individual search instead
                            record_data = primary_index.search(numeric_key)
                            if not isinstance(record_data, bytes):
                                continue

                        document = self.record_layout.decode_record(record_data)
                        if self._matches_filter(document, query.filter_conditions):
                            matching_docs.append(document)
                    else:
                        logging.debug(f"Unexpected record format: {type(record_item)}")
                except Exception as e:
                    logging.error(f"Error processing record: {e}")
                    continue
        except Exception as e:
            logging.error(f"Error in range_query: {e}")
            return []

        # Apply projection
        if query.projection:
            projected_docs = []
            for doc in matching_docs:
                projected = {}
                # Always include _id unless explicitly excluded
                if "_id" in doc:
                    projected["_id"] = doc["_id"]
                for field in query.projection:
                    value = JSONPathParser.get_value_by_path(doc, field)
                    if value is not None:
                        projected[field] = value
                projected_docs.append(projected)
            matching_docs = projected_docs

        # Apply sorting
        if query.sort_by:
            for sort_field, direction in reversed(query.sort_by):
                reverse = direction.lower() == "desc"
                matching_docs.sort(
                    key=lambda doc: JSONPathParser.get_value_by_path(doc, sort_field)
                    or "",
                    reverse=reverse,
                )

        # Apply skip and limit
        if query.skip:
            matching_docs = matching_docs[query.skip :]
        if query.limit:
            matching_docs = matching_docs[: query.limit]

        return matching_docs

    def find_document_by_id(
        self, collection: str, doc_id: str, schema: str = "public"
    ) -> Optional[Dict[str, Any]]:
        """Find a document by its ID"""
        collection_key = f"{schema}.{collection}"

        if collection_key not in self.collections:
            return None

        primary_index = self.path_indexes[f"{collection_key}._id"]
        numeric_key = self._string_to_numeric_key(doc_id)
        record_data = primary_index.search(numeric_key)

        if record_data:
            return self.record_layout.decode_record(record_data)

        return None

    def update_document(
        self,
        collection: str,
        doc_id: str,
        updates: Dict[str, Any],
        schema: str = "public",
    ) -> bool:
        """Update a document"""
        collection_key = f"{schema}.{collection}"

        if collection_key not in self.collections:
            return False

        # Get existing document
        existing_doc = self.find_document_by_id(collection, doc_id, schema)
        if not existing_doc:
            return False

        # Apply updates
        updated_doc = existing_doc.copy()
        updated_doc.update(updates)
        updated_doc["_updated_at"] = datetime.now().isoformat()

        # Re-encode and store
        record_data = self.record_layout.encode_record(
            record_type=RecordType.DOCUMENT, data=updated_doc
        )

        primary_index = self.path_indexes[f"{collection_key}._id"]
        numeric_key = self._string_to_numeric_key(doc_id)
        primary_index.insert(numeric_key, record_data)  # This will overwrite existing

        # Update path indexes
        self._update_path_indexes(
            collection_key, doc_id, updated_doc, remove_old=existing_doc
        )

        return True

    def delete_document(
        self, collection: str, doc_id: str, schema: str = "public"
    ) -> bool:
        """Delete a document"""
        collection_key = f"{schema}.{collection}"

        if collection_key not in self.collections:
            return False

        # Get existing document for index cleanup
        existing_doc = self.find_document_by_id(collection, doc_id, schema)
        if not existing_doc:
            return False

        # Remove from primary index
        primary_index = self.path_indexes[f"{collection_key}._id"]
        numeric_key = self._string_to_numeric_key(doc_id)
        primary_index.delete(numeric_key)

        # Clean up path indexes
        self._remove_from_path_indexes(collection_key, doc_id, existing_doc)

        # Update collection statistics
        collection_obj = self.collections[collection_key]
        collection_obj.document_count -= 1

        return True

    def create_index(self, collection: str, path: str, schema: str = "public") -> str:
        """Create an index on a document path"""
        collection_key = f"{schema}.{collection}"

        if collection_key not in self.collections:
            raise ValueError(f"Collection '{collection_key}' does not exist")

        index_path = f"{collection_key}.{path}"

        if index_path in self.path_indexes:
            raise ValueError(f"Index on path '{path}' already exists")

        # Create new B+ tree index
        index = BPTree(order=100)
        self.path_indexes[index_path] = index

        # Populate index with existing documents
        primary_index = self.path_indexes[f"{collection_key}._id"]
        all_doc_ids = []

        def collect_ids(node):
            if hasattr(node, "keys"):
                for key in node.keys:
                    if key is not None:
                        all_doc_ids.append(key)
            if hasattr(node, "children"):
                for child in node.children:
                    if child is not None:
                        collect_ids(child)

        if primary_index.root:
            collect_ids(primary_index.root)

        # Index existing documents
        for doc_id in all_doc_ids:
            try:
                record_data = primary_index.search(doc_id)
                if record_data:
                    document = self.record_layout.decode_record(record_data)
                    value = JSONPathParser.get_value_by_path(document, path)
                    if value is not None:
                        # Convert value to string for indexing
                        index_key = str(value) if not isinstance(value, str) else value
                        index.insert(index_key, doc_id)
            except:
                continue

        # Update collection metadata
        collection_obj = self.collections[collection_key]
        index_name = f"idx_{schema}_{collection}_{path.replace('.', '_')}"
        collection_obj.indexes[path] = index_name

        return index_name

    def drop_index(self, collection: str, path: str, schema: str = "public") -> bool:
        """Drop an index on a document path"""
        collection_key = f"{schema}.{collection}"
        index_path = f"{collection_key}.{path}"

        if index_path not in self.path_indexes:
            return False

        del self.path_indexes[index_path]

        # Update collection metadata
        collection_obj = self.collections.get(collection_key)
        if collection_obj and path in collection_obj.indexes:
            del collection_obj.indexes[path]

        return True

    def execute_query(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a document store operation based on the plan.

        Args:
            plan: Query execution plan

        Returns:
            Query result
        """
        try:
            operation_type = plan.get("type", "").upper()

            if operation_type == "CREATE_COLLECTION":
                collection_name = plan.get("collection")
                options = plan.get("options", {})

                if not collection_name:
                    return {"error": "Collection name is required", "status": "error"}

                # Create collection in DocumentStoreAdapter
                try:
                    result = self.create_collection(collection_name)
                    if result:
                        # Also register with catalog manager for consistency
                        self.catalog_manager.create_collection(collection_name, options)
                        return {
                            "message": f"Collection '{collection_name}' created successfully",
                            "status": "success",
                        }
                    else:
                        return {
                            "error": f"Failed to create collection '{collection_name}'",
                            "status": "error",
                        }
                except ValueError as e:
                    return {"error": str(e), "status": "error"}

            elif operation_type == "DROP_COLLECTION":
                collection_name = plan.get("collection")

                if not collection_name:
                    return {"error": "Collection name is required", "status": "error"}

                # Drop collection from DocumentStoreAdapter
                try:
                    result = self.drop_collection(collection_name)
                    if result:
                        # Also remove from catalog manager for consistency
                        self.catalog_manager.drop_collection(collection_name)
                        return {
                            "message": f"Collection '{collection_name}' dropped successfully",
                            "status": "success",
                        }
                    else:
                        return {
                            "error": f"Collection '{collection_name}' does not exist",
                            "status": "error",
                        }
                except ValueError as e:
                    return {"error": str(e), "status": "error"}

            elif operation_type == "DOCUMENT_INSERT":
                collection_name = plan.get("collection")
                document = plan.get("document")

                if not collection_name:
                    return {"error": "Collection name is required", "status": "error"}

                if not document:
                    return {"error": "Document is required", "status": "error"}

                # Parse document if it's a string
                if isinstance(document, str):
                    try:
                        import json

                        document = json.loads(document)
                    except json.JSONDecodeError as e:
                        return {
                            "error": f"Invalid JSON document: {str(e)}",
                            "status": "error",
                        }

                # Insert document
                try:
                    doc_id = self.insert_document(collection_name, document)
                    return {
                        "message": f"Document inserted successfully",
                        "document_id": doc_id,
                        "status": "success",
                    }
                except Exception as e:
                    return {
                        "error": f"Failed to insert document: {str(e)}",
                        "status": "error",
                    }

            elif operation_type == "DOCUMENT_FIND":
                collection_name = plan.get("collection")
                query_filter = plan.get("query_filter", "{}")
                projection = plan.get("projection", "{}")

                if not collection_name:
                    return {"error": "Collection name is required", "status": "error"}

                # Parse filters and projection
                try:
                    import json

                    if isinstance(query_filter, str):
                        filter_dict = json.loads(query_filter)
                    else:
                        filter_dict = query_filter or {}

                    if isinstance(projection, str):
                        projection_dict = json.loads(projection)
                    else:
                        projection_dict = projection or {}

                    # Convert projection dict to list of fields
                    projection_fields = (
                        [field for field, include in projection_dict.items() if include]
                        if projection_dict
                        else None
                    )

                except json.JSONDecodeError as e:
                    return {
                        "error": f"Invalid JSON in query: {str(e)}",
                        "status": "error",
                    }

                # Create and execute query
                try:
                    query = DocumentQuery(
                        collection=collection_name,
                        filter_conditions=filter_dict,
                        projection=projection_fields,
                    )
                    documents = self.find_documents(query)

                    return {
                        "documents": documents,
                        "count": len(documents),
                        "status": "success",
                    }
                except Exception as e:
                    return {
                        "error": f"Failed to find documents: {str(e)}",
                        "status": "error",
                    }

            elif operation_type == "DOCUMENT_UPDATE":
                collection_name = plan.get("collection")
                filter_dict = plan.get("filter", "{}")
                update_dict = plan.get("update", "{}")

                if not collection_name:
                    return {"error": "Collection name is required", "status": "error"}

                # Parse filter and update documents
                try:
                    import json

                    if isinstance(filter_dict, str):
                        filter_conditions = json.loads(filter_dict)
                    else:
                        filter_conditions = filter_dict or {}

                    if isinstance(update_dict, str):
                        updates = json.loads(update_dict)
                    else:
                        updates = update_dict or {}

                except json.JSONDecodeError as e:
                    return {
                        "error": f"Invalid JSON in update: {str(e)}",
                        "status": "error",
                    }

                # Find matching documents and update them
                try:
                    query = DocumentQuery(
                        collection=collection_name, filter_conditions=filter_conditions
                    )
                    matching_docs = self.find_documents(query)

                    updated_count = 0
                    for doc in matching_docs:
                        doc_id = doc.get("_id")
                        if doc_id and self.update_document(
                            collection_name, doc_id, updates
                        ):
                            updated_count += 1

                    return {
                        "message": f"Updated {updated_count} document(s)",
                        "count": updated_count,
                        "status": "success",
                    }
                except Exception as e:
                    return {
                        "error": f"Failed to update documents: {str(e)}",
                        "status": "error",
                    }

            elif operation_type == "DOCUMENT_DELETE":
                collection_name = plan.get("collection")
                filter_dict = plan.get("filter", "{}")

                if not collection_name:
                    return {"error": "Collection name is required", "status": "error"}

                # Parse filter
                try:
                    import json

                    if isinstance(filter_dict, str):
                        filter_conditions = json.loads(filter_dict)
                    else:
                        filter_conditions = filter_dict or {}

                except json.JSONDecodeError as e:
                    return {
                        "error": f"Invalid JSON in filter: {str(e)}",
                        "status": "error",
                    }

                # Find matching documents and delete them
                try:
                    query = DocumentQuery(
                        collection=collection_name, filter_conditions=filter_conditions
                    )
                    matching_docs = self.find_documents(query)

                    deleted_count = 0
                    for doc in matching_docs:
                        doc_id = doc.get("_id")
                        if doc_id and self.delete_document(collection_name, doc_id):
                            deleted_count += 1

                    return {
                        "message": f"Deleted {deleted_count} document(s)",
                        "count": deleted_count,
                        "status": "success",
                    }
                except Exception as e:
                    return {
                        "error": f"Failed to delete documents: {str(e)}",
                        "status": "error",
                    }

            elif operation_type == "DOCUMENT_AGGREGATE":
                collection_name = plan.get("collection")
                pipeline = plan.get("pipeline", [])

                if not collection_name:
                    return {"error": "Collection name is required", "status": "error"}

                # Execute aggregation pipeline
                try:
                    results = self.aggregate(collection_name, pipeline)

                    return {
                        "results": results,
                        "count": len(results),
                        "status": "success",
                    }
                except Exception as e:
                    return {
                        "error": f"Failed to execute aggregation: {str(e)}",
                        "status": "error",
                    }

            else:
                return {
                    "error": f"Unsupported document store operation: {operation_type}",
                    "status": "error",
                }

        except Exception as e:
            logging.error(f"Error in DocumentStoreAdapter.execute_query: {str(e)}")
            return {
                "error": f"Document store execution error: {str(e)}",
                "status": "error",
            }

    def get_collection_info(
        self, name: str, schema: str = "public"
    ) -> Optional[DocumentCollection]:
        """Get information about a collection"""
        collection_key = f"{schema}.{name}"
        return self.collections.get(collection_key)

    def list_collections(self, schema: str = None) -> List[DocumentCollection]:
        """List all collections, optionally filtered by schema"""
        if schema:
            return [
                col
                for key, col in self.collections.items()
                if key.startswith(f"{schema}.")
            ]
        return list(self.collections.values())

    def aggregate(
        self, collection: str, pipeline: List[Dict[str, Any]], schema: str = "public"
    ) -> List[Dict[str, Any]]:
        """Execute an aggregation pipeline (basic implementation)"""
        # Start with all documents
        query = DocumentQuery(collection=f"{schema}.{collection}")
        results = self.find_documents(query)

        # Process each stage in pipeline
        for stage in pipeline:
            if "$match" in stage:
                # Filter stage
                filtered = []
                for doc in results:
                    if self._matches_filter(doc, stage["$match"]):
                        filtered.append(doc)
                results = filtered

            elif "$project" in stage:
                # Projection stage
                projected = []
                for doc in results:
                    new_doc = {}
                    for field, include in stage["$project"].items():
                        if include:
                            value = JSONPathParser.get_value_by_path(doc, field)
                            if value is not None:
                                new_doc[field] = value
                    projected.append(new_doc)
                results = projected

            elif "$sort" in stage:
                # Sort stage
                sort_fields = [
                    (field, "desc" if direction == -1 else "asc")
                    for field, direction in stage["$sort"].items()
                ]
                for field, direction in reversed(sort_fields):
                    reverse = direction == "desc"
                    results.sort(
                        key=lambda doc: JSONPathParser.get_value_by_path(doc, field)
                        or "",
                        reverse=reverse,
                    )

            elif "$limit" in stage:
                # Limit stage
                results = results[: stage["$limit"]]

            elif "$skip" in stage:
                # Skip stage
                results = results[stage["$skip"] :]

            elif "$group" in stage:
                # Group stage - basic implementation
                group_spec = stage["$group"]
                group_id = group_spec.get("_id")

                if group_id is None:
                    # Group all documents together
                    grouped = {"_id": None, "items": results}
                    # Apply aggregation functions
                    for field, aggregation in group_spec.items():
                        if field == "_id":
                            continue
                        if isinstance(aggregation, dict):
                            for op, path in aggregation.items():
                                if op == "$sum":
                                    if path == 1:
                                        grouped[field] = len(results)
                                    else:
                                        path = path.lstrip("$")
                                        total = sum(
                                            JSONPathParser.get_value_by_path(doc, path)
                                            or 0
                                            for doc in results
                                        )
                                        grouped[field] = total
                                elif op == "$avg":
                                    path = path.lstrip("$")
                                    values = [
                                        JSONPathParser.get_value_by_path(doc, path)
                                        for doc in results
                                        if JSONPathParser.get_value_by_path(doc, path)
                                        is not None
                                    ]
                                    grouped[field] = (
                                        sum(values) / len(values) if values else 0
                                    )
                                elif op == "$max":
                                    path = path.lstrip("$")
                                    values = [
                                        JSONPathParser.get_value_by_path(doc, path)
                                        for doc in results
                                        if JSONPathParser.get_value_by_path(doc, path)
                                        is not None
                                    ]
                                    grouped[field] = max(values) if values else None
                                elif op == "$min":
                                    path = path.lstrip("$")
                                    values = [
                                        JSONPathParser.get_value_by_path(doc, path)
                                        for doc in results
                                        if JSONPathParser.get_value_by_path(doc, path)
                                        is not None
                                    ]
                                    grouped[field] = min(values) if values else None
                    results = [grouped]
                else:
                    # Group by specific field
                    groups = {}
                    group_id_path = (
                        group_id.lstrip("$") if isinstance(group_id, str) else group_id
                    )

                    for doc in results:
                        key = JSONPathParser.get_value_by_path(doc, group_id_path)
                        key = str(key) if key is not None else "null"

                        if key not in groups:
                            groups[key] = {"_id": key, "items": []}
                        groups[key]["items"].append(doc)

                    # Apply aggregation functions
                    grouped_results = []
                    for group_key, group_data in groups.items():
                        group_result = {"_id": group_key}

                        for field, aggregation in group_spec.items():
                            if field == "_id":
                                continue
                            if isinstance(aggregation, dict):
                                for op, path in aggregation.items():
                                    if op == "$sum":
                                        if path == 1:
                                            group_result[field] = len(
                                                group_data["items"]
                                            )
                                        else:
                                            path = path.lstrip("$")
                                            total = sum(
                                                JSONPathParser.get_value_by_path(
                                                    doc, path
                                                )
                                                or 0
                                                for doc in group_data["items"]
                                            )
                                            group_result[field] = total
                                    elif op == "$avg":
                                        path = path.lstrip("$")
                                        values = [
                                            JSONPathParser.get_value_by_path(doc, path)
                                            for doc in group_data["items"]
                                            if JSONPathParser.get_value_by_path(
                                                doc, path
                                            )
                                            is not None
                                        ]
                                        group_result[field] = (
                                            sum(values) / len(values) if values else 0
                                        )

                        grouped_results.append(group_result)

                    results = grouped_results

            elif "$unwind" in stage:
                # Unwind stage - expand array field into separate documents
                unwind_path = stage["$unwind"]
                if isinstance(unwind_path, str):
                    unwind_path = unwind_path.lstrip("$")
                elif isinstance(unwind_path, dict):
                    unwind_path = unwind_path.get("path", "").lstrip("$")

                unwound_results = []
                for doc in results:
                    array_value = JSONPathParser.get_value_by_path(doc, unwind_path)

                    if isinstance(array_value, list):
                        for item in array_value:
                            new_doc = doc.copy()
                            # Set the unwound field to the individual item
                            path_parts = unwind_path.split(".")
                            current = new_doc
                            for part in path_parts[:-1]:
                                current = current.get(part, {})
                            if isinstance(current, dict):
                                current[path_parts[-1]] = item
                            unwound_results.append(new_doc)
                    else:
                        # If not an array, keep original document
                        unwound_results.append(doc)

                results = unwound_results

        return results

    def _string_to_numeric_key(self, string_key: str) -> float:
        """Convert string key to numeric key for BPTree compatibility"""
        import hashlib

        if isinstance(string_key, str):
            # Hash string keys to numeric
            hash_obj = hashlib.md5(string_key.encode())
            numeric_key = float(int(hash_obj.hexdigest()[:8], 16))
        else:
            numeric_key = float(string_key)
        return numeric_key

    def _numeric_to_string_key(
        self, numeric_key: float, original_string: str = None
    ) -> str:
        """Convert numeric key back to string key (for lookup operations)"""
        # For document stores, we maintain a mapping in the document itself
        # This is a simple approach - in production we might use a reverse lookup table
        return original_string if original_string else str(int(numeric_key))

    def _update_path_indexes(
        self,
        collection_key: str,
        doc_id: str,
        document: Dict[str, Any],
        remove_old: Dict[str, Any] = None,
    ):
        """Update path indexes for a document"""
        # Extract all JSON paths from the document
        paths = JSONPathParser.extract_paths(document)

        # Update indexes for each path
        for path, value in paths:
            # Skip the _id field since it's handled by the primary index
            if path == "_id":
                continue

            index_key = f"{collection_key}.{path}"

            # Create index if it doesn't exist
            if index_key not in self.path_indexes:
                self.path_indexes[index_key] = BPTree(order=100)

            path_index = self.path_indexes[index_key]

            # Convert value to numeric key for indexing
            if isinstance(value, (int, float)):
                numeric_value = float(value)
            else:
                numeric_value = self._string_to_numeric_key(str(value))

            # Insert into path index
            try:
                numeric_doc_id = self._string_to_numeric_key(doc_id)
                path_index.insert(numeric_value, numeric_doc_id)
            except Exception:
                # Skip problematic paths
                pass

    def _remove_from_path_indexes(
        self, collection_key: str, doc_id: str, document: Dict[str, Any]
    ):
        """Remove document from path indexes"""
        # Extract all JSON paths from the document
        paths = JSONPathParser.extract_paths(document)

        # Remove from indexes for each path
        for path, value in paths:
            index_key = f"{collection_key}.{path}"

            if index_key in self.path_indexes:
                path_index = self.path_indexes[index_key]

                # Convert value to numeric key for indexing
                if isinstance(value, (int, float)):
                    numeric_value = float(value)
                else:
                    numeric_value = self._string_to_numeric_key(str(value))

                # Remove from path index
                try:
                    path_index.delete(numeric_value)
                except Exception:
                    # Skip if not found
                    pass

    def _matches_filter(
        self, document: Dict[str, Any], filter_conditions: Dict[str, Any]
    ) -> bool:
        """Check if a document matches the filter conditions"""
        if not filter_conditions:
            return True

        for field, condition in filter_conditions.items():
            doc_value = JSONPathParser.get_value_by_path(document, field)

            if isinstance(condition, dict):
                # MongoDB-style operators
                for operator, expected_value in condition.items():
                    if operator == "$eq":
                        if doc_value != expected_value:
                            return False
                    elif operator == "$ne":
                        if doc_value == expected_value:
                            return False
                    elif operator == "$gt":
                        if not (doc_value and doc_value > expected_value):
                            return False
                    elif operator == "$gte":
                        if not (doc_value and doc_value >= expected_value):
                            return False
                    elif operator == "$lt":
                        if not (doc_value and doc_value < expected_value):
                            return False
                    elif operator == "$lte":
                        if not (doc_value and doc_value <= expected_value):
                            return False
                    elif operator == "$in":
                        if doc_value not in expected_value:
                            return False
                    elif operator == "$nin":
                        if doc_value in expected_value:
                            return False
                    elif operator == "$exists":
                        if expected_value and doc_value is None:
                            return False
                        if not expected_value and doc_value is not None:
                            return False
                    elif operator == "$regex":
                        if not doc_value or not re.search(
                            expected_value, str(doc_value)
                        ):
                            return False
            else:
                # Simple equality
                if doc_value != condition:
                    return False

        return True
