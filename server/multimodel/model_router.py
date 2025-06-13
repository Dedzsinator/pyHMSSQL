"""
ModelRouter - Unified multimodel query dispatcher for pyHMSSQL.

This module acts as the central dispatcher for all model types:
- Relational (existing SQL)
- Object-Relational (typed composites, inheritance)
- Document Store (NoSQL/JSON documents)
- Graph (vertices/edges with traversal)

It routes queries to appropriate model handlers and handles hybrid operations.
"""

import logging
from typing import Dict, Any, Union, List, Optional
from enum import Enum

# Import model adapters (lazy loading to avoid circular imports)
from .unified.type_system import TypeRegistry, type_registry


class ModelType(Enum):
    """Supported data model types."""

    RELATIONAL = "relational"
    OBJECT_RELATIONAL = "object_relational"
    DOCUMENT = "document"
    GRAPH = "graph"


class ModelRouter:
    """
    Central router that dispatches queries to appropriate model handlers.

    Features:
    - Automatic model detection based on query type
    - Hybrid query support (cross-model operations)
    - Unified result format
    - Schema mapping between models
    - Performance optimization routing
    """

    def __init__(
        self, catalog_manager, index_manager, execution_engine, transaction_manager=None
    ):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager
        self.execution_engine = execution_engine
        self.transaction_manager = transaction_manager

        # Model adapters will be initialized lazily
        self._or_adapter = None
        self._doc_adapter = None
        self._graph_adapter = None

        # Model routing registry
        self.model_registry = {}

        # Query type mappings
        self.query_type_mappings = {
            # Object-Relational patterns
            "CREATE_TYPE": ModelType.OBJECT_RELATIONAL,
            "CREATE_TABLE_INHERITANCE": ModelType.OBJECT_RELATIONAL,
            "SELECT_COMPOSITE": ModelType.OBJECT_RELATIONAL,
            # Document patterns
            "INSERT_DOCUMENT": ModelType.DOCUMENT,
            "DOCUMENT_INSERT": ModelType.DOCUMENT,
            "FIND_DOCUMENT": ModelType.DOCUMENT,
            "DOCUMENT_FIND": ModelType.DOCUMENT,
            "UPDATE_DOCUMENT": ModelType.DOCUMENT,
            "DOCUMENT_UPDATE": ModelType.DOCUMENT,
            "DELETE_DOCUMENT": ModelType.DOCUMENT,
            "DOCUMENT_DELETE": ModelType.DOCUMENT,
            "CREATE_COLLECTION": ModelType.DOCUMENT,
            "DROP_COLLECTION": ModelType.DOCUMENT,
            "DOCUMENT_AGGREGATE": ModelType.DOCUMENT,
            # Graph patterns
            "CREATE_GRAPH_SCHEMA": ModelType.GRAPH,
            "CREATE_VERTEX": ModelType.GRAPH,
            "CREATE_EDGE": ModelType.GRAPH,
            "MATCH_GRAPH": ModelType.GRAPH,
            "TRAVERSE_GRAPH": ModelType.GRAPH,
            "SHORTEST_PATH": ModelType.GRAPH,
            # Hybrid patterns
            "JOIN_DOCUMENT_RELATIONAL": ModelType.DOCUMENT,
            "GRAPH_JOIN_RELATIONAL": ModelType.GRAPH,
        }

        logging.info("ModelRouter initialized with multimodel support")

    @property
    def or_adapter(self):
        """Lazy initialization of Object-Relational adapter."""
        if self._or_adapter is None:
            try:
                from multimodel.object_relational.or_adapter import (
                    ObjectRelationalAdapter,
                )

                self._or_adapter = ObjectRelationalAdapter(
                    self.catalog_manager, self.index_manager
                )
            except ImportError as e:
                logging.error(f"Failed to import ObjectRelationalAdapter: {str(e)}")
                self._or_adapter = None
        return self._or_adapter

    @property
    def doc_adapter(self):
        """Lazy initialization of Document Store adapter."""
        if self._doc_adapter is None:
            try:
                logging.info("Attempting to import DocumentStoreAdapter...")
                from multimodel.document_store.doc_adapter import DocumentStoreAdapter

                logging.info("DocumentStoreAdapter imported successfully")
                self._doc_adapter = DocumentStoreAdapter(
                    self.catalog_manager, self.transaction_manager
                )
                logging.info("DocumentStoreAdapter initialized successfully")
            except ImportError as e:
                logging.error(f"Failed to import DocumentStoreAdapter: {str(e)}")
                self._doc_adapter = None
            except Exception as e:
                logging.error(f"Failed to initialize DocumentStoreAdapter: {str(e)}")
                self._doc_adapter = None
        return self._doc_adapter

    @property
    def graph_adapter(self):
        """Lazy initialization of Graph Store adapter."""
        if self._graph_adapter is None:
            try:
                from multimodel.graph_store.graph_adapter import GraphStoreAdapter

                self._graph_adapter = GraphStoreAdapter(
                    self.catalog_manager, self.transaction_manager
                )
            except ImportError as e:
                logging.error(f"Failed to import GraphStoreAdapter: {str(e)}")
                self._graph_adapter = None
        return self._graph_adapter

    def route_query(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """
        Route a query to the appropriate model handler.

        Args:
            plan: Query execution plan

        Returns:
            Query result with unified format
        """
        try:
            # Detect model type from plan
            model_type = self._detect_model_type(plan)

            logging.debug(
                f"Routing query type '{plan.get('type')}' to {model_type.value} model"
            )

            # Route to appropriate adapter
            if model_type == ModelType.RELATIONAL:
                return self._route_relational(plan)
            elif model_type == ModelType.OBJECT_RELATIONAL:
                adapter = self.or_adapter
                if adapter:
                    return adapter.execute_query(plan)
                else:
                    return {
                        "error": "Object-Relational adapter not available",
                        "status": "error",
                    }
            elif model_type == ModelType.DOCUMENT:
                adapter = self.doc_adapter
                if adapter:
                    return adapter.execute_query(plan)
                else:
                    return {
                        "error": "Document Store adapter not available",
                        "status": "error",
                    }
            elif model_type == ModelType.GRAPH:
                adapter = self.graph_adapter
                if adapter:
                    return adapter.execute_query(plan)
                else:
                    return {
                        "error": "Graph Store adapter not available",
                        "status": "error",
                    }
            else:
                return {
                    "error": f"Unsupported model type: {model_type}",
                    "status": "error",
                }

        except Exception as e:
            logging.error(f"Error in ModelRouter.route_query: {str(e)}")
            return {"error": f"Model routing error: {str(e)}", "status": "error"}

    def _detect_model_type(self, plan: Dict[str, Any]) -> ModelType:
        """
        Detect the appropriate model type for a query plan.

        Args:
            plan: Query execution plan

        Returns:
            Detected model type
        """
        query_type = plan.get("type", "").upper()

        # Check direct mappings first
        if query_type in self.query_type_mappings:
            return self.query_type_mappings[query_type]

        # Check for composite type usage (Object-Relational)
        if self._has_composite_types(plan):
            return ModelType.OBJECT_RELATIONAL

        # Check for document/JSON patterns
        if self._has_document_patterns(plan):
            return ModelType.DOCUMENT

        # Check for graph patterns
        if self._has_graph_patterns(plan):
            return ModelType.GRAPH

        # Default to relational
        return ModelType.RELATIONAL

    def _has_composite_types(self, plan: Dict[str, Any]) -> bool:
        """Check if plan contains composite type patterns."""
        # Look for dot notation in columns (e.g., address.street)
        columns = plan.get("columns", [])
        if isinstance(columns, list):
            for col in columns:
                if isinstance(col, str) and "." in col and not col.startswith("_"):
                    return True

        # Check for inheritance patterns
        table_name = plan.get("table", "")
        if table_name:
            # Check if table has inheritance metadata
            if hasattr(self.catalog_manager, "get_table_metadata"):
                table_meta = self.catalog_manager.get_table_metadata(table_name)
                if table_meta and table_meta.get("inheritance_parent"):
                    return True

        return False

    def _has_document_patterns(self, plan: Dict[str, Any]) -> bool:
        """Check if plan contains document store patterns."""
        # Look for JSON operators or document-style queries
        condition = plan.get("condition", "")
        if isinstance(condition, str):
            # MongoDB-style operators
            doc_operators = ["$gt", "$lt", "$eq", "$ne", "$in", "$exists", "$regex"]
            if any(op in condition for op in doc_operators):
                return True

        # Check for document collections
        table_name = plan.get("table", "")
        if table_name:
            if hasattr(self.catalog_manager, "get_table_metadata"):
                table_meta = self.catalog_manager.get_table_metadata(table_name)
                if table_meta and table_meta.get("model_type") == "document":
                    return True

        return False

    def _has_graph_patterns(self, plan: Dict[str, Any]) -> bool:
        """Check if plan contains graph patterns."""
        # Look for graph-specific keywords
        query_str = str(plan).upper()
        graph_keywords = ["MATCH", "WHERE", "->", "<-", "VERTEX", "EDGE", "PATH"]

        if any(keyword in query_str for keyword in graph_keywords):
            return True

        # Check for graph tables (vertices/edges)
        table_name = plan.get("table", "")
        if table_name:
            if table_name.endswith("_vertices") or table_name.endswith("_edges"):
                return True

            if hasattr(self.catalog_manager, "get_table_metadata"):
                table_meta = self.catalog_manager.get_table_metadata(table_name)
                if table_meta and table_meta.get("model_type") == "graph":
                    return True

        return False

    def _route_relational(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Route to existing relational execution engine bypassing multimodel routing."""
        # Directly route to appropriate executor to avoid circular dependency
        plan_type = plan.get("type", "").upper()

        try:
            if plan_type == "SELECT":
                return self.execution_engine.select_executor.execute_select(plan)
            elif plan_type == "INSERT":
                return self.execution_engine.dml_executor.execute_insert(plan)
            elif plan_type == "UPDATE":
                return self.execution_engine.dml_executor.execute_update(plan)
            elif plan_type == "DELETE":
                return self.execution_engine.dml_executor.execute_delete(plan)
            elif plan_type == "CREATE_TABLE":
                return self.execution_engine.schema_manager.execute_create_table(plan)
            elif plan_type == "DROP_TABLE":
                return self.execution_engine.schema_manager.execute_drop_table(plan)
            elif plan_type == "CREATE_INDEX":
                return self.execution_engine.execute_create_index(plan)
            elif plan_type == "DROP_INDEX":
                return self.execution_engine.execute_drop_index(plan)
            elif plan_type == "JOIN":
                return self.execution_engine.join_executor.execute_join(plan)
            elif plan_type == "AGGREGATE":
                return self.execution_engine.aggregate_executor.execute_aggregate(plan)
            elif plan_type == "GROUP_BY":
                return self.execution_engine.group_by_executor.execute_group_by(plan)
            else:
                # For other plan types, use a controlled execution without multimodel routing
                # Save current state to avoid infinite recursion
                original_router = getattr(
                    self.execution_engine, "_in_relational_mode", False
                )
                self.execution_engine._in_relational_mode = True
                try:
                    # Execute without multimodel checks
                    if hasattr(self.execution_engine, "_execute_relational_only"):
                        result = self.execution_engine._execute_relational_only(plan)
                    else:
                        # Fallback: return an error for unsupported operations
                        result = {
                            "error": f"Unsupported relational operation: {plan_type}",
                            "status": "error",
                        }
                finally:
                    self.execution_engine._in_relational_mode = original_router
                return result

        except Exception as e:
            logging.error(f"Error in relational routing: {str(e)}")
            return {"error": f"Relational execution error: {str(e)}", "status": "error"}

    def register_collection(
        self, name: str, model_type: ModelType, schema: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Register a new collection/table with a specific model type.

        Args:
            name: Collection/table name
            model_type: Model type for this collection
            schema: Optional schema definition

        Returns:
            Registration result
        """
        try:
            import datetime

            self.model_registry[name] = {
                "model_type": model_type,
                "schema": schema,
                "created_at": datetime.datetime.now().isoformat(),
            }

            # Route to appropriate adapter for collection creation
            if model_type == ModelType.DOCUMENT:
                return self.doc_adapter.create_collection(name, schema)
            elif model_type == ModelType.GRAPH:
                return self.graph_adapter.create_graph_schema(name, schema)
            elif model_type == ModelType.OBJECT_RELATIONAL:
                return self.or_adapter.create_type(name, schema)
            else:
                # Use standard relational table creation
                table_plan = {
                    "type": "CREATE_TABLE",
                    "table": name,
                    "columns": schema.get("columns", []) if schema else [],
                }
                return self.execution_engine.execute(table_plan)

        except Exception as e:
            logging.error(f"Error registering collection {name}: {str(e)}")
            return {
                "error": f"Collection registration failed: {str(e)}",
                "status": "error",
            }

    def get_collection_info(self, name: str) -> Optional[Dict[str, Any]]:
        """Get information about a registered collection."""
        return self.model_registry.get(name)

    def list_collections_by_type(self, model_type: ModelType) -> List[str]:
        """List all collections of a specific model type."""
        return [
            name
            for name, info in self.model_registry.items()
            if info["model_type"] == model_type
        ]
