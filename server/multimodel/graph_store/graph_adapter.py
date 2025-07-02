"""
Graph Store Adapter for pyHMSSQL Multimodel Extension

This adapter provides Graph Database capabilities including:
- Vertex and Edge management
- Graph traversal algorithms (BFS, DFS, shortest path)
- GraphQL-like query support
- Integration with existing B+ tree infrastructure
- Relationship indexing and path queries
"""

import json
import uuid
import logging
from typing import Dict, List, Any, Optional, Union, Tuple, Set
from dataclasses import dataclass, field
from collections import deque
import heapq
from datetime import datetime
import logging

from ..unified.record_layout import UnifiedRecordLayout, RecordType
from catalog_manager import CatalogManager

# Force using regular BPlusTree for graph storage due to optimized version limitations with bytes
from bptree import BPlusTreeOptimized as BPTree
from transaction.transaction_manager import TransactionManager


@dataclass
class Vertex:
    """Represents a graph vertex"""

    id: str
    label: str
    properties: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "label": self.label,
            "properties": self.properties,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Vertex":
        return cls(
            id=data["id"],
            label=data["label"],
            properties=data.get("properties", {}),
            created_at=datetime.fromisoformat(
                data.get("created_at", datetime.now().isoformat())
            ),
            updated_at=datetime.fromisoformat(
                data.get("updated_at", datetime.now().isoformat())
            ),
        )


@dataclass
class Edge:
    """Represents a graph edge"""

    id: str
    from_vertex: str
    to_vertex: str
    label: str
    properties: Dict[str, Any] = field(default_factory=dict)
    weight: float = 1.0
    directed: bool = True
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "from_vertex": self.from_vertex,
            "to_vertex": self.to_vertex,
            "label": self.label,
            "properties": self.properties,
            "weight": self.weight,
            "directed": self.directed,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Edge":
        return cls(
            id=data["id"],
            from_vertex=data["from_vertex"],
            to_vertex=data["to_vertex"],
            label=data["label"],
            properties=data.get("properties", {}),
            weight=data.get("weight", 1.0),
            directed=data.get("directed", True),
            created_at=datetime.fromisoformat(
                data.get("created_at", datetime.now().isoformat())
            ),
            updated_at=datetime.fromisoformat(
                data.get("updated_at", datetime.now().isoformat())
            ),
        )


@dataclass
class GraphQuery:
    """Represents a graph query"""

    query_type: str  # 'match', 'traverse', 'shortest_path', etc.
    start_vertex: Optional[str] = None
    end_vertex: Optional[str] = None
    vertex_filter: Dict[str, Any] = field(default_factory=dict)
    edge_filter: Dict[str, Any] = field(default_factory=dict)
    max_depth: Optional[int] = None
    limit: Optional[int] = None
    return_paths: bool = False


class GraphStoreAdapter:
    """Adapter for Graph Database operations"""

    def __init__(
        self, catalog_manager: CatalogManager, transaction_manager: TransactionManager
    ):
        self.catalog_manager = catalog_manager
        self.transaction_manager = transaction_manager
        self.record_layout = UnifiedRecordLayout()

        # Graph storage indexes
        self.vertex_index = BPTree(order=100)  # vertex_id -> vertex_data
        self.edge_index = BPTree(order=100)  # edge_id -> edge_data
        self.outgoing_edges = BPTree(order=100)  # from_vertex -> [edge_ids]
        self.incoming_edges = BPTree(order=100)  # to_vertex -> [edge_ids]
        self.vertex_labels = BPTree(order=100)  # label -> [vertex_ids]
        self.edge_labels = BPTree(order=100)  # label -> [edge_ids]

        # Property indexes (can be created dynamically)
        self.vertex_property_indexes = {}  # property_name -> BPTree
        self.edge_property_indexes = {}  # property_name -> BPTree

        # Load existing graph data
        self._load_graph_data()

    def _load_graph_data(self):
        """Load existing graph data from storage"""
        try:
            # Try to load graph metadata from catalog
            # This would be implemented when catalog extensions are ready
            pass
        except:
            # Graph storage not initialized yet
            pass

    def create_vertex(
        self, label: str, properties: Dict[str, Any] = None, vertex_id: str = None
    ) -> str:
        """Create a new vertex"""
        if vertex_id is None:
            vertex_id = str(uuid.uuid4())

        if properties is None:
            properties = {}

        # Check if vertex already exists
        if self._vertex_exists(vertex_id):
            raise ValueError(f"Vertex with ID '{vertex_id}' already exists")

        # Create vertex object
        vertex = Vertex(id=vertex_id, label=label, properties=properties)

        # Serialize vertex using UnifiedRecordLayout
        vertex_data = self.record_layout.encode_record(
            record_type=RecordType.GRAPH_VERTEX, data=vertex.to_dict()
        )

        # Store in vertex index with numeric key
        numeric_key = self._string_to_numeric_key(vertex_id)
        self.vertex_index.insert(numeric_key, vertex_data)

        # Update label index
        self._update_label_index(self.vertex_labels, label, vertex_id, add=True)

        # Update property indexes
        self._update_vertex_property_indexes(vertex, add=True)

        return vertex_id

    def create_edge(
        self,
        from_vertex: str,
        to_vertex: str,
        label: str,
        properties: Dict[str, Any] = None,
        weight: float = 1.0,
        directed: bool = True,
        edge_id: str = None,
    ) -> str:
        """Create a new edge"""
        if edge_id is None:
            edge_id = str(uuid.uuid4())

        if properties is None:
            properties = {}

        # Validate that vertices exist
        if not self._vertex_exists(from_vertex):
            raise ValueError(f"From vertex '{from_vertex}' does not exist")
        if not self._vertex_exists(to_vertex):
            raise ValueError(f"To vertex '{to_vertex}' does not exist")

        # Check if edge already exists
        if self._edge_exists(edge_id):
            raise ValueError(f"Edge with ID '{edge_id}' already exists")

        # Create edge object
        edge = Edge(
            id=edge_id,
            from_vertex=from_vertex,
            to_vertex=to_vertex,
            label=label,
            properties=properties,
            weight=weight,
            directed=directed,
        )

        # Serialize edge using UnifiedRecordLayout
        edge_data = self.record_layout.encode_record(
            record_type=RecordType.GRAPH_EDGE, data=edge.to_dict()
        )

        # Store in edge index with numeric key
        numeric_key = self._string_to_numeric_key(edge_id)
        self.edge_index.insert(numeric_key, edge_data)

        # Update outgoing/incoming edge indexes
        self._update_adjacency_index(
            self.outgoing_edges, from_vertex, edge_id, add=True
        )
        self._update_adjacency_index(self.incoming_edges, to_vertex, edge_id, add=True)

        # For undirected edges, add reverse adjacency
        if not directed:
            self._update_adjacency_index(
                self.outgoing_edges, to_vertex, edge_id, add=True
            )
            self._update_adjacency_index(
                self.incoming_edges, from_vertex, edge_id, add=True
            )

        # Update label index
        self._update_label_index(self.edge_labels, label, edge_id, add=True)

        # Update property indexes
        self._update_edge_property_indexes(edge, add=True)

        return edge_id

    def get_vertex(self, vertex_id: str) -> Optional[Vertex]:
        """Get a vertex by ID"""
        numeric_key = self._string_to_numeric_key(vertex_id)
        vertex_data = self.vertex_index.search(numeric_key)
        if vertex_data:
            decoded_data = self.record_layout.decode_record(vertex_data)
            return Vertex.from_dict(decoded_data)
        return None

    def get_edge(self, edge_id: str) -> Optional[Edge]:
        """Get an edge by ID"""
        numeric_key = self._string_to_numeric_key(edge_id)
        edge_data = self.edge_index.search(numeric_key)
        if edge_data:
            decoded_data = self.record_layout.decode_record(edge_data)
            return Edge.from_dict(decoded_data)
        return None

    def find_vertices(
        self, label: str = None, properties: Dict[str, Any] = None
    ) -> List[Vertex]:
        """Find vertices by label and/or properties"""
        vertex_ids = set()

        if label:
            # Get vertices by label using numeric key
            numeric_label_key = self._string_to_numeric_key(label)
            label_data = self.vertex_labels.search(numeric_label_key)
            if label_data:
                vertex_ids.update(json.loads(label_data))
        else:
            # Get all vertices
            vertex_ids = self._get_all_vertex_ids()

        # Filter by properties
        if properties:
            filtered_ids = set()
            for vertex_id in vertex_ids:
                vertex = self.get_vertex(vertex_id)
                if vertex and self._matches_properties(vertex.properties, properties):
                    filtered_ids.add(vertex_id)
            vertex_ids = filtered_ids

        # Convert IDs to vertex objects
        vertices = []
        for vertex_id in vertex_ids:
            vertex = self.get_vertex(vertex_id)
            if vertex:
                vertices.append(vertex)

        return vertices

    def find_edges(
        self,
        label: str = None,
        properties: Dict[str, Any] = None,
        from_vertex: str = None,
        to_vertex: str = None,
    ) -> List[Edge]:
        """Find edges by label, properties, and/or endpoints"""
        edge_ids = set()

        if label:
            # Get edges by label using numeric key
            numeric_label_key = self._string_to_numeric_key(label)
            label_data = self.edge_labels.search(numeric_label_key)
            if label_data:
                edge_ids.update(json.loads(label_data))
        else:
            # Get all edges
            edge_ids = self._get_all_edge_ids()

        # Filter by endpoints
        if from_vertex:
            numeric_from_key = self._string_to_numeric_key(from_vertex)
            outgoing_data = self.outgoing_edges.search(numeric_from_key)
            if outgoing_data:
                from_edges = set(json.loads(outgoing_data))
                edge_ids = edge_ids.intersection(from_edges)
            else:
                edge_ids = set()

        if to_vertex:
            numeric_to_key = self._string_to_numeric_key(to_vertex)
            incoming_data = self.incoming_edges.search(numeric_to_key)
            if incoming_data:
                to_edges = set(json.loads(incoming_data))
                edge_ids = edge_ids.intersection(to_edges)
            else:
                edge_ids = set()

        # Filter by properties
        if properties:
            filtered_ids = set()
            for edge_id in edge_ids:
                edge = self.get_edge(edge_id)
                if edge and self._matches_properties(edge.properties, properties):
                    filtered_ids.add(edge_id)
            edge_ids = filtered_ids

        # Convert IDs to edge objects
        edges = []
        for edge_id in edge_ids:
            edge = self.get_edge(edge_id)
            if edge:
                edges.append(edge)

        return edges

    def traverse_bfs(
        self,
        start_vertex: str,
        max_depth: int = None,
        vertex_filter: Dict[str, Any] = None,
        edge_filter: Dict[str, Any] = None,
    ) -> List[Tuple[Vertex, int]]:
        """Breadth-First Search traversal"""
        if not self._vertex_exists(start_vertex):
            raise ValueError(f"Start vertex '{start_vertex}' does not exist")

        visited = set()
        queue = deque([(start_vertex, 0)])  # (vertex_id, depth)
        result = []

        while queue:
            vertex_id, depth = queue.popleft()

            if vertex_id in visited:
                continue

            if max_depth is not None and depth > max_depth:
                continue

            vertex = self.get_vertex(vertex_id)
            if not vertex:
                continue

            # Apply vertex filter
            if vertex_filter and not self._matches_properties(
                vertex.properties, vertex_filter
            ):
                continue

            visited.add(vertex_id)
            result.append((vertex, depth))

            # Get outgoing edges
            numeric_vertex_key = self._string_to_numeric_key(vertex_id)
            outgoing_data = self.outgoing_edges.search(numeric_vertex_key)
            if outgoing_data:
                edge_ids = json.loads(outgoing_data)
                for edge_id in edge_ids:
                    edge = self.get_edge(edge_id)
                    if edge:
                        # Apply edge filter
                        if edge_filter and not self._matches_properties(
                            edge.properties, edge_filter
                        ):
                            continue

                        next_vertex = (
                            edge.to_vertex
                            if edge.from_vertex == vertex_id
                            else edge.from_vertex
                        )
                        if next_vertex not in visited:
                            queue.append((next_vertex, depth + 1))

        return result

    def traverse_dfs(
        self,
        start_vertex: str,
        max_depth: int = None,
        vertex_filter: Dict[str, Any] = None,
        edge_filter: Dict[str, Any] = None,
    ) -> List[Tuple[Vertex, int]]:
        """Depth-First Search traversal"""
        if not self._vertex_exists(start_vertex):
            raise ValueError(f"Start vertex '{start_vertex}' does not exist")

        visited = set()
        result = []

        def dfs_recursive(vertex_id: str, depth: int):
            if vertex_id in visited:
                return

            if max_depth is not None and depth > max_depth:
                return

            vertex = self.get_vertex(vertex_id)
            if not vertex:
                return

            # Apply vertex filter
            if vertex_filter and not self._matches_properties(
                vertex.properties, vertex_filter
            ):
                return

            visited.add(vertex_id)
            result.append((vertex, depth))

            # Get outgoing edges
            numeric_vertex_key = self._string_to_numeric_key(vertex_id)
            outgoing_data = self.outgoing_edges.search(numeric_vertex_key)
            if outgoing_data:
                edge_ids = json.loads(outgoing_data)
                for edge_id in edge_ids:
                    edge = self.get_edge(edge_id)
                    if edge:
                        # Apply edge filter
                        if edge_filter and not self._matches_properties(
                            edge.properties, edge_filter
                        ):
                            continue

                        next_vertex = (
                            edge.to_vertex
                            if edge.from_vertex == vertex_id
                            else edge.from_vertex
                        )
                        if next_vertex not in visited:
                            dfs_recursive(next_vertex, depth + 1)

        dfs_recursive(start_vertex, 0)
        return result

    def shortest_path(
        self, start_vertex: str, end_vertex: str, weight_property: str = None
    ) -> Optional[Tuple[List[Vertex], float]]:
        """Find shortest path between two vertices using Dijkstra's algorithm"""
        if not self._vertex_exists(start_vertex) or not self._vertex_exists(end_vertex):
            return None

        # Priority queue: (distance, vertex_id, path)
        pq = [(0.0, start_vertex, [start_vertex])]
        visited = set()
        distances = {start_vertex: 0.0}

        while pq:
            current_dist, current_vertex, path = heapq.heappop(pq)

            if current_vertex in visited:
                continue

            visited.add(current_vertex)

            if current_vertex == end_vertex:
                # Reconstruct path with vertex objects
                vertex_path = []
                for vertex_id in path:
                    vertex = self.get_vertex(vertex_id)
                    if vertex:
                        vertex_path.append(vertex)
                return vertex_path, current_dist

            # Get outgoing edges
            numeric_current_key = self._string_to_numeric_key(current_vertex)
            outgoing_data = self.outgoing_edges.search(numeric_current_key)
            if outgoing_data:
                edge_ids = json.loads(outgoing_data)
                for edge_id in edge_ids:
                    edge = self.get_edge(edge_id)
                    if edge:
                        next_vertex = (
                            edge.to_vertex
                            if edge.from_vertex == current_vertex
                            else edge.from_vertex
                        )

                        # Calculate edge weight
                        if weight_property and weight_property in edge.properties:
                            edge_weight = float(edge.properties[weight_property])
                        else:
                            edge_weight = edge.weight

                        new_dist = current_dist + edge_weight

                        if next_vertex not in visited and (
                            next_vertex not in distances
                            or new_dist < distances[next_vertex]
                        ):
                            distances[next_vertex] = new_dist
                            new_path = path + [next_vertex]
                            heapq.heappush(pq, (new_dist, next_vertex, new_path))

        return None  # No path found

    def execute_query(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a graph store query"""
        query_type = plan.get("type", "").upper()

        try:
            if query_type == "CREATE_GRAPH_SCHEMA":
                # Handle both parameter formats for compatibility
                schema_name = plan.get("schema_name") or plan.get("graph")
                vertex_labels = plan.get("vertex_labels") or plan.get("vertex_types")
                edge_labels = plan.get("edge_labels") or plan.get("edge_types")

                success = self.create_graph_schema(
                    schema_name, vertex_labels, edge_labels
                )
                return {
                    "status": "success" if success else "error",
                    "message": (
                        f"Graph schema '{schema_name}' created"
                        if success
                        else "Failed to create graph schema"
                    ),
                }

            elif query_type == "CREATE_VERTEX":
                # Extract vertex data from nested structure
                vertex_data = plan.get("vertex", {})
                label = vertex_data.get("label") or plan.get("label")
                properties = vertex_data.get("properties", {}) or plan.get(
                    "properties", {}
                )
                vertex_id = vertex_data.get("id") or plan.get("id")
                graph_name = plan.get("graph")

                # Validate graph exists if specified
                if graph_name:
                    graph_schema = self.catalog_manager.get_graph_schema(graph_name)
                    if not graph_schema:
                        return {
                            "status": "error",
                            "error": f"Graph '{graph_name}' does not exist",
                        }

                created_id = self.create_vertex(label, properties, vertex_id)
                return {
                    "status": "success",
                    "vertex_id": created_id,
                    "message": f"Vertex created with ID: {created_id}",
                }

            elif query_type == "CREATE_EDGE":
                # Extract edge data from nested structure
                edge_data = plan.get("edge", {})
                from_vertex = edge_data.get("from_vertex") or plan.get("from_vertex")
                to_vertex = edge_data.get("to_vertex") or plan.get("to_vertex")
                label = edge_data.get("label") or plan.get("label")
                properties = edge_data.get("properties", {}) or plan.get(
                    "properties", {}
                )
                weight = edge_data.get("weight", 1.0) or plan.get("weight", 1.0)
                directed = edge_data.get("directed", True) or plan.get("directed", True)
                edge_id = edge_data.get("id") or plan.get("id")
                graph_name = plan.get("graph")

                # Validate graph exists if specified
                if graph_name:
                    graph_schema = self.catalog_manager.get_graph_schema(graph_name)
                    if not graph_schema:
                        return {
                            "status": "error",
                            "error": f"Graph '{graph_name}' does not exist",
                        }

                created_id = self.create_edge(
                    from_vertex, to_vertex, label, properties, weight, directed, edge_id
                )
                return {
                    "status": "success",
                    "edge_id": created_id,
                    "message": f"Edge created with ID: {created_id}",
                }

            elif query_type == "MATCH_GRAPH":
                query_obj = GraphQuery(
                    query_type="match",
                    vertex_filter=plan.get("vertex_filter", {}),
                    edge_filter=plan.get("edge_filter", {}),
                    limit=plan.get("limit"),
                )

                return self.execute_graph_query(query_obj)

            elif query_type == "TRAVERSE_GRAPH":
                method = plan.get("method", "bfs").lower()
                query_obj = GraphQuery(
                    query_type=f"traverse_{method}",
                    start_vertex=plan.get("start_vertex"),
                    vertex_filter=plan.get("vertex_filter", {}),
                    edge_filter=plan.get("edge_filter", {}),
                    max_depth=plan.get("max_depth"),
                    limit=plan.get("limit"),
                )

                return self.execute_graph_query(query_obj)

            elif query_type == "SHORTEST_PATH":
                query_obj = GraphQuery(
                    query_type="shortest_path",
                    start_vertex=plan.get("start_vertex"),
                    end_vertex=plan.get("end_vertex"),
                )

                return self.execute_graph_query(query_obj)

            elif query_type == "GET_VERTEX":
                vertex_id = plan.get("id")
                vertex = self.get_vertex(vertex_id)

                return {
                    "status": "success",
                    "vertex": vertex.to_dict() if vertex else None,
                    "found": vertex is not None,
                }

            elif query_type == "GET_EDGE":
                edge_id = plan.get("id")
                edge = self.get_edge(edge_id)

                return {
                    "status": "success",
                    "edge": edge.to_dict() if edge else None,
                    "found": edge is not None,
                }

            elif query_type == "FIND_VERTICES":
                label = plan.get("label")
                properties = plan.get("properties", {})

                vertices = self.find_vertices(label, properties)
                return {
                    "status": "success",
                    "vertices": [v.to_dict() for v in vertices],
                    "count": len(vertices),
                }

            elif query_type == "FIND_EDGES":
                label = plan.get("label")
                properties = plan.get("properties", {})
                from_vertex = plan.get("from_vertex")
                to_vertex = plan.get("to_vertex")

                edges = self.find_edges(label, properties, from_vertex, to_vertex)
                return {
                    "status": "success",
                    "edges": [e.to_dict() for e in edges],
                    "count": len(edges),
                }

            elif query_type == "GRAPH_TRAVERSE":
                start_vertex = plan.get("start_vertex")
                method = plan.get("method", "bfs").lower()
                max_depth = plan.get("max_depth", 10)
                direction = plan.get("direction", "outgoing")
                edge_filter = plan.get("edge_filter", {})
                vertex_filter = plan.get("vertex_filter", {})

                # Use BFS traversal for graph traversal
                try:
                    result_vertices = self.traverse_bfs(
                        start_vertex, max_depth, vertex_filter, edge_filter
                    )
                    return {
                        "status": "success",
                        "vertices": [
                            {"vertex": v.to_dict(), "depth": d}
                            for v, d in result_vertices
                        ],
                        "count": len(result_vertices),
                    }
                except Exception as e:
                    return {
                        "error": f"Graph traversal failed: {str(e)}",
                        "status": "error",
                    }

            elif query_type == "GRAPH_PATTERN":
                # Basic pattern matching - find vertices and edges matching criteria
                pattern = plan.get("pattern", "")
                where_clause = plan.get("where", {})
                return_clause = plan.get("return", [])

                try:
                    # For pattern matching, we need to handle complex where clauses
                    # For now, implement basic filtering that doesn't fail on missing properties
                    vertices = []

                    # If no specific where clause, return empty results (since we can't get all vertices)
                    if not where_clause:
                        vertices = []
                    else:
                        # Try to find vertices, but handle missing properties gracefully
                        try:
                            vertices = (
                                self.find_vertices()
                            )  # Get all available vertices
                            # Apply manual filtering for complex conditions
                            filtered_vertices = []
                            for vertex in vertices:
                                match = True
                                for prop_path, condition in where_clause.items():
                                    if "." in prop_path:
                                        # Handle dot notation like 'p.age'
                                        parts = prop_path.split(".")
                                        if len(parts) == 2:
                                            prop_name = parts[1]
                                            vertex_props = vertex.properties

                                            # Check if property exists
                                            if prop_name not in vertex_props:
                                                match = False
                                                break

                                            prop_value = vertex_props[prop_name]

                                            # Handle comparison operators
                                            if isinstance(condition, dict):
                                                for op, value in condition.items():
                                                    if op == "$gte" and not (
                                                        prop_value >= value
                                                    ):
                                                        match = False
                                                        break
                                                    elif op == "$lte" and not (
                                                        prop_value <= value
                                                    ):
                                                        match = False
                                                        break
                                                    elif op == "$eq" and not (
                                                        prop_value == value
                                                    ):
                                                        match = False
                                                        break
                                            else:
                                                if prop_value != condition:
                                                    match = False
                                                    break

                                if match:
                                    filtered_vertices.append(vertex)

                            vertices = filtered_vertices
                        except Exception:
                            # If vertex retrieval fails, return empty results
                            vertices = []

                    # Filter results based on return clause if specified
                    result_data = []
                    for vertex in vertices:
                        vertex_dict = vertex.to_dict()
                        if return_clause:
                            # Extract only requested fields
                            filtered_result = {}
                            for field in return_clause:
                                if "." in field:
                                    # Handle dot notation like 'p.name'
                                    parts = field.split(".")
                                    if (
                                        len(parts) == 2 and parts[0] == "p"
                                    ):  # vertex property
                                        prop_name = parts[1]
                                        if prop_name in vertex_dict.get(
                                            "properties", {}
                                        ):
                                            filtered_result[field] = vertex_dict[
                                                "properties"
                                            ][prop_name]
                                        elif prop_name in vertex_dict:
                                            filtered_result[field] = vertex_dict[
                                                prop_name
                                            ]
                            result_data.append(filtered_result)
                        else:
                            result_data.append(vertex_dict)

                    return {
                        "status": "success",
                        "results": result_data,
                        "count": len(result_data),
                    }
                except Exception as e:
                    return {
                        "error": f"Graph pattern matching failed: {str(e)}",
                        "status": "error",
                    }

            elif query_type == "GRAPH_PATH":
                # Find path between two vertices
                from_vertex = plan.get("from_vertex")
                to_vertex = plan.get("to_vertex")
                max_hops = plan.get("max_hops", 10)

                try:
                    if not from_vertex or not to_vertex:
                        return {
                            "error": "Both from_vertex and to_vertex are required for path finding",
                            "status": "error",
                        }

                    # Use BFS to find shortest path
                    path = self.shortest_path(from_vertex, to_vertex, max_hops)

                    return {
                        "status": "success",
                        "path": path,
                        "length": len(path) - 1 if path else 0,
                        "found": bool(path),
                    }
                except Exception as e:
                    return {
                        "error": f"Graph path finding failed: {str(e)}",
                        "status": "error",
                    }

            else:
                return {
                    "error": f"Unknown graph query type: {query_type}",
                    "status": "error",
                }

        except Exception as e:
            logging.error(f"Error in GraphStoreAdapter.execute_query: {str(e)}")
            return {
                "error": f"Graph query execution failed: {str(e)}",
                "status": "error",
            }

    def execute_graph_query(self, query: GraphQuery) -> Dict[str, Any]:
        """Execute a graph query"""
        try:
            if query.query_type == "match":
                return self._execute_match_query(query)
            elif query.query_type == "traverse_bfs":
                return self._execute_traverse_query(query, "bfs")
            elif query.query_type == "traverse_dfs":
                return self._execute_traverse_query(query, "dfs")
            elif query.query_type == "shortest_path":
                return self._execute_shortest_path_query(query)
            else:
                return {
                    "error": f"Unknown graph query type: {query.query_type}",
                    "status": "error",
                }
        except Exception as e:
            return {
                "error": f"Graph query execution failed: {str(e)}",
                "status": "error",
            }

    def _execute_match_query(self, query: GraphQuery) -> Dict[str, Any]:
        """Execute a MATCH query"""
        vertices = self.find_vertices(
            properties=query.vertex_filter if query.vertex_filter else None
        )

        edges = self.find_edges(
            properties=query.edge_filter if query.edge_filter else None
        )

        if query.limit:
            vertices = vertices[: query.limit]
            edges = edges[: query.limit]

        return {
            "vertices": [v.to_dict() for v in vertices],
            "edges": [e.to_dict() for e in edges],
            "status": "success",
            "count": {"vertices": len(vertices), "edges": len(edges)},
        }

    def _execute_traverse_query(self, query: GraphQuery, method: str) -> Dict[str, Any]:
        """Execute a traversal query"""
        if not query.start_vertex:
            return {
                "error": "Start vertex is required for traversal",
                "status": "error",
            }

        if method == "bfs":
            results = self.traverse_bfs(
                query.start_vertex,
                query.max_depth,
                query.vertex_filter,
                query.edge_filter,
            )
        else:  # dfs
            results = self.traverse_dfs(
                query.start_vertex,
                query.max_depth,
                query.vertex_filter,
                query.edge_filter,
            )

        if query.limit:
            results = results[: query.limit]

        return {
            "vertices": [{"vertex": v.to_dict(), "depth": d} for v, d in results],
            "status": "success",
            "count": len(results),
        }

    def _execute_shortest_path_query(self, query: GraphQuery) -> Dict[str, Any]:
        """Execute a shortest path query"""
        if not query.start_vertex or not query.end_vertex:
            return {
                "error": "Both start and end vertices are required for shortest path",
                "status": "error",
            }

        result = self.shortest_path(query.start_vertex, query.end_vertex)

        if result:
            path, distance = result
            return {
                "path": [v.to_dict() for v in path],
                "distance": distance,
                "status": "success",
                "length": len(path),
            }
        else:
            return {
                "path": [],
                "distance": float("inf"),
                "status": "success",
                "message": "No path found",
            }

    def create_graph_schema(
        self,
        schema_name: str,
        vertex_labels: List[str] = None,
        edge_labels: List[str] = None,
    ) -> bool:
        """Create a graph schema with predefined vertex and edge labels"""
        try:
            # Create the graph schema in the catalog manager
            catalog_result = self.catalog_manager.create_graph_schema(
                schema_name, vertex_labels, edge_labels
            )

            if (
                isinstance(catalog_result, dict)
                and catalog_result.get("status") == "error"
            ):
                logging.error(
                    f"Failed to create graph schema in catalog: {catalog_result.get('error')}"
                )
                return False

            # Initialize vertex label indexes for predefined labels
            if vertex_labels:
                for label in vertex_labels:
                    label_key = self._string_to_numeric_key(str(label))
                    if not self.vertex_labels.search(label_key):
                        self.vertex_labels.insert(label_key, json.dumps([]))

            # Initialize edge label indexes for predefined labels
            if edge_labels:
                for label in edge_labels:
                    label_key = self._string_to_numeric_key(str(label))
                    if not self.edge_labels.search(label_key):
                        self.edge_labels.insert(label_key, json.dumps([]))

            logging.info(
                f"Graph schema '{schema_name}' created with vertex labels: {vertex_labels}, edge labels: {edge_labels}"
            )
            return True

        except Exception as e:
            logging.error(f"Error creating graph schema: {str(e)}")
            return False

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

    # Helper methods
    def _vertex_exists(self, vertex_id: str) -> bool:
        """Check if a vertex exists"""
        if vertex_id is None:
            return False
        numeric_key = self._string_to_numeric_key(vertex_id)
        return self.vertex_index.search(numeric_key) is not None

    def _edge_exists(self, edge_id: str) -> bool:
        """Check if an edge exists"""
        if edge_id is None:
            return False
        numeric_key = self._string_to_numeric_key(edge_id)
        return self.edge_index.search(numeric_key) is not None

    def _update_label_index(
        self, index: BPTree, label: str, item_id: str, add: bool = True
    ):
        """Update label index"""
        # Convert label to numeric key for consistency
        numeric_label_key = self._string_to_numeric_key(label)
        existing_data = index.search(numeric_label_key)
        if existing_data:
            item_ids = set(json.loads(existing_data))
        else:
            item_ids = set()

        if add:
            item_ids.add(item_id)
        else:
            item_ids.discard(item_id)

        if item_ids:
            index.insert(numeric_label_key, json.dumps(list(item_ids)))
        else:
            try:
                index.delete(numeric_label_key)
            except:
                pass

    def _update_adjacency_index(
        self, index: BPTree, vertex_id: str, edge_id: str, add: bool = True
    ):
        """Update adjacency index"""
        # Use numeric key for consistency with vertex_index
        numeric_vertex_key = self._string_to_numeric_key(vertex_id)
        existing_data = index.search(numeric_vertex_key)
        if existing_data:
            edge_ids = set(json.loads(existing_data))
        else:
            edge_ids = set()

        if add:
            edge_ids.add(edge_id)
        else:
            edge_ids.discard(edge_id)

        if edge_ids:
            index.insert(numeric_vertex_key, json.dumps(list(edge_ids)))
        else:
            try:
                index.delete(numeric_vertex_key)
            except:
                pass

    def _update_vertex_property_indexes(self, vertex: Vertex, add: bool = True):
        """Update vertex property indexes"""
        for prop_name, prop_value in vertex.properties.items():
            if prop_name not in self.vertex_property_indexes:
                self.vertex_property_indexes[prop_name] = BPTree(order=100)

            index = self.vertex_property_indexes[prop_name]
            # Convert property value to numeric key for consistency
            numeric_prop_key = self._string_to_numeric_key(str(prop_value))

            existing_data = index.search(numeric_prop_key)
            if existing_data:
                vertex_ids = set(json.loads(existing_data))
            else:
                vertex_ids = set()

            if add:
                vertex_ids.add(vertex.id)
            else:
                vertex_ids.discard(vertex.id)

            if vertex_ids:
                index.insert(numeric_prop_key, json.dumps(list(vertex_ids)))
            else:
                try:
                    index.delete(numeric_prop_key)
                except:
                    pass

    def _update_edge_property_indexes(self, edge: Edge, add: bool = True):
        """Update edge property indexes"""
        for prop_name, prop_value in edge.properties.items():
            if prop_name not in self.edge_property_indexes:
                self.edge_property_indexes[prop_name] = BPTree(order=100)

            index = self.edge_property_indexes[prop_name]
            # Convert property value to numeric key for consistency
            numeric_prop_key = self._string_to_numeric_key(str(prop_value))

            existing_data = index.search(numeric_prop_key)
            if existing_data:
                edge_ids = set(json.loads(existing_data))
            else:
                edge_ids = set()

            if add:
                edge_ids.add(edge.id)
            else:
                edge_ids.discard(edge.id)

            if edge_ids:
                index.insert(numeric_prop_key, json.dumps(list(edge_ids)))
            else:
                try:
                    index.delete(numeric_prop_key)
                except:
                    pass

    def _matches_properties(
        self, obj_properties: Dict[str, Any], filter_properties: Dict[str, Any]
    ) -> bool:
        """Check if object properties match filter"""
        for key, value in filter_properties.items():
            if key not in obj_properties:
                return False
            if obj_properties[key] != value:
                return False
        return True

    def _get_all_vertex_ids(self) -> Set[str]:
        """Get all vertex IDs by iterating through the vertex index"""
        vertex_ids = set()

        # Since we can't reverse the hash, we'll need to collect vertices differently
        # For now, return empty set for GRAPH_PATTERN queries without specific filters
        # This is a limitation of the current hashing approach

        return vertex_ids

    def _get_all_edge_ids(self) -> Set[str]:
        """Get all edge IDs by iterating through the edge index"""
        edge_ids = set()

        # Since we can't reverse the hash, we'll need to collect edges differently
        # For now, return empty set for queries without specific filters
        # This is a limitation of the current hashing approach

        return edge_ids
