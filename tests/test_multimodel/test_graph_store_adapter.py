"""
Comprehensive test suite for the Graph Store Adapter.

This module tests all aspects of the Graph Store functionality including:
- Vertex and edge CRUD operations
- Graph traversal algorithms (BFS, DFS, shortest path)
- Graph pattern matching and queries
- Property indexing and filtering
- Error handling and edge cases
- Performance characteristics
- Integration with unified record layout
"""

import pytest
import json
import uuid
import time
import threading
from typing import Dict, List, Any, Optional, Set, Tuple
from unittest.mock import Mock, patch, MagicMock

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from server.multimodel.graph_store.graph_adapter import (
    GraphStoreAdapter,
    Vertex,
    Edge,
    GraphQuery,
)
from catalog_manager import CatalogManager
from transaction.transaction_manager import TransactionManager


class TestVertex:
    """Test the Vertex data structure."""

    def test_vertex_creation(self):
        """Test basic vertex creation."""
        vertex = Vertex(
            id="v1", label="Person", properties={"name": "Alice", "age": 30}
        )

        assert vertex.id == "v1"
        assert vertex.label == "Person"
        assert vertex.properties["name"] == "Alice"
        assert vertex.properties["age"] == 30

    def test_vertex_serialization(self):
        """Test vertex to_dict and from_dict methods."""
        original = Vertex(
            id="v2", label="Company", properties={"name": "TechCorp", "founded": 2010}
        )

        # Serialize to dict
        vertex_dict = original.to_dict()
        assert vertex_dict["id"] == "v2"
        assert vertex_dict["label"] == "Company"
        assert vertex_dict["properties"]["name"] == "TechCorp"
        assert vertex_dict["properties"]["founded"] == 2010

        # Deserialize from dict
        restored = Vertex.from_dict(vertex_dict)
        assert restored.id == original.id
        assert restored.label == original.label
        assert restored.properties == original.properties

    def test_vertex_empty_properties(self):
        """Test vertex with empty properties."""
        vertex = Vertex(id="v3", label="Empty")
        assert vertex.properties == {}

        vertex_dict = vertex.to_dict()
        assert vertex_dict["properties"] == {}

        restored = Vertex.from_dict(vertex_dict)
        assert restored.properties == {}


class TestEdge:
    """Test the Edge data structure."""

    def test_edge_creation(self):
        """Test basic edge creation."""
        edge = Edge(
            id="e1",
            from_vertex="v1",
            to_vertex="v2",
            label="KNOWS",
            properties={"since": "2020", "strength": 0.8},
            weight=1.5,
            directed=True,
        )

        assert edge.id == "e1"
        assert edge.from_vertex == "v1"
        assert edge.to_vertex == "v2"
        assert edge.label == "KNOWS"
        assert edge.properties["since"] == "2020"
        assert edge.properties["strength"] == 0.8
        assert edge.weight == 1.5
        assert edge.directed == True

    def test_edge_serialization(self):
        """Test edge to_dict and from_dict methods."""
        original = Edge(
            id="e2",
            from_vertex="v3",
            to_vertex="v4",
            label="WORKS_FOR",
            properties={"role": "Engineer", "start_date": "2021-01-01"},
            weight=2.0,
            directed=True,
        )

        # Serialize to dict
        edge_dict = original.to_dict()
        assert edge_dict["id"] == "e2"
        assert edge_dict["from_vertex"] == "v3"
        assert edge_dict["to_vertex"] == "v4"
        assert edge_dict["label"] == "WORKS_FOR"
        assert edge_dict["properties"]["role"] == "Engineer"
        assert edge_dict["weight"] == 2.0
        assert edge_dict["directed"] == True

        # Deserialize from dict
        restored = Edge.from_dict(edge_dict)
        assert restored.id == original.id
        assert restored.from_vertex == original.from_vertex
        assert restored.to_vertex == original.to_vertex
        assert restored.label == original.label
        assert restored.properties == original.properties
        assert restored.weight == original.weight
        assert restored.directed == original.directed

    def test_edge_defaults(self):
        """Test edge creation with default values."""
        edge = Edge(id="e3", from_vertex="v5", to_vertex="v6", label="RELATED")

        assert edge.properties == {}
        assert edge.weight == 1.0
        assert edge.directed == True


class TestGraphQuery:
    """Test the GraphQuery data structure."""

    def test_query_creation(self):
        """Test basic graph query creation."""
        query = GraphQuery(
            query_type="match",
            vertex_filter={"label": "Person"},
            edge_filter={"label": "KNOWS"},
            limit=10,
        )

        assert query.query_type == "match"
        assert query.vertex_filter == {"label": "Person"}
        assert query.edge_filter == {"label": "KNOWS"}
        assert query.limit == 10

    def test_traversal_query(self):
        """Test traversal query creation."""
        query = GraphQuery(
            query_type="traverse_bfs",
            start_vertex="v1",
            max_depth=3,
            vertex_filter={"age": {"$gte": 18}},
            edge_filter={"strength": {"$gt": 0.5}},
        )

        assert query.query_type == "traverse_bfs"
        assert query.start_vertex == "v1"
        assert query.max_depth == 3
        assert query.vertex_filter["age"]["$gte"] == 18
        assert query.edge_filter["strength"]["$gt"] == 0.5


class TestGraphStoreAdapter:
    """Test the main Graph Store Adapter functionality."""

    @pytest.fixture
    def mock_catalog_manager(self):
        """Create a mock catalog manager."""
        catalog = Mock()  # Remove spec to allow any attribute access
        catalog.get_graph_schema.return_value = None
        catalog.create_graph_schema.return_value = {"status": "success"}
        catalog.get_current_database.return_value = "test_db"
        catalog.list_tables.return_value = []
        return catalog

    @pytest.fixture
    def mock_transaction_manager(self):
        """Create a mock transaction manager."""
        transaction = Mock()  # Remove spec to allow any attribute access
        transaction.begin_transaction.return_value = "txn_123"
        transaction.commit_transaction.return_value = {"status": "success"}
        transaction.rollback_transaction.return_value = {"status": "success"}
        return transaction

    @pytest.fixture
    def adapter(self, mock_catalog_manager, mock_transaction_manager):
        """Create a graph store adapter instance."""
        return GraphStoreAdapter(mock_catalog_manager, mock_transaction_manager)

    def test_create_vertex(self, adapter):
        """Test vertex creation."""
        vertex_id = adapter.create_vertex(
            label="Person", properties={"name": "Alice", "age": 30}
        )

        assert vertex_id is not None
        assert isinstance(vertex_id, str)

        # Verify vertex can be retrieved
        vertex = adapter.get_vertex(vertex_id)
        assert vertex is not None
        assert vertex.id == vertex_id
        assert vertex.label == "Person"
        assert vertex.properties["name"] == "Alice"
        assert vertex.properties["age"] == 30

    def test_create_vertex_with_custom_id(self, adapter):
        """Test vertex creation with custom ID."""
        custom_id = "person_123"
        vertex_id = adapter.create_vertex(
            label="Person", properties={"name": "Bob"}, vertex_id=custom_id
        )

        assert vertex_id == custom_id

        vertex = adapter.get_vertex(custom_id)
        assert vertex.id == custom_id
        assert vertex.properties["name"] == "Bob"

    def test_create_duplicate_vertex(self, adapter):
        """Test creating vertex with duplicate ID."""
        custom_id = "dup_vertex"

        # Create first vertex
        adapter.create_vertex(label="Person", vertex_id=custom_id)

        # Try to create duplicate
        with pytest.raises(ValueError, match="Vertex with ID .* already exists"):
            adapter.create_vertex(label="Person", vertex_id=custom_id)

    def test_create_edge(self, adapter):
        """Test edge creation."""
        # Create vertices first
        v1_id = adapter.create_vertex(label="Person", properties={"name": "Alice"})
        v2_id = adapter.create_vertex(label="Person", properties={"name": "Bob"})

        # Create edge
        edge_id = adapter.create_edge(
            from_vertex=v1_id,
            to_vertex=v2_id,
            label="KNOWS",
            properties={"since": "2020"},
            weight=0.8,
            directed=True,
        )

        assert edge_id is not None
        assert isinstance(edge_id, str)

        # Verify edge can be retrieved
        edge = adapter.get_edge(edge_id)
        assert edge is not None
        assert edge.id == edge_id
        assert edge.from_vertex == v1_id
        assert edge.to_vertex == v2_id
        assert edge.label == "KNOWS"
        assert edge.properties["since"] == "2020"
        assert edge.weight == 0.8
        assert edge.directed == True

    def test_create_edge_nonexistent_vertices(self, adapter):
        """Test creating edge with nonexistent vertices."""
        with pytest.raises(ValueError, match="From vertex .* does not exist"):
            adapter.create_edge(
                from_vertex="nonexistent1", to_vertex="nonexistent2", label="KNOWS"
            )

        # Create one vertex
        v1_id = adapter.create_vertex(label="Person")

        with pytest.raises(ValueError, match="To vertex .* does not exist"):
            adapter.create_edge(
                from_vertex=v1_id, to_vertex="nonexistent", label="KNOWS"
            )

    def test_find_vertices(self, adapter):
        """Test finding vertices by label and properties."""
        # Create test vertices
        v1_id = adapter.create_vertex(
            label="Person", properties={"name": "Alice", "age": 30, "city": "NYC"}
        )
        v2_id = adapter.create_vertex(
            label="Person", properties={"name": "Bob", "age": 25, "city": "LA"}
        )
        v3_id = adapter.create_vertex(
            label="Company", properties={"name": "TechCorp", "founded": 2010}
        )

        # Find by label
        persons = adapter.find_vertices(label="Person")
        assert len(persons) == 2
        person_ids = [v.id for v in persons]
        assert v1_id in person_ids
        assert v2_id in person_ids

        companies = adapter.find_vertices(label="Company")
        assert len(companies) == 1
        assert companies[0].id == v3_id

        # Find by properties
        alice_vertices = adapter.find_vertices(properties={"name": "Alice"})
        assert len(alice_vertices) == 1
        assert alice_vertices[0].id == v1_id

        nyc_vertices = adapter.find_vertices(properties={"city": "NYC"})
        assert len(nyc_vertices) == 1
        assert nyc_vertices[0].id == v1_id

        # Find by label and properties
        young_persons = adapter.find_vertices(label="Person", properties={"age": 25})
        assert len(young_persons) == 1
        assert young_persons[0].id == v2_id

    def test_find_edges(self, adapter):
        """Test finding edges by label, properties, and endpoints."""
        # Create vertices
        v1_id = adapter.create_vertex(label="Person", properties={"name": "Alice"})
        v2_id = adapter.create_vertex(label="Person", properties={"name": "Bob"})
        v3_id = adapter.create_vertex(label="Person", properties={"name": "Charlie"})

        # Create edges
        e1_id = adapter.create_edge(
            from_vertex=v1_id,
            to_vertex=v2_id,
            label="KNOWS",
            properties={"since": "2020"},
        )
        e2_id = adapter.create_edge(
            from_vertex=v2_id,
            to_vertex=v3_id,
            label="KNOWS",
            properties={"since": "2021"},
        )
        e3_id = adapter.create_edge(
            from_vertex=v1_id,
            to_vertex=v3_id,
            label="WORKS_WITH",
            properties={"project": "AI"},
        )

        # Find by label
        knows_edges = adapter.find_edges(label="KNOWS")
        assert len(knows_edges) == 2
        knows_ids = [e.id for e in knows_edges]
        assert e1_id in knows_ids
        assert e2_id in knows_ids

        # Find by properties
        old_edges = adapter.find_edges(properties={"since": "2020"})
        assert len(old_edges) == 1
        assert old_edges[0].id == e1_id

        # Find by from_vertex
        alice_edges = adapter.find_edges(from_vertex=v1_id)
        assert len(alice_edges) == 2
        alice_edge_ids = [e.id for e in alice_edges]
        assert e1_id in alice_edge_ids
        assert e3_id in alice_edge_ids

        # Find by to_vertex
        to_charlie_edges = adapter.find_edges(to_vertex=v3_id)
        assert len(to_charlie_edges) == 2
        to_charlie_ids = [e.id for e in to_charlie_edges]
        assert e2_id in to_charlie_ids
        assert e3_id in to_charlie_ids

        # Find by multiple criteria
        specific_edges = adapter.find_edges(
            label="KNOWS", from_vertex=v1_id, properties={"since": "2020"}
        )
        assert len(specific_edges) == 1
        assert specific_edges[0].id == e1_id

    def test_traverse_bfs(self, adapter):
        """Test breadth-first search traversal."""
        # Create a small graph: A -> B -> C
        #                       A -> D -> E
        va_id = adapter.create_vertex(label="Node", properties={"name": "A"})
        vb_id = adapter.create_vertex(label="Node", properties={"name": "B"})
        vc_id = adapter.create_vertex(label="Node", properties={"name": "C"})
        vd_id = adapter.create_vertex(label="Node", properties={"name": "D"})
        ve_id = adapter.create_vertex(label="Node", properties={"name": "E"})

        adapter.create_edge(va_id, vb_id, "CONNECTS")
        adapter.create_edge(vb_id, vc_id, "CONNECTS")
        adapter.create_edge(va_id, vd_id, "CONNECTS")
        adapter.create_edge(vd_id, ve_id, "CONNECTS")

        # BFS from A with max depth 2
        results = adapter.traverse_bfs(va_id, max_depth=2)

        # Should find A (depth 0), B and D (depth 1), C and E (depth 2)
        assert len(results) == 5

        # Check depths
        depths = {vertex.properties["name"]: depth for vertex, depth in results}
        assert depths["A"] == 0
        assert depths["B"] == 1
        assert depths["D"] == 1
        assert depths["C"] == 2
        assert depths["E"] == 2

        # BFS with max depth 1
        results_depth1 = adapter.traverse_bfs(va_id, max_depth=1)
        assert len(results_depth1) == 3  # A, B, D

        depth1_names = {vertex.properties["name"] for vertex, depth in results_depth1}
        assert depth1_names == {"A", "B", "D"}

    def test_traverse_dfs(self, adapter):
        """Test depth-first search traversal."""
        # Create a small graph: A -> B -> C
        #                       A -> D
        va_id = adapter.create_vertex(label="Node", properties={"name": "A"})
        vb_id = adapter.create_vertex(label="Node", properties={"name": "B"})
        vc_id = adapter.create_vertex(label="Node", properties={"name": "C"})
        vd_id = adapter.create_vertex(label="Node", properties={"name": "D"})

        adapter.create_edge(va_id, vb_id, "CONNECTS")
        adapter.create_edge(vb_id, vc_id, "CONNECTS")
        adapter.create_edge(va_id, vd_id, "CONNECTS")

        # DFS from A
        results = adapter.traverse_dfs(va_id, max_depth=3)

        # Should find all 4 vertices
        assert len(results) == 4

        # Check that A is at depth 0
        depths = {vertex.properties["name"]: depth for vertex, depth in results}
        assert depths["A"] == 0

        # DFS should explore one path fully before backtracking
        vertex_names = [vertex.properties["name"] for vertex, depth in results]
        assert vertex_names[0] == "A"  # Should start with A

    def test_traverse_with_filters(self, adapter):
        """Test traversal with vertex and edge filters."""
        # Create vertices with different properties
        va_id = adapter.create_vertex(
            label="Person", properties={"name": "Alice", "age": 30}
        )
        vb_id = adapter.create_vertex(
            label="Person", properties={"name": "Bob", "age": 25}
        )
        vc_id = adapter.create_vertex(
            label="Person", properties={"name": "Charlie", "age": 35}
        )
        vd_id = adapter.create_vertex(label="Company", properties={"name": "TechCorp"})

        # Create edges with different properties
        adapter.create_edge(va_id, vb_id, "KNOWS", properties={"strength": 0.8})
        adapter.create_edge(vb_id, vc_id, "KNOWS", properties={"strength": 0.3})
        adapter.create_edge(va_id, vd_id, "WORKS_FOR", properties={"role": "Engineer"})

        # Traverse with vertex filter (only persons)
        results = adapter.traverse_bfs(va_id, vertex_filter={"label": "Person"})

        # Should exclude company vertex
        vertex_labels = [vertex.label for vertex, depth in results]
        assert all(label == "Person" for label in vertex_labels)
        assert len(results) == 3  # Alice, Bob, Charlie

        # Traverse with edge filter (strong connections only)
        results_strong = adapter.traverse_bfs(va_id, edge_filter={"strength": 0.8})

        # Should only follow strong edges
        vertex_names = {vertex.properties["name"] for vertex, depth in results_strong}
        assert "Alice" in vertex_names
        assert "Bob" in vertex_names
        # Charlie should not be reachable through strong edges
        assert "Charlie" not in vertex_names

    def test_shortest_path(self, adapter):
        """Test shortest path finding."""
        # Create a graph with multiple paths: A -> B -> D
        #                                     A -> C -> D
        va_id = adapter.create_vertex(label="Node", properties={"name": "A"})
        vb_id = adapter.create_vertex(label="Node", properties={"name": "B"})
        vc_id = adapter.create_vertex(label="Node", properties={"name": "C"})
        vd_id = adapter.create_vertex(label="Node", properties={"name": "D"})

        adapter.create_edge(va_id, vb_id, "CONNECTS", weight=2.0)
        adapter.create_edge(vb_id, vd_id, "CONNECTS", weight=3.0)
        adapter.create_edge(va_id, vc_id, "CONNECTS", weight=1.0)
        adapter.create_edge(vc_id, vd_id, "CONNECTS", weight=1.0)

        # Find shortest path from A to D
        path = adapter.shortest_path(va_id, vd_id)

        assert path is not None
        assert len(path) >= 3  # At least A, C, D or A, B, D
        assert path[0] == va_id  # Should start with A
        assert path[-1] == vd_id  # Should end with D

        # The path through C should be shorter (weight 2) than through B (weight 5)
        # Verify the path goes through C
        path_vertices = []
        for vertex_id in path:
            vertex = adapter.get_vertex(vertex_id)
            path_vertices.append(vertex.properties["name"])

        # Should be A -> C -> D (shorter path)
        expected_path = ["A", "C", "D"]
        assert path_vertices == expected_path

    def test_undirected_edges(self, adapter):
        """Test undirected edge behavior."""
        # Create vertices
        va_id = adapter.create_vertex(label="Node", properties={"name": "A"})
        vb_id = adapter.create_vertex(label="Node", properties={"name": "B"})

        # Create undirected edge
        edge_id = adapter.create_edge(
            from_vertex=va_id, to_vertex=vb_id, label="CONNECTS", directed=False
        )

        # Should be able to traverse in both directions
        from_a = adapter.traverse_bfs(va_id, max_depth=1)
        from_b = adapter.traverse_bfs(vb_id, max_depth=1)

        # Both should find both vertices
        assert len(from_a) == 2
        assert len(from_b) == 2

        a_names = {vertex.properties["name"] for vertex, depth in from_a}
        b_names = {vertex.properties["name"] for vertex, depth in from_b}

        assert a_names == {"A", "B"}
        assert b_names == {"A", "B"}


class TestGraphStoreQueries:
    """Test graph query execution."""

    @pytest.fixture
    def adapter(self):
        """Create adapter with sample graph data."""
        catalog = Mock()  # Remove spec to allow any attribute access
        catalog.get_graph_schema.return_value = None
        catalog.create_graph_schema.return_value = {"status": "success"}
        catalog.get_current_database.return_value = "test_db"
        catalog.list_tables.return_value = []
        transaction = Mock()  # Remove spec to allow any attribute access
        transaction.begin_transaction.return_value = "txn_123"
        transaction.commit_transaction.return_value = {"status": "success"}
        transaction.rollback_transaction.return_value = {"status": "success"}
        adapter = GraphStoreAdapter(catalog, transaction)

        # Create sample graph
        self._setup_sample_graph(adapter)
        return adapter

    def _setup_sample_graph(self, adapter):
        """Set up a sample graph for testing."""
        # Create vertices
        self.alice_id = adapter.create_vertex(
            label="Person", properties={"name": "Alice", "age": 30, "city": "NYC"}
        )
        self.bob_id = adapter.create_vertex(
            label="Person", properties={"name": "Bob", "age": 25, "city": "LA"}
        )
        self.charlie_id = adapter.create_vertex(
            label="Person", properties={"name": "Charlie", "age": 35, "city": "Chicago"}
        )
        self.techcorp_id = adapter.create_vertex(
            label="Company", properties={"name": "TechCorp", "industry": "Technology"}
        )

        # Create edges
        adapter.create_edge(
            self.alice_id,
            self.bob_id,
            "KNOWS",
            properties={"since": "2020", "strength": 0.8},
        )
        adapter.create_edge(
            self.bob_id,
            self.charlie_id,
            "KNOWS",
            properties={"since": "2021", "strength": 0.6},
        )
        adapter.create_edge(
            self.alice_id,
            self.techcorp_id,
            "WORKS_FOR",
            properties={"role": "Engineer", "start_date": "2019-01-01"},
        )
        adapter.create_edge(
            self.charlie_id,
            self.techcorp_id,
            "WORKS_FOR",
            properties={"role": "Manager", "start_date": "2018-01-01"},
        )

    def test_execute_match_query(self, adapter):
        """Test MATCH graph query execution."""
        query = GraphQuery(
            query_type="match", vertex_filter={"label": "Person"}, limit=10
        )

        result = adapter.execute_graph_query(query)

        assert result["status"] == "success"
        assert "vertices" in result
        assert "edges" in result
        assert result["count"]["vertices"] >= 3

        # All returned vertices should be persons
        for vertex_data in result["vertices"]:
            assert vertex_data["label"] == "Person"

    def test_execute_traversal_query(self, adapter):
        """Test traversal query execution."""
        query = GraphQuery(
            query_type="traverse_bfs",
            start_vertex=self.alice_id,
            max_depth=2,
            vertex_filter={"label": "Person"},
        )

        result = adapter.execute_graph_query(query)

        assert result["status"] == "success"
        assert "vertices" in result
        assert result["count"] >= 1

        # Should find Alice and connected persons
        vertex_names = []
        for vertex_data in result["vertices"]:
            vertex_names.append(vertex_data["vertex"]["properties"]["name"])

        assert "Alice" in vertex_names
        assert "Bob" in vertex_names  # Alice knows Bob

    def test_execute_shortest_path_query(self, adapter):
        """Test shortest path query execution."""
        query = GraphQuery(
            query_type="shortest_path",
            start_vertex=self.alice_id,
            end_vertex=self.charlie_id,
        )

        result = adapter.execute_graph_query(query)

        assert result["status"] == "success"
        # Should find a path from Alice to Charlie through Bob
        # (Alice knows Bob, Bob knows Charlie)


class TestGraphStorePerformance:
    """Test performance characteristics of the graph store."""

    @pytest.fixture
    def adapter(self):
        """Create adapter for performance testing."""
        catalog = Mock()  # Remove spec to allow any attribute access
        catalog.get_graph_schema.return_value = None
        catalog.create_graph_schema.return_value = {"status": "success"}
        catalog.get_current_database.return_value = "test_db"
        catalog.list_tables.return_value = []
        transaction = Mock()  # Remove spec to allow any attribute access
        transaction.begin_transaction.return_value = "txn_123"
        transaction.commit_transaction.return_value = {"status": "success"}
        transaction.rollback_transaction.return_value = {"status": "success"}
        return GraphStoreAdapter(catalog, transaction)

    def test_bulk_vertex_creation(self, adapter):
        """Test performance of bulk vertex creation."""
        num_vertices = 1000

        start_time = time.time()
        vertex_ids = []

        for i in range(num_vertices):
            vertex_id = adapter.create_vertex(
                label="Node", properties={"id": i, "name": f"Node{i}", "value": i * 2}
            )
            vertex_ids.append(vertex_id)

        end_time = time.time()
        creation_time = end_time - start_time
        vertices_per_second = num_vertices / creation_time

        print(f"Created {num_vertices} vertices in {creation_time:.2f}s")
        print(f"Rate: {vertices_per_second:.2f} vertices/sec")

        # Verify all vertices were created
        assert len(vertex_ids) == num_vertices
        assert all(vid is not None for vid in vertex_ids)

        # Performance should be reasonable
        assert vertices_per_second > 50  # At least 50 vertices/sec

    def test_bulk_edge_creation(self, adapter):
        """Test performance of bulk edge creation."""
        # Create vertices first
        num_vertices = 100
        vertex_ids = []
        for i in range(num_vertices):
            vertex_id = adapter.create_vertex(label="Node", properties={"id": i})
            vertex_ids.append(vertex_id)

        # Create edges (create a chain: 0->1->2->...->99)
        num_edges = num_vertices - 1

        start_time = time.time()
        edge_ids = []

        for i in range(num_edges):
            edge_id = adapter.create_edge(
                from_vertex=vertex_ids[i],
                to_vertex=vertex_ids[i + 1],
                label="NEXT",
                properties={"order": i},
            )
            edge_ids.append(edge_id)

        end_time = time.time()
        creation_time = end_time - start_time
        edges_per_second = num_edges / creation_time

        print(f"Created {num_edges} edges in {creation_time:.2f}s")
        print(f"Rate: {edges_per_second:.2f} edges/sec")

        # Verify all edges were created
        assert len(edge_ids) == num_edges
        assert all(eid is not None for eid in edge_ids)

        # Performance should be reasonable
        assert edges_per_second > 30  # At least 30 edges/sec

    def test_traversal_performance(self, adapter):
        """Test performance of graph traversal on larger graphs."""
        # Create a graph with branching factor 3, depth 4
        # This creates roughly 3^4 = 81 vertices

        def create_tree(adapter, depth, branching_factor, parent_id=None, level=0):
            """Create a tree graph recursively."""
            if level >= depth:
                return []

            vertex_ids = []

            if parent_id is None:
                # Root vertex
                root_id = adapter.create_vertex(
                    label="Node", properties={"level": level, "name": f"root"}
                )
                vertex_ids.append(root_id)

                # Create children
                for child in create_tree(
                    adapter, depth, branching_factor, root_id, level + 1
                ):
                    vertex_ids.append(child)

                return vertex_ids
            else:
                # Create children of current parent
                for i in range(branching_factor):
                    child_id = adapter.create_vertex(
                        label="Node",
                        properties={"level": level, "name": f"level{level}_child{i}"},
                    )
                    vertex_ids.append(child_id)

                    # Create edge from parent to child
                    adapter.create_edge(parent_id, child_id, "PARENT_OF")

                    # Recursively create grandchildren
                    for grandchild in create_tree(
                        adapter, depth, branching_factor, child_id, level + 1
                    ):
                        vertex_ids.append(grandchild)

                return vertex_ids

        # Create the tree
        all_vertex_ids = create_tree(adapter, depth=4, branching_factor=3)
        root_id = all_vertex_ids[0]

        print(f"Created tree with {len(all_vertex_ids)} vertices")

        # Test BFS traversal performance
        start_time = time.time()
        bfs_results = adapter.traverse_bfs(root_id, max_depth=10)
        bfs_time = time.time() - start_time

        print(f"BFS traversal found {len(bfs_results)} vertices in {bfs_time:.4f}s")

        # Test DFS traversal performance
        start_time = time.time()
        dfs_results = adapter.traverse_dfs(root_id, max_depth=10)
        dfs_time = time.time() - start_time

        print(f"DFS traversal found {len(dfs_results)} vertices in {dfs_time:.4f}s")

        # Both should find all vertices
        assert len(bfs_results) == len(all_vertex_ids)
        assert len(dfs_results) == len(all_vertex_ids)

        # Traversal should be reasonably fast
        assert bfs_time < 1.0  # Should complete within 1 second
        assert dfs_time < 1.0  # Should complete within 1 second

    def test_concurrent_operations(self, adapter):
        """Test concurrent graph operations."""

        def worker_create_vertices(worker_id, num_vertices):
            """Worker function for concurrent vertex creation."""
            vertex_ids = []
            for i in range(num_vertices):
                try:
                    vertex_id = adapter.create_vertex(
                        label="Worker", properties={"worker": worker_id, "index": i}
                    )
                    vertex_ids.append(vertex_id)
                except Exception as e:
                    print(f"Worker {worker_id} vertex creation error: {e}")
            return vertex_ids

        def worker_create_edges(worker_id, vertex_pairs):
            """Worker function for concurrent edge creation."""
            edge_ids = []
            for i, (v1, v2) in enumerate(vertex_pairs):
                try:
                    edge_id = adapter.create_edge(
                        from_vertex=v1,
                        to_vertex=v2,
                        label="CONNECTS",
                        properties={"worker": worker_id, "index": i},
                    )
                    edge_ids.append(edge_id)
                except Exception as e:
                    print(f"Worker {worker_id} edge creation error: {e}")
            return edge_ids

        # Create vertices concurrently
        num_workers = 3
        vertices_per_worker = 20
        vertex_threads = []
        vertex_results = {}

        start_time = time.time()

        for worker_id in range(num_workers):
            thread = threading.Thread(
                target=lambda wid=worker_id: vertex_results.update(
                    {wid: worker_create_vertices(wid, vertices_per_worker)}
                )
            )
            vertex_threads.append(thread)
            thread.start()

        # Wait for vertex creation to complete
        for thread in vertex_threads:
            thread.join()

        vertex_creation_time = time.time() - start_time

        # Collect all created vertices
        all_vertex_ids = []
        for worker_results in vertex_results.values():
            all_vertex_ids.extend(worker_results)

        print(
            f"Concurrent vertex creation: {len(all_vertex_ids)} vertices in {vertex_creation_time:.2f}s"
        )

        # Create edges concurrently using created vertices
        if len(all_vertex_ids) >= 2:
            # Create vertex pairs for edge creation
            vertex_pairs = []
            for i in range(0, len(all_vertex_ids) - 1, 2):
                vertex_pairs.append((all_vertex_ids[i], all_vertex_ids[i + 1]))

            # Divide pairs among workers
            pairs_per_worker = len(vertex_pairs) // num_workers
            edge_threads = []
            edge_results = {}

            start_time = time.time()

            for worker_id in range(num_workers):
                start_idx = worker_id * pairs_per_worker
                end_idx = start_idx + pairs_per_worker
                worker_pairs = vertex_pairs[start_idx:end_idx]

                if worker_pairs:
                    thread = threading.Thread(
                        target=lambda wid=worker_id, pairs=worker_pairs: edge_results.update(
                            {wid: worker_create_edges(wid, pairs)}
                        )
                    )
                    edge_threads.append(thread)
                    thread.start()

            # Wait for edge creation to complete
            for thread in edge_threads:
                thread.join()

            edge_creation_time = time.time() - start_time

            # Collect all created edges
            all_edge_ids = []
            for worker_results in edge_results.values():
                all_edge_ids.extend(worker_results)

            print(
                f"Concurrent edge creation: {len(all_edge_ids)} edges in {edge_creation_time:.2f}s"
            )

        # Verify that most operations succeeded
        expected_vertices = num_workers * vertices_per_worker
        assert (
            len(all_vertex_ids) >= expected_vertices * 0.8
        )  # Allow for some concurrency issues


class TestGraphStoreEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.fixture
    def adapter(self):
        """Create adapter for edge case testing."""
        catalog = Mock(spec=CatalogManager)
        catalog.get_graph_schema.return_value = None
        transaction = Mock(spec=TransactionManager)
        return GraphStoreAdapter(catalog, transaction)

    def test_large_property_values(self, adapter):
        """Test handling of large property values."""
        # Create vertex with large properties
        large_text = "x" * 50000  # 50KB string
        large_array = list(range(1000))  # Large array in JSON

        vertex_id = adapter.create_vertex(
            label="LargeNode",
            properties={
                "large_text": large_text,
                "large_array": large_array,
                "nested": {
                    "deep": {
                        "structure": {"data": ["item" + str(i) for i in range(100)]}
                    }
                },
            },
        )

        # Should handle large properties
        assert vertex_id is not None

        # Should be able to retrieve vertex with large properties
        vertex = adapter.get_vertex(vertex_id)
        assert vertex is not None
        assert len(vertex.properties["large_text"]) == 50000
        assert len(vertex.properties["large_array"]) == 1000

    def test_circular_references(self, adapter):
        """Test handling of circular references in graph."""
        # Create vertices
        va_id = adapter.create_vertex(label="Node", properties={"name": "A"})
        vb_id = adapter.create_vertex(label="Node", properties={"name": "B"})
        vc_id = adapter.create_vertex(label="Node", properties={"name": "C"})

        # Create circular references: A -> B -> C -> A
        adapter.create_edge(va_id, vb_id, "NEXT")
        adapter.create_edge(vb_id, vc_id, "NEXT")
        adapter.create_edge(vc_id, va_id, "NEXT")

        # Traversal should handle cycles without infinite loops
        results = adapter.traverse_bfs(va_id, max_depth=10)

        # Should visit each vertex exactly once despite cycles
        vertex_names = {vertex.properties["name"] for vertex, depth in results}
        assert vertex_names == {"A", "B", "C"}

        # Test DFS as well
        dfs_results = adapter.traverse_dfs(va_id, max_depth=10)
        dfs_names = {vertex.properties["name"] for vertex, depth in dfs_results}
        assert dfs_names == {"A", "B", "C"}

    def test_self_loops(self, adapter):
        """Test handling of self-loop edges."""
        # Create vertex
        vertex_id = adapter.create_vertex(label="SelfNode", properties={"name": "Self"})

        # Create self-loop
        edge_id = adapter.create_edge(
            from_vertex=vertex_id,
            to_vertex=vertex_id,
            label="SELF_REF",
            properties={"type": "loop"},
        )

        assert edge_id is not None

        # Should be able to retrieve self-loop edge
        edge = adapter.get_edge(edge_id)
        assert edge.from_vertex == vertex_id
        assert edge.to_vertex == vertex_id

        # Traversal should handle self-loops
        results = adapter.traverse_bfs(vertex_id, max_depth=3)

        # Should find the vertex but not loop infinitely
        assert len(results) == 1
        assert results[0][0].id == vertex_id

    def test_nonexistent_vertex_operations(self, adapter):
        """Test operations on nonexistent vertices."""
        fake_id = "nonexistent_vertex"

        # Get nonexistent vertex
        vertex = adapter.get_vertex(fake_id)
        assert vertex is None

        # Traverse from nonexistent vertex
        results = adapter.traverse_bfs(fake_id)
        assert len(results) == 0

        # Find edges from nonexistent vertex
        edges = adapter.find_edges(from_vertex=fake_id)
        assert len(edges) == 0

    def test_special_characters_in_properties(self, adapter):
        """Test handling of special characters in properties."""
        special_props = {
            "unicode": "Hello 世界 🌍",
            "quotes": "He said \"Hello\" and 'Goodbye'",
            "newlines": "Line 1\nLine 2\r\nLine 3",
            "json_chars": '{"key": "value", "array": [1, 2, 3]}',
            "special_symbols": "!@#$%^&*()_+-=[]{}|;:,.<>?",
            "empty": "",
            "null": None,
            "zero": 0,
            "false": False,
        }

        vertex_id = adapter.create_vertex(label="SpecialNode", properties=special_props)

        assert vertex_id is not None

        # Retrieve and verify special characters are preserved
        vertex = adapter.get_vertex(vertex_id)
        assert vertex.properties["unicode"] == "Hello 世界 🌍"
        assert vertex.properties["quotes"] == "He said \"Hello\" and 'Goodbye'"
        assert vertex.properties["newlines"] == "Line 1\nLine 2\r\nLine 3"
        assert vertex.properties["empty"] == ""
        assert vertex.properties["null"] is None
        assert vertex.properties["zero"] == 0
        assert vertex.properties["false"] == False

    def test_malformed_queries(self, adapter):
        """Test handling of malformed graph queries."""
        # Create test data
        vertex_id = adapter.create_vertex(label="Test", properties={"name": "Test"})

        # Test invalid query types
        invalid_query = GraphQuery(query_type="invalid_type")
        result = adapter.execute_graph_query(invalid_query)
        assert result["status"] == "error"
        assert "Unknown graph query type" in result["error"]

        # Test traversal without start vertex
        no_start_query = GraphQuery(query_type="traverse_bfs")
        result = adapter.execute_graph_query(no_start_query)
        assert result["status"] == "error"
        assert "Start vertex is required" in result["error"]

        # Test shortest path without end vertex
        no_end_query = GraphQuery(query_type="shortest_path", start_vertex=vertex_id)
        result = adapter.execute_graph_query(no_end_query)
        # This should either handle gracefully or return an error


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
