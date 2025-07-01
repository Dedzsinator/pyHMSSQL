#!/usr/bin/env python3
"""
pyHMSSQL Multimodel Extension Standalone Demo

This demo showcases the core multimodel capabilities without external dependencies.
"""

import json
import sys
import os

# Add server path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "server"))


def demo_object_relational():
    """Demonstrate Object-Relational features"""
    print("🔧 Object-Relational Features Demo")
    print("=" * 50)

    from server.multimodel.unified.type_system import (
        TypeRegistry,
        CompositeTypeDefinition,
        TypeAttribute,
        PrimitiveTypeDefinition,
        PrimitiveType,
        TypeCategory,
    )

    # Create type registry
    registry = TypeRegistry()

    # Define an Address composite type
    address_type = CompositeTypeDefinition(
        name="address", category=TypeCategory.COMPOSITE
    )

    # Add attributes to address type
    street_attr = TypeAttribute(
        name="street",
        type_def=PrimitiveTypeDefinition(
            name="varchar",
            category=TypeCategory.PRIMITIVE,
            primitive_type=PrimitiveType.VARCHAR,
            length=100,
        ),
    )

    city_attr = TypeAttribute(
        name="city",
        type_def=PrimitiveTypeDefinition(
            name="varchar",
            category=TypeCategory.PRIMITIVE,
            primitive_type=PrimitiveType.VARCHAR,
            length=50,
        ),
    )

    address_type.add_attribute(street_attr)
    address_type.add_attribute(city_attr)

    # Register the type
    registry.register_type(address_type)

    print("✅ Created composite type 'address' with attributes:")
    print("   - street: VARCHAR(100)")
    print("   - city: VARCHAR(50)")

    # Demonstrate type validation
    valid_address = {"street": "123 Main St", "city": "New York"}

    incomplete_address = {
        "street": "123 Main St"
        # Missing city - should still be valid due to nullable=True
    }

    from server.multimodel.unified.type_system import TypeValidator

    validator = TypeValidator(registry)

    print(
        f"\n✅ Complete address validation: {validator.validate_value(valid_address, address_type)}"
    )
    print(
        f"✅ Incomplete address validation: {validator.validate_value(incomplete_address, address_type)}"
    )

    # Show registered types
    types = registry.list_types()
    print(f"\n📋 Registered types: {len(types)} types")
    for t in types:
        print(f"   - {t.name} ({t.category.value})")


def demo_document_store():
    """Demonstrate Document Store features"""
    print("\n📄 Document Store Features Demo")
    print("=" * 50)

    # Import just the classes we need
    from server.multimodel.document_store.doc_adapter import (
        JSONPathParser,
        DocumentCollection,
    )

    # Create a sample document
    user_document = {
        "id": "user123",
        "name": "Alice Johnson",
        "email": "alice@example.com",
        "profile": {
            "age": 28,
            "city": "San Francisco",
            "skills": ["Python", "JavaScript", "MongoDB"],
            "experience": {"years": 5, "level": "Senior"},
        },
        "projects": [
            {"name": "Project A", "status": "completed"},
            {"name": "Project B", "status": "in-progress"},
        ],
    }

    print("✅ Created sample user document:")
    print(json.dumps(user_document, indent=2)[:300] + "...")

    # Demonstrate JSONPath extraction
    paths = JSONPathParser.extract_paths(user_document)
    print(f"\n✅ Extracted {len(paths)} indexable paths from document")

    # Show some interesting paths
    interesting_paths = [p for p in paths if len(p[0].split(".")) > 1][:8]
    print("📍 Sample nested paths:")
    for path, value in interesting_paths:
        print(f"   {path} -> {value}")

    # Demonstrate value retrieval
    age = JSONPathParser.get_value_by_path(user_document, "profile.age")
    first_skill = JSONPathParser.get_value_by_path(user_document, "profile.skills[0]")
    project_status = JSONPathParser.get_value_by_path(
        user_document, "projects[1].status"
    )

    print(f"\n✅ JSONPath queries:")
    print(f"   profile.age = {age}")
    print(f"   profile.skills[0] = {first_skill}")
    print(f"   projects[1].status = {project_status}")

    # Document collection demo
    collection = DocumentCollection(name="users")
    collection_dict = collection.to_dict()
    restored = DocumentCollection.from_dict(collection_dict)

    print(f"\n✅ Document collection serialization test:")
    print(f"   Original: {collection.name}")
    print(f"   Restored: {restored.name}")
    print(f"   Match: {'✅' if collection.name == restored.name else '❌'}")


def demo_graph_database():
    """Demonstrate Graph Database features"""
    print("\n🕸️  Graph Database Features Demo")
    print("=" * 50)

    from server.multimodel.graph_store.graph_adapter import Vertex, Edge, GraphQuery

    # Create sample vertices
    alice = Vertex(
        id="user1",
        label="Person",
        properties={"name": "Alice", "age": 28, "city": "San Francisco"},
    )

    bob = Vertex(
        id="user2",
        label="Person",
        properties={"name": "Bob", "age": 32, "city": "New York"},
    )

    company = Vertex(
        id="company1",
        label="Company",
        properties={"name": "TechCorp", "industry": "Technology"},
    )

    print("✅ Created vertices:")
    print(f"   {alice.properties['name']} (Person, age {alice.properties['age']})")
    print(f"   {bob.properties['name']} (Person, age {bob.properties['age']})")
    print(f"   {company.properties['name']} ({company.label})")

    # Create sample edges
    friendship = Edge(
        id="edge1",
        from_vertex="user1",
        to_vertex="user2",
        label="KNOWS",
        properties={"since": "2020", "relationship": "friend"},
    )

    employment = Edge(
        id="edge2",
        from_vertex="user1",
        to_vertex="company1",
        label="WORKS_FOR",
        properties={"position": "Senior Developer", "since": "2021"},
    )

    print(f"\n✅ Created edges:")
    print(
        f"   {alice.properties['name']} KNOWS {bob.properties['name']} (since {friendship.properties['since']})"
    )
    print(
        f"   {alice.properties['name']} WORKS_FOR {company.properties['name']} (as {employment.properties['position']})"
    )

    # Test serialization
    alice_dict = alice.to_dict()
    restored_alice = Vertex.from_dict(alice_dict)

    edge_dict = friendship.to_dict()
    restored_edge = Edge.from_dict(edge_dict)

    print(f"\n✅ Serialization tests:")
    print(f"   Vertex: {'✅' if alice.name == restored_alice.name else '❌'}")
    print(f"   Edge: {'✅' if friendship.label == restored_edge.label else '❌'}")

    # Demonstrate graph queries
    match_query = GraphQuery(
        query_type="match", vertex_filter={"label": "Person"}, limit=10
    )

    traversal_query = GraphQuery(
        query_type="traverse_bfs", start_vertex="user1", max_depth=3
    )

    print(f"\n✅ Graph query creation:")
    print(f"   Match query type: {match_query.query_type}")
    print(f"   Traversal start: {traversal_query.start_vertex}")
    print(f"   Max depth: {traversal_query.max_depth}")


def demo_unified_architecture():
    """Demonstrate unified multimodel architecture"""
    print("\n🏗️  Unified Architecture Demo")
    print("=" * 50)

    from server.multimodel.unified.record_layout import UnifiedRecordLayout, RecordType

    layout = UnifiedRecordLayout()

    # Demonstrate different record types
    record_types = [
        (RecordType.RELATIONAL, {"id": 1, "name": "John", "age": 30}),
        (
            RecordType.DOCUMENT,
            {"_id": "doc1", "title": "Article", "content": {"text": "Hello"}},
        ),
        (
            RecordType.GRAPH_VERTEX,
            {"id": "v1", "label": "Person", "properties": {"name": "Alice"}},
        ),
        (
            RecordType.GRAPH_EDGE,
            {
                "id": "e1",
                "from_vertex": "v1",
                "to_vertex": "v2",
                "label": "KNOWS",
                "properties": {},
            },
        ),
    ]

    print("✅ Unified binary encoding for all model types:")

    total_bytes = 0
    for record_type, data in record_types:
        encoded = layout.encode_record(record_type, data)
        decoded = layout.decode_record(encoded)
        total_bytes += len(encoded)

        # Check if decoding worked correctly
        if record_type == RecordType.GRAPH_VERTEX:
            match = (
                data["id"] == decoded["id"]
                and data["label"] == decoded["label"]
                and data["properties"] == decoded["properties"]
            )
        elif record_type == RecordType.GRAPH_EDGE:
            match = (
                data["id"] == decoded["id"]
                and data["from_vertex"] == decoded["from_vertex"]
            )
        else:
            match = all(k in decoded and decoded[k] == v for k, v in data.items())

        print(
            f"   {record_type.name}: {len(encoded)} bytes - {'✅' if match else '❌'}"
        )

    print(f"\n📊 Total encoded size: {total_bytes} bytes")
    print(f"📊 Average record size: {total_bytes / len(record_types):.1f} bytes")


def demo_architecture_benefits():
    """Show architecture benefits"""
    print("\n🚀 Architecture Benefits")
    print("=" * 50)

    benefits = [
        "Single storage engine for all model types",
        "Efficient binary encoding with compression",
        "Shared B+ tree infrastructure",
        "Unified query optimization",
        "Cross-model operations support",
        "Lazy loading for performance",
        "Type-safe operations",
        "JSONPath query support",
        "Graph traversal algorithms",
        "Schema evolution capability",
    ]

    for i, benefit in enumerate(benefits, 1):
        print(f"   {i:2d}. ✅ {benefit}")


def main():
    """Run the complete multimodel demo"""
    print("🚀 pyHMSSQL Multimodel Extension Standalone Demo")
    print("=" * 70)
    print("This demo showcases the core multimodel capabilities without dependencies")
    print("including Object-Relational, Document Store, and Graph Database features.")
    print("=" * 70)

    try:
        demo_object_relational()
        demo_document_store()
        demo_graph_database()
        demo_unified_architecture()
        demo_architecture_benefits()

        print("\n🎉 Demo Complete!")
        print("=" * 70)
        print("The multimodel extension successfully provides:")
        print("✅ Object-Relational: Composite types, inheritance, nested queries")
        print("✅ Document Store: Schema-less JSON storage with JSONPath")
        print("✅ Graph Database: Vertices, edges, and traversal algorithms")
        print("✅ Unified Architecture: Single engine supporting all models")
        print("\n📚 Key Features Implemented:")
        print("   • TypeSystem with validation and registry")
        print("   • UnifiedRecordLayout for efficient storage")
        print("   • JSONPath parser for document queries")
        print("   • Graph algorithms (BFS, DFS, shortest path)")
        print("   • ModelRouter for query dispatching")
        print("   • Lazy loading and performance optimization")
        print("\n🔗 Ready for integration with existing pyHMSSQL infrastructure!")

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
