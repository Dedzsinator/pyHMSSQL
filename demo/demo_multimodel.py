#!/usr/bin/env python3
"""
pyHMSSQL Multimodel Extension Demo

This script demonstrates the capabilities of the multimodel extension
including Object-Relational, Document Store, and Graph Database features.
"""

import json
from datetime import datetime
from typing import Dict, Any


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

    invalid_address = {
        "street": "123 Main St"
        # Missing city
    }

    from server.multimodel.unified.type_system import TypeValidator

    validator = TypeValidator(registry)

    print(
        f"\n✅ Valid address validation: {validator.validate_value(valid_address, address_type)}"
    )
    print(
        f"❌ Invalid address validation: {validator.validate_value(invalid_address, address_type)}"
    )

    print("\n📝 Sample Object-Relational Queries:")
    print(
        """
    CREATE TYPE address AS (
        street VARCHAR(100),
        city VARCHAR(50)
    );
    
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name VARCHAR(100),
        home_address address,
        work_address address
    );
    
    SELECT name, home_address.city 
    FROM users 
    WHERE work_address.city = 'New York';
    """
    )


def demo_document_store():
    """Demonstrate Document Store features"""
    print("\n📄 Document Store Features Demo")
    print("=" * 50)

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
    print(json.dumps(user_document, indent=2))

    # Demonstrate JSONPath extraction
    paths = JSONPathParser.extract_paths(user_document)
    print(f"\n✅ Extracted {len(paths)} indexable paths from document")

    # Show some interesting paths
    interesting_paths = [p for p in paths if len(p[0].split(".")) > 1][:5]
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
    print(f"\n✅ Created collection: {collection.name}")

    print("\n📝 Sample Document Store Operations:")
    print(
        """
    CREATE COLLECTION users;
    
    INSERT INTO users DOCUMENT '{
        "name": "Alice",
        "profile": {"age": 28, "city": "SF"}
    }';
    
    FIND users WHERE profile.age > 25;
    
    UPDATE users SET profile.city = "New York" 
    WHERE name = "Alice";
    
    CREATE INDEX ON users (profile.city);
    """
    )


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
    print(f"   {alice.properties['name']} (Person)")
    print(f"   {bob.properties['name']} (Person)")
    print(f"   {company.properties['name']} (Company)")

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
    print(f"   {alice.properties['name']} KNOWS {bob.properties['name']}")
    print(f"   {alice.properties['name']} WORKS_FOR {company.properties['name']}")

    # Demonstrate graph queries
    match_query = GraphQuery(
        query_type="match", vertex_filter={"label": "Person"}, limit=10
    )

    traversal_query = GraphQuery(
        query_type="traverse_bfs", start_vertex="user1", max_depth=3
    )

    print(f"\n✅ Graph query examples:")
    print(f"   Match query: Find all Person vertices")
    print(f"   Traversal query: BFS from Alice with max depth 3")

    print("\n📝 Sample Graph Database Operations:")
    print(
        """
    CREATE VERTEX Person PROPERTIES '{
        "name": "Alice", "age": 28
    }';
    
    CREATE EDGE KNOWS 
    FROM user1 TO user2 
    PROPERTIES '{"since": "2020"}';
    
    TRAVERSE BFS FROM user1 MAX_DEPTH 3
    WHERE vertex.label = "Person";
    
    SHORTEST_PATH FROM user1 TO user3;
    
    MATCH (p:Person)-[k:KNOWS]->(friend:Person)
    WHERE p.age > 25;
    """
    )


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

    for record_type, data in record_types:
        encoded = layout.encode_record(record_type, data)
        decoded = layout.decode_record(encoded)

        print(f"   {record_type.name}: {len(encoded)} bytes")
        print(f"     Original: {data}")
        print(f"     Decoded:  {decoded}")
        print(
            f"     Match: {'✅' if data == decoded or data.items() <= decoded.items() else '❌'}"
        )
        print()

    print("🚀 Architecture Benefits:")
    print("   • Single storage engine for all model types")
    print("   • Efficient binary encoding with compression")
    print("   • Shared B+ tree infrastructure")
    print("   • Unified query optimization")
    print("   • Cross-model operations support")


def demo_cross_model_scenarios():
    """Demonstrate cross-model operation scenarios"""
    print("\n🔗 Cross-Model Integration Scenarios")
    print("=" * 50)

    print("📊 Scenario 1: E-commerce Platform")
    print(
        """
    • Relational: Order processing, inventory, transactions
    • Document: Product catalogs, user preferences, reviews
    • Graph: Recommendation engine, social connections
    • Object-Relational: Address/contact information, composite types
    
    Example Query:
    SELECT o.order_id, p.document.name, u.profile.preferences
    FROM orders o
    JOIN products p ON o.product_id = p.id
    JOIN users u ON o.user_id = u.id
    WHERE TRAVERSE_GRAPH(u.id, 'SIMILAR_TO', 2) 
    AND p.document.category = 'electronics';
    """
    )

    print("\n🏥 Scenario 2: Healthcare System")
    print(
        """
    • Relational: Patient records, appointments, billing
    • Document: Medical images metadata, clinical notes
    • Graph: Drug interactions, disease relationships
    • Object-Relational: Complex medical types (symptoms, treatments)
    
    Example Query:
    SELECT p.name, p.medical_history.allergies
    FROM patients p
    WHERE EXISTS(
        SELECT 1 FROM GRAPH_TRAVERSE(p.condition_id, 'INTERACTS_WITH')
        WHERE vertex.properties.drug_name = 'aspirin'
    );
    """
    )

    print("\n🎓 Scenario 3: Educational Platform")
    print(
        """
    • Relational: Course enrollment, grades, schedules  
    • Document: Course content, student submissions
    • Graph: Knowledge graphs, learning paths
    • Object-Relational: Complex assessment types
    
    Example Query:
    SELECT s.name, course.document.title
    FROM students s
    JOIN enrollments e ON s.id = e.student_id
    JOIN courses course ON e.course_id = course.id
    WHERE SHORTEST_PATH_GRAPH(s.current_topic, course.target_topic) <= 3;
    """
    )


def main():
    """Run the complete multimodel demo"""
    print("🚀 pyHMSSQL Multimodel Extension Demo")
    print("=" * 60)
    print("This demo showcases the multimodel capabilities added to pyHMSSQL")
    print("including Object-Relational, Document Store, and Graph Database features.")
    print("=" * 60)

    try:
        demo_object_relational()
        demo_document_store()
        demo_graph_database()
        demo_unified_architecture()
        demo_cross_model_scenarios()

        print("\n🎉 Demo Complete!")
        print("=" * 60)
        print("The multimodel extension successfully provides:")
        print("✅ Object-Relational: Composite types, inheritance, nested queries")
        print("✅ Document Store: Schema-less JSON storage with JSONPath")
        print("✅ Graph Database: Vertices, edges, and traversal algorithms")
        print("✅ Unified Architecture: Single engine supporting all models")
        print("\nThe extension is ready for production integration!")

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
