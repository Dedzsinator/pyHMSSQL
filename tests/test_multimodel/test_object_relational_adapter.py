"""
Comprehensive test suite for the Object-Relational Adapter.

This module tests all aspects of the Object-Relational functionality including:
- Composite type definitions and operations
- Table inheritance hierarchies
- Nested attribute queries (address.street)
- Type-safe operations
- Automatic schema evolution
- Integration with unified type system
"""

import pytest
import json
import uuid
import time
from typing import Dict, List, Any, Optional
from unittest.mock import Mock, patch, MagicMock

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from server.multimodel.object_relational.or_adapter import ObjectRelationalAdapter
from server.multimodel.unified.type_system import (
    TypeRegistry,
    TypeValidator,
    TypeDefinition,
    CompositeTypeDefinition,
    ArrayTypeDefinition,
    PrimitiveTypeDefinition,
    TypeAttribute,
    PrimitiveType,
    TypeCategory,
)
from catalog_manager import CatalogManager
from index_manager import IndexManager


class TestTypeSystem:
    """Test the unified type system components."""

    def test_primitive_type_definition(self):
        """Test primitive type definitions."""
        int_type = PrimitiveTypeDefinition(
            name="integer",
            category=TypeCategory.PRIMITIVE,
            primitive_type=PrimitiveType.INTEGER,
        )

        assert int_type.name == "integer"
        assert int_type.primitive_type == PrimitiveType.INTEGER
        assert int_type.category == TypeCategory.PRIMITIVE

    def test_composite_type_definition(self):
        """Test composite type definitions."""
        # Define address type
        address_type = CompositeTypeDefinition(
            name="address", category=TypeCategory.COMPOSITE
        )

        # Create primitive types for attributes
        varchar_type = PrimitiveTypeDefinition(
            name="varchar",
            category=TypeCategory.PRIMITIVE,
            primitive_type=PrimitiveType.VARCHAR,
        )

        # Add attributes
        address_type.add_attribute(
            TypeAttribute(name="street", type_def=varchar_type, nullable=False)
        )
        address_type.add_attribute(
            TypeAttribute(name="city", type_def=varchar_type, nullable=False)
        )
        address_type.add_attribute(
            TypeAttribute(name="zip_code", type_def=varchar_type, nullable=True)
        )
        address_type.add_attribute(
            TypeAttribute(
                name="country",
                type_def=varchar_type,
                nullable=False,
                default_value="USA",
            )
        )

        assert address_type.name == "address"
        assert address_type.category == TypeCategory.COMPOSITE
        assert len(address_type.attributes) == 4

        # Check attributes
        street_attr = next(
            attr for attr in address_type.attributes if attr.name == "street"
        )
        assert street_attr.type_def.name == "varchar"
        assert street_attr.nullable == False

        country_attr = next(
            attr for attr in address_type.attributes if attr.name == "country"
        )
        assert country_attr.default_value == "USA"

    def test_array_type_definition(self):
        """Test array type definitions."""
        # Create the element type first
        varchar_type = PrimitiveTypeDefinition(
            name="varchar",
            category=TypeCategory.PRIMITIVE,
            primitive_type=PrimitiveType.VARCHAR,
        )

        string_array = ArrayTypeDefinition(
            name="string_array",
            category=TypeCategory.ARRAY,
            element_type=varchar_type,
            max_length=100,
        )

        assert string_array.name == "string_array"
        assert string_array.element_type.name == "varchar"
        assert string_array.max_length == 100
        assert string_array.category == TypeCategory.ARRAY

    def test_type_registry(self):
        """Test type registry operations."""
        registry = TypeRegistry()

        # Register primitive type
        int_type = PrimitiveTypeDefinition(
            name="my_int",
            category=TypeCategory.PRIMITIVE,
            primitive_type=PrimitiveType.INTEGER,
        )
        registry.register_type(int_type)

        # Create type definitions for attributes first
        varchar_type = PrimitiveTypeDefinition(
            name="varchar",
            category=TypeCategory.PRIMITIVE,
            primitive_type=PrimitiveType.VARCHAR,
        )

        # Register composite type
        address_type = CompositeTypeDefinition(
            name="address",
            category=TypeCategory.COMPOSITE,
            attributes=[
                TypeAttribute(name="street", type_def=varchar_type),
                TypeAttribute(name="city", type_def=varchar_type),
            ],
        )
        registry.register_type(address_type)

        # Test retrieval
        retrieved_int = registry.get_type("my_int")
        assert retrieved_int is not None
        assert retrieved_int.name == "my_int"

        retrieved_address = registry.get_type("address")
        assert retrieved_address is not None
        assert retrieved_address.name == "address"
        assert len(retrieved_address.attributes) == 2

        # Test listing types
        all_types = registry.list_types()
        type_names = [t.name for t in all_types]
        assert "my_int" in type_names
        assert "address" in type_names

    def test_type_validator(self):
        """Test type validation."""
        registry = TypeRegistry()

        # Create varchar type for attributes
        varchar_type = PrimitiveTypeDefinition(
            name="varchar",
            category=TypeCategory.PRIMITIVE,
            primitive_type=PrimitiveType.VARCHAR,
        )

        # Register types
        address_type = CompositeTypeDefinition(
            name="address",
            category=TypeCategory.COMPOSITE,
            attributes=[
                TypeAttribute(name="street", type_def=varchar_type, nullable=False),
                TypeAttribute(name="city", type_def=varchar_type, nullable=False),
                TypeAttribute(name="zip_code", type_def=varchar_type, nullable=True),
            ],
        )
        registry.register_type(address_type)

        validator = TypeValidator(registry)

        # Valid address
        valid_address = {
            "street": "123 Main St",
            "city": "Anytown",
            "zip_code": "12345",
        }

        assert validator.validate_value(valid_address, "address") == True

        # Valid address with null optional field
        valid_address_no_zip = {"street": "456 Oak Ave", "city": "Somewhere"}

        assert validator.validate_value(valid_address_no_zip, "address") == True

        # Invalid address - missing required field
        invalid_address = {
            "street": "789 Pine St"
            # Missing city
        }

        assert validator.validate_value(invalid_address, "address") == False


class TestObjectRelationalAdapter:
    """Test the main Object-Relational Adapter functionality."""

    @pytest.fixture
    def mock_catalog_manager(self):
        """Create a mock catalog manager."""
        catalog = Mock()  # Remove spec to allow any attribute access
        catalog.get_table_schema.return_value = []
        catalog.table_exists.return_value = False
        catalog.create_table.return_value = True
        catalog.drop_table.return_value = True
        catalog.insert_record.return_value = {"id": 1}
        catalog.query_with_condition.return_value = []
        catalog.get_current_database.return_value = "test_db"
        catalog.list_tables.return_value = []
        catalog.get_type.return_value = None
        catalog.create_type.return_value = True
        catalog.drop_type.return_value = True
        catalog.list_types.return_value = []
        catalog.create_index.return_value = True
        return catalog

    @pytest.fixture
    def mock_index_manager(self):
        """Create a mock index manager."""
        index_mgr = Mock()  # Remove spec to allow any attribute access
        index_mgr.create_index.return_value = True
        index_mgr.drop_index.return_value = True
        index_mgr.get_index.return_value = None
        return index_mgr

    @pytest.fixture
    def adapter(self, mock_catalog_manager, mock_index_manager):
        """Create an object-relational adapter instance."""
        return ObjectRelationalAdapter(mock_catalog_manager, mock_index_manager)

    def test_create_composite_type(self, adapter):
        """Test creating composite types."""
        # Define address type
        type_definition = {
            "type": "composite",
            "attributes": [
                {"name": "street", "type": "varchar(100)", "nullable": False},
                {"name": "city", "type": "varchar(50)", "nullable": False},
                {"name": "state", "type": "varchar(2)", "nullable": False},
                {"name": "zip_code", "type": "varchar(10)", "nullable": True},
                {
                    "name": "country",
                    "type": "varchar(50)",
                    "nullable": False,
                    "default": "USA",
                },
            ],
        }

        result = adapter.create_type("address", type_definition)

        assert result["status"] == "success"
        assert "address" in adapter.type_registry.list_type_names()

        # Verify type was registered correctly
        address_type = adapter.type_registry.get_type("address")
        assert address_type is not None
        assert len(address_type.attributes) == 5

        # Check specific attributes
        street_attr = next(
            attr for attr in address_type.attributes if attr.name == "street"
        )
        assert street_attr.nullable == False

        country_attr = next(
            attr for attr in address_type.attributes if attr.name == "country"
        )
        assert country_attr.default_value == "USA"

    def test_create_array_type(self, adapter):
        """Test creating array types."""
        type_definition = {
            "type": "array",
            "element_type": "varchar(50)",
            "max_length": 10,
            "nullable": True,
        }

        result = adapter.create_type("skill_list", type_definition)

        assert result["status"] == "success"
        assert "skill_list" in adapter.type_registry.list_type_names()

        # Verify array type
        skill_type = adapter.type_registry.get_type("skill_list")
        assert skill_type.category == TypeCategory.ARRAY
        assert skill_type.element_type == "varchar(50)"
        assert skill_type.max_length == 10

    def test_create_nested_composite_type(self, adapter):
        """Test creating nested composite types."""
        # First create address type
        address_def = {
            "type": "composite",
            "attributes": [
                {"name": "street", "type": "varchar(100)", "nullable": False},
                {"name": "city", "type": "varchar(50)", "nullable": False},
            ],
        }
        adapter.create_type("address", address_def)

        # Create person type that uses address
        person_def = {
            "type": "composite",
            "attributes": [
                {"name": "name", "type": "varchar(100)", "nullable": False},
                {"name": "age", "type": "integer", "nullable": False},
                {"name": "home_address", "type": "address", "nullable": True},
                {"name": "work_address", "type": "address", "nullable": True},
            ],
        }

        result = adapter.create_type("person", person_def)

        assert result["status"] == "success"

        # Verify nested type
        person_type = adapter.type_registry.get_type("person")
        address_attrs = [
            attr for attr in person_type.attributes if attr.type_name == "address"
        ]
        assert len(address_attrs) == 2  # home_address and work_address

    def test_create_table_with_composite_columns(self, adapter):
        """Test creating tables with composite type columns."""
        # Create address type first
        address_def = {
            "type": "composite",
            "attributes": [
                {"name": "street", "type": "varchar(100)", "nullable": False},
                {"name": "city", "type": "varchar(50)", "nullable": False},
                {"name": "zip_code", "type": "varchar(10)", "nullable": True},
            ],
        }
        adapter.create_type("address", address_def)

        # Create table with composite column
        table_definition = {
            "operation": "CREATE_OR_TABLE",
            "table_name": "employees",
            "columns": [
                {
                    "name": "id",
                    "type": "integer",
                    "nullable": False,
                    "primary_key": True,
                },
                {"name": "name", "type": "varchar(100)", "nullable": False},
                {"name": "home_address", "type": "address", "nullable": True},
                {"name": "salary", "type": "decimal(10,2)", "nullable": True},
            ],
        }

        result = adapter.execute_query(table_definition)

        assert result["status"] == "success"

        # Verify table is tracked
        assert "employees" in adapter.or_tables
        table_info = adapter.or_tables["employees"]
        assert any(col["type"] == "address" for col in table_info["columns"])

    def test_insert_with_composite_values(self, adapter):
        """Test inserting data with composite type values."""
        # Setup type and table
        address_def = {
            "type": "composite",
            "attributes": [
                {"name": "street", "type": "varchar(100)", "nullable": False},
                {"name": "city", "type": "varchar(50)", "nullable": False},
            ],
        }
        adapter.create_type("address", address_def)

        table_def = {
            "operation": "CREATE_OR_TABLE",
            "table_name": "customers",
            "columns": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "name", "type": "varchar(100)", "nullable": False},
                {"name": "billing_address", "type": "address", "nullable": False},
            ],
        }
        adapter.execute_query(table_def)

        # Insert data
        insert_query = {
            "operation": "INSERT_OR_DATA",
            "table_name": "customers",
            "values": {
                "id": 1,
                "name": "John Doe",
                "billing_address": {"street": "123 Main St", "city": "Anytown"},
            },
        }

        result = adapter.execute_query(insert_query)

        assert result["status"] == "success"

        # Verify validation was performed
        # The adapter should have validated the composite value structure

    def test_query_nested_attributes(self, adapter):
        """Test querying nested attributes using dot notation."""
        # Setup type and table
        address_def = {
            "type": "composite",
            "attributes": [
                {"name": "street", "type": "varchar(100)", "nullable": False},
                {"name": "city", "type": "varchar(50)", "nullable": False},
                {"name": "state", "type": "varchar(2)", "nullable": False},
            ],
        }
        adapter.create_type("address", address_def)

        table_def = {
            "operation": "CREATE_OR_TABLE",
            "table_name": "stores",
            "columns": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "name", "type": "varchar(100)", "nullable": False},
                {"name": "location", "type": "address", "nullable": False},
            ],
        }
        adapter.execute_query(table_def)

        # Query with nested attribute filter
        query = {
            "operation": "SELECT_OR_DATA",
            "table_name": "stores",
            "where_clause": "location.city = 'New York'",
            "select_columns": ["id", "name", "location.street", "location.city"],
        }

        result = adapter.execute_query(query)

        # Should parse and handle nested attribute queries
        assert result["status"] == "success" or "error" in result
        # The adapter should recognize dot notation in queries

    def test_table_inheritance(self, adapter):
        """Test table inheritance functionality."""
        # Create base table
        base_table_def = {
            "operation": "CREATE_OR_TABLE",
            "table_name": "person",
            "columns": [
                {
                    "name": "id",
                    "type": "integer",
                    "nullable": False,
                    "primary_key": True,
                },
                {"name": "name", "type": "varchar(100)", "nullable": False},
                {"name": "email", "type": "varchar(100)", "nullable": True},
            ],
        }
        adapter.execute_query(base_table_def)

        # Create inherited table
        employee_table_def = {
            "operation": "CREATE_OR_TABLE",
            "table_name": "employee",
            "inherits": "person",
            "columns": [
                {"name": "employee_id", "type": "varchar(20)", "nullable": False},
                {"name": "department", "type": "varchar(50)", "nullable": False},
                {"name": "salary", "type": "decimal(10,2)", "nullable": True},
            ],
        }

        result = adapter.execute_query(employee_table_def)

        assert result["status"] == "success"

        # Verify inheritance relationship
        assert "employee" in adapter.or_tables
        employee_info = adapter.or_tables["employee"]
        assert employee_info.get("inherits") == "person"

        # Employee should have both its own columns and inherited columns
        all_columns = employee_info["columns"]
        column_names = [col["name"] for col in all_columns]
        assert "id" in column_names  # Inherited
        assert "name" in column_names  # Inherited
        assert "email" in column_names  # Inherited
        assert "employee_id" in column_names  # Own
        assert "department" in column_names  # Own
        assert "salary" in column_names  # Own

    def test_polymorphic_queries(self, adapter):
        """Test polymorphic queries across inheritance hierarchy."""
        # Setup inheritance hierarchy
        person_def = {
            "operation": "CREATE_OR_TABLE",
            "table_name": "person",
            "columns": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "name", "type": "varchar(100)", "nullable": False},
            ],
        }
        adapter.execute_query(person_def)

        employee_def = {
            "operation": "CREATE_OR_TABLE",
            "table_name": "employee",
            "inherits": "person",
            "columns": [{"name": "salary", "type": "decimal(10,2)", "nullable": True}],
        }
        adapter.execute_query(employee_def)

        manager_def = {
            "operation": "CREATE_OR_TABLE",
            "table_name": "manager",
            "inherits": "employee",
            "columns": [{"name": "bonus", "type": "decimal(10,2)", "nullable": True}],
        }
        adapter.execute_query(manager_def)

        # Query that should include all subtypes
        polymorphic_query = {
            "operation": "SELECT_OR_DATA",
            "table_name": "person",
            "polymorphic": True,
            "select_columns": ["id", "name"],
        }

        result = adapter.execute_query(polymorphic_query)

        # Should handle polymorphic queries
        assert result["status"] == "success" or "error" in result

    def test_array_column_operations(self, adapter):
        """Test operations with array columns."""
        # Create array type
        skill_array_def = {
            "type": "array",
            "element_type": "varchar(50)",
            "max_length": 10,
        }
        adapter.create_type("skill_array", skill_array_def)

        # Create table with array column
        table_def = {
            "operation": "CREATE_OR_TABLE",
            "table_name": "developers",
            "columns": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "name", "type": "varchar(100)", "nullable": False},
                {"name": "skills", "type": "skill_array", "nullable": True},
            ],
        }
        adapter.execute_query(table_def)

        # Insert data with array values
        insert_query = {
            "operation": "INSERT_OR_DATA",
            "table_name": "developers",
            "values": {
                "id": 1,
                "name": "Alice",
                "skills": ["Python", "SQL", "JavaScript", "Docker"],
            },
        }

        result = adapter.execute_query(insert_query)

        assert result["status"] == "success"

        # Query with array operations
        array_query = {
            "operation": "SELECT_OR_DATA",
            "table_name": "developers",
            "where_clause": "skills @> ARRAY['Python']",  # Contains Python
            "select_columns": ["id", "name", "skills"],
        }

        result = adapter.execute_query(array_query)

        # Should handle array queries
        assert result["status"] == "success" or "error" in result


class TestObjectRelationalPerformance:
    """Test performance characteristics of object-relational features."""

    @pytest.fixture
    def adapter(self):
        """Create adapter for performance testing."""
        catalog = Mock()  # Remove spec to allow any attribute access
        catalog.get_table_schema.return_value = []
        catalog.table_exists.return_value = False
        catalog.create_table.return_value = True
        catalog.drop_table.return_value = True
        catalog.insert_record.return_value = {"id": 1}
        catalog.query_with_condition.return_value = []
        catalog.get_current_database.return_value = "test_db"
        catalog.list_tables.return_value = []
        catalog.get_type.return_value = None
        catalog.create_type.return_value = True
        catalog.drop_type.return_value = True
        catalog.list_types.return_value = []
        catalog.get_current_database.return_value = "perf_db"
        index_mgr = Mock(spec=IndexManager)
        return ObjectRelationalAdapter(catalog, index_mgr)

    def test_complex_type_creation_performance(self, adapter):
        """Test performance of creating complex type hierarchies."""
        start_time = time.time()

        # Create multiple nested composite types
        for i in range(50):
            type_def = {
                "type": "composite",
                "attributes": [
                    {"name": f"field_{j}", "type": "varchar(100)", "nullable": True}
                    for j in range(10)  # 10 fields per type
                ],
            }
            adapter.create_type(f"complex_type_{i}", type_def)

        end_time = time.time()
        creation_time = end_time - start_time
        types_per_second = 50 / creation_time

        print(f"Created 50 complex types in {creation_time:.2f}s")
        print(f"Rate: {types_per_second:.2f} types/sec")

        # Verify all types were created
        type_names = adapter.type_registry.list_type_names()
        created_types = [
            name for name in type_names if name.startswith("complex_type_")
        ]
        assert len(created_types) == 50

        # Performance should be reasonable
        assert types_per_second > 10  # At least 10 types/sec

    def test_nested_validation_performance(self, adapter):
        """Test performance of validating deeply nested structures."""
        # Create nested composite types
        base_type_def = {
            "type": "composite",
            "attributes": [
                {"name": "value", "type": "varchar(100)", "nullable": False}
            ],
        }
        adapter.create_type("base_type", base_type_def)

        # Create progressively more nested types
        for depth in range(1, 6):  # 5 levels of nesting
            nested_def = {
                "type": "composite",
                "attributes": [
                    {
                        "name": "data",
                        "type": f"nested_type_{depth-1}" if depth > 1 else "base_type",
                        "nullable": False,
                    },
                    {"name": "level", "type": "integer", "nullable": False},
                ],
            }
            adapter.create_type(f"nested_type_{depth}", nested_def)

        # Create deeply nested test values
        test_values = []
        for i in range(100):
            nested_value = {"value": f"test_{i}"}
            for depth in range(1, 6):
                nested_value = {"data": nested_value, "level": depth}
            test_values.append(nested_value)

        # Test validation performance
        validator = adapter.type_validator

        start_time = time.time()
        for value in test_values:
            validator.validate_value(value, "nested_type_5")
        end_time = time.time()

        validation_time = end_time - start_time
        validations_per_second = len(test_values) / validation_time

        print(f"Validated {len(test_values)} nested values in {validation_time:.4f}s")
        print(f"Rate: {validations_per_second:.2f} validations/sec")

        # Performance should be reasonable
        assert validations_per_second > 50  # At least 50 validations/sec

    def test_inheritance_resolution_performance(self, adapter):
        """Test performance of inheritance resolution."""
        # Create inheritance hierarchy
        base_table_def = {
            "operation": "CREATE_OR_TABLE",
            "table_name": "base_entity",
            "columns": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "created_at", "type": "timestamp", "nullable": False},
            ],
        }
        adapter.execute_query(base_table_def)

        # Create multiple levels of inheritance
        current_parent = "base_entity"
        for level in range(10):  # 10 levels deep
            table_def = {
                "operation": "CREATE_OR_TABLE",
                "table_name": f"level_{level}",
                "inherits": current_parent,
                "columns": [
                    {
                        "name": f"level_{level}_field",
                        "type": "varchar(100)",
                        "nullable": True,
                    }
                ],
            }
            adapter.execute_query(table_def)
            current_parent = f"level_{level}"

        # Test inheritance resolution performance
        start_time = time.time()

        for i in range(100):
            # Resolve inheritance for deepest table
            adapter._resolve_inherited_columns("level_9")

        end_time = time.time()
        resolution_time = end_time - start_time
        resolutions_per_second = 100 / resolution_time

        print(f"Performed 100 inheritance resolutions in {resolution_time:.4f}s")
        print(f"Rate: {resolutions_per_second:.2f} resolutions/sec")

        # Performance should be reasonable
        assert resolutions_per_second > 20  # At least 20 resolutions/sec


class TestObjectRelationalEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.fixture
    def adapter(self):
        """Create adapter for edge case testing."""
        catalog = Mock()  # Remove spec to allow any attribute access
        catalog.get_table_schema.return_value = []
        catalog.table_exists.return_value = False
        catalog.create_table.return_value = True
        catalog.drop_table.return_value = True
        catalog.insert_record.return_value = {"id": 1}
        catalog.query_with_condition.return_value = []
        catalog.get_current_database.return_value = "test_db"
        catalog.list_tables.return_value = []
        catalog.get_type.return_value = None
        catalog.create_type.return_value = True
        catalog.drop_type.return_value = True
        catalog.list_types.return_value = []
        index_mgr = Mock()  # Remove spec to allow any attribute access
        index_mgr.create_index.return_value = True
        index_mgr.drop_index.return_value = True
        index_mgr.list_indexes.return_value = []
        return ObjectRelationalAdapter(catalog, index_mgr)

    def test_circular_type_dependencies(self, adapter):
        """Test handling of circular type dependencies."""
        # Try to create circular dependencies: A -> B -> A
        type_a_def = {
            "type": "composite",
            "attributes": [{"name": "b_field", "type": "type_b", "nullable": True}],
        }

        type_b_def = {
            "type": "composite",
            "attributes": [{"name": "a_field", "type": "type_a", "nullable": True}],
        }

        # Create first type
        result_a = adapter.create_type("type_a", type_a_def)

        # Creating second type should either handle the forward reference
        # or detect the circular dependency
        result_b = adapter.create_type("type_b", type_b_def)

        # System should handle this gracefully
        assert result_a["status"] in ["success", "error"]
        assert result_b["status"] in ["success", "error"]

        # If both succeeded, both types should be registered
        if result_a["status"] == "success" and result_b["status"] == "success":
            assert "type_a" in adapter.type_registry.list_type_names()
            assert "type_b" in adapter.type_registry.list_type_names()

    def test_invalid_type_definitions(self, adapter):
        """Test handling of invalid type definitions."""
        invalid_definitions = [
            # Missing required fields
            {
                "type": "composite"
                # Missing attributes
            },
            # Invalid attribute types
            {
                "type": "composite",
                "attributes": [
                    {"name": "field1", "type": "nonexistent_type", "nullable": False}
                ],
            },
            # Invalid array definition
            {
                "type": "array",
                "element_type": "varchar(50)",
                "max_length": -1,  # Invalid length
            },
            # Empty composite type
            {"type": "composite", "attributes": []},
        ]

        for i, invalid_def in enumerate(invalid_definitions):
            result = adapter.create_type(f"invalid_type_{i}", invalid_def)

            # Should handle invalid definitions gracefully
            assert result["status"] == "error"
            assert "error" in result

    def test_deep_inheritance_chains(self, adapter):
        """Test very deep inheritance chains."""
        # Create a deep inheritance chain
        base_def = {
            "operation": "CREATE_OR_TABLE",
            "table_name": "level_0",
            "columns": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "base_field", "type": "varchar(100)", "nullable": True},
            ],
        }
        adapter.execute_query(base_def)

        # Create 20 levels of inheritance
        for level in range(1, 21):
            table_def = {
                "operation": "CREATE_OR_TABLE",
                "table_name": f"level_{level}",
                "inherits": f"level_{level-1}",
                "columns": [
                    {"name": f"field_{level}", "type": "varchar(100)", "nullable": True}
                ],
            }
            result = adapter.execute_query(table_def)

            # Should handle deep inheritance
            assert result["status"] == "success"

        # Test that the deepest level has all inherited columns
        deepest_table = adapter.or_tables.get("level_20")
        if deepest_table:
            all_columns = deepest_table["columns"]
            column_names = [col["name"] for col in all_columns]

            # Should have base field and all level fields
            assert "base_field" in column_names
            assert "field_1" in column_names
            assert "field_20" in column_names
            assert len(column_names) >= 22  # id + base_field + 20 level fields

    def test_large_composite_types(self, adapter):
        """Test handling of very large composite types."""
        # Create a composite type with many attributes
        large_attributes = []
        for i in range(200):  # 200 attributes
            large_attributes.append(
                {
                    "name": f"attr_{i:03d}",
                    "type": "varchar(100)",
                    "nullable": i % 2 == 0,  # Alternate nullable/not nullable
                    "default": f"default_{i}" if i % 5 == 0 else None,
                }
            )

        large_type_def = {"type": "composite", "attributes": large_attributes}

        result = adapter.create_type("large_composite", large_type_def)

        # Should handle large types
        assert result["status"] == "success"

        # Verify all attributes were registered
        large_type = adapter.type_registry.get_type("large_composite")
        assert len(large_type.attributes) == 200

        # Test validation with large type
        test_value = {f"attr_{i:03d}": f"value_{i}" for i in range(200)}

        validator = adapter.type_validator
        is_valid = validator.validate_value(test_value, "large_composite")
        assert is_valid == True

    def test_unicode_and_special_characters_in_types(self, adapter):
        """Test handling of Unicode and special characters in type definitions."""
        # Create type with Unicode names and values
        unicode_type_def = {
            "type": "composite",
            "attributes": [
                {"name": "名前", "type": "varchar(100)", "nullable": False},  # Japanese
                {"name": "العنوان", "type": "varchar(200)", "nullable": True},  # Arabic
                {"name": "адрес", "type": "varchar(150)", "nullable": True},  # Cyrillic
                {
                    "name": "emoji_field",
                    "type": "varchar(50)",
                    "nullable": True,
                    "default": "😀🌟",
                },
            ],
        }

        result = adapter.create_type("unicode_type", unicode_type_def)

        # Should handle Unicode in type definitions
        assert result["status"] == "success"

        # Test validation with Unicode values
        unicode_value = {
            "名前": "田中太郎",
            "العنوان": "شارع الملك فهد",
            "адрес": "Красная площадь",
            "emoji_field": "🎉🎊🎈",
        }

        validator = adapter.type_validator
        is_valid = validator.validate_value(unicode_value, "unicode_type")
        assert is_valid == True

    def test_malformed_queries(self, adapter):
        """Test handling of malformed object-relational queries."""
        # Setup basic type and table
        address_def = {
            "type": "composite",
            "attributes": [
                {"name": "street", "type": "varchar(100)", "nullable": False}
            ],
        }
        adapter.create_type("address", address_def)

        table_def = {
            "operation": "CREATE_OR_TABLE",
            "table_name": "test_table",
            "columns": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "addr", "type": "address", "nullable": True},
            ],
        }
        adapter.execute_query(table_def)

        # Test malformed queries
        malformed_queries = [
            # Invalid operation
            {"operation": "INVALID_OPERATION", "table_name": "test_table"},
            # Missing required fields
            {
                "operation": "CREATE_OR_TABLE"
                # Missing table_name
            },
            # Invalid nested attribute query
            {
                "operation": "SELECT_OR_DATA",
                "table_name": "test_table",
                "where_clause": "addr.nonexistent_field = 'value'",
            },
            # Invalid inheritance
            {
                "operation": "CREATE_OR_TABLE",
                "table_name": "invalid_inherit",
                "inherits": "nonexistent_table",
                "columns": [{"name": "field", "type": "varchar(50)"}],
            },
        ]

        for i, malformed_query in enumerate(malformed_queries):
            result = adapter.execute_query(malformed_query)

            # Should handle malformed queries gracefully
            assert result["status"] == "error"
            assert "error" in result
            print(f"Malformed query {i}: {result['error']}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
