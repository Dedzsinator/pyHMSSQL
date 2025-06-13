"""
Object-Relational Type System for pyHMSSQL Multimodel Extension

This module implements a comprehensive type system for object-relational features
including composite types, arrays, and table inheritance. It integrates with the
existing catalog manager and provides type validation and serialization.
"""

import json
from typing import Dict, List, Any, Optional, Union, Type as PyType
from dataclasses import dataclass, field
from enum import Enum
import uuid


class TypeCategory(Enum):
    """Categories of types in the object-relational model"""

    PRIMITIVE = "primitive"
    COMPOSITE = "composite"
    ARRAY = "array"
    REFERENCE = "reference"
    DOMAIN = "domain"


class PrimitiveType(Enum):
    """Primitive data types"""

    INTEGER = "integer"
    BIGINT = "bigint"
    FLOAT = "float"
    DOUBLE = "double"
    DECIMAL = "decimal"
    VARCHAR = "varchar"
    CHAR = "char"
    TEXT = "text"
    BOOLEAN = "boolean"
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    BLOB = "blob"
    JSON = "json"


@dataclass
class TypeAttribute:
    """Represents an attribute in a composite type"""

    name: str
    type_def: "TypeDefinition"
    nullable: bool = True
    default_value: Any = None
    constraints: List[str] = field(default_factory=list)


@dataclass
class TypeDefinition:
    """Base class for all type definitions"""

    name: str
    category: TypeCategory
    schema_name: str = "public"
    created_at: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self) -> Dict[str, Any]:
        """Serialize type definition to dictionary"""
        return {
            "name": self.name,
            "category": self.category.value,
            "schema_name": self.schema_name,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TypeDefinition":
        """Deserialize type definition from dictionary"""
        category = TypeCategory(data["category"])

        if category == TypeCategory.PRIMITIVE:
            return PrimitiveTypeDefinition.from_dict(data)
        elif category == TypeCategory.COMPOSITE:
            return CompositeTypeDefinition.from_dict(data)
        elif category == TypeCategory.ARRAY:
            return ArrayTypeDefinition.from_dict(data)
        elif category == TypeCategory.REFERENCE:
            return ReferenceTypeDefinition.from_dict(data)
        elif category == TypeCategory.DOMAIN:
            return DomainTypeDefinition.from_dict(data)
        else:
            raise ValueError(f"Unknown type category: {category}")


@dataclass
class PrimitiveTypeDefinition(TypeDefinition):
    """Definition for primitive types"""

    primitive_type: PrimitiveType = field(default=PrimitiveType.VARCHAR)
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None

    def __post_init__(self):
        self.category = TypeCategory.PRIMITIVE

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data.update(
            {
                "primitive_type": self.primitive_type.value,
                "length": self.length,
                "precision": self.precision,
                "scale": self.scale,
            }
        )
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PrimitiveTypeDefinition":
        return cls(
            name=data["name"],
            category=TypeCategory.PRIMITIVE,
            schema_name=data.get("schema_name", "public"),
            created_at=data.get("created_at", str(uuid.uuid4())),
            primitive_type=PrimitiveType(data["primitive_type"]),
            length=data.get("length"),
            precision=data.get("precision"),
            scale=data.get("scale"),
        )


@dataclass
class CompositeTypeDefinition(TypeDefinition):
    """Definition for composite/structured types"""

    attributes: List[TypeAttribute] = field(default_factory=list)

    def __post_init__(self):
        self.category = TypeCategory.COMPOSITE

    def add_attribute(self, attr: TypeAttribute):
        """Add an attribute to the composite type"""
        # Check for duplicate attribute names
        if any(a.name == attr.name for a in self.attributes):
            raise ValueError(
                f"Attribute '{attr.name}' already exists in type '{self.name}'"
            )
        self.attributes.append(attr)

    def get_attribute(self, name: str) -> Optional[TypeAttribute]:
        """Get an attribute by name"""
        return next((attr for attr in self.attributes if attr.name == name), None)

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data.update(
            {
                "attributes": [
                    {
                        "name": attr.name,
                        "type_def": attr.type_def.to_dict(),
                        "nullable": attr.nullable,
                        "default_value": attr.default_value,
                        "constraints": attr.constraints,
                    }
                    for attr in self.attributes
                ]
            }
        )
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CompositeTypeDefinition":
        comp_type = cls(
            name=data["name"],
            category=TypeCategory.COMPOSITE,
            schema_name=data.get("schema_name", "public"),
            created_at=data.get("created_at", str(uuid.uuid4())),
        )

        for attr_data in data.get("attributes", []):
            attr = TypeAttribute(
                name=attr_data["name"],
                type_def=TypeDefinition.from_dict(attr_data["type_def"]),
                nullable=attr_data.get("nullable", True),
                default_value=attr_data.get("default_value"),
                constraints=attr_data.get("constraints", []),
            )
            comp_type.attributes.append(attr)

        return comp_type


@dataclass
class ArrayTypeDefinition(TypeDefinition):
    """Definition for array types"""

    element_type: TypeDefinition = field(default=None)
    max_length: Optional[int] = None

    def __post_init__(self):
        self.category = TypeCategory.ARRAY

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data.update(
            {"element_type": self.element_type.to_dict(), "max_length": self.max_length}
        )
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ArrayTypeDefinition":
        return cls(
            name=data["name"],
            category=TypeCategory.ARRAY,
            schema_name=data.get("schema_name", "public"),
            created_at=data.get("created_at", str(uuid.uuid4())),
            element_type=TypeDefinition.from_dict(data["element_type"]),
            max_length=data.get("max_length"),
        )


@dataclass
class ReferenceTypeDefinition(TypeDefinition):
    """Definition for reference types (foreign key to another type)"""

    referenced_type: str = field(default="")
    referenced_schema: str = "public"

    def __post_init__(self):
        self.category = TypeCategory.REFERENCE

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data.update(
            {
                "referenced_type": self.referenced_type,
                "referenced_schema": self.referenced_schema,
            }
        )
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ReferenceTypeDefinition":
        return cls(
            name=data["name"],
            category=TypeCategory.REFERENCE,
            schema_name=data.get("schema_name", "public"),
            created_at=data.get("created_at", str(uuid.uuid4())),
            referenced_type=data["referenced_type"],
            referenced_schema=data.get("referenced_schema", "public"),
        )


@dataclass
class DomainTypeDefinition(TypeDefinition):
    """Definition for domain types (constrained primitive types)"""

    base_type: TypeDefinition = field(default=None)
    constraints: List[str] = field(default_factory=list)
    check_expression: Optional[str] = None

    def __post_init__(self):
        self.category = TypeCategory.DOMAIN

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data.update(
            {
                "base_type": self.base_type.to_dict(),
                "constraints": self.constraints,
                "check_expression": self.check_expression,
            }
        )
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DomainTypeDefinition":
        return cls(
            name=data["name"],
            category=TypeCategory.DOMAIN,
            schema_name=data.get("schema_name", "public"),
            created_at=data.get("created_at", str(uuid.uuid4())),
            base_type=TypeDefinition.from_dict(data["base_type"]),
            constraints=data.get("constraints", []),
            check_expression=data.get("check_expression"),
        )


class TypeRegistry:
    """Registry for managing user-defined types"""

    def __init__(self):
        self._types: Dict[str, TypeDefinition] = {}
        self._schema_types: Dict[str, Dict[str, TypeDefinition]] = {}
        self._initialize_builtin_types()

    def _initialize_builtin_types(self):
        """Initialize built-in primitive types"""
        for primitive in PrimitiveType:
            type_def = PrimitiveTypeDefinition(
                name=primitive.value,
                category=TypeCategory.PRIMITIVE,
                schema_name="system",
                primitive_type=primitive,
            )
            self._types[primitive.value] = type_def

    def register_type(self, type_def: TypeDefinition) -> None:
        """Register a new type definition"""
        full_name = f"{type_def.schema_name}.{type_def.name}"

        if full_name in self._types:
            raise ValueError(f"Type '{full_name}' already exists")

        # Validate type definition
        self._validate_type_definition(type_def)

        self._types[full_name] = type_def

        # Update schema index
        if type_def.schema_name not in self._schema_types:
            self._schema_types[type_def.schema_name] = {}
        self._schema_types[type_def.schema_name][type_def.name] = type_def

    def get_type(self, name: str, schema: str = "public") -> Optional[TypeDefinition]:
        """Get a type definition by name"""
        # Try with schema prefix first
        full_name = f"{schema}.{name}"
        if full_name in self._types:
            return self._types[full_name]

        # Try without schema (for built-in types)
        if name in self._types:
            return self._types[name]

        return None

    def drop_type(self, name: str, schema: str = "public") -> bool:
        """Drop a type definition"""
        full_name = f"{schema}.{name}"

        if full_name not in self._types:
            return False

        # Check for dependencies before dropping
        if self._has_dependencies(name, schema):
            raise ValueError(f"Cannot drop type '{full_name}': type has dependencies")

        del self._types[full_name]
        if schema in self._schema_types and name in self._schema_types[schema]:
            del self._schema_types[schema][name]

        return True

    def list_types(self, schema: str = None) -> List[TypeDefinition]:
        """List all types, optionally filtered by schema"""
        if schema:
            return list(self._schema_types.get(schema, {}).values())
        return [t for t in self._types.values() if not t.schema_name == "system"]

    def _validate_type_definition(self, type_def: TypeDefinition) -> None:
        """Validate a type definition for consistency"""
        if type_def.category == TypeCategory.COMPOSITE:
            comp_type = type_def
            for attr in comp_type.attributes:
                # Skip validation for primitive types
                if attr.type_def.category == TypeCategory.PRIMITIVE:
                    continue

                # Validate attribute type exists
                attr_type = self.get_type(attr.type_def.name, attr.type_def.schema_name)
                if not attr_type:
                    raise ValueError(
                        f"Unknown type '{attr.type_def.name}' for attribute '{attr.name}'"
                    )

                # Check for circular references
                if self._has_circular_reference(type_def, attr.type_def):
                    raise ValueError(
                        f"Circular reference detected in type '{type_def.name}'"
                    )

        elif type_def.category == TypeCategory.ARRAY:
            array_type = type_def
            # Validate element type exists
            elem_type = self.get_type(
                array_type.element_type.name, array_type.element_type.schema_name
            )
            if not elem_type:
                raise ValueError(
                    f"Unknown element type '{array_type.element_type.name}' for array type '{type_def.name}'"
                )

        elif type_def.category == TypeCategory.REFERENCE:
            ref_type = type_def
            # Validate referenced type exists
            referenced = self.get_type(
                ref_type.referenced_type, ref_type.referenced_schema
            )
            if not referenced:
                raise ValueError(
                    f"Unknown referenced type '{ref_type.referenced_type}' for reference type '{type_def.name}'"
                )

    def _has_circular_reference(
        self, type_def: TypeDefinition, check_type: TypeDefinition, visited: set = None
    ) -> bool:
        """Check for circular references in composite types"""
        if visited is None:
            visited = set()

        if check_type.name in visited:
            return True

        if check_type.category != TypeCategory.COMPOSITE:
            return False

        visited.add(check_type.name)

        comp_type = check_type
        for attr in comp_type.attributes:
            if attr.type_def.name == type_def.name:
                return True
            if self._has_circular_reference(type_def, attr.type_def, visited.copy()):
                return True

        return False

    def _has_dependencies(self, name: str, schema: str) -> bool:
        """Check if a type has dependencies (used by other types)"""
        full_name = f"{schema}.{name}"

        for type_def in self._types.values():
            if type_def.category == TypeCategory.COMPOSITE:
                comp_type = type_def
                for attr in comp_type.attributes:
                    if (
                        attr.type_def.name == name
                        and attr.type_def.schema_name == schema
                    ):
                        return True

            elif type_def.category == TypeCategory.ARRAY:
                array_type = type_def
                if (
                    array_type.element_type.name == name
                    and array_type.element_type.schema_name == schema
                ):
                    return True

            elif type_def.category == TypeCategory.REFERENCE:
                ref_type = type_def
                if (
                    ref_type.referenced_type == name
                    and ref_type.referenced_schema == schema
                ):
                    return True

        return False


class TypeValidator:
    """Validates values against type definitions"""

    def __init__(self, registry: TypeRegistry):
        self.registry = registry

    def validate_value(self, value: Any, type_def: TypeDefinition) -> bool:
        """Validate a value against a type definition"""
        if value is None:
            return True  # NULL values are handled separately

        if type_def.category == TypeCategory.PRIMITIVE:
            return self._validate_primitive(value, type_def)
        elif type_def.category == TypeCategory.COMPOSITE:
            return self._validate_composite(value, type_def)
        elif type_def.category == TypeCategory.ARRAY:
            return self._validate_array(value, type_def)
        elif type_def.category == TypeCategory.DOMAIN:
            return self._validate_domain(value, type_def)

        return False

    def _validate_primitive(
        self, value: Any, type_def: PrimitiveTypeDefinition
    ) -> bool:
        """Validate a primitive value"""
        primitive_type = type_def.primitive_type

        if primitive_type in [PrimitiveType.INTEGER, PrimitiveType.BIGINT]:
            return isinstance(value, int)
        elif primitive_type in [
            PrimitiveType.FLOAT,
            PrimitiveType.DOUBLE,
            PrimitiveType.DECIMAL,
        ]:
            return isinstance(value, (int, float))
        elif primitive_type in [
            PrimitiveType.VARCHAR,
            PrimitiveType.CHAR,
            PrimitiveType.TEXT,
        ]:
            if not isinstance(value, str):
                return False
            if type_def.length and len(value) > type_def.length:
                return False
            return True
        elif primitive_type == PrimitiveType.BOOLEAN:
            return isinstance(value, bool)
        elif primitive_type == PrimitiveType.JSON:
            try:
                json.dumps(value)
                return True
            except (TypeError, ValueError):
                return False

        return True

    def _validate_composite(
        self, value: Any, type_def: CompositeTypeDefinition
    ) -> bool:
        """Validate a composite value"""
        if not isinstance(value, dict):
            return False

        # Check all required attributes are present
        for attr in type_def.attributes:
            if not attr.nullable and attr.name not in value:
                return False

            if attr.name in value:
                if not self.validate_value(value[attr.name], attr.type_def):
                    return False

        return True

    def _validate_array(self, value: Any, type_def: ArrayTypeDefinition) -> bool:
        """Validate an array value"""
        if not isinstance(value, list):
            return False

        if type_def.max_length and len(value) > type_def.max_length:
            return False

        for item in value:
            if not self.validate_value(item, type_def.element_type):
                return False

        return True

    def _validate_domain(self, value: Any, type_def: DomainTypeDefinition) -> bool:
        """Validate a domain value"""
        # First validate against base type
        if not self.validate_value(value, type_def.base_type):
            return False

        # TODO: Implement constraint checking
        # This would require a constraint evaluation engine

        return True


# Global type registry instance
type_registry = TypeRegistry()
