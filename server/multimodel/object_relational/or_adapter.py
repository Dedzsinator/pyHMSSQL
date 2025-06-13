"""
Object-Relational Adapter for pyHMSSQL.

This module provides Object-Relational mapping capabilities:
- Composite/nested attributes (address.street, profile.age)
- Table inheritance (User < PremiumUser)
- Type system with proper indexing
- Object-oriented views over relational storage

Leverages existing B+ tree infrastructure and query optimization.
"""

import logging
import json
from typing import Dict, Any, List, Optional, Union
from ..unified.record_layout import UnifiedRecordLayout
from ..unified.type_system import (
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


class ObjectRelationalAdapter:
    """
    Object-Relational adapter that extends relational tables with OO features.

    Features:
    - Composite type definitions
    - Table inheritance hierarchies
    - Nested attribute queries (address.street)
    - Type-safe operations
    - Automatic schema evolution
    """

    def __init__(self, catalog_manager, index_manager):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager

        # Initialize type system
        self.type_registry = TypeRegistry()
        self.type_validator = TypeValidator(self.type_registry)

        # Initialize unified record layout
        self.record_layout = UnifiedRecordLayout()

        # Cache for type definitions
        self.type_cache = {}

        # Inheritance hierarchy cache
        self.inheritance_cache = {}

        logging.info("ObjectRelationalAdapter initialized")

    def execute_query(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an object-relational query."""
        query_type = plan.get("type", "").upper()

        try:
            if query_type == "CREATE_TYPE":
                # Handle both 'type_definition' and direct attributes
                type_def = plan.get("type_definition") or plan.get("attributes")
                type_name = plan.get("type_name")

                if not type_name:
                    return {
                        "error": "type_name is required for CREATE_TYPE",
                        "status": "error",
                    }

                if not type_def:
                    return {
                        "error": "type_definition or attributes are required for CREATE_TYPE",
                        "status": "error",
                    }

                # Convert array format to dict format if needed
                if isinstance(type_def, list):
                    attr_dict = {}
                    for attr in type_def:
                        if isinstance(attr, dict) and "name" in attr and "type" in attr:
                            attr_dict[attr["name"]] = attr["type"]
                        else:
                            return {
                                "error": f"Invalid attribute definition: {attr}",
                                "status": "error",
                            }
                    type_def = {"attributes": attr_dict}

                return self.create_type(type_name, type_def)
            elif query_type == "CREATE_TABLE_INHERITANCE":
                return self.create_inherited_table(plan)
            elif query_type == "SELECT_COMPOSITE":
                return self.select_with_composite_attributes(plan)
            elif query_type == "INSERT":
                return self.insert_with_composite_handling(plan)
            elif query_type == "UPDATE":
                return self.update_with_composite_handling(plan)
            else:
                # Try to handle as composite-aware relational query
                return self.execute_composite_aware_query(plan)

        except Exception as e:
            logging.error(f"Error in ObjectRelationalAdapter.execute_query: {str(e)}")
            return {
                "error": f"Object-relational query execution failed: {str(e)}",
                "status": "error",
            }

    def _parse_sql_type(self, type_str: str) -> tuple:
        """Parse SQL type string like 'varchar(100)' into base type and parameters"""
        import re

        # Match pattern like VARCHAR(100) or DECIMAL(10,2)
        match = re.match(r"(\w+)(?:\(([^)]+)\))?", type_str.strip(), re.IGNORECASE)
        if not match:
            return type_str.lower(), None, None, None

        base_type = match.group(1).lower()
        params = match.group(2)

        length = precision = scale = None

        if params:
            param_parts = [p.strip() for p in params.split(",")]
            if len(param_parts) == 1:
                # Single parameter (length)
                try:
                    length = int(param_parts[0])
                except ValueError:
                    pass
            elif len(param_parts) == 2:
                # Two parameters (precision, scale)
                try:
                    precision = int(param_parts[0])
                    scale = int(param_parts[1])
                except ValueError:
                    pass

        return base_type, length, precision, scale

    def _map_sql_type_to_primitive(self, sql_type: str) -> PrimitiveType:
        """Map SQL type names to PrimitiveType enum values"""
        sql_type = sql_type.lower()

        # Map common SQL type names to PrimitiveType enum values
        type_mapping = {
            "int": PrimitiveType.INTEGER,
            "integer": PrimitiveType.INTEGER,
            "bigint": PrimitiveType.BIGINT,
            "float": PrimitiveType.FLOAT,
            "real": PrimitiveType.FLOAT,
            "double": PrimitiveType.DOUBLE,
            "decimal": PrimitiveType.DECIMAL,
            "numeric": PrimitiveType.DECIMAL,
            "varchar": PrimitiveType.VARCHAR,
            "char": PrimitiveType.CHAR,
            "text": PrimitiveType.TEXT,
            "boolean": PrimitiveType.BOOLEAN,
            "bool": PrimitiveType.BOOLEAN,
            "date": PrimitiveType.DATE,
            "time": PrimitiveType.TIME,
            "timestamp": PrimitiveType.TIMESTAMP,
            "datetime": PrimitiveType.TIMESTAMP,
            "blob": PrimitiveType.BLOB,
            "json": PrimitiveType.JSON,
        }

        return type_mapping.get(sql_type, PrimitiveType.VARCHAR)

    def create_type(
        self, type_name: str, type_definition: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Create a new composite type definition.

        Args:
            type_name: Name of the new type
            type_definition: Type structure definition

        Returns:
            Creation result
        """
        try:
            # Validate inputs
            if not type_name:
                return {"error": "Type name cannot be empty", "status": "error"}

            if not type_definition:
                return {"error": "Type definition cannot be empty", "status": "error"}

            # Parse the type definition into TypeSystem format
            if "attributes" in type_definition:
                # Create composite type
                comp_type = CompositeTypeDefinition(
                    name=type_name,
                    category=TypeCategory.COMPOSITE,
                    schema_name=type_definition.get("schema", "public"),
                )

                # Handle both list and dict format for attributes
                attributes = type_definition["attributes"]
                if isinstance(attributes, list):
                    # Convert list format to dict format
                    attr_dict = {}
                    for attr in attributes:
                        if isinstance(attr, dict) and "name" in attr and "type" in attr:
                            # Store the type info, not the whole dict
                            attr_dict[attr["name"]] = attr["type"]
                        else:
                            raise ValueError(f"Invalid attribute definition: {attr}")
                    attributes = attr_dict
                elif not isinstance(attributes, dict):
                    raise ValueError("Attributes must be either a list or a dict")

                # Add attributes
                for attr_name, attr_def in attributes.items():
                    # Create attribute type definition
                    if isinstance(attr_def, str):
                        # Simple primitive type - parse SQL type string
                        base_type, length, precision, scale = self._parse_sql_type(
                            attr_def
                        )
                        primitive_type = self._map_sql_type_to_primitive(base_type)

                        attr_type = PrimitiveTypeDefinition(
                            name=base_type,  # Use base type, not full SQL string
                            category=TypeCategory.PRIMITIVE,
                            primitive_type=primitive_type,
                            length=length,
                            precision=precision,
                            scale=scale,
                        )
                    elif isinstance(attr_def, dict):
                        # Complex type definition
                        type_str = attr_def.get("type", "varchar")
                        base_type, length, precision, scale = self._parse_sql_type(
                            type_str
                        )
                        primitive_type = self._map_sql_type_to_primitive(base_type)

                        attr_type = PrimitiveTypeDefinition(
                            name=base_type,  # Use base type, not full SQL string
                            category=TypeCategory.PRIMITIVE,
                            primitive_type=primitive_type,
                            length=length or attr_def.get("length"),
                            precision=precision or attr_def.get("precision"),
                            scale=scale or attr_def.get("scale"),
                        )
                    else:
                        raise ValueError(
                            f"Invalid attribute definition for '{attr_name}'"
                        )

                    # Create type attribute
                    type_attr = TypeAttribute(
                        name=attr_name,
                        type_def=attr_type,
                        nullable=(
                            attr_def.get("nullable", True)
                            if isinstance(attr_def, dict)
                            else True
                        ),
                        default_value=(
                            attr_def.get("default")
                            if isinstance(attr_def, dict)
                            else None
                        ),
                    )

                    comp_type.add_attribute(type_attr)

                # Register the type
                self.type_registry.register_type(comp_type)

                # Cache the type definition
                self.type_cache[type_name] = comp_type

                # Store in catalog with OR metadata
                metadata = {
                    "model_type": "object_relational",
                    "type_definition": comp_type.to_dict(),
                    "composite_attributes": self._extract_composite_attributes_from_type(
                        comp_type
                    ),
                }

                # Use catalog manager to store type - convert to catalog format
                catalog_fields = []
                for attr_name, attr_def in attributes.items():
                    if isinstance(attr_def, str):
                        catalog_fields.append({"name": attr_name, "type": attr_def})
                    elif isinstance(attr_def, dict):
                        catalog_fields.append(
                            {"name": attr_name, "type": attr_def.get("type", "varchar")}
                        )

                self.catalog_manager.create_type(type_name, catalog_fields)

                return {
                    "message": f"Type '{type_name}' created successfully",
                    "status": "success",
                    "type": "create_type_result",
                }
            else:
                return {
                    "error": "Type definition must include 'attributes'",
                    "status": "error",
                }

        except Exception as e:
            logging.error(f"Error creating type {type_name}: {str(e)}")
            return {"error": f"Failed to create type: {str(e)}", "status": "error"}

    def create_inherited_table(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a table with inheritance from a parent table.

        Args:
            plan: Table creation plan with inheritance info

        Returns:
            Creation result
        """
        try:
            table_name = plan.get("table")
            parent_table = plan.get("inherits_from")
            additional_columns = plan.get("columns", [])

            # Get parent table schema
            parent_schema = self.catalog_manager.get_table_schema(parent_table)
            if not parent_schema:
                return {
                    "error": f"Parent table '{parent_table}' does not exist",
                    "status": "error",
                }

            # Combine parent columns with additional columns
            inherited_columns = self._merge_inherited_columns(
                parent_schema, additional_columns
            )

            # Create the table with full schema
            table_plan = {
                "type": "CREATE_TABLE",
                "table": table_name,
                "columns": inherited_columns,
            }

            result = self.catalog_manager.create_table(table_name, inherited_columns)

            if result:
                # Store inheritance metadata
                inheritance_info = {
                    "parent_table": parent_table,
                    "model_type": "object_relational",
                    "inheritance_type": "table_inheritance",
                }

                self.catalog_manager.store_table_metadata(table_name, inheritance_info)
                self.inheritance_cache[table_name] = parent_table

                return {
                    "message": f"Inherited table '{table_name}' created from '{parent_table}'",
                    "status": "success",
                    "type": "create_table_result",
                }
            else:
                return {"error": "Failed to create inherited table", "status": "error"}

        except Exception as e:
            logging.error(f"Error creating inherited table: {str(e)}")
            return {
                "error": f"Inherited table creation failed: {str(e)}",
                "status": "error",
            }

    def select_with_composite_attributes(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute SELECT query with composite attribute support.

        Handles queries like: SELECT address.street, profile.age FROM users
        """
        try:
            table_name = plan.get("table")
            columns = plan.get("columns", [])
            condition = plan.get("condition")

            # Parse composite attributes
            composite_columns, flat_columns = self._parse_composite_columns(columns)

            # If no composite attributes, use standard relational query
            if not composite_columns:
                return self._execute_relational_query(plan)

            # Get all records from table
            all_columns = ["*"]  # We need all data to extract composites
            records = self.catalog_manager.query_with_condition(
                table_name,
                self._parse_condition(condition) if condition else [],
                all_columns,
            )

            # Extract composite values from records
            result_rows = []
            for record in records:
                row = []

                # Add flat columns first
                for col in flat_columns:
                    row.append(record.get(col))

                # Add composite columns
                for comp_col in composite_columns:
                    comp_value = self._extract_composite_value(record, comp_col)
                    row.append(comp_value)

                result_rows.append(row)

            return {
                "columns": flat_columns + composite_columns,
                "rows": result_rows,
                "status": "success",
                "row_count": len(result_rows),
            }

        except Exception as e:
            logging.error(f"Error in composite SELECT: {str(e)}")
            return {"error": f"Composite SELECT failed: {str(e)}", "status": "error"}

    def insert_with_composite_handling(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Insert with composite type handling."""
        try:
            table_name = plan.get("table")
            values = plan.get("values", [])
            columns = plan.get("columns", [])

            # Process each row of values
            processed_records = []
            for row_values in values:
                if len(row_values) != len(columns):
                    return {
                        "error": "Column count doesn't match value count",
                        "status": "error",
                    }

                # Build record with composite handling
                record = {}
                for i, col in enumerate(columns):
                    value = row_values[i]

                    if "." in col:
                        # Composite attribute - store as JSON
                        self._set_composite_value(record, col, value)
                    else:
                        # Regular attribute
                        record[col] = value

                processed_records.append(record)

            # Insert processed records
            if len(processed_records) == 1:
                result = self.catalog_manager.insert_record(
                    table_name, processed_records[0]
                )
            else:
                result = self.catalog_manager.insert_records_batch(
                    table_name, processed_records
                )

            return result

        except Exception as e:
            logging.error(f"Error in composite INSERT: {str(e)}")
            return {"error": f"Composite INSERT failed: {str(e)}", "status": "error"}

    def update_with_composite_handling(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Update with composite type handling."""
        try:
            table_name = plan.get("table")
            updates = plan.get("set", {})
            condition = plan.get("condition")

            # Get records to update
            records = self.catalog_manager.query_with_condition(
                table_name, self._parse_condition(condition) if condition else [], ["*"]
            )

            updated_count = 0
            for record in records:
                record_id = record.get("id")  # Assume 'id' is primary key

                # Process updates with composite handling
                processed_updates = {}
                for col, value in updates.items():
                    if "." in col:
                        # Update composite attribute
                        current_data = record.copy()
                        self._set_composite_value(current_data, col, value)

                        # Extract the root attribute that was modified
                        root_attr = col.split(".")[0]
                        processed_updates[root_attr] = current_data.get(root_attr)
                    else:
                        processed_updates[col] = value

                # Update the record
                if self.catalog_manager.update_record(
                    table_name, record_id, processed_updates
                ):
                    updated_count += 1

            return {
                "status": "success",
                "message": f"Updated {updated_count} records with composite attributes",
                "count": updated_count,
            }

        except Exception as e:
            logging.error(f"Error in composite UPDATE: {str(e)}")
            return {"error": f"Composite UPDATE failed: {str(e)}", "status": "error"}

    def execute_composite_aware_query(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute any query with composite attribute awareness."""
        # Check if query involves composite attributes
        has_composites = False

        # Check columns
        columns = plan.get("columns", [])
        if isinstance(columns, list):
            has_composites = any("." in col for col in columns if isinstance(col, str))

        # Check conditions
        condition = plan.get("condition", "")
        if isinstance(condition, str) and "." in condition:
            has_composites = True

        if has_composites:
            # Handle as composite query
            if plan.get("type", "").upper() == "SELECT":
                return self.select_with_composite_attributes(plan)
            else:
                # For other query types, fall back to relational
                return self._execute_relational_query(plan)
        else:
            # No composites - use standard relational query
            return self._execute_relational_query(plan)

    def _execute_relational_query(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute query using standard relational engine."""
        # Import here to avoid circular imports
        from ...execution_engine import ExecutionEngine

        # Create a temporary execution engine or use existing one
        # For now, delegate to catalog manager's query methods
        if plan.get("type", "").upper() == "SELECT":
            table_name = plan.get("table")
            columns = plan.get("columns", ["*"])
            condition = plan.get("condition")

            records = self.catalog_manager.query_with_condition(
                table_name,
                self._parse_condition(condition) if condition else [],
                columns,
            )

            return {
                "columns": columns,
                "rows": records,
                "status": "success",
                "row_count": len(records),
            }
        else:
            # For other operations, return error for now
            return {
                "error": "Non-SELECT operations not implemented in OR adapter",
                "status": "error",
            }

    def _extract_composite_attributes_from_type(
        self, comp_type: CompositeTypeDefinition
    ) -> Dict[str, Any]:
        """Extract composite attributes information from a type definition"""
        attributes = {}
        for type_attr in comp_type.attributes:
            attr_info = {
                "name": type_attr.name,
                "type": type_attr.type_def.name if type_attr.type_def else "unknown",
                "nullable": type_attr.nullable,
                "default_value": type_attr.default_value,
            }
            if hasattr(type_attr.type_def, "primitive_type"):
                attr_info["primitive_type"] = type_attr.type_def.primitive_type.value
                attr_info["length"] = type_attr.type_def.length
                attr_info["precision"] = type_attr.type_def.precision
                attr_info["scale"] = type_attr.type_def.scale
            attributes[type_attr.name] = attr_info
        return attributes

    def _merge_inherited_columns(
        self, parent_schema: List[Dict], additional_columns: List[Dict]
    ) -> List[Dict]:
        """Merge parent table columns with additional columns for inheritance"""
        # Start with parent columns
        merged_columns = parent_schema.copy()

        # Add additional columns, checking for conflicts
        parent_column_names = {col["name"] for col in parent_schema if "name" in col}

        for new_col in additional_columns:
            if new_col.get("name") in parent_column_names:
                # Override parent column definition
                for i, existing_col in enumerate(merged_columns):
                    if existing_col.get("name") == new_col.get("name"):
                        merged_columns[i] = new_col
                        break
            else:
                # Add new column
                merged_columns.append(new_col)

        return merged_columns

    def _parse_composite_columns(self, columns: List[str]) -> tuple:
        """Parse columns into composite and flat lists."""
        composite_columns = []
        flat_columns = []

        for col in columns:
            if isinstance(col, str):
                if "." in col and not col.startswith("_"):
                    composite_columns.append(col)
                else:
                    flat_columns.append(col)
            else:
                flat_columns.append(col)

        return composite_columns, flat_columns

    def _extract_composite_value(
        self, record: Dict[str, Any], composite_col: str
    ) -> Any:
        """Extract value of a composite attribute from a record."""
        parts = composite_col.split(".")
        value = record

        try:
            for part in parts:
                if isinstance(value, str):
                    # Try to parse as JSON if it's a string
                    try:
                        value = json.loads(value)
                    except (json.JSONDecodeError, TypeError):
                        pass

                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    return None

            return value
        except Exception:
            return None

    def _set_composite_value(
        self, record: Dict[str, Any], composite_col: str, value: Any
    ):
        """Set value of a composite attribute in a record."""
        parts = composite_col.split(".")
        root_attr = parts[0]

        # Initialize root attribute if not exists
        if root_attr not in record:
            record[root_attr] = {}

        # Navigate to the nested location
        current = record[root_attr]
        if isinstance(current, str):
            try:
                current = json.loads(current)
                record[root_attr] = current
            except json.JSONDecodeError:
                current = {}
                record[root_attr] = current
        elif not isinstance(current, dict):
            current = {}
            record[root_attr] = current

        # Set the value at the nested location
        for part in parts[1:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]

        if len(parts) > 1:
            current[parts[-1]] = value

        # Convert back to JSON for storage
        if isinstance(record[root_attr], dict):
            record[root_attr] = json.dumps(record[root_attr])

    def _parse_condition(self, condition_str: str) -> List[Dict[str, Any]]:
        """Parse condition string into condition list."""
        if not condition_str:
            return []

        # Simple condition parsing - can be enhanced
        conditions = []

        # Handle composite conditions like "address.city = 'NYC'"
        if "." in condition_str and "=" in condition_str:
            # This is a simplified parser - in production you'd want more robust parsing
            parts = condition_str.split("=")
            if len(parts) == 2:
                col = parts[0].strip()
                val = parts[1].strip().strip("'\"")

                conditions.append({"column": col, "operator": "=", "value": val})

        return conditions
