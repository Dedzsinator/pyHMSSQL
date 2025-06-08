"""Procedure Manager for handling stored procedures.

This module manages the creation, storage, and execution of stored procedures
in the pyHMSSQL database system.
"""

import logging
import re
from typing import Dict, Any, List, Optional
from shared.utils import get_current_database_or_error


class ProcedureManager:
    """Handles stored procedure operations."""

    def __init__(self, catalog_manager, execution_engine=None):
        self.catalog_manager = catalog_manager
        self.execution_engine = execution_engine
        self.logger = logging.getLogger(__name__)

    def execute_procedure_operation(self, plan):
        """Execute procedure operations."""
        plan_type = plan.get("type")

        if plan_type == "CREATE_PROCEDURE":
            return self.execute_create_procedure(plan)
        elif plan_type == "DROP_PROCEDURE":
            return self.execute_drop_procedure(plan)
        elif plan_type == "CALL_PROCEDURE":
            return self.execute_call_procedure(plan)
        else:
            return {
                "error": f"Unsupported procedure operation: {plan_type}",
                "status": "error",
            }

    def execute_create_procedure(self, plan):
        """Execute CREATE PROCEDURE operation."""
        procedure_name = plan.get("procedure_name")
        parameters = plan.get("parameters", [])
        body = plan.get("body", "")

        if not procedure_name:
            return {
                "error": "No procedure name specified",
                "status": "error"
            }

        db_name, error = get_current_database_or_error(self.catalog_manager)
        if error:
            return error

        # Validate procedure body
        if not body.strip():
            return {
                "error": "Procedure body cannot be empty",
                "status": "error"
            }

        try:
            # Store procedure in catalog
            result = self.catalog_manager.create_procedure(
                procedure_name, parameters, body
            )

            if result is True:
                return {
                    "message": f"Procedure '{procedure_name}' created successfully",
                    "status": "success"
                }
            else:
                return {
                    "error": str(result),
                    "status": "error"
                }

        except Exception as e:
            self.logger.error(f"Error creating procedure: {str(e)}")
            return {
                "error": f"Error creating procedure: {str(e)}",
                "status": "error"
            }

    def execute_drop_procedure(self, plan):
        """Execute DROP PROCEDURE operation."""
        procedure_name = plan.get("procedure_name")

        if not procedure_name:
            return {
                "error": "No procedure name specified",
                "status": "error"
            }

        db_name, error = get_current_database_or_error(self.catalog_manager)
        if error:
            return error

        try:
            result = self.catalog_manager.drop_procedure(procedure_name)

            if result is True:
                return {
                    "message": f"Procedure '{procedure_name}' dropped successfully",
                    "status": "success"
                }
            else:
                return {
                    "error": str(result),
                    "status": "error"
                }

        except Exception as e:
            self.logger.error(f"Error dropping procedure: {str(e)}")
            return {
                "error": f"Error dropping procedure: {str(e)}",
                "status": "error"
            }

    def execute_call_procedure(self, plan):
        """Execute CALL procedure operation."""
        procedure_name = plan.get("procedure_name")
        arguments = plan.get("arguments", [])

        if not procedure_name:
            return {
                "error": "No procedure name specified",
                "status": "error"
            }

        db_name, error = get_current_database_or_error(self.catalog_manager)
        if error:
            return error

        try:
            # Get procedure definition
            procedure_def = self.catalog_manager.get_procedure(procedure_name)
            if not procedure_def:
                return {
                    "error": f"Procedure '{procedure_name}' does not exist",
                    "status": "error"
                }

            # Validate arguments
            expected_params = procedure_def.get("parameters", [])
            if len(arguments) != len(expected_params):
                return {
                    "error": f"Procedure '{procedure_name}' expects {len(expected_params)} arguments, got {len(arguments)}",
                    "status": "error"
                }

            # Execute the procedure body
            body = procedure_def.get("body", "")
            result = self._execute_procedure_body(body, arguments, expected_params)

            return {
                "message": f"Procedure '{procedure_name}' executed successfully",
                "status": "success",
                "result": result
            }

        except Exception as e:
            self.logger.error(f"Error calling procedure: {str(e)}")
            return {
                "error": f"Error calling procedure: {str(e)}",
                "status": "error"
            }

    def _execute_procedure_body(self, body: str, arguments: List[str], parameters: List[Dict]) -> Any:
        """Execute the procedure body with parameter substitution."""
        # Create parameter mapping
        param_map = {}
        for i, param in enumerate(parameters):
            param_name = param.get("name")
            if i < len(arguments):
                param_map[param_name] = arguments[i]

        # Simple parameter substitution
        processed_body = body
        for param_name, param_value in param_map.items():
            # Replace parameter placeholders with actual values
            # For string values, add quotes; for numeric values, use as-is
            if isinstance(param_value, str):
                # Escape single quotes in the string and wrap in quotes
                escaped_value = param_value.replace("'", "''")
                formatted_value = f"'{escaped_value}'"
            else:
                formatted_value = str(param_value)
            
            processed_body = processed_body.replace(f"@{param_name}", formatted_value)
            processed_body = processed_body.replace(f":{param_name}", str(param_value))

        # Execute each statement in the procedure body
        statements = self._split_statements(processed_body)
        results = []

        for statement in statements:
            if statement.strip():
                try:
                    # Use execution engine to execute the statement
                    if self.execution_engine:
                        # Parse and execute the statement
                        from sqlglot_parser import SQLGlotParser
                        parser = SQLGlotParser()
                        parsed = parser.parse(statement)
                        
                        if "error" not in parsed:
                            from planner import Planner
                            planner = Planner(self.catalog_manager, None)
                            plan = planner.plan_query(parsed)
                            result = self.execution_engine.execute(plan)
                            results.append(result)
                        else:
                            results.append(parsed)
                    else:
                        results.append({"error": "No execution engine available"})
                except Exception as e:
                    results.append({"error": f"Error executing statement: {str(e)}"})

        return results

    def _split_statements(self, body: str) -> List[str]:
        """Split procedure body into individual SQL statements."""
        # Simple statement splitting by semicolon
        statements = [stmt.strip() for stmt in body.split(';')]
        return [stmt for stmt in statements if stmt]

    def list_procedures(self) -> Dict[str, Any]:
        """List all procedures in the current database."""
        db_name, error = get_current_database_or_error(self.catalog_manager)
        if error:
            return error

        try:
            procedures = self.catalog_manager.list_procedures()
            return {
                "procedures": procedures,
                "status": "success"
            }
        except Exception as e:
            return {
                "error": f"Error listing procedures: {str(e)}",
                "status": "error"
            }

    # Convenience methods for easier testing and usage
    def create_procedure(self, plan):
        """Create a procedure (convenience method)."""
        return self.execute_create_procedure(plan)
    
    def drop_procedure(self, plan):
        """Drop a procedure (convenience method)."""
        return self.execute_drop_procedure(plan)
    
    def call_procedure(self, plan):
        """Call a procedure (convenience method)."""
        return self.execute_call_procedure(plan)
