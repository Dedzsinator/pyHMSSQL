"""Trigger Manager for handling database triggers.

This module manages the creation, storage, and execution of triggers
in the pyHMSSQL database system.
"""

import logging
from typing import Dict, Any, List, Optional
from shared.utils import get_current_database_or_error


class TriggerManager:
    """Handles database trigger operations."""

    def __init__(self, catalog_manager, execution_engine=None):
        self.catalog_manager = catalog_manager
        self.execution_engine = execution_engine
        self.logger = logging.getLogger(__name__)

    def execute_trigger_operation(self, plan):
        """Execute trigger operations."""
        plan_type = plan.get("type")

        if plan_type == "CREATE_TRIGGER":
            return self.execute_create_trigger(plan)
        elif plan_type == "DROP_TRIGGER":
            return self.execute_drop_trigger(plan)
        else:
            return {
                "error": f"Unsupported trigger operation: {plan_type}",
                "status": "error",
            }

    def execute_create_trigger(self, plan):
        """Execute CREATE TRIGGER operation."""
        trigger_name = plan.get("trigger_name")
        timing = plan.get("timing")  # BEFORE or AFTER
        event = plan.get("event")    # INSERT, UPDATE, or DELETE
        table = plan.get("table")
        body = plan.get("body", "")

        if not trigger_name:
            return {
                "error": "No trigger name specified",
                "status": "error"
            }

        if not table:
            return {
                "error": "No table specified for trigger",
                "status": "error"
            }

        db_name, error = get_current_database_or_error(self.catalog_manager)
        if error:
            return error

        # Validate timing and event
        if timing not in ["BEFORE", "AFTER"]:
            return {
                "error": "Trigger timing must be BEFORE or AFTER",
                "status": "error"
            }

        if event not in ["INSERT", "UPDATE", "DELETE"]:
            return {
                "error": "Trigger event must be INSERT, UPDATE, or DELETE",
                "status": "error"
            }

        # Validate table exists
        tables = self.catalog_manager.list_tables(db_name)
        if table not in tables:
            return {
                "error": f"Table '{table}' does not exist",
                "status": "error"
            }

        # Validate trigger body
        if not body.strip():
            return {
                "error": "Trigger body cannot be empty",
                "status": "error"
            }

        try:
            # Store trigger in catalog
            result = self.catalog_manager.create_trigger(
                trigger_name, timing, event, table, body
            )

            if result is True:
                return {
                    "message": f"Trigger '{trigger_name}' created successfully",
                    "status": "success"
                }
            else:
                return {
                    "error": str(result),
                    "status": "error"
                }

        except Exception as e:
            self.logger.error(f"Error creating trigger: {str(e)}")
            return {
                "error": f"Error creating trigger: {str(e)}",
                "status": "error"
            }

    def execute_drop_trigger(self, plan):
        """Execute DROP TRIGGER operation."""
        trigger_name = plan.get("trigger_name")

        if not trigger_name:
            return {
                "error": "No trigger name specified",
                "status": "error"
            }

        db_name, error = get_current_database_or_error(self.catalog_manager)
        if error:
            return error

        try:
            result = self.catalog_manager.drop_trigger(trigger_name)

            if result is True:
                return {
                    "message": f"Trigger '{trigger_name}' dropped successfully",
                    "status": "success"
                }
            else:
                return {
                    "error": str(result),
                    "status": "error"
                }

        except Exception as e:
            self.logger.error(f"Error dropping trigger: {str(e)}")
            return {
                "error": f"Error dropping trigger: {str(e)}",
                "status": "error"
            }

    def fire_triggers(self, event: str, table: str, timing: str, old_data: Dict = None, new_data: Dict = None):
        """Fire triggers for a specific event on a table."""
        try:
            db_name = self.catalog_manager.get_current_database()
            if not db_name:
                return

            # Get triggers for this table and event
            triggers = self.catalog_manager.get_triggers_for_table(table)
            
            for trigger_name, trigger_def in triggers.items():
                if (trigger_def.get("event") == event.upper() and 
                    trigger_def.get("timing") == timing.upper()):
                    
                    self._execute_trigger(trigger_name, trigger_def, old_data, new_data)

        except Exception as e:
            self.logger.error(f"Error firing triggers: {str(e)}")

    def _execute_trigger(self, trigger_name: str, trigger_def: Dict, old_data: Dict = None, new_data: Dict = None):
        """Execute a specific trigger."""
        try:
            body = trigger_def.get("body", "")
            if not body:
                return

            # Simple trigger body execution
            # In a real implementation, you'd have OLD and NEW record references
            processed_body = body
            
            # Replace OLD and NEW references with actual data
            if old_data:
                for column, value in old_data.items():
                    processed_body = processed_body.replace(f"OLD.{column}", str(value))
            
            if new_data:
                for column, value in new_data.items():
                    processed_body = processed_body.replace(f"NEW.{column}", str(value))

            # Execute trigger statements
            statements = self._split_statements(processed_body)
            
            for statement in statements:
                if statement.strip():
                    try:
                        # Execute the statement using the execution engine
                        if self.execution_engine:
                            from sqlglot_parser import SQLGlotParser
                            parser = SQLGlotParser()
                            parsed = parser.parse(statement)
                            
                            if "error" not in parsed:
                                from planner import Planner
                                planner = Planner(self.catalog_manager, None)
                                plan = planner.plan_query(parsed)
                                self.execution_engine.execute(plan)
                    except Exception as e:
                        self.logger.error(f"Error executing trigger statement: {str(e)}")

        except Exception as e:
            self.logger.error(f"Error executing trigger '{trigger_name}': {str(e)}")

    def _split_statements(self, body: str) -> List[str]:
        """Split trigger body into individual SQL statements."""
        # Simple statement splitting by semicolon
        statements = [stmt.strip() for stmt in body.split(';')]
        return [stmt for stmt in statements if stmt]

    def list_triggers(self, table: str = None) -> Dict[str, Any]:
        """List triggers in the current database, optionally filtered by table."""
        db_name, error = get_current_database_or_error(self.catalog_manager)
        if error:
            return error

        try:
            if table:
                triggers = self.catalog_manager.get_triggers_for_table(table)
            else:
                triggers = self.catalog_manager.list_triggers()
            
            return {
                "triggers": triggers,
                "status": "success"
            }
        except Exception as e:
            return {
                "error": f"Error listing triggers: {str(e)}",
                "status": "error"
            }

    # Convenience methods for ExecutionEngine compatibility
    def create_trigger(self, plan):
        """Convenience method for creating triggers."""
        return self.execute_create_trigger(plan)

    def drop_trigger(self, plan):
        """Convenience method for dropping triggers."""
        return self.execute_drop_trigger(plan)

    def get_trigger_definition(self, trigger_name: str) -> Optional[Dict[str, Any]]:
        """Get trigger definition by name."""
        try:
            return self.catalog_manager.get_trigger(trigger_name)
        except Exception as e:
            self.logger.error(f"Error getting trigger definition: {str(e)}")
            return None
