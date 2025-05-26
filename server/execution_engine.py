"""_summary_

Returns:
    _type_: _description_
"""
import logging
import re
import os
import time
import traceback
from query_processor.join_executor import JoinExecutor
from query_processor.aggregate_executor import AggregateExecutor
from query_processor.select_executor import SelectExecutor
from query_processor.dml_executor import DMLExecutor
from ddl_processor.schema_manager import SchemaManager
from ddl_processor.view_manager import ViewManager
from transaction.transaction_manager import TransactionManager
from parsers.condition_parser import ConditionParser
from utils.visualizer import Visualizer
from utils.sql_helpers import parse_simple_condition, check_database_selected

class ExecutionEngine:
    """Main execution engine that coordinates between different modules"""

    def __init__(self, catalog_manager, index_manager):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager
        self.current_database = catalog_manager.get_current_database()
        self.preferences = self.catalog_manager.get_preferences()

        # Initialize all sub-modules
        self.join_executor = JoinExecutor(catalog_manager, index_manager)
        self.aggregate_executor = AggregateExecutor(catalog_manager)
        self.select_executor = SelectExecutor(
            catalog_manager, self.join_executor, self.aggregate_executor
        )

        # Set execution_engine reference only once (removing duplicate)
        self.select_executor.execution_engine = self

        self.dml_executor = DMLExecutor(catalog_manager, index_manager)
        self.schema_manager = SchemaManager(catalog_manager)
        self.view_manager = ViewManager(catalog_manager)
        self.transaction_manager = TransactionManager(catalog_manager)
        self.visualizer = Visualizer(catalog_manager, index_manager)

        # Share the condition parser with all modules that need it
        self.condition_parser = ConditionParser()

        # Set the condition parser for each executor that needs it
        self.select_executor.condition_parser = self.condition_parser
        self.dml_executor.condition_parser = self.condition_parser
        self.join_executor.condition_parser = self.condition_parser

    def execute_distinct(self, plan):
        """
        Execute a DISTINCT query.
        """
        table_name = plan["table"]
        column = plan["column"]

        error = check_database_selected(self.catalog_manager)
        if error:
            return error

        # Get the database name after confirming it exists
        db_name = self.catalog_manager.get_current_database()

        # Handle case sensitivity for table names
        tables = self.catalog_manager.list_tables(db_name)
        case_corrected_table = None

        # Try direct match first
        if table_name in tables:
            case_corrected_table = table_name
        else:
            # Try case-insensitive match
            for db_table in tables:
                if db_table.lower() == table_name.lower():
                    case_corrected_table = db_table
                    break

        # Verify the table exists
        if not case_corrected_table:
            return {"error": f"Table '{table_name}' does not exist", "status": "error"}

        # Use the case-corrected table name for the query
        actual_table_name = case_corrected_table

        # Use catalog manager to get data
        results = self.catalog_manager.query_with_condition(
            actual_table_name, [], [column])

        # Extract distinct values
        distinct_values = set()
        for record in results:
            if column in record and record[column] is not None:
                distinct_values.add(record[column])

        # Sort the distinct values for consistent output
        sorted_values = sorted(distinct_values, key=lambda x: (x is None, x))

        return {
            "columns": [column],
            "rows": [[value] for value in sorted_values],
            "status": "success",
        }

    def execute_create_index(self, plan):
        """Create an index on a table column"""
        # Forward to schema_manager with the correct parameters
        return self.schema_manager.execute_create_index(plan)

    def execute_drop_index(self, plan):
        """Drop an index"""
        # Forward to schema_manager with the correct parameters
        return self.schema_manager.execute_drop_index(plan)

    def execute_visualize_index(self, plan):
        """Visualize index structure"""
        index_name = plan.get("index_name")
        table_name = plan.get("table")

        # Use the index_manager directly instead of through schema_manager
        index_manager = self.index_manager

        if not index_manager:
            return {"error": "Index manager not available", "status": "error"}

        # If specific index is requested
        if index_name and table_name:
            full_index_name = f"{table_name}.{index_name}"
            index = index_manager.get_index(full_index_name)

            if not index:
                return {
                    "error": f"Index '{full_index_name}' not found",
                    "status": "error",
                }

            # Visualize the index
            try:
                index.visualize(self.visualizer, output_name=full_index_name)
                return {
                    "message": f"Visualized index '{index_name}' on table '{table_name}'",
                    "status": "success",
                }
            except RuntimeError as e:
                return {
                    "error": f"Error visualizing index: {str(e)}",
                    "status": "error",
                }
        else:
            # Visualize all indexes
            try:
                count = index_manager.visualize_all_indexes()
                return {"message": f"Visualized {count} indexes", "status": "success"}
            except RuntimeError as e:
                return {
                    "error": f"Error visualizing indexes: {str(e)}",
                    "status": "error",
                }

    def execute_batch_insert(self, plan):
        """Execute a batch insert operation."""
        table_name = plan.get("table")
        records = plan.get("records", [])

        if not records:
            return {"status": "error", "error": "No records to insert"}

        # Check for valid database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected", "status": "error"}

        # Get optimal batch size (default to 1000)
        batch_size = plan.get("batch_size", 1000)

        logging.info(f"Starting batch insert of {len(records)} records into {table_name} with batch size {batch_size}")

        try:
            # Start a batch operation in the catalog manager
            self.catalog_manager.begin_batch_operation()

            # Insert records in batches
            total_inserted = 0
            batch_number = 0

            for i in range(0, len(records), batch_size):
                batch_number += 1
                current_batch = records[i:i+batch_size]
                batch_start_time = time.time()

                logging.info(f"Processing batch {batch_number} with {len(current_batch)} records")

                # Insert the batch
                result = self.catalog_manager.insert_records_batch(table_name, current_batch)

                if isinstance(result, dict) and "error" in result:
                    # Rollback on error
                    self.catalog_manager.end_batch_operation(commit=False)
                    return result

                total_inserted += result
                batch_time = time.time() - batch_start_time
                batch_rate = len(current_batch) / batch_time if batch_time > 0 else 0

                logging.info(f"Batch {batch_number} completed in {batch_time:.2f}s ({batch_rate:.0f} inserts/sec)")

            # Commit all changes
            self.catalog_manager.end_batch_operation(commit=True)

            # Return success message
            return {
                "status": "success",
                "message": f"Inserted {total_inserted} records into {table_name}",
                "count": total_inserted,
                "type": "batch_insert_result"
            }

        except Exception as e:
            # Rollback on any exception
            self.catalog_manager.end_batch_operation(commit=False)
            logging.error(f"Error in batch insert: {str(e)}")
            logging.error(traceback.format_exc())
            return {"status": "error", "error": f"Error in batch insert: {str(e)}"}

    def execute_set_operation(self, plan):
        """Execute a set operation (UNION, INTERSECT, EXCEPT)."""
        operation = plan.get("type")
        left_plan = plan.get("left")
        right_plan = plan.get("right")

        logging.info(f"Executing {operation} operation")
        logging.info(f"Left plan: {left_plan}")
        logging.info(f"Right plan: {right_plan}")

        # Execute both sides
        left_result = self.execute(left_plan)
        right_result = self.execute(right_plan)

        # Check for errors
        if left_result.get("status") == "error":
            return left_result
        if right_result.get("status") == "error":
            return right_result

        logging.info(f"Left result: {left_result}")
        logging.info(f"Right result: {right_result}")

        # Get column names from the left result
        columns = left_result.get("columns", [])

        # Get rows data - handle different result formats
        left_rows = []
        right_rows = []

        # Extract rows from data or rows format
        if "data" in left_result:
            left_data = left_result["data"]
            left_rows = [list(row.values()) for row in left_data]
        elif "rows" in left_result:
            left_rows = left_result["rows"]

        if "data" in right_result:
            right_data = right_result["data"]
            right_rows = [list(row.values()) for row in right_data]
        elif "rows" in right_result:
            right_rows = right_result["rows"]

        # Perform the set operation
        result_rows = []

        if operation == "UNION":
            # Use a set to eliminate duplicates (convert rows to tuples for hashing)
            seen = set()
            for row in left_rows + right_rows:
                row_tuple = tuple(row)
                if row_tuple not in seen:
                    seen.add(row_tuple)
                    result_rows.append(row)

        elif operation == "INTERSECT":
            # Find rows that exist in both sets
            left_tuples = [tuple(row) for row in left_rows]
            right_tuples = [tuple(row) for row in right_rows]
            common_tuples = set(left_tuples).intersection(set(right_tuples))

            # Convert back to rows
            for tuple_row in common_tuples:
                result_rows.append(list(tuple_row))

        elif operation == "EXCEPT":
            # Find rows in left that aren't in right
            left_tuples = [tuple(row) for row in left_rows]
            right_tuples = [tuple(row) for row in right_rows]
            diff_tuples = set(left_tuples).difference(set(right_tuples))

            # Convert back to rows
            for tuple_row in diff_tuples:
                result_rows.append(list(tuple_row))

        # Return in the expected format for SELECT results
        return {
            "columns": columns,
            "rows": result_rows,
            "status": "success",
            "type": f"{operation.lower()}_result",
            "rowCount": len(result_rows)
        }

    def _has_subquery(self, condition):
        """Check if a condition contains a subquery."""
        if isinstance(condition, dict):
            if "subquery" in condition:
                return True
            for value in condition.values():
                if isinstance(value, dict) and self._has_subquery(value):
                    return True
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict) and self._has_subquery(item):
                            return True
        return False

    def _resolve_subqueries(self, condition):
        """Resolve any subqueries in the condition and replace with results."""
        if not isinstance(condition, dict):
            return

        if "subquery" in condition:
            # Execute the subquery
            subquery_plan = condition["subquery"]

            # IMPORTANT: Fix table case in subquery plan
            if "tables" in subquery_plan and subquery_plan["tables"]:
                db_name = self.catalog_manager.get_current_database()
                tables = self.catalog_manager.list_tables(db_name)
                tables_lower = {table.lower(): table for table in tables}

                # Fix table case sensitivity issues
                for i, table in enumerate(subquery_plan["tables"]):
                    if table.lower() in tables_lower:
                        subquery_plan["tables"][i] = tables_lower[table.lower()]
                        if "table" in subquery_plan and subquery_plan["table"].lower() == table.lower():
                            subquery_plan["table"] = tables_lower[table.lower()]

            # Log subquery execution
            logging.info(f"Executing subquery: {subquery_plan}")

            # Execute the subquery to get results
            subquery_result = self.execute(subquery_plan)

            # Log the result for debugging
            logging.info(f"Subquery result: {subquery_result}")

            # Extract values from the result
            if subquery_result.get("status") == "success":
                # For IN conditions, we need a list of values
                if condition.get("operator") == "IN":
                    values = []

                    # Handle different result formats
                    if "rows" in subquery_result and subquery_result["rows"]:
                        # Rows format (list of lists)
                        for row in subquery_result["rows"]:
                            if row and len(row) > 0:
                                values.append(row[0])  # Take first column

                    # Check for columns/data format
                    elif "data" in subquery_result and subquery_result["data"]:
                        # Extract column name from first record
                        if len(subquery_result["data"]) > 0:
                            first_record = subquery_result["data"][0]
                            if first_record and len(first_record) > 0:
                                # Get the first column name
                                first_col = list(first_record.keys())[0]
                                # Extract values
                                values = [record[first_col] for record in subquery_result["data"]]

                    # Alternative format for select_result
                    elif "columns" in subquery_result and "rows" in subquery_result:
                        for row in subquery_result["rows"]:
                            if row and len(row) > 0:
                                values.append(row[0])  # Take first column

                    # Log extracted values
                    logging.info(f"Extracted subquery values: {values}")

                    # Replace subquery with values
                    condition["values"] = values
                    condition.pop("subquery")  # Remove the subquery

        # Recursively process nested conditions
        for key, value in condition.items():
            if isinstance(value, dict):
                self._resolve_subqueries(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        self._resolve_subqueries(item)

    def _evaluate_condition(self, record, condition):
        """
        Evaluate a condition against a record.

        Args:
            record: The record to check
            condition: Condition dictionary with operator, column, and value

        Returns:
            bool: True if the record matches the condition
        """
        if not condition:
            return True

        if condition.get("operator") == "AND":
            return all(self._evaluate_condition(record, operand)
                    for operand in condition.get("operands", []))

        if condition.get("operator") == "OR":
            return any(self._evaluate_condition(record, operand)
                    for operand in condition.get("operands", []))

        column = condition.get("column")
        operator = condition.get("operator")
        value = condition.get("value")

        if not column or not operator or column not in record:
            return False

        record_value = record[column]

        # Convert string values to numeric for comparison if needed
        try:
            # Convert to numeric if both values are numeric strings
            if isinstance(value, str) and value.replace('.', '', 1).replace('-', '', 1).isdigit():
                value = float(value) if '.' in value else int(value)

            if isinstance(record_value, str) and record_value.replace('.', '', 1).replace('-', '', 1).isdigit():
                record_value = float(record_value) if '.' in record_value else int(record_value)
        except (ValueError, TypeError):
            # If conversion fails, keep original values
            pass

        # Apply operator
        if operator == "=":
            return record_value == value
        elif operator == "!=":
            return record_value != value
        elif operator == ">":
            return record_value > value
        elif operator == ">=":
            return record_value >= value
        elif operator == "<":
            return record_value < value
        elif operator == "<=":
            return record_value <= value
        elif operator.upper() == "LIKE":
            if not isinstance(record_value, str) or not isinstance(value, str):
                return False
            pattern = value.replace("%", ".*").replace("_", ".")
            return bool(re.match(f"^{pattern}$", record_value))
        elif operator.upper() == "IN":
            return record_value in value if isinstance(value, list) else False

        return False

    def execute(self, plan):
        """Execute the query plan by dispatching to appropriate module."""
        plan_type = plan.get("type", "UNKNOWN")
        
        logging.info(f"Executing plan type: {plan_type}")
        
        try:
            result = None

            if plan_type == "SCRIPT":
                result = self.execute_script(plan)
            elif plan_type == "SELECT":
                result = self.select_executor.execute_select(plan)
            elif plan_type == "INSERT":
                result = self.dml_executor.execute_insert(plan)
            elif plan_type == "UPDATE":
                result = self.dml_executor.execute_update(plan)
            elif plan_type == "DELETE":
                result = self.dml_executor.execute_delete(plan)
            elif plan_type == "CREATE_TABLE":
                result = self.schema_manager.create_table(plan)
            elif plan_type == "DROP_TABLE":
                result = self.schema_manager.drop_table(plan)
            elif plan_type == "CREATE_DATABASE":
                result = self.schema_manager.create_database(plan)
            elif plan_type == "DROP_DATABASE":
                result = self.schema_manager.drop_database(plan)
            elif plan_type == "USE_DATABASE":
                result = self.schema_manager.use_database(plan)
            elif plan_type == "SHOW":
                result = self.schema_manager.execute_show(plan)  # Make sure this is called
            elif plan_type == "SHOW_TABLES":
                result = self.schema_manager.show_tables(plan)
            elif plan_type == "SHOW_COLUMNS":
                result = self.schema_manager.show_columns(plan)
            elif plan_type == "CREATE_INDEX":
                result = self.execute_create_index(plan)
            elif plan_type == "DROP_INDEX":
                result = self.execute_drop_index(plan)
            elif plan_type == "DISTINCT":
                result = self.execute_distinct(plan)
            elif plan_type in ["UNION", "INTERSECT", "EXCEPT"]:
                result = self.select_executor.execute_set_operation(plan)
            elif plan_type == "VISUALIZE":
                result = self.visualizer.execute_visualization(plan)
            elif plan_type == "TRANSACTION":
                result = self.transaction_manager.execute_transaction(plan)
            else:
                result = {"error": f"Unsupported operation type: {plan_type}", "status": "error"}

            return result

        except Exception as e:
            logging.error(f"Error in execution engine: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"error": f"Execution error: {str(e)}", "status": "error"}

    def execute_set_preference(self, plan):
        """Update user preferences."""
        preference = plan["preference"]
        value = plan["value"]
        user_id = plan.get("user_id")

        # Update preferences
        self.preferences[preference] = value
        self.catalog_manager.update_preferences({preference: value}, user_id)

        return {
            "message": f"Preference '{preference}' set to '{value}'.",
            "status": "success",
        }

    def execute_script(self, plan):
        """Execute a SQL script file."""
        import os  # Add this import
        
        filename = plan.get("filename")
        if not filename:
            return {"error": "No filename specified for SCRIPT command", "status": "error"}
        
        try:
            # First, try the filename as-is (relative to server directory)
            script_paths_to_try = [
                filename,
                os.path.join("../client", filename),  # Try client directory
                os.path.join("client", filename),     # Try client directory (alternative path)
                os.path.abspath(filename)             # Try absolute path
            ]
            
            script_content = None
            used_path = None
            
            for path in script_paths_to_try:
                if os.path.exists(path):
                    with open(path, 'r', encoding='utf-8') as f:
                        script_content = f.read()
                    used_path = path
                    break
            
            if script_content is None:
                return {"error": f"Script file '{filename}' not found in any of the expected locations", "status": "error"}
            
            logging.info(f"Executing script from: {used_path}")
            
            # Split into individual statements (split by semicolon)
            statements = [stmt.strip() for stmt in script_content.split(';') if stmt.strip()]
            
            results = []
            successful_count = 0
            failed_count = 0
            
            for i, statement in enumerate(statements):
                if statement:
                    logging.info(f"Executing statement {i+1}: {statement[:100]}...")  # Log first 100 chars
                    
                    try:
                        # Parse and execute each statement
                        from parser import SQLParser
                        parser = SQLParser()
                        parsed_stmt = parser.parse_sql(statement)
                        
                        if "error" in parsed_stmt:
                            results.append(f"Error in statement {i+1}: {parsed_stmt['error']}")
                            failed_count += 1
                            continue
                        
                        # Plan and execute the statement
                        from planner import Planner
                        planner = Planner(self.catalog_manager, self.index_manager)
                        plan = planner.plan_query(parsed_stmt)
                        
                        if "error" in plan:
                            results.append(f"Error planning statement {i+1}: {plan['error']}")
                            failed_count += 1
                            continue
                        
                        # Execute the planned statement
                        result = self.execute(plan)
                        if isinstance(result, dict) and result.get("status") == "error":
                            results.append(f"Error executing statement {i+1}: {result.get('error', 'Unknown error')}")
                            failed_count += 1
                        else:
                            results.append(f"Statement {i+1} executed successfully")
                            successful_count += 1
                            
                    except Exception as e:
                        results.append(f"Exception in statement {i+1}: {str(e)}")
                        failed_count += 1
                        logging.error(f"Error executing statement {i+1}: {str(e)}")
            
            return {
                "message": f"Script '{filename}' executed from {used_path}",
                "total_statements": len(statements),
                "successful": successful_count,
                "failed": failed_count,
                "results": results,
                "status": "success"
            }
            
        except Exception as e:
            logging.error(f"Error executing script: {str(e)}")
            return {"error": f"Error executing script: {str(e)}", "status": "error"}