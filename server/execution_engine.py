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

        # Handle case sensitivity for table names - PRESERVE ORIGINAL CASE
        tables = self.catalog_manager.list_tables(db_name)
        case_corrected_table = None

        # Try direct match first
        if table_name in tables:
            case_corrected_table = table_name
        else:
            # Try case-insensitive match but preserve database case
            for db_table in tables:
                if db_table.lower() == table_name.lower():
                    case_corrected_table = db_table  # Use database case, not uppercase
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

    def execute_visualize_bptree(self, plan):
        """Execute a VISUALIZE BPTREE command."""
        index_name = plan.get("index_name")
        table_name = plan.get("table")

        # Get current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {
                "error": "No database selected. Use 'USE database_name' first.",
                "status": "error",
            }

        # Validate that table and index are specified
        if not table_name or not index_name:
            return {
                "error": "Both table name and index name must be specified",
                "status": "error"
            }

        try:
            # Check if the index exists in the catalog
            indexes = self.catalog_manager.get_indexes_for_table(table_name)
            if index_name not in [idx.split('.')[-1] for idx in indexes.keys()]:
                return {
                    "error": f"Index '{index_name}' not found for table '{table_name}'",
                    "status": "error"
                }
            
            # Try to load the index file directly
            index_file_path = f"data/indexes/{db_name}_{table_name}_{index_name}.idx"
            
            # Check if index file exists
            if not os.path.exists(index_file_path):
                return {
                    "error": f"Index file not found: {index_file_path}",
                    "status": "error"
                }
            
            # Try to load the B+ tree from file
            try:
                from bptree_optimized import BPlusTreeOptimized
                from bptree import BPlusTree
                
                # Try to load as optimized B+ tree first
                try:
                    index_obj = BPlusTreeOptimized.load_from_file(index_file_path)
                    logging.info(f"Loaded optimized B+ tree from {index_file_path}")
                except:
                    # Fallback to regular B+ tree
                    index_obj = BPlusTree.load_from_file(index_file_path)
                    logging.info(f"Loaded regular B+ tree from {index_file_path}")
                    
            except Exception as load_error:
                logging.error(f"Failed to load index file {index_file_path}: {str(load_error)}")
                return {
                    "error": f"Failed to load index file: {str(load_error)}",
                    "status": "error"
                }

            # Generate visualization
            output_name = f"{table_name}_{index_name}_bptree"
            
            # Check if it's an optimized B+ tree
            if hasattr(index_obj, '__class__') and 'Optimized' in index_obj.__class__.__name__:
                visualization_path = self._visualize_optimized_bptree(index_obj, output_name)
            else:
                visualization_path = self.visualizer.visualize_tree(index_obj, output_name)
            
            if visualization_path:
                # Generate HTML content for web display
                html_content = self._generate_html_visualization(visualization_path, index_obj)
                
                return {
                    "message": f"B+ tree visualization generated successfully",
                    "visualization": html_content,
                    "visualization_path": visualization_path,
                    "status": "success"
                }
            else:
                return {
                    "error": "Failed to generate visualization",
                    "status": "error"
                }
                
        except Exception as e:
            logging.error(f"Error visualizing B+ tree: {str(e)}")
            return {
                "error": f"Error visualizing B+ tree: {str(e)}",
                "status": "error"
            }

    def execute_batch_insert(self, plan):
        """Execute a batch insert operation with optimizations."""
        table_name = plan.get("table")
        records = plan.get("records", [])

        if not records:
            return {"status": "error", "error": "No records to insert"}

        # Check for valid database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected", "status": "error"}

        # Get optimal batch size (increase default for better performance)
        batch_size = plan.get("batch_size", 5000)  # Increased from 1000 to 5000

        logging.info(f"Starting batch insert of {len(records)} records into {table_name} with batch size {batch_size}")

        try:
            # For large datasets, use the optimized batch insert directly
            if len(records) <= batch_size:
                # Single batch - use the optimized method directly
                result = self.catalog_manager.insert_records_batch(table_name, records)
                
                if result.get("status") == "success":
                    inserted_count = result.get("inserted_count", 0)
                    return {
                        "status": "success",
                        "message": f"Inserted {inserted_count} records into {table_name}",
                        "count": inserted_count,
                        "type": "batch_insert_result"
                    }
                else:
                    return result
            else:
                # Multiple batches for very large datasets
                total_inserted = 0
                batch_number = 0

                for i in range(0, len(records), batch_size):
                    batch_number += 1
                    current_batch = records[i:i+batch_size]
                    batch_start_time = time.time()

                    logging.info(f"Processing batch {batch_number} with {len(current_batch)} records")

                    # Insert the batch using the optimized method
                    result = self.catalog_manager.insert_records_batch(table_name, current_batch)

                    if isinstance(result, dict) and result.get("status") == "success":
                        batch_inserted = result.get("inserted_count", 0)
                        total_inserted += batch_inserted
                        batch_time = time.time() - batch_start_time
                        batch_rate = len(current_batch) / batch_time if batch_time > 0 else 0
                        logging.info(f"Batch {batch_number} completed in {batch_time:.2f}s ({batch_rate:.0f} inserts/sec)")
                    else:
                        logging.error(f"Batch {batch_number} failed: {result}")
                        return result

                return {
                    "status": "success",
                    "message": f"Inserted {total_inserted} records into {table_name}",
                    "count": total_inserted,
                    "type": "batch_insert_result"
                }

        except Exception as e:
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
    
    def execute_aggregate_with_groupby(self, plan):
        """Execute an aggregate query with GROUP BY clause."""
        table_name = plan.get("table")
        group_by_columns = plan.get("group_by", [])
        aggregate_columns = plan.get("columns", [])
        condition = plan.get("condition")

        error = check_database_selected(self.catalog_manager)
        if error:
            return error

        db_name = self.catalog_manager.get_current_database()

        # Handle case sensitivity for table names
        tables = self.catalog_manager.list_tables(db_name)
        case_corrected_table = None

        if table_name in tables:
            case_corrected_table = table_name
        else:
            for db_table in tables:
                if db_table.lower() == table_name.lower():
                    case_corrected_table = db_table
                    break

        if not case_corrected_table:
            return {"error": f"Table '{table_name}' does not exist", "status": "error"}

        # Get all records from the table
        try:
            all_records = self.catalog_manager.query_with_condition(
                case_corrected_table, [], ["*"]
            )
        except Exception as e:
            return {"error": f"Error querying table: {str(e)}", "status": "error"}

        if not all_records:
            return {"columns": [], "rows": [], "status": "success"}

        # Apply WHERE condition if present
        if condition:
            parsed_conditions = self.catalog_manager._parse_conditions(condition)
            filtered_records = []
            for record in all_records:
                if self.catalog_manager._record_matches_conditions(record, parsed_conditions):
                    filtered_records.append(record)
            all_records = filtered_records

        # Group records by the GROUP BY columns
        groups = {}
        for record in all_records:
            # Create group key from GROUP BY columns
            group_key_parts = []
            for col in group_by_columns:
                if col in record:
                    group_key_parts.append(str(record[col]))
                else:
                    group_key_parts.append('NULL')
            
            group_key = '|'.join(group_key_parts)
            
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(record)

        # Process aggregate functions for each group
        result_columns = []
        result_rows = []

        # Build column names for result
        for col in group_by_columns:
            result_columns.append(col)
        
        for agg_col in aggregate_columns:
            if agg_col not in group_by_columns:
                # Parse aggregate function and alias
                if ' as ' in agg_col.lower():
                    _, alias = agg_col.lower().split(' as ', 1)
                    result_columns.append(alias.strip())
                else:
                    result_columns.append(agg_col)

        # Calculate aggregates for each group
        for group_key, group_records in groups.items():
            row = []
            
            # Add GROUP BY column values
            group_values = group_key.split('|')
            for i, col in enumerate(group_by_columns):
                if i < len(group_values) and group_values[i] != 'NULL':
                    # Try to convert back to original type
                    val = group_values[i]
                    try:
                        if '.' in val:
                            val = float(val)
                        else:
                            val = int(val)
                    except ValueError:
                        pass  # Keep as string
                    row.append(val)
                else:
                    row.append(None)
            
            # Calculate aggregate values
            for agg_col in aggregate_columns:
                if agg_col in group_by_columns:
                    continue  # Already added above
                    
                # Parse the aggregate function
                agg_value = self._calculate_group_aggregate(agg_col, group_records)
                row.append(agg_value)
            
            result_rows.append(row)

        return {
            "columns": result_columns,
            "rows": result_rows,
            "status": "success",
            "type": "aggregate_result",
            "rowCount": len(result_rows)
        }

    def _calculate_group_aggregate(self, agg_expression, records):
        """Calculate aggregate value for a group of records."""
        import re
        
        # Parse aggregate function: FUNC(column) [as alias]
        agg_match = re.match(r'(\w+)\s*\(\s*([^)]+)\s*\)(?:\s+as\s+\w+)?', agg_expression.strip(), re.IGNORECASE)
        if not agg_match:
            return None
        
        func_name = agg_match.group(1).upper()
        column_expr = agg_match.group(2).strip()
        
        # Handle special cases
        if column_expr == '*':
            if func_name == 'COUNT':
                return len(records)
            else:
                return None
        
        # Get values from the specified column
        values = []
        for record in records:
            if column_expr in record and record[column_expr] is not None:
                try:
                    val = float(record[column_expr])
                    values.append(val)
                except (ValueError, TypeError):
                    if func_name == 'COUNT':
                        values.append(1)  # Count non-null values
        
        if not values:
            return 0 if func_name == 'COUNT' else None
        
        # Calculate aggregate
        if func_name == 'COUNT':
            return len(values)
        elif func_name == 'SUM':
            return sum(values)
        elif func_name == 'AVG':
            return sum(values) / len(values)
        elif func_name == 'MIN':
            return min(values)
        elif func_name == 'MAX':
            return max(values)
        else:
            return None

    def execute(self, plan):
        """Execute the query plan by dispatching to appropriate module."""
        try:
            # CRITICAL FIX: Validate plan is a dictionary
            if not isinstance(plan, dict):
                logging.error(f"ExecutionEngine: Plan is not a dict: {type(plan)} - {plan}")
                return {"error": "Plan must be a dictionary", "status": "error"}
            
            plan_type = plan.get("type", "UNKNOWN")
            
            logging.info(f"Executing plan type: {plan_type}")
            
            if plan_type == "SELECT":
                return self.select_executor.execute_select(plan)
            elif plan_type == "INSERT":
                return self.dml_executor.execute_insert(plan)
            elif plan_type == "SHOW":
                # FIX: Use schema_manager instead of catalog_manager for SHOW commands
                return self.schema_manager.execute_show(plan)
            elif plan_type == "UPDATE":
                return self.dml_executor.execute_update(plan)
            elif plan_type == "DELETE":
                return self.dml_executor.execute_delete(plan)
            elif plan_type == "CREATE_TABLE":
                return self.schema_manager.execute_create_table(plan)
            elif plan_type == "DROP_TABLE":
                return self.schema_manager.execute_drop_table(plan)
            elif plan_type == "CREATE_INDEX":
                return self.schema_manager.execute_create_index(plan)
            elif plan_type == "DROP_INDEX":
                return self.schema_manager.execute_drop_index(plan)
            elif plan_type == "CREATE_DATABASE":
                return self.schema_manager.execute_create_database(plan)
            elif plan_type == "USE_DATABASE":
                return self.schema_manager.execute_use_database(plan)
            elif plan_type == "SHOW_TABLES":
                return self.schema_manager.execute_show_tables(plan)
            elif plan_type == "SHOW_DATABASES":
                return self.schema_manager.execute_show_databases(plan)
            elif plan_type == "SHOW_COLUMNS":
                return self.schema_manager.execute_show_columns(plan)
            elif plan_type == "VISUALIZE":
                return self.visualizer.execute_visualization(plan)
            elif plan_type == "JOIN":
                return self.join_executor.execute_join(plan)
            elif plan_type == "AGGREGATE":
                return self.aggregate_executor.execute_aggregate(plan)
            elif plan_type == "AGGREGATE_GROUP":
                return self.execute_aggregate_with_groupby(plan)
            elif plan_type == "DISTINCT":
                return self.execute_distinct(plan)
            elif plan_type in ["UNION", "INTERSECT", "EXCEPT"]:
                return self.execute_set_operation(plan)
            elif plan_type == "BATCH_INSERT":
                return self.execute_batch_insert(plan)
            elif plan_type == "SCRIPT":
                return self.execute_script(plan)
            elif plan_type in ["BEGIN_TRANSACTION", "COMMIT_TRANSACTION", "ROLLBACK_TRANSACTION"]:
                return self.transaction_manager.execute_transaction(plan)
            else:
                return {"error": f"Unknown plan type: {plan_type}", "status": "error"}

        except Exception as e:
            logging.error(f"Error in execution engine: {str(e)}")
            logging.error(traceback.format_exc())
            return {"error": f"Error in execution engine: {str(e)}", "status": "error"}

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