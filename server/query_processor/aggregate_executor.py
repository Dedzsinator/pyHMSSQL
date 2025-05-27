"""_summary_

Raises:
    ValueError: _description_


Returns:
    _type_: _description_
"""

import logging
import traceback
import math
import random
from parsers.condition_parser import ConditionParser


class AggregateExecutor:
    """_summary_"""

    def __init__(self, catalog_manager):
        self.catalog_manager = catalog_manager

    def _execute_avg(self, records, actual_column, original_column):
        """Execute AVG aggregate function with better error handling."""
        if not actual_column:
            raise ValueError(f"Column '{original_column}' not found")
        
        # Get numeric values from the column
        values = []
        for record in records:
            if isinstance(record, dict) and actual_column in record:
                value = record[actual_column]
                if value is not None:
                    try:
                        # Convert to numeric
                        if isinstance(value, str):
                            value = float(value) if '.' in value else int(value)
                        values.append(float(value))
                    except (ValueError, TypeError):
                        # Skip non-numeric values
                        continue
        
        if not values:
            return {
                "columns": [f"AVG({original_column})"],
                "rows": [[None]],
                "status": "success"
            }
        
        avg_value = sum(values) / len(values)
        
        return {
            "columns": [f"AVG({original_column})"],
            "rows": [[avg_value]],
            "status": "success"
        }

    def _execute_count(self, records, actual_column, original_column):
        """Execute COUNT aggregate function."""
        if original_column == "*":
            count = len(records)
        else:
            if not actual_column:
                raise ValueError(f"Column '{original_column}' not found")
            
            count = 0
            for record in records:
                if isinstance(record, dict) and actual_column in record and record[actual_column] is not None:
                    count += 1
        
        return {
            "columns": [f"COUNT({original_column})"],
            "rows": [[count]],
            "status": "success"
        }

    def _execute_sum(self, records, actual_column, original_column):
        """Execute SUM aggregate function."""
        if not actual_column:
            raise ValueError(f"Column '{original_column}' not found")
        
        total = 0
        for record in records:
            if isinstance(record, dict) and actual_column in record:
                value = record[actual_column]
                if value is not None:
                    try:
                        if isinstance(value, str):
                            value = float(value) if '.' in value else int(value)
                        total += float(value)
                    except (ValueError, TypeError):
                        continue
        
        return {
            "columns": [f"SUM({original_column})"],
            "rows": [[total]],
            "status": "success"
        }

    def _execute_min(self, records, actual_column, original_column):
        """Execute MIN aggregate function."""
        if not actual_column:
            raise ValueError(f"Column '{original_column}' not found")
        
        values = []
        for record in records:
            if isinstance(record, dict) and actual_column in record:
                value = record[actual_column]
                if value is not None:
                    values.append(value)
        
        if not values:
            min_value = None
        else:
            min_value = min(values)
        
        return {
            "columns": [f"MIN({original_column})"],
            "rows": [[min_value]],
            "status": "success"
        }

    def _execute_max(self, records, actual_column, original_column):
        """Execute MAX aggregate function."""
        if not actual_column:
            raise ValueError(f"Column '{original_column}' not found")
        
        values = []
        for record in records:
            if isinstance(record, dict) and actual_column in record:
                value = record[actual_column]
                if value is not None:
                    values.append(value)
        
        if not values:
            max_value = None
        else:
            max_value = max(values)
        
        return {
            "columns": [f"MAX({original_column})"],
            "rows": [[max_value]],
            "status": "success"
        }

    def _execute_rand(self, result, param_str):
        """Execute RAND aggregate function."""
        logging.info("Executing RAND with params: '%s'", param_str)

        # Parse parameters
        params = [p.strip() for p in param_str.split(",")]

        try:
            if len(params) == 1:
                n = int(params[0])
                if n <= 0:
                    raise ValueError("Number of random records must be positive")

                if result and len(result) > 0:
                    n = min(n, len(result))
                    logging.info(
                        "Selecting %s random records from %s total",
                        n, len(result)
                    )
                    selected_indices = random.sample(range(len(result)), n)
                    # Return the actual selected records, not just the count
                    return [result[i] for i in selected_indices]
                else:
                    logging.warning("No records to sample from")
                    return []

            elif len(params) == 3:
                n = int(params[0])
                min_val = float(params[1])
                max_val = float(params[2])

                logging.info(
                    "Generating %s random values between %s and %s",
                    n, min_val, max_val
                )
                if n <= 0:
                    raise ValueError(
                        "Number of random values must be positive")

                random_values = [random.uniform(
                    min_val, max_val) for _ in range(n)]
                avg = sum(random_values) / len(random_values)
                logging.info("Generated values with average: %s", avg)
                return avg

            else:
                raise ValueError(
                    "RAND requires either 1 parameter (n) or 3 parameters (n,min,max)"
                )

        except ValueError as e:
            logging.error("Error in RAND: %s", str(e))

    def _execute_gcd(self, result, actual_column, column):
        """Execute GCD aggregate function."""

        logging.info("Computing GCD for column '%s' (actual: '%s')",
            column, actual_column
        )

        if column == "*":
            raise ValueError("Cannot use GCD with *")
        if not actual_column:
            raise ValueError(f"Column '{column}' not found")

        # Collect integer values
        values = []
        for record in result:
            if actual_column in record and record[actual_column] is not None:
                try:
                    val = int(float(record[actual_column]))
                    if val != 0:  # Skip zeros
                        values.append(abs(val))
                        logging.info("Added value %s to GCD calculation", val)
                except (ValueError, TypeError):
                    logging.warning(
                        "Skipping non-numeric value: %s",
                        record[actual_column]
                    )

        if not values:
            logging.warning("No valid values found for GCD calculation")
            return None

        gcd_result = values[0]
        for value in values[1:]:
            gcd_result = math.gcd(gcd_result, value)

        logging.info("GCD result: %s", gcd_result)
        return gcd_result

    def execute_aggregate(self, plan):
        """Execute aggregate functions with improved table name handling."""
        function = plan.get("function")
        column = plan.get("column")
        table_name = plan.get("table")
        condition = plan.get("condition")
        top = plan.get("top")
        limit = plan.get("limit")

        logging.info("===== AGGREGATE FUNCTION CALLED =====")
        logging.info("Plan: %s", plan)
        logging.info("Function: %s", function)
        logging.info("Column: %s", column)
        logging.info("Table: %s", table_name)
        logging.info("Condition: %s", condition)
        logging.info("Top: %s", top)
        logging.info("Limit: %s", limit)

        # CRITICAL FIX: Handle case-insensitive table name lookup
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected", "status": "error"}

        # Get actual table name with correct case
        tables = self.catalog_manager.list_tables(db_name)
        actual_table_name = None
        
        # Try exact match first
        if table_name in tables:
            actual_table_name = table_name
        else:
            # Try case-insensitive match
            for db_table in tables:
                if db_table.lower() == table_name.lower():
                    actual_table_name = db_table
                    break
        
        if not actual_table_name:
            logging.error("Table '%s' not found in database '%s'", table_name, db_name)
            return {"error": f"Table '{table_name}' not found", "status": "error"}

        # Use the corrected table name for querying
        try:
            result = self.catalog_manager.query_with_condition(
                actual_table_name, [], ["*"]
            )
            logging.info("Queried %d records from %s", len(result), actual_table_name)
        except Exception as e:
            logging.error("Error querying table %s: %s", actual_table_name, str(e))
            return {"error": f"Error querying table: {str(e)}", "status": "error"}

        if not result:
            logging.warning("No records found in table %s", actual_table_name)
            return {
                "columns": [f"{function}({column})"],
                "rows": [[0 if function == "COUNT" else None]],
                "status": "success"
            }

        # Get the actual column name from the first record
        actual_column = None
        if result and isinstance(result[0], dict):
            # Try exact match first
            if column in result[0]:
                actual_column = column
            else:
                # Try case-insensitive match
                for record_col in result[0].keys():
                    if record_col.lower() == column.lower():
                        actual_column = record_col
                        break

        logging.info("Using actual column name: '%s' for function %s(%s)", actual_column, function, column)

        # Execute the appropriate aggregate function
        try:
            if function == "AVG":
                aggregate_result = self._execute_avg(result, actual_column, column)
            elif function == "COUNT":
                aggregate_result = self._execute_count(result, actual_column, column)
            elif function == "SUM":
                aggregate_result = self._execute_sum(result, actual_column, column)
            elif function == "MIN":
                aggregate_result = self._execute_min(result, actual_column, column)
            elif function == "MAX":
                aggregate_result = self._execute_max(result, actual_column, column)
            elif function == "RAND":
                aggregate_result = self._execute_rand(result, actual_column, column, top, limit)
            elif function == "GCD":
                aggregate_result = self._execute_gcd(result, actual_column, column)
            else:
                return {"error": f"Unsupported aggregate function: {function}", "status": "error"}

            return aggregate_result

        except Exception as e:
            logging.error("Error executing aggregate function: %s", str(e))
            return {"error": f"Error executing aggregate: {str(e)}", "status": "error"}
