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

    def _execute_count(self, result, actual_column, column):
        """Execute COUNT aggregate function."""
        if column == "*":
            return len(result)
        else:
            if not actual_column:
                raise ValueError(f"Column '{column}' not found")
            return sum(
                1
                for record in result
                if actual_column in record and record[actual_column] is not None
            )

    def _execute_sum(self, result, actual_column, column):
        """Execute SUM aggregate function."""
        if column == "*":
            raise ValueError("Cannot use SUM with *")
        if not actual_column:
            raise ValueError(f"Column '{column}' not found")

        total = 0
        count = 0
        for record in result:
            if actual_column in record and record[actual_column] is not None:
                try:
                    total += float(record[actual_column])
                    count += 1
                except (ValueError, TypeError):
                    pass  # Skip non-numeric values
        return total if count > 0 else None

    def _execute_avg(self, result, actual_column, column):
        """Execute AVG aggregate function."""
        if column == "*":
            raise ValueError("Cannot use AVG with *")
        if not actual_column:
            raise ValueError(f"Column '{column}' not found")

        total = 0
        count = 0
        for record in result:
            if actual_column in record and record[actual_column] is not None:
                try:
                    total += float(record[actual_column])
                    count += 1
                except (ValueError, TypeError):
                    pass  # Skip non-numeric values
        return (total / count) if count > 0 else None

    def _execute_min(self, result, actual_column, column):
        """Execute MIN aggregate function."""
        if column == "*":
            raise ValueError("Cannot use MIN with *")
        if not actual_column:
            raise ValueError(f"Column '{column}' not found")

        values = []
        for record in result:
            if actual_column in record and record[actual_column] is not None:
                values.append(record[actual_column])
        return min(values) if values else None

    def _execute_max(self, result, actual_column, column):
        """Execute MAX aggregate function."""
        if column == "*":
            raise ValueError("Cannot use MAX with *")
        if not actual_column:
            raise ValueError(f"Column '{column}' not found")

        values = []
        for record in result:
            if actual_column in record and record[actual_column] is not None:
                values.append(record[actual_column])
        return max(values) if values else None

    def _execute_rand(self, result, param_str):
        """Execute RAND aggregate function."""
        import random

        logging.info(f"Executing RAND with params: '{param_str}'")

        # Parse parameters
        params = [p.strip() for p in param_str.split(",")]

        try:
            if len(params) == 1:
                n = int(params[0])
                if n <= 0:
                    raise ValueError(
                        "Number of random records must be positive")

                if result and len(result) > 0:
                    n = min(n, len(result))
                    logging.info(
                        f"Selecting {n} random records from {
                            len(result)} total"
                    )
                    selected = random.sample(range(len(result)), n)
                    return n
                else:
                    logging.warning("No records to sample from")
                    return 0

            elif len(params) == 3:
                n = int(params[0])
                min_val = float(params[1])
                max_val = float(params[2])

                logging.info(
                    f"Generating {n} random values between {
                        min_val} and {max_val}"
                )
                if n <= 0:
                    raise ValueError(
                        "Number of random values must be positive")

                random_values = [random.uniform(
                    min_val, max_val) for _ in range(n)]
                avg = sum(random_values) / len(random_values)
                logging.info(f"Generated values with average: {avg}")
                return avg

            else:
                raise ValueError(
                    "RAND requires either 1 parameter (n) or 3 parameters (n,min,max)"
                )

        except ValueError as e:
            logging.error(f"Error in RAND: {str(e)}")
            raise ValueError(f"Invalid parameters for RAND: {str(e)}")

    def _execute_gcd(self, result, actual_column, column):
        """Execute GCD aggregate function."""
        import math

        logging.info(f"Computing GCD for column '{
                     column}' (actual: '{actual_column}')")

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
                        logging.info(f"Added value {val} to GCD calculation")
                except (ValueError, TypeError):
                    logging.warning(
                        f"Skipping non-numeric value: {record[actual_column]}"
                    )

        if not values:
            logging.warning("No valid values found for GCD calculation")
            return None

        gcd_result = values[0]
        for value in values[1:]:
            gcd_result = math.gcd(gcd_result, value)

        logging.info(f"GCD result: {gcd_result}")
        return gcd_result

    def execute_aggregate(self, plan):
        """Execute an aggregation function (COUNT, SUM, AVG, MIN, MAX, RAND, GCD)."""
        logging.info(f"===== AGGREGATE FUNCTION CALLED =====")
        logging.info(f"Plan: {plan}")
        logging.info(f"Function: {plan.get('function')}")
        logging.info(f"Column: {plan.get('column')}")
        logging.info(f"Table: {plan.get('table')}")
        logging.info(f"Condition: {plan.get('condition')}")
        logging.info(f"Top: {plan.get('top')}")
        logging.info(f"Limit: {plan.get('limit')}")

        function = plan.get("function", "").upper()
        column = plan.get("column")
        table_name = plan.get("table")
        condition = plan.get("condition")

        # Handle TOP and LIMIT parameters
        top_n = plan.get("top")
        limit = plan.get("limit")
        if top_n is not None:
            try:
                top_n = int(top_n)
                logging.info(f"TOP {top_n} specified for aggregate")
            except (ValueError, TypeError):
                top_n = None

        if limit is not None:
            try:
                limit = int(limit)
                logging.info(f"LIMIT {limit} specified for aggregate")
            except (ValueError, TypeError):
                limit = None

        # Get raw data using catalog manager
        try:
            # Parse and apply conditions
            conditions = (
                ConditionParser.parse_condition_to_list(
                    condition) if condition else []
            )
            result = self.catalog_manager.query_with_condition(
                table_name, conditions, ["*"]
            )
            logging.info(
                f"Queried {len(result) if result else 0} records from {
                    table_name}"
            )

            # Apply TOP/LIMIT to the raw data before aggregation if appropriate
            # For most aggregates, we need to aggregate all data first, then limit the results
            # But some functions like RAND may benefit from limiting data first
            if function == "RAND" and top_n is not None and top_n > 0 and result:
                result = result[:top_n]
                logging.info(f"Applied TOP {
                             top_n} to raw data for RAND function")
            elif function == "RAND" and limit is not None and limit > 0 and result:
                result = result[:limit]
                logging.info(f"Applied LIMIT {
                             limit} to raw data for RAND function")

        except Exception as e:
            logging.error(f"Error querying data: {str(e)}")
            return {
                "error": f"Error querying data: {str(e)}",
                "status": "error",
                "type": "error",
            }

        try:
            # Find the actual column name with original case
            actual_column = None
            if column != "*" and result and len(result) > 0:
                for col in result[0]:
                    if col.lower() == column.lower():
                        actual_column = col
                        break

                if not actual_column and function not in ["COUNT"]:
                    logging.warning(
                        f"Column '{column}' not found in table {
                            table_name}, available columns: {list(result[0].keys())}"
                    )

            logging.info(
                f"Using actual column name: '{
                    actual_column}' for function {function}({column})"
            )

            # Dispatch to appropriate function handler
            if function == "COUNT":
                aggregate_result = self._execute_count(
                    result, actual_column, column)
            elif function == "SUM":
                aggregate_result = self._execute_sum(
                    result, actual_column, column)
            elif function == "AVG":
                aggregate_result = self._execute_avg(
                    result, actual_column, column)
            elif function == "MIN":
                aggregate_result = self._execute_min(
                    result, actual_column, column)
            elif function == "MAX":
                aggregate_result = self._execute_max(
                    result, actual_column, column)
            elif function == "RAND":
                aggregate_result = self._execute_rand(result, column)
            elif function == "GCD":
                aggregate_result = self._execute_gcd(
                    result, actual_column, column)
            else:
                return {
                    "error": f"Unsupported aggregation function: {function}",
                    "status": "error",
                    "type": "error",
                }

            # Format result column name and value
            result_column = f"{function}({column})"
            if aggregate_result is None:
                display_value = "NULL"
            elif isinstance(aggregate_result, float):
                display_value = f"{aggregate_result:.2f}"
            else:
                display_value = str(aggregate_result)

            logging.info(f"Aggregate result: {
                         function}({column}) = {display_value}")

            # Return in the standard result format
            return {
                "columns": [result_column],
                "rows": [[display_value]],
                "status": "success",
                "type": "select_result",
                "rowCount": 1,
            }

        except Exception as e:
            logging.error(f"Error executing aggregate: {str(e)}")
            logging.error(traceback.format_exc())
            return {
                "error": f"Error executing aggregate {function}: {str(e)}",
                "status": "error",
                "type": "error",
            }
