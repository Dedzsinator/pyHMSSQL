"""_summary_

Raises:
    ValueError: _description_

Returns:
    _type_: _description_
"""

import logging
import re


class JoinExecutor:
    """
    Class to execute JOIN queries using different join algorithms.
    """

    def __init__(self, catalog_manager, index_manager):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager

    def execute_join(self, plan):
        """Execute a JOIN operation based on the plan."""
        join_type = plan.get("join_type", "INNER")
        join_algorithm = plan.get("join_algorithm", "HASH")
        table1 = plan.get("table1")
        table2 = plan.get("table2")
        condition = plan.get("condition")
        columns = plan.get("columns", ["*"])

        # CRITICAL FIX: Get WHERE conditions from the correct plan location
        where_conditions = plan.get("where_conditions") or plan.get("parsed_condition")

        logging.info(
            f"🔍 Executing JOIN plan with WHERE conditions: {where_conditions}"
        )

        # Parse the join condition
        left_column, right_column = self._parse_join_condition(condition)

        # Get table data (this would be replaced with actual data fetching)
        left_table_data = self._get_table_data(table1)
        right_table_data = self._get_table_data(table2)

        # Debug the join algorithm choice
        logging.info(f"🔧 Join algorithm chosen: {join_algorithm}")
        logging.info(f"🔧 Join type: {join_type}")
        logging.info(
            f"🔧 About to call join with left_table_data={len(left_table_data)} records, right_table_data={len(right_table_data)} records"
        )

        # Execute the join based on algorithm
        if join_algorithm == "HASH":
            logging.info(f"🔧 Calling _execute_hash_join")
            results = self._execute_hash_join(
                left_table_data,
                right_table_data,
                left_column,
                right_column,
                table1,
                table2,
                join_type,
            )
        else:
            logging.info(f"🔧 Calling _execute_nested_loop_join")
            results = self._execute_nested_loop_join(
                left_table_data,
                right_table_data,
                left_column,
                right_column,
                table1,
                table2,
                join_type,
            )

        logging.info(f"Hash join produced {len(results)} results")

        # Apply WHERE conditions if present
        if where_conditions:
            logging.info(f"Applying WHERE conditions: {where_conditions}")
            filtered_results = []
            for record in results:
                if self._evaluate_where_condition(record, where_conditions):
                    filtered_results.append(record)

            logging.info(
                f"WHERE filtering reduced results from {len(results)} to {len(filtered_results)}"
            )
            results = filtered_results

        logging.info(f"After WHERE filtering: {len(results)} results")

        # Apply column selection
        if columns and columns != ["*"]:
            logging.info(f"Applying column selection: {columns}")
            results = self._apply_column_selection(results, columns)

        # CRITICAL FIX: Remove table prefixes for cleaner display
        if results:
            cleaned_results = []
            for record in results:
                cleaned_record = {}
                for key, value in record.items():
                    # Remove table prefix for display if the column name is unique
                    if "." in key:
                        simple_name = key.split(".", 1)[1]
                        # Check if this simple name would conflict with other columns
                        conflicts = [
                            k
                            for k in record.keys()
                            if k != key and k.endswith(f".{simple_name}")
                        ]
                        if not conflicts:
                            cleaned_record[simple_name] = value
                        else:
                            cleaned_record[key] = (
                                value  # Keep prefixed if there's a conflict
                            )
                    else:
                        cleaned_record[key] = value
                cleaned_results.append(cleaned_record)

            return cleaned_results

        return results

    def _get_table_data(self, table_name):
        """Get table data from storage."""
        # CRITICAL FIX: Use the correct catalog manager method
        try:
            # Use query_with_condition to get all table data
            data = self.catalog_manager.query_with_condition(table_name, [], ["*"])
            logging.info(f"📊 Fetched {len(data)} records from table '{table_name}'")

            # Special debug for customers table
            if table_name == "customers":
                customer_500 = [record for record in data if record.get("id") == 500]
                if customer_500:
                    logging.info(
                        f"✅ Customer 500 found in fetched data: {customer_500[0]}"
                    )
                else:
                    logging.warning(f"❌ Customer 500 NOT found in fetched data")
                    # Show all customer IDs for debugging
                    customer_ids = [
                        record.get("id") for record in data if record.get("id")
                    ]
                    logging.info(
                        f"Customer IDs in fetched data: {sorted(customer_ids)}"
                    )

            return data
        except Exception as e:
            logging.error(f"Error fetching table data for '{table_name}': {e}")
            return []

    def _apply_column_selection(self, results, requested_columns):
        """Apply column selection to limit which columns are returned."""
        if not results or not requested_columns:
            return results

        # If requesting all columns, return as-is
        if "*" in requested_columns:
            return results

        filtered_results = []
        for record in results:
            filtered_record = {}
            for col in requested_columns:
                # Handle table.column format
                if "." in col:
                    # Direct match for table.column
                    if col in record:
                        filtered_record[col] = record[col]
                    else:
                        filtered_record[col] = None
                else:
                    # Simple column name - find it with any table prefix
                    found = False
                    for key, value in record.items():
                        if key == col or (key.endswith(f".{col}") and "." in key):
                            filtered_record[col] = value
                            found = True
                            break
                    if not found:
                        filtered_record[col] = None

            filtered_results.append(filtered_record)

        return filtered_results

    def _parse_join_condition(self, condition):
        """Parse join condition to extract column names."""
        # Example: "products.product_id = order_details.product_id"
        parts = condition.split(" = ")
        if len(parts) != 2:
            raise ValueError(f"Invalid join condition: {condition}")

        left_column = parts[0].strip()
        right_column = parts[1].strip()

        return left_column, right_column

    def _execute_hash_join(
        self,
        left_table_data,
        right_table_data,
        left_column,
        right_column,
        left_alias,
        right_alias,
        join_type="INNER",
    ):
        """Execute hash join algorithm."""
        results = []

        logging.info(
            f"Hash join: {len(left_table_data)} left records, {len(right_table_data)} right records"
        )
        logging.info(f"Join condition: {left_column} = {right_column}")
        logging.info(f"Join type: {join_type}")

        # Debug: Check if customer 500 is in left_table_data
        customer_500_in_left_data = None
        for i, record in enumerate(left_table_data):
            customer_id = self._get_column_value(record, left_column, left_alias)
            if customer_id == 500:
                customer_500_in_left_data = record
                logging.info(
                    f"🎯 Customer 500 found at index {i} in left_table_data: {record}"
                )
                break

        if customer_500_in_left_data is None:
            logging.error(f"❌ Customer 500 NOT found in left_table_data!")
            # Log a sample of customer IDs to see what we have
            sample_ids = []
            for i, record in enumerate(left_table_data[:5]):  # First 5
                cid = self._get_column_value(record, left_column, left_alias)
                sample_ids.append(f"[{i}]={cid}")
            for i, record in enumerate(
                left_table_data[-5:], len(left_table_data) - 5
            ):  # Last 5
                cid = self._get_column_value(record, left_column, left_alias)
                sample_ids.append(f"[{i}]={cid}")
            logging.info(
                f"Sample customer IDs in left_table_data: {', '.join(sample_ids)}"
            )
        else:
            logging.info(f"✅ Customer 500 confirmed in left_table_data")

        # Build hash table from smaller table (right table in this case)
        hash_table = {}
        for record in right_table_data:
            # Extract the join key value
            key_value = self._get_column_value(record, right_column, right_alias)
            logging.debug(
                f"Right record key {right_column}: {key_value}, record: {record}"
            )
            if key_value is not None:
                if key_value not in hash_table:
                    hash_table[key_value] = []
                hash_table[key_value].append(record)

        logging.info(f"Built hash table with {len(hash_table)} unique keys")

        # Debug: Log hash table keys
        logging.info(f"Hash table keys: {sorted(list(hash_table.keys()))}")

        # Debug: Check for customer ID 500 specifically
        customer_500_found = False
        customer_500_records = []
        for record in left_table_data:
            customer_id = self._get_column_value(record, left_column, left_alias)
            if customer_id == 500:
                customer_500_found = True
                customer_500_records.append(record)
                logging.info(f"🔍 Found customer 500 in left table: {record}")

        if not customer_500_found:
            logging.warning("⚠️ Customer 500 not found in left table data!")
            # Log first few and last few records to debug
            logging.info(f"First 3 left records: {left_table_data[:3]}")
            logging.info(f"Last 3 left records: {left_table_data[-3:]}")
            customer_ids = [
                self._get_column_value(record, left_column, left_alias)
                for record in left_table_data
            ]
            logging.info(
                f"All customer IDs in left table: {sorted([id for id in customer_ids if id is not None])}"
            )
        else:
            logging.info(
                f"✅ Customer 500 found {len(customer_500_records)} times in left table"
            )

        # Probe with left table
        matched_left_records = 0
        unmatched_left_records = 0
        for left_record in left_table_data:
            left_key_value = self._get_column_value(
                left_record, left_column, left_alias
            )

            # Special debugging for customer ID 500
            if left_key_value == 500:
                logging.info(f"🔍 Processing customer 500: {left_record}")
                logging.info(f"🔍 Customer 500 key value: {left_key_value}")
                logging.info(f"🔍 Key 500 in hash table: {500 in hash_table}")
                logging.info(f"🔍 Join type: {join_type}")

            logging.debug(
                f"Left record key {left_column}: {left_key_value}, record: {left_record}"
            )

            if left_key_value is not None and left_key_value in hash_table:
                # Found matching records
                matched_left_records += 1
                for right_record in hash_table[left_key_value]:
                    joined_record = self._create_joined_record(
                        left_record, right_record, left_alias, right_alias
                    )
                    results.append(joined_record)
                    logging.debug(f"Joined record created: {joined_record}")

                    # Special debug for customer 500
                    if left_key_value == 500:
                        logging.info(
                            f"🔍 Customer 500 matched with right record: {right_record}"
                        )
            elif join_type in ["LEFT", "FULL"]:
                # Left/Full join - include left record with null values for right (no match found or left key is NULL)
                unmatched_left_records += 1

                # Special debug for customer 500
                if left_key_value == 500:
                    logging.info(
                        f"🔍 Customer 500 being added as unmatched LEFT JOIN record"
                    )

                logging.info(
                    f"LEFT/FULL join: Adding unmatched left record {left_record} with key {left_key_value}"
                )
                joined_record = self._create_joined_record(
                    left_record, None, left_alias, right_alias
                )
                results.append(joined_record)
                logging.debug(f"Added unmatched left record: {joined_record}")
            # For INNER join, we don't include unmatched records

        logging.info(
            f"LEFT JOIN stats: matched={matched_left_records}, unmatched={unmatched_left_records}, total_left={len(left_table_data)}"
        )

        # Handle right join case
        if join_type in ["RIGHT", "FULL"]:
            # Add unmatched right records
            matched_right_keys = set()
            for left_record in left_table_data:
                left_key_value = self._get_column_value(
                    left_record, left_column, left_alias
                )
                if left_key_value in hash_table:
                    matched_right_keys.add(left_key_value)

            for key, right_records in hash_table.items():
                if key not in matched_right_keys:
                    for right_record in right_records:
                        joined_record = self._create_joined_record(
                            None, right_record, left_alias, right_alias
                        )
                        results.append(joined_record)

        logging.info(
            f"Hash join produced {len(results)} results before WHERE filtering"
        )
        return results

    def _execute_nested_loop_join(
        self,
        left_table_data,
        right_table_data,
        left_column,
        right_column,
        left_alias,
        right_alias,
        join_type="INNER",
    ):
        """Execute nested loop join algorithm."""
        results = []

        for left_record in left_table_data:
            matched = False
            left_key_value = self._get_column_value(
                left_record, left_column, left_alias
            )

            for right_record in right_table_data:
                right_key_value = self._get_column_value(
                    right_record, right_column, right_alias
                )

                if left_key_value == right_key_value:
                    joined_record = self._create_joined_record(
                        left_record, right_record, left_alias, right_alias
                    )
                    results.append(joined_record)
                    matched = True

            # Handle left/full joins for unmatched left records
            if not matched and join_type in ["LEFT", "FULL"]:
                joined_record = self._create_joined_record(
                    left_record, None, left_alias, right_alias
                )
                results.append(joined_record)

        # Handle right/full joins for unmatched right records
        if join_type in ["RIGHT", "FULL"]:
            for right_record in right_table_data:
                matched = False
                right_key_value = self._get_column_value(
                    right_record, right_column, right_alias
                )

                for left_record in left_table_data:
                    left_key_value = self._get_column_value(
                        left_record, left_column, left_alias
                    )
                    if left_key_value == right_key_value:
                        matched = True
                        break

                if not matched:
                    joined_record = self._create_joined_record(
                        None, right_record, left_alias, right_alias
                    )
                    results.append(joined_record)

        return results

    def _get_column_value(self, record, column_name, table_alias):
        """Get column value from record, handling table prefixes."""
        if not record:
            return None

        # Extract table and column parts
        if "." in column_name:
            table_part, col_part = column_name.split(".", 1)
        else:
            table_part = table_alias
            col_part = column_name

        # Try different column name formats
        possible_keys = [
            column_name,  # exact match
            col_part,  # just column name
            f"{table_alias}.{col_part}",  # with alias prefix
            f"{table_part}.{col_part}",  # with original table prefix
        ]

        for key in possible_keys:
            if key in record:
                logging.debug(f"Found column {column_name} as {key} = {record[key]}")
                return record[key]

        # Debug: show available keys
        logging.debug(
            f"Column {column_name} not found. Available keys: {list(record.keys())}"
        )
        return None

    def _create_joined_record(self, left_record, right_record, left_alias, right_alias):
        """Create a joined record from left and right records."""
        joined = {}

        # Add left record fields with table prefix
        if left_record:
            for key, value in left_record.items():
                # Always use table prefix for JOIN results to avoid conflicts
                if "." not in key:
                    prefixed_key = f"{left_alias}.{key}"
                else:
                    prefixed_key = key
                joined[prefixed_key] = value

        # Add right record fields with table prefix
        if right_record:
            for key, value in right_record.items():
                # Always use table prefix for JOIN results to avoid conflicts
                if "." not in key:
                    prefixed_key = f"{right_alias}.{key}"
                else:
                    prefixed_key = key
                joined[prefixed_key] = value

        return joined

    def _evaluate_where_condition(self, record, condition):
        """Evaluate a WHERE condition against a joined record."""
        if not condition:
            return True

        operator = condition.get("operator")

        if operator == "AND":
            operands = condition.get("operands", [])
            return all(self._evaluate_where_condition(record, op) for op in operands)

        elif operator == "OR":
            operands = condition.get("operands", [])
            return any(self._evaluate_where_condition(record, op) for op in operands)

        else:
            # Simple condition
            column = condition.get("column")
            op = condition.get("operator")
            value = condition.get("value")

            if not column or not op:
                return True

            # CRITICAL FIX: Get the record value for this column - handle table prefixes correctly
            record_value = None

            # Log for debugging
            logging.debug(f"Evaluating: {column} {op} {value}")
            logging.debug(f"Record keys: {list(record.keys())}")

            # Try exact match first (for fully qualified column names like products.category)
            if column in record:
                record_value = record[column]
                logging.debug(f"Found exact match for '{column}': {record_value}")
            else:
                # Extract just the column name from table.column format
                if "." in column:
                    table_name, col_name = column.split(".", 1)
                    # Try to find the exact table.column match
                    if column in record:
                        record_value = record[column]
                        logging.debug(
                            f"Found table-qualified match for '{column}': {record_value}"
                        )
                    else:
                        # Look for any column that ends with this column name
                        for key in record.keys():
                            if key.endswith(f".{col_name}") or key == col_name:
                                record_value = record[key]
                                logging.debug(
                                    f"Found suffix match for '{column}' using key '{key}': {record_value}"
                                )
                                break
                else:
                    # Column doesn't have table prefix, try to find it with any table prefix
                    for key in record.keys():
                        if key.endswith(f".{column}") or key == column:
                            record_value = record[key]
                            logging.debug(
                                f"Found prefix match for '{column}' using key '{key}': {record_value}"
                            )
                            break

            if record_value is None:
                logging.debug(f"Column '{column}' not found in record")
                return False

            # Apply the operator with type conversion
            try:
                if op == "=":
                    if (
                        isinstance(value, str)
                        and value.startswith("'")
                        and value.endswith("'")
                    ):
                        value = value[1:-1]  # Remove quotes
                    result = (
                        str(record_value).lower() == str(value).lower()
                        if isinstance(record_value, str)
                        else record_value == value
                    )
                elif op == ">":
                    # Convert to numbers for comparison
                    try:
                        result = float(record_value) > float(value)
                    except (ValueError, TypeError):
                        result = str(record_value) > str(value)
                elif op == "<":
                    try:
                        result = float(record_value) < float(value)
                    except (ValueError, TypeError):
                        result = str(record_value) < str(value)
                elif op == ">=":
                    try:
                        result = float(record_value) >= float(value)
                    except (ValueError, TypeError):
                        result = str(record_value) >= str(value)
                elif op == "<=":
                    try:
                        result = float(record_value) <= float(value)
                    except (ValueError, TypeError):
                        result = str(record_value) <= str(value)
                elif op == "!=":
                    if (
                        isinstance(value, str)
                        and value.startswith("'")
                        and value.endswith("'")
                    ):
                        value = value[1:-1]  # Remove quotes
                    result = record_value != value
                elif op.upper() == "LIKE":
                    pattern = str(value).replace("%", ".*").replace("_", ".")
                    result = (
                        re.search(pattern, str(record_value), re.IGNORECASE) is not None
                    )
                elif op.upper() == "IN":
                    result = record_value in value
                else:
                    result = False

                logging.debug(
                    f"Condition '{column}' {op} '{value}' evaluated to: {result} (record_value: {record_value})"
                )
                return result

            except Exception as e:
                logging.error(
                    f"Error evaluating condition '{column}' {op} '{value}': {e}"
                )
                return False

        return False
