"""_summary_

Raises:
    ValueError: _description_

Returns:
    _type_: _description_
"""
import logging
import re
import traceback


class JoinExecutor:
    """
    Class to execute JOIN queries using different join algorithms.
    """
    def __init__(self, catalog_manager, index_manager):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager

    def execute_join(self, plan):
        """Execute a JOIN query using the appropriate join algorithm."""
        join_type = plan.get("join_type", "INNER").upper()
        if not join_type and plan.get("join_info", {}).get("type"):
            join_type = plan.get("join_info", {}).get("type").upper()
            
        join_algorithm = plan.get("join_algorithm", "HASH").upper()
        
        # Get tables from different possible locations in the plan
        join_info = plan.get("join_info", {})
        table1 = plan.get("table1", "")
        table2 = plan.get("table2", "")
        
        # Use join_info if table1/table2 aren't directly in plan
        if not table1 and join_info.get("table1"):
            table1 = join_info.get("table1", "")
        if not table2 and join_info.get("table2"):
            table2 = join_info.get("table2", "")
            
        condition = plan.get("condition")
        if condition is None and join_info.get("condition") is not None:
            condition = join_info.get("condition")

        logging.info("Executing %s JOIN using %s algorithm", join_type, join_algorithm)
        logging.info("Table 1: %s", table1)
        logging.info("Table 2: %s", table2)
        logging.info("Condition: %s", condition)

        # Handle table aliases
        table1_parts = table1.split() if isinstance(table1, str) else []
        table1_name = table1_parts[0] if table1_parts else ""
        table1_alias = table1_parts[1] if len(table1_parts) > 1 else table1_name

        table2_parts = table2.split() if isinstance(table2, str) else []
        table2_name = table2_parts[0] if table2_parts else ""
        table2_alias = table2_parts[1] if len(table2_parts) > 1 else table2_name

        # Get current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {
                "error": "No database selected. Use 'USE database_name' first.",
                "status": "error",
                "type": "error",
            }

        # Verify tables exist
        tables = self.catalog_manager.list_tables(db_name)
        if table1_name not in tables:
            return {
                "error": f"Table '{table1_name}' does not exist in database '{db_name}'",
                "status": "error",
                "type": "error",
            }
        if table2_name not in tables:
            return {
                "error": f"Table '{table2_name}' does not exist in database '{db_name}'",
                "status": "error",
                "type": "error",
            }

        # Skip condition check for CROSS JOINs
        if join_type != "CROSS" and not condition:
            return {
                "error": "No join condition specified",
                "status": "error",
                "type": "error",
            }

        try:
            # Get data from both tables
            table1_data = self.catalog_manager.query_with_condition(
                table1_name, [], ["*"]
            )
            table2_data = self.catalog_manager.query_with_condition(
                table2_name, [], ["*"]
            )

            # For CROSS JOINs, we don't need join columns
            if join_type == "CROSS":
                left_column, right_column = None, None
            else:
                # Parse the join condition to extract column names
                join_columns = self._parse_join_condition(condition) if condition else (None, None)
                left_column, right_column = join_columns

            # For CROSS JOIN, we should use nested loop regardless of the algorithm specified
            if join_type == "CROSS":
                result = self._execute_nested_loop_join(
                    table1_data,
                    table2_data,
                    left_column,
                    right_column,
                    table1_alias,
                    table2_alias,
                    join_type,
                )
            elif join_algorithm == "HASH":
                result = self._execute_hash_join(
                    table1_data,
                    table2_data,
                    left_column,
                    right_column,
                    table1_alias,
                    table2_alias,
                    join_type,
                )
            elif join_algorithm == "MERGE":
                result = self._execute_merge_join(
                    table1_data,
                    table2_data,
                    left_column,
                    right_column,
                    table1_alias,
                    table2_alias,
                    join_type,
                )
            elif join_algorithm == "INDEX":
                result = self._execute_index_join(
                    table1_name,
                    table2_name,
                    left_column,
                    right_column,
                    table1_alias,
                    table2_alias,
                    join_type,
                )
            elif join_algorithm == "NESTED_LOOP":
                result = self._execute_nested_loop_join(
                    table1_data,
                    table2_data,
                    left_column,
                    right_column,
                    table1_alias,
                    table2_alias,
                    join_type,
                )
            else:
                # Default to hash join
                result = self._execute_hash_join(
                    table1_data,
                    table2_data,
                    left_column,
                    right_column,
                    table1_alias,
                    table2_alias,
                    join_type,
                )

            # Format columns for result
            columns = plan.get("columns", ["*"])
            if "*" in columns:
                # Get all columns from both tables with table aliases
                if result and len(result) > 0:
                    columns = list(result[0].keys())

            # Format rows for result
            result_rows = []
            for record in result:
                row = []
                for col in columns:
                    if col == "*":
                        # Include all fields in order
                        row.extend(record.values())
                    else:
                        # Include specific column
                        value = record.get(col, None)
                        row.append(value)
                result_rows.append(row)

            return {
                "columns": columns,
                "rows": result_rows,
                "status": "success",
                "type": "join_result",
                "rowCount": len(result_rows),
            }

        except RuntimeError as e:
            logging.error(f"Error executing join: {str(e)}")
            logging.error(traceback.format_exc())
            return {
                "error": f"Error executing join: {str(e)}",
                "status": "error",
                "type": "error",
            }

    def _parse_join_condition(self, condition):
        """Parse a join condition like 'table1.col1 = table2.col2' into column names."""
        if not condition:
            return None, None

        # Try to match the pattern table.column = table.column
        match = re.search(r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", condition)
        if match:
            left_table, left_col = match.group(1), match.group(2)
            right_table, right_col = match.group(3), match.group(4)
            return left_col, right_col

        # Try simpler pattern column = column
        match = re.search(r"(\w+)\s*=\s*(\w+)", condition)
        if match:
            return match.group(1), match.group(2)

        return None, None

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
        """
        Execute a hash join with support for INNER, LEFT, RIGHT and FULL joins.

        Args:
            left_table_data: Records from the left table
            right_table_data: Records from the right table
            left_column: Join column in left table
            right_column: Join column in right table
            left_alias: Alias for the left table
            right_alias: Alias for the right table
            join_type: Type of join (INNER, LEFT, RIGHT, FULL)

        Returns:
            List of joined records
        """
        logging.info(
            f"Executing {join_type} hash join: {left_alias}.{left_column} = {
                right_alias
            }.{right_column}"
        )
        logging.info(
            f"Table sizes: {left_alias}={len(left_table_data)} records, {right_alias}={
                len(right_table_data)
            } records"
        )

        # For INNER and LEFT joins, build hash table on right table
        # For RIGHT joins, build hash table on left table
        # For efficiency, always build hash table on the smaller table
        if join_type == "RIGHT" or (
            join_type != "LEFT" and len(
                left_table_data) > len(right_table_data)
        ):
            # Swap tables to make implementation simpler
            left_table_data, right_table_data = right_table_data, left_table_data
            left_column, right_column = right_column, left_column
            left_alias, right_alias = right_alias, left_alias
            # Also swap join type if needed
            if join_type == "LEFT":
                join_type = "RIGHT"
            elif join_type == "RIGHT":
                join_type = "LEFT"

        # Build hash table from the smaller table (now always left_table_data)
        hash_table = {}
        for record in left_table_data:
            # Find the join value case-insensitively
            join_value = None
            for col in record:
                if col.lower() == left_column.lower():
                    join_value = record[col]
                    break

            if join_value is not None:
                if join_value not in hash_table:
                    hash_table[join_value] = []
                hash_table[join_value].append(record)

        logging.debug(f"Hash table built with {len(hash_table)} unique keys")

        # Probe the hash table with records from the larger table
        result = []
        matched_left_records = (
            set()
        )  # Track which left records were matched (for LEFT/FULL joins)

        for right_record in right_table_data:
            # Find the join value case-insensitively
            join_value = None
            for col in right_record:
                if col.lower() == right_column.lower():
                    join_value = right_record[col]
                    break

            found_match = False
            if join_value is not None and join_value in hash_table:
                # Match found - join with all matching records
                for left_record in hash_table[join_value]:
                    # Create a joined record
                    joined_record = {}

                    # Add fields from left table with alias prefix
                    for field, value in left_record.items():
                        joined_record[f"{left_alias}.{field}"] = value

                    # Add fields from right table with alias prefix
                    for field, value in right_record.items():
                        joined_record[f"{right_alias}.{field}"] = value

                    result.append(joined_record)
                    found_match = True

                    # Track this left record as matched
                    record_id = tuple(sorted(left_record.items()))
                    matched_left_records.add(record_id)

            # For RIGHT or FULL join, include right records without matches
            if not found_match and (join_type == "RIGHT" or join_type == "FULL"):
                joined_record = {}

                # Add NULL values for left table
                for left_record in left_table_data[
                    :1
                ]:  # Use first record to get field names
                    for field in left_record:
                        joined_record[f"{left_alias}.{field}"] = None

                # Add fields from right table
                for field, value in right_record.items():
                    joined_record[f"{right_alias}.{field}"] = value

                result.append(joined_record)

        # For LEFT or FULL join, include left records that weren't matched
        if join_type == "LEFT" or join_type == "FULL":
            for left_record in left_table_data:
                record_id = tuple(sorted(left_record.items()))
                if record_id not in matched_left_records:
                    joined_record = {}

                    # Add fields from left table
                    for field, value in left_record.items():
                        joined_record[f"{left_alias}.{field}"] = value

                    # Add NULL values for right table
                    for right_record in right_table_data[
                        :1
                    ]:  # Use first record to get field names
                        for field in right_record:
                            joined_record[f"{right_alias}.{field}"] = None

                    result.append(joined_record)

        logging.info(f"Hash join produced {len(result)} results")
        return result

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
        """
        Execute a nested loop join with support for all join types.

        This is the simplest join algorithm but can be inefficient for large tables.
        For each record in the outer table, it scans all records in the inner table.
        """
        logging.info(f"Executing {join_type} nested loop join")

        result = []

        # For cross joins, we don't need column matching
        is_cross_join = join_type == "CROSS" or (
            left_column is None and right_column is None
        )

        # Track matched records for LEFT/RIGHT/FULL joins
        matched_left = set()
        matched_right = set()

        # Outer loop - iterate through left table
        for left_record in left_table_data:
            left_matched = False

            # Get join value from left record
            left_value = None
            if not is_cross_join:
                for col in left_record:
                    if col.lower() == left_column.lower():
                        left_value = left_record[col]
                        break

            # Inner loop - iterate through right table
            for right_record in right_table_data:
                # For cross join, match all records
                if is_cross_join:
                    joined_record = self._create_joined_record(
                        left_record, right_record, left_alias, right_alias
                    )
                    result.append(joined_record)
                    left_matched = True
                    matched_right.add(id(right_record))
                    continue

                # Get join value from right record
                right_value = None
                for col in right_record:
                    if col.lower() == right_column.lower():
                        right_value = right_record[col]
                        break

                # Check if values match for join
                if left_value == right_value:
                    joined_record = self._create_joined_record(
                        left_record, right_record, left_alias, right_alias
                    )
                    result.append(joined_record)
                    left_matched = True
                    matched_right.add(id(right_record))

            # LEFT or FULL join: include left records without matches
            if not left_matched and (join_type == "LEFT" or join_type == "FULL"):
                joined_record = {}

                # Add fields from left table
                for field, value in left_record.items():
                    joined_record[f"{left_alias}.{field}"] = value

                # Add NULL values for right table
                if right_table_data:
                    for field in right_table_data[0]:
                        joined_record[f"{right_alias}.{field}"] = None

                result.append(joined_record)

            if left_matched:
                matched_left.add(id(left_record))

        # RIGHT or FULL join: include right records without matches
        if join_type == "RIGHT" or join_type == "FULL":
            for right_record in right_table_data:
                if id(right_record) not in matched_right:
                    joined_record = {}

                    # Add NULL values for left table
                    if left_table_data:
                        for field in left_table_data[0]:
                            joined_record[f"{left_alias}.{field}"] = None

                    # Add fields from right table
                    for field, value in right_record.items():
                        joined_record[f"{right_alias}.{field}"] = value

                    result.append(joined_record)

        logging.info(f"Nested loop join produced {len(result)} results")
        return result

    def _create_joined_record(self, left_record, right_record, left_alias, right_alias):
        """Helper method to create a joined record with proper field prefixes."""
        joined_record = {}

        # Add fields from left record with table alias
        for field, value in left_record.items():
            joined_record[f"{left_alias}.{field}"] = value

        # Add fields from right record with table alias
        for field, value in right_record.items():
            joined_record[f"{right_alias}.{field}"] = value

        return joined_record

    def _execute_merge_join(
        self,
        left_table_data,
        right_table_data,
        left_column,
        right_column,
        left_alias,
        right_alias,
        join_type="INNER",
    ):
        """
        Execute a sort-merge join with support for all join types.

        This algorithm works well when data is already sorted or when the dataset is too
        large to fit in memory for a hash join.
        """
        logging.info(f"Executing {join_type} merge join")

        if not left_column or not right_column:
            if join_type == "CROSS":
                # For cross joins, fall back to nested loop
                return self._execute_nested_loop_join(
                    left_table_data,
                    right_table_data,
                    left_column,
                    right_column,
                    left_alias,
                    right_alias,
                    join_type,
                )
            else:
                raise ValueError("Sort-merge join requires join columns")

        # Helper function to get join key from a record
        def get_key(record, column_name):
            for col in record:
                if col.lower() == column_name.lower():
                    return record[col]
            return None

        # Sort both tables on join columns
        sorted_left = sorted(
            left_table_data,
            key=lambda r: (get_key(r, left_column) is None,
                           get_key(r, left_column)),
        )
        sorted_right = sorted(
            right_table_data,
            key=lambda r: (get_key(r, right_column) is None,
                           get_key(r, right_column)),
        )

        result = []
        i = j = 0

        # Track matched records for outer joins
        matched_left = [False] * len(sorted_left)
        matched_right = [False] * len(sorted_right)

        # Merge sorted tables
        while i < len(sorted_left) and j < len(sorted_right):
            left_val = get_key(sorted_left[i], left_column)
            right_val = get_key(sorted_right[j], right_column)

            # Skip null values in inner join
            if left_val is None and join_type == "INNER":
                i += 1
                continue
            if right_val is None and join_type == "INNER":
                j += 1
                continue

            if left_val == right_val:
                # Values match - find all matching records on both sides

                # First, collect all matching records from left side
                left_matches = []
                k = i
                while (
                    k < len(sorted_left)
                    and get_key(sorted_left[k], left_column) == left_val
                ):
                    left_matches.append(k)
                    k += 1

                # Then, collect all matching records from right side
                right_matches = []
                l = j
                while (
                    l < len(sorted_right)
                    and get_key(sorted_right[l], right_column) == right_val
                ):
                    right_matches.append(l)
                    l += 1

                # Join all matching pairs
                for left_idx in left_matches:
                    matched_left[left_idx] = True
                    for right_idx in right_matches:
                        matched_right[right_idx] = True
                        joined_record = self._create_joined_record(
                            sorted_left[left_idx],
                            sorted_right[right_idx],
                            left_alias,
                            right_alias,
                        )
                        result.append(joined_record)

                # Move pointers past all processed records
                i = k
                j = l
            elif left_val < right_val:
                i += 1
            else:
                j += 1

        # Handle outer joins - add unmatched records
        if join_type in ["LEFT", "FULL"]:
            for idx, record in enumerate(sorted_left):
                if not matched_left[idx]:
                    joined_record = {}

                    # Add fields from left record
                    for field, value in record.items():
                        joined_record[f"{left_alias}.{field}"] = value

                    # Add NULL values for right table
                    if right_table_data:
                        for field in right_table_data[0]:
                            joined_record[f"{right_alias}.{field}"] = None

                    result.append(joined_record)

        if join_type in ["RIGHT", "FULL"]:
            for idx, record in enumerate(sorted_right):
                if not matched_right[idx]:
                    joined_record = {}

                    # Add NULL values for left table
                    if left_table_data:
                        for field in left_table_data[0]:
                            joined_record[f"{left_alias}.{field}"] = None

                    # Add fields from right record
                    for field, value in record.items():
                        joined_record[f"{right_alias}.{field}"] = value

                    result.append(joined_record)

        logging.info(f"Merge join produced {len(result)} results")
        return result

    def _execute_index_join(
        self,
        left_table_name,
        right_table_name,
        left_column,
        right_column,
        left_alias,
        right_alias,
        join_type="INNER",
    ):
        """
        Execute an index join between two tables using an existing index if available.
        Supports all join types.
        """
        logging.info(f"Executing {join_type} index join")

        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            logging.error("No database selected for index join")
            return []

        # Check for indexes on left and right tables
        left_index = self.index_manager.get_index(
            f"{left_table_name}.{left_column}")
        right_index = self.index_manager.get_index(
            f"{right_table_name}.{right_column}")

        # Determine which table has an index (prefer right table index for probe-build pattern)
        if right_index:
            # Use right table's index (optimal case)
            indexed_table_name = right_table_name
            indexed_column = right_column
            indexed_alias = right_alias
            probe_table_name = left_table_name
            probe_column = left_column
            probe_alias = left_alias
            index = right_index
            is_right_indexed = True
        elif left_index:
            # Use left table's index
            indexed_table_name = left_table_name
            indexed_column = left_column
            indexed_alias = left_alias
            probe_table_name = right_table_name
            probe_column = right_column
            probe_alias = right_alias
            index = left_index
            is_right_indexed = False
        else:
            # No index available, fall back to hash join
            logging.warning(
                f"No index found for join columns, falling back to hash join"
            )
            return self._execute_hash_join(
                self.catalog_manager.query_with_condition(
                    left_table_name, [], ["*"]),
                self.catalog_manager.query_with_condition(
                    right_table_name, [], ["*"]),
                left_column,
                right_column,
                left_alias,
                right_alias,
                join_type,
            )

        # Get the probe table data (full scan of non-indexed table)
        probe_data = self.catalog_manager.query_with_condition(
            probe_table_name, [], ["*"]
        )

        # Initialize indexed_data with empty list for inner joins
        indexed_data = []

        # For outer joins, we may need all data from the indexed table
        if join_type in ["LEFT", "RIGHT", "FULL"]:
            # Adjust join type based on which table is indexed
            if (join_type == "LEFT" and is_right_indexed) or (
                join_type == "RIGHT" and not is_right_indexed
            ):
                # We need all data from indexed table
                indexed_data = self.catalog_manager.query_with_condition(
                    indexed_table_name, [], ["*"]
                )
            elif join_type == "FULL":
                # For FULL joins, we always need all data from indexed table
                indexed_data = self.catalog_manager.query_with_condition(
                    indexed_table_name, [], ["*"]
                )

        result = []
        matched_probe_records = set()  # Track which probe records were matched
        matched_indexed_values = set()  # Track which indexed values were matched

        # Probe the index with each record from the probe table
        for probe_record in probe_data:
            # Find the join value in the probe record
            probe_value = None
            for col in probe_record:
                if col.lower() == probe_column.lower():
                    probe_value = probe_record[col]
                    break

            found_match = False

            if probe_value is not None:
                # Use the index to look up matching indexed records
                try:
                    matching_keys = index.search(probe_value)

                    if matching_keys:
                        found_match = True

                        # Handle different return types from index search
                        if not isinstance(matching_keys, list):
                            matching_keys = [matching_keys]

                        # Retrieve each matching indexed record
                        for key in matching_keys:
                            if key is None:
                                continue

                            # We need to fetch the actual record using the key
                            indexed_record = self.catalog_manager.get_record_by_key(
                                indexed_table_name, key
                            )

                            if indexed_record:
                                # Create joined record
                                if is_right_indexed:
                                    joined_record = self._create_joined_record(
                                        probe_record,
                                        indexed_record,
                                        probe_alias,
                                        indexed_alias,
                                    )
                                else:
                                    joined_record = self._create_joined_record(
                                        indexed_record,
                                        probe_record,
                                        indexed_alias,
                                        probe_alias,
                                    )

                                result.append(joined_record)
                                matched_indexed_values.add(probe_value)
                except RuntimeError as e:
                    logging.error(
                        f"Error searching index for value {
                            probe_value}: {str(e)}"
                    )
                    logging.error(traceback.format_exc())

            # For LEFT or FULL outer join, include probe records without matches
            if not found_match and (
                (not is_right_indexed and (join_type == "LEFT" or join_type == "FULL"))
                or (is_right_indexed and (join_type == "RIGHT" or join_type == "FULL"))
            ):
                joined_record = {}

                # Add fields from probe record
                for field, value in probe_record.items():
                    joined_record[f"{probe_alias}.{field}"] = value

                # Add NULL values for indexed table fields
                # We need a sample record to get the field names
                sample_indexed = next(
                    iter(indexed_data), {}
                )  # Safe now since indexed_data is always initialized
                for field in sample_indexed:
                    joined_record[f"{indexed_alias}.{field}"] = None

                result.append(joined_record)

            if found_match:
                matched_probe_records.add(id(probe_record))

        # For RIGHT or FULL join, include indexed records without matches
        if join_type in ["RIGHT", "FULL"] and indexed_data:  # Safe check now
            for indexed_record in indexed_data:
                # Get the join value from this indexed record
                indexed_value = None
                for col in indexed_record:
                    if col.lower() == indexed_column.lower():
                        indexed_value = indexed_record[col]
                        break

                # Skip if this value was already matched
                if (
                    indexed_value is not None
                    and indexed_value not in matched_indexed_values
                ):
                    joined_record = {}

                    # Add fields from indexed record
                    for field, value in indexed_record.items():
                        joined_record[f"{indexed_alias}.{field}"] = value

                    # Add NULL values for probe table fields
                    if probe_data:
                        for field in probe_data[0]:
                            joined_record[f"{probe_alias}.{field}"] = None

                    result.append(joined_record)

        logging.info(f"Index join produced {len(result)} results")
        return result
