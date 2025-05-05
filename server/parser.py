import re
import logging
import traceback
import sqlparse


class SQLParser:
    """
    SQL Parser for translating SQL statements into structured query plans.
    Handles various SQL commands including SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, and SHOW.
    Also handles specialized visualization commands.
    """

    def __init__(self, engine=None):
        self.engine = engine

    def set_engine(self, engine):
        """Set the execution engine reference"""
        self.engine = engine

    def parse_sql(self, sql):
        """
        Parse SQL query into a structured format.
        """
        try:
            # First check for set operations (UNION, INTERSECT, EXCEPT)
            if re.search(r'\s+UNION\s+|\s+INTERSECT\s+|\s+EXCEPT\s+', sql, re.IGNORECASE):
                return self._parse_set_operation(sql)

            if sql.strip().upper().startswith("CREATE INDEX"):
                return self.parse_create_index(sql)

            # Parse SQL query
            parsed = sqlparse.parse(sql)
            if not parsed:
                return {"error": "Failed to parse SQL statement"}

            # Get the first statement (we only support one at a time)
            stmt = parsed[0]

            # Handle special statements first
            if sql.strip().upper().startswith("SHOW "):
                result = {"type": "SHOW", "query": sql}
                self._extract_show_elements(stmt, result)
                return result

            if sql.strip().upper().startswith("USE "):
                db_name = sql.strip()[4:].strip()
                return {"type": "USE_DATABASE", "database": db_name, "query": sql}

            if sql.upper().startswith("VISUALIZE"):
                return self.parse_visualize_command(sql)

            # Initialize result with the query
            result = {"type": None, "query": sql}

            # Determine statement type
            stmt_type = stmt.get_type()

            if stmt_type == "SELECT":
                result["type"] = "SELECT"
                self._extract_select_elements(stmt, result)
            elif stmt_type == "INSERT":
                result["type"] = "INSERT"
                self._extract_insert_elements(stmt, result)
            elif stmt_type == "UPDATE":
                result["type"] = "UPDATE"
                self._extract_update_elements(stmt, result)
            elif stmt_type == "DELETE":
                result["type"] = "DELETE"
                self._extract_delete_elements(stmt, result)
            elif stmt_type == "CREATE":
                result["type"] = "CREATE"
                self._extract_create_elements(stmt, result)
            elif stmt_type == "DROP":
                # First extract the drop elements to determine drop_type
                self._extract_drop_elements(stmt, result)
                # Now set the correct operation type based on drop_type
                drop_type = result.get("drop_type")
                if drop_type == "TABLE":
                    result["type"] = "DROP_TABLE"
                elif drop_type == "DATABASE":
                    result["type"] = "DROP_DATABASE"
                elif drop_type == "INDEX":
                    result["type"] = "DROP_INDEX"
                elif drop_type == "VIEW":
                    result["type"] = "DROP_VIEW"
                else:
                    result["type"] = "DROP"  # Fallback
            elif stmt_type == "SHOW":
                result["type"] = "SHOW"
                self._extract_show_elements(stmt, result)
            else:
                result["error"] = "Unsupported SQL statement type"

            if "type" in result:
                result["operation"] = result["type"]
            return result
        except RuntimeError as e:
            logging.error("Error extracting elements: %s", str(e))
            logging.error(traceback.format_exc())
            return {"error": f"Error extracting SQL elements: {str(e)}"}

    def parse_visualize_command(self, query):
        """
        Parse a VISUALIZE command.
        """
        result = {"type": "VISUALIZE"}

        # Check for VISUALIZE BPTREE
        if "BPTREE" in query.upper():
            result["object"] = "BPTREE"

            # Check for the pattern: VISUALIZE BPTREE <index_name> ON <table_name>
            match = re.search(
                r"VISUALIZE\s+BPTREE\s+(\w+)\s+ON\s+(\w+)", query, re.IGNORECASE
            )
            if match:
                # We have both an index name and table name
                result["index_name"] = match.group(1)
                result["table"] = match.group(2)
            else:
                # Check for pattern: VISUALIZE BPTREE ON <table_name>
                match = re.search(
                    r"VISUALIZE\s+BPTREE\s+ON\s+(\w+)", query, re.IGNORECASE
                )
                if match:
                    result["table"] = match.group(1)
                    # Don't set index_name to indicate all indexes on this table
                # Otherwise, just use VISUALIZE BPTREE (for all B+ trees)

        return result

    def _parse_set_operation(self, sql):
        """Parse a set operation (UNION, INTERSECT, EXCEPT)"""
        # Determine which set operation we're dealing with
        operation = None
        if re.search(r'\s+UNION\s+', sql, re.IGNORECASE):
            operation = "UNION"
        elif re.search(r'\s+INTERSECT\s+', sql, re.IGNORECASE):
            operation = "INTERSECT"
        elif re.search(r'\s+EXCEPT\s+', sql, re.IGNORECASE):
            operation = "EXCEPT"

        if not operation:
            return {"error": "Invalid set operation"}

        # Split the query at the operation
        parts = re.split(rf'\s+{operation}\s+', sql, maxsplit=1, flags=re.IGNORECASE)
        if len(parts) != 2:
            return {"error": f"Invalid {operation} statement format"}

        left_query = parts[0].strip()
        right_query = parts[1].strip()

        # Parse both parts as separate queries
        left_plan = self.parse_sql(left_query)
        right_plan = self.parse_sql(right_query)

        # Combine into a set operation plan
        return {
            "type": operation,
            "left": left_plan,
            "right": right_plan,
            "query": sql
        }

    def _extract_select_elements(self, parsed, result):
        """Extract elements from a SELECT query"""
        # Initialize components
        columns = []
        tables = []
        condition = None
        order_by = None
        limit = None
        join_type = None
        join_condition = None

        # Get original SQL to preserve case for column names
        original_sql = str(parsed)

        # Extract columns from SELECT part
        select_match = re.search(r"SELECT\s+(.+?)\s+FROM", original_sql, re.IGNORECASE)
        if select_match:
            columns_part = select_match.group(1).strip()
            columns = [col.strip() for col in columns_part.split(',')]
            result["columns"] = columns

        # Extract table name properly - don't include WHERE clause
        from_match = re.search(r"FROM\s+(\w+(?:\s+\w+)?(?:\s*,\s*\w+(?:\s+\w+)?)*)\s*(?:WHERE|ORDER BY|LIMIT|GROUP BY|HAVING|$)",
                            original_sql, re.IGNORECASE)
        if from_match:
            tables_part = from_match.group(1).strip()
            # Process tables (comma-separated list)
            tables = [table.strip() for table in tables_part.split(',')]
            result["tables"] = tables

        # Extract WHERE clause with special handling for subqueries and logical operators
        where_match = re.search(r"\bWHERE\s+(.+?)(?:\s+ORDER\s+BY|\s+LIMIT|\s*$)",
                                original_sql, re.IGNORECASE)
        if where_match:
            condition = where_match.group(1).strip()
            result["where"] = condition
            result["condition"] = condition

            # Parse logical structure of WHERE clause
            parsed_condition = self._parse_where_condition(condition)
            if parsed_condition:
                result["parsed_condition"] = parsed_condition

        # Extract ORDER BY clause
        order_match = re.search(r"\bORDER\s+BY\s+(.+?)(?:\s+LIMIT|\s*$)",
                                original_sql, re.IGNORECASE)
        if order_match:
            order_by_str = order_match.group(1).strip()
            parts = order_by_str.split()
            # Parse into proper format for tests
            if len(parts) >= 2 and parts[-1].upper() in ("ASC", "DESC"):
                result["order_by"] = {"column": " ".join(parts[:-1]),
                                    "direction": parts[-1].upper()}
            else:
                result["order_by"] = {"column": order_by_str, "direction": "ASC"}

        # Extract LIMIT clause
        limit_match = re.search(r"\bLIMIT\s+(\d+)", original_sql, re.IGNORECASE)
        if limit_match:
            limit = int(limit_match.group(1))
            result["limit"] = limit

        if " FROM " in original_sql.upper():
            parts = original_sql.upper().split(" FROM ", 1)
            if len(parts) > 1:
                from_clause = parts[1].strip()
                # Table part goes up to WHERE, ORDER BY, GROUP BY, HAVING, LIMIT, etc.
                end_keywords = [
                    " WHERE ",
                    " ORDER BY ",
                    " GROUP BY ",
                    " HAVING ",
                    " LIMIT ",
                ]
                table_end = len(from_clause)
                for keyword in end_keywords:
                    pos = from_clause.find(keyword)
                    if pos != -1 and pos < table_end:
                        table_end = pos

                # Get the original case version for tables
                original_from_clause = original_sql.split(" FROM ", 1)[1].strip()[:table_end]
                tables_part = original_from_clause.strip()

                if re.search(r'\s+(JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN|CROSS JOIN)\s+',\
                    from_clause, re.IGNORECASE):
                    # This query has JOIN syntax
                    result["type"] = "JOIN"

                    # First check for CROSS JOIN which doesn't need an ON clause
                    cross_join_match = re.search(
                        r'(\w+)(?:\s+(\w+))?\s+(CROSS\s+JOIN)\s+(\w+)(?:\s+(\w+))?',
                        original_from_clause,
                        re.IGNORECASE
                    )

                    if cross_join_match:
                        table1 = cross_join_match.group(1)
                        alias1 = cross_join_match.group(2) or table1
                        table2 = cross_join_match.group(4)
                        alias2 = cross_join_match.group(5) or table2

                        # Build tables list with aliases
                        tables = [f"{table1} {alias1}", f"{table2} {alias2}"]

                        # Store the join info for the execution engine
                        result["join_info"] = {
                            "type": "CROSS",
                            "condition": None,  # CROSS JOIN has no condition
                            "table1": f"{table1} {alias1}",
                            "table2": f"{table2} {alias2}"
                        }
                    else:
                        # Check for other JOIN types with ON clause
                        join_match = re.search(
                            r'(\w+)(?:\s+(\w+))?\s+(JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN)\s+(\w+)(?:\s+(\w+))?\s+ON\s+(.+?)(?:\s+WITH\s*\(|\s+WHERE|\s+ORDER|\s+LIMIT|\s*$)',
                            original_from_clause,
                            re.IGNORECASE
                        )

                        if join_match:
                            table1 = join_match.group(1)
                            alias1 = join_match.group(2) or table1
                            join_type = join_match.group(3).upper().replace(" JOIN", "")
                            table2 = join_match.group(4)
                            alias2 = join_match.group(5) or table2
                            join_condition = join_match.group(6)

                            # Check for JOIN algorithm hint
                            join_algorithm = "HASH"  # Default
                            hint_match = re.search(r"WITH\s*\(\s*JOIN_TYPE\s*=\s*'(\w+)'\s*\)", original_from_clause, re.IGNORECASE)
                            if hint_match:
                                join_algorithm = hint_match.group(1).upper()

                            # Build tables list with aliases
                            tables = [f"{table1} {alias1}", f"{table2} {alias2}"]

                            # Store the join info for the execution engine
                            result["join_info"] = {
                                "type": join_type,
                                "condition": join_condition,
                                "table1": f"{table1} {alias1}",
                                "table2": f"{table2} {alias2}",
                                "join_algorithm": join_algorithm
                            }
                        else:
                            # Fall back to simple tables if JOIN syntax isn't matched
                            tables = self._process_from_clause(from_clause)
                else:
                    # No JOIN syntax, process as normal table list
                    tables = self._process_from_clause(from_clause)

                # Add tables to result
                result["tables"] = tables

    def _parse_where_condition(self, condition_str):
        """Parse the logical structure of a WHERE condition."""
        if not condition_str:
            return None

        # Look for top-level AND/OR operators
        and_parts = re.split(r'\sAND\s', condition_str, flags=re.IGNORECASE)
        if len(and_parts) > 1:
            return {
                "operator": "AND",
                "operands": [self._parse_where_condition(part) for part in and_parts]
            }

        or_parts = re.split(r'\sOR\s', condition_str, flags=re.IGNORECASE)
        if len(or_parts) > 1:
            return {
                "operator": "OR",
                "operands": [self._parse_where_condition(part) for part in or_parts]
            }

        # Look for NOT operator
        if condition_str.upper().startswith("NOT "):
            return {
                "operator": "NOT",
                "operand": self._parse_where_condition(condition_str[4:])
            }

        # Look for subquery with IN
        in_subquery_match = re.search(r'(.+?)\s+IN\s+\((SELECT.+?)\)', condition_str, re.IGNORECASE)
        if in_subquery_match:
            column = in_subquery_match.group(1).strip()
            subquery = in_subquery_match.group(2).strip()
            subquery_plan = self.parse_sql(subquery)
            return {
                "operator": "IN",
                "column": column,
                "subquery": subquery_plan
            }

        # Simple comparison - identify the operator and operands
        for op in [">=", "<=", "<>", "!=", "=", ">", "<", "LIKE", "NOT LIKE", "IS NULL", "IS NOT NULL"]:
            if op.lower() in condition_str.lower():
                parts = re.split(r'\s*' + re.escape(op) + r'\s*', condition_str, flags=re.IGNORECASE, maxsplit=1)
                if len(parts) == 2:
                    return {
                        "operator": op,
                        "column": parts[0].strip(),
                        "value": parts[1].strip()
                    }

        # If it doesn't match any pattern, return as is
        return {"raw_condition": condition_str}

    def _process_from_clause(self, tables_part):
        """Process FROM clause to extract table names."""
        # Fix: Only extract the table name part before any clauses like WHERE, ORDER BY, etc.
        end_clauses = [" WHERE ", " ORDER BY ", " GROUP BY ", " HAVING ", " LIMIT "]

        # Find the position of the first clause marker
        end_pos = len(tables_part)
        for clause in end_clauses:
            pos = tables_part.upper().find(clause)
            if pos != -1 and pos < end_pos:
                end_pos = pos

        # Extract only the table portion
        table_part = tables_part[:end_pos].strip()

        # Now split by commas for multiple tables
        if " JOIN " not in table_part:
            tables = [t.strip() for t in table_part.split(",")]
            return tables
        else:
            # Handle JOIN syntax (existing code)
            return []

    def _extract_join_tables(self, tables_part, join_keyword):
        """Extract table names from a JOIN clause"""
        parts = tables_part.split(join_keyword)
        if len(parts) < 2:
            return []

        left_table = parts[0].strip().split()[-1]  # Get last word before JOIN

        # Get right table (up to ON clause or end)
        right_part = parts[1]
        if " ON " in right_part:
            right_table = right_part.split(" ON ")[0].strip()
        else:
            right_table = right_part.strip()

        # Return table names as-is without converting to uppercase
        return [left_table, right_table]

    def _extract_insert_elements(self, parsed, result):
        """Extract elements from an INSERT statement"""
        # Get original SQL to preserve case
        raw_sql = str(parsed)

        # Extract table name
        table_match = re.search(
            r"INSERT\s+INTO\s+(\w+)", raw_sql, re.IGNORECASE)
        table_name = table_match.group(1) if table_match else None

        # Extract column names
        columns = []
        if "(" in raw_sql and ")" in raw_sql:
            # First set of parentheses should be column names
            cols_match = re.search(
                r"INSERT\s+INTO\s+\w+\s*\(([^)]+)\)", raw_sql, re.IGNORECASE
            )
            if cols_match:
                cols_str = cols_match.group(1)
                columns = [col.strip() for col in cols_str.split(",")]

        # Extract values
        values = []

        # Look for VALUES clause with multiple rows
        values_clause = re.search(r"VALUES\s*(.+)", raw_sql, re.IGNORECASE)
        if values_clause:
            values_str = values_clause.group(1).strip()

            # Split into individual rows by finding all patterns of (val1, val2, ...)
            row_pattern = r"\(([^)]+)\)"
            rows = re.findall(row_pattern, values_str)

            for row in rows:
                row_values = []
                # Split by comma and process each value
                for val in row.split(","):
                    val = val.strip()
                    # Keep quotes for strings
                    if (val.startswith("'") and val.endswith("'")) or\
                        (val.startswith('"') and val.endswith('"')):
                        row_values.append(val)
                    elif val.isdigit():
                        # Integer
                        row_values.append(int(val))
                    elif re.match(r"^[0-9]+\.[0-9]+$", val):
                        # Float
                        row_values.append(float(val))
                    else:
                        # Other
                        row_values.append(val)

                values.append(row_values)

        # Update result with extracted components
        result.update(
            {"table": table_name, "columns": columns, "values": values})

    def _extract_update_elements(self, parsed, result):
        """Extract elements from an UPDATE statement"""
        table_name = None
        set_pairs = {}
        where_clause = None

        # Get original SQL to preserve case
        raw_sql = str(parsed)

        # Extract table name (comes after UPDATE keyword)
        table_match = re.search(r"UPDATE\s+(\w+)", raw_sql, re.IGNORECASE)
        if table_match:
            table_name = table_match.group(1)

        # Extract SET clause
        set_match = re.search(
            r"SET\s+(.*?)(?:\s+WHERE\s+|$)", raw_sql, re.IGNORECASE)
        if set_match:
            set_clause = set_match.group(1).strip()
            logging.debug("Extracted SET clause: %s", set_clause)

            # Process each assignment (column = value)
            for assignment in set_clause.split(","):
                if "=" in assignment:
                    column, value = assignment.split("=", 1)
                    value = value.strip()

                    # Convert value types as needed
                    if value.isdigit():
                        set_pairs[column.strip()] = int(value)
                    elif re.match(r"^[0-9]+\.[0-9]+$", value):
                        set_pairs[column.strip()] = float(value)
                    else:
                        set_pairs[column.strip()] = value

            logging.debug("Final SET pairs: %s", set_pairs)

        # Extract WHERE clause
        where_match = re.search(r"WHERE\s+(.+)$", raw_sql, re.IGNORECASE)
        if where_match:
            where_clause = where_match.group(1).strip()

        # Update result with extracted components
        result.update(
            {
                "table": table_name,
                "set": set_pairs,
                "where": where_clause,
                "condition": where_clause,  # Also add as "condition" for consistency
            }
        )

    def _extract_delete_elements(self, parsed, result):
        """Extract elements from a DELETE statement"""
        table_name = None
        where_clause = None

        # Get original SQL to preserve case
        raw_sql = str(parsed)

        # Extract table name (comes after FROM keyword)
        table_match = re.search(
            r"DELETE\s+FROM\s+(\w+)", raw_sql, re.IGNORECASE)
        if table_match:
            table_name = table_match.group(1)

        # Extract WHERE clause
        where_match = re.search(r"WHERE\s+(.+)$", raw_sql, re.IGNORECASE)
        if where_match:
            where_clause = where_match.group(1).strip()

        # Update result with extracted components
        result.update(
            {
                "table": table_name,
                "where": where_clause,
                "condition": where_clause,  # Also add as "condition" for consistency
            }
        )

    def _extract_create_elements(self, parsed, result):
        """Extract elements from a CREATE statement"""
        table_name = None
        database_name = None
        index_name = None
        column_name = None
        is_unique = False
        columns = []
        constraints = []

        # Get original SQL to preserve case
        raw_sql = str(parsed)

        # Check the statement type
        create_type = None
        if re.search(r"CREATE\s+TABLE", raw_sql, re.IGNORECASE):
            create_type = "TABLE"
        elif re.search(r"CREATE\s+DATABASE", raw_sql, re.IGNORECASE):
            create_type = "DATABASE"
        elif re.search(r"CREATE\s+UNIQUE\s+INDEX", raw_sql, re.IGNORECASE):
            create_type = "INDEX"
            is_unique = True
        elif re.search(r"CREATE\s+INDEX", raw_sql, re.IGNORECASE):
            create_type = "INDEX"

        # Extract index details
        if create_type == "INDEX":
            # Split into tokens for better parsing
            tokens = raw_sql.split()

            # Find the index name - it will be after CREATE INDEX or CREATE UNIQUE INDEX
            idx = 2
            if is_unique:
                idx = 3  # Skip UNIQUE

            if idx < len(tokens):
                index_name = tokens[idx]
                idx += 1

                # Look for ON keyword
                if idx < len(tokens) and tokens[idx].upper() == "ON":
                    idx += 1
                    if idx < len(tokens):
                        table_name = tokens[idx]
                        idx += 1

                        # Look for column in parentheses or as next token
                        if idx < len(tokens):
                            if "(" in tokens[idx]:
                                # Extract column from parentheses
                                column_text = " ".join(tokens[idx:])
                                match = re.search(
                                    r"\(\s*([^,\s\)]+)", column_text)
                                if match:
                                    column_name = match.group(1)
                            else:
                                # Take next token as column name
                                column_name = tokens[idx]

            # Update result with the correct type for execution engine
            result["type"] = "CREATE_INDEX"
            result.update(
                {
                    "index_name": index_name,
                    "table": table_name,
                    "column": column_name,
                    "unique": is_unique,
                }
            )
            return

        # Extract table details
        elif create_type == "TABLE":
            # Extract table name
            match = re.search(r"CREATE\s+TABLE\s+(\w+)",
                              raw_sql, re.IGNORECASE)
            if match:
                table_name = match.group(1)

            # Extract column definitions
            if "(" in raw_sql and ")" in raw_sql:
                col_text = raw_sql.split("(", 1)[1].rsplit(")", 1)[0]
                col_defs = [c.strip() for c in col_text.split(",")]
                for col_def in col_defs:
                    if col_def:
                        if re.match(
                            r"^\s*(PRIMARY|FOREIGN|UNIQUE|CHECK|CONSTRAINT)",
                            col_def,
                            re.IGNORECASE,
                        ):
                            constraints.append(col_def)
                        else:
                            columns.append(col_def)

            # Update result type
            result["type"] = "CREATE_TABLE"

        # Extract database name
        elif create_type == "DATABASE":
            match = re.search(r"CREATE\s+DATABASE\s+(\w+)",
                              raw_sql, re.IGNORECASE)
            if match:
                database_name = match.group(1)

            # Update result type
            result["type"] = "CREATE_DATABASE"

        # Update result with extracted components
        result.update(
            {
                "create_type": create_type,
                "table": table_name,
                "database": database_name,
                "index_name": index_name,
                "column": column_name,
                "unique": is_unique,
                "columns": columns,
                "constraints": constraints,
            }
        )

    def _extract_drop_elements(self, parsed, result):
        """Extract elements from a DROP statement"""
        table_name = None
        database_name = None
        index_name = None

        # Get original SQL to preserve case
        raw_sql = str(parsed)

        # Determine what type of DROP statement this is
        drop_type = None
        if re.search(r"DROP\s+TABLE", raw_sql, re.IGNORECASE):
            drop_type = "TABLE"
        elif re.search(r"DROP\s+DATABASE", raw_sql, re.IGNORECASE):
            drop_type = "DATABASE"
        elif re.search(r"DROP\s+INDEX", raw_sql, re.IGNORECASE):
            drop_type = "INDEX"
        elif re.search(r"DROP\s+VIEW", raw_sql, re.IGNORECASE):
            drop_type = "VIEW"

        # Extract the names based on the type
        if drop_type == "TABLE":
            # Format: DROP TABLE table_name
            match = re.search(r"DROP\s+TABLE\s+(\w+)", raw_sql, re.IGNORECASE)
            if match:
                table_name = match.group(1)

        elif drop_type == "DATABASE":
            # Format: DROP DATABASE db_name
            match = re.search(r"DROP\s+DATABASE\s+(\w+)",
                              raw_sql, re.IGNORECASE)
            if match:
                database_name = match.group(1)

        elif drop_type == "INDEX":
            # Format: DROP INDEX index_name ON table_name
            match = re.search(
                r"DROP\s+INDEX\s+(\w+)\s+ON\s+(\w+)", raw_sql, re.IGNORECASE
            )
            if match:
                index_name = match.group(1)
                table_name = match.group(2)

        # Update result with extracted components
        result.update(
            {
                "drop_type": drop_type,
                "table": table_name,
                "database": database_name,
                "index": index_name,
            }
        )

    def _extract_show_elements(self, parsed, result):
        """Extract elements from a SHOW statement"""
        object_type = None
        table_name = None

        # Get the raw SQL directly
        raw_sql = str(parsed).upper()

        if "SHOW DATABASES" in raw_sql:
            object_type = "DATABASES"
        elif "SHOW ALL_TABLES" in raw_sql:  # Add support for SHOW ALL_TABLES
            object_type = "ALL_TABLES"
        elif "SHOW TABLES" in raw_sql:
            object_type = "TABLES"
        elif "SHOW INDEXES FOR" in raw_sql:
            object_type = "INDEXES"
            # Extract table name after FOR
            parts = str(parsed).split("FOR", 1)
            if len(parts) > 1:
                table_name = parts[1].strip()
        elif "SHOW INDEXES" in raw_sql or "SHOW INDICES" in raw_sql:
            object_type = "INDEXES"
        elif "SHOW COLUMNS" in raw_sql:
            object_type = "COLUMNS"
            # Extract table name
            if "FROM" in raw_sql:
                parts = str(parsed).split("FROM", 1)
                if len(parts) > 1:
                    table_name = parts[1].strip()

        # Update result
        result.update({"object": object_type, "table": table_name})

        # Log the extracted components for debugging
        logging.debug("SHOW command: object_type=%s, table=%s", object_type, table_name)

    def _extract_create_index_elements(self, parsed, result):
        """Extract elements from a CREATE INDEX statement"""
        index_name = None
        table_name = None
        column_name = None
        is_unique = False

        # Check if it's a UNIQUE INDEX
        for token in parsed.tokens:
            if token.is_keyword and token.value.upper() == "UNIQUE":
                is_unique = True
                break

        # Process tokens to extract other elements
        for i, token in enumerate(parsed.tokens):
            if token.is_whitespace:
                continue

            # Find the index name
            if (
                token.ttype is None
                and isinstance(token, sqlparse.sql.Identifier)
                and not index_name
            ):
                # This should be right after CREATE INDEX
                index_name = str(token)

            # Find the ON keyword and the table name after it
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "ON":
                if i + 1 < len(parsed.tokens):
                    table_token = parsed.tokens[i + 1]
                    if not table_token.is_whitespace:
                        table_name = str(table_token)

            # Find the column in parentheses
            if str(token) == "(" and table_name:
                if i + 1 < len(parsed.tokens):
                    column_token = parsed.tokens[i + 1]
                    if isinstance(column_token, sqlparse.sql.Identifier):
                        column_name = str(column_token)
                    elif isinstance(column_token, sqlparse.sql.IdentifierList):
                        # For now, just take the first column if there are multiple
                        identifiers = list(column_token.get_identifiers())
                        if identifiers:
                            column_name = str(identifiers[0])

        # Update result
        result.update(
            {
                "index": index_name,
                "table": table_name,
                "column": column_name,
                "unique": is_unique,
            }
        )

    def parse_create_index(self, sql):
        """Parse CREATE INDEX statement"""
        logging.debug("Parsing CREATE INDEX: %s", sql)

        # Check for UNIQUE index
        is_unique = "UNIQUE" in sql.upper()

        # Extract index name
        index_pattern = r"CREATE\s+(UNIQUE\s+)?INDEX\s+(\w+)"
        index_match = re.search(index_pattern, sql, re.IGNORECASE)
        if not index_match:
            return {"error": "Could not parse index name"}
        index_name = index_match.group(2)

        # Extract table name
        table_pattern = r"ON\s+(\w+)"
        table_match = re.search(table_pattern, sql, re.IGNORECASE)
        if not table_match:
            return {"error": "Could not parse table name"}
        table_name = table_match.group(1)

        # Extract column name
        # Try both with and without parentheses
        column_pattern = r"ON\s+\w+\s*\((\w+)\)"
        column_match = re.search(column_pattern, sql, re.IGNORECASE)

        if column_match:
            column_name = column_match.group(1)
        else:
            # Try without parentheses - just the last word
            words = sql.split()
            if "(" in words[-1]:
                # Handle case where there's no space: ON table(column)
                column_name = words[-1].strip("()")
            else:
                # Take the last word after table name
                remaining = sql.split(f"ON {table_name}", 1)[1].strip()
                if remaining.startswith("("):
                    # Handle ON table (column)
                    column_name = remaining.strip("() ")
                else:
                    # Just take the next word after table name
                    parts = remaining.split()
                    column_name = parts[0] if parts else None

        if not column_name:
            return {"error": "Could not parse column name"}

        return {
            "type": "CREATE_INDEX",
            "index_name": index_name,
            "table": table_name,
            "column": column_name,
            "unique": is_unique,
        }

    def _parse_insert_statement(self, query):
        """Parse an INSERT statement."""
        match = re.match(r"INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*(.+)",
                        query, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid INSERT statement format")

        table = match.group(1)
        columns = [col.strip() for col in match.group(2).split(',')]
        values_part = match.group(3)

        # Fix: Support multiple rows in VALUES clause
        value_groups = []
        # Split by '),(' to get individual value groups
        current_group = ""
        in_string = False
        paren_level = 0

        for char in values_part:
            if char == "'" and not in_string:
                in_string = True
                current_group += char
            elif char == "'" and in_string:
                in_string = False
                current_group += char
            elif char == '(' and not in_string:
                paren_level += 1
                if paren_level == 1:  # Start of a new group
                    current_group = ""
                else:
                    current_group += char
            elif char == ')' and not in_string:
                paren_level -= 1
                if paren_level == 0:  # End of a group
                    value_groups.append(current_group)
                else:
                    current_group += char
            else:
                if paren_level > 0:  # Only add if we're inside a group
                    current_group += char

        # Process each value group
        processed_value_groups = []
        for group in value_groups:
            value_list = []
            current_value = ""
            in_string = False

            for char in group + ",":  # Add a comma to handle the last value
                if char == "'" and not in_string:
                    in_string = True
                    current_value += char
                elif char == "'" and in_string:
                    in_string = False
                    current_value += char
                elif char == ',' and not in_string:
                    value_list.append(self._process_value_literal(current_value.strip()))
                    current_value = ""
                else:
                    current_value += char

            processed_value_groups.append(value_list)

        return {
            "type": "INSERT",
            "table": table,
            "columns": columns,
            "values": processed_value_groups
        }

    def _process_value_literal(self, value):
        """Process a value literal, preserving quotes for strings."""
        if value is None:
            return None

        # Preserve quotes for string literals
        if isinstance(value, str):
            if (value.startswith("'") and value.endswith("'")) or\
                (value.startswith('"') and value.endswith('"')):
                return value  # Keep quotes intact

            # Try to convert to numeric types if possible
            try:
                if '.' in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                # Not a number, return as is
                return value

        return value
