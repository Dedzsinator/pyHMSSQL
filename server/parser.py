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
            if sql.strip().upper().startswith(
                "CREATE INDEX"
            ) or sql.strip().upper().startswith("CREATE UNIQUE INDEX"):
                return self.parse_create_index(sql)

            # Parse SQL query
            parsed = sqlparse.parse(sql)
            if not parsed:
                return {"error": "Failed to parse SQL statement"}

            # Get the first statement (we only support one at a time)
            stmt = parsed[0]

            # Handle SHOW commands specially since sqlparse may not recognize them correctly
            if sql.strip().upper().startswith("SHOW "):
                result = {"type": "SHOW", "query": sql}
                self._extract_show_elements(stmt, result)
                return result

            # Handle USE DATABASE command
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
        except Exception as e:
            logging.error(f"Error extracting elements: {str(e)}")
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

        # Convert to uppercase only for keyword detection
        raw_sql = original_sql.upper()

        if " FROM " in raw_sql:
            parts = raw_sql.split(" FROM ", 1)
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

                # Improved JOIN handling
                tables_part = from_clause[:table_end].strip()
                join_type = "INNER"  # Default join type

                # Handle different JOIN types
                if " JOIN " in tables_part:
                    # Extract join information
                    join_info = {}

                    # Match different join patterns
                    if " LEFT JOIN " in tables_part:
                        join_type = "LEFT"
                        tables = self._extract_join_tables(
                            tables_part, " LEFT JOIN ")
                    elif " RIGHT JOIN " in tables_part:
                        join_type = "RIGHT"
                        tables = self._extract_join_tables(
                            tables_part, " RIGHT JOIN ")
                    elif (
                        " FULL JOIN " in tables_part
                        or " FULL OUTER JOIN " in tables_part
                    ):
                        join_type = "FULL"
                        tables = self._extract_join_tables(
                            tables_part, " FULL JOIN "
                        ) or self._extract_join_tables(tables_part, " FULL OUTER JOIN ")
                    elif " CROSS JOIN " in tables_part:
                        join_type = "CROSS"
                        tables = self._extract_join_tables(
                            tables_part, " CROSS JOIN ")
                    elif " INNER JOIN " in tables_part:
                        join_type = "INNER"
                        tables = self._extract_join_tables(
                            tables_part, " INNER JOIN ")
                    else:
                        # Regular JOIN is INNER JOIN
                        join_type = "INNER"
                        tables = self._extract_join_tables(
                            tables_part, " JOIN ")

                    # Extract join condition from ON clause
                    join_condition = None
                    if " ON " in tables_part:
                        on_parts = tables_part.split(" ON ", 1)
                        if len(on_parts) > 1:
                            join_condition = on_parts[1].strip()
                            # Handle condition with ending clauses
                            for clause in [
                                " WHERE ",
                                " GROUP BY ",
                                " HAVING ",
                                " ORDER BY ",
                                " LIMIT ",
                            ]:
                                if clause in join_condition:
                                    join_condition = join_condition.split(clause, 1)[
                                        0
                                    ].strip()

                    join_info = {
                        "type": join_type,
                        "condition": join_condition,
                        "tables": tables,
                    }

                    result["join_info"] = join_info
                else:
                    # Simple table list without joins
                    tables = [t.strip() for t in tables_part.split(",")]

        # Now extract column names FROM THE ORIGINAL SQL to preserve case
        if " FROM " in raw_sql:
            orig_parts = original_sql.split(" FROM ", 1)
            col_part = orig_parts[0].replace("SELECT", "", 1).strip()

            # Debug logging
            logging.error(f"Column part before processing: '{col_part}'")

            # Always process columns regardless of aggregate functions
            if col_part == "*":
                columns = ["*"]
            else:
                # Split columns preserving case
                columns = [c.strip() for c in col_part.split(",")]
                logging.error(f"Extracted columns: {columns}")

                # Additional log for aggregate detection
                for col in columns:
                    if "(" in col and ")" in col:
                        logging.error(
                            f"Potential aggregate function found: {col}")

        # Extract WHERE condition
        if " WHERE " in raw_sql:
            where_parts = raw_sql.split(" WHERE ", 1)
            if len(where_parts) > 1:
                condition_part = where_parts[1]
                # Condition ends at the next clause
                end_keywords = [" ORDER BY ",
                                " GROUP BY ", " HAVING ", " LIMIT "]
                condition_end = len(condition_part)
                for keyword in end_keywords:
                    pos = condition_part.find(keyword)
                    if pos != -1 and pos < condition_end:
                        condition_end = pos

                # Get condition from original SQL to preserve case
                orig_where_parts = original_sql.split(" WHERE ", 1)
                if len(orig_where_parts) > 1:
                    orig_condition_part = orig_where_parts[1]
                    condition = orig_condition_part[:condition_end].strip()
                else:
                    condition = condition_part[:condition_end].strip()

        # Extract ORDER BY
        if " ORDER BY " in raw_sql:
            order_parts = raw_sql.split(" ORDER BY ", 1)
            if len(order_parts) > 1:
                order_part = order_parts[1]
                # Order by ends at LIMIT
                limit_pos = order_part.find(" LIMIT ")
                if limit_pos != -1:
                    order_by = order_part[:limit_pos].strip()
                else:
                    order_by = order_part.strip()

        # Extract LIMIT
        if " LIMIT " in raw_sql:
            limit_parts = raw_sql.split(" LIMIT ", 1)
            if len(limit_parts) > 1:
                limit = limit_parts[1].strip()
                try:
                    limit = int(limit)
                except:
                    limit = None

        # Update result with extracted components
        result.update(
            {
                "columns": columns,
                "tables": tables,
                "condition": condition,
                "order_by": order_by,
                "limit": limit,
                "join_type": join_type,
                "join_condition": join_condition,
            }
        )

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
        values_match = re.search(
            r"VALUES\s*\(([^)]+)\)", raw_sql, re.IGNORECASE)
        if values_match:
            values_str = values_match.group(1)
            value_list = []

            # Parse individual values, handling strings and numbers
            for val in values_str.split(","):
                val = val.strip()
                if val.startswith("'") and val.endswith("'"):
                    # String value
                    value_list.append(val[1:-1])
                elif val.isdigit():
                    # Integer
                    value_list.append(int(val))
                elif re.match(r"^[0-9]+\.[0-9]+$", val):
                    # Float
                    value_list.append(float(val))
                else:
                    # Other (keep as is)
                    value_list.append(val)

            values.append(value_list)

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

        # Extract SET clause - improved regex to properly match everything between SET and WHERE (or end of string)
        set_match = re.search(
            r"SET\s+(.*?)(?:\s+WHERE\s+|$)", raw_sql, re.IGNORECASE)
        if set_match:
            set_clause = set_match.group(1).strip()
            logging.debug(f"Extracted SET clause: {set_clause}")

            # Process each assignment (column = value)
            for assignment in set_clause.split(","):
                if "=" in assignment:
                    column, value = assignment.split("=", 1)
                    set_pairs[column.strip()] = value.strip()

            logging.debug(f"Final SET pairs: {set_pairs}")

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
        logging.debug(f"SHOW command: object_type={
                      object_type}, table={table_name}")

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
        logging.debug(f"Parsing CREATE INDEX: {sql}")

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
