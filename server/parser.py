import re
import logging
import traceback
import sqlparse
from haskell_parser import HaskellParser

class SQLParser:
    """
    SQL Parser for translating SQL statements into structured query plans.
    Handles various SQL commands including SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, and SHOW.
    Also handles specialized visualization commands.
    """

    def __init__(self, engine=None):
        self.engine = engine
        
        # Initialize the Haskell parser with better error reporting
        try:
            self.haskell_parser = HaskellParser()
            self.use_haskell_parser = True
            logging.info("✅ Successfully initialized Haskell SQL parser for enhanced performance")
            print("✅ Haskell SQL parser initialized successfully")
        except FileNotFoundError as e:
            logging.warning(f"❌ Haskell parser binary not found: {e}")
            print(f"❌ Haskell parser not found: {e}")
            logging.warning("🔄 Falling back to Python parser")
            print("🔄 Falling back to Python parser")
            self.use_haskell_parser = False
        except Exception as e:
            logging.warning(f"❌ Failed to initialize Haskell parser: {e}")
            print(f"❌ Failed to initialize Haskell parser: {e}")
            logging.warning("🔄 Falling back to Python parser")
            print("🔄 Falling back to Python parser")
            self.use_haskell_parser = False

    # Modify parse_sql to use Haskell parser when available
    def parse_sql(self, sql):
        """
        Parse SQL query into a structured format.
        """
        if not sql or not sql.strip():
            return {"error": "Empty query"}
        
        # Use Haskell parser if available for all SQL statements
        if self.use_haskell_parser:
            try:
                logging.info("🚀 Using Haskell parser for query: %s", sql[:50] + "..." if len(sql) > 50 else sql)
                # Try parsing with the Haskell parser
                result = self.haskell_parser.parse(sql)
                logging.info("✅ Haskell parser succeeded")
                return result
            except Exception as e:
                # Log the error and fall back to Python parser
                logging.warning(f"❌ Haskell parser error: {str(e)}")
                logging.warning("🔄 Falling back to Python parser for this query")
                # Continue with Python parser

        # Log when using Python parser
        logging.info("🐍 Using Python parser for query: %s", sql[:50] + "..." if len(sql) > 50 else sql)

        # Original Python parser logic follows
        if sql.strip().upper().startswith("SCRIPT "):
            return self._parse_script_statement(sql)

        # Parse transaction control statements first (they need special handling)
        transaction_result = self._parse_transaction_statement(sql)
        if transaction_result:
            return transaction_result

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

    def _parse_script_statement(self, sql):
        """Parse a SCRIPT statement."""
        # Extract filename from SCRIPT command
        match = re.match(r"SCRIPT\s+(.+)", sql.strip(), re.IGNORECASE)
        if not match:
            return {"error": "Invalid SCRIPT statement format", "status": "error"}
        
        filename = match.group(1).strip()
        
        # Remove quotes if present
        if filename.startswith('"') and filename.endswith('"'):
            filename = filename[1:-1]
        elif filename.startswith("'") and filename.endswith("'"):
            filename = filename[1:-1]
        
        return {
            "type": "SCRIPT",
            "filename": filename,
            "operation": "SCRIPT"
        }

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

    def _parse_transaction_statement(self, sql_string):
        """Parse transaction control statements."""
        # Normalize the SQL string for easier pattern matching
        sql_upper = sql_string.strip().upper()

        # BEGIN TRANSACTION pattern
        if re.match(r'^BEGIN\s+TRANSACTION\s*$|^START\s+TRANSACTION\s*$', sql_upper):
            logging.info("Detected BEGIN TRANSACTION statement")
            return {
                "type": "BEGIN_TRANSACTION"
            }

        # COMMIT TRANSACTION pattern
        if re.match(r'^COMMIT\s+TRANSACTION\s*$|^COMMIT\s*$', sql_upper):
            logging.info("Detected COMMIT TRANSACTION statement")
            return {
                "type": "COMMIT"
            }

        # ROLLBACK TRANSACTION pattern
        if re.match(r'^ROLLBACK\s+TRANSACTION\s*$|^ROLLBACK\s*$', sql_upper):
            logging.info("Detected ROLLBACK TRANSACTION statement")
            return {
                "type": "ROLLBACK"
            }

        # Not a transaction statement
        return None

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
        distinct = False
        

        # Get original SQL to preserve case for column names
        original_sql = str(parsed)

        if re.search(r"SELECT\s+DISTINCT\s+", original_sql, re.IGNORECASE):
            distinct = True
            result["distinct"] = True

        # Extract columns from SELECT part
        select_part_regex = r"SELECT\s+(DISTINCT\s+)?(.+?)\s+FROM"
        select_match = re.search(select_part_regex, original_sql, re.IGNORECASE)
        if select_match:
            # Get the columns part, ignoring the DISTINCT keyword if present
            is_distinct = select_match.group(1) is not None
            columns_part = select_match.group(2).strip()

            # Handle DISTINCT operation specially if only one column
            if is_distinct and "," not in columns_part:
                # Extract the simple column name
                column_name = columns_part.strip()
                # Set type to DISTINCT operation
                result["type"] = "DISTINCT"
                result["column"] = column_name
                result["operation"] = "DISTINCT"
            else:
                # Regular columns handling
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

        # CRITICAL FIX: Improved JOIN parsing with WHERE condition handling
        if " FROM " in original_sql.upper():
            # Get the FROM clause up to WHERE/ORDER BY/etc
            from_part = re.search(r"FROM\s+(.+?)(?:\s+WHERE|\s+ORDER\s+BY|\s+GROUP\s+BY|\s+HAVING|\s+LIMIT|\s*$)", 
                                original_sql, re.IGNORECASE)
            
            if from_part:
                from_clause = from_part.group(1).strip()
                
                # Check for JOIN syntax
                join_match = re.search(r'\s+(INNER\s+JOIN|JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|CROSS\s+JOIN)\s+', 
                                     from_clause, re.IGNORECASE)
                
                if join_match:
                    result["type"] = "JOIN"
                    
                    # Parse the join more carefully
                    join_pattern = r'(\w+)(?:\s+(\w+))?\s+(INNER\s+JOIN|JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|CROSS\s+JOIN)\s+(\w+)(?:\s+(\w+))?(?:\s+ON\s+(.+?))?(?:\s+WHERE|\s*$)'
                    
                    match = re.search(join_pattern, from_clause, re.IGNORECASE)
                    if match:
                        table1 = match.group(1)
                        alias1 = match.group(2) or table1
                        join_type_full = match.group(3).upper()
                        table2 = match.group(4) 
                        alias2 = match.group(5) or table2
                        join_condition = match.group(6) if match.group(6) else None
                        
                        # Clean up join type
                        join_type = join_type_full.replace(" JOIN", "").replace("INNER", "INNER")
                        if join_type == "JOIN":
                            join_type = "INNER"
                        
                        # Set tables correctly
                        tables = [table1, table2]
                        
                        # Store join info
                        result["join_info"] = {
                            "type": join_type,
                            "condition": join_condition,
                            "table1": table1,
                            "table2": table2,
                            "join_algorithm": "HASH"  # Default
                        }
                        
                        # Check for algorithm hints
                        hint_match = re.search(r"WITH\s*\(\s*JOIN_TYPE\s*=\s*'(\w+)'\s*\)", original_sql, re.IGNORECASE)
                        if hint_match:
                            result["join_info"]["join_algorithm"] = hint_match.group(1).upper()
                    
                    else:
                        # Fallback: try to extract tables from a simpler pattern
                        tables_before_join = from_clause.split(join_match.group(0))[0].strip()
                        tables_after_join = from_clause.split(join_match.group(0))[1].strip()
                        
                        # Extract table names
                        table1 = tables_before_join.split()[-1] if tables_before_join else ""
                        
                        # For table2, take everything before ON or end
                        if " ON " in tables_after_join:
                            table2_part = tables_after_join.split(" ON ")[0].strip()
                            join_condition = tables_after_join.split(" ON ")[1].strip()
                        else:
                            table2_part = tables_after_join.strip()
                            join_condition = None
                        
                        table2 = table2_part.split()[0] if table2_part else ""
                        
                        if table1 and table2:
                            tables = [table1, table2]
                            result["join_info"] = {
                                "type": "INNER",
                                "condition": join_condition,
                                "table1": table1,
                                "table2": table2,
                                "join_algorithm": "HASH"
                            }
                        else:
                            # Fallback to original tables extraction
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

        # Strip trailing semicolons from the condition string
        condition_str = condition_str.rstrip(';')

        # Look for top-level AND/OR operators
        and_parts = re.split(r'\sAND\s', condition_str, flags=re.IGNORECASE)
        if len(and_parts) > 1:
            return {
                "operator": "AND",
                "operands": [self._parse_where_condition(part.strip()) for part in and_parts]
            }

        or_parts = re.split(r'\sOR\s', condition_str, flags=re.IGNORECASE)
        if len(or_parts) > 1:
            return {
                "operator": "OR",
                "operands": [self._parse_where_condition(part.strip()) for part in or_parts]
            }

        # Look for NOT operator
        if condition_str.upper().startswith("NOT "):
            return {
                "operator": "NOT",
                "operand": self._parse_where_condition(condition_str[4:].strip())
            }

        # Look for subquery with IN
        in_subquery_match = re.search(r'(.+?)\s+IN\s+\((SELECT.+?)\)', condition_str, re.IGNORECASE)
        if in_subquery_match:
            column = in_subquery_match.group(1).strip()
            subquery = in_subquery_match.group(2).strip()
            return {
                "operator": "IN",
                "column": column,
                "subquery": subquery
            }

        # Simple comparison - identify the operator and operands
        for op in [">=", "<=", "<>", "!=", "=", ">", "<", "LIKE", "NOT LIKE", "IS NULL", "IS NOT NULL"]:
            if f" {op} " in condition_str.upper():
                parts = re.split(rf'\s+{re.escape(op)}\s+', condition_str, maxsplit=1, flags=re.IGNORECASE)
                if len(parts) == 2:
                    column = parts[0].strip()
                    value_str = parts[1].strip()
                    
                    # Parse the value
                    if value_str.upper() == "NULL":
                        value = None
                    elif value_str.startswith("'") and value_str.endswith("'"):
                        value = value_str[1:-1]  # Remove quotes
                    elif value_str.startswith('"') and value_str.endswith('"'):
                        value = value_str[1:-1]  # Remove quotes
                    else:
                        try:
                            if '.' in value_str:
                                value = float(value_str)
                            else:
                                value = int(value_str)
                        except ValueError:
                            value = value_str
                    
                    return {
                        "operator": op.upper(),
                        "column": column,
                        "value": value
                    }
                break

        # If no operator found, return as raw condition
        return {
            "operator": "RAW",
            "condition": condition_str
        }

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

        # Extract column names (optional)
        columns = []
        columns_match = re.search(r"\(\s*([^)]+)\s*\)\s*VALUES", raw_sql, re.IGNORECASE)
        if columns_match:
            # Parse column names
            columns_str = columns_match.group(1)
            columns = [col.strip() for col in columns_str.split(',')]
        else:
            # Check if it's INSERT INTO table VALUES format (no column specification)
            if re.search(r"INSERT\s+INTO\s+\w+\s+VALUES", raw_sql, re.IGNORECASE):
                # No columns specified - will need to infer from table schema
                columns = None

        # Extract values - handle multiple rows
        values = []
        values_clause = re.search(r"VALUES\s*(.+)", raw_sql, re.IGNORECASE | re.DOTALL)
        if values_clause:
            values_str = values_clause.group(1).strip()
            
            # Handle multiple value sets: VALUES (...), (...), (...)
            # Split by ), ( pattern but be careful with nested parentheses
            value_sets = []
            current_set = ""
            paren_count = 0
            i = 0
            
            while i < len(values_str):
                char = values_str[i]
                current_set += char
                
                if char == '(':
                    paren_count += 1
                elif char == ')':
                    paren_count -= 1
                    if paren_count == 0:
                        # Found complete value set
                        value_sets.append(current_set.strip())
                        current_set = ""
                        # Skip comma and whitespace
                        while i + 1 < len(values_str) and values_str[i + 1] in ', \t\n':
                            i += 1
                i += 1
            
            # Process each value set
            for value_set in value_sets:
                if value_set.startswith('(') and value_set.endswith(')'):
                    # Remove outer parentheses
                    inner_values = value_set[1:-1]
                    
                    # Parse individual values
                    row_values = []
                    current_value = ""
                    in_quotes = False
                    quote_char = None
                    
                    for char in inner_values:
                        if char in ('"', "'") and not in_quotes:
                            in_quotes = True
                            quote_char = char
                            current_value += char
                        elif char == quote_char and in_quotes:
                            in_quotes = False
                            quote_char = None
                            current_value += char
                        elif char == ',' and not in_quotes:
                            # End of value
                            row_values.append(self._process_value_literal(current_value.strip()))
                            current_value = ""
                        else:
                            current_value += char
                    
                    # Don't forget the last value
                    if current_value.strip():
                        row_values.append(self._process_value_literal(current_value.strip()))
                    
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
            match = re.search(r"CREATE\s+TABLE\s+(\w+)", raw_sql, re.IGNORECASE)
            if match:
                table_name = match.group(1)

            # Extract column definitions
            if "(" in raw_sql and ")" in raw_sql:
                columns_str = raw_sql.split("(", 1)[1].rsplit(")", 1)[0].strip()

                # Parse compound primary key constraints
                compound_pk_match = re.search(
                    r'PRIMARY\s+KEY\s*\(\s*([^)]+)\s*\)', 
                    columns_str, 
                    re.IGNORECASE
                )
                
                compound_pk_columns = []
                if compound_pk_match:
                    # Extract compound primary key columns
                    pk_cols_str = compound_pk_match.group(1)
                    compound_pk_columns = [col.strip() for col in pk_cols_str.split(',')]
                    
                    # Remove the PRIMARY KEY constraint from columns_str for processing
                    columns_str = re.sub(
                        r',?\s*PRIMARY\s+KEY\s*\([^)]+\)', 
                        '', 
                        columns_str, 
                        flags=re.IGNORECASE
                    ).strip()

                # Process individual columns
                depth = 0
                current = ""
                in_string = False

                for char in columns_str + ",":
                    if char == "'" or char == '"':
                        in_string = not in_string
                        current += char
                    elif char == "(" and not in_string:
                        depth += 1
                        current += char
                    elif char == ")" and not in_string:
                        depth -= 1
                        current += char
                    elif char == "," and depth == 0 and not in_string:
                        item = current.strip()
                        if item:
                            item_upper = item.upper()
                            if item_upper.startswith("FOREIGN KEY") or \
                            item_upper.startswith("UNIQUE") or \
                            item_upper.startswith("CHECK"):
                                constraints.append(item)
                            else:
                                columns.append(item)
                        current = ""
                    else:
                        current += char

                # Add compound primary key as a constraint if found
                if compound_pk_columns:
                    constraints.append({
                        "type": "PRIMARY_KEY",
                        "columns": compound_pk_columns
                    })

                logging.info(f"Extracted columns: {columns}, constraints: {constraints}")

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
        elif "SHOW ALL_TABLES" in raw_sql or "SHOW ALL_TABLE" in raw_sql:  # Handle both forms
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
        """Parse CREATE INDEX statement with compound column support."""
        logging.debug("Parsing CREATE INDEX: %s", sql)

        # Check for UNIQUE index
        is_unique = "UNIQUE" in sql.upper()

        # Extract index name
        index_pattern = r"CREATE\s+(UNIQUE\s+)?INDEX\s+(\w+)"
        index_match = re.search(index_pattern, sql, re.IGNORECASE)
        if not index_match:
            raise ValueError("Invalid CREATE INDEX syntax")
        index_name = index_match.group(2)

        # Extract table name
        table_pattern = r"ON\s+(\w+)"
        table_match = re.search(table_pattern, sql, re.IGNORECASE)
        if not table_match:
            raise ValueError("Table name not found in CREATE INDEX")
        table_name = table_match.group(1)

        # Extract columns (support compound indexes)
        # Pattern: ON table_name (col1, col2, col3)
        columns_pattern = r"ON\s+\w+\s*\(([^)]+)\)"
        columns_match = re.search(columns_pattern, sql, re.IGNORECASE)

        columns = []
        if columns_match:
            columns_str = columns_match.group(1)
            columns = [col.strip() for col in columns_str.split(',')]
        else:
            # Single column without parentheses
            column_pattern = r"ON\s+\w+\s+(\w+)"
            column_match = re.search(column_pattern, sql, re.IGNORECASE)
            if column_match:
                columns = [column_match.group(1)]

        if not columns:
            raise ValueError("Column name(s) not found in CREATE INDEX")

        return {
            "type": "CREATE_INDEX",
            "index_name": index_name,
            "table": table_name,
            "columns": columns,  # This is the key change - using "columns" instead of "column"
            "unique": is_unique,
        }

    def _parse_insert_statement(self, query):
        """Parse an INSERT statement."""
        match = re.match(r"INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*(.+)",
                        query, re.IGNORECASE)
        if not match:
            return {"error": f"Invalid INSERT syntax: {query}"}

        table = match.group(1)
        columns = [col.strip() for col in match.group(2).split(',')]
        values_part = match.group(3)

        # Fix: Support multiple rows in VALUES clause
        value_groups = []
        current_group = ""
        in_string = False
        quote_char = None
        paren_level = 0

        i = 0
        while i < len(values_part):
            char = values_part[i]
            
            if char in ('"', "'") and not in_string:
                in_string = True
                quote_char = char
                current_group += char
            elif char == quote_char and in_string:
                # Check if it's an escaped quote
                if i + 1 < len(values_part) and values_part[i + 1] == quote_char:
                    current_group += char + char
                    i += 1 # Skip the next quote
                else:
                    in_string = False
                    quote_char = None
                    current_group += char
            elif char == '(' and not in_string:
                paren_level += 1
                current_group += char
            elif char == ')' and not in_string:
                paren_level -= 1
                current_group += char
                
                # If we've closed all parentheses, we have a complete group
                if paren_level == 0:
                    value_groups.append(current_group.strip())
                    current_group = ""
            elif char == ',' and not in_string and paren_level == 0:
                # This comma separates value groups, not values within a group
                if current_group.strip():
                    value_groups.append(current_group.strip())
                    current_group = ""
            else:
                if not (char.isspace() and not current_group and not in_string):
                    current_group += char
            
            i += 1

        # Add the last group if there's any content
        if current_group.strip():
            value_groups.append(current_group.strip())

        # Process each value group
        processed_value_groups = []
        for group in value_groups:
            # Remove outer parentheses
            group = group.strip()
            if group.startswith('(') and group.endswith(')'):
                group = group[1:-1]
            
            # Split values within the group
            values = []
            current_value = ""
            in_string = False
            quote_char = None
            
            i = 0
            while i < len(group):
                char = group[i]
                
                if char in ('"', "'") and not in_string:
                    in_string = True
                    quote_char = char
                    current_value += char
                elif char == quote_char and in_string:
                    # Check if it's an escaped quote
                    if i + 1 < len(group) and group[i + 1] == quote_char:
                        current_value += char + char
                        i += 1  # Skip the next quote
                    else:
                        in_string = False
                        quote_char = None
                        current_value += char
                elif char == ',' and not in_string:
                    values.append(self._process_value_literal(current_value.strip()))
                    current_value = ""
                else:
                    current_value += char
                
                i += 1
            
            # Add the last value
            if current_value.strip():
                values.append(self._process_value_literal(current_value.strip()))
            
            processed_value_groups.append(values)

        return {
            "type": "INSERT",
            "table": table,
            "columns": columns,
            "values": processed_value_groups
        }

    def _process_value_literal(self, value):
        """Process a value literal and convert to appropriate type"""
        if not value:
            return None
        
        value = value.strip()
        
        # Handle NULL
        if value.upper() == 'NULL':
            return None
        
        # Handle quoted strings
        if (value.startswith("'") and value.endswith("'")) or \
        (value.startswith('"') and value.endswith('"')):
            return value[1:-1]  # Remove quotes
        
        # Try to convert to number
        try:
            # Try integer first
            if '.' not in value and 'e' not in value.lower():
                return int(value)
            else:
                return float(value)
        except ValueError:
            # Return as string if conversion fails
            return value

    def parse(self, sql):
        """Parse SQL and return structured result."""
        try:
            # Clean and normalize the SQL
            sql = sql.strip()
            if not sql:
                return {"error": "Empty query"}

            # Remove trailing semicolon
            if sql.endswith(';'):
                sql = sql[:-1]

            # Determine query type - CRITICAL: Check SELECT before SHOW
            sql_upper = sql.upper()
            
            if sql_upper.startswith("SELECT"):
                return self._parse_select(sql)
            elif sql_upper.startswith("SHOW"):
                return self._parse_show(sql)
            elif sql_upper.startswith("INSERT"):
                return self._parse_insert(sql)
            elif sql_upper.startswith("UPDATE"):
                return self._parse_update(sql)
            elif sql_upper.startswith("DELETE"):
                return self._parse_delete(sql)
            elif sql_upper.startswith("CREATE"):
                return self._parse_create(sql)
            elif sql_upper.startswith("DROP"):
                return self._parse_drop(sql)
            elif sql_upper.startswith("ALTER"):
                return self._parse_alter(sql)
            elif sql_upper.startswith("USE"):
                return self._parse_use(sql)
            else:
                return {"error": f"Unsupported query type: {sql[:20]}..."}

        except Exception as e:
            logging.error(f"Error parsing SQL: {e}")
            return {"error": f"Parse error: {str(e)}"}