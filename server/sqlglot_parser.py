"""SQLGlot-based SQL Parser for pyHMSSQL.

This         try:
            # Parse with SQLGlot
            parsed = parse_one(sql, dialect=self.dialect)

            # Convert to our internal format
            result = self._convert_to_internal_format(parsed, sql)

            logging.debug(f"Successfully parsed SQL: {sql[:50]}...")
            return result

        except Exception as e:
            logging.error(f"SQLGlot parsing error: {e}")

            # Try fallback parsing for certain constructs
            fallback_result = self._try_fallback_parsing(sql, str(e))
            if fallback_result:
                return fallback_result

            return {"error": f"Parse error: {str(e)}", "query": sql}es a unified SQL parsing interface using SQLGlot,
replacing all custom regex-based parsing throughout the system.
"""

import logging
import sqlglot
from sqlglot import exp, parse_one, transpile
from sqlglot.optimizer import optimize
from typing import Dict, Any, List, Optional, Union


class SQLGlotParser:
    """
    Unified SQL parser using SQLGlot for parsing, optimization, and transpilation.

    This replaces all custom regex parsing and provides a standardized interface
    for SQL statement analysis and transformation.
    """

    def __init__(self, dialect="mysql"):
        """Initialize the SQLGlot parser.

        Args:
            dialect: SQL dialect to use for parsing (mysql, postgres, sqlite, etc.)
        """
        self.dialect = dialect
        logging.info(f"✅ Initialized SQLGlot parser with dialect: {dialect}")

    def parse(self, sql: str) -> Dict[str, Any]:
        """Parse a SQL statement into a structured format.

        Args:
            sql: The SQL statement to parse

        Returns:
            A dictionary containing the parsed SQL statement structure
        """
        try:
            # Clean the SQL
            sql = sql.strip()
            if not sql:
                return {"error": "Empty query"}

            # Remove trailing semicolon
            if sql.endswith(';'):
                sql = sql[:-1]

            # Parse with SQLGlot
            parsed = parse_one(sql, dialect=self.dialect)

            # Convert to our internal format
            result = self._convert_to_internal_format(parsed, sql)

            logging.debug(f"Successfully parsed SQL: {sql[:50]}...")
            return result

        except Exception as e:
            logging.error(f"SQLGlot parsing error: {e}")

            # Try fallback parsing for certain constructs
            fallback_result = self._try_fallback_parsing(sql, str(e))
            if fallback_result:
                return fallback_result

            return {"error": f"Parse error: {str(e)}", "query": sql}

    def optimize(self, sql: str) -> str:
        """Optimize a SQL query using SQLGlot's optimizer.

        Args:
            sql: The SQL statement to optimize

        Returns:
            The optimized SQL statement
        """
        try:
            parsed = parse_one(sql, dialect=self.dialect)
            optimized = optimize(parsed, dialect=self.dialect)
            return optimized.sql(dialect=self.dialect)
        except Exception as e:
            logging.warning(f"SQLGlot optimization failed: {e}")
            return sql  # Return original if optimization fails

    def transpile(self, sql: str, target_dialect: str) -> str:
        """Transpile SQL from one dialect to another.

        Args:
            sql: The SQL statement to transpile
            target_dialect: Target SQL dialect

        Returns:
            The transpiled SQL statement
        """
        try:
            return transpile(sql, read=self.dialect, write=target_dialect)[0]
        except Exception as e:
            logging.warning(f"SQLGlot transpilation failed: {e}")
            return sql  # Return original if transpilation fails

    def _try_fallback_parsing(self, sql: str, error_msg: str) -> Optional[Dict[str, Any]]:
        """Try fallback parsing for SQL constructs that SQLGlot can't handle.

        Args:
            sql: The SQL statement
            error_msg: The error message from SQLGlot

        Returns:
            Parsed result if successful, None otherwise
        """
        import re

        # Handle custom SCRIPT statement
        if sql.strip().upper().startswith("SCRIPT"):
            return self._parse_script(sql)

        # Handle custom VISUALIZE statement
        if sql.strip().upper().startswith("VISUALIZE"):
            return self._parse_visualize(sql)

        # Handle DELETE with ORDER BY/LIMIT (MySQL specific)
        if sql.strip().upper().startswith("DELETE") and "ORDER BY" in sql.upper():
            match = re.match(
                r"DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?\s+ORDER\s+BY\s+(.+?)(?:\s+LIMIT\s+(\d+))?\s*$",
                sql.strip(),
                re.IGNORECASE | re.DOTALL
            )
            if match:
                table_name = match.group(1)
                where_clause = match.group(2)
                order_by = match.group(3)
                limit = match.group(4)

                result = {
                    "type": "DELETE",
                    "operation": "DELETE",
                    "query": sql,
                    "table": table_name
                }

                if where_clause:
                    # Simple condition parsing for WHERE clause
                    result["condition"] = self._parse_simple_condition(where_clause.strip())
                    result["where"] = where_clause.strip()

                if order_by:
                    result["order_by"] = order_by.strip()

                if limit:
                    result["limit"] = int(limit)

                logging.info(f"✅ Fallback parsing successful for DELETE with ORDER BY/LIMIT")
                return result

        return None

    def _parse_simple_condition(self, condition_str: str) -> List[Dict[str, Any]]:
        """Parse simple WHERE conditions for fallback parsing.

        Args:
            condition_str: The WHERE condition string

        Returns:
            List of condition dictionaries
        """
        import re

        # Handle simple conditions like "column = 'value'"
        match = re.match(r"(\w+)\s*=\s*'([^']+)'", condition_str.strip())
        if match:
            return [{
                "column": match.group(1),
                "operator": "=",
                "value": match.group(2)
            }]

        # Handle numeric conditions like "column = 123"
        match = re.match(r"(\w+)\s*=\s*(\d+)", condition_str.strip())
        if match:
            return [{
                "column": match.group(1),
                "operator": "=",
                "value": int(match.group(2))
            }]

        # For complex conditions, return the original string
        return [{"raw": condition_str}]

    def _convert_to_internal_format(self, parsed: exp.Expression, original_sql: str) -> Dict[str, Any]:
        """Convert SQLGlot AST to our internal format.

        Args:
            parsed: SQLGlot parsed expression
            original_sql: Original SQL string

        Returns:
            Dictionary in our internal format
        """
        result = {"query": original_sql, "operation": None}

        if isinstance(parsed, exp.Select):
            result.update(self._parse_select(parsed))
        elif isinstance(parsed, exp.Insert):
            result.update(self._parse_insert(parsed))
        elif isinstance(parsed, exp.Update):
            result.update(self._parse_update(parsed))
        elif isinstance(parsed, exp.Delete):
            result.update(self._parse_delete(parsed))
        elif isinstance(parsed, exp.Create):
            result.update(self._parse_create(parsed))
        elif isinstance(parsed, exp.Drop):
            result.update(self._parse_drop(parsed))
        elif isinstance(parsed, exp.Show):
            result.update(self._parse_show(parsed))
        elif isinstance(parsed, exp.Use):
            result.update(self._parse_use(parsed))
        elif isinstance(parsed, exp.Union):
            result.update(self._parse_union(parsed))
        elif isinstance(parsed, exp.Intersect):
            result.update(self._parse_intersect(parsed))
        elif isinstance(parsed, exp.Except):
            result.update(self._parse_except(parsed))
        elif isinstance(parsed, exp.Merge):
            result.update(self._parse_merge(parsed))
        elif isinstance(parsed, exp.Command):
            # Handle REPLACE and other command-like statements
            if "REPLACE" in original_sql.upper():
                result.update(self._parse_replace(original_sql))
            elif "SHOW" in original_sql.upper():
                result.update(self._parse_show_fallback(original_sql))
            else:
                result["error"] = f"Unsupported command: {original_sql}"
        elif hasattr(exp, 'TruncateTable') and isinstance(parsed, exp.TruncateTable):
            result.update(self._parse_truncate(parsed))
        elif isinstance(parsed, exp.Describe):
            result.update(self._parse_describe(parsed, original_sql))
        elif isinstance(parsed, exp.Transaction):
            result.update(self._parse_transaction(parsed, original_sql))
        else:
            # Handle special cases
            if "SCRIPT" in original_sql.upper():
                result.update(self._parse_script(original_sql))
            elif "VISUALIZE" in original_sql.upper():
                result.update(self._parse_visualize(original_sql))
            elif "SHOW" in original_sql.upper():
                result.update(self._parse_show_fallback(original_sql))
            elif "BEGIN" in original_sql.upper() and "TRANSACTION" in original_sql.upper():
                result.update({"type": "BEGIN_TRANSACTION", "operation": "BEGIN_TRANSACTION"})
            elif "START" in original_sql.upper() and "TRANSACTION" in original_sql.upper():
                result.update({"type": "BEGIN_TRANSACTION", "operation": "BEGIN_TRANSACTION"})
            elif "COMMIT" in original_sql.upper():
                result.update({"type": "COMMIT", "operation": "COMMIT"})
            elif "ROLLBACK" in original_sql.upper():
                result.update({"type": "ROLLBACK", "operation": "ROLLBACK"})
            else:
                result["error"] = f"Unsupported statement type: {type(parsed).__name__}"

        # Ensure operation is set
        if "type" in result and "operation" not in result:
            result["operation"] = result["type"]

        return result

    def _parse_select(self, select: exp.Select) -> Dict[str, Any]:
        """Parse a SELECT statement."""
        result = {"type": "SELECT", "operation": "SELECT"}

        # Extract columns
        if select.expressions:
            columns = []
            for expr in select.expressions:
                if isinstance(expr, exp.Star):
                    columns.append("*")
                elif isinstance(expr, exp.Column):
                    columns.append(expr.name)
                elif isinstance(expr, exp.Alias):
                    # Handle aliased expressions
                    if isinstance(expr.this, exp.Column):
                        columns.append(expr.this.name)
                    else:
                        columns.append(str(expr.this))
                else:
                    columns.append(str(expr))
            result["columns"] = columns

        # Check for DISTINCT (but don't change the type - keep as SELECT)
        if select.distinct:
            result["distinct"] = True
            # For compatibility, add column info for simple DISTINCT queries
            if len(result.get("columns", [])) == 1 and result["columns"][0] != "*":
                result["column"] = result["columns"][0]

        # Check for JOINs FIRST (before aggregates and tables)
        joins = list(select.find_all(exp.Join))
        if joins:
            # Handle joins - prioritize JOIN classification
            result.update(self._parse_join_select(select, joins))
            return result

        # Extract tables if no JOINs
        if select.find(exp.From):
            from_clause = select.find(exp.From)
            if from_clause:
                tables = []
                from_expr = from_clause.this
                if isinstance(from_expr, exp.Table):
                    tables.append(from_expr.name)
                result["tables"] = tables

        # Check for aggregates after JOIN check
        has_aggregates = self._check_aggregates(select, result)

        # Extract WHERE clause
        where_clause = select.find(exp.Where)
        if where_clause:
            result["condition"] = str(where_clause.this)
            result["where"] = str(where_clause.this)
            result["parsed_condition"] = self._parse_condition(where_clause.this)

        # Extract GROUP BY
        group_clause = select.find(exp.Group)
        if group_clause:
            group_by = [str(expr) for expr in group_clause.expressions]
            result["group_by"] = group_by

        # Extract ORDER BY
        order_clause = select.find(exp.Order)
        if order_clause:
            order_items = []
            for order in order_clause.expressions:
                if isinstance(order, exp.Ordered):
                    direction = "DESC" if order.desc else "ASC"
                    column = str(order.this)
                    order_items.append({"column": column, "direction": direction})
            if order_items:
                result["order_by"] = order_items[0]  # For compatibility

        # Extract LIMIT
        limit_clause = select.find(exp.Limit)
        if limit_clause:
            result["limit"] = int(str(limit_clause.expression))

        # Extract OFFSET
        offset_clause = select.find(exp.Offset)
        if offset_clause:
            result["offset"] = int(str(offset_clause.expression))

        return result

    def _parse_join_select(self, select: exp.Select, joins: List[exp.Join]) -> Dict[str, Any]:
        """Parse SELECT statement with JOIN operations."""
        result = {"type": "JOIN", "operation": "JOIN"}

        # Extract tables
        tables = []
        left_table = None
        right_table = None

        # Get left table from FROM clause
        from_clause = select.find(exp.From)
        if from_clause and isinstance(from_clause.this, exp.Table):
            left_table = from_clause.this.name
            tables.append(left_table)

        # Get right table from first JOIN (we'll handle multiple JOINs later)
        join_expr = joins[0]
        if isinstance(join_expr.this, exp.Table):
            right_table = join_expr.this.name
            tables.append(right_table)

        result["tables"] = tables

        # Determine join type
        join_type = "INNER"
        if join_expr.side:
            join_type = join_expr.side.upper()
        if join_expr.kind:
            if join_expr.kind.upper() == "CROSS":
                join_type = "CROSS"

        # Extract join condition
        join_condition = None
        if hasattr(join_expr, 'on') and join_expr.on:
            join_condition = str(join_expr.on)

        # Store join info
        result["join_info"] = {
            "type": join_type,
            "condition": join_condition,
            "table1": left_table,
            "table2": right_table,
            "join_algorithm": "HASH"  # Default
        }

        # Extract columns (if any)
        if select.expressions:
            columns = []
            for expr in select.expressions:
                if isinstance(expr, exp.Star):
                    columns.append("*")
                else:
                    columns.append(str(expr))
            result["columns"] = columns

        # Extract WHERE clause (separate from JOIN condition)
        where_clause = select.find(exp.Where)
        if where_clause:
            result["condition"] = str(where_clause.this)
            result["where"] = str(where_clause.this)
            result["where_conditions"] = self._parse_condition(where_clause.this)
            result["parsed_condition"] = self._parse_condition(where_clause.this)

        return result

    def _check_aggregates(self, select: exp.Select, result: Dict[str, Any]) -> bool:
        """Check for aggregate functions in SELECT.

        Returns:
            True if aggregates were found and result was modified, False otherwise
        """
        for expr in select.expressions:
            if self._is_aggregate_expression(expr):
                func_name, column = self._extract_aggregate_info(expr)
                if func_name:
                    result["type"] = "AGGREGATE"
                    result["operation"] = "AGGREGATE"
                    result["function"] = func_name
                    result["column"] = column
                    return True
        return False

    def _is_aggregate_expression(self, expr: exp.Expression) -> bool:
        """Check if an expression contains aggregate functions."""
        if isinstance(expr, (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)):
            return True
        if isinstance(expr, exp.Alias) and isinstance(expr.this, (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)):
            return True
        return False

    def _extract_aggregate_info(self, expr: exp.Expression) -> tuple:
        """Extract aggregate function name and column."""
        if isinstance(expr, exp.Alias):
            expr = expr.this

        if isinstance(expr, exp.Count):
            column = "*" if expr.this.name == "*" else str(expr.this)
            return "COUNT", column
        elif isinstance(expr, exp.Sum):
            return "SUM", str(expr.this)
        elif isinstance(expr, exp.Avg):
            return "AVG", str(expr.this)
        elif isinstance(expr, exp.Min):
            return "MIN", str(expr.this)
        elif isinstance(expr, exp.Max):
            return "MAX", str(expr.this)

        return None, None

    def _parse_condition(self, condition: exp.Expression) -> List[Dict[str, Any]]:
        """Parse WHERE conditions into structured format."""
        if isinstance(condition, exp.And):
            # Handle AND conditions
            left_conds = self._parse_condition(condition.left)
            right_conds = self._parse_condition(condition.right)
            return left_conds + right_conds
        elif isinstance(condition, exp.Or):
            # Handle OR conditions
            return [{
                "operator": "OR",
                "operands": self._parse_condition(condition.left) + self._parse_condition(condition.right)
            }]
        elif isinstance(condition, exp.EQ):
            return [{"column": str(condition.left), "operator": "=", "value": self._extract_value(condition.right)}]
        elif isinstance(condition, exp.NEQ):
            return [{"column": str(condition.left), "operator": "!=", "value": self._extract_value(condition.right)}]
        elif isinstance(condition, exp.GT):
            return [{"column": str(condition.left), "operator": ">", "value": self._extract_value(condition.right)}]
        elif isinstance(condition, exp.GTE):
            return [{"column": str(condition.left), "operator": ">=", "value": self._extract_value(condition.right)}]
        elif isinstance(condition, exp.LT):
            return [{"column": str(condition.left), "operator": "<", "value": self._extract_value(condition.right)}]
        elif isinstance(condition, exp.LTE):
            return [{"column": str(condition.left), "operator": "<=", "value": self._extract_value(condition.right)}]
        else:
            # Fallback for complex conditions
            return [{"column": "unknown", "operator": "RAW", "value": str(condition)}]

    def _extract_value(self, expr: exp.Expression) -> Any:
        """Extract value from an expression."""
        if isinstance(expr, exp.Literal):
            if expr.is_string:
                return expr.this
            elif expr.is_int:
                return int(expr.this)
            elif expr.is_number:
                return float(expr.this)
        elif isinstance(expr, exp.Null):
            return None
        return str(expr)

    def _parse_insert(self, insert: exp.Insert) -> Dict[str, Any]:
        """Parse INSERT statement."""
        result = {"type": "INSERT", "operation": "INSERT"}

        # Extract table name - SQLGlot structure: insert.this.this.name
        table_name = None
        if insert.this:
            if hasattr(insert.this, 'this') and hasattr(insert.this.this, 'name'):
                table_name = insert.this.this.name
            elif hasattr(insert.this, 'name'):
                table_name = insert.this.name
            else:
                table_name = str(insert.this)
        elif hasattr(insert, 'table') and insert.table:
            if hasattr(insert.table, 'name'):
                table_name = insert.table.name
            else:
                table_name = str(insert.table)

        if table_name:
            result["table"] = table_name

        # Extract columns - handle different SQLGlot versions
        columns = []
        if hasattr(insert, 'columns') and insert.columns:
            columns = [col.name for col in insert.columns]
        elif hasattr(insert, 'this') and hasattr(insert.this, 'expressions'):
            # SQLGlot structure: insert.this.expressions contains the column identifiers
            for expr in insert.this.expressions:
                if hasattr(expr, 'name'):
                    columns.append(expr.name)
                elif hasattr(expr, 'this'):
                    columns.append(expr.this)
                else:
                    columns.append(str(expr))
        elif hasattr(insert, 'expression') and hasattr(insert.expression, 'this') and hasattr(insert.expression.this, 'expressions'):
            # Alternative approach for different SQLGlot structure
            for expr in insert.expression.this.expressions:
                if hasattr(expr, 'name'):
                    columns.append(expr.name)

        if columns:
            result["columns"] = columns

        # Extract values
        values = []
        if hasattr(insert, 'expression') and insert.expression:
            if isinstance(insert.expression, exp.Values):
                for tuple_expr in insert.expression.expressions:
                    if isinstance(tuple_expr, exp.Tuple):
                        row_values = [self._extract_value(expr) for expr in tuple_expr.expressions]
                        values.append(row_values)
            elif hasattr(insert.expression, 'expressions'):
                # Handle direct tuple insertion
                row_values = [self._extract_value(expr) for expr in insert.expression.expressions]
                values.append(row_values)

        if values:
            result["values"] = values

        return result

    def _parse_update(self, update: exp.Update) -> Dict[str, Any]:
        """Parse UPDATE statement."""
        result = {"type": "UPDATE", "operation": "UPDATE"}

        # Extract table name
        if update.this:
            result["table"] = update.this.name

        # Extract SET clause
        if update.expressions:
            set_pairs = {}
            updates = []
            for expr in update.expressions:
                if isinstance(expr, exp.EQ):
                    column = str(expr.left)
                    value = self._extract_value(expr.right)
                    set_pairs[column] = value
                    updates.append((column, value))
            result["set"] = set_pairs
            result["updates"] = updates

        # Extract WHERE clause
        where_clause = update.find(exp.Where)
        if where_clause:
            result["condition"] = self._parse_condition(where_clause.this)
            result["where"] = str(where_clause.this)

        return result

    def _parse_delete(self, delete: exp.Delete) -> Dict[str, Any]:
        """Parse DELETE statement."""
        result = {"type": "DELETE", "operation": "DELETE"}

        # Extract table name
        if delete.this:
            result["table"] = delete.this.name

        # Extract WHERE clause
        where_clause = delete.find(exp.Where)
        if where_clause:
            result["condition"] = self._parse_condition(where_clause.this)
            result["where"] = str(where_clause.this)

        return result

    def _parse_create(self, create: exp.Create) -> Dict[str, Any]:
        """Parse CREATE statement."""
        if isinstance(create.this, exp.Schema):
            # CREATE TABLE
            result = {"type": "CREATE_TABLE", "operation": "CREATE_TABLE"}
            result["table"] = create.this.this.name

            # Extract columns
            columns = []
            constraints = []

            if create.this.expressions:
                for expr in create.this.expressions:
                    if isinstance(expr, exp.ColumnDef):
                        col_def = str(expr)
                        columns.append(col_def)
                    else:
                        constraints.append(str(expr))

            result["columns"] = columns
            result["constraints"] = constraints
            return result

        elif hasattr(create, 'kind') and create.kind == "DATABASE":
            # CREATE DATABASE
            return {
                "type": "CREATE_DATABASE",
                "operation": "CREATE_DATABASE",
                "database": create.this.name
            }

        elif hasattr(create, 'kind') and create.kind == "INDEX":
            # CREATE INDEX
            result = {"type": "CREATE_INDEX", "operation": "CREATE_INDEX"}
            if create.this:
                if hasattr(create.this, 'this') and hasattr(create.this.this, 'name'):
                    result["index_name"] = create.this.this.name
                elif hasattr(create.this, 'name'):
                    result["index_name"] = create.this.name
                else:
                    result["index_name"] = str(create.this)

            # Extract table name - SQLGlot structure: create.this.table.name
            table_name = None
            if hasattr(create.this, 'table') and create.this.table:
                if hasattr(create.this.table, 'name'):
                    table_name = create.this.table.name
                elif hasattr(create.this.table, 'this') and hasattr(create.this.table.this, 'name'):
                    table_name = create.this.table.this.name
                else:
                    table_name = str(create.this.table)
            # Fallback: find any Table node in the AST
            if not table_name:
                table_nodes = list(create.find_all(exp.Table))
                if table_nodes:
                    table_name = table_nodes[0].name

            if table_name:
                result["table"] = table_name

            # Extract columns from index parameters
            columns = []
            if hasattr(create.this, 'args') and create.this.args.get('params'):
                params = create.this.args['params']
                if hasattr(params, 'args') and params.args.get('columns'):
                    for col_expr in params.args['columns']:
                        # Handle Ordered expressions that wrap Column expressions
                        if hasattr(col_expr, 'this') and isinstance(col_expr.this, exp.Column):
                            if hasattr(col_expr.this, 'name'):
                                columns.append(col_expr.this.name)
                            else:
                                columns.append(str(col_expr.this))
                        elif hasattr(col_expr, 'name'):
                            columns.append(col_expr.name)
                        else:
                            columns.append(str(col_expr))

            # Fallback: use find_all for Column nodes
            if not columns:
                col_nodes = list(create.find_all(exp.Column))
                columns = [col.name for col in col_nodes]

            if columns:
                result["columns"] = columns

            result["unique"] = create.args.get('unique', False)
            return result

        return {"error": "Unsupported CREATE statement"}

    def _parse_drop(self, drop: exp.Drop) -> Dict[str, Any]:
        """Parse DROP statement."""
        if drop.kind == "TABLE":
            return {
                "type": "DROP_TABLE",
                "operation": "DROP_TABLE",
                "table": drop.this.name
            }
        elif drop.kind == "DATABASE":
            return {
                "type": "DROP_DATABASE",
                "operation": "DROP_DATABASE",
                "database": drop.this.name
            }
        elif drop.kind == "INDEX":
            result = {"type": "DROP_INDEX", "operation": "DROP_INDEX"}
            result["index_name"] = drop.this.name

            # Extract table name from cluster argument
            if hasattr(drop, 'args') and drop.args.get('cluster'):
                cluster = drop.args['cluster']
                if hasattr(cluster, 'this') and hasattr(cluster.this, 'name'):
                    result["table"] = cluster.this.name

            return result

        return {"error": "Unsupported DROP statement"}

    def _parse_show(self, show: exp.Show) -> Dict[str, Any]:
        """Parse SHOW statement."""
        result = {"type": "SHOW", "operation": "SHOW"}

        if hasattr(show, 'kind'):
            if show.kind == "DATABASES":
                result["object"] = "DATABASES"
            elif show.kind == "TABLES":
                result["object"] = "TABLES"
            elif show.kind == "INDEXES":
                result["object"] = "INDEXES"
                if hasattr(show, 'this') and show.this:
                    result["table"] = show.this.name

        return result

    def _parse_use(self, use: exp.Use) -> Dict[str, Any]:
        """Parse USE statement."""
        return {
            "type": "USE_DATABASE",
            "operation": "USE_DATABASE",
            "database": use.this.name
        }

    def _parse_union(self, union: exp.Union) -> Dict[str, Any]:
        """Parse UNION statement."""
        return {
            "type": "UNION",
            "operation": "UNION",
            "left": self._convert_to_internal_format(union.left, str(union.left)),
            "right": self._convert_to_internal_format(union.right, str(union.right))
        }

    def _parse_intersect(self, intersect: exp.Intersect) -> Dict[str, Any]:
        """Parse INTERSECT statement."""
        return {
            "type": "INTERSECT",
            "operation": "INTERSECT",
            "left": self._convert_to_internal_format(intersect.left, str(intersect.left)),
            "right": self._convert_to_internal_format(intersect.right, str(intersect.right))
        }

    def _parse_except(self, except_expr: exp.Except) -> Dict[str, Any]:
        """Parse EXCEPT statement."""
        return {
            "type": "EXCEPT",
            "operation": "EXCEPT",
            "left": self._convert_to_internal_format(except_expr.left, str(except_expr.left)),
            "right": self._convert_to_internal_format(except_expr.right, str(except_expr.right))
        }

    def _parse_merge(self, merge: exp.Merge) -> Dict[str, Any]:
        """Parse MERGE statement."""
        result = {"type": "MERGE", "operation": "MERGE"}

        # Extract target table
        if hasattr(merge, 'this') and merge.this:
            result["target_table"] = merge.this.name

        # Extract source table/query
        if hasattr(merge, 'using') and merge.using:
            if hasattr(merge.using, 'name'):
                result["source_table"] = merge.using.name
            else:
                result["source"] = str(merge.using)

        # Extract join condition
        if hasattr(merge, 'on') and merge.on:
            result["on_condition"] = str(merge.on)

        # Extract WHEN clauses
        when_clauses = []
        if hasattr(merge, 'expressions') and merge.expressions:
            for expr in merge.expressions:
                if hasattr(expr, 'kind'):
                    when_clause = {
                        "type": expr.kind,
                        "condition": str(expr.this) if hasattr(expr, 'this') else None,
                        "action": str(expr.then) if hasattr(expr, 'then') else None
                    }
                    when_clauses.append(when_clause)

        if when_clauses:
            result["when_clauses"] = when_clauses

        return result

    def _parse_replace(self, sql: str) -> Dict[str, Any]:
        """Parse REPLACE statement (MySQL-specific)."""
        result = {"type": "REPLACE", "operation": "REPLACE"}

        import re
        # Parse REPLACE INTO table_name (columns) VALUES (values)
        match = re.match(r"REPLACE\s+INTO\s+(\w+)(?:\s*\(([^)]+)\))?\s+VALUES\s*\((.+)\)", sql.strip(), re.IGNORECASE | re.DOTALL)
        if match:
            result["table"] = match.group(1)

            if match.group(2):  # columns specified
                columns = [col.strip().strip("'\"") for col in match.group(2).split(',')]
                result["columns"] = columns

            # Parse values
            values_str = match.group(3)
            values = []
            # Simple parsing - can be enhanced for complex values
            for val in values_str.split(','):
                val = val.strip()
                if val.startswith("'") and val.endswith("'"):
                    values.append(val[1:-1])  # Remove quotes
                elif val.isdigit():
                    values.append(int(val))
                else:
                    values.append(val)
            result["values"] = values

        return result

    def _parse_truncate(self, truncate: exp.TruncateTable) -> Dict[str, Any]:
        """Parse TRUNCATE statement."""
        result = {"type": "TRUNCATE", "operation": "TRUNCATE"}

        # Extract table name
        if hasattr(truncate, 'this') and truncate.this:
            if hasattr(truncate.this, 'name'):
                result["table"] = truncate.this.name
            else:
                # Handle case where this is a list of tables
                if isinstance(truncate.this, list):
                    tables = []
                    for table in truncate.this:
                        if hasattr(table, 'name'):
                            tables.append(table.name)
                        else:
                            tables.append(str(table))
                    result["tables"] = tables
                else:
                    result["table"] = str(truncate.this)

        return result

    def _parse_script(self, sql: str) -> Dict[str, Any]:
        """Parse SCRIPT statement."""
        import re
        match = re.match(r"SCRIPT\s+(.+)", sql.strip(), re.IGNORECASE)
        if match:
            filename = match.group(1).strip().strip('"\'')
            return {
                "type": "SCRIPT",
                "operation": "SCRIPT",
                "filename": filename
            }
        return {"error": "Invalid SCRIPT statement"}

    def _parse_visualize(self, sql: str) -> Dict[str, Any]:
        """Parse VISUALIZE statement."""
        import re
        result = {"type": "VISUALIZE", "operation": "VISUALIZE"}

        if "BPTREE" in sql.upper():
            result["object"] = "BPTREE"

            match = re.search(r"VISUALIZE\s+BPTREE\s+(\w+)\s+ON\s+(\w+)", sql, re.IGNORECASE)
            if match:
                result["index_name"] = match.group(1)
                result["table"] = match.group(2)
            else:
                match = re.search(r"VISUALIZE\s+BPTREE\s+ON\s+(\w+)", sql, re.IGNORECASE)
                if match:
                    result["table"] = match.group(1)

        return result

    def _parse_show_fallback(self, sql: str) -> Dict[str, Any]:
        """Parse SHOW statement using regex fallback."""
        import re
        result = {"type": "SHOW", "operation": "SHOW"}

        sql_upper = sql.upper().strip()

        if "SHOW DATABASES" in sql_upper:
            result["object"] = "DATABASES"
        elif "SHOW TABLES" in sql_upper:
            result["object"] = "TABLES"
        elif "SHOW COLUMNS" in sql_upper or "SHOW FIELDS" in sql_upper:
            result["object"] = "COLUMNS"
            match = re.search(r"FROM\s+(\w+)", sql, re.IGNORECASE)
            if match:
                result["table"] = match.group(1)
        elif "SHOW INDEXES" in sql_upper or "SHOW INDEX" in sql_upper:
            result["object"] = "INDEXES"
            match = re.search(r"FROM\s+(\w+)", sql, re.IGNORECASE)
            if match:
                result["table"] = match.group(1)
        elif "SHOW CREATE TABLE" in sql_upper:
            result["object"] = "CREATE_TABLE"
            match = re.search(r"TABLE\s+(\w+)", sql, re.IGNORECASE)
            if match:
                result["table"] = match.group(1)
        elif "SHOW PROCESSLIST" in sql_upper:
            result["object"] = "PROCESSLIST"
        else:
            result["object"] = "UNKNOWN"

        return result

    def _parse_describe(self, describe: exp.Describe, original_sql: str) -> Dict[str, Any]:
        """Parse DESCRIBE/EXPLAIN statement."""
        result = {"type": "EXPLAIN", "operation": "EXPLAIN"}

        # If it's EXPLAIN with a query, try to parse the inner query
        if hasattr(describe, 'this') and describe.this:
            if isinstance(describe.this, exp.Select):
                result["explained_query"] = self._parse_select(describe.this)
            else:
                result["explained_statement"] = str(describe.this)

        return result

    def _parse_transaction(self, transaction: exp.Transaction, original_sql: str) -> Dict[str, Any]:
        """Parse transaction statement."""
        sql_upper = original_sql.upper().strip()

        if "BEGIN" in sql_upper or "START" in sql_upper:
            return {"type": "BEGIN_TRANSACTION", "operation": "BEGIN_TRANSACTION"}
        elif "COMMIT" in sql_upper:
            return {"type": "COMMIT", "operation": "COMMIT"}
        elif "ROLLBACK" in sql_upper:
            return {"type": "ROLLBACK", "operation": "ROLLBACK"}
        else:
            return {"type": "TRANSACTION", "operation": "TRANSACTION"}

    def parse_column_definition(self, col_def: str) -> Dict[str, Any]:
        """Parse a column definition string using SQLGlot instead of regex.

        Args:
            col_def: Column definition string like "id INT PRIMARY KEY" or "name VARCHAR(50) NOT NULL"

        Returns:
            Dictionary with parsed column information
        """
        try:
            # Create a temporary CREATE TABLE statement to parse the column
            temp_sql = f"CREATE TABLE temp ({col_def})"
            parsed = parse_one(temp_sql, dialect=self.dialect)

            if isinstance(parsed, exp.Create) and isinstance(parsed.this, exp.Schema):
                if parsed.this.expressions:
                    col_expr = parsed.this.expressions[0]

                    if isinstance(col_expr, exp.ColumnDef):
                        column_info = {
                            "name": col_expr.this.name if hasattr(col_expr.this, 'name') else str(col_expr.this),
                            "type": str(col_expr.kind) if col_expr.kind else "UNKNOWN",
                            "nullable": True,  # Default
                            "primary_key": False,
                            "default": None,
                            "identity": False
                        }

                        # Check for constraints
                        if col_expr.constraints:
                            for constraint in col_expr.constraints:
                                if hasattr(constraint, 'kind'):
                                    if hasattr(exp, 'NotNullColumnConstraint') and isinstance(constraint.kind, exp.NotNullColumnConstraint):
                                        column_info["nullable"] = False
                                    elif hasattr(exp, 'PrimaryKeyColumnConstraint') and isinstance(constraint.kind, exp.PrimaryKeyColumnConstraint):
                                        column_info["primary_key"] = True
                                        column_info["nullable"] = False
                                    elif hasattr(exp, 'DefaultColumnConstraint') and isinstance(constraint.kind, exp.DefaultColumnConstraint):
                                        column_info["default"] = str(constraint.kind.this) if constraint.kind.this else None
                                    elif hasattr(exp, 'GeneratedAsIdentityColumnConstraint') and isinstance(constraint.kind, exp.GeneratedAsIdentityColumnConstraint):
                                        column_info["identity"] = True
                                        # Try to extract seed and increment values
                                        identity_str = str(constraint.kind)
                                        import re
                                        start_match = re.search(r'START WITH (\d+)', identity_str)
                                        increment_match = re.search(r'INCREMENT BY (\d+)', identity_str)
                                        if start_match:
                                            column_info["identity_seed"] = int(start_match.group(1))
                                        if increment_match:
                                            column_info["identity_increment"] = int(increment_match.group(1))

                        return column_info

        except Exception as e:
            logging.warning(f"SQLGlot column parsing failed for '{col_def}': {e}")

        # Fallback to regex-based parsing
        import re

        # Initialize result
        result = {
            "name": "",
            "type": "UNKNOWN",
            "nullable": True,
            "primary_key": False,
            "default": None,
            "identity": False
        }

        # Extract column name (first word)
        parts = col_def.strip().split()
        if parts:
            result["name"] = parts[0]

        # Extract data type
        type_match = re.search(r'\b(INT|INTEGER|VARCHAR|CHAR|TEXT|DECIMAL|FLOAT|DOUBLE|BOOLEAN|DATE|DATETIME|TIME|TIMESTAMP)\b(?:\(\d+(?:,\d+)?\))?', col_def, re.IGNORECASE)
        if type_match:
            result["type"] = type_match.group(0).upper()

        # Check for constraints
        if re.search(r'\bPRIMARY\s+KEY\b', col_def, re.IGNORECASE):
            result["primary_key"] = True
            result["nullable"] = False

        if re.search(r'\bNOT\s+NULL\b', col_def, re.IGNORECASE):
            result["nullable"] = False

        # Extract default value
        default_match = re.search(r'\bDEFAULT\s+([^\s]+)', col_def, re.IGNORECASE)
        if default_match:
            result["default"] = default_match.group(1).strip("'\"")

        # Check for IDENTITY
        identity_match = re.search(r'\bIDENTITY\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)', col_def, re.IGNORECASE)
        if identity_match:
            result["identity"] = True
            result["identity_seed"] = int(identity_match.group(1))
            result["identity_increment"] = int(identity_match.group(2))

        return result
