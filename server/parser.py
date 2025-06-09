"""
Unified SQL Parser for pyHMSSQL using SQLGlot.

This module provides a single interface for SQL parsing that uses SQLGlot
for robust and standardized SQL statement analysis.
"""

import logging

try:
    from .sqlglot_parser import SQLGlotParser
except ImportError:
    from sqlglot_parser import SQLGlotParser


class SQLParser:
    """
    Main SQL Parser class that wraps SQLGlot functionality.

    This replaces all previous parsing implementations with a unified
    SQLGlot-based approach for better SQL compatibility and optimization.
    """

    def __init__(self, engine=None, dialect="mysql"):
        """Initialize the SQL parser.

        Args:
            engine: Database engine instance (for compatibility)
            dialect: SQL dialect to use for parsing
        """
        self.engine = engine
        self.dialect = dialect

        # Initialize SQLGlot parser
        try:
            self.sqlglot_parser = SQLGlotParser(dialect=dialect)
            logging.info(f"✅ Initialized SQLGlot parser with dialect: {dialect}")
        except Exception as e:
            logging.error(f"❌ Failed to initialize SQLGlot parser: {e}")
            raise

    def parse_sql(self, sql):
        """
        Parse SQL query into a structured format using SQLGlot.

        Args:
            sql: The SQL statement to parse

        Returns:
            A dictionary containing the parsed SQL statement structure
        """
        if not sql or not sql.strip():
            return {"error": "Empty query"}

        try:
            # Use SQLGlot parser for all SQL parsing
            result = self.sqlglot_parser.parse(sql)

            # Log successful parsing
            logging.debug(f"✅ Successfully parsed SQL with SQLGlot: {sql[:50]}...")

            return result

        except Exception as e:
            logging.error(
                f"❌ SQLGlot parsing failed for query: {sql[:50]}... Error: {e}"
            )
            return {"error": f"Parse error: {str(e)}", "query": sql}

    def parse(self, sql):
        """Alias for parse_sql for compatibility."""
        return self.parse_sql(sql)

    def optimize(self, sql):
        """Optimize a SQL query using SQLGlot's optimizer."""
        try:
            return self.sqlglot_parser.optimize(sql)
        except Exception as e:
            logging.warning(f"Optimization failed: {e}")
            return sql

    def transpile(self, sql, target_dialect):
        """Transpile SQL to a different dialect."""
        try:
            return self.sqlglot_parser.transpile(sql, target_dialect)
        except Exception as e:
            logging.warning(f"Transpilation failed: {e}")
            return sql
