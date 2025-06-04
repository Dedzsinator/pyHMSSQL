"""HaskellParser module for pyHMSSQL.

This module provides integration with the Haskell SQL parser.
"""

import os
import json
import subprocess
import logging
import glob
from typing import Dict, Any, Optional, List, Tuple

class HaskellParser:
    """
    Python interface to the Haskell SQL parser.
    
    This class integrates the standalone Haskell parser executable with the Python
    server code, providing high-performance parsing of SQL queries. The parser
    converts SQL statements into a structured AST that can be used by the execution
    engine.
    """
    def __init__(self, binary_path: Optional[str] = None):
        """Initialize the Haskell parser.
        
        Args:
            binary_path: Path to the hsqlparser binary. If None, will look in standard locations.
        """
        self.binary_path = binary_path or self._find_binary_path()
        
        # Check if binary exists and is executable
        if not os.path.exists(self.binary_path):
            raise FileNotFoundError(f"Haskell parser binary not found at {self.binary_path}")
        
        if not os.access(self.binary_path, os.X_OK):
            raise PermissionError(f"Haskell parser binary at {self.binary_path} is not executable")
        
        # Test the binary by running it with --help
        try:
            result = subprocess.run(
                [self.binary_path, "--help"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode != 0:
                raise RuntimeError(f"Haskell parser binary test failed: {result.stderr}")
        except subprocess.TimeoutExpired:
            raise RuntimeError("Haskell parser binary test timed out")
        except FileNotFoundError:
            raise FileNotFoundError(f"Cannot execute Haskell parser binary at {self.binary_path}")
        
        logging.info(f"✅ Initialized Haskell parser with binary path: {self.binary_path}")

    def _find_binary_path(self) -> str:
        """Find the path to the hsqlparser binary."""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        potential_paths = [
            # Direct binary in server directory
            os.path.join(current_dir, "hsqlparser"),
            # Stack build output
            os.path.join(current_dir, ".stack-work", "dist", "x86_64-linux-tinfo6", "ghc-9.4.8", "build", "hsqlparser", "hsqlparser"),
            # Alternative stack paths
            os.path.join(current_dir, ".stack-work", "install", "*", "*", "bin", "hsqlparser"),
            # System installation paths
            "/usr/local/bin/hsqlparser",
            os.path.join(os.path.expanduser("~"), ".local", "bin", "hsqlparser")
        ]
        
        for path in potential_paths:
            # Handle glob patterns
            if "*" in path:
                matches = glob.glob(path)
                for match in matches:
                    if os.path.exists(match) and os.access(match, os.X_OK):
                        logging.info(f"Found Haskell parser binary at: {match}")
                        return match
            elif os.path.exists(path) and os.access(path, os.X_OK):
                logging.info(f"Found Haskell parser binary at: {path}")
                return path
        
        # Log all attempted paths for debugging
        logging.warning("Haskell parser binary not found in any of these locations:")
        for path in potential_paths:
            logging.warning(f"  - {path}")
        
        # Default to hoping it's in PATH
        return "hsqlparser"

    def parse(self, sql: str) -> Dict[str, Any]:
        """Parse a SQL statement using the Haskell parser.
        
        Args:
            sql: The SQL statement to parse
            
        Returns:
            A dictionary containing the parsed SQL statement structure
            
        Raises:
            RuntimeError: If the parser process fails
            ValueError: If the SQL statement cannot be parsed
        """
        try:
            logging.debug(f"Executing Haskell parser: {self.binary_path} with query: {sql[:100]}...")
            
            process = subprocess.run(
                [self.binary_path, sql],
                capture_output=True,
                text=True,
                check=True,
                timeout=30  # Add timeout
            )
            
            logging.debug(f"Haskell parser stdout: {process.stdout}")
            if process.stderr:
                logging.debug(f"Haskell parser stderr: {process.stderr}")
            
            result = json.loads(process.stdout)
            
            # Check for error result
            if isinstance(result, dict) and "tag" in result:
                if result["tag"] == "Error":
                    raise ValueError(f"SQL parsing error: {result.get('contents', 'Unknown error')}")
                elif result["tag"] == "Success":
                    parsed = result.get("contents", {})
                    # Add the original query for reference
                    parsed["query"] = sql
                    return self._process_parsed_result(parsed)
            
            return result
            
        except subprocess.CalledProcessError as e:
            error_msg = f"Haskell parser process failed (exit code {e.returncode}): {e.stderr}"
            logging.error(error_msg)
            raise RuntimeError(error_msg)
        except subprocess.TimeoutExpired:
            error_msg = "Haskell parser timed out"
            logging.error(error_msg)
            raise RuntimeError(error_msg)
        except json.JSONDecodeError as e:
            error_msg = f"Failed to decode parser output: {e}. Output was: {process.stdout}"
            logging.error(error_msg)
            raise RuntimeError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error in Haskell parser: {e}"
            logging.error(error_msg)
            raise RuntimeError(error_msg)

    def _process_parsed_result(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        """Process and transform the parsed result to match Python parser format.
        
        Args:
            parsed: The raw parser output
            
        Returns:
            A dictionary in the format expected by the execution engine
        """
        # Extract the statement type from the tag
        stmt_type = None
        stmt_data = {}
        
        if "tag" in parsed:
            tag = parsed["tag"]
            contents = parsed.get("contents", {})
            
            # Map Haskell tags to Python parser types
            if tag == "SelectStatement":
                stmt_type = "SELECT"
                stmt_data = self._process_select_statement(contents)
            elif tag == "InsertStatement":
                stmt_type = "INSERT"
                stmt_data = contents
            elif tag == "UpdateStatement":
                stmt_type = "UPDATE"
                stmt_data = contents
            elif tag == "DeleteStatement":
                stmt_type = "DELETE"
                stmt_data = contents
            elif tag == "CreateStatement":
                stmt_type = self._extract_create_type(contents)
                stmt_data = self._extract_create_data(contents)
            elif tag == "DropStatement":
                stmt_type = self._extract_drop_type(contents)
                stmt_data = self._extract_drop_data(contents)
            elif tag == "ShowStatement":
                stmt_type = "SHOW"
                stmt_data = self._extract_show_data(contents)
            elif tag == "VisualizeStatement":
                stmt_type = "VISUALIZE"
                stmt_data = self._extract_visualize_data(contents)
            elif tag == "UseStatement":
                stmt_type = "USE_DATABASE"
                stmt_data = {"database": contents}
            elif tag == "ScriptStatement":
                stmt_type = "SCRIPT"
                stmt_data = {"filename": contents}
            elif tag == "BeginTransaction":
                stmt_type = "BEGIN_TRANSACTION"
            elif tag == "CommitTransaction":
                stmt_type = "COMMIT"
            elif tag == "RollbackTransaction":
                stmt_type = "ROLLBACK"
            elif tag == "CacheStatement":
                stmt_type = "CACHE"
                stmt_data = self._extract_cache_data(contents)
        
        # Construct the result dictionary
        result = {"type": stmt_type, "operation": stmt_type}
        result.update(stmt_data)
        
        # Add the original query
        if "query" in parsed:
            result["query"] = parsed["query"]
        
        return result

    def _process_select_statement(self, contents: Dict[str, Any]) -> Dict[str, Any]:
        """Process a SELECT statement from Haskell parser output."""
        result = {}
        
        # Process columns
        if "columns" in contents:
            columns = []
            for col in contents["columns"]:
                if isinstance(col, dict) and col.get("tag") == "AllColumns":
                    columns.append("*")
                elif isinstance(col, dict) and col.get("tag") == "ExprColumn":
                    # Handle expression columns
                    expr_contents = col.get("contents", [])
                    if len(expr_contents) >= 1:
                        expr = expr_contents[0]
                        if isinstance(expr, dict) and expr.get("tag") == "ColumnRef":
                            col_name = expr.get("contents", ["unknown"])[0]
                            columns.append(col_name)
                        else:
                            columns.append(str(expr))
                    else:
                        columns.append("unknown")
                else:
                    columns.append(str(col))
            result["columns"] = columns
        
        # Process tables
        if "tables" in contents:
            tables = []
            for table in contents["tables"]:
                if isinstance(table, dict) and table.get("tag") == "Table":
                    table_contents = table.get("contents", [])
                    if len(table_contents) >= 1:
                        table_name = table_contents[0]
                        tables.append(table_name)
                else:
                    tables.append(str(table))
            result["tables"] = tables
        
        # Process other fields
        if "distinct" in contents:
            result["distinct"] = contents["distinct"]
        
        if "where" in contents and contents["where"]:
            # Convert WHERE clause if present
            result["where"] = self._process_where_clause(contents["where"])
        
        if "order_by" in contents and contents["order_by"]:
            result["order_by"] = self._process_order_by(contents["order_by"])
        
        if "limit" in contents and contents["limit"]:
            result["limit"] = contents["limit"]
        
        if "offset" in contents and contents["offset"]:
            result["offset"] = contents["offset"]
        
        if "join_info" in contents and contents["join_info"]:
            result["join_info"] = contents["join_info"]
        
        return result

    def _process_where_clause(self, where_clause: Dict[str, Any]) -> str:
        """Convert WHERE clause to string format."""
        if isinstance(where_clause, dict):
            tag = where_clause.get("tag")
            contents = where_clause.get("contents", {})
            
            if tag == "Where":
                # Recursively process the contents
                return self._process_expression(contents)
            else:
                return self._process_expression(where_clause)
        else:
            return str(where_clause)

    def _process_expression(self, expr: Any) -> str:
        """Convert expression to string format."""
        if isinstance(expr, dict):
            tag = expr.get("tag")
            contents = expr.get("contents", [])
            
            if tag == "BinaryOp":
                if len(contents) >= 3:
                    op = contents[0]
                    left = self._process_expression(contents[1])
                    right = self._process_expression(contents[2])
                    return f"{left} {op} {right}"
            elif tag == "ColumnRef":
                if contents and len(contents) > 0:
                    return contents[0]
            elif tag == "LiteralInt":
                return str(contents)
            elif tag == "LiteralString":
                return f"'{contents}'"
            elif tag == "LiteralDecimal":
                return str(contents)
        
        return str(expr)

    def _process_order_by(self, order_by: Dict[str, Any]) -> Dict[str, str]:
        """Process ORDER BY clause."""
        if isinstance(order_by, dict):
            column = order_by.get("column", "")
            direction = order_by.get("direction", "ASC")
            return {"column": column, "direction": direction}
        return {"column": "", "direction": "ASC"}

    def _extract_create_type(self, contents: Dict[str, Any]) -> str:
        """Extract the type of CREATE statement."""
        if isinstance(contents, dict) and "tag" in contents:
            tag = contents["tag"]
            if tag == "CreateTableExpr":
                return "CREATE_TABLE"
            elif tag == "CreateDatabaseExpr":
                return "CREATE_DATABASE"
            elif tag == "CreateIndexExpr":
                return "CREATE_INDEX"
            elif tag == "CreateViewExpr":
                return "CREATE_VIEW"
        return "CREATE_UNKNOWN"

    def _extract_create_data(self, contents: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from CREATE statement."""
        result = {}
        if isinstance(contents, dict):
            tag = contents.get("tag")
            data = contents.get("contents", {})
            
            if tag == "CreateTableExpr":
                result["create_type"] = "TABLE"
                result["table"] = data.get("cteName", "")
                result["columns"] = data.get("cteColumns", [])
                result["constraints"] = data.get("cteConstraints", [])
            elif tag == "CreateDatabaseExpr":
                result["create_type"] = "DATABASE"
                result["database"] = data
            elif tag == "CreateIndexExpr":
                result["create_type"] = "INDEX"
                result["index_name"] = data.get("cieIndexName", "")
                result["table"] = data.get("cieTable", "")
                result["columns"] = data.get("cieColumns", [])
                result["unique"] = data.get("cieUnique", False)
        
        return result

    def _extract_drop_type(self, contents: Dict[str, Any]) -> str:
        """Extract the type of DROP statement."""
        if isinstance(contents, dict) and "tag" in contents:
            tag = contents["tag"]
            if tag == "DropTableExpr":
                return "DROP_TABLE"
            elif tag == "DropDatabaseExpr":
                return "DROP_DATABASE"
            elif tag == "DropIndexExpr":
                return "DROP_INDEX"
            elif tag == "DropViewExpr":
                return "DROP_VIEW"
        return "DROP_UNKNOWN"

    def _extract_drop_data(self, contents: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from DROP statement."""
        result = {}
        if isinstance(contents, dict):
            tag = contents.get("tag")
            data = contents.get("contents", {})
            
            if tag == "DropTableExpr":
                result["table"] = data
            elif tag == "DropDatabaseExpr":
                result["database"] = data
            elif tag == "DropIndexExpr":
                result["index_name"] = data.get("dieIndexName", "")
                result["table"] = data.get("dieTable", "")
            elif tag == "DropViewExpr":
                result["view"] = data
        
        return result

    def _extract_show_data(self, contents: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from SHOW statement."""
        result = {}
        if isinstance(contents, dict) and "tag" in contents:
            tag = contents["tag"]
            
            if tag == "ShowDatabases":
                result["object"] = "DATABASES"
            elif tag == "ShowTables":
                result["object"] = "TABLES"
            elif tag == "ShowAllTables":
                result["object"] = "ALL_TABLES"
            elif tag == "ShowColumns":
                result["object"] = "COLUMNS"
                result["table"] = contents.get("contents", "")
            elif tag == "ShowIndexes":
                result["object"] = "INDEXES"
                result["table"] = contents.get("contents")
        
        return result

    def _extract_visualize_data(self, contents: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from VISUALIZE statement."""
        result = {}
        if isinstance(contents, dict) and "tag" in contents:
            tag = contents["tag"]
            
            if tag == "VisualizeBPTree":
                result["object"] = "BPTREE"
                data = contents.get("contents", [])
                if len(data) >= 2:
                    result["index_name"] = data[0]
                    result["table"] = data[1]
                elif len(data) == 1:
                    result["table"] = data[0]
        
        return result

    def _extract_cache_data(self, contents: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from CACHE statement."""
        result = {}
        if isinstance(contents, dict) and "tag" in contents:
            tag = contents["tag"]
            
            if tag == "CacheStats":
                result["action"] = "STATS"
            elif tag == "CacheClear":
                result["action"] = "CLEAR"
                clear_data = contents.get("contents", {})
                if isinstance(clear_data, dict) and "tag" in clear_data:
                    clear_tag = clear_data["tag"]
                    if clear_tag == "CacheClearAll":
                        result["target"] = "ALL"
                    elif clear_tag == "CacheClearTable":
                        result["target"] = "TABLE"
                        result["table"] = clear_data.get("contents", "")
        
        return result