import logging
import re
from collections import defaultdict
from parsers.condition_parser import ConditionParser


class GroupByExecutor:
    """Execute GROUP BY queries with aggregate functions."""
    
    def __init__(self, catalog_manager):
        self.catalog_manager = catalog_manager
        self.condition_parser = None  # Will be set by ExecutionEngine
    
    def execute_group_by(self, plan):
        """Execute a GROUP BY query with aggregate functions."""
        logging.info("Executing GROUP BY with plan: %s", plan)
        
        table_name = plan.get("table")
        columns = plan.get("columns", [])
        group_by_columns = plan.get("group_by", [])
        condition = plan.get("condition")
        order_by = plan.get("order_by")
        limit = plan.get("limit")
        
        if not table_name:
            return {"error": "No table specified", "status": "error", "type": "error"}
        
        if not group_by_columns:
            return {"error": "No GROUP BY columns specified", "status": "error", "type": "error"}
        
        # Get current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {"error": "No database selected", "status": "error", "type": "error"}
        
        # Verify table exists (case-insensitive)
        tables = self.catalog_manager.list_tables(db_name)
        tables_lower = {table.lower(): table for table in tables}
        
        actual_table_name = table_name
        if table_name.lower() in tables_lower:
            actual_table_name = tables_lower[table_name.lower()]
        elif table_name not in tables:
            return {"error": f"Table '{table_name}' does not exist", "status": "error", "type": "error"}
        
        try:
            # Parse conditions if present
            conditions = []
            if condition:
                logging.debug("Parsing condition: %s", condition)
                if hasattr(ConditionParser, "parse_condition_to_list"):
                    conditions = ConditionParser.parse_condition_to_list(condition)
                
                # Clean quoted values
                for cond in conditions:
                    val = cond.get("value")
                    if isinstance(val, str) and ((val.startswith("'") and val.endswith("'")) or 
                                               (val.startswith('"') and val.endswith('"'))):
                        cond["value"] = val[1:-1]
            
            # Get all records
            all_records = self.catalog_manager.query_with_condition(
                actual_table_name, conditions, ["*"]
            )
            
            if not all_records:
                return {
                    "columns": [],
                    "rows": [],
                    "status": "success",
                    "type": "select_result"
                }
            
            # Group records by the GROUP BY columns
            groups = defaultdict(list)
            
            for record in all_records:
                # Create group key from GROUP BY columns
                group_key_parts = []
                for group_col in group_by_columns:
                    # Find actual column name (case-insensitive)
                    actual_col = None
                    for record_col in record.keys():
                        if record_col.lower() == group_col.lower():
                            actual_col = record_col
                            break
                    
                    if actual_col:
                        group_key_parts.append(str(record[actual_col]))
                    else:
                        group_key_parts.append("NULL")
                
                group_key = "|".join(group_key_parts)
                groups[group_key].append(record)
            
            logging.info("Created %d groups", len(groups))
            
            # Process each column to determine if it's an aggregate or group column
            result_columns = []
            column_specs = []
            
            for col in columns:
                if isinstance(col, str):
                    # Check if it's an aggregate function
                    match = re.search(r"(\w+)\s*\(\s*([^)]*)\s*\)", col)
                    if match:
                        func_name = match.group(1).upper()
                        col_name = match.group(2).strip()
                        
                        if func_name in ("COUNT", "SUM", "AVG", "MIN", "MAX"):
                            result_columns.append(f"{func_name}({col_name})")
                            column_specs.append({
                                "type": "aggregate",
                                "function": func_name,
                                "column": col_name,
                                "original": col
                            })
                        else:
                            # Not an aggregate, treat as group column
                            result_columns.append(col)
                            column_specs.append({
                                "type": "group",
                                "column": col,
                                "original": col
                            })
                    else:
                        # Regular column, should be in GROUP BY
                        result_columns.append(col)
                        column_specs.append({
                            "type": "group",
                            "column": col,
                            "original": col
                        })
            
            # Calculate results for each group
            result_rows = []
            
            for group_key, group_records in groups.items():
                row = []
                
                for spec in column_specs:
                    if spec["type"] == "group":
                        # Get the group column value
                        group_col = spec["column"]
                        # Find actual column name
                        actual_col = None
                        for record_col in group_records[0].keys():
                            if record_col.lower() == group_col.lower():
                                actual_col = record_col
                                break
                        
                        if actual_col:
                            row.append(group_records[0][actual_col])
                        else:
                            row.append(None)
                    
                    elif spec["type"] == "aggregate":
                        # Calculate aggregate value
                        func = spec["function"]
                        col = spec["column"]
                        
                        # Find actual column name
                        actual_col = None
                        if col != "*":
                            for record_col in group_records[0].keys():
                                if record_col.lower() == col.lower():
                                    actual_col = record_col
                                    break
                        
                        if func == "COUNT":
                            if col == "*":
                                row.append(len(group_records))
                            elif actual_col:
                                count = sum(1 for r in group_records if r.get(actual_col) is not None)
                                row.append(count)
                            else:
                                row.append(0)
                        
                        elif func == "SUM":
                            if actual_col:
                                total = 0
                                for r in group_records:
                                    val = r.get(actual_col)
                                    if val is not None:
                                        try:
                                            total += float(val)
                                        except (ValueError, TypeError):
                                            pass
                                row.append(total)
                            else:
                                row.append(None)
                        
                        elif func == "AVG":
                            if actual_col:
                                values = []
                                for r in group_records:
                                    val = r.get(actual_col)
                                    if val is not None:
                                        try:
                                            values.append(float(val))
                                        except (ValueError, TypeError):
                                            pass
                                
                                if values:
                                    row.append(sum(values) / len(values))
                                else:
                                    row.append(None)
                            else:
                                row.append(None)
                        
                        elif func == "MIN":
                            if actual_col:
                                values = [r.get(actual_col) for r in group_records if r.get(actual_col) is not None]
                                row.append(min(values) if values else None)
                            else:
                                row.append(None)
                        
                        elif func == "MAX":
                            if actual_col:
                                values = [r.get(actual_col) for r in group_records if r.get(actual_col) is not None]
                                row.append(max(values) if values else None)
                            else:
                                row.append(None)
                
                result_rows.append(row)
            
            # Apply ORDER BY if specified
            if order_by and result_rows:
                if isinstance(order_by, dict):
                    order_column = order_by.get("column")
                    direction = order_by.get("direction", "ASC").upper()
                else:
                    parts = order_by.strip().split()
                    order_column = parts[0]
                    direction = "DESC" if len(parts) > 1 and parts[1].upper() == "DESC" else "ASC"
                
                # Find column index for sorting
                order_col_index = None
                for i, col in enumerate(result_columns):
                    if col.lower() == order_column.lower():
                        order_col_index = i
                        break
                
                if order_col_index is not None:
                    reverse = direction == "DESC"
                    result_rows.sort(key=lambda x: x[order_col_index] if x[order_col_index] is not None else "", reverse=reverse)
            
            # Apply LIMIT if specified
            if limit is not None:
                try:
                    limit_int = int(limit)
                    if limit_int >= 0:
                        result_rows = result_rows[:limit_int]
                except (ValueError, TypeError):
                    pass
            
            logging.info("GROUP BY result: %d rows with columns: %s", len(result_rows), result_columns)
            
            return {
                "columns": result_columns,
                "rows": result_rows,
                "status": "success",
                "type": "select_result"
            }
            
        except Exception as e:
            logging.error("Error executing GROUP BY: %s", str(e))
            return {"error": f"Error executing GROUP BY: {str(e)}", "status": "error", "type": "error"}
