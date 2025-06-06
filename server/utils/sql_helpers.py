"""Utility functions for SQL operations."""

def parse_simple_condition(condition):
    """Parse a simple condition string in format "column = value" or "condition1 AND condition2".
    
    DEPRECATED: This function is now deprecated in favor of SQLGlot parsing.
    Use sqlglot_parser.SQLGlotParser._parse_condition() instead.

    Args:
        condition: A string like "id = 5" or "name = 'John' AND age > 30"

    Returns:
        list: List of condition dictionaries
    """
    import logging
    logging.warning("parse_simple_condition() is deprecated. Use SQLGlot parsing instead.")
    
    conditions = []
    if not condition:
        return conditions

    # Check for AND conditions
    if " AND " in condition:
        parts = condition.split(" AND ")
        for part in parts:
            conditions.extend(parse_simple_condition(part.strip()))
        return conditions

    # Handle simple equality condition
    parts = condition.split('=')
    if len(parts) == 2:
        col = parts[0].strip()
        val = parts[1].strip()
        try:
            # Try to convert value to number if possible
            val = int(val)
        except ValueError:
            try:
                val = float(val)
            except ValueError:
                # Remove quotes if present
                if (val.startswith("'") and val.endswith("'")) or \
                   (val.startswith('"') and val.endswith('"')):
                    val = val[1:-1]

        conditions.append({
            "column": col,
            "operator": "=",
            "value": val
        })

    return conditions

def check_database_selected(catalog_manager):
    """Check if a database is selected and return error if not.

    Args:
        catalog_manager: The catalog manager instance

    Returns:
        dict or None: Error dictionary if no database selected, None otherwise
    """
    db_name = catalog_manager.get_current_database()
    if not db_name:
        return {
            "error": "No database selected. Use 'USE database_name' first.",
            "status": "error",
        }
    return None

def parse_condition_with_sqlglot(condition_string):
    """Parse a condition string using SQLGlot.
    
    Args:
        condition_string: SQL condition string
        
    Returns:
        List of parsed condition dictionaries
    """
    try:
        from sqlglot_parser import SQLGlotParser
        import sqlglot
        from sqlglot import exp
        
        # Parse as a WHERE clause
        parsed = sqlglot.parse_one(f"SELECT * FROM dummy WHERE {condition_string}")
        if parsed and parsed.find(exp.Where):
            where_clause = parsed.find(exp.Where)
            parser = SQLGlotParser()
            return parser._parse_condition(where_clause.this)
    except Exception as e:
        import logging
        logging.warning(f"SQLGlot condition parsing failed: {e}, falling back to simple parsing")
        return parse_simple_condition(condition_string)
    
    return []
