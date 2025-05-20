"""Utility functions for SQL operations."""

def parse_simple_condition(condition):
    """Parse a simple condition string in format "column = value" or "condition1 AND condition2".

    Args:
        condition: A string like "id = 5" or "name = 'John' AND age > 30"

    Returns:
        list: List of condition dictionaries
    """
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
