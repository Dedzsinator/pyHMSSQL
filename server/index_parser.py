import logging
import re

class IndexParser:
    """
    Parser specifically for index-related SQL commands
    """
    
    def parse_create_index(self, sql):
        """Parse CREATE INDEX statement"""
        logging.debug(f"Parsing CREATE INDEX: {sql}")
        
        # Check for UNIQUE index
        is_unique = "UNIQUE" in sql.upper()
        
        # Extract index name
        index_pattern = r'CREATE\s+(UNIQUE\s+)?INDEX\s+(\w+)'
        index_match = re.search(index_pattern, sql, re.IGNORECASE)
        if not index_match:
            return {'error': "Could not parse index name"}
        index_name = index_match.group(2)
        
        # Extract table name
        table_pattern = r'ON\s+(\w+)'
        table_match = re.search(table_pattern, sql, re.IGNORECASE)
        if not table_match:
            return {'error': "Could not parse table name"}
        table_name = table_match.group(1)
        
        # Extract column name
        # Try both with and without parentheses
        column_pattern = r'ON\s+\w+\s*\((\w+)\)'
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
            return {'error': "Could not parse column name"}
        
        return {
            'type': 'CREATE_INDEX',
            'index_name': index_name,
            'table': table_name,
            'column': column_name,
            'unique': is_unique
        }