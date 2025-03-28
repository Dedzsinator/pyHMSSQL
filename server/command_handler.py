import logging
import re

class CommandHandler:
    """
    Handler for special SQL commands that need custom processing
    before they reach the SQL parser
    """
    
    def __init__(self, engine=None):
        self.engine = engine
    
    def set_engine(self, engine):
        self.engine = engine
    
    def handle_command(self, command):
        """
        Handle special commands like USE DATABASE and CREATE INDEX
        Returns True if handled, False if should be passed to SQL parser
        """
        # Strip extra spaces and make command case-insensitive for matching
        cmd = command.strip()
        
        # Handle USE DATABASE command
        if cmd.upper().startswith("USE "):
            database_name = cmd[4:].strip()
            return self._handle_use_database(database_name)
        
        # Handle CREATE INDEX command
        elif cmd.upper().startswith("CREATE INDEX") or cmd.upper().startswith("CREATE UNIQUE INDEX"):
            return self._handle_create_index(cmd)
        
        return False, None
    
    def _handle_use_database(self, database_name):
        """Process USE DATABASE command"""
        if not self.engine:
            return False, {"error": "Engine not initialized"}
        
        # Create the execution plan
        plan = {
            "type": "USE_DATABASE",
            "database": database_name
        }
        
        # Execute the plan
        result = self.engine.execute(plan)
        return True, result
    
    def _handle_create_index(self, command):
        """Process CREATE INDEX command"""
        if not self.engine:
            return False, {"error": "Engine not initialized"}
            
        # Check for UNIQUE index
        is_unique = "UNIQUE" in command.upper()
        
        # Extract index name
        index_pattern = r'CREATE\s+(UNIQUE\s+)?INDEX\s+(\w+)'
        index_match = re.search(index_pattern, command, re.IGNORECASE)
        if not index_match:
            return True, {"error": "Could not parse index name"}
        index_name = index_match.group(2)
        
        # Extract table name
        table_pattern = r'ON\s+(\w+)'
        table_match = re.search(table_pattern, command, re.IGNORECASE)
        if not table_match:
            return True, {"error": "Could not parse table name"}
        table_name = table_match.group(1)
        
        # Extract column name
        # Try with parentheses first
        column_pattern = r'ON\s+\w+\s*\(\s*(\w+)\s*\)'
        column_match = re.search(column_pattern, command, re.IGNORECASE)
        
        if column_match:
            column_name = column_match.group(1)
        else:
            # Try without parentheses - just the last word
            parts = command.split(f"ON {table_name}", 1)
            if len(parts) > 1:
                remaining = parts[1].strip()
                if remaining:
                    # If there are parentheses, extract from them
                    if '(' in remaining and ')' in remaining:
                        col_part = remaining.split('(', 1)[1].split(')', 1)[0]
                        column_name = col_part.strip()
                    else:
                        # Just take the next word
                        column_name = remaining.split()[0] if remaining.split() else None
                else:
                    column_name = None
            else:
                column_name = None
        
        if not column_name:
            return True, {"error": "Could not parse column name"}
        
        # Create the execution plan
        plan = {
            "type": "CREATE_INDEX",
            "index_name": index_name,
            "table": table_name,
            "column": column_name,
            "unique": is_unique
        }
        
        # Execute the plan
        result = self.engine.execute(plan)
        return True, result