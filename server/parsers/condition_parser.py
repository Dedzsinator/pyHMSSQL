# This file is deprecated and replaced by SQLGlot parsing.
# All condition parsing is now handled in sqlglot_parser.py

# Compatibility wrapper to maintain existing imports
from sqlglot_parser import SQLGlotParser


class ConditionParser:
    """
    Deprecated: Compatibility wrapper for the old ConditionParser.
    All functionality has been moved to SQLGlotParser.
    """
    
    def __init__(self):
        self.sqlglot_parser = SQLGlotParser()
    
    def parse_condition(self, condition_str):
        """Parse condition using SQLGlot."""
        # Create a dummy SELECT to parse the WHERE condition
        temp_sql = f"SELECT * FROM dummy WHERE {condition_str}"
        result = self.sqlglot_parser.parse(temp_sql)
        
        if "parsed_condition" in result:
            return result["parsed_condition"]
        return []
    
    def evaluate_condition(self, condition, record):
        """Evaluate condition against a record."""
        # This is a simplified implementation for compatibility
        if isinstance(condition, list):
            for cond in condition:
                if isinstance(cond, dict):
                    column = cond.get("column")
                    operator = cond.get("operator")
                    value = cond.get("value")
                    
                    if column in record:
                        record_val = record[column]
                        
                        if operator == "=":
                            return record_val == value
                        elif operator == "!=":
                            return record_val != value
                        elif operator == ">":
                            return record_val > value
                        elif operator == ">=":
                            return record_val >= value
                        elif operator == "<":
                            return record_val < value
                        elif operator == "<=":
                            return record_val <= value
                        elif operator.upper() == "LIKE":
                            import fnmatch
                            pattern = str(value).replace("%", "*")
                            return fnmatch.fnmatch(str(record_val), pattern)
                        elif operator.upper() == "IN":
                            if isinstance(value, list):
                                return record_val in value
                            return False
        
        return True  # Default to true for compatibility


# Create an alias for backward compatibility
condition_parser = ConditionParser()
