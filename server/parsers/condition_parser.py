"""_summary_

Returns:
    _type_: _description_
"""

import logging


class ConditionParser:
    """_summary_"""

    @staticmethod
    def parse_value(value_str):
        """
        Parse a value string into the appropriate Python type.

        Args:
            value_str: String representation of a value

        Returns:
            The value converted to the appropriate type
        """
        # Handle quoted strings
        if value_str.startswith('"') or value_str.startswith("'"):
            # Remove quotes
            return (
                value_str[1:-1]
                if value_str.endswith('"') or value_str.endswith("'")
                else value_str[1:]
            )
        # Handle NULL keyword
        elif value_str.upper() == "NULL":
            return None
        # Handle boolean literals
        elif value_str.upper() in ("TRUE", "FALSE"):
            return value_str.upper() == "TRUE"
        else:
            # Try to convert to number if possible
            try:
                if "." in value_str:
                    return float(value_str)
                else:
                    return int(value_str)
            except ValueError:
                return value_str

    @staticmethod
    def parse_condition_to_list(condition_str):
        """
        Parse a SQL condition string into a list of condition dictionaries for B+ tree querying.

        Handles basic conditions like:
        - age > 30
        - name = 'John'
        - price <= 100
        - age > 30 AND name = 'John'
        - age > 30 OR price < 50

        Args:
            condition_str: SQL condition string (e.g., "age > 30")

        Returns:
            List of condition dictionaries for query processing
        """
        if not condition_str:
            return []

        # Check for OR conditions first
        if " OR " in condition_str:
            parts = condition_str.split(" OR ")
            # Return a special structure for OR conditions
            return [{
                "operator": "OR",
                "operands": [cond for part in parts 
                            for cond in ConditionParser.parse_condition_to_list(part.strip())]
            }]

        # Check for AND conditions
        if " AND " in condition_str:
            parts = condition_str.split(" AND ")
            conditions = []
            for part in parts:
                # Parse each part recursively
                part_conditions = ConditionParser.parse_condition_to_list(part.strip())
                conditions.extend(part_conditions)
            return conditions
            
        # Basic parsing for simpler conditions
        conditions = []

        # Check if this is a simple condition with a comparison operator
        for op in [">=", "<=", "!=", "<>", "=", ">", "<"]:
            if op in condition_str:
                parts = condition_str.split(op, 1)
                if len(parts) == 2:
                    column = parts[0].strip()
                    value_str = parts[1].strip()
                    value = ConditionParser.parse_value(value_str)

                    # Map <> to !=
                    operator = op if op != "<>" else "!="

                    conditions.append(
                        {"column": column, "operator": operator, "value": value}
                    )
                    break

        logging.debug('Parsed condition "%s" to: %s',
                    condition_str, conditions)
        return conditions

    @staticmethod
    def parse_condition_to_dict(condition_str):
        """
        Parse a SQL condition string into a dictionary format.
        This is used for UPDATE operations.

        Args:
            condition_str: SQL condition string (e.g., "age > 30")

        Returns:
            Dictionary with condition information
        """
        if not condition_str:
            return {}

        # Use the existing condition parsing logic
        conditions = ConditionParser.parse_condition_to_list(condition_str)
        if conditions and len(conditions) > 0:
            return conditions[0]  # Return first condition

        return {}

    @staticmethod
    def parse_expression(tokens, start=0):
        """_summary_

        Args:
            tokens (_type_): _description_
            start (int, optional): _description_. Defaults to 0.

        Returns:
            _type_: _description_
        """
        conditions = []
        i = start
        current_condition = None
        current_operator = "AND"  # Default operator

        while i < len(tokens):
            token = tokens[i].upper()

            # Handle parenthesized expressions
            if token == "(":
                sub_conditions, end_idx = ConditionParser.parse_expression(
                    tokens, i + 1
                )

                if current_condition is None:
                    current_condition = {
                        "type": "group",
                        "operator": current_operator,
                        "conditions": sub_conditions,
                    }
                else:
                    # Join with current condition using current_operator
                    conditions.append(current_condition)
                    current_condition = {
                        "type": "group",
                        "operator": current_operator,
                        "conditions": sub_conditions,
                    }

                i = end_idx + 1  # Skip past the closing parenthesis
                continue
            elif token == ")":
                # End of parenthesized expression
                if current_condition is not None:
                    conditions.append(current_condition)
                return conditions, i

            # Handle logical operators
            elif token in ("AND", "OR"):
                current_operator = token
                i += 1
                continue

            # Handle NOT operator
            elif token == "NOT":
                # Look ahead to handle NOT IN, NOT BETWEEN, NOT LIKE
                if i + 1 < len(tokens) and tokens[i + 1].upper() in (
                    "IN",
                    "BETWEEN",
                    "LIKE",
                ):
                    token = f"NOT {tokens[i + 1].upper()}"
                    i += 1  # Skip the next token as we've combined it
                else:
                    # Standalone NOT - needs to be applied to the next condition
                    next_cond, end_idx = ConditionParser.parse_expression(
                        tokens, i + 1)
                    current_condition = {
                        "type": "NOT",
                        "condition": next_cond[0] if next_cond else {},
                    }
                    i = end_idx
                    continue

            # Process condition based on operator
            if i + 2 < len(
                tokens
            ):  # Ensure we have at least 3 tokens (column, operator, value)
                column = tokens[i]
                operator = tokens[i + 1].upper()

                # Handle special operators
                if operator in ("IN", "NOT IN"):
                    # Parse IN list: column IN (val1, val2, ...)
                    values = []
                    if i + 3 < len(tokens) and tokens[i + 2] == "(":
                        j = i + 3
                        while j < len(tokens) and tokens[j] != ")":
                            if tokens[j] != ",":
                                values.append(
                                    ConditionParser.parse_value(tokens[j]))
                            j += 1

                        current_condition = {
                            "column": column,
                            "operator": operator,
                            "value": values,
                        }
                        i = j + 1  # Skip to after the closing parenthesis
                        continue

                elif operator in ("BETWEEN", "NOT BETWEEN"):
                    # Parse BETWEEN: column BETWEEN value1 AND value2
                    if i + 4 < len(tokens) and tokens[i + 3].upper() == "AND":
                        value1 = ConditionParser.parse_value(tokens[i + 2])
                        value2 = ConditionParser.parse_value(tokens[i + 4])

                        current_condition = {
                            "column": column,
                            "operator": operator,
                            "value": [value1, value2],
                        }
                        i += 5  # Skip all processed tokens
                        continue

                elif operator in ("LIKE", "NOT LIKE"):
                    # Handle LIKE operator
                    pattern = ConditionParser.parse_value(tokens[i + 2])

                    current_condition = {
                        "column": column,
                        "operator": operator,
                        "value": pattern,
                    }
                    i += 3
                    continue

                elif operator in ("IS"):
                    # Handle IS NULL or IS NOT NULL
                    if i + 2 < len(tokens) and tokens[i + 2].upper() == "NULL":
                        current_condition = {
                            "column": column,
                            "operator": "IS NULL",
                            "value": None,
                        }
                        i += 3
                    elif (
                        i + 3 < len(tokens)
                        and tokens[i + 2].upper() == "NOT"
                        and tokens[i + 3].upper() == "NULL"
                    ):
                        current_condition = {
                            "column": column,
                            "operator": "IS NOT NULL",
                            "value": None,
                        }
                        i += 4
                    continue

                # Standard comparison operators
                elif operator in ("=", ">", "<", ">=", "<=", "!=", "<>"):
                    value = ConditionParser.parse_value(tokens[i + 2])

                    # Map <> to !=
                    if operator == "<>":
                        operator = "!="

                    current_condition = {
                        "column": column,
                        "operator": operator,
                        "value": value,
                    }
                    i += 3
                    continue

            # If nothing matched, just advance
            i += 1

        # Add the last condition if it exists
        if current_condition is not None:
            conditions.append(current_condition)

        return conditions, len(tokens) - 1

    @staticmethod
    def flatten_conditions(conditions):
        """
        Flatten nested condition structures into a list of simple conditions.
        Used by query engine when not optimizing with complex expressions.

        Args:
            conditions: Nested structure of conditions

        Returns:
            List of simplified conditions for basic B+ tree queries
        """
        result = []

        # For single conditions
        if isinstance(conditions, dict):
            conditions = [conditions]

        for condition in conditions:
            if condition.get("type") == "group":
                # Recursively process sub-conditions
                sub_results = ConditionParser.flatten_conditions(
                    condition.get("conditions", [])
                )
                result.extend(sub_results)
            elif condition.get("type") == "NOT":
                # Negate the condition and add it (if simple enough to negate)
                sub_condition = condition.get("condition", {})
                if "operator" in sub_condition and "column" in sub_condition:
                    negated = ConditionParser.negate_condition(sub_condition)
                    if negated:
                        result.append(negated)
            elif "column" in condition and "operator" in condition:
                # Standard condition
                result.append(condition)

        return result

    @staticmethod
    def negate_condition(condition):
        """
        Negate a simple condition for the NOT operator.

        Args:
            condition: Original condition dictionary

        Returns:
            Negated condition dictionary
        """
        op_map = {
            "=": "!=",
            "!=": "=",
            "<>": "=",
            ">": "<=",
            "<": ">=",
            ">=": "<",
            "<=": ">",
            "LIKE": "NOT LIKE",
            "NOT LIKE": "LIKE",
            "IN": "NOT IN",
            "NOT IN": "IN",
            "BETWEEN": "NOT BETWEEN",
            "NOT BETWEEN": "BETWEEN",
            "IS NULL": "IS NOT NULL",
            "IS NOT NULL": "IS NULL",
        }

        if condition["operator"] in op_map:
            return {
                "column": condition["column"],
                "operator": op_map[condition["operator"]],
                "value": condition["value"],
            }

        # If operator can't be simply negated
        return None
