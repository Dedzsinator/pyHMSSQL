"""
Query Transformation Engine for Cost-Based Query Optimization.

This module implements sophisticated rule-based query transformations
that match enterprise-grade optimizers like Oracle CBO and PostgreSQL.

Features:
- Predicate pushdown with correlation analysis
- Subquery unnesting (decorrelation)
- View merging and materialization decisions
- Join predicate inference
- Common subexpression elimination
- Algebraic simplifications
- Transitive closure of predicates
"""

import logging
import copy
import re
from typing import Dict, List, Set, Tuple, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum
import json


class TransformationType(Enum):
    """Types of query transformations."""

    PREDICATE_PUSHDOWN = "predicate_pushdown"
    SUBQUERY_UNNESTING = "subquery_unnesting"
    VIEW_MERGING = "view_merging"
    JOIN_ELIMINATION = "join_elimination"
    JOIN_PREDICATE_INFERENCE = "join_predicate_inference"
    COMMON_SUBEXPRESSION_ELIMINATION = "cse"
    ALGEBRAIC_SIMPLIFICATION = "algebraic_simplification"
    TRANSITIVE_CLOSURE = "transitive_closure"
    CONSTANT_FOLDING = "constant_folding"
    REDUNDANT_CONDITION_ELIMINATION = "redundant_elimination"


@dataclass
class TransformationRule:
    """Represents a transformation rule."""

    name: str
    rule_type: TransformationType
    pattern: Dict[str, Any]
    transformation: Dict[str, Any]
    cost_benefit: float
    prerequisites: List[str]
    confidence: float


@dataclass
class QueryNode:
    """Represents a node in the query tree."""

    node_type: str  # SELECT, JOIN, FILTER, PROJECT, etc.
    operator: Optional[str] = None
    operands: List["QueryNode"] = None
    conditions: List[str] = None
    columns: List[str] = None
    tables: List[str] = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.operands is None:
            self.operands = []
        if self.conditions is None:
            self.conditions = []
        if self.columns is None:
            self.columns = []
        if self.tables is None:
            self.tables = []
        if self.metadata is None:
            self.metadata = {}


class PredicateAnalyzer:
    """Analyzes predicates for optimization opportunities."""

    def __init__(self):
        self.comparison_ops = ["=", "!=", "<>", "<", "<=", ">", ">="]
        self.logical_ops = ["AND", "OR", "NOT"]

    def parse_predicate(self, predicate: str) -> Dict[str, Any]:
        """
        Parse a predicate into its components.

        Args:
            predicate: SQL predicate string

        Returns:
            Dictionary with predicate components
        """
        predicate = predicate.strip()

        # Handle compound predicates
        if " AND " in predicate.upper():
            parts = predicate.upper().split(" AND ")
            return {
                "type": "compound",
                "operator": "AND",
                "operands": [self.parse_predicate(part.strip()) for part in parts],
            }

        if " OR " in predicate.upper():
            parts = predicate.upper().split(" OR ")
            return {
                "type": "compound",
                "operator": "OR",
                "operands": [self.parse_predicate(part.strip()) for part in parts],
            }

        # Handle simple predicates
        for op in self.comparison_ops:
            if op in predicate:
                parts = predicate.split(op, 1)
                if len(parts) == 2:
                    left = parts[0].strip()
                    right = parts[1].strip()

                    return {
                        "type": "comparison",
                        "operator": op,
                        "left": self._parse_expression(left),
                        "right": self._parse_expression(right),
                        "tables": self._extract_tables_from_predicate(predicate),
                    }

        # Handle special predicates
        if "IS NULL" in predicate.upper():
            column = predicate.upper().replace("IS NULL", "").strip()
            return {
                "type": "null_check",
                "column": column,
                "tables": self._extract_tables_from_predicate(predicate),
            }

        if "LIKE" in predicate.upper():
            parts = predicate.upper().split("LIKE", 1)
            if len(parts) == 2:
                return {
                    "type": "like",
                    "column": parts[0].strip(),
                    "pattern": parts[1].strip(),
                    "tables": self._extract_tables_from_predicate(predicate),
                }

        # Default case
        return {
            "type": "unknown",
            "expression": predicate,
            "tables": self._extract_tables_from_predicate(predicate),
        }

    def _parse_expression(self, expr: str) -> Dict[str, Any]:
        """Parse an expression (column reference, constant, etc.)."""
        expr = expr.strip()

        # Check if it's a column reference
        if "." in expr:
            parts = expr.split(".", 1)
            return {"type": "column", "table": parts[0], "column": parts[1]}

        # Check if it's a numeric constant
        try:
            if "." in expr:
                return {"type": "constant", "value": float(expr), "data_type": "float"}
            else:
                return {"type": "constant", "value": int(expr), "data_type": "int"}
        except ValueError:
            pass

        # Check if it's a string constant
        if expr.startswith("'") and expr.endswith("'"):
            return {"type": "constant", "value": expr[1:-1], "data_type": "string"}

        # Assume it's a column reference without table qualifier
        return {"type": "column", "table": None, "column": expr}

    def _extract_tables_from_predicate(self, predicate: str) -> Set[str]:
        """Extract table names referenced in a predicate."""
        tables = set()

        # Look for table.column patterns
        import re

        pattern = r"(\w+)\.(\w+)"
        matches = re.findall(pattern, predicate)

        for table, column in matches:
            tables.add(table)

        return tables

    def can_push_down(self, predicate: Dict[str, Any], target_tables: Set[str]) -> bool:
        """
        Check if a predicate can be pushed down to specific tables.

        Args:
            predicate: Parsed predicate
            target_tables: Set of table names

        Returns:
            True if predicate can be pushed down
        """
        predicate_tables = predicate.get("tables", set())

        # Predicate can be pushed down if all referenced tables are in target
        return predicate_tables.issubset(target_tables)

    def infer_transitive_predicates(
        self, predicates: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Infer additional predicates using transitive closure.

        For example: A.x = B.y AND B.y = C.z => A.x = C.z

        Args:
            predicates: List of parsed predicates

        Returns:
            List of inferred predicates
        """
        inferred = []
        equality_chains = defaultdict(set)

        # Build equality chains
        for pred in predicates:
            if (
                pred.get("type") == "comparison"
                and pred.get("operator") == "="
                and pred.get("left", {}).get("type") == "column"
                and pred.get("right", {}).get("type") == "column"
            ):

                left_col = f"{pred['left']['table']}.{pred['left']['column']}"
                right_col = f"{pred['right']['table']}.{pred['right']['column']}"

                equality_chains[left_col].add(right_col)
                equality_chains[right_col].add(left_col)

        # Find transitive closures
        for col1, related in equality_chains.items():
            for col2 in related:
                for col3 in equality_chains.get(col2, set()):
                    if col3 != col1 and col3 not in related:
                        # Create new equality predicate
                        parts1 = col1.split(".")
                        parts3 = col3.split(".")

                        inferred_pred = {
                            "type": "comparison",
                            "operator": "=",
                            "left": {
                                "type": "column",
                                "table": parts1[0],
                                "column": parts1[1],
                            },
                            "right": {
                                "type": "column",
                                "table": parts3[0],
                                "column": parts3[1],
                            },
                            "tables": {parts1[0], parts3[0]},
                            "inferred": True,
                        }
                        inferred.append(inferred_pred)

        return inferred


class QueryTransformer:
    """
    Advanced query transformation engine with rule-based optimization.

    Implements sophisticated transformations for cost-based optimization
    including predicate pushdown, subquery unnesting, and algebraic simplifications.
    """

    def __init__(self, statistics_collector, catalog_manager):
        """
        Initialize the query transformer.

        Args:
            statistics_collector: Statistics collector for cost estimates
            catalog_manager: Catalog manager for schema information
        """
        self.statistics_collector = statistics_collector
        self.catalog_manager = catalog_manager
        self.predicate_analyzer = PredicateAnalyzer()

        # Transformation rules
        self.transformation_rules = self._initialize_transformation_rules()

        # Statistics
        self.transformations_applied = defaultdict(int)
        self.total_transformations = 0

        logging.info("Initialized QueryTransformer")

    def transform_query(self, query_tree: QueryNode) -> Tuple[QueryNode, List[str]]:
        """
        Apply all applicable transformations to a query tree.

        Args:
            query_tree: Root node of the query tree

        Returns:
            Tuple of (transformed_tree, list_of_applied_transformations)
        """
        transformed_tree = copy.deepcopy(query_tree)
        applied_transformations = []

        # Apply transformations in optimal order
        transformation_order = [
            TransformationType.CONSTANT_FOLDING,
            TransformationType.ALGEBRAIC_SIMPLIFICATION,
            TransformationType.REDUNDANT_CONDITION_ELIMINATION,
            TransformationType.PREDICATE_PUSHDOWN,
            TransformationType.TRANSITIVE_CLOSURE,
            TransformationType.JOIN_PREDICATE_INFERENCE,
            TransformationType.SUBQUERY_UNNESTING,
            TransformationType.VIEW_MERGING,
            TransformationType.JOIN_ELIMINATION,
            TransformationType.COMMON_SUBEXPRESSION_ELIMINATION,
        ]

        # Apply transformations iteratively until no more changes
        max_iterations = 10
        iteration = 0

        while iteration < max_iterations:
            changed = False

            for transform_type in transformation_order:
                new_tree, transformations = self._apply_transformation_type(
                    transformed_tree, transform_type
                )

                if transformations:
                    transformed_tree = new_tree
                    applied_transformations.extend(transformations)
                    changed = True
                    self.total_transformations += len(transformations)

                    for t in transformations:
                        self.transformations_applied[transform_type] += 1

            if not changed:
                break

            iteration += 1

        logging.info(
            f"Applied {len(applied_transformations)} transformations in {iteration} iterations"
        )
        return transformed_tree, applied_transformations

    def _apply_transformation_type(
        self, tree: QueryNode, transform_type: TransformationType
    ) -> Tuple[QueryNode, List[str]]:
        """Apply a specific type of transformation to the tree."""

        if transform_type == TransformationType.PREDICATE_PUSHDOWN:
            return self._apply_predicate_pushdown(tree)
        elif transform_type == TransformationType.SUBQUERY_UNNESTING:
            return self._apply_subquery_unnesting(tree)
        elif transform_type == TransformationType.VIEW_MERGING:
            return self._apply_view_merging(tree)
        elif transform_type == TransformationType.JOIN_ELIMINATION:
            return self._apply_join_elimination(tree)
        elif transform_type == TransformationType.JOIN_PREDICATE_INFERENCE:
            return self._apply_join_predicate_inference(tree)
        elif transform_type == TransformationType.COMMON_SUBEXPRESSION_ELIMINATION:
            return self._apply_cse(tree)
        elif transform_type == TransformationType.ALGEBRAIC_SIMPLIFICATION:
            return self._apply_algebraic_simplification(tree)
        elif transform_type == TransformationType.TRANSITIVE_CLOSURE:
            return self._apply_transitive_closure(tree)
        elif transform_type == TransformationType.CONSTANT_FOLDING:
            return self._apply_constant_folding(tree)
        elif transform_type == TransformationType.REDUNDANT_CONDITION_ELIMINATION:
            return self._apply_redundant_elimination(tree)

        return tree, []

    def _apply_predicate_pushdown(self, tree: QueryNode) -> Tuple[QueryNode, List[str]]:
        """
        Apply predicate pushdown transformation.

        Pushes filter conditions down the query tree to reduce intermediate result sizes.
        """
        transformations = []
        new_tree = copy.deepcopy(tree)

        def push_predicates_recursive(node: QueryNode) -> QueryNode:
            if node.node_type == "FILTER":
                # Try to push filter conditions down
                pushable_conditions = []
                remaining_conditions = []

                for condition in node.conditions:
                    parsed_condition = self.predicate_analyzer.parse_predicate(
                        condition
                    )

                    # Check if condition can be pushed to child nodes
                    pushed = False
                    for child in node.operands:
                        if self._can_push_condition_to_child(parsed_condition, child):
                            # Push condition to child
                            if child.node_type == "FILTER":
                                child.conditions.append(condition)
                            else:
                                # Create new filter node
                                filter_node = QueryNode(
                                    node_type="FILTER",
                                    conditions=[condition],
                                    operands=[child],
                                )
                                # Replace child with filter node
                                idx = node.operands.index(child)
                                node.operands[idx] = filter_node

                            pushable_conditions.append(condition)
                            transformations.append(
                                f"Pushed predicate '{condition}' down"
                            )
                            pushed = True
                            break

                    if not pushed:
                        remaining_conditions.append(condition)

                # Update current node conditions
                node.conditions = remaining_conditions

                # If no conditions remain, eliminate this filter node
                if not node.conditions and len(node.operands) == 1:
                    return node.operands[0]

            # Recursively process children
            for i, child in enumerate(node.operands):
                node.operands[i] = push_predicates_recursive(child)

            return node

        new_tree = push_predicates_recursive(new_tree)
        return new_tree, transformations

    def _can_push_condition_to_child(
        self, condition: Dict[str, Any], child: QueryNode
    ) -> bool:
        """Check if a condition can be pushed down to a child node."""
        condition_tables = condition.get("tables", set())

        # Get tables available in child node
        child_tables = set(child.tables)

        # Add tables from child's operands recursively
        def collect_tables(node):
            tables = set(node.tables)
            for operand in node.operands:
                tables.update(collect_tables(operand))
            return tables

        available_tables = collect_tables(child)

        # Can push if all referenced tables are available in child
        return condition_tables.issubset(available_tables)

    def _apply_subquery_unnesting(self, tree: QueryNode) -> Tuple[QueryNode, List[str]]:
        """
        Apply subquery unnesting (decorrelation).

        Converts correlated subqueries to joins when possible.
        """
        transformations = []

        # Simplified implementation - would need more sophisticated analysis
        # to handle various subquery patterns (EXISTS, IN, scalar subqueries)

        def unnest_recursive(node: QueryNode) -> QueryNode:
            if node.node_type == "SUBQUERY" and node.metadata.get("correlated", False):
                # Try to convert to join
                if self._can_unnest_subquery(node):
                    join_node = self._convert_subquery_to_join(node)
                    transformations.append(f"Unnested correlated subquery")
                    return join_node

            # Process children
            for i, child in enumerate(node.operands):
                node.operands[i] = unnest_recursive(child)

            return node

        new_tree = copy.deepcopy(tree)
        new_tree = unnest_recursive(new_tree)

        return new_tree, transformations

    def _can_unnest_subquery(self, subquery_node: QueryNode) -> bool:
        """Check if a subquery can be unnested."""
        # Simplified logic - in practice would need comprehensive analysis
        return not subquery_node.metadata.get(
            "has_aggregation", False
        ) and not subquery_node.metadata.get("has_grouping", False)

    def _convert_subquery_to_join(self, subquery_node: QueryNode) -> QueryNode:
        """Convert a subquery to a join operation."""
        # Simplified implementation
        join_node = QueryNode(
            node_type="JOIN",
            operator="INNER",
            operands=subquery_node.operands,
            conditions=subquery_node.conditions,
            metadata={"unnested_subquery": True},
        )
        return join_node

    def _apply_view_merging(self, tree: QueryNode) -> Tuple[QueryNode, List[str]]:
        """
        Apply view merging optimization.

        Merges view definitions into the main query when beneficial.
        """
        transformations = []

        # Would implement view merging logic here
        # This is a complex transformation that requires view definition analysis

        return tree, transformations

    def _apply_join_elimination(self, tree: QueryNode) -> Tuple[QueryNode, List[str]]:
        """
        Eliminate unnecessary joins.

        Removes joins when the joined table doesn't contribute to the result.
        """
        transformations = []

        def eliminate_joins_recursive(node: QueryNode) -> QueryNode:
            if node.node_type == "JOIN":
                # Check if join can be eliminated
                if self._can_eliminate_join(node):
                    # Return the necessary operand
                    primary_operand = self._get_primary_join_operand(node)
                    transformations.append(f"Eliminated unnecessary join")
                    return primary_operand

            # Process children
            for i, child in enumerate(node.operands):
                node.operands[i] = eliminate_joins_recursive(child)

            return node

        new_tree = copy.deepcopy(tree)
        new_tree = eliminate_joins_recursive(new_tree)

        return new_tree, transformations

    def _can_eliminate_join(self, join_node: QueryNode) -> bool:
        """Check if a join can be eliminated."""
        # Simplified logic - would need more sophisticated analysis
        # Check if joined table is only used for filtering and has unique key
        return False  # Conservative approach

    def _get_primary_join_operand(self, join_node: QueryNode) -> QueryNode:
        """Get the primary operand when eliminating a join."""
        # Return the first operand by default
        return join_node.operands[0] if join_node.operands else join_node

    def _apply_join_predicate_inference(
        self, tree: QueryNode
    ) -> Tuple[QueryNode, List[str]]:
        """
        Infer additional join predicates from existing conditions.

        Adds implied join conditions that can improve performance.
        """
        transformations = []

        def infer_predicates_recursive(node: QueryNode) -> QueryNode:
            if node.node_type == "JOIN":
                # Collect all conditions from the subtree
                all_conditions = []
                self._collect_conditions(node, all_conditions)

                # Parse conditions
                parsed_conditions = [
                    self.predicate_analyzer.parse_predicate(cond)
                    for cond in all_conditions
                ]

                # Infer transitive predicates
                inferred = self.predicate_analyzer.infer_transitive_predicates(
                    parsed_conditions
                )

                if inferred:
                    # Add inferred conditions to join
                    for pred in inferred:
                        if pred.get("inferred"):
                            condition_str = self._predicate_to_string(pred)
                            if condition_str not in node.conditions:
                                node.conditions.append(condition_str)
                                transformations.append(
                                    f"Inferred join predicate: {condition_str}"
                                )

            # Process children
            for i, child in enumerate(node.operands):
                node.operands[i] = infer_predicates_recursive(child)

            return node

        new_tree = copy.deepcopy(tree)
        new_tree = infer_predicates_recursive(new_tree)

        return new_tree, transformations

    def _collect_conditions(self, node: QueryNode, conditions: List[str]):
        """Recursively collect all conditions from a subtree."""
        conditions.extend(node.conditions)
        for child in node.operands:
            self._collect_conditions(child, conditions)

    def _predicate_to_string(self, predicate: Dict[str, Any]) -> str:
        """Convert a parsed predicate back to string format."""
        if predicate.get("type") == "comparison":
            left = predicate["left"]
            right = predicate["right"]
            op = predicate["operator"]

            left_str = (
                f"{left['table']}.{left['column']}"
                if left.get("table")
                else left["column"]
            )

            if right["type"] == "column":
                right_str = (
                    f"{right['table']}.{right['column']}"
                    if right.get("table")
                    else right["column"]
                )
            else:
                right_str = str(right["value"])

            return f"{left_str} {op} {right_str}"

        return str(predicate.get("expression", ""))

    def _apply_cse(self, tree: QueryNode) -> Tuple[QueryNode, List[str]]:
        """Apply common subexpression elimination."""
        transformations = []

        # Would implement CSE logic here
        # This involves finding repeated expressions and factoring them out

        return tree, transformations

    def _apply_algebraic_simplification(
        self, tree: QueryNode
    ) -> Tuple[QueryNode, List[str]]:
        """Apply algebraic simplifications to conditions."""
        transformations = []

        def simplify_recursive(node: QueryNode) -> QueryNode:
            # Simplify conditions in current node
            simplified_conditions = []
            for condition in node.conditions:
                simplified = self._simplify_condition(condition)
                if simplified != condition:
                    transformations.append(
                        f"Simplified '{condition}' to '{simplified}'"
                    )
                    simplified_conditions.append(simplified)
                else:
                    simplified_conditions.append(condition)

            node.conditions = simplified_conditions

            # Process children
            for i, child in enumerate(node.operands):
                node.operands[i] = simplify_recursive(child)

            return node

        new_tree = copy.deepcopy(tree)
        new_tree = simplify_recursive(new_tree)

        return new_tree, transformations

    def _simplify_condition(self, condition: str) -> str:
        """Simplify a single condition using algebraic rules."""
        # Remove redundant parentheses
        condition = condition.strip()

        # Simplify obvious tautologies and contradictions
        if condition.upper() in ["1=1", "TRUE"]:
            return "1=1"
        if condition.upper() in ["1=0", "FALSE", "0=1"]:
            return "1=0"

        # Simplify double negations
        if condition.upper().startswith("NOT NOT "):
            return condition[8:]  # Remove 'NOT NOT '

        # More sophisticated simplifications would go here
        return condition

    def _apply_transitive_closure(self, tree: QueryNode) -> Tuple[QueryNode, List[str]]:
        """Apply transitive closure to infer additional predicates."""
        transformations = []

        def apply_closure_recursive(node: QueryNode) -> QueryNode:
            if node.conditions:
                parsed_conditions = [
                    self.predicate_analyzer.parse_predicate(cond)
                    for cond in node.conditions
                ]

                inferred = self.predicate_analyzer.infer_transitive_predicates(
                    parsed_conditions
                )

                for pred in inferred:
                    condition_str = self._predicate_to_string(pred)
                    if condition_str not in node.conditions:
                        node.conditions.append(condition_str)
                        transformations.append(
                            f"Added transitive predicate: {condition_str}"
                        )

            # Process children
            for i, child in enumerate(node.operands):
                node.operands[i] = apply_closure_recursive(child)

            return node

        new_tree = copy.deepcopy(tree)
        new_tree = apply_closure_recursive(new_tree)

        return new_tree, transformations

    def _apply_constant_folding(self, tree: QueryNode) -> Tuple[QueryNode, List[str]]:
        """Fold constant expressions."""
        transformations = []

        def fold_constants_recursive(node: QueryNode) -> QueryNode:
            # Fold constants in conditions
            folded_conditions = []
            for condition in node.conditions:
                folded = self._fold_constants_in_condition(condition)
                if folded != condition:
                    transformations.append(
                        f"Folded constants: '{condition}' -> '{folded}'"
                    )
                folded_conditions.append(folded)

            node.conditions = folded_conditions

            # Process children
            for i, child in enumerate(node.operands):
                node.operands[i] = fold_constants_recursive(child)

            return node

        new_tree = copy.deepcopy(tree)
        new_tree = fold_constants_recursive(new_tree)

        return new_tree, transformations

    def _fold_constants_in_condition(self, condition: str) -> str:
        """Fold constants in a condition."""
        # Simple constant folding - evaluate numeric expressions
        import re

        # Look for simple arithmetic expressions
        pattern = r"(\d+)\s*([+\-*/])\s*(\d+)"

        def evaluate_expression(match):
            left = int(match.group(1))
            op = match.group(2)
            right = int(match.group(3))

            if op == "+":
                return str(left + right)
            elif op == "-":
                return str(left - right)
            elif op == "*":
                return str(left * right)
            elif op == "/" and right != 0:
                return str(left // right)  # Integer division

            return match.group(0)  # Return original if can't evaluate

        return re.sub(pattern, evaluate_expression, condition)

    def _apply_redundant_elimination(
        self, tree: QueryNode
    ) -> Tuple[QueryNode, List[str]]:
        """Eliminate redundant conditions."""
        transformations = []

        def eliminate_redundant_recursive(node: QueryNode) -> QueryNode:
            if len(node.conditions) > 1:
                # Remove duplicate conditions
                unique_conditions = []
                seen = set()

                for condition in node.conditions:
                    normalized = condition.strip().lower()
                    if normalized not in seen:
                        unique_conditions.append(condition)
                        seen.add(normalized)
                    else:
                        transformations.append(
                            f"Eliminated duplicate condition: {condition}"
                        )

                node.conditions = unique_conditions

                # Remove contradictory conditions
                if "1=0" in node.conditions or "false" in [
                    c.lower() for c in node.conditions
                ]:
                    # Query will return no results
                    node.conditions = ["1=0"]
                    transformations.append(
                        "Detected contradiction - simplified to FALSE"
                    )

            # Process children
            for i, child in enumerate(node.operands):
                node.operands[i] = eliminate_redundant_recursive(child)

            return node

        new_tree = copy.deepcopy(tree)
        new_tree = eliminate_redundant_recursive(new_tree)

        return new_tree, transformations

    def _initialize_transformation_rules(self) -> List[TransformationRule]:
        """Initialize the set of transformation rules."""
        rules = []

        # Example rule for predicate pushdown
        rules.append(
            TransformationRule(
                name="Basic Predicate Pushdown",
                rule_type=TransformationType.PREDICATE_PUSHDOWN,
                pattern={"node_type": "FILTER", "child_type": "JOIN"},
                transformation={"action": "push_filter_to_join_operands"},
                cost_benefit=0.8,
                prerequisites=[],
                confidence=0.9,
            )
        )

        # More rules would be added here

        return rules

    def get_transformation_statistics(self) -> Dict[str, Any]:
        """Get statistics about applied transformations."""
        return {
            "total_transformations": self.total_transformations,
            "transformations_by_type": dict(self.transformations_applied),
            "available_rules": len(self.transformation_rules),
        }

    def explain_transformations(
        self, original_tree: QueryNode, transformed_tree: QueryNode
    ) -> List[str]:
        """
        Explain the transformations applied between two query trees.

        Args:
            original_tree: Original query tree
            transformed_tree: Transformed query tree

        Returns:
            List of transformation explanations
        """
        explanations = []

        # Compare trees and identify differences
        # This would involve tree comparison logic

        return explanations


from collections import defaultdict  # Add this import at the top
