"""_summary_

Returns:
    _type_: _description_
"""

import logging
import re
import os
from bptree import BPlusTree


class Optimizer:
    """Query optimizer class"""

    def __init__(self, catalog_manager, index_manager):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager

    def optimize(self, plan):
        """
        Optimize the execution plan.
        """
        # Do not print the whole plan in the log message, it's verbose
        logging.info(
            "Starting plan optimization for plan type: %s", plan.get(
                "type", "UNKNOWN")
        )

        # Apply different optimization strategies based on plan type
        if plan["type"] == "SELECT":
            optimized_plan = self.optimize_select(plan)
        elif plan["type"] == "JOIN":
            optimized_plan = self.optimize_join(plan)
        else:
            # For other plan types, don't modify
            optimized_plan = plan

        # Check for optimizations applied
        changes = self._get_plan_changes(plan, optimized_plan)
        if changes:
            logging.info("Optimizations applied:")
            for change in changes:
                logging.info("  - %s", change)
        else:
            logging.info("No optimizations applied")

        return optimized_plan

    def _get_plan_changes(self, original, optimized):
        """
        Get a list of changes made during optimization.

        Args:
            original: The original execution plan
            optimized: The optimized execution plan

        Returns:
            list: Descriptions of optimizations applied
        """
        changes = []

        # Check for index usage
        if optimized.get("use_index") and not original.get("use_index"):
            changes.append(f"Using index for: {
                           optimized.get('table', 'unknown')}")

        # Check for join algorithm changes
        if (
            original.get("type") == "JOIN"
            and optimized.get("type") == "JOIN"
            and original.get("join_algorithm") != optimized.get("join_algorithm")
        ):
            changes.append(
                f"Changed join algorithm from {original.get('join_algorithm', 'NONE')} to {
                    optimized.get('join_algorithm', 'NONE')}"
            )

        # Check for index join optimization
        if original.get("type") == "JOIN" and optimized.get("type") == "INDEX_JOIN":
            changes.append("Converted nested loop join to index join")

        # Check for filter merging with join
        if (
            original.get("type") == "JOIN"
            and "filter" in original
            and optimized.get("type") == "JOIN"
            and "filter" not in optimized
        ):
            changes.append("Merged filter condition into join condition")

        # Check for projection merging
        if (
            original.get("type") == "PROJECT"
            and optimized.get("type") == "PROJECT"
            and "child" in original
            and "child" in optimized
            and original["child"].get("type") == "PROJECT"
            and optimized["child"].get("type") != "PROJECT"
        ):
            changes.append("Merged multiple projections")

        # Check for always-true filter elimination
        if (
            original.get("type") == "FILTER"
            and original.get("condition") == "1=1"
            and optimized.get("type") != "FILTER"
        ):
            changes.append("Eliminated always-true filter condition")

        # Check for filter merge into scan
        if (
            original.get("type") == "FILTER"
            and "child" in original
            and original["child"].get("type") == "SEQ_SCAN"
            and optimized.get("type") == "SEQ_SCAN"
            and "filter" in optimized
        ):
            changes.append("Merged filter condition into sequential scan")

        # Check for expression rewrite in join
        if (
            original.get("type") == "JOIN"
            and optimized.get("type") == "JOIN"
            and original.get("condition") != optimized.get("condition")
            and (
                "#0" in optimized.get("condition", "")
                or "#1" in optimized.get("condition", "")
            )
        ):
            changes.append("Rewrote join condition for better execution")

        # Check for ORDER BY to INDEX SCAN optimization
        if original.get("type") == "ORDER_BY" and optimized.get("type") == "INDEX_SCAN":
            changes.append(
                f"Converted ORDER BY to index scan on {
                    optimized.get('column', 'unknown')} column"
            )

        # Check for SORT+LIMIT to TOP N optimization
        if (
            original.get("type") == "SORT"
            and "limit" in original
            and optimized.get("type") == "TOP_N"
        ):
            changes.append(
                f"Optimized SORT+LIMIT to TOP_N with limit {
                    optimized.get('n', 'unknown')}"
            )

        # Check for predicate pushdown
        if (
            original.get("type") == "FILTER"
            and "child" in original
            and optimized.get("type") in ["SEQ_SCAN", "INDEX_SCAN"]
            and "filter" in optimized
        ):
            changes.append("Pushed down filter predicate to scan operation")

        # Check for join reordering
        if (
            original.get("type") == "JOIN"
            and optimized.get("type") == "JOIN"
            and original.get("table1") == optimized.get("table2")
            and original.get("table2") == optimized.get("table1")
        ):
            changes.append(
                f"Reordered join tables ({optimized.get(
                    'table2', 'unknown')} first) to utilize available index"
            )

        # Check for additional index optimizations in general
        if "index" in optimized and "index" not in original:
            index_name = optimized.get("index", "unknown")
            if isinstance(index_name, dict) and "name" in index_name:
                index_name = index_name["name"]
            changes.append(f"Using index {index_name} for faster access")

        # Check for additional query plan structure changes
        if original.get("type") != optimized.get("type"):
            # If not covered by other checks, report the general change
            if not any(change.startswith("Converted") for change in changes):
                changes.append(
                    f"Changed plan type from {original.get('type', 'unknown')} to {
                        optimized.get('type', 'unknown')}"
                )

        # Check if a subplan was eliminated (plan depth decreased)
        original_depth = self._calculate_plan_depth(original)
        optimized_depth = self._calculate_plan_depth(optimized)
        if original_depth > optimized_depth:
            changes.append(
                f"Reduced query plan depth from {
                    original_depth} to {optimized_depth}"
            )

        return changes

    def _calculate_plan_depth(self, plan, current_depth=1):
        """Calculate the depth of a query plan."""
        if not isinstance(plan, dict) or "child" not in plan:
            return current_depth
        return self._calculate_plan_depth(plan["child"], current_depth + 1)

    def optimize_select(self, plan):
        """
        Optimize a SELECT query.
        """
        logging.debug(f"Optimizing SELECT plan: {plan}")
        table_name = plan["table"]
        condition = plan.get("condition")

        # Check if an index exists for the condition column
        if condition:
            column = condition.split("=")[0].strip()
            index = self.index_manager.get_index(f"{table_name}.idx_{column}")
            if index:
                plan["use_index"] = True
                plan["index"] = index
                logging.debug(
                    f"Using index for column: {column} in table: {table_name}"
                )

        return plan

    def optimize_join(self, plan):
        """
        Optimize a JOIN query by selecting the most appropriate join algorithm.
        """
        logging.debug(f"Optimizing JOIN plan: {plan}")

        table1 = plan.get("table1", "")
        table2 = plan.get("table2", "")
        condition = plan.get("condition", "")

        # Extract column names from condition
        left_column, right_column = self._extract_join_columns(condition)
        if not left_column or not right_column:
            # Default to hash join for cross joins or complex conditions
            plan["join_algorithm"] = "HASH"
            return plan

        # Check for indexes on join columns
        left_index = self.index_manager.get_index(f"{table1}.{left_column}")
        right_index = self.index_manager.get_index(f"{table2}.{right_column}")

        # Get table statistics
        left_table_size = self.catalog_manager.get_table_size(table1)
        right_table_size = self.catalog_manager.get_table_size(table2)

        # Algorithm selection logic:
        if left_index or right_index:
            # Use index join if an index exists on either join column
            plan["join_algorithm"] = "INDEX"
            logging.debug(f"Selected INDEX join due to available index")
        elif left_table_size < 100 and right_table_size < 100:
            # For very small tables, nested loop can be efficient
            plan["join_algorithm"] = "NESTED_LOOP"
            logging.debug(f"Selected NESTED_LOOP join for small tables")
        elif abs(left_table_size - right_table_size) > 10 * min(
            left_table_size, right_table_size
        ):
            # For tables with very different sizes, hash join is usually best
            plan["join_algorithm"] = "HASH"
            logging.debug(
                f"Selected HASH join for tables with different sizes")
        elif self.catalog_manager.is_table_sorted(
            table1, left_column
        ) or self.catalog_manager.is_table_sorted(table2, right_column):
            # If one of the tables is already sorted on join column
            plan["join_algorithm"] = "MERGE"
            logging.debug(f"Selected MERGE join for pre-sorted data")
        else:
            # Default to hash join
            plan["join_algorithm"] = "HASH"
            logging.debug(f"Selected HASH join as default")

        # Override with user hint if provided
        if "join_algorithm" in plan and plan["join_algorithm"]:
            logging.debug(
                f"Using user-specified join algorithm: {
                    plan['join_algorithm']}"
            )

        return plan

    def _extract_join_columns(self, condition):
        """
        Extract column names from join condition.

        Args:
            condition: Join condition string like "t1.col1 = t2.col2"

        Returns:
            Tuple of (left_column, right_column)
        """
        if not condition:
            return None, None

        # Try to match table.column = table.column pattern
        match = re.search(r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", condition)
        if match:
            return match.group(2), match.group(4)

        # Try simpler column = column pattern
        match = re.search(r"(\w+)\s*=\s*(\w+)", condition)
        if match:
            return match.group(1), match.group(2)

        return None, None

    def merge_filter_into_join(self, plan):
        """
        Merge filter condition into nested loop join.
        """
        logging.debug(f"Merging filter into join: {plan}")
        if plan["type"] == "JOIN" and "filter" in plan:
            plan["condition"] = f"{plan['condition']} AND {plan['filter']}"
            del plan["filter"]
            logging.debug(f"Merged filter into join: {plan['condition']}")

        return plan

    def merge_projections(self, plan):
        """
        Merge identical projections.
        """
        logging.debug(f"Merging projections: {plan}")
        if plan["type"] == "PROJECT" and "child" in plan:
            if plan["child"]["type"] == "PROJECT":
                plan["columns"] = list(
                    set(plan["columns"] + plan["child"]["columns"]))
                plan["child"] = plan["child"]["child"]
                logging.debug(f"Merged projections: {plan['columns']}")

        return plan

    def optimize_index_join(self, plan):
        """
        Optimize nested loop join into index join.
        """
        logging.debug(f"Optimizing index join: {plan}")
        if plan["type"] == "JOIN":
            condition = plan["condition"]
            col1, col2 = condition.split("=")
            col1 = col1.strip().split(".")[1]
            col2 = col2.strip().split(".")[1]

            index1 = self.index_manager.get_index(
                f"{plan['table1']}.idx_{col1}")
            index2 = self.index_manager.get_index(
                f"{plan['table2']}.idx_{col2}")

            if index1 or index2:
                plan["type"] = "INDEX_JOIN"
                plan["index"] = index1 if index1 else index2
                logging.debug(
                    f"Optimized join to INDEX_JOIN with condition: {condition}"
                )

        return plan

    def eliminate_always_true_filter(self, plan):
        """
        Eliminate always true filter conditions.
        """
        logging.debug(f"Eliminating always true filter: {plan}")
        if plan["type"] == "FILTER" and plan["condition"] == "1=1":
            logging.debug("Eliminated always true filter condition")
            return plan["child"]

        return plan

    def merge_filter_into_scan(self, plan):
        """
        Merge filter into sequential scan.
        """
        logging.debug(f"Merging filter into scan: {plan}")
        if plan["type"] == "FILTER" and plan["child"]["type"] == "SEQ_SCAN":
            plan["child"]["filter"] = plan["condition"]
            logging.debug(f"Merged filter into scan: {plan['condition']}")
            return plan["child"]

        return plan

    def rewrite_expression(self, plan):
        """
        Rewrite expressions for nested loop joins.
        """
        logging.debug(f"Rewriting expression: {plan}")
        if plan["type"] == "JOIN":
            condition = plan["condition"]
            col1, col2 = condition.split("=")
            col1 = col1.strip()
            col2 = col2.strip()

            if col1.startswith("#0") and col2.startswith("#0"):
                plan["condition"] = f"#0.{col1.split('.')[1]} = #1.{
                    col2.split('.')[1]}"
                logging.debug(f"Rewritten join condition: {plan['condition']}")

        return plan

    def optimize_order_by(self, plan):
        """
        Optimize ORDER BY as index scan.
        """
        logging.debug(f"Optimizing ORDER BY: {plan}")
        if plan["type"] == "ORDER_BY":
            index = self.index_manager.get_index(
                f"{plan['table']}.idx_{plan['column']}"
            )
            if index:
                plan["type"] = "INDEX_SCAN"
                plan["index"] = index
                logging.debug(
                    f"Optimized ORDER BY to INDEX_SCAN on column: {
                        plan['column']}"
                )

        return plan

    def optimize_sort_limit(self, plan):
        """
        Optimize SORT + LIMIT as top N.
        """
        logging.debug(f"Optimizing SORT + LIMIT: {plan}")
        if plan["type"] == "SORT" and "limit" in plan:
            plan["type"] = "TOP_N"
            plan["n"] = plan["limit"]
            del plan["limit"]
            logging.debug(
                f"Optimized SORT + LIMIT to TOP_N with limit: {plan['n']}")

        return plan

    def optimize_predicate_pushdown(self, plan):
        """
        Push filter conditions down the query plan.
        """
        logging.debug(f"Optimizing predicate pushdown: {plan}")
        if plan["type"] == "FILTER" and "child" in plan:
            if plan["child"]["type"] in ["SEQ_SCAN", "INDEX_SCAN"]:
                plan["child"]["filter"] = plan["condition"]
                logging.debug(f"Pushed down filter condition: {
                              plan['condition']}")
                return plan["child"]

        return plan

    def optimize_reorder_join(self, plan):
        """
        Reorder joins to use indexes.
        """
        logging.debug(f"Optimizing reorder join: {plan}")
        if plan["type"] == "JOIN":
            condition = plan["condition"]
            col1, col2 = condition.split("=")
            col1 = col1.strip().split(".")[1]
            col2 = col2.strip().split(".")[1]

            index1 = self.index_manager.get_index(
                f"{plan['table1']}.idx_{col1}")
            index2 = self.index_manager.get_index(
                f"{plan['table2']}.idx_{col2}")

            if index1 and not index2:
                plan["table1"], plan["table2"] = plan["table2"], plan["table1"]
                logging.debug(
                    f"Reordered join tables to use index on: {plan['table1']}"
                )

        return plan

    def get_table_size(self, table_name):
        """
        Get the approximate size (number of records) of a table.

        Args:
            table_name: Name of the table

        Returns:
            Approximate number of records in the table
        """
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return 0

        # Load the table file
        table_file = os.path.join(
            self.catalog_manager.tables_dir, db_name, f"{table_name}.tbl"
        )
        if not os.path.exists(table_file):
            return 0

        try:
            # Load the B+ tree
            tree = BPlusTree.load_from_file(table_file)
            if tree is None:
                return 0

            # Count records
            all_records = tree.range_query(float("-inf"), float("inf"))
            return len(all_records) if all_records else 0
        except Exception as e:
            logging.error(f"Error getting table size: {str(e)}")
            return 0

    def is_table_sorted(self, table_name, column_name):
        """
        Check if a table is already sorted by a specific column.
        This is an approximation as we don't actually track sorting.

        Args:
            table_name: Name of the table
            column_name: Name of the column

        Returns:
            True if an index exists on this column (which implies sorting capability)
        """
        # Check if an index exists on this column
        # Using an index is a good indicator that sorted access is possible
        indexes = self.catalog_manager.get_indexes_for_table(table_name)
        for _, idx_info in indexes.items():
            if idx_info.get("column") == column_name:
                return True

        return False
