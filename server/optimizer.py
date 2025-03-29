import logging


class Optimizer:
    def __init__(self, catalog_manager, index_manager):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager

    def optimize(self, plan):
        """
        Optimize the execution plan.
        """
        logging.debug(f"Optimizing plan: {plan}")
        # Log the initial plan
        logging.info(f"Initial plan: {plan}")
        if plan["type"] == "SELECT":
            optimized_plan = self.optimize_select(plan)
        elif plan["type"] == "JOIN":
            optimized_plan = self.optimize_join(plan)
        else:
            optimized_plan = plan
        # Log the optimized plan
        logging.info(f"Optimized plan: {optimized_plan}")
        return optimized_plan

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
        Optimize a JOIN query.
        """
        logging.debug(f"Optimizing JOIN plan: {plan}")
        if plan["type"] == "JOIN":
            condition = plan["condition"]
            if "=" in condition:  # Equality-based join condition
                plan["type"] = "HASH_JOIN"
                plan["hash_key"] = condition.split("=")[0].strip()
                logging.debug(
                    f"Optimized join to HASH_JOIN with condition: {condition}"
                )

        return plan

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

