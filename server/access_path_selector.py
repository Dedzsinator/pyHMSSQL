"""
Access Path Selection Module for Cost-Based Query Optimization.

This module implements sophisticated access path selection that chooses
the optimal way to access data from tables based on:
- Available indexes
- Query predicates and selectivity
- Memory constraints
- System workload

Features:
- Table scan vs index scan decisions
- Index-only scan optimization
- Bitmap scan for multiple indexes
- Multi-index intersection strategies
- Cost-based access path ranking
"""

import logging
import math
from typing import Dict, List, Tuple, Optional, Any, Set
from dataclasses import dataclass
from enum import Enum
import json


class AccessMethod(Enum):
    """Different access methods available."""

    TABLE_SCAN = "table_scan"
    INDEX_SCAN = "index_scan"
    INDEX_ONLY_SCAN = "index_only_scan"
    BITMAP_HEAP_SCAN = "bitmap_heap_scan"
    BITMAP_INDEX_SCAN = "bitmap_index_scan"
    MULTI_INDEX_SCAN = "multi_index_scan"
    PARALLEL_SEQ_SCAN = "parallel_seq_scan"
    PARALLEL_INDEX_SCAN = "parallel_index_scan"


@dataclass
class IndexInfo:
    """Information about an available index."""

    name: str
    table_name: str
    columns: List[str]
    is_unique: bool
    is_clustered: bool
    is_covering: bool  # Whether index covers all needed columns
    selectivity: float
    pages: int
    height: int
    clustering_factor: float

    def matches_predicate(self, column: str) -> bool:
        """Check if index can be used for a column predicate."""
        return column in self.columns

    def get_leading_column(self) -> Optional[str]:
        """Get the leading column of the index."""
        return self.columns[0] if self.columns else None


@dataclass
class Predicate:
    """Represents a query predicate."""

    column: str
    operator: str  # =, <, >, <=, >=, LIKE, IN, etc.
    value: Any
    selectivity: float
    table: Optional[str] = None

    def is_equality(self) -> bool:
        """Check if this is an equality predicate."""
        return self.operator == "="

    def is_range(self) -> bool:
        """Check if this is a range predicate."""
        return self.operator in ["<", ">", "<=", ">=", "BETWEEN"]

    def can_use_index(self, index: IndexInfo) -> bool:
        """Check if this predicate can use the given index."""
        if not index.matches_predicate(self.column):
            return False

        # Leading column can always be used
        if index.get_leading_column() == self.column:
            return True

        # Non-leading columns can only be used for equality in compound indexes
        return self.is_equality()


@dataclass
class AccessPath:
    """Represents a complete access path with cost information."""

    method: AccessMethod
    table_name: str
    estimated_cost: float
    estimated_rows: int
    startup_cost: float
    total_cost: float
    indexes_used: List[str]
    predicates_used: List[Predicate]
    columns_needed: List[str]
    is_covering: bool  # Whether all needed columns are available
    parallel_workers: int = 1
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class AccessPathSelector:
    """
    Advanced access path selector that chooses optimal data access strategies.

    Uses cost-based analysis to select between:
    - Sequential table scans
    - Various index scan strategies
    - Bitmap scans for complex predicates
    - Parallel scan strategies
    - Multi-index intersection/union
    """

    def __init__(self, cost_estimator, statistics_collector, index_manager):
        """
        Initialize the access path selector.

        Args:
            cost_estimator: Cost estimator for path evaluation
            statistics_collector: Statistics collector for selectivity
            index_manager: Index manager for available indexes
        """
        self.cost_estimator = cost_estimator
        self.statistics_collector = statistics_collector
        self.index_manager = index_manager

        # Configuration parameters
        self.min_parallel_table_size = 10000  # Minimum rows for parallel scan
        self.bitmap_scan_threshold = 0.1  # Use bitmap scan if selectivity > 10%
        self.index_scan_threshold = 0.05  # Use index scan if selectivity < 5%
        self.multi_index_threshold = 2  # Minimum indexes for intersection

        # Statistics
        self.paths_evaluated = 0
        self.index_scans_chosen = 0
        self.table_scans_chosen = 0
        self.bitmap_scans_chosen = 0

        logging.info("Initialized AccessPathSelector")

    def select_access_path(
        self,
        table_name: str,
        predicates: List[Predicate],
        columns_needed: List[str],
        limit: Optional[int] = None,
    ) -> AccessPath:
        """
        Select the optimal access path for a table.

        Args:
            table_name: Name of the table to access
            predicates: List of predicates to apply
            columns_needed: List of columns that need to be retrieved
            limit: Optional LIMIT clause value

        Returns:
            AccessPath with the lowest estimated cost
        """
        logging.debug(
            f"Selecting access path for table {table_name} with {len(predicates)} predicates"
        )

        # Get table statistics
        table_stats = self.statistics_collector.collect_table_statistics(table_name)

        # Get available indexes
        available_indexes = self._get_available_indexes(table_name)

        # Generate all possible access paths
        candidate_paths = []

        # 1. Table scan
        table_scan_path = self._evaluate_table_scan(
            table_name, predicates, columns_needed, table_stats, limit
        )
        candidate_paths.append(table_scan_path)

        # 2. Single index scans
        for index in available_indexes:
            applicable_predicates = [p for p in predicates if p.can_use_index(index)]

            if applicable_predicates:
                # Regular index scan
                index_scan_path = self._evaluate_index_scan(
                    table_name,
                    index,
                    applicable_predicates,
                    columns_needed,
                    table_stats,
                    limit,
                )
                candidate_paths.append(index_scan_path)

                # Index-only scan (if possible)
                if self._can_use_index_only_scan(index, columns_needed, predicates):
                    index_only_path = self._evaluate_index_only_scan(
                        table_name,
                        index,
                        applicable_predicates,
                        columns_needed,
                        table_stats,
                        limit,
                    )
                    candidate_paths.append(index_only_path)

        # 3. Bitmap scans
        bitmap_paths = self._evaluate_bitmap_scans(
            table_name,
            available_indexes,
            predicates,
            columns_needed,
            table_stats,
            limit,
        )
        candidate_paths.extend(bitmap_paths)

        # 4. Multi-index scans
        if len(available_indexes) >= self.multi_index_threshold:
            multi_index_paths = self._evaluate_multi_index_scans(
                table_name,
                available_indexes,
                predicates,
                columns_needed,
                table_stats,
                limit,
            )
            candidate_paths.extend(multi_index_paths)

        # 5. Parallel scans
        if table_stats.row_count >= self.min_parallel_table_size:
            parallel_paths = self._evaluate_parallel_scans(
                table_name, predicates, columns_needed, table_stats, limit
            )
            candidate_paths.extend(parallel_paths)

        # Filter out invalid paths
        valid_paths = [path for path in candidate_paths if path is not None]

        if not valid_paths:
            # Fallback to table scan
            logging.warning(
                f"No valid access paths found for {table_name}, using table scan"
            )
            return table_scan_path

        # Select the path with lowest cost
        best_path = min(valid_paths, key=lambda p: p.total_cost)

        # Update statistics
        self.paths_evaluated += len(valid_paths)
        if best_path.method == AccessMethod.TABLE_SCAN:
            self.table_scans_chosen += 1
        elif best_path.method in [
            AccessMethod.INDEX_SCAN,
            AccessMethod.INDEX_ONLY_SCAN,
        ]:
            self.index_scans_chosen += 1
        elif best_path.method in [
            AccessMethod.BITMAP_HEAP_SCAN,
            AccessMethod.BITMAP_INDEX_SCAN,
        ]:
            self.bitmap_scans_chosen += 1

        logging.debug(
            f"Selected {best_path.method.value} for {table_name} "
            f"(cost: {best_path.total_cost:.2f}, rows: {best_path.estimated_rows})"
        )

        return best_path

    def select_access_paths(self, plan: Dict[str, Any]) -> List[AccessPath]:
        """
        Select optimal access paths for all tables in a query plan.

        Args:
            plan: Query execution plan

        Returns:
            List of AccessPath objects for each table
        """
        access_paths = []

        def extract_table_access(node):
            """Recursively extract table access operations from plan."""
            if isinstance(node, dict):
                if node.get("operation") == "table_scan" and "table_name" in node:
                    # Extract information for access path selection
                    table_name = node["table_name"]
                    predicates = self._extract_predicates(node.get("predicates", []))
                    columns_needed = node.get("columns", ["*"])
                    limit = node.get("limit")

                    # Select optimal access path
                    access_path = self.select_access_path(
                        table_name, predicates, columns_needed, limit
                    )
                    access_paths.append(access_path)

                # Recursively process child nodes
                for value in node.values():
                    if isinstance(value, (dict, list)):
                        extract_table_access(value)
            elif isinstance(node, list):
                for item in node:
                    extract_table_access(item)

        extract_table_access(plan)
        return access_paths

    def _extract_predicates(self, predicate_list: List[Any]) -> List[Predicate]:
        """Convert plan predicates to Predicate objects."""
        predicates = []
        for pred in predicate_list:
            if isinstance(pred, dict):
                predicates.append(
                    Predicate(
                        column=pred.get("column", ""),
                        operator=pred.get("operator", "="),
                        value=pred.get("value", None),
                        selectivity=pred.get("selectivity", 0.1),
                    )
                )
        return predicates

    def _get_available_indexes(self, table_name: str) -> List[IndexInfo]:
        """Get all available indexes for a table."""
        indexes = []

        try:
            # Get indexes from index manager
            table_indexes = getattr(
                self.index_manager, "get_table_indexes", lambda x: {}
            )(table_name)

            for index_name, index_data in table_indexes.items():
                index_info = IndexInfo(
                    name=index_name,
                    table_name=table_name,
                    columns=index_data.get("columns", []),
                    is_unique=index_data.get("unique", False),
                    is_clustered=index_data.get("clustered", False),
                    is_covering=False,  # Will be determined per query
                    selectivity=index_data.get("selectivity", 1.0),
                    pages=index_data.get("pages", 100),
                    height=index_data.get("height", 3),
                    clustering_factor=index_data.get("clustering_factor", 0.5),
                )
                indexes.append(index_info)

        except Exception as e:
            logging.debug(f"Error getting indexes for {table_name}: {e}")

        return indexes

    def _evaluate_table_scan(
        self,
        table_name: str,
        predicates: List[Predicate],
        columns_needed: List[str],
        table_stats: Any,
        limit: Optional[int],
    ) -> AccessPath:
        """Evaluate a sequential table scan."""
        # Get base table scan cost
        scan_cost = self.cost_estimator.estimate_table_scan_cost(
            table_name, table_stats
        )

        # Apply predicates (all predicates can be applied during scan)
        combined_selectivity = 1.0
        for predicate in predicates:
            combined_selectivity *= predicate.selectivity

        estimated_rows = int(table_stats.row_count * combined_selectivity)

        # Apply LIMIT if present
        if limit and limit < estimated_rows:
            estimated_rows = limit
            # Adjust cost proportionally (early termination)
            scan_cost.total_cost *= limit / table_stats.row_count

        return AccessPath(
            method=AccessMethod.TABLE_SCAN,
            table_name=table_name,
            estimated_cost=scan_cost.total_cost,
            estimated_rows=estimated_rows,
            startup_cost=scan_cost.startup_cost,
            total_cost=scan_cost.total_cost,
            indexes_used=[],
            predicates_used=predicates,
            columns_needed=columns_needed,
            is_covering=True,  # Table scan always covers all columns
            metadata={
                "selectivity": combined_selectivity,
                "base_rows": table_stats.row_count,
            },
        )

    def _evaluate_index_scan(
        self,
        table_name: str,
        index: IndexInfo,
        predicates: List[Predicate],
        columns_needed: List[str],
        table_stats: Any,
        limit: Optional[int],
    ) -> AccessPath:
        """Evaluate an index scan access path."""
        # Calculate selectivity of predicates that can use this index
        index_selectivity = 1.0
        usable_predicates = []

        for predicate in predicates:
            if predicate.can_use_index(index):
                index_selectivity *= predicate.selectivity
                usable_predicates.append(predicate)

        if not usable_predicates:
            return None  # No usable predicates

        # Estimate cost
        scan_cost = self.cost_estimator.estimate_index_scan_cost(
            table_name, index.name, index_selectivity, table_stats
        )

        estimated_rows = int(table_stats.row_count * index_selectivity)

        # Apply remaining predicates that couldn't use the index
        remaining_predicates = [p for p in predicates if p not in usable_predicates]
        if remaining_predicates:
            remaining_selectivity = 1.0
            for predicate in remaining_predicates:
                remaining_selectivity *= predicate.selectivity

            estimated_rows = int(estimated_rows * remaining_selectivity)

            # Add cost for applying remaining predicates
            filter_cost = self.cost_estimator.estimate_filter_cost(
                estimated_rows,
                table_stats.avg_row_length,
                [p.column for p in remaining_predicates],
                remaining_selectivity,
            )
            scan_cost.total_cost += filter_cost.total_cost

        # Apply LIMIT
        if limit and limit < estimated_rows:
            estimated_rows = limit

        return AccessPath(
            method=AccessMethod.INDEX_SCAN,
            table_name=table_name,
            estimated_cost=scan_cost.total_cost,
            estimated_rows=estimated_rows,
            startup_cost=scan_cost.startup_cost,
            total_cost=scan_cost.total_cost,
            indexes_used=[index.name],
            predicates_used=usable_predicates,
            columns_needed=columns_needed,
            is_covering=False,  # Regular index scan needs heap access
            metadata={
                "index_selectivity": index_selectivity,
                "remaining_predicates": len(remaining_predicates),
                "clustering_factor": index.clustering_factor,
            },
        )

    def _can_use_index_only_scan(
        self, index: IndexInfo, columns_needed: List[str], predicates: List[Predicate]
    ) -> bool:
        """Check if an index-only scan is possible."""
        # All needed columns must be in the index
        index_columns = set(index.columns)
        needed_columns = set(columns_needed)

        # Add columns from predicates
        predicate_columns = {p.column for p in predicates}
        all_needed = needed_columns | predicate_columns

        return all_needed.issubset(index_columns)

    def _evaluate_index_only_scan(
        self,
        table_name: str,
        index: IndexInfo,
        predicates: List[Predicate],
        columns_needed: List[str],
        table_stats: Any,
        limit: Optional[int],
    ) -> AccessPath:
        """Evaluate an index-only scan access path."""
        # Similar to index scan but no heap access required
        index_selectivity = 1.0
        for predicate in predicates:
            if predicate.can_use_index(index):
                index_selectivity *= predicate.selectivity

        # Index-only scan cost (no heap lookups)
        estimated_rows = int(table_stats.row_count * index_selectivity)

        # Simplified cost model for index-only scan
        index_pages_to_read = max(1, int(index.pages * index_selectivity))
        io_cost = index_pages_to_read * self.cost_estimator.cost_factors.seq_page_cost
        cpu_cost = (
            estimated_rows * self.cost_estimator.cost_factors.cpu_index_tuple_cost
        )
        total_cost = io_cost + cpu_cost + 5.0  # Small startup cost

        if limit and limit < estimated_rows:
            estimated_rows = limit
            total_cost *= limit / max(1, estimated_rows)

        return AccessPath(
            method=AccessMethod.INDEX_ONLY_SCAN,
            table_name=table_name,
            estimated_cost=total_cost,
            estimated_rows=estimated_rows,
            startup_cost=5.0,
            total_cost=total_cost,
            indexes_used=[index.name],
            predicates_used=predicates,
            columns_needed=columns_needed,
            is_covering=True,  # Index-only scan covers all needed columns
            metadata={
                "index_selectivity": index_selectivity,
                "index_pages": index_pages_to_read,
            },
        )

    def _evaluate_bitmap_scans(
        self,
        table_name: str,
        indexes: List[IndexInfo],
        predicates: List[Predicate],
        columns_needed: List[str],
        table_stats: Any,
        limit: Optional[int],
    ) -> List[AccessPath]:
        """Evaluate bitmap scan access paths."""
        bitmap_paths = []

        # Find predicates that could benefit from bitmap scans
        # (typically medium selectivity predicates)
        bitmap_predicates = [
            p for p in predicates if self.bitmap_scan_threshold <= p.selectivity <= 0.5
        ]

        if not bitmap_predicates:
            return bitmap_paths

        # Try bitmap scans for indexes that match these predicates
        for index in indexes:
            applicable_predicates = [
                p for p in bitmap_predicates if p.can_use_index(index)
            ]

            if applicable_predicates:
                bitmap_path = self._create_bitmap_scan_path(
                    table_name,
                    index,
                    applicable_predicates,
                    predicates,
                    columns_needed,
                    table_stats,
                    limit,
                )
                if bitmap_path:
                    bitmap_paths.append(bitmap_path)

        return bitmap_paths

    def _create_bitmap_scan_path(
        self,
        table_name: str,
        index: IndexInfo,
        bitmap_predicates: List[Predicate],
        all_predicates: List[Predicate],
        columns_needed: List[str],
        table_stats: Any,
        limit: Optional[int],
    ) -> Optional[AccessPath]:
        """Create a bitmap scan access path."""
        # Calculate bitmap selectivity
        bitmap_selectivity = 1.0
        for predicate in bitmap_predicates:
            bitmap_selectivity *= predicate.selectivity

        estimated_rows = int(table_stats.row_count * bitmap_selectivity)

        # Bitmap scan cost model
        # 1. Create bitmap from index
        bitmap_creation_cost = (
            estimated_rows * self.cost_estimator.cost_factors.cpu_index_tuple_cost
        )

        # 2. Scan heap using bitmap (more efficient than random access)
        heap_pages = max(
            1,
            int(
                estimated_rows
                * table_stats.avg_row_length
                / self.cost_estimator.cost_factors.page_size
            ),
        )
        heap_scan_cost = (
            heap_pages * self.cost_estimator.cost_factors.seq_page_cost * 1.5
        )

        # 3. Apply remaining predicates
        remaining_predicates = [p for p in all_predicates if p not in bitmap_predicates]
        remaining_cost = 0.0
        if remaining_predicates:
            remaining_selectivity = 1.0
            for predicate in remaining_predicates:
                remaining_selectivity *= predicate.selectivity
            estimated_rows = int(estimated_rows * remaining_selectivity)
            remaining_cost = (
                estimated_rows * self.cost_estimator.cost_factors.cpu_tuple_cost
            )

        total_cost = bitmap_creation_cost + heap_scan_cost + remaining_cost + 10.0

        if limit and limit < estimated_rows:
            estimated_rows = limit

        return AccessPath(
            method=AccessMethod.BITMAP_HEAP_SCAN,
            table_name=table_name,
            estimated_cost=total_cost,
            estimated_rows=estimated_rows,
            startup_cost=10.0,
            total_cost=total_cost,
            indexes_used=[index.name],
            predicates_used=bitmap_predicates,
            columns_needed=columns_needed,
            is_covering=False,
            metadata={
                "bitmap_selectivity": bitmap_selectivity,
                "heap_pages": heap_pages,
                "remaining_predicates": len(remaining_predicates),
            },
        )

    def _evaluate_multi_index_scans(
        self,
        table_name: str,
        indexes: List[IndexInfo],
        predicates: List[Predicate],
        columns_needed: List[str],
        table_stats: Any,
        limit: Optional[int],
    ) -> List[AccessPath]:
        """Evaluate multi-index intersection/union strategies."""
        multi_paths = []

        # Look for opportunities to use multiple indexes
        usable_indexes = []
        for index in indexes:
            applicable_predicates = [p for p in predicates if p.can_use_index(index)]
            if applicable_predicates:
                usable_indexes.append((index, applicable_predicates))

        if len(usable_indexes) < 2:
            return multi_paths

        # Try intersection of the two most selective indexes
        usable_indexes.sort(key=lambda x: min(p.selectivity for p in x[1]))

        if len(usable_indexes) >= 2:
            index1, preds1 = usable_indexes[0]
            index2, preds2 = usable_indexes[1]

            intersection_path = self._create_index_intersection_path(
                table_name,
                [(index1, preds1), (index2, preds2)],
                predicates,
                columns_needed,
                table_stats,
                limit,
            )
            if intersection_path:
                multi_paths.append(intersection_path)

        return multi_paths

    def _create_index_intersection_path(
        self,
        table_name: str,
        index_predicate_pairs: List[Tuple[IndexInfo, List[Predicate]]],
        all_predicates: List[Predicate],
        columns_needed: List[str],
        table_stats: Any,
        limit: Optional[int],
    ) -> Optional[AccessPath]:
        """Create an index intersection access path."""
        # Calculate combined selectivity (intersection means AND)
        combined_selectivity = 1.0
        used_predicates = []
        used_indexes = []

        for index, predicates in index_predicate_pairs:
            index_selectivity = 1.0
            for predicate in predicates:
                index_selectivity *= predicate.selectivity

            # For intersection, we use the most selective
            combined_selectivity = min(combined_selectivity, index_selectivity)
            used_predicates.extend(predicates)
            used_indexes.append(index.name)

        estimated_rows = int(table_stats.row_count * combined_selectivity)

        # Cost model for index intersection
        intersection_cost = 0.0
        for index, predicates in index_predicate_pairs:
            # Cost to scan each index
            index_selectivity = 1.0
            for predicate in predicates:
                index_selectivity *= predicate.selectivity

            index_rows = int(table_stats.row_count * index_selectivity)
            intersection_cost += (
                index_rows * self.cost_estimator.cost_factors.cpu_index_tuple_cost
            )

        # Cost to intersect the bitmaps
        intersection_cost += (
            estimated_rows * self.cost_estimator.cost_factors.cpu_operator_cost * 2
        )

        # Cost to access heap
        heap_cost = estimated_rows * self.cost_estimator.cost_factors.random_page_cost

        total_cost = intersection_cost + heap_cost + 15.0  # Higher startup cost

        if limit and limit < estimated_rows:
            estimated_rows = limit

        return AccessPath(
            method=AccessMethod.MULTI_INDEX_SCAN,
            table_name=table_name,
            estimated_cost=total_cost,
            estimated_rows=estimated_rows,
            startup_cost=15.0,
            total_cost=total_cost,
            indexes_used=used_indexes,
            predicates_used=used_predicates,
            columns_needed=columns_needed,
            is_covering=False,
            metadata={
                "intersection_selectivity": combined_selectivity,
                "num_indexes": len(index_predicate_pairs),
            },
        )

    def _evaluate_parallel_scans(
        self,
        table_name: str,
        predicates: List[Predicate],
        columns_needed: List[str],
        table_stats: Any,
        limit: Optional[int],
    ) -> List[AccessPath]:
        """Evaluate parallel scan strategies."""
        parallel_paths = []

        # Only consider parallel scans for large tables
        if table_stats.row_count < self.min_parallel_table_size:
            return parallel_paths

        # Parallel table scan
        parallel_workers = min(4, max(2, table_stats.row_count // 50000))

        # Get base table scan cost
        base_scan = self._evaluate_table_scan(
            table_name, predicates, columns_needed, table_stats, limit
        )

        # Estimate parallel cost
        parallel_cost = self.cost_estimator.estimate_parallel_cost(
            base_scan, parallel_workers
        )

        parallel_path = AccessPath(
            method=AccessMethod.PARALLEL_SEQ_SCAN,
            table_name=table_name,
            estimated_cost=parallel_cost.total_cost,
            estimated_rows=base_scan.estimated_rows,
            startup_cost=parallel_cost.startup_cost,
            total_cost=parallel_cost.total_cost,
            indexes_used=[],
            predicates_used=predicates,
            columns_needed=columns_needed,
            is_covering=True,
            parallel_workers=parallel_workers,
            metadata={
                "base_cost": base_scan.total_cost,
                "parallel_speedup": base_scan.total_cost / parallel_cost.total_cost,
            },
        )

        parallel_paths.append(parallel_path)

        return parallel_paths

    def explain_access_path(self, path: AccessPath) -> str:
        """Generate a human-readable explanation of an access path."""
        lines = [f"Access Method: {path.method.value}"]
        lines.append(f"Table: {path.table_name}")
        lines.append(f"Estimated Cost: {path.total_cost:.2f}")
        lines.append(f"Estimated Rows: {path.estimated_rows:,}")

        if path.indexes_used:
            lines.append(f"Indexes Used: {', '.join(path.indexes_used)}")

        if path.predicates_used:
            pred_strs = [
                f"{p.column} {p.operator} {p.value}" for p in path.predicates_used
            ]
            lines.append(f"Predicates: {', '.join(pred_strs)}")

        if path.parallel_workers > 1:
            lines.append(f"Parallel Workers: {path.parallel_workers}")

        if path.is_covering:
            lines.append("Covering: Yes (all columns available)")
        else:
            lines.append("Covering: No (heap access required)")

        return "\n".join(lines)

    def get_selection_statistics(self) -> Dict[str, Any]:
        """Get access path selection statistics."""
        total_selections = (
            self.table_scans_chosen + self.index_scans_chosen + self.bitmap_scans_chosen
        )

        return {
            "paths_evaluated": self.paths_evaluated,
            "total_selections": total_selections,
            "table_scans_chosen": self.table_scans_chosen,
            "index_scans_chosen": self.index_scans_chosen,
            "bitmap_scans_chosen": self.bitmap_scans_chosen,
            "avg_paths_per_selection": self.paths_evaluated / max(1, total_selections),
            "index_scan_ratio": self.index_scans_chosen / max(1, total_selections),
            "table_scan_ratio": self.table_scans_chosen / max(1, total_selections),
        }
