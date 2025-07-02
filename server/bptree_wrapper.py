"""
Adapter module that EXCLUSIVELY uses the optimized B+ tree implementation.
This ensures optimal performance across all database operations.
"""

import logging
import os
from typing import Optional

# Import ONLY the optimized implementation
try:
    from bptree import BPlusTreeOptimized

    OPTIMIZED_AVAILABLE = True
except ImportError:
    from bptree_adapter import BPlusTree as BPlusTreeOptimized

    OPTIMIZED_AVAILABLE = False

# Setup logging
logger = logging.getLogger("bptree_adapter")


class BPlusTreeFactory:
    """Factory for creating B+ trees using ONLY the optimized implementation"""

    @staticmethod
    def create(
        order: int = 50, name: Optional[str] = None, use_optimized: bool = True
    ) -> BPlusTreeOptimized:
        """
        Create a new optimized B+ tree

        Args:
            order: The order of the B+ tree
            name: Optional name for the tree
            use_optimized: Ignored - always uses optimized implementation

        Returns:
            An optimized B+ tree instance
        """
        logger.info("Creating optimized B+ tree with order %s", order)
        return BPlusTreeOptimized(order=order, name=name)

    @staticmethod
    def load_from_file(
        file_path: str, prefer_optimized: bool = True
    ) -> BPlusTreeOptimized:
        """
        Load an optimized B+ tree from a file

        Args:
            file_path: Path to the serialized B+ tree file
            prefer_optimized: Ignored - always uses optimized implementation

        Returns:
            A loaded optimized B+ tree instance or None if unsuccessful
        """
        if not os.path.exists(file_path):
            logger.error("File not found: %s", file_path)
            return None

        try:
            tree = BPlusTreeOptimized.load_from_file(file_path)
            if tree is not None:
                logger.info("Successfully loaded optimized B+ tree from %s", file_path)
                return tree
        except Exception as e:
            logger.error(
                "Failed to load optimized B+ tree from %s: %s", file_path, str(e)
            )
            return None

        logger.error("Could not load B+ tree from %s", file_path)
        return None
