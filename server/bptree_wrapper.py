"""
Adapter module to provide API compatibility between original BPlusTree
and optimized BPlusTreeOptimized implementations.
"""
import logging
import os
from typing import Optional, Union

# Import both implementations
from bptree import BPlusTree as OriginalBPlusTree
from bptree_optimized import BPlusTreeOptimized

# Setup logging
logger = logging.getLogger("bptree_adapter")

class BPlusTreeFactory:
    """Factory for creating B+ trees with automatic selection of implementation"""

    @staticmethod
    def create(
        order: int = 50,
        name: Optional[str] = None,
        use_optimized: bool = True
    ) -> Union[BPlusTreeOptimized, OriginalBPlusTree]:
        """
        Create a new B+ tree with the specified implementation

        Args:
            order: The order of the B+ tree
            name: Optional name for the tree
            use_optimized: Whether to use the optimized implementation

        Returns:
            A B+ tree instance
        """
        if use_optimized:
            logger.info("Creating optimized B+ tree with order %s", order)
            return BPlusTreeOptimized(order=order, name=name)
        else:
            logger.info("Creating original B+ tree with order %s", order)
            return OriginalBPlusTree(order=order, name=name)

    @staticmethod
    def load_from_file(
        file_path: str,
        prefer_optimized: bool = True
    ) -> Union[BPlusTreeOptimized, OriginalBPlusTree]:
        """
        Load a B+ tree from a file, attempting to use the specified implementation

        Args:
            file_path: Path to the serialized B+ tree file
            prefer_optimized: Whether to prefer the optimized implementation

        Returns:
            A loaded B+ tree instance or None if unsuccessful
        """
        if not os.path.exists(file_path):
            logger.error("File not found: %s", file_path)
            return None

        # Try the preferred implementation first
        if prefer_optimized:
            try:
                tree = BPlusTreeOptimized.load_from_file(file_path)
                if tree is not None:
                    return tree
            except RuntimeError as e:
                logger.warning(
                    "Failed to load with optimized implementation: %s. "
                    "Falling back to original implementation.",
                    str(e)
                )

            # Fall back to original implementation
            return OriginalBPlusTree.load_from_file(file_path)
        else:
            # Try original implementation first
            tree = OriginalBPlusTree.load_from_file(file_path)
            if tree is not None:
                return tree

            # Fall back to optimized implementation
            try:
                return BPlusTreeOptimized.load_from_file(file_path)
            except RuntimeError as e:
                logger.error("Failed to load B+ tree: %s", str(e))
                return None