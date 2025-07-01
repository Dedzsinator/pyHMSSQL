"""_summary_

Returns:
    _type_: _description_
"""

import pickle
import logging
import json
from datetime import datetime


# Create a dedicated B+ tree logger
def setup_bptree_logger():
    """
    Instead of creating a separate logger, get a child logger from the root
    """
    # Get a child logger from the root logger
    logger = logging.getLogger("bptree")
    # Don't set level or handlers - inherit from root
    return logger


bptree_logger = setup_bptree_logger()


class BPlusTreeNode:
    """_summary_"""

    def __init__(self, leaf=False):
        self.keys = []
        self.leaf = leaf
        self.children = []
        self.next = None  # For leaf nodes to point to next leaf


class BPlusTree:
    """_summary_"""

    def __init__(self, order=50, name=None):
        self.root = BPlusTreeNode(leaf=True)
        self.order = order  # Maximum number of keys per node
        self.name = name or f"tree_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        self.operation_counter = 0
        bptree_logger.info("Created new B+ tree '%s' with order %s", self.name, order)

    def insert(self, key, value):
        """_summary_

        Args:
            key (_type_): _description_
            value (_type_): _description_
        """
        self.operation_counter += 1
        bptree_logger.debug(
            "[%s][%s] INSERT - key: %s, value: %s",
            self.name,
            self.operation_counter,
            key,
            value,
        )

        # Log tree state before insert if debugging detail needed
        if bptree_logger.level <= logging.DEBUG:
            bptree_logger.debug(
                "Tree before insert: %s", self._get_tree_structure_json()
            )

        # Original insert logic
        root = self.root
        if len(root.keys) == (2 * self.order) - 1:
            bptree_logger.debug("[%s] Root node is full, creating new root")
            new_root = BPlusTreeNode(leaf=False)
            self.root = new_root
            new_root.children.append(root)
            self._split_child(new_root, 0)
            self._insert_non_full(new_root, key, value)
        else:
            self._insert_non_full(root, key, value)

        # Log tree state after insert if debugging detail needed
        if bptree_logger.level <= logging.DEBUG:
            bptree_logger.debug(
                "Tree after insert: %s", self._get_tree_structure_json()
            )

    def _split_child(self, parent, index):
        self.operation_counter += 1
        order = self.order
        y = parent.children[index]
        z = BPlusTreeNode(leaf=y.leaf)

        bptree_logger.debug(
            "[%s][%s] SPLIT - Splitting child at index: %s",
            self.name,
            self.operation_counter,
            index,
        )
        bptree_logger.debug("[%s] Parent keys: %s", self.name, parent.keys)
        bptree_logger.debug("[%s] Child keys before split: %s", self.name, y.keys)

        # Move the latter half of y's keys to z
        mid = order - 1

        if y.leaf:
            # For leaf nodes, we copy the middle key
            z.keys = y.keys[mid:]
            y.keys = y.keys[:mid]

            # Connect leaves for range queries
            z.next = y.next
            y.next = z

            bptree_logger.debug("[%s] Leaf node split - mid point: %s", self.name, mid)
            bptree_logger.debug(
                "[%s] Left node keys after split: %s", self.name, y.keys
            )
            bptree_logger.debug(
                "[%s] Right node keys after split: %s", self.name, z.keys
            )
        else:
            # For internal nodes, we move the middle key up
            middle_key = y.keys[mid]
            z.keys = y.keys[mid + 1 :]
            parent.keys.insert(index, middle_key)
            y.keys = y.keys[:mid]

            # Move the corresponding children
            z.children = y.children[mid + 1 :]
            y.children = y.children[: mid + 1]

            bptree_logger.debug(
                "[%s] Internal node split - mid key moving up: %s",
                self.name,
                middle_key,
            )
            bptree_logger.debug(
                "[%s] Left node keys after split: %s", self.name, y.keys
            )
            bptree_logger.debug(
                "[%s] Right node keys after split: %s", self.name, z.keys
            )

        # Insert z as a child of parent
        parent.children.insert(index + 1, z)
        bptree_logger.debug("[%s] Parent keys after split: %s", self.name, parent.keys)

    def _insert_non_full(self, node, key, value):
        """Insert a key-value pair into a non-full node"""
        if node.leaf:
            # Find the correct position to insert the key
            i = len(node.keys) - 1

            # Search backward for the right position
            while i >= 0 and key < node.keys[i][0]:
                i -= 1

            # Check if key already exists
            for idx, (k, _) in enumerate(node.keys):
                if k == key:
                    # Replace the value for existing key
                    old_value = node.keys[idx][1]
                    node.keys[idx] = (key, value)
                    bptree_logger.debug(
                        "[%s] UPDATE - key: %s, old value: %s, new value: %s",
                        self.name,
                        key,
                        old_value,
                        value,
                    )
                    return

            # Insert the new key-value pair
            node.keys.insert(i + 1, (key, value))
            bptree_logger.debug(
                "[%s] INSERT LEAF - key: %s, value: %s, position: %s",
                self.name,
                key,
                value,
                i + 1,
            )
        else:
            # Find the child which will have the new key
            i = len(node.keys) - 1

            # Search backward for the right position
            while i >= 0 and key < node.keys[i]:
                i -= 1

            # Move to the child that should contain the key
            i += 1

            # Check if i is within bounds
            if i >= len(node.children):
                i = len(node.children) - 1

            bptree_logger.debug(
                "[%s] INSERT INTERNAL - traversing to child at index %s", self.name, i
            )

            # Check if the child is full
            if len(node.children[i].keys) == (2 * self.order) - 1:
                bptree_logger.debug(
                    "[%s] Child node at index %s is full, needs splitting", self.name, i
                )
                # Split the child
                self._split_child(node, i)

                # After splitting, decide which child to go to
                if i < len(node.keys) and key > node.keys[i]:
                    i += 1
                    bptree_logger.debug(
                        "[%s] After split, moving to child at index %s", self.name, i
                    )

                # Ensure i is within bounds after potentially incrementing
                if i >= len(node.children):
                    i = len(node.children) - 1

            # Recursively insert the key-value pair
            self._insert_non_full(node.children[i], key, value)

    def search(self, key):
        """_summary_

        Args:
            key (_type_): _description_

        Returns:
            _type_: _description_
        """
        self.operation_counter += 1
        bptree_logger.debug(
            "[%s][%s] SEARCH - key: %s", self.name, self.operation_counter, key
        )
        return self._search(self.root, key)

    def _search(self, node, key):
        """Search for a key in a node recursively.

        Args:
            node: The node to search in
            key: The key to search for

        Returns:
            The associated value or None if key is not found
        """
        # Find the position where the key should be
        i = 0
        if node.leaf:
            # For leaf nodes, keys are (key, value) tuples
            for i, (k, v) in enumerate(node.keys):
                if k == key:
                    bptree_logger.debug(
                        "[%s] FOUND - key: %s, value: %s", self.name, key, v
                    )
                    return v
            bptree_logger.debug("[%s] NOT FOUND - key: %s", self.name, key)
            return None  # Key not found
        else:
            # For internal nodes, determine which child to go to
            while i < len(node.keys) and key > node.keys[i]:
                i += 1

            # If key is less than key at position i, go to child i
            # If key is greater than all keys in node, go to rightmost child
            if i == len(node.keys) or key < node.keys[i]:
                return self._search(node.children[i], key)
            else:
                # If key equals key at position i, go to child i+1
                return self._search(node.children[i + 1], key)

    def range_query(self, start_key, end_key):
        """Get all values with keys between start_key and end_key."""
        self.operation_counter += 1
        bptree_logger.debug(
            "[%s][%s] RANGE QUERY - from %s to %s",
            self.name,
            self.operation_counter,
            start_key,
            end_key,
        )
        result = []
        self._range_query(self.root, start_key, end_key, result)
        bptree_logger.debug(
            "[%s] RANGE QUERY RESULT - found %s entries", self.name, len(result)
        )
        return result

    def visualize(self, visualizer=None, output_name=None):
        """_summary_

        Args:
            visualizer (_type_, optional): _description_. Defaults to None.
            output_name (_type_, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        if visualizer is None:
            # Update to use the new visualizer
            from bptree_visualizer import BPTreeVisualizer

            visualizer = BPTreeVisualizer()
        return visualizer.visualize_tree(self, output_name)

    def _get_tree_structure_json(self):
        """Generate a JSON representation of the tree structure for debugging"""

        def node_to_dict(node, node_id=0):
            node_data = {
                "id": node_id,
                "leaf": node.leaf,
                "keys": (
                    [k[0] if node.leaf else k for k in node.keys]
                    if node.leaf
                    else node.keys
                ),
                "children": [],
            }

            for i, child in enumerate(node.children):
                child_id = node_id * 10 + i + 1
                node_data["children"].append(node_to_dict(child, child_id))

            return node_data

        return json.dumps(node_to_dict(self.root))

    def _range_query(self, node, start_key, end_key, result):
        """Find all key-value pairs with key between start_key and end_key.

        Args:
            node: The node to search in
            start_key: The lower bound (inclusive)
            end_key: The upper bound (inclusive)
            result: List to store results
        """
        if node.leaf:
            # Find all keys in the range
            for k, v in node.keys:
                if start_key <= k <= end_key:
                    result.append((k, v))

            # If we haven't reached end_key yet, check the next leaf
            if node.next and node.keys and node.keys[-1][0] < end_key:
                self._range_query(node.next, start_key, end_key, result)
        else:
            # Find the first child that might contain keys in range
            i = 0
            while i < len(node.keys) and start_key > node.keys[i]:
                i += 1

            # Recurse to this child
            if i < len(node.children):
                self._range_query(node.children[i], start_key, end_key, result)

            # Continue to next children if needed
            i = 0
            while i < len(node.keys) and node.keys[i] <= end_key:
                if i + 1 < len(node.children):
                    self._range_query(node.children[i + 1], start_key, end_key, result)
                i += 1

    def __getitem__(self, key):
        return self.search(key)

    def __setitem__(self, key, value):
        self.insert(key, value)

    def get(self, key, default=None):
        """_summary_

        Args:
            key (_type_): _description_
            default (_type_, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        result = self.search(key)
        return result if result is not None else default

    def save_to_file(self, filename):
        """_summary_

        Args:
            filename (_type_): _description_
        """
        logging.debug("Saving B+ tree to file: %s", filename)
        with open(filename, "wb") as f:
            pickle.dump(self, f)

    @classmethod
    def load_from_file(cls, file_path):
        """
        Load a B+ tree from a file.

        Args:
            file_path: Path to the file containing the serialized B+ tree

        Returns:
            The loaded B+ tree or None if unsuccessful
        """
        try:
            with open(file_path, "rb") as file:
                tree_data = pickle.load(file)

                # Make sure we have a proper tree structure
                if not isinstance(tree_data, cls):
                    logging.error("File %s does not contain a valid B+ tree", file_path)
                    return None

                # Verify the tree structure is intact
                if not hasattr(tree_data, "root") or not tree_data.root:
                    logging.error("Loaded B+ tree from %s has no root node", file_path)
                    return None

                return tree_data
        except RuntimeError as e:
            logging.error("Error loading B+ tree from %s: %s", file_path, str(e))
            return None

    def delete(self, key):
        """Delete a key from the B+ tree.

        Args:
            key: The key to delete

        Returns:
            True if key was deleted, False if key was not found
        """
        self.operation_counter += 1
        bptree_logger.debug(
            "[%s][%s] DELETE - key: %s", self.name, self.operation_counter, key
        )

        return self._delete(self.root, key)

    def _delete(self, node, key):
        """Delete a key from a node recursively.

        Args:
            node: The node to delete from
            key: The key to delete

        Returns:
            True if key was deleted, False if key was not found
        """
        if node.leaf:
            # For leaf nodes, find and remove the key
            for i, (k, v) in enumerate(node.keys):
                if k == key:
                    del node.keys[i]
                    bptree_logger.debug(
                        "[%s] DELETED - key: %s, value: %s", self.name, key, v
                    )
                    return True
            bptree_logger.debug("[%s] DELETE NOT FOUND - key: %s", self.name, key)
            return False
        else:
            # For internal nodes, find the appropriate child
            i = 0
            while i < len(node.keys) and key > node.keys[i]:
                i += 1

            # Recursively delete from the appropriate child
            if i < len(node.children):
                return self._delete(node.children[i], key)

            return False
