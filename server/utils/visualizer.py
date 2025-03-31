import logging
import os

class Visualizer:
    """Handles visualization of database objects"""
    
    def __init__(self, catalog_manager, index_manager):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager
    
    def execute_visualize(self, plan):
        """
        Execute a VISUALIZE command.
        """
        # Get visualization object type
        object_type = plan.get("object")

        if object_type == "BPTREE":
            index_name = plan.get("index_name")
            table_name = plan.get("table")

            # Get current database
            db_name = self.catalog_manager.get_current_database()
            if not db_name:
                return {
                    "error": "No database selected. Use 'USE database_name' first.",
                    "status": "error",
                }

            # Get index manager to access B+ trees
            index_manager = self.index_manager

            # If both table and index are specified, visualize that specific index
            if table_name and index_name and index_name.upper() != "ON":
                # Try to get the index object
                full_index_name = f"{table_name}.{index_name}"
                index_obj = index_manager.get_index(full_index_name)

                if not index_obj:
                    # Get indexes from catalog to see if it exists
                    indexes = self.catalog_manager.get_indexes_for_table(
                        table_name)
                    if not indexes or index_name not in indexes:
                        return {
                            "error": f"Index '{index_name}' not found for table '{table_name}'",
                            "status": "error",
                        }
                    else:
                        # Index exists in catalog but file not found - rebuild it
                        try:
                            logging.info(
                                f"Attempting to rebuild index {
                                    index_name} on {table_name}..."
                            )
                            column = indexes[index_name].get("column")
                            is_unique = indexes[index_name].get(
                                "unique", False)
                            index_obj = index_manager.build_index(
                                table_name, index_name, column, is_unique, db_name
                            )
                        except Exception as e:
                            return {
                                "error": f"Error rebuilding index: {str(e)}",
                                "status": "error",
                            }

                # Now visualize the index
                if index_obj:
                    try:
                        # Try NetworkX visualization first
                        try:
                            from bptree_networkx import BPlusTreeNetworkXVisualizer

                            visualizer = BPlusTreeNetworkXVisualizer(
                                output_dir="visualizations"
                            )
                            viz_path = visualizer.visualize_tree(
                                index_obj, output_name=full_index_name
                            )

                            if viz_path:
                                # Convert to absolute path
                                abs_path = os.path.abspath(viz_path)
                                logging.info(
                                    f"Generated visualization at {abs_path}")

                                # Generate text representation
                                text_repr = self._get_tree_text(index_obj)

                                return {
                                    "message": f"B+ Tree visualization for '{index_name}' on '{table_name}'",
                                    "status": "success",
                                    "visualization_path": abs_path,
                                    "text_representation": text_repr,
                                }
                        except ImportError:
                            logging.warning(
                                "NetworkX not available, falling back to Graphviz"
                            )

                        # Fall back to Graphviz
                        viz_path = index_obj.visualize(
                            index_manager.visualizer, f"{
                                full_index_name}_visualization"
                        )

                        if viz_path:
                            # Convert to absolute path
                            abs_path = os.path.abspath(viz_path)
                            logging.info(
                                f"Generated visualization at {abs_path}")

                            # Generate text representation
                            text_repr = self._get_tree_text(index_obj)

                            return {
                                "message": f"B+ Tree visualization for '{index_name}' on '{table_name}'",
                                "status": "success",
                                "visualization_path": abs_path,
                                "text_representation": text_repr,
                            }
                        else:
                            return {
                                "message": f"Text representation for '{index_name}' on '{table_name}'",
                                "status": "success",
                                "text_representation": self._get_tree_text(index_obj),
                            }

                    except Exception as e:
                        logging.error(f"Error visualizing B+ tree: {str(e)}")
                        return {
                            "error": f"Error visualizing B+ tree: {str(e)}",
                            "status": "error",
                        }
                else:
                    return {
                        "error": f"Could not find or rebuild index '{index_name}'",
                        "status": "error",
                    }

    def _get_tree_text(self, tree):
        """Generate a text representation of the tree"""
        lines = [f"B+ Tree '{tree.name}' (Order: {tree.order})"]

        def print_node(node, level=0, prefix="Root: "):
            indent = "  " * level

            # Print the current node
            if hasattr(node, "leaf") and node.leaf:
                key_values = []
                for item in node.keys:
                    if isinstance(item, tuple) and len(item) >= 2:
                        key_values.append(f"{item[0]}:{item[1]}")
                    else:
                        key_values.append(str(item))
                lines.append(
                    f"{indent}{prefix}LEAF {{{', '.join(key_values)}}}")
            else:
                lines.append(
                    f"{indent}{
                        prefix}NODE {{{', '.join(map(str, node.keys))}}}"
                )

            # Print children recursively
            if hasattr(node, "children"):
                for i, child in enumerate(node.children):
                    child_prefix = f"Child {i}: "
                    print_node(child, level + 1, child_prefix)

        print_node(tree.root)
        return "\n".join(lines)

    def _count_nodes(self, node):
        """Count total nodes in the tree"""
        if node is None:
            return 0
        count = 1  # Count this node
        if hasattr(node, "children"):
            for child in node.children:
                count += self._count_nodes(child)
        return count

    def _count_leaves(self, node):
        """Count leaf nodes in the tree"""
        if node is None:
            return 0
        if hasattr(node, "leaf") and node.leaf:
            return 1
        count = 0
        if hasattr(node, "children"):
            for child in node.children:
                count += self._count_leaves(child)
        return count

    def _tree_height(self, node, level=0):
        """Calculate the height of the tree"""
        if node is None:
            return level
        if hasattr(node, "leaf") and node.leaf:
            return level + 1
        if hasattr(node, "children") and node.children:
            return self._tree_height(node.children[0], level + 1)
        return level + 1

    def execute_visualize_index(self, plan):
        """
        Execute a VISUALIZE INDEX command.
        """
        index_name = plan.get("index_name")
        table_name = plan.get("table")

        # Get the current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {
                "error": "No database selected. Use 'USE database_name' first.",
                "status": "error",
            }

        # Determine the index name format based on parameters
        if table_name and index_name:
            full_index_name = f"{table_name}.{index_name}"
        elif table_name:
            # Visualize all indexes for the table
            indexes = self.catalog_manager.get_indexes_for_table(table_name)
            if not indexes:
                return {
                    "error": f"No indexes found for table '{table_name}'",
                    "status": "error",
                }

            results = []
            for idx_name in indexes:
                full_index_name = f"{table_name}.{idx_name}"
                result = self.visualize_index(full_index_name)
                if result:
                    results.append(result)

            return {
                "message": f"Visualized {len(results)} indexes for table '{table_name}'",
                "visualizations": results,
                "status": "success",
            }
        else:
            # Visualize all indexes
            count = self.visualize_all_indexes()
            return {"message": f"Visualized {count} indexes", "status": "success"}

        # Visualize specific index
        result = self.visualize_index(full_index_name)

        if result:
            return {
                "message": f"Index '{full_index_name}' visualized successfully",
                "visualization": result,
                "status": "success",
            }
        else:
            return {
                "error": f"Failed to visualize index '{full_index_name}'",
                "status": "error",
            }

    def visualize_index(self, index_name):
        """
        Visualize a single index.
        """
        # Get the index from the index manager
        index = self.index_manager.get_index(index_name)
        if not index:
            return None

        # Generate a visualization
        visualization_path = f"visualizations/{index_name}_visualization.png"
        try:
            # Check if we have the BPlusTreeVisualizer
            from bptree import BPlusTreeVisualizer

            visualizer = BPlusTreeVisualizer()
            actual_path = index.visualize(visualizer, output_name=index_name)

            return {
                "index_name": index_name,
                "visualization_path": actual_path or visualization_path,
            }
        except ImportError:
            logging.warning(
                "BPlusTreeVisualizer not available. Install graphviz for visualizations."
            )
            return {
                "index_name": index_name,
                "error": "Visualization libraries not available",
            }

    def visualize_all_indexes(self):
        """
        Visualize all indexes in the current database.
        """
        try:
            # Check if we're running the visualization code
            count = self.index_manager.visualize_all_indexes()
            return count
        except Exception as e:
            logging.error(f"Error visualizing all indexes: {str(e)}")
            return 0
