import os
import logging
import networkx as nx
import matplotlib.pyplot as plt

try:
    from bptree import BPlusTreeOptimized as BPlusTree

    OPTIMIZED_AVAILABLE = True
except ImportError:
    from bptree_adapter import BPlusTree

    OPTIMIZED_AVAILABLE = False
from bptree_visualizer import BPTreeVisualizer


class Visualizer:
    """Visualization utilities for database structures."""

    def __init__(self, catalog_manager, index_manager):
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager
        self.output_dir = "data/indexes/visualizations"
        os.makedirs(self.output_dir, exist_ok=True)
        self.visualizer = BPTreeVisualizer(self.output_dir)

    def execute_visualization(self, plan):
        """Execute a VISUALIZE command."""
        object_type = plan.get("object")

        if object_type == "BPTREE":
            return self.execute_visualize_bptree(plan)
        elif object_type == "INDEX":
            return self.execute_visualize_index(plan)
        else:
            return {
                "error": f"Unsupported visualization object: {object_type}",
                "status": "error",
            }

    def execute_visualize_bptree(self, plan):
        """Execute a VISUALIZE BPTREE command."""
        index_name = plan.get("index_name")
        table_name = plan.get("table")

        # Get current database
        db_name = self.catalog_manager.get_current_database()
        if not db_name:
            return {
                "error": "No database selected. Use 'USE database_name' first.",
                "status": "error",
            }

        # Validate that table and index are specified
        if not table_name or not index_name:
            return {
                "error": "Both table name and index name must be specified",
                "status": "error",
            }

        try:
            # Check if the index exists in the catalog
            indexes = self.catalog_manager.get_indexes_for_table(table_name)
            logging.info(
                f"Available indexes for table {table_name}: {list(indexes.keys())}"
            )

            if index_name not in indexes:
                return {
                    "error": f"Index '{index_name}' not found for table '{table_name}'. Available indexes: {list(indexes.keys())}",
                    "status": "error",
                }

            # Get the index definition
            index_def = indexes[index_name]
            column_name = index_def.get('column', 'unknown')

            # Try multiple file naming conventions
            possible_files = [
                f"data/indexes/{db_name}_{table_name}_{index_name}.idx",
                f"data/indexes/{db_name}_{table_name}_{column_name}.idx",
                f"data/indexes/{table_name}_{index_name}.idx",
                f"data/indexes/{index_name}.idx",
            ]

            index_file_path = None
            for file_path in possible_files:
                if os.path.exists(file_path):
                    index_file_path = file_path
                    logging.info(f"Found index file: {index_file_path}")
                    break

            if not index_file_path:
                # Try to rebuild the index using the DDL index manager
                full_index_name = f"{table_name}.{index_name}"
                logging.info(
                    f"Index file not found, attempting to rebuild: {full_index_name}"
                )

                # Use the index manager to rebuild
                if hasattr(self.index_manager, "rebuild_index"):
                    success = self.index_manager.rebuild_index(full_index_name)
                    if success:
                        # Try to find the file again after rebuild
                        for file_path in possible_files:
                            if os.path.exists(file_path):
                                index_file_path = file_path
                                break

                if not index_file_path:
                    return {
                        "error": f"Index file not found and could not be rebuilt. Tried: {possible_files}",
                        "status": "error",
                    }

            # Try to load the B+ tree from file
            try:
                from bptree import BPlusTreeOptimized

                # Try to load as optimized B+ tree
                try:
                    index_obj = BPlusTreeOptimized.load_from_file(index_file_path)
                    logging.info(f"Loaded optimized B+ tree from {index_file_path}")
                except Exception as e:
                    # If loading fails, try to get from index manager
                    logging.warning(f"Could not load index from {index_file_path}: {e}")
                    full_index_name = f"{table_name}.{index_name}"
                    index_obj = self.index_manager.get_index(full_index_name)
                    if not index_obj:
                        # Last resort: create a new tree
                        index_obj = BPlusTreeOptimized(order=50, name=f"{table_name}_{column_name}_index")

            except Exception as load_error:
                logging.error(
                    f"Failed to load index file {index_file_path}: {str(load_error)}"
                )
                return {
                    "error": f"Failed to load index file: {str(load_error)}",
                    "status": "error",
                }

            # Generate visualization
            output_name = f"{table_name}_{index_name}_bptree"

            # Check if it's an optimized B+ tree
            if (
                hasattr(index_obj, "__class__")
                and "Optimized" in index_obj.__class__.__name__
            ):
                visualization_path = self._visualize_optimized_bptree(
                    index_obj, output_name
                )
            else:
                visualization_path = self.visualizer.visualize_tree(
                    index_obj, output_name
                )

            if visualization_path:
                # Generate HTML content for web display
                html_content = self._generate_html_visualization(
                    visualization_path, index_obj
                )

                return {
                    "message": f"B+ tree visualization generated successfully",
                    "visualization": html_content,
                    "visualization_path": visualization_path,
                    "status": "success",
                }
            else:
                return {"error": "Failed to generate visualization", "status": "error"}

        except Exception as e:
            logging.error(f"Error visualizing B+ tree: {str(e)}")
            return {"error": f"Error visualizing B+ tree: {str(e)}", "status": "error"}

    def _visualize_optimized_bptree(self, tree, output_name):
        """Visualize an optimized B+ tree."""
        try:
            logging.info(f"Visualizing optimized B+ tree: {tree.name}")
            logging.info(
                f"Tree has {len(getattr(tree, 'value_store', {}))} items in value store"
            )

            # Try direct text visualization first to see if there's data
            text_path = self._create_text_visualization(tree, output_name)
            if text_path:
                # If text visualization worked, try the wrapper approach
                tree_wrapper = OptimizedTreeWrapper(tree)
                result = self.visualizer.visualize_tree(tree_wrapper, output_name)
                if result:
                    return result

            # If all else fails, return the text visualization
            return text_path

        except Exception as e:
            logging.error(f"Error visualizing optimized B+ tree: {str(e)}")
            # Fall back to text representation
            return self._create_text_visualization(tree, output_name)

    def _create_text_visualization(self, tree, output_name):
        """Create a text-based visualization for the optimized B+ tree."""
        output_path = os.path.join(self.output_dir, f"{output_name}.txt")

        try:
            # Get tree information
            tree_name = getattr(tree, "name", "Unknown")
            tree_order = getattr(tree, "order", "Unknown")

            # Try multiple ways to get items
            items = []

            if hasattr(tree, "_get_all_items"):
                try:
                    items = tree._get_all_items()
                    logging.info(f"Got {len(items)} items from _get_all_items")
                except Exception as e:
                    logging.error(f"Error calling _get_all_items: {e}")

            if not items and hasattr(tree, "value_store"):
                try:
                    value_store = getattr(tree, "value_store", {})
                    logging.info(f"Value store has {len(value_store)} entries")
                    for key, value_holder in value_store.items():
                        if hasattr(value_holder, "value"):
                            items.append((key, value_holder.value))
                        else:
                            items.append((key, value_holder))
                except Exception as e:
                    logging.error(f"Error extracting from value_store: {e}")

            with open(output_path, "w") as f:
                f.write(f"Optimized B+ Tree Visualization: {tree_name}\n")
                f.write(f"Order: {tree_order}\n")
                f.write(f"Total items: {len(items)}\n\n")

                if items:
                    f.write("Tree contents:\n")
                    for i, (key, value) in enumerate(
                        items[:50]
                    ):  # Limit to first 50 items
                        f.write(f"  {key}: {value}\n")
                    if len(items) > 50:
                        f.write(f"  ... and {len(items) - 50} more items\n")
                else:
                    f.write("No data found in the tree.\n")

                    # Debug information
                    f.write("\nDebug Information:\n")
                    f.write(f"Tree type: {type(tree).__name__}\n")
                    f.write(f"Tree attributes: {dir(tree)}\n")
                    if hasattr(tree, "root"):
                        f.write(f"Root is null: {tree.root is None}\n")

            logging.info(f"Text visualization saved to: {output_path}")
            return output_path

        except Exception as e:
            logging.error(f"Error creating text visualization: {str(e)}")
            return None

    def _generate_html_visualization(self, visualization_path, tree):
        """Generate HTML content for displaying the visualization."""
        if not visualization_path or not os.path.exists(visualization_path):
            return self._generate_fallback_html(tree)

        # Check if it's an image file
        if visualization_path.endswith((".png", ".jpg", ".jpeg", ".svg")):
            # Read the image file and encode it as base64
            import base64

            try:
                with open(visualization_path, "rb") as f:
                    image_data = base64.b64encode(f.read()).decode("utf-8")

                file_ext = visualization_path.split(".")[-1]
                mime_type = f"image/{file_ext}"

                return f"""
                <html>
                <head>
                    <title>B+ Tree Visualization: {tree.name}</title>
                    <style>
                        body {{ font-family: Arial, sans-serif; margin: 20px; }}
                        .header {{ text-align: center; margin-bottom: 20px; }}
                        .image-container {{ text-align: center; }}
                        img {{ max-width: 100%; height: auto; border: 1px solid #ccc; }}
                    </style>
                </head>
                <body>
                    <div class="header">
                        <h1>B+ Tree Visualization</h1>
                        <h2>{tree.name}</h2>
                        <p>Order: {tree.order} | Type: {type(tree).__name__}</p>
                    </div>
                    <div class="image-container">
                        <img src="data:{mime_type};base64,{image_data}" alt="B+ Tree Visualization" />
                    </div>
                </body>
                </html>
                """
            except Exception as e:
                logging.error(f"Error reading image file: {str(e)}")
                return self._generate_fallback_html(tree)

        # Check if it's a text file
        elif visualization_path.endswith(".txt"):
            try:
                with open(visualization_path, "r") as f:
                    content = f.read()

                return f"""
                <html>
                <head>
                    <title>B+ Tree Visualization: {tree.name}</title>
                    <style>
                        body {{ font-family: monospace; margin: 20px; }}
                        .header {{ text-align: center; margin-bottom: 20px; }}
                        .content {{ white-space: pre-wrap; background-color: #f5f5f5; padding: 15px; border: 1px solid #ccc; }}
                    </style>
                </head>
                <body>
                    <div class="header">
                        <h1>B+ Tree Text Visualization</h1>
                        <h2>{tree.name}</h2>
                        <p>Order: {tree.order} | Type: {type(tree).__name__}</p>
                    </div>
                    <div class="content">{content}</div>
                </body>
                </html>
                """
            except Exception as e:
                logging.error(f"Error reading text file: {str(e)}")
                return self._generate_fallback_html(tree)

        return self._generate_fallback_html(tree)

    def _generate_fallback_html(self, tree):
        """Generate a fallback HTML representation when visualization fails."""
        try:
            # Get basic tree information safely
            tree_info = {
                "name": getattr(tree, "name", "Unknown"),
                "order": getattr(tree, "order", "Unknown"),
                "type": type(tree).__name__,
            }

            # Try to get some sample data
            sample_data = ""
            try:
                if hasattr(tree, "_get_all_items"):
                    items = tree._get_all_items()
                    if items:
                        sample_data = f"Sample data ({len(items)} items):\n"
                        # Show first 10 items
                        for i, (key, value) in enumerate(items[:10]):
                            sample_data += f"  {key}: {value}\n"
                        if len(items) > 10:
                            sample_data += f"  ... and {len(items) - 10} more items\n"
                    else:
                        sample_data = "No data found in the tree."
                else:
                    sample_data = "Unable to retrieve tree data."
            except Exception:
                sample_data = "Error retrieving tree data."

            return f"""
            <html>
            <head>
                <title>B+ Tree Information: {tree_info['name']}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    .header {{ text-align: center; margin-bottom: 20px; }}
                    .info {{ background-color: #f0f0f0; padding: 15px; border-radius: 5px; }}
                    .sample {{ background-color: #f5f5f5; padding: 15px; border-radius: 5px; margin-top: 15px; font-family: monospace; white-space: pre-wrap; }}
                    .error {{ color: #d32f2f; }}
                </style>
            </head>
            <body>
                <div class="header">
                    <h1>B+ Tree Information</h1>
                    <h2>{tree_info['name']}</h2>
                </div>
                <div class="info">
                    <strong>Tree Type:</strong> {tree_info['type']}<br>
                    <strong>Order:</strong> {tree_info['order']}<br>
                    <p class="error">Note: Graphical visualization is not available. This is a text-based representation.</p>
                </div>
                <div class="sample">
{sample_data}
                </div>
            </body>
            </html>
            """
        except Exception as e:
            logging.error(f"Error generating fallback HTML: {str(e)}")
            return f"""
            <html>
            <head><title>Visualization Error</title></head>
            <body>
                <h1>Visualization Error</h1>
                <p>Unable to generate visualization: {str(e)}</p>
            </body>
            </html>
            """

    def execute_visualize_index(self, plan):
        """Execute a VISUALIZE INDEX command."""
        # Redirect to the B+ tree visualization
        return self.execute_visualize_bptree(plan)


class OptimizedTreeWrapper:
    """Wrapper to make optimized B+ tree compatible with the standard visualizer."""

    def __init__(self, optimized_tree):
        self.optimized_tree = optimized_tree
        self.name = getattr(optimized_tree, "name", "Unknown")
        self.order = getattr(optimized_tree, "order", 50)
        self.root = self._create_compatible_root()

    def _create_compatible_root(self):
        """Create a compatible root node structure."""
        try:
            # Try to get all items from the optimized tree
            items = []
            if hasattr(self.optimized_tree, "_get_all_items"):
                items = self.optimized_tree._get_all_items()
            elif (
                hasattr(self.optimized_tree, "value_store")
                and self.optimized_tree.value_store
            ):
                # Extract from value store if available
                for value_ptr, value_holder in self.optimized_tree.value_store.items():
                    items.append(
                        (
                            value_ptr,
                            (
                                value_holder.value
                                if hasattr(value_holder, "value")
                                else value_holder
                            ),
                        )
                    )

            logging.info(f"OptimizedTreeWrapper found {len(items)} items in tree")

            # Create a simple leaf node representation
            root = CompatibleNode()
            root.leaf = True

            if items:
                # Show first 20 items for visualization
                root.keys = []
                for i, item in enumerate(items[:20]):
                    if isinstance(item, (list, tuple)) and len(item) >= 2:
                        # Format as key:value for display
                        key_val = f"{item[0]}:{item[1]}"
                    else:
                        key_val = str(item)
                    root.keys.append(key_val)
            else:
                root.keys = ["(empty tree)"]

            root.children = []
            return root

        except Exception as e:
            logging.error(f"Error creating compatible root: {str(e)}")
            # Return a root with error message
            root = CompatibleNode()
            root.leaf = True
            root.keys = [f"Error: {str(e)}"]
            root.children = []
            return root


class CompatibleNode:
    """Compatible node structure for visualization."""

    def __init__(self):
        self.leaf = True
        self.keys = []
        self.children = []
        self.next = None
