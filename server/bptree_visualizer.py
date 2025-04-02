import os
import logging
import json
from datetime import datetime
import networkx as nx
import matplotlib.pyplot as plt

# Try to import visualization libraries with fallbacks
try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches

    HAS_MATPLOTLIB = True

    try:
        import networkx as nx

        HAS_NETWORKX = True
    except ImportError:
        HAS_NETWORKX = False
        logging.info(
            "NetworkX not installed. Using direct matplotlib visualization.")
except ImportError:
    HAS_MATPLOTLIB = False
    HAS_NETWORKX = False
    logging.warning(
        "Matplotlib not installed. Visualization capabilities limited.")

try:
    import graphviz

    HAS_GRAPHVIZ = True
except ImportError:
    HAS_GRAPHVIZ = False
    logging.info(
        "Graphviz not installed. Some visualization features unavailable.")


class BPTreeVisualizer:
    """Unified B+ Tree visualizer with multiple visualization methods and fallbacks"""

    def __init__(self, output_dir="data/indexes/visualizations"):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        self.logger = logging.getLogger("bptree_viz")

    def visualize_tree(self, tree, output_name=None):
        """
        Visualize a B+ tree using the best available method

        Args:
            tree: The BPlusTree instance to visualize
            output_name: Optional name for the visualization

        Returns:
            Path to the generated visualization file
        """
        if not tree or not hasattr(tree, "root"):
            return self._export_text_only(tree, "Invalid tree structure", output_name)

        output_name = (
            output_name or f"{tree.name}_{
                datetime.now().strftime('%Y%m%d%H%M%S')}"
        )

        # Try visualizers in order of preference
        if HAS_NETWORKX:
            try:
                return self._visualize_networkx(tree, output_name)
            except Exception as e:
                self.logger.warning(f"NetworkX visualization failed: {str(e)}")

        if HAS_MATPLOTLIB:
            try:
                return self._visualize_matplotlib(tree, output_name)
            except Exception as e:
                self.logger.warning(
                    f"Matplotlib visualization failed: {str(e)}")

        if HAS_GRAPHVIZ:
            try:
                return self._visualize_graphviz(tree, output_name)
            except Exception as e:
                self.logger.warning(f"Graphviz visualization failed: {str(e)}")

        # Fall back to text representation if all visualization methods fail
        return self._export_tree_text(tree, output_name)

    def _visualize_networkx(self, tree, output_name):
        """Visualize using NetworkX and matplotlib"""
        G = nx.DiGraph()

        # Add nodes and edges to the graph
        node_labels = {}
        node_colors = []

        def add_to_graph(node, parent_id=None, node_counter=[0]):
            # Create a unique ID for this node
            node_id = f"node_{node_counter[0]}"
            node_counter[0] += 1

            # Add the node to the graph
            G.add_node(node_id)

            # Set node label and color
            if node.leaf:
                # For leaf nodes, include both keys and values
                label_parts = []
                for item in node.keys:
                    if isinstance(item, tuple) and len(item) >= 2:
                        label_parts.append(f"{item[0]}:{item[1]}")
                    else:
                        label_parts.append(str(item))
                node_labels[node_id] = "\n".join(label_parts)
                node_colors.append("lightgreen")
            else:
                # For internal nodes, include only keys
                node_labels[node_id] = "\n".join(map(str, node.keys))
                node_colors.append("lightblue")

            # Add edge from parent if it exists
            if parent_id:
                G.add_edge(parent_id, node_id)

            # Process children recursively
            if hasattr(node, "children") and node.children:
                for child in node.children:
                    child_id = add_to_graph(child, node_id, node_counter)

            # Add 'next' pointer for leaf nodes
            if node.leaf and hasattr(node, "next") and node.next:
                next_id = f"node_{node_counter[0]}"
                next_node = add_to_graph(node.next, None, node_counter)
                G.add_edge(node_id, next_id, style="dashed", color="red")

            return node_id

        # Build the graph
        add_to_graph(tree.root)

        # Create the plot
        plt.figure(figsize=(12, 8))
        try:
            pos = nx.nx_agraph.graphviz_layout(G, prog="dot")
        except:
            pos = nx.spring_layout(G, k=0.5, iterations=50)

        # Draw nodes with different colors
        nx.draw(
            G,
            pos,
            node_color=node_colors,
            node_size=2500,
            font_size=10,
            font_weight="bold",
            arrowsize=20,
            with_labels=False,
            alpha=0.8,
        )

        # Draw node labels
        nx.draw_networkx_labels(G, pos, labels=node_labels, font_size=8)

        # Add title
        plt.title(f"B+ Tree: {tree.name} (Order: {tree.order})")
        plt.axis("off")

        # Save the visualization
        output_path = os.path.join(self.output_dir, f"{output_name}.png")
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()

        self.logger.info(f"NetworkX visualization saved to: {output_path}")
        return output_path

    def _visualize_matplotlib(self, tree, output_name):
        """Direct visualization using only matplotlib"""
        fig, ax = plt.subplots(figsize=(12, 8))
        ax.set_xlim(0, 100)
        ax.set_ylim(0, 100)
        ax.axis("off")

        # Add title
        plt.title(f"B+ Tree: {tree.name} (Order: {tree.order})")

        # Calculate tree height
        tree_info = self._analyze_tree(tree.root)
        height = tree_info["height"]

        # Draw the tree recursively
        self._draw_node(
            ax, tree.root, level=0, x_pos=50, y_pos=90, width=80, level_height=15
        )

        # Save the visualization
        output_path = os.path.join(self.output_dir, f"{output_name}.png")
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()

        self.logger.info(f"Matplotlib visualization saved to: {output_path}")
        return output_path

    def _draw_node(self, ax, node, level, x_pos, y_pos, width, level_height):
        """Draw a node using matplotlib patches"""
        is_leaf = hasattr(node, "leaf") and node.leaf
        color = "lightgreen" if is_leaf else "lightblue"

        # Create node text
        if is_leaf:
            # Format key-value pairs for leaf nodes
            node_text = ", ".join(
                [f"{k}:{v}" if isinstance(k, tuple) else str(k)
                 for k in node.keys]
            )
        else:
            # Just keys for internal nodes
            node_text = ", ".join(map(str, node.keys))

        # Adjust box width based on text length
        box_width = min(40, max(10, len(node_text) * 0.7))

        # Draw the node
        rect = patches.Rectangle(
            (x_pos - box_width / 2, y_pos - 3),
            box_width,
            6,
            linewidth=1,
            edgecolor="black",
            facecolor=color,
            alpha=0.8,
        )
        ax.add_patch(rect)
        ax.text(
            x_pos,
            y_pos,
            node_text,
            ha="center",
            va="center",
            fontsize=8,
            fontweight="bold",
        )

        # Draw children
        if not is_leaf and hasattr(node, "children") and node.children:
            children = node.children
            child_width = width / len(children)

            for i, child in enumerate(children):
                child_x = x_pos - width / 2 + (i + 0.5) * child_width
                child_y = y_pos - level_height

                # Draw line to child
                ax.plot(
                    [x_pos, child_x],
                    [y_pos - 3, child_y + 3],
                    color="black",
                    linestyle="-",
                    linewidth=1,
                )

                # Recursively draw the child
                self._draw_node(
                    ax, child, level + 1, child_x, child_y, child_width, level_height
                )

    def _visualize_graphviz(self, tree, output_name):
        """Visualize using graphviz"""
        # Create a new digraph
        dot = graphviz.Digraph(
            name=output_name,
            comment=f"B+ Tree '{tree.name}' visualization",
            format="png",
        )

        # Configure graph attributes
        dot.attr("graph", rankdir="TB", ranksep="0.5", nodesep="0.5")
        dot.attr("node", shape="record", style="filled", fillcolor="lightblue")

        # Generate visualization based on the tree structure
        self._add_nodes_to_graph(dot, tree.root)

        # Save the graph
        output_path = os.path.join(self.output_dir, output_name)
        rendered_path = dot.render(filename=output_path, cleanup=True)

        self.logger.info(f"Graphviz visualization saved to: {rendered_path}")
        return rendered_path

    def _add_nodes_to_graph(
        self, dot, node, parent_id=None, edge_label=None, node_counter=[0]
    ):
        """Add nodes recursively to the Graphviz graph"""
        node_id = f"node_{node_counter[0]}"
        node_counter[0] += 1

        # Create label for the node
        if hasattr(node, "leaf") and node.leaf:
            # For leaf nodes, include both keys and values
            key_labels = []
            for item in node.keys:
                if isinstance(item, tuple) and len(item) >= 2:
                    key_labels.append(f"{item[0]}:{item[1]}")
                else:
                    key_labels.append(str(item))
            label = "{{{}}}".format("|".join(key_labels))
            dot.node(node_id, label=label, fillcolor="lightgreen")
        else:
            # For internal nodes, include only keys
            label = "{{{}}}".format("|".join(map(str, node.keys)))
            dot.node(node_id, label=label)

        # Add edge from parent to this node if needed
        if parent_id is not None:
            dot.edge(parent_id, node_id, label=edge_label)

        # Process child nodes recursively
        if hasattr(node, "children"):
            for i, child in enumerate(node.children):
                edge_label = (
                    f"≤ {node.keys[i]}" if i < len(node.keys) else f"> {
                        node.keys[-1]}"
                )
                self._add_nodes_to_graph(
                    dot, child, node_id, edge_label, node_counter)

        # Show next pointers in leaf nodes
        if (
            hasattr(node, "leaf")
            and node.leaf
            and hasattr(node, "next")
            and node.next is not None
        ):
            next_id = f"node_{node_counter[0]}"  # ID for the next node
            self._add_nodes_to_graph(dot, node.next, None, None, node_counter)
            dot.edge(node_id, next_id, style="dashed",
                     label="next", constraint="false")

        return node_id

    def _export_tree_text(self, tree, output_name):
        """Export a text representation of the tree"""
        output_path = os.path.join(self.output_dir, f"{output_name}.txt")

        # Generate a text representation and save to file
        text = self._get_tree_text(tree)

        with open(output_path, "w") as f:
            f.write(text)

        self.logger.info(f"Text representation saved to: {output_path}")
        return output_path

    def _export_text_only(self, tree, message, output_name):
        """Export a simple text message when tree cannot be visualized"""
        output_path = os.path.join(
            self.output_dir, f"{output_name or 'error'}.txt")

        with open(output_path, "w") as f:
            f.write(f"Error: {message}\n")
            if tree and hasattr(tree, "name"):
                f.write(f"Tree name: {tree.name}\n")

        return output_path

    def _get_tree_text(self, tree):
        """Generate a text representation of the tree"""
        text = [
            f"B+ Tree '{tree.name}' (Order: {
                tree.order if hasattr(tree, 'order') else 'Unknown'
            })\n"
        ]

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
                text.append(
                    f"{indent}{prefix}LEAF {{{', '.join(key_values)}}}")
            else:
                text.append(
                    f"{indent}{
                        prefix}NODE {{{', '.join(map(str, node.keys))}}}"
                )

            # Print children recursively
            if hasattr(node, "children"):
                for i, child in enumerate(node.children):
                    child_prefix = f"Child {i}: "
                    print_node(child, level + 1, child_prefix)

        if hasattr(tree, "root"):
            print_node(tree.root)
        else:
            text.append("Tree structure not available")

        return "\n".join(text)

    def _analyze_tree(self, node, level=0):
        """Analyze tree structure to get properties like height, node count, etc."""
        info = {"height": level + 1, "leaf_count": 0, "node_count": 1}

        if node.leaf:
            info["leaf_count"] = 1
            return info

        child_info = {"height": 0, "leaf_count": 0, "node_count": 0}
        for child in node.children:
            child_result = self._analyze_tree(child, level + 1)
            child_info["height"] = max(
                child_info["height"], child_result["height"])
            child_info["leaf_count"] += child_result["leaf_count"]
            child_info["node_count"] += child_result["node_count"]

        info["height"] = max(info["height"], child_info["height"])
        info["leaf_count"] = child_info["leaf_count"]
        info["node_count"] += child_info["node_count"]

        return info
