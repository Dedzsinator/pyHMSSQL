import os
import logging
from datetime import datetime

# Try to import visualization libraries
try:
    import graphviz
    HAS_GRAPHVIZ = True
except ImportError:
    HAS_GRAPHVIZ = False
    logging.warning("Graphviz Python package not installed. Install with 'pip install graphviz' for B+ tree visualization.")
    logging.warning("Also ensure Graphviz software is installed on your system: https://graphviz.org/download/")

class BPlusTreeVisualizer:
    def __init__(self, output_dir='visualizations'):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        self.logger = logging.getLogger('btree_visualizer')
        self.logger.setLevel(logging.INFO)
        
    def visualize_tree(self, tree, output_name=None):
        """Visualize a B+ tree using Graphviz"""
        if not HAS_GRAPHVIZ:
            self.logger.warning("Skipping visualization: Graphviz not available")
            return self._export_tree_text(tree, output_name)
            
        output_name = output_name or f"{tree.name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        self.logger.info(f"Visualizing tree '{tree.name}' as '{output_name}'")
        
        # Create a new digraph
        dot = graphviz.Digraph(
            name=output_name,
            comment=f"B+ Tree '{tree.name}' visualization",
            format='png'
        )
        
        # Configure graph attributes
        dot.attr('graph', rankdir='TB', ranksep='0.5', nodesep='0.5')
        dot.attr('node', shape='record', style='filled', fillcolor='lightblue')
        
        # Generate visualization based on the tree structure
        self._add_nodes_to_graph(dot, tree.root)
        
        # Save the graph
        output_path = os.path.join(self.output_dir, output_name)
        try:
            rendered_path = dot.render(filename=output_path, cleanup=True)
            self.logger.info(f"B+ Tree visualization saved to: {rendered_path}")
            return rendered_path
        except Exception as e:
            self.logger.error(f"Error rendering B+ Tree visualization: {str(e)}")
            return self._export_tree_text(tree, output_name)
    
    def _add_nodes_to_graph(self, dot, node, parent_id=None, edge_label=None, node_counter=[0]):
        """Add nodes recursively to the graph"""
        node_id = f"node_{node_counter[0]}"
        node_counter[0] += 1
        
        # Create label for the node
        if hasattr(node, 'leaf') and node.leaf:
            # For leaf nodes, include both keys and values
            key_labels = []
            for item in node.keys:
                if isinstance(item, tuple) and len(item) >= 2:
                    key_labels.append(f"{item[0]}:{item[1]}")
                else:
                    key_labels.append(str(item))
            label = "{{{}}}".format("|".join(key_labels))
            dot.node(node_id, label=label, fillcolor='lightgreen')
        else:
            # For internal nodes, include only keys
            label = "{{{}}}".format("|".join(map(str, node.keys)))
            dot.node(node_id, label=label)
        
        # Add edge from parent to this node if needed
        if parent_id is not None:
            dot.edge(parent_id, node_id, label=edge_label)
        
        # Process child nodes recursively
        if hasattr(node, 'children'):
            for i, child in enumerate(node.children):
                edge_label = f"≤ {node.keys[i]}" if i < len(node.keys) else f"> {node.keys[-1]}"
                self._add_nodes_to_graph(dot, child, node_id, edge_label, node_counter)
        
        # Show next pointers in leaf nodes
        if hasattr(node, 'leaf') and node.leaf and hasattr(node, 'next') and node.next is not None:
            next_id = f"node_{node_counter[0]}"  # ID for the next node
            dot.edge(node_id, next_id, style='dashed', label='next', constraint='false')
        
        return node_id
    
    def _export_tree_text(self, tree, output_name=None):
        """Export a text representation of the tree if Graphviz fails"""
        output_name = output_name or f"{tree.name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        output_path = os.path.join(self.output_dir, f"{output_name}.txt")
        
        # Generate a text representation and save to file
        text = self._get_tree_text(tree)
        
        with open(output_path, 'w') as f:
            f.write(text)
            
        self.logger.info(f"Text representation saved to: {output_path}")
        return output_path
    
    def _get_tree_text(self, tree):
        """Generate a text representation of the tree"""
        text = [f"B+ Tree '{tree.name}' (Order: {tree.order if hasattr(tree, 'order') else 'Unknown'})\n"]
        
        def print_node(node, level=0, prefix='Root: '):
            indent = '  ' * level
            
            # Print the current node
            if hasattr(node, 'leaf') and node.leaf:
                key_values = []
                for item in node.keys:
                    if isinstance(item, tuple) and len(item) >= 2:
                        key_values.append(f"{item[0]}:{item[1]}")
                    else:
                        key_values.append(str(item))
                text.append(f"{indent}{prefix}LEAF {{{', '.join(key_values)}}}")
            else:
                text.append(f"{indent}{prefix}NODE {{{', '.join(map(str, node.keys))}}}")
            
            # Print children recursively
            if hasattr(node, 'children'):
                for i, child in enumerate(node.children):
                    child_prefix = f"Child {i}: "
                    print_node(child, level + 1, child_prefix)
        
        if hasattr(tree, 'root'):
            print_node(tree.root)
        else:
            text.append("Tree structure not available")
            
        return '\n'.join(text)