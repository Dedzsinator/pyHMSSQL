<script>
	export let data;
	export let onNodeClick = () => {};
	
	import { onMount } from 'svelte';
	
	let container;
	let network = null;
	
	$: if (data && data.nodes && container) {
		updateTopology();
	}
	
	onMount(() => {
		// Simple topology visualization without external dependencies
		updateTopology();
	});
	
	function updateTopology() {
		if (!container || !data || !data.nodes) return;
		
		// Clear existing content
		container.innerHTML = '';
		
		// Create simple SVG topology
		const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
		svg.setAttribute('width', '100%');
		svg.setAttribute('height', '400');
		svg.setAttribute('viewBox', '0 0 800 400');
		
		const nodes = data.nodes || [];
		if (nodes.length === 0) {
			// Show empty state
			const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
			text.setAttribute('x', '400');
			text.setAttribute('y', '200');
			text.setAttribute('text-anchor', 'middle');
			text.setAttribute('fill', '#666');
			text.setAttribute('font-size', '16');
			text.textContent = 'No topology data available';
			svg.appendChild(text);
			container.appendChild(svg);
			return;
		}
		
		// Position nodes in a simple layout
		const centerX = 400;
		const centerY = 200;
		const radius = 120;
		
		nodes.forEach((node, index) => {
			const angle = (index / nodes.length) * 2 * Math.PI;
			const x = centerX + Math.cos(angle) * radius;
			const y = centerY + Math.sin(angle) * radius;
			
			// Create node circle
			const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
			circle.setAttribute('cx', x);
			circle.setAttribute('cy', y);
			circle.setAttribute('r', '30');
			
			// Color based on role
			let fill = '#6c757d'; // default
			if (node.role === 'primary_master') fill = '#dc3545';
			else if (node.role === 'semi_master') fill = '#fd7e14';
			else if (node.role === 'replica') fill = '#28a745';
			
			circle.setAttribute('fill', fill);
			circle.setAttribute('stroke', '#333');
			circle.setAttribute('stroke-width', '2');
			circle.style.cursor = 'pointer';
			
			// Add click handler
			circle.addEventListener('click', () => onNodeClick(node.id));
			
			svg.appendChild(circle);
			
			// Add node label
			const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
			text.setAttribute('x', x);
			text.setAttribute('y', y + 5);
			text.setAttribute('text-anchor', 'middle');
			text.setAttribute('fill', 'white');
			text.setAttribute('font-size', '12');
			text.setAttribute('font-weight', 'bold');
			text.textContent = node.id ? node.id.split('-')[1] || node.id.slice(-4) : 'N/A';
			text.style.pointerEvents = 'none';
			svg.appendChild(text);
			
			// Add node info below
			const infoText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
			infoText.setAttribute('x', x);
			infoText.setAttribute('y', y + 45);
			infoText.setAttribute('text-anchor', 'middle');
			infoText.setAttribute('fill', '#333');
			infoText.setAttribute('font-size', '10');
			infoText.textContent = node.health || 'unknown';
			svg.appendChild(infoText);
		});
		
		// Draw edges
		const edges = data.edges || [];
		edges.forEach(edge => {
			const fromNode = nodes.find(n => n.id === edge.from);
			const toNode = nodes.find(n => n.id === edge.to);
			
			if (fromNode && toNode) {
				const fromIndex = nodes.indexOf(fromNode);
				const toIndex = nodes.indexOf(toNode);
				
				const fromAngle = (fromIndex / nodes.length) * 2 * Math.PI;
				const toAngle = (toIndex / nodes.length) * 2 * Math.PI;
				
				const fromX = centerX + Math.cos(fromAngle) * radius;
				const fromY = centerY + Math.sin(fromAngle) * radius;
				const toX = centerX + Math.cos(toAngle) * radius;
				const toY = centerY + Math.sin(toAngle) * radius;
				
				const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
				line.setAttribute('x1', fromX);
				line.setAttribute('y1', fromY);
				line.setAttribute('x2', toX);
				line.setAttribute('y2', toY);
				line.setAttribute('stroke', '#666');
				line.setAttribute('stroke-width', '2');
				line.setAttribute('opacity', '0.6');
				
				svg.insertBefore(line, svg.firstChild); // Add behind nodes
			}
		});
		
		container.appendChild(svg);
	}
</script>

<div class="topology-graph">
	<h2>🌐 Cluster Topology</h2>
	
	<div class="topology-controls">
		<div class="legend">
			<div class="legend-item">
				<div class="legend-color primary"></div>
				<span>Primary Master</span>
			</div>
			<div class="legend-item">
				<div class="legend-color semi"></div>
				<span>Semi Master</span>
			</div>
			<div class="legend-item">
				<div class="legend-color replica"></div>
				<span>Replica</span>
			</div>
			<div class="legend-item">
				<div class="legend-color offline"></div>
				<span>Offline</span>
			</div>
		</div>
	</div>
	
	<div class="topology-container" bind:this={container}>
		<!-- SVG topology will be inserted here -->
	</div>
</div>

<style>
	.topology-graph {
		background: rgba(255, 255, 255, 0.95);
		border-radius: 12px;
		padding: 20px;
		box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
		backdrop-filter: blur(10px);
	}
	
	.topology-graph h2 {
		margin: 0 0 20px 0;
		color: #333;
		font-size: 1.4rem;
	}
	
	.topology-controls {
		margin-bottom: 15px;
	}
	
	.legend {
		display: flex;
		gap: 20px;
		flex-wrap: wrap;
		font-size: 0.9rem;
	}
	
	.legend-item {
		display: flex;
		align-items: center;
		gap: 6px;
	}
	
	.legend-color {
		width: 16px;
		height: 16px;
		border-radius: 50%;
		border: 1px solid #333;
	}
	
	.legend-color.primary {
		background: #dc3545;
	}
	
	.legend-color.semi {
		background: #fd7e14;
	}
	
	.legend-color.replica {
		background: #28a745;
	}
	
	.legend-color.offline {
		background: #6c757d;
	}
	
	.topology-container {
		min-height: 400px;
		border: 1px solid #e9ecef;
		border-radius: 8px;
		background: #f8f9fa;
		display: flex;
		align-items: center;
		justify-content: center;
	}
	
	.topology-container :global(svg) {
		max-width: 100%;
		height: auto;
	}
</style>
