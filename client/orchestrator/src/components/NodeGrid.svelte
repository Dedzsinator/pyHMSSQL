<script>
	export let nodes;
	export let serverStats;
	export let onFailover;
	
	$: nodeList = Object.entries(nodes || {}).map(([id, node]) => ({
		...node,
		stats: serverStats[id] || {}
	}));
	
	function getHealthClass(health) {
		switch (health) {
			case 'healthy': return 'node-healthy';
			case 'degraded': return 'node-degraded';
			case 'failed': return 'node-failed';
			default: return 'node-unknown';
		}
	}
	
	function getRoleClass(role) {
		switch (role) {
			case 'primary': return 'role-primary';
			case 'replica': return 'role-replica';
			case 'leader': return 'role-leader';
			default: return 'role-unknown';
		}
	}
	
	function formatUptime(uptime) {
		if (!uptime) return '0h';
		return `${(uptime / 3600).toFixed(1)}h`;
	}
</script>

<div class="node-grid">
	<h2>🖥️ Database Nodes</h2>
	
	{#if nodeList.length === 0}
		<div class="no-nodes">
			<div class="no-nodes-icon">🔍</div>
			<p>No nodes discovered yet</p>
		</div>
	{:else}
		<div class="nodes-container">
			{#each nodeList as node (node.node_id)}
				<div class="node-card {getHealthClass(node.health)}">
					<div class="node-header">
						<div class="node-id">{node.node_id}</div>
						<div class="node-role {getRoleClass(node.role)}">
							{node.role || 'unknown'}
						</div>
					</div>
					
					<div class="node-details">
						<div class="detail-row">
							<span class="detail-label">Host:</span>
							<span class="detail-value">{node.host}:{node.port}</span>
						</div>
						
						<div class="detail-row">
							<span class="detail-label">Health:</span>
							<span class="detail-value">{node.health || 'unknown'}</span>
						</div>
						
						<div class="detail-row">
							<span class="detail-label">Uptime:</span>
							<span class="detail-value">{formatUptime(node.uptime)}</span>
						</div>
						
						<div class="detail-row">
							<span class="detail-label">Connections:</span>
							<span class="detail-value">{node.connections || 0}</span>
						</div>
						
						<div class="detail-row">
							<span class="detail-label">RAFT State:</span>
							<span class="detail-value">{node.raft_state || 'unknown'}</span>
						</div>
						
						<div class="detail-row">
							<span class="detail-label">Repl. Lag:</span>
							<span class="detail-value">{node.replication_lag || 0}ms</span>
						</div>
						
						{#if node.stats && node.stats.performance}
							<div class="detail-row">
								<span class="detail-label">CPU:</span>
								<span class="detail-value">{node.stats.performance.cpu_usage?.toFixed(1) || 0}%</span>
							</div>
							
							<div class="detail-row">
								<span class="detail-label">Memory:</span>
								<span class="detail-value">{node.stats.performance.memory_usage?.toFixed(1) || 0}%</span>
							</div>
						{/if}
					</div>
					
					<div class="node-actions">
						{#if node.role !== 'primary' && node.health === 'healthy'}
							<button 
								class="promote-btn"
								on:click={() => onFailover(node.node_id)}
							>
								Promote to Primary
							</button>
						{:else if node.role === 'primary'}
							<div class="primary-badge">PRIMARY</div>
						{:else}
							<div class="unavailable-badge">Not Available</div>
						{/if}
					</div>
				</div>
			{/each}
		</div>
	{/if}
</div>

<style>
	.node-grid {
		background: rgba(255, 255, 255, 0.95);
		border-radius: 12px;
		padding: 20px;
		box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
		backdrop-filter: blur(10px);
	}
	
	.node-grid h2 {
		margin: 0 0 20px 0;
		color: #333;
		font-size: 1.4rem;
	}
	
	.no-nodes {
		text-align: center;
		padding: 40px;
		color: #666;
	}
	
	.no-nodes-icon {
		font-size: 3rem;
		margin-bottom: 15px;
		opacity: 0.6;
	}
	
	.nodes-container {
		display: grid;
		grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
		gap: 20px;
	}
	
	.node-card {
		padding: 20px;
		border-radius: 10px;
		border: 1px solid #e9ecef;
		transition: all 0.3s ease;
		position: relative;
	}
	
	.node-card:hover {
		transform: translateY(-3px);
		box-shadow: 0 8px 25px rgba(0, 0, 0, 0.15);
	}
	
	.node-healthy {
		background: linear-gradient(135deg, #d4edda 0%, #ffffff 100%);
		border-left: 4px solid #28a745;
	}
	
	.node-degraded {
		background: linear-gradient(135deg, #fff3cd 0%, #ffffff 100%);
		border-left: 4px solid #ffc107;
	}
	
	.node-failed {
		background: linear-gradient(135deg, #f8d7da 0%, #ffffff 100%);
		border-left: 4px solid #dc3545;
	}
	
	.node-unknown {
		background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);
		border-left: 4px solid #6c757d;
	}
	
	.node-header {
		display: flex;
		justify-content: space-between;
		align-items: center;
		margin-bottom: 15px;
	}
	
	.node-id {
		font-size: 1.1rem;
		font-weight: bold;
		color: #333;
	}
	
	.node-role {
		padding: 4px 12px;
		border-radius: 20px;
		font-size: 0.8rem;
		font-weight: bold;
		text-transform: uppercase;
	}
	
	.role-primary, .role-leader {
		background: #007bff;
		color: white;
	}
	
	.role-replica {
		background: #28a745;
		color: white;
	}
	
	.role-unknown {
		background: #6c757d;
		color: white;
	}
	
	.node-details {
		display: grid;
		grid-template-columns: 1fr 1fr;
		gap: 8px;
		margin-bottom: 15px;
	}
	
	.detail-row {
		display: flex;
		justify-content: space-between;
		font-size: 0.9rem;
		padding: 4px 0;
	}
	
	.detail-label {
		color: #666;
		font-weight: 500;
	}
	
	.detail-value {
		color: #333;
		font-weight: 600;
	}
	
	.node-actions {
		display: flex;
		justify-content: center;
	}
	
	.promote-btn {
		background: #28a745;
		color: white;
		border: none;
		padding: 8px 16px;
		border-radius: 6px;
		cursor: pointer;
		font-weight: 600;
		transition: background-color 0.2s ease;
	}
	
	.promote-btn:hover {
		background: #218838;
	}
	
	.primary-badge {
		background: #007bff;
		color: white;
		padding: 8px 16px;
		border-radius: 6px;
		font-weight: bold;
		font-size: 0.8rem;
	}
	
	.unavailable-badge {
		background: #6c757d;
		color: white;
		padding: 8px 16px;
		border-radius: 6px;
		font-weight: bold;
		font-size: 0.8rem;
	}
	
	@media (max-width: 768px) {
		.nodes-container {
			grid-template-columns: 1fr;
		}
	}
</style>
