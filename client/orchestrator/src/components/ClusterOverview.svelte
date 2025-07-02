<script>
	export let clusterData;
	export let totalNodes;
	
	$: healthSummary = clusterData.health_summary || {};
	$: healthyNodes = healthSummary.healthy_nodes || 0;
	$: healthPercentage = healthSummary.health_percentage || 0;
	$: status = healthSummary.status || 'unknown';
	$: topology = clusterData.topology || 'unknown';
	
	function getStatusColor(status) {
		switch (status) {
			case 'healthy': return '#28a745';
			case 'degraded': return '#ffc107';
			case 'critical': return '#dc3545';
			default: return '#6c757d';
		}
	}
</script>

<div class="cluster-overview">
	<h2>📊 Cluster Overview</h2>
	
	<div class="overview-grid">
		<div class="metric-card">
			<div class="metric-icon">🖥️</div>
			<div class="metric-content">
				<h3>Total Nodes</h3>
				<div class="metric-value">{totalNodes}</div>
				<div class="metric-subtext">{healthyNodes} healthy</div>
			</div>
		</div>
		
		<div class="metric-card">
			<div class="metric-icon">❤️</div>
			<div class="metric-content">
				<h3>Cluster Health</h3>
				<div class="metric-value" style="color: {getStatusColor(status)}">
					{healthPercentage.toFixed(1)}%
				</div>
				<div class="metric-subtext">{status.toUpperCase()}</div>
			</div>
		</div>
		
		<div class="metric-card">
			<div class="metric-icon">🌐</div>
			<div class="metric-content">
				<h3>Topology</h3>
				<div class="metric-value">{topology}</div>
				<div class="metric-subtext">consensus mode</div>
			</div>
		</div>
	</div>
</div>

<style>
	.cluster-overview {
		background: rgba(255, 255, 255, 0.95);
		border-radius: 12px;
		padding: 20px;
		box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
		backdrop-filter: blur(10px);
	}
	
	.cluster-overview h2 {
		margin: 0 0 20px 0;
		color: #333;
		font-size: 1.4rem;
	}
	
	.overview-grid {
		display: grid;
		grid-template-columns: 1fr;
		gap: 15px;
	}
	
	.metric-card {
		display: flex;
		align-items: center;
		gap: 15px;
		padding: 15px;
		background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);
		border-radius: 8px;
		border: 1px solid #e9ecef;
		transition: transform 0.2s ease;
	}
	
	.metric-card:hover {
		transform: translateY(-2px);
		box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
	}
	
	.metric-icon {
		font-size: 2rem;
		opacity: 0.8;
	}
	
	.metric-content h3 {
		margin: 0;
		font-size: 0.9rem;
		color: #666;
		text-transform: uppercase;
		font-weight: 600;
		letter-spacing: 0.5px;
	}
	
	.metric-value {
		font-size: 1.8rem;
		font-weight: bold;
		color: #333;
		margin: 5px 0;
		line-height: 1;
	}
	
	.metric-subtext {
		font-size: 0.8rem;
		color: #666;
		margin: 0;
	}
	
	@media (min-width: 600px) {
		.overview-grid {
			grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
		}
	}
</style>
