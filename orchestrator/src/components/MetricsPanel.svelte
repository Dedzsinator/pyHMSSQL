<script>
	export let metrics;
	export let healthSummary;
	
	$: discoveryRuns = metrics.discovery_runs || 0;
	$: failovers = metrics.failovers_executed || 0;
	$: lastDiscovery = metrics.last_discovery;
	$: totalNodes = metrics.total_nodes_discovered || 0;
	$: healthyNodes = metrics.healthy_nodes || 0;
	$: failedNodes = metrics.failed_nodes || 0;
	$: healthPercentage = healthSummary.health_percentage || 0;
	
	function formatTime(isoString) {
		if (!isoString) return 'Never';
		return new Date(isoString).toLocaleTimeString();
	}
</script>

<div class="metrics-panel">
	<h2>📈 System Metrics</h2>
	
	<div class="metrics-grid">
		<div class="metric-item">
			<div class="metric-label">Discovery Runs</div>
			<div class="metric-value">{discoveryRuns}</div>
		</div>
		
		<div class="metric-item">
			<div class="metric-label">Failovers</div>
			<div class="metric-value failover-count">{failovers}</div>
		</div>
		
		<div class="metric-item">
			<div class="metric-label">Healthy Nodes</div>
			<div class="metric-value healthy-count">{healthyNodes}</div>
		</div>
		
		<div class="metric-item">
			<div class="metric-label">Failed Nodes</div>
			<div class="metric-value failed-count">{failedNodes}</div>
		</div>
		
		<div class="metric-item">
			<div class="metric-label">Health</div>
			<div class="metric-value health-percentage" data-health={healthSummary.status}>{healthPercentage.toFixed(1)}%</div>
		</div>
		
		<div class="metric-item full-width">
			<div class="metric-label">Last Discovery</div>
			<div class="metric-value time-value">{formatTime(lastDiscovery)}</div>
		</div>
	</div>
</div>

<style>
	.metrics-panel {
		background: rgba(255, 255, 255, 0.95);
		border-radius: 12px;
		padding: 20px;
		box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
		backdrop-filter: blur(10px);
	}
	
	.metrics-panel h2 {
		margin: 0 0 20px 0;
		color: #333;
		font-size: 1.4rem;
	}
	
	.metrics-grid {
		display: grid;
		grid-template-columns: 1fr 1fr;
		gap: 15px;
	}
	
	.metric-item {
		padding: 15px;
		background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);
		border-radius: 8px;
		border: 1px solid #e9ecef;
		text-align: center;
		transition: transform 0.2s ease;
	}
	
	.metric-item:hover {
		transform: translateY(-2px);
	}
	
	.metric-item.full-width {
		grid-column: 1 / -1;
	}
	
	.metric-label {
		font-size: 0.85rem;
		color: #666;
		text-transform: uppercase;
		font-weight: 600;
		letter-spacing: 0.5px;
		margin-bottom: 8px;
	}
	
	.metric-value {
		font-size: 1.6rem;
		font-weight: bold;
		color: #333;
	}
	
	.failover-count {
		color: #ffc107;
	}
	
	.health-percentage {
		color: #28a745;
	}
	
	.health-percentage[data-health="critical"] {
		color: #dc3545;
	}
	
	.health-percentage[data-health="degraded"] {
		color: #ffc107;
	}
	
	.healthy-count {
		color: #28a745;
	}
	
	.failed-count {
		color: #dc3545;
	}
	
	.time-value {
		font-size: 1.2rem;
		color: #007bff;
	}
</style>
