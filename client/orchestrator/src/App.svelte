<script>
	import { onMount } from 'svelte';
	import ClusterOverview from './components/ClusterOverview.svelte';
	import NodeGrid from './components/NodeGrid.svelte';
	import MetricsPanel from './components/MetricsPanel.svelte';
	import TopologyGraph from './components/TopologyGraph.svelte';
	import LogViewer from './components/LogViewer.svelte';
	
	let clusterData = {
		nodes: {},
		metrics: {},
		health_summary: {},
		topology: 'unknown'
	};
	
	let serverStats = {};
	let topologyData = { nodes: [], edges: [] };
	let refreshing = false;
	let lastRefresh = null;
	
	// Auto-refresh interval
	let refreshInterval = 10000; // 10 seconds
	
	onMount(() => {
		refreshData();
		const interval = setInterval(refreshData, refreshInterval);
		return () => clearInterval(interval);
	});
	
	async function refreshData() {
		if (refreshing) return;
		refreshing = true;
		
		try {
			// Fetch cluster status
			const clusterResponse = await fetch('/api/cluster/status');
			if (clusterResponse.ok) {
				clusterData = await clusterResponse.json();
			}
			
			// Fetch server stats
			const statsResponse = await fetch('/api/stats/servers');
			if (statsResponse.ok) {
				serverStats = await statsResponse.json();
			}
			
			// Fetch topology data
			const topologyResponse = await fetch('/api/topology/graph');
			if (topologyResponse.ok) {
				topologyData = await topologyResponse.json();
			}
			
			lastRefresh = new Date();
		} catch (error) {
			console.error('Failed to refresh data:', error);
		} finally {
			refreshing = false;
		}
	}
	
	async function handleFailover(nodeId) {
		if (!confirm(`Promote ${nodeId} to primary?`)) return;
		
		try {
			const response = await fetch('/api/cluster/failover', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ target_node: nodeId })
			});
			
			const result = await response.json();
			if (result.success) {
				alert('Failover initiated successfully');
				await refreshData();
			} else {
				alert(`Failover failed: ${result.error}`);
			}
		} catch (error) {
			alert(`Failover request failed: ${error.message}`);
		}
	}
</script>

<main class="orchestrator-app">
	<header class="app-header">
		<div class="header-content">
			<h1>🎯 HMSSQL Database Orchestrator</h1>
			<p>Real-time cluster monitoring and management</p>
			<div class="header-actions">
				<button 
					class="refresh-btn" 
					class:refreshing 
					on:click={refreshData}
					disabled={refreshing}
				>
					{refreshing ? '🔄 Refreshing...' : '🔄 Refresh'}
				</button>
				{#if lastRefresh}
					<span class="last-refresh">
						Last updated: {lastRefresh.toLocaleTimeString()}
					</span>
				{/if}
			</div>
		</div>
	</header>

	<div class="dashboard-container">
		<!-- Cluster Overview -->
		<section class="overview-section">
			<ClusterOverview 
				{clusterData} 
				totalNodes={Object.keys(clusterData.nodes || {}).length}
			/>
		</section>

		<!-- Metrics Panel -->
		<section class="metrics-section">
			<MetricsPanel 
				metrics={clusterData.metrics || {}}
				healthSummary={clusterData.health_summary || {}}
			/>
		</section>

		<!-- Topology Visualization -->
		<section class="topology-section full-width">
			<TopologyGraph 
				data={topologyData}
				onNodeClick={(nodeId) => console.log('Clicked node:', nodeId)}
			/>
		</section>

		<!-- Node Grid -->
		<section class="nodes-section full-width">
			<NodeGrid 
				nodes={clusterData.nodes || {}}
				{serverStats}
				onFailover={handleFailover}
			/>
		</section>

		<!-- Log Viewer -->
		<section class="logs-section full-width">
			<LogViewer />
		</section>
	</div>
</main>

<style>
	.orchestrator-app {
		min-height: 100vh;
		background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
		padding: 0;
		margin: 0;
	}

	.app-header {
		background: rgba(255, 255, 255, 0.95);
		backdrop-filter: blur(10px);
		border-bottom: 1px solid rgba(255, 255, 255, 0.2);
		padding: 20px;
		box-shadow: 0 2px 20px rgba(0, 0, 0, 0.1);
	}

	.header-content {
		max-width: 1400px;
		margin: 0 auto;
		display: flex;
		justify-content: space-between;
		align-items: center;
		flex-wrap: wrap;
		gap: 20px;
	}

	.header-content h1 {
		margin: 0;
		background: linear-gradient(45deg, #667eea, #764ba2);
		-webkit-background-clip: text;
		-webkit-text-fill-color: transparent;
		background-clip: text;
		font-size: 2rem;
	}

	.header-content p {
		margin: 5px 0 0 0;
		color: #666;
		font-size: 1rem;
	}

	.header-actions {
		display: flex;
		align-items: center;
		gap: 15px;
	}

	.refresh-btn {
		background: #007bff;
		color: white;
		border: none;
		padding: 10px 20px;
		border-radius: 8px;
		cursor: pointer;
		font-weight: 600;
		transition: all 0.2s ease;
	}

	.refresh-btn:hover:not(:disabled) {
		background: #0056b3;
		transform: translateY(-1px);
	}

	.refresh-btn:disabled {
		opacity: 0.6;
		cursor: not-allowed;
	}

	.refresh-btn.refreshing {
		animation: spin 1s linear infinite;
	}

	@keyframes spin {
		from { transform: rotate(0deg); }
		to { transform: rotate(360deg); }
	}

	.last-refresh {
		color: #666;
		font-size: 0.9rem;
	}

	.dashboard-container {
		max-width: 1400px;
		margin: 0 auto;
		padding: 20px;
		display: grid;
		grid-template-columns: 1fr 1fr;
		gap: 20px;
	}

	.overview-section, .metrics-section {
		min-height: 200px;
	}

	.full-width {
		grid-column: 1 / -1;
	}

	.topology-section, .nodes-section, .logs-section {
		min-height: 300px;
	}

	@media (max-width: 768px) {
		.dashboard-container {
			grid-template-columns: 1fr;
			padding: 15px;
		}
		
		.header-content {
			text-align: center;
		}
		
		.header-content h1 {
			font-size: 1.5rem;
		}
	}
</style>
