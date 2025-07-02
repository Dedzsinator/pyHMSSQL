<script>
	import { onMount } from 'svelte';
	
	let logs = [];
	let loading = false;
	
	onMount(() => {
		// Initialize with some sample logs since we don't have a real log endpoint
		generateSampleLogs();
	});
	
	function generateSampleLogs() {
		const sampleEvents = [
			{ level: 'info', message: 'Discovery loop started', timestamp: new Date().toISOString() },
			{ level: 'info', message: 'Found 3 nodes in cluster', timestamp: new Date(Date.now() - 30000).toISOString() },
			{ level: 'warning', message: 'Node localhost-9998 marked as degraded', timestamp: new Date(Date.now() - 60000).toISOString() },
			{ level: 'info', message: 'Health monitoring loop active', timestamp: new Date(Date.now() - 90000).toISOString() },
			{ level: 'info', message: 'HMSSQL Database Orchestrator started', timestamp: new Date(Date.now() - 120000).toISOString() }
		];
		logs = sampleEvents;
	}
	
	function formatTimestamp(isoString) {
		return new Date(isoString).toLocaleTimeString();
	}
	
	function getLevelClass(level) {
		switch (level) {
			case 'error': return 'log-error';
			case 'warning': return 'log-warning';
			case 'info': return 'log-info';
			default: return 'log-default';
		}
	}
	
	function refreshLogs() {
		loading = true;
		// Simulate API call
		setTimeout(() => {
			generateSampleLogs();
			loading = false;
		}, 500);
	}
</script>

<div class="log-viewer">
	<div class="log-header">
		<h2>📋 Recent Events</h2>
		<button 
			class="refresh-logs-btn" 
			class:loading 
			on:click={refreshLogs}
			disabled={loading}
		>
			{loading ? '🔄' : '↻'} Refresh
		</button>
	</div>
	
	<div class="log-container">
		{#if logs.length === 0}
			<div class="no-logs">
				<div class="no-logs-icon">📝</div>
				<p>No recent events</p>
			</div>
		{:else}
			{#each logs as log}
				<div class="log-entry {getLevelClass(log.level)}">
					<div class="log-time">{formatTimestamp(log.timestamp)}</div>
					<div class="log-level">{log.level.toUpperCase()}</div>
					<div class="log-message">{log.message}</div>
				</div>
			{/each}
		{/if}
	</div>
</div>

<style>
	.log-viewer {
		background: rgba(255, 255, 255, 0.95);
		border-radius: 12px;
		padding: 20px;
		box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
		backdrop-filter: blur(10px);
	}
	
	.log-header {
		display: flex;
		justify-content: space-between;
		align-items: center;
		margin-bottom: 20px;
	}
	
	.log-header h2 {
		margin: 0;
		color: #333;
		font-size: 1.4rem;
	}
	
	.refresh-logs-btn {
		background: #007bff;
		color: white;
		border: none;
		padding: 8px 12px;
		border-radius: 6px;
		cursor: pointer;
		font-size: 0.9rem;
		transition: all 0.2s ease;
	}
	
	.refresh-logs-btn:hover:not(:disabled) {
		background: #0056b3;
	}
	
	.refresh-logs-btn:disabled {
		opacity: 0.6;
		cursor: not-allowed;
	}
	
	.refresh-logs-btn.loading {
		animation: spin 1s linear infinite;
	}
	
	@keyframes spin {
		from { transform: rotate(0deg); }
		to { transform: rotate(360deg); }
	}
	
	.log-container {
		background: #f8f9fa;
		border-radius: 8px;
		padding: 15px;
		max-height: 300px;
		overflow-y: auto;
		font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
		font-size: 0.85rem;
		line-height: 1.4;
	}
	
	.no-logs {
		text-align: center;
		padding: 40px;
		color: #666;
	}
	
	.no-logs-icon {
		font-size: 2rem;
		margin-bottom: 10px;
		opacity: 0.6;
	}
	
	.log-entry {
		display: grid;
		grid-template-columns: auto auto 1fr;
		gap: 12px;
		padding: 8px 0;
		border-bottom: 1px solid #e9ecef;
		align-items: baseline;
	}
	
	.log-entry:last-child {
		border-bottom: none;
	}
	
	.log-time {
		color: #666;
		font-size: 0.8rem;
		white-space: nowrap;
	}
	
	.log-level {
		font-weight: bold;
		font-size: 0.75rem;
		padding: 2px 6px;
		border-radius: 4px;
		white-space: nowrap;
	}
	
	.log-message {
		color: #333;
	}
	
	.log-info .log-level {
		background: #cce5ff;
		color: #004085;
	}
	
	.log-warning .log-level {
		background: #fff3cd;
		color: #856404;
	}
	
	.log-error .log-level {
		background: #f8d7da;
		color: #721c24;
	}
	
	.log-default .log-level {
		background: #e2e3e5;
		color: #383d41;
	}
	
	/* Custom scrollbar */
	.log-container::-webkit-scrollbar {
		width: 6px;
	}
	
	.log-container::-webkit-scrollbar-track {
		background: #f1f1f1;
		border-radius: 3px;
	}
	
	.log-container::-webkit-scrollbar-thumb {
		background: #c1c1c1;
		border-radius: 3px;
	}
	
	.log-container::-webkit-scrollbar-thumb:hover {
		background: #a8a8a8;
	}
</style>
