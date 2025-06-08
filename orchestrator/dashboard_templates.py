"""Monitoring dashboard templates for HMSSQL Orchestrator"""

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HMSSQL Cluster Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: rgba(255, 255, 255, 0.95);
            border-radius: 20px;
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1);
            padding: 30px;
        }
        
        .header {
            text-align: center;
            margin-bottom: 40px;
            color: #333;
        }
        
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            background: linear-gradient(45deg, #667eea, #764ba2);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        
        .cluster-overview {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }
        
        .metric-card {
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 20px rgba(0, 0, 0, 0.05);
            border: 1px solid #e1e5e9;
            transition: transform 0.3s ease;
        }
        
        .metric-card:hover {
            transform: translateY(-5px);
        }
        
        .metric-card h3 {
            color: #666;
            font-size: 0.9em;
            text-transform: uppercase;
            margin-bottom: 10px;
            letter-spacing: 1px;
        }
        
        .metric-value {
            font-size: 2.5em;
            font-weight: bold;
            color: #333;
            margin-bottom: 5px;
        }
        
        .metric-trend {
            font-size: 0.85em;
            color: #28a745;
        }
        
        .metric-trend.down {
            color: #dc3545;
        }
        
        .nodes-section {
            margin-bottom: 40px;
        }
        
        .section-title {
            font-size: 1.5em;
            color: #333;
            margin-bottom: 20px;
            border-bottom: 2px solid #e1e5e9;
            padding-bottom: 10px;
        }
        
        .nodes-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
            gap: 20px;
        }
        
        .node-card {
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 20px rgba(0, 0, 0, 0.05);
            border-left: 4px solid #28a745;
            transition: all 0.3s ease;
        }
        
        .node-card.leader {
            border-left-color: #007bff;
            background: linear-gradient(135deg, #e3f2fd 0%, #f8f9fa 100%);
        }
        
        .node-card.failed {
            border-left-color: #dc3545;
            background: linear-gradient(135deg, #ffebee 0%, #f8f9fa 100%);
        }
        
        .node-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }
        
        .node-id {
            font-size: 1.2em;
            font-weight: bold;
            color: #333;
        }
        
        .node-status {
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.8em;
            font-weight: bold;
            text-transform: uppercase;
        }
        
        .status-healthy {
            background: #d4edda;
            color: #155724;
        }
        
        .status-leader {
            background: #cce5ff;
            color: #004085;
        }
        
        .status-failed {
            background: #f8d7da;
            color: #721c24;
        }
        
        .node-details {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
            font-size: 0.9em;
        }
        
        .detail-item {
            display: flex;
            justify-content: space-between;
            padding: 5px 0;
            border-bottom: 1px solid #f1f3f5;
        }
        
        .detail-label {
            color: #666;
        }
        
        .detail-value {
            font-weight: bold;
            color: #333;
        }
        
        .actions-section {
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 20px rgba(0, 0, 0, 0.05);
        }
        
        .action-buttons {
            display: flex;
            gap: 15px;
            flex-wrap: wrap;
        }
        
        .btn {
            padding: 12px 25px;
            border: none;
            border-radius: 8px;
            font-weight: bold;
            cursor: pointer;
            transition: all 0.3s ease;
            text-decoration: none;
            display: inline-block;
        }
        
        .btn-primary {
            background: #007bff;
            color: white;
        }
        
        .btn-warning {
            background: #ffc107;
            color: #212529;
        }
        
        .btn-danger {
            background: #dc3545;
            color: white;
        }
        
        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.2);
        }
        
        .refresh-indicator {
            display: none;
            text-align: center;
            color: #666;
            margin-top: 20px;
        }
        
        .log-section {
            margin-top: 30px;
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 20px rgba(0, 0, 0, 0.05);
        }
        
        .log-container {
            background: #f8f9fa;
            border-radius: 8px;
            padding: 20px;
            max-height: 300px;
            overflow-y: auto;
            font-family: 'Courier New', monospace;
            font-size: 0.85em;
            line-height: 1.4;
        }
        
        .log-entry {
            margin-bottom: 5px;
        }
        
        .log-timestamp {
            color: #666;
        }
        
        .log-level-info {
            color: #17a2b8;
        }
        
        .log-level-warning {
            color: #ffc107;
        }
        
        .log-level-error {
            color: #dc3545;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🗄️ HMSSQL Cluster Dashboard</h1>
            <p>Real-time monitoring and management for your HMSSQL database cluster</p>
        </div>
        
        <div class="cluster-overview">
            <div class="metric-card">
                <h3>Total Nodes</h3>
                <div class="metric-value" id="total-nodes">{{ cluster_stats.total_nodes }}</div>
                <div class="metric-trend">{{ cluster_stats.healthy_nodes }} healthy</div>
            </div>
            
            <div class="metric-card">
                <h3>Current Leader</h3>
                <div class="metric-value" id="current-leader">{{ cluster_stats.leader_id[:8] if cluster_stats.leader_id else 'None' }}</div>
                <div class="metric-trend">Term {{ cluster_stats.leader_term }}</div>
            </div>
            
            <div class="metric-card">
                <h3>Cluster Health</h3>
                <div class="metric-value" id="cluster-health">{{ cluster_stats.health_percentage }}%</div>
                <div class="metric-trend">{{ 'Excellent' if cluster_stats.health_percentage > 80 else 'Good' if cluster_stats.health_percentage > 60 else 'Poor' }}</div>
            </div>
            
            <div class="metric-card">
                <h3>Operations/sec</h3>
                <div class="metric-value" id="operations-rate">{{ cluster_stats.operations_per_second }}</div>
                <div class="metric-trend">{{ cluster_stats.total_operations }} total</div>
            </div>
        </div>
        
        <div class="nodes-section">
            <h2 class="section-title">🖥️ Cluster Nodes</h2>
            <div class="nodes-grid" id="nodes-container">
                {% for node in nodes %}
                <div class="node-card {{ 'leader' if node.role == 'leader' else 'failed' if node.health == 'failed' else '' }}">
                    <div class="node-header">
                        <div class="node-id">{{ node.node_id[:12] }}</div>
                        <div class="node-status {{ 'status-leader' if node.role == 'leader' else 'status-failed' if node.health == 'failed' else 'status-healthy' }}">
                            {{ node.role.upper() }}
                        </div>
                    </div>
                    <div class="node-details">
                        <div class="detail-item">
                            <span class="detail-label">Host:</span>
                            <span class="detail-value">{{ node.host }}:{{ node.port }}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Health:</span>
                            <span class="detail-value">{{ node.health.title() }}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Uptime:</span>
                            <span class="detail-value">{{ "%.1f"|format(node.uptime) }}h</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Connections:</span>
                            <span class="detail-value">{{ node.connections }}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">RAFT Term:</span>
                            <span class="detail-value">{{ node.raft_term }}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Repl. Lag:</span>
                            <span class="detail-value">{{ node.replication_lag }}ms</span>
                        </div>
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>
        
        <div class="actions-section">
            <h2 class="section-title">⚡ Cluster Actions</h2>
            <div class="action-buttons">
                <button class="btn btn-primary" onclick="refreshDashboard()">🔄 Refresh</button>
                <button class="btn btn-warning" onclick="triggerFailover()">🔄 Manual Failover</button>
                <button class="btn btn-primary" onclick="rebalanceCluster()">⚖️ Rebalance</button>
                <button class="btn btn-danger" onclick="emergencyStop()">🛑 Emergency Stop</button>
            </div>
        </div>
        
        <div class="log-section">
            <h2 class="section-title">📋 Recent Events</h2>
            <div class="log-container" id="log-container">
                {% for log in recent_logs %}
                <div class="log-entry">
                    <span class="log-timestamp">{{ log.timestamp }}</span>
                    <span class="log-level-{{ log.level }}">{{ log.level.upper() }}</span>
                    {{ log.message }}
                </div>
                {% endfor %}
            </div>
        </div>
        
        <div class="refresh-indicator" id="refresh-indicator">
            🔄 Refreshing dashboard...
        </div>
    </div>
    
    <script>
        // Auto-refresh dashboard every 30 seconds
        setInterval(refreshDashboard, 30000);
        
        function refreshDashboard() {
            const indicator = document.getElementById('refresh-indicator');
            indicator.style.display = 'block';
            
            fetch('/api/cluster/status')
                .then(response => response.json())
                .then(data => {
                    updateDashboard(data);
                    indicator.style.display = 'none';
                })
                .catch(error => {
                    console.error('Failed to refresh dashboard:', error);
                    indicator.style.display = 'none';
                });
        }
        
        function updateDashboard(data) {
            // Update metrics
            document.getElementById('total-nodes').textContent = data.cluster_stats.total_nodes;
            document.getElementById('current-leader').textContent = data.cluster_stats.leader_id ? data.cluster_stats.leader_id.substring(0, 8) : 'None';
            document.getElementById('cluster-health').textContent = data.cluster_stats.health_percentage + '%';
            document.getElementById('operations-rate').textContent = data.cluster_stats.operations_per_second;
            
            // Update nodes (simplified - in production, update individual nodes)
            console.log('Dashboard updated with latest data');
        }
        
        function triggerFailover() {
            if (confirm('Are you sure you want to trigger a manual failover?')) {
                fetch('/api/cluster/failover', { method: 'POST' })
                    .then(response => response.json())
                    .then(data => {
                        alert('Failover initiated: ' + data.message);
                        setTimeout(refreshDashboard, 5000);
                    })
                    .catch(error => {
                        alert('Failover failed: ' + error.message);
                    });
            }
        }
        
        function rebalanceCluster() {
            if (confirm('Are you sure you want to rebalance the cluster?')) {
                fetch('/api/cluster/rebalance', { method: 'POST' })
                    .then(response => response.json())
                    .then(data => {
                        alert('Rebalance initiated: ' + data.message);
                        setTimeout(refreshDashboard, 5000);
                    })
                    .catch(error => {
                        alert('Rebalance failed: ' + error.message);
                    });
            }
        }
        
        function emergencyStop() {
            if (confirm('EMERGENCY STOP will halt all cluster operations. Are you absolutely sure?')) {
                if (confirm('This action cannot be undone easily. Please confirm again.')) {
                    fetch('/api/cluster/emergency-stop', { method: 'POST' })
                        .then(response => response.json())
                        .then(data => {
                            alert('Emergency stop initiated: ' + data.message);
                        })
                        .catch(error => {
                            alert('Emergency stop failed: ' + error.message);
                        });
                }
            }
        }
        
        // Initialize dashboard
        document.addEventListener('DOMContentLoaded', function() {
            console.log('HMSSQL Dashboard loaded');
        });
    </script>
</body>
</html>
"""

API_DOCS_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HMSSQL Orchestrator API Documentation</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background: #f8f9fa;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 10px;
            padding: 30px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
        }
        
        h1, h2, h3 {
            color: #333;
        }
        
        .endpoint {
            background: #f8f9fa;
            border-left: 4px solid #007bff;
            padding: 20px;
            margin: 20px 0;
            border-radius: 5px;
        }
        
        .method {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-weight: bold;
            color: white;
            font-size: 0.8em;
        }
        
        .method-get { background: #28a745; }
        .method-post { background: #007bff; }
        .method-put { background: #ffc107; color: #212529; }
        .method-delete { background: #dc3545; }
        
        .code {
            background: #f1f3f4;
            padding: 15px;
            border-radius: 5px;
            font-family: 'Courier New', monospace;
            margin: 10px 0;
            overflow-x: auto;
        }
        
        .response-example {
            background: #e8f5e8;
            border-left: 4px solid #28a745;
            padding: 15px;
            margin: 10px 0;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🔧 HMSSQL Orchestrator API Documentation</h1>
        <p>RESTful API for managing HMSSQL database clusters with RAFT consensus.</p>
        
        <h2>Base URL</h2>
        <div class="code">http://your-orchestrator-host:8080/api</div>
        
        <h2>Authentication</h2>
        <p>Currently, the API uses basic authentication or token-based authentication.</p>
        
        <h2>Endpoints</h2>
        
        <div class="endpoint">
            <h3><span class="method method-get">GET</span> /cluster/status</h3>
            <p>Get comprehensive cluster status including all nodes and their health.</p>
            <div class="response-example">
                <strong>Response Example:</strong>
                <div class="code">{
  "cluster_stats": {
    "total_nodes": 3,
    "healthy_nodes": 3,
    "leader_id": "node-1",
    "leader_term": 5,
    "health_percentage": 100,
    "operations_per_second": 150,
    "total_operations": 12450
  },
  "nodes": [
    {
      "node_id": "node-1",
      "host": "192.168.1.10",
      "port": 9999,
      "role": "leader",
      "health": "healthy",
      "raft_term": 5,
      "uptime": 24.5,
      "connections": 15
    }
  ]
}</div>
            </div>
        </div>
        
        <div class="endpoint">
            <h3><span class="method method-post">POST</span> /cluster/failover</h3>
            <p>Trigger manual failover to elect a new leader.</p>
            <div class="code">POST /api/cluster/failover
{
  "target_node": "node-2"  // Optional: specify target leader
}</div>
        </div>
        
        <div class="endpoint">
            <h3><span class="method method-get">GET</span> /nodes</h3>
            <p>List all nodes in the cluster with detailed information.</p>
        </div>
        
        <div class="endpoint">
            <h3><span class="method method-get">GET</span> /nodes/{node_id}</h3>
            <p>Get detailed information about a specific node.</p>
        </div>
        
        <div class="endpoint">
            <h3><span class="method method-post">POST</span> /nodes/{node_id}/actions/restart</h3>
            <p>Restart a specific node (if supported by the deployment).</p>
        </div>
        
        <div class="endpoint">
            <h3><span class="method method-get">GET</span> /metrics</h3>
            <p>Get Prometheus-style metrics for the cluster.</p>
        </div>
        
        <div class="endpoint">
            <h3><span class="method method-get">GET</span> /health</h3>
            <p>Simple health check endpoint.</p>
            <div class="response-example">
                <div class="code">{"status": "healthy", "timestamp": "2025-06-08T10:30:00Z"}</div>
            </div>
        </div>
        
        <h2>WebSocket Endpoints</h2>
        
        <div class="endpoint">
            <h3><span class="method method-get">WS</span> /ws/cluster-events</h3>
            <p>Real-time cluster events via WebSocket connection.</p>
        </div>
        
        <h2>Error Responses</h2>
        <p>All endpoints return standard HTTP status codes and JSON error messages:</p>
        <div class="code">{
  "error": "Node not found",
  "code": "NODE_NOT_FOUND",
  "timestamp": "2025-06-08T10:30:00Z"
}</div>
        
        <h2>Rate Limiting</h2>
        <p>API requests are rate-limited to 100 requests per minute per IP address.</p>
    </div>
</body>
</html>
"""
