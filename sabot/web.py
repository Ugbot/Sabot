# -*- coding: utf-8 -*-
"""Sabot Web Dashboard - Real-time monitoring and management."""

import asyncio
import json
import logging
from typing import Dict, Any, Optional
from pathlib import Path

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn

from .app import App
from .metrics import get_metrics

logger = logging.getLogger(__name__)


class SabotWebDashboard:
    """Web dashboard for Sabot applications."""

    def __init__(self, app: Optional[App] = None):
        self.app = app
        self.fastapi_app = FastAPI(
            title="Sabot Dashboard",
            description="Real-time monitoring and management for Sabot",
            version="0.1.0"
        )

        # WebSocket connections for real-time updates
        self.websocket_connections = set()

        # Setup routes
        self._setup_routes()

        # Setup static files and templates
        self._setup_static_files()

    def _setup_routes(self):
        """Setup FastAPI routes."""

        @self.fastapi_app.get("/", response_class=HTMLResponse)
        async def dashboard(request: Request):
            """Main dashboard page."""
            return self._get_dashboard_html()

        @self.fastapi_app.get("/api/stats")
        async def get_stats():
            """Get application statistics."""
            if self.app:
                stats = self.app.get_stats()
            else:
                stats = {"status": "No application connected"}

            # Add metrics
            stats["metrics"] = get_metrics().get_metrics_dict()

            return JSONResponse(stats)

        @self.fastapi_app.get("/api/metrics")
        async def get_prometheus_metrics():
            """Get Prometheus metrics."""
            return HTMLResponse(
                get_metrics().get_metrics_text(),
                media_type="text/plain; charset=utf-8"
            )

        @self.fastapi_app.get("/api/health")
        async def health_check():
            """Health check endpoint."""
            return {"status": "healthy", "timestamp": asyncio.get_event_loop().time()}

        @self.fastapi_app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            """WebSocket endpoint for real-time updates."""
            await websocket.accept()
            self.websocket_connections.add(websocket)

            try:
                while True:
                    # Send periodic updates
                    if self.app:
                        stats = self.app.get_stats()
                        stats["metrics"] = get_metrics().get_metrics_dict()

                        await websocket.send_json(stats)

                    await asyncio.sleep(5)  # Update every 5 seconds

            except WebSocketDisconnect:
                self.websocket_connections.remove(websocket)

    def _setup_static_files(self):
        """Setup static files and templates."""
        # For now, we'll embed the HTML/CSS/JS inline
        # In a real implementation, you'd have separate files
        pass

    def _get_dashboard_html(self) -> str:
        """Generate dashboard HTML."""
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sabot Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }}
        .stat-card {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .stat-title {{
            font-size: 14px;
            color: #666;
            margin-bottom: 8px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .stat-value {{
            font-size: 32px;
            font-weight: bold;
            color: #333;
        }}
        .chart-container {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }}
        .agents-list {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .agent-item {{
            padding: 10px;
            border-bottom: 1px solid #eee;
            display: flex;
            justify-content: space-between;
        }}
        .status-indicator {{
            width: 12px;
            height: 12px;
            border-radius: 50%;
            display: inline-block;
            margin-right: 8px;
        }}
        .status-running {{ background: #4CAF50; }}
        .status-stopped {{ background: #f44336; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🎯 Sabot Dashboard</h1>
        <p>High-performance columnar streaming with Arrow + Faust-inspired agents</p>
        <div id="app-status">Connecting...</div>
    </div>

    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-title">Active Agents</div>
            <div class="stat-value" id="active-agents">-</div>
        </div>
        <div class="stat-card">
            <div class="stat-title">Messages Processed</div>
            <div class="stat-value" id="messages-processed">-</div>
        </div>
        <div class="stat-card">
            <div class="stat-title">System CPU</div>
            <div class="stat-value" id="system-cpu">-%</div>
        </div>
        <div class="stat-card">
            <div class="stat-title">Memory Usage</div>
            <div class="stat-value" id="memory-usage">-%</div>
        </div>
    </div>

    <div class="chart-container">
        <h2>📊 Message Processing Rate</h2>
        <canvas id="messageChart" width="400" height="200"></canvas>
    </div>

    <div class="agents-list">
        <h2>🤖 Active Agents</h2>
        <div id="agents-container">
            <p>Loading agents...</p>
        </div>
    </div>

    <script>
        let messageChart;
        let messageData = [];
        let timestamps = [];

        // Initialize chart
        function initChart() {{
            const ctx = document.getElementById('messageChart').getContext('2d');
            messageChart = new Chart(ctx, {{
                type: 'line',
                data: {{
                    labels: timestamps,
                    datasets: [{{
                        label: 'Messages/sec',
                        data: messageData,
                        borderColor: '#667eea',
                        backgroundColor: 'rgba(102, 126, 234, 0.1)',
                        tension: 0.4
                    }}]
                }},
                options: {{
                    responsive: true,
                    scales: {{
                        y: {{
                            beginAtZero: true
                        }}
                    }}
                }}
            }});
        }}

        // Update dashboard with data
        function updateDashboard(data) {{
            document.getElementById('app-status').textContent = `Connected to ${data.app_id || 'Sabot App'}`;
            document.getElementById('active-agents').textContent = data.agents || 0;
            document.getElementById('messages-processed').textContent = data.agent_details?.total_messages_processed || 0;

            if (data.metrics) {{
                document.getElementById('system-cpu').textContent = `${data.metrics.system_cpu_percent?.toFixed(1) || 0}%`;
                document.getElementById('memory-usage').textContent = `${data.metrics.system_memory_percent?.toFixed(1) || 0}%`;
            }}

            // Update message rate chart
            const now = new Date().toLocaleTimeString();
            timestamps.push(now);
            messageData.push(data.agent_details?.total_messages_processed || 0);

            if (timestamps.length > 20) {{
                timestamps.shift();
                messageData.shift();
            }}

            if (messageChart) {{
                messageChart.update();
            }}

            // Update agents list
            updateAgentsList(data.agent_details || {{}});
        }}

        function updateAgentsList(agentDetails) {{
            const container = document.getElementById('agents-container');
            let html = '';

            if (Object.keys(agentDetails).length === 0) {{
                html = '<p>No active agents</p>';
            }} else {{
                for (const [agentName, stats] of Object.entries(agentDetails)) {{
                    const statusClass = stats.running ? 'status-running' : 'status-stopped';
                    html += `
                        <div class="agent-item">
                            <div>
                                <span class="status-indicator ${statusClass}"></span>
                                <strong>${agentName}</strong>
                            </div>
                            <div>
                                Messages: ${stats.messages_processed || 0} |
                                Errors: ${stats.errors || 0}
                            </div>
                        </div>
                    `;
                }}
            }}

            container.innerHTML = html;
        }}

        // WebSocket connection
        let ws;
        function connectWebSocket() {{
            ws = new WebSocket(`ws://${window.location.host}/ws`);

            ws.onopen = function(event) {{
                console.log('Connected to Sabot dashboard');
            }};

            ws.onmessage = function(event) {{
                const data = JSON.parse(event.data);
                updateDashboard(data);
            }};

            ws.onclose = function(event) {{
                console.log('Disconnected from Sabot dashboard');
                setTimeout(connectWebSocket, 5000); // Reconnect after 5 seconds
            }};

            ws.onerror = function(error) {{
                console.error('WebSocket error:', error);
            }};
        }}

        // Initialize
        document.addEventListener('DOMContentLoaded', function() {{
            initChart();
            connectWebSocket();

            // Initial data fetch
            fetch('/api/stats')
                .then(response => response.json())
                .then(updateDashboard)
                .catch(console.error);
        }});
    </script>
</body>
</html>
        """
        return html


def start_web_server(host: str = "localhost", port: int = 8080, app: Optional[App] = None):
    """Start the web dashboard server."""
    dashboard = SabotWebDashboard(app)

    logger.info(f"Starting Sabot web dashboard on http://{host}:{port}")

    uvicorn.run(
        dashboard.fastapi_app,
        host=host,
        port=port,
        log_level="info"
    )
