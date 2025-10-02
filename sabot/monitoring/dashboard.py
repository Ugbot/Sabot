#!/usr/bin/env python3
"""
Monitoring Dashboard for Sabot

Provides a comprehensive web-based monitoring interface with:
- Real-time metrics visualization
- Health status overview
- Alert management
- Component status monitoring
- Interactive charts and graphs
"""

import asyncio
import json
import logging
from typing import Dict, List, Any, Optional, Callable
from pathlib import Path
from dataclasses import dataclass

try:
    from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException
    from fastapi.responses import HTMLResponse, JSONResponse
    from fastapi.staticfiles import StaticFiles
    from fastapi.templating import Jinja2Templates
    import uvicorn
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False
    FastAPI = None
    WebSocket = None

from .collector import MetricsCollector
from .health import HealthChecker
from .alerts import AlertManager

logger = logging.getLogger(__name__)


@dataclass
class DashboardConfig:
    """Configuration for the monitoring dashboard."""
    host: str = "0.0.0.0"
    port: int = 8080
    title: str = "Sabot Monitoring Dashboard"
    refresh_interval: float = 5.0  # seconds
    enable_websocket: bool = True
    static_dir: Optional[Path] = None


class MonitoringDashboard:
    """
    Web-based monitoring dashboard for Sabot applications.

    Provides real-time monitoring, health checks, and alert management
    through an interactive web interface.
    """

    def __init__(self, config: DashboardConfig,
                 metrics_collector: MetricsCollector,
                 health_checker: HealthChecker,
                 alert_manager: AlertManager):
        self.config = config
        self.metrics = metrics_collector
        self.health = health_checker
        self.alerts = alert_manager

        if not FASTAPI_AVAILABLE:
            raise RuntimeError("FastAPI is required for the monitoring dashboard. Install with: pip install fastapi uvicorn")

        # FastAPI app
        self.app = FastAPI(title=config.title)
        self.templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

        # WebSocket connections
        self.websocket_connections: List[WebSocket] = []

        # Setup routes
        self._setup_routes()

        # Setup static files
        self._setup_static_files()

        # Background tasks
        self._running = False
        self._broadcast_task: Optional[asyncio.Task] = None

    def _setup_routes(self):
        """Setup FastAPI routes."""

        @self.app.get("/", response_class=HTMLResponse)
        async def dashboard(request: Request):
            """Main dashboard page."""
            return self.templates.TemplateResponse("dashboard.html", {
                "request": request,
                "title": self.config.title,
                "refresh_interval": int(self.config.refresh_interval * 1000)
            })

        @self.app.get("/api/health")
        async def get_health():
            """Get system health status."""
            return JSONResponse(self.health.get_overall_health())

        @self.app.get("/api/metrics")
        async def get_metrics():
            """Get all metrics data."""
            return JSONResponse(self.metrics.get_all_metrics())

        @self.app.get("/api/alerts")
        async def get_alerts():
            """Get alert summary."""
            return JSONResponse(self.alerts.get_alert_summary())

        @self.app.get("/api/components")
        async def get_components():
            """Get component status."""
            return JSONResponse(self._get_component_status())

        @self.app.get("/api/prometheus")
        async def get_prometheus_metrics():
            """Get metrics in Prometheus format."""
            prometheus_data = self.metrics.get_prometheus_metrics()
            return prometheus_data.decode('utf-8') if prometheus_data else ""

        @self.app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            """WebSocket endpoint for real-time updates."""
            await websocket.accept()
            self.websocket_connections.append(websocket)

            try:
                while True:
                    # Keep connection alive and wait for client messages
                    data = await websocket.receive_text()
                    # Echo back for keepalive
                    await websocket.send_text("pong")

            except WebSocketDisconnect:
                if websocket in self.websocket_connections:
                    self.websocket_connections.remove(websocket)

    def _setup_static_files(self):
        """Setup static file serving."""
        static_path = self.config.static_dir or (Path(__file__).parent / "static")
        if static_path.exists():
            self.app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

    def _get_component_status(self) -> Dict[str, Any]:
        """Get status of all system components."""
        return {
            "stream_engine": self._get_stream_engine_status(),
            "state_manager": self._get_state_manager_status(),
            "agent_manager": self._get_agent_manager_status(),
            "metrics_collector": self._get_metrics_status(),
            "timestamp": asyncio.get_event_loop().time()
        }

    def _get_stream_engine_status(self) -> Dict[str, Any]:
        """Get stream engine status."""
        # This would need access to the actual stream engine instance
        return {
            "status": "unknown",
            "active_streams": 0,
            "message_rate": 0,
            "error_rate": 0
        }

    def _get_state_manager_status(self) -> Dict[str, Any]:
        """Get state manager status."""
        # This would need access to the actual state manager instance
        return {
            "status": "unknown",
            "active_tables": 0,
            "total_operations": 0
        }

    def _get_agent_manager_status(self) -> Dict[str, Any]:
        """Get agent manager status."""
        # This would need access to the actual agent manager instance
        return {
            "status": "unknown",
            "active_agents": 0,
            "failed_agents": 0
        }

    def _get_metrics_status(self) -> Dict[str, Any]:
        """Get metrics collector status."""
        return {
            "status": "healthy" if self.metrics._running else "stopped",
            "counters": len(self.metrics._counters),
            "gauges": len(self.metrics._gauges),
            "histograms": len(self.metrics._histograms)
        }

    async def broadcast_update(self) -> None:
        """Broadcast real-time updates to all connected WebSocket clients."""
        if not self.websocket_connections:
            return

        try:
            # Collect current data
            update_data = {
                "type": "update",
                "timestamp": asyncio.get_event_loop().time(),
                "health": self.health.get_overall_health(),
                "alerts": self.alerts.get_alert_summary(),
                "metrics": {
                    "counters": {
                        name: ts.calculate_stats()
                        for name, ts in self.metrics._counters.items()
                    },
                    "gauges": {
                        name: {
                            **ts.calculate_stats(),
                            "current": ts.get_recent_samples(60)[-1].value
                            if ts.get_recent_samples(60) else 0
                        }
                        for name, ts in self.metrics._gauges.items()
                    }
                }
            }

            # Send to all connected clients
            dead_connections = []
            for websocket in self.websocket_connections:
                try:
                    await websocket.send_json(update_data)
                except Exception:
                    dead_connections.append(websocket)

            # Clean up dead connections
            for websocket in dead_connections:
                if websocket in self.websocket_connections:
                    self.websocket_connections.remove(websocket)

        except Exception as e:
            logger.error(f"Failed to broadcast update: {e}")

    def start_broadcasting(self) -> None:
        """Start broadcasting real-time updates."""
        if not self._running:
            self._running = True
            self._broadcast_task = asyncio.create_task(self._broadcast_loop())

    def stop_broadcasting(self) -> None:
        """Stop broadcasting real-time updates."""
        self._running = False
        if self._broadcast_task:
            self._broadcast_task.cancel()

    async def _broadcast_loop(self) -> None:
        """Broadcast loop for real-time updates."""
        while self._running:
            try:
                await self.broadcast_update()
                await asyncio.sleep(self.config.refresh_interval)
            except Exception as e:
                logger.error(f"Error in broadcast loop: {e}")
                await asyncio.sleep(5)

    def run(self) -> None:
        """Run the dashboard server."""
        logger.info(f"Starting monitoring dashboard on {self.config.host}:{self.config.port}")

        # Start broadcasting
        self.start_broadcasting()

        try:
            uvicorn.run(
                self.app,
                host=self.config.host,
                port=self.config.port,
                log_level="info"
            )
        finally:
            self.stop_broadcasting()


# HTML Template (would normally be in templates/dashboard.html)
DASHBOARD_HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ title }}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .status-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 20px; }
        .status-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .status-healthy { border-left: 4px solid #4CAF50; }
        .status-degraded { border-left: 4px solid #FF9800; }
        .status-unhealthy { border-left: 4px solid #F44336; }
        .metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 15px; }
        .metric-card { background: white; padding: 15px; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
        .alert-list { background: white; padding: 20px; border-radius: 8px; margin-top: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .alert-critical { background: #ffebee; border-left: 4px solid #F44336; }
        .alert-error { background: #fff3e0; border-left: 4px solid #FF9800; }
        .alert-warning { background: #fffde7; border-left: 4px solid #FFC107; }
        .chart-container { height: 300px; margin: 20px 0; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{{ title }}</h1>
            <div id="overall-health">Loading...</div>
        </div>

        <div class="status-grid">
            <div class="status-card" id="stream-engine-status">
                <h3>Stream Engine</h3>
                <div id="stream-engine-content">Loading...</div>
            </div>

            <div class="status-card" id="state-manager-status">
                <h3>State Manager</h3>
                <div id="state-manager-content">Loading...</div>
            </div>

            <div class="status-card" id="agent-manager-status">
                <h3>Agent Manager</h3>
                <div id="agent-manager-content">Loading...</div>
            </div>

            <div class="status-card" id="metrics-status">
                <h3>Metrics Collector</h3>
                <div id="metrics-content">Loading...</div>
            </div>
        </div>

        <div class="metric-grid">
            <div class="metric-card">
                <h4>Messages Processed</h4>
                <canvas id="messages-chart"></canvas>
            </div>

            <div class="metric-card">
                <h4>Memory Usage</h4>
                <canvas id="memory-chart"></canvas>
            </div>

            <div class="metric-card">
                <h4>Active Streams</h4>
                <canvas id="streams-chart"></canvas>
            </div>

            <div class="metric-card">
                <h4>Error Rate</h4>
                <canvas id="errors-chart"></canvas>
            </div>
        </div>

        <div class="alert-list">
            <h3>Active Alerts</h3>
            <div id="alerts-content">Loading...</div>
        </div>
    </div>

    <script>
        let ws = null;
        let charts = {};

        function connectWebSocket() {
            ws = new WebSocket(`ws://${window.location.host}/ws`);

            ws.onmessage = function(event) {
                const data = JSON.parse(event.data);
                updateDashboard(data);
            };

            ws.onclose = function() {
                setTimeout(connectWebSocket, 5000);
            };

            ws.onerror = function(error) {
                console.error('WebSocket error:', error);
            };
        }

        function updateDashboard(data) {
            // Update overall health
            const health = data.health;
            document.getElementById('overall-health').innerHTML =
                `<strong>Status:</strong> ${health.status.toUpperCase()} - ${health.message}`;

            // Update component statuses
            updateComponentStatus('stream-engine', data.health?.checks?.stream_engine);
            updateComponentStatus('state-manager', data.health?.checks?.state_manager);
            updateComponentStatus('agent-manager', data.health?.checks?.agent_manager);
            updateComponentStatus('metrics', data.health?.checks?.metrics_collector);

            // Update alerts
            updateAlerts(data.alerts);

            // Update charts
            updateCharts(data.metrics);
        }

        function updateComponentStatus(componentId, status) {
            const element = document.getElementById(`${componentId}-content`);
            if (!element) return;

            const statusClass = status?.status === 'healthy' ? 'status-healthy' :
                              status?.status === 'degraded' ? 'status-degraded' : 'status-unhealthy';

            element.innerHTML = `
                <div class="${statusClass}">
                    <strong>Status:</strong> ${status?.status || 'unknown'}<br>
                    <strong>Duration:</strong> ${status?.duration?.toFixed(3) || 'N/A'}s
                </div>
            `;
        }

        function updateAlerts(alerts) {
            const element = document.getElementById('alerts-content');
            if (!alerts?.most_recent_alerts?.length) {
                element.innerHTML = '<p>No active alerts</p>';
                return;
            }

            const alertHtml = alerts.most_recent_alerts.map(alert => `
                <div class="alert alert-${alert.severity}">
                    <strong>${alert.severity.toUpperCase()}:</strong> ${alert.message}<br>
                    <small>${new Date(alert.timestamp * 1000).toLocaleString()}</small>
                </div>
            `).join('');

            element.innerHTML = alertHtml;
        }

        function updateCharts(metrics) {
            // Simple chart updates - in a real implementation you'd use Chart.js
            // This is a placeholder for the actual chart implementation
        }

        // Initial data load
        async function loadInitialData() {
            try {
                const [healthRes, metricsRes, alertsRes] = await Promise.all([
                    fetch('/api/health'),
                    fetch('/api/metrics'),
                    fetch('/api/alerts')
                ]);

                const health = await healthRes.json();
                const metrics = await metricsRes.json();
                const alerts = await alertsRes.json();

                updateDashboard({ health, metrics, alerts });
            } catch (error) {
                console.error('Failed to load initial data:', error);
            }
        }

        // Initialize
        document.addEventListener('DOMContentLoaded', function() {
            loadInitialData();
            connectWebSocket();
        });

        // Refresh every {{ refresh_interval }}ms as fallback
        setInterval(loadInitialData, {{ refresh_interval }});
    </script>
</body>
</html>
"""

# Template directory creation helper
def create_template_directory(base_path: Path = None) -> Path:
    """Create template directory and files."""
    if base_path is None:
        base_path = Path(__file__).parent

    template_dir = base_path / "templates"
    template_dir.mkdir(exist_ok=True)

    # Write dashboard template
    dashboard_file = template_dir / "dashboard.html"
    with open(dashboard_file, 'w') as f:
        f.write(DASHBOARD_HTML_TEMPLATE)

    return template_dir


def create_static_directory(base_path: Path = None) -> Path:
    """Create static directory for CSS/JS files."""
    if base_path is None:
        base_path = Path(__file__).parent

    static_dir = base_path / "static"
    static_dir.mkdir(exist_ok=True)

    # Create basic CSS file
    css_file = static_dir / "dashboard.css"
    with open(css_file, 'w') as f:
        f.write("""
body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
.container { max-width: 1200px; margin: 0 auto; }
.header { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
.status-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 20px; }
.status-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
.status-healthy { border-left: 4px solid #4CAF50; }
.status-degraded { border-left: 4px solid #FF9800; }
.status-unhealthy { border-left: 4px solid #F44336; }
.metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 15px; }
.metric-card { background: white; padding: 15px; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
.alert-list { background: white; padding: 20px; border-radius: 8px; margin-top: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
.alert-critical { background: #ffebee; border-left: 4px solid #F44336; }
.alert-error { background: #fff3e0; border-left: 4px solid #FF9800; }
.alert-warning { background: #fffde7; border-left: 4px solid #FFC107; }
.chart-container { height: 300px; margin: 20px 0; }
""")

    return static_dir
