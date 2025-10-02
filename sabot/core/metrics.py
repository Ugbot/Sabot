#!/usr/bin/env python3
"""
Metrics Collection and Monitoring for Sabot

Provides comprehensive metrics collection for:
- Throughput monitoring
- Latency tracking
- Error rates
- Resource usage
- Health checks
- Prometheus integration
"""

import time
import logging
import asyncio
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict, deque
import threading
import statistics

try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, generate_latest
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    # Mock prometheus classes
    class MockMetric:
        def inc(self, value=1): pass
        def set(self, value): pass
        def observe(self, value): pass
        def labels(self, **kwargs): return self

    Counter = Gauge = Histogram = MockMetric
    CollectorRegistry = object
    generate_latest = lambda x: b""

logger = logging.getLogger(__name__)


@dataclass
class MetricPoint:
    """Individual metric measurement point."""
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class MetricSeries:
    """Time series data for a metric."""
    name: str
    points: deque = field(default_factory=lambda: deque(maxlen=1000))
    aggregation: str = "latest"  # latest, sum, avg, count, rate

    def add_point(self, value: float, labels: Dict[str, str] = None) -> None:
        """Add a measurement point."""
        point = MetricPoint(
            timestamp=time.time(),
            value=value,
            labels=labels or {}
        )
        self.points.append(point)

    def get_latest(self) -> Optional[float]:
        """Get the latest value."""
        return self.points[-1].value if self.points else None

    def get_average(self, window_seconds: float = 60.0) -> Optional[float]:
        """Get average over time window."""
        if not self.points:
            return None

        cutoff = time.time() - window_seconds
        recent_points = [p.value for p in self.points if p.timestamp >= cutoff]

        return statistics.mean(recent_points) if recent_points else None

    def get_rate(self, window_seconds: float = 60.0) -> Optional[float]:
        """Get rate of change over time window."""
        if len(self.points) < 2:
            return None

        cutoff = time.time() - window_seconds
        recent_points = [p for p in self.points if p.timestamp >= cutoff]

        if len(recent_points) < 2:
            return None

        # Calculate rate as (last - first) / time_delta
        time_delta = recent_points[-1].timestamp - recent_points[0].timestamp
        value_delta = recent_points[-1].value - recent_points[0].value

        return value_delta / time_delta if time_delta > 0 else 0


class MetricsCollector:
    """
    Comprehensive metrics collection system for Sabot.

    Tracks:
    - Message throughput and latency
    - Error rates and types
    - Resource usage (CPU, memory)
    - Stream processing statistics
    - Agent performance metrics
    - System health indicators
    """

    def __init__(self, enable_prometheus: bool = True):
        """
        Initialize metrics collector.

        Args:
            enable_prometheus: Whether to enable Prometheus metrics export
        """
        self.enable_prometheus = enable_prometheus and PROMETHEUS_AVAILABLE

        # In-memory metrics storage
        self._metrics: Dict[str, MetricSeries] = {}
        self._lock = threading.Lock()

        # Prometheus registry and metrics
        self.registry = CollectorRegistry() if self.enable_prometheus else None

        if self.enable_prometheus:
            self._setup_prometheus_metrics()
        else:
            logger.warning("Prometheus not available, using in-memory metrics only")

        # Health check data
        self._health_checks: Dict[str, Dict[str, Any]] = {}
        self._last_health_check = 0.0

        logger.info(f"MetricsCollector initialized (Prometheus: {self.enable_prometheus})")

    def _setup_prometheus_metrics(self):
        """Set up Prometheus metric definitions."""
        # Message processing metrics
        self.messages_processed = Counter(
            'sabot_messages_processed_total',
            'Total number of messages processed',
            ['stream_id', 'agent_id'],
            registry=self.registry
        )

        self.message_processing_time = Histogram(
            'sabot_message_processing_duration_seconds',
            'Time spent processing messages',
            ['stream_id', 'agent_id'],
            registry=self.registry
        )

        # Throughput metrics
        self.throughput = Gauge(
            'sabot_throughput_messages_per_second',
            'Current message throughput',
            ['stream_id'],
            registry=self.registry
        )

        # Error metrics
        self.errors_total = Counter(
            'sabot_errors_total',
            'Total number of errors',
            ['stream_id', 'agent_id', 'error_type'],
            registry=self.registry
        )

        # Resource usage metrics
        self.memory_usage = Gauge(
            'sabot_memory_usage_bytes',
            'Current memory usage',
            ['component'],
            registry=self.registry
        )

        self.cpu_usage = Gauge(
            'sabot_cpu_usage_percent',
            'Current CPU usage',
            ['component'],
            registry=self.registry
        )

        # Agent metrics
        self.active_agents = Gauge(
            'sabot_active_agents',
            'Number of active agents',
            ['agent_type'],
            registry=self.registry
        )

        # Health check metrics
        self.health_status = Gauge(
            'sabot_health_status',
            'Health status (1=healthy, 0=unhealthy)',
            ['component'],
            registry=self.registry
        )

    async def record_message_processed(self, stream_id: str, agent_id: str = "",
                                     message_count: int = 1) -> None:
        """
        Record message processing.

        Args:
            stream_id: Stream identifier
            agent_id: Agent identifier (optional)
            message_count: Number of messages processed
        """
        metric_key = f"messages_processed_{stream_id}_{agent_id}"

        with self._lock:
            if metric_key not in self._metrics:
                self._metrics[metric_key] = MetricSeries(metric_key, aggregation="sum")
            self._metrics[metric_key].add_point(message_count)

        if self.enable_prometheus:
            self.messages_processed.labels(
                stream_id=stream_id, agent_id=agent_id
            ).inc(message_count)

    async def record_processing_time(self, stream_id: str, duration_ms: float,
                                   agent_id: str = "") -> None:
        """
        Record message processing time.

        Args:
            stream_id: Stream identifier
            duration_ms: Processing duration in milliseconds
            agent_id: Agent identifier (optional)
        """
        metric_key = f"processing_time_{stream_id}_{agent_id}"
        duration_seconds = duration_ms / 1000.0

        with self._lock:
            if metric_key not in self._metrics:
                self._metrics[metric_key] = MetricSeries(metric_key, aggregation="avg")
            self._metrics[metric_key].add_point(duration_seconds)

        if self.enable_prometheus:
            self.message_processing_time.labels(
                stream_id=stream_id, agent_id=agent_id
            ).observe(duration_seconds)

    async def record_throughput(self, stream_id: str, messages_per_second: float) -> None:
        """
        Record current throughput.

        Args:
            stream_id: Stream identifier
            messages_per_second: Current throughput
        """
        metric_key = f"throughput_{stream_id}"

        with self._lock:
            if metric_key not in self._metrics:
                self._metrics[metric_key] = MetricSeries(metric_key, aggregation="latest")
            self._metrics[metric_key].add_point(messages_per_second)

        if self.enable_prometheus:
            self.throughput.labels(stream_id=stream_id).set(messages_per_second)

    async def record_error(self, stream_id: str, error_message: str,
                          agent_id: str = "", error_type: str = "unknown") -> None:
        """
        Record an error.

        Args:
            stream_id: Stream identifier
            error_message: Error message
            agent_id: Agent identifier (optional)
            error_type: Type of error
        """
        metric_key = f"errors_{stream_id}_{agent_id}_{error_type}"

        with self._lock:
            if metric_key not in self._metrics:
                self._metrics[metric_key] = MetricSeries(metric_key, aggregation="count")
            self._metrics[metric_key].add_point(1)

        if self.enable_prometheus:
            self.errors_total.labels(
                stream_id=stream_id,
                agent_id=agent_id,
                error_type=error_type
            ).inc()

        logger.warning(f"Error recorded: {error_message} (stream: {stream_id}, agent: {agent_id})")

    async def record_backpressure_event(self, stream_id: str) -> None:
        """
        Record a backpressure event.

        Args:
            stream_id: Stream identifier
        """
        metric_key = f"backpressure_{stream_id}"

        with self._lock:
            if metric_key not in self._metrics:
                self._metrics[metric_key] = MetricSeries(metric_key, aggregation="count")
            self._metrics[metric_key].add_point(1)

    async def record_resource_usage(self, component: str, memory_bytes: int,
                                  cpu_percent: float) -> None:
        """
        Record resource usage.

        Args:
            component: Component name (stream, agent, etc.)
            memory_bytes: Memory usage in bytes
            cpu_percent: CPU usage percentage
        """
        mem_key = f"memory_{component}"
        cpu_key = f"cpu_{component}"

        with self._lock:
            # Memory
            if mem_key not in self._metrics:
                self._metrics[mem_key] = MetricSeries(mem_key, aggregation="latest")
            self._metrics[mem_key].add_point(memory_bytes)

            # CPU
            if cpu_key not in self._metrics:
                self._metrics[cpu_key] = MetricSeries(cpu_key, aggregation="latest")
            self._metrics[cpu_key].add_point(cpu_percent)

        if self.enable_prometheus:
            self.memory_usage.labels(component=component).set(memory_bytes)
            self.cpu_usage.labels(component=component).set(cpu_percent)

    async def record_agent_status(self, agent_id: str, status: str,
                                agent_type: str = "stream") -> None:
        """
        Record agent status.

        Args:
            agent_id: Agent identifier
            status: Agent status (running, stopped, failed)
            agent_type: Type of agent
        """
        status_value = {"running": 1, "stopped": 0, "failed": -1}.get(status, 0)

        metric_key = f"agent_status_{agent_id}"

        with self._lock:
            if metric_key not in self._metrics:
                self._metrics[metric_key] = MetricSeries(metric_key, aggregation="latest")
            self._metrics[metric_key].add_point(status_value)

        if self.enable_prometheus:
            # For health status, use 1 for healthy (running), 0 for unhealthy
            health_value = 1 if status == "running" else 0
            self.health_status.labels(component=f"agent_{agent_id}").set(health_value)

    def get_metric_value(self, metric_name: str, aggregation: str = None) -> Optional[float]:
        """
        Get current value for a metric.

        Args:
            metric_name: Metric name
            aggregation: Aggregation method (latest, avg, rate)

        Returns:
            Metric value or None if not found
        """
        with self._lock:
            if metric_name not in self._metrics:
                return None

            series = self._metrics[metric_name]
            agg_method = aggregation or series.aggregation

            if agg_method == "latest":
                return series.get_latest()
            elif agg_method == "avg":
                return series.get_average()
            elif agg_method == "rate":
                return series.get_rate()
            else:
                return series.get_latest()

    def get_all_metrics(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all current metric values.

        Returns:
            Dictionary of metric names to their current values and metadata
        """
        result = {}

        with self._lock:
            for name, series in self._metrics.items():
                latest = series.get_latest()
                avg_1m = series.get_average(60.0)
                rate_1m = series.get_rate(60.0)

                result[name] = {
                    "latest": latest,
                    "avg_1m": avg_1m,
                    "rate_1m": rate_1m,
                    "count": len(series.points),
                    "aggregation": series.aggregation
                }

        return result

    def get_prometheus_metrics(self) -> bytes:
        """
        Get Prometheus-formatted metrics.

        Returns:
            Prometheus metrics as bytes
        """
        if not self.enable_prometheus:
            return b"# Prometheus metrics not available\n"

        return generate_latest(self.registry)

    async def perform_health_check(self) -> Dict[str, Any]:
        """
        Perform comprehensive health check.

        Returns:
            Health check results
        """
        current_time = time.time()

        # Don't perform health checks too frequently
        if current_time - self._last_health_check < 10.0:  # Max once per 10 seconds
            return self._health_checks.get("overall", {"status": "unknown"})

        try:
            # Check system resources
            import psutil
            process = psutil.Process()

            memory_percent = process.memory_percent()
            cpu_percent = process.cpu_percent(interval=0.1)

            # Check for critical issues
            issues = []

            if memory_percent > 90:
                issues.append("High memory usage")
            if cpu_percent > 95:
                issues.append("High CPU usage")

            # Check metric health
            metrics_healthy = len(self._metrics) > 0

            if not metrics_healthy:
                issues.append("No metrics being collected")

            # Overall health status
            if issues:
                status = "degraded"
                message = f"Health check found issues: {', '.join(issues)}"
            else:
                status = "healthy"
                message = "All systems operational"

            health_result = {
                "status": status,
                "message": message,
                "timestamp": current_time,
                "memory_percent": memory_percent,
                "cpu_percent": cpu_percent,
                "metrics_count": len(self._metrics),
                "issues": issues
            }

            self._health_checks["overall"] = health_result
            self._last_health_check = current_time

            # Update Prometheus health metric
            if self.enable_prometheus:
                health_value = 1 if status == "healthy" else 0
                self.health_status.labels(component="system").set(health_value)

            return health_result

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "error",
                "message": f"Health check failed: {e}",
                "timestamp": current_time
            }

    def reset_metrics(self, pattern: str = None) -> int:
        """
        Reset metrics matching a pattern.

        Args:
            pattern: Pattern to match (None for all metrics)

        Returns:
            Number of metrics reset
        """
        with self._lock:
            if pattern is None:
                count = len(self._metrics)
                self._metrics.clear()
                return count
            else:
                to_remove = [name for name in self._metrics.keys() if pattern in name]
                for name in to_remove:
                    del self._metrics[name]
                return len(to_remove)

    def export_metrics_json(self) -> str:
        """
        Export all metrics as JSON string.

        Returns:
            JSON representation of all metrics
        """
        import json
        metrics_data = self.get_all_metrics()
        return json.dumps(metrics_data, indent=2, default=str)
