# -*- coding: utf-8 -*-
"""Sabot Metrics - Monitoring and observability using FastRedis."""

import asyncio
import time
from typing import Dict, Any, Optional
import threading

import psutil
import logging

logger = logging.getLogger(__name__)

# Prometheus imports (optional)
try:
    from prometheus_client import (
        Counter, Gauge, Histogram, Summary, CollectorRegistry,
        generate_latest, CONTENT_TYPE_LATEST
    )
    from prometheus_client.exposition import make_wsgi_app
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    # Mock prometheus classes for when not available
    class MockMetric:
        def inc(self, value=1): pass
        def set(self, value): pass
        def observe(self, value): pass
        def labels(self, **kwargs): return self

    Counter = Gauge = Histogram = Summary = MockMetric
    CollectorRegistry = object
    generate_latest = lambda x: b""
    CONTENT_TYPE_LATEST = "text/plain"
    make_wsgi_app = lambda: None

# FastRedis imports
try:
    from fastredis import ObservabilityManager
    FASTREDIS_AVAILABLE = True
except ImportError:
    logger.warning("FastRedis not available, falling back to basic Prometheus metrics")
    FASTREDIS_AVAILABLE = False


class SabotMetrics:
    """Comprehensive metrics collection for Sabot using FastRedis observability."""

    def __init__(self, app_id: str = "sabot", redis_client=None):
        self.app_id = app_id
        self.redis_client = redis_client
        self.registry = CollectorRegistry()

        # Initialize FastRedis observability if available
        if FASTREDIS_AVAILABLE and redis_client:
            self.observability = ObservabilityManager(redis_client, metrics_prefix="sabot")
        else:
            self.observability = None

        # Core Prometheus metrics (fallback/compatibility)
        self.messages_processed = Counter(
            'sabot_messages_processed_total',
            'Total number of messages processed',
            ['agent', 'topic'],
            registry=self.registry
        )

        self.message_processing_time = Histogram(
            'sabot_message_processing_duration_seconds',
            'Time spent processing messages',
            ['agent', 'topic'],
            registry=self.registry
        )

        self.agent_errors = Counter(
            'sabot_agent_errors_total',
            'Total number of agent errors',
            ['agent', 'error_type'],
            registry=self.registry
        )

        self.active_agents = Gauge(
            'sabot_active_agents',
            'Number of currently active agents',
            registry=self.registry
        )

        # Arrow-specific metrics
        self.arrow_operations = Counter(
            'sabot_arrow_operations_total',
            'Total number of Arrow operations',
            ['operation_type'],
            registry=self.registry
        )

        self.arrow_operation_time = Histogram(
            'sabot_arrow_operation_duration_seconds',
            'Time spent in Arrow operations',
            ['operation_type'],
            registry=self.registry
        )

        self.arrow_memory_usage = Gauge(
            'sabot_arrow_memory_bytes',
            'Arrow memory usage in bytes',
            registry=self.registry
        )

        # System metrics
        self.system_cpu_percent = Gauge(
            'sabot_system_cpu_percent',
            'System CPU usage percentage',
            registry=self.registry
        )

        self.system_memory_percent = Gauge(
            'sabot_system_memory_percent',
            'System memory usage percentage',
            registry=self.registry
        )

        self.system_network_bytes = Counter(
            'sabot_system_network_bytes_total',
            'Total network bytes transferred',
            ['direction'],
            registry=self.registry
        )

        # Pipeline metrics
        self.pipeline_operations = Counter(
            'sabot_pipeline_operations_total',
            'Total pipeline operations executed',
            ['pipeline_name', 'operation'],
            registry=self.registry
        )

        self.pipeline_execution_time = Histogram(
            'sabot_pipeline_execution_duration_seconds',
            'Pipeline execution time',
            ['pipeline_name'],
            registry=self.registry
        )

        # Flight metrics (if enabled)
        self.flight_requests = Counter(
            'sabot_flight_requests_total',
            'Total Arrow Flight requests',
            ['method', 'status'],
            registry=self.registry
        )

        self.flight_request_time = Histogram(
            'sabot_flight_request_duration_seconds',
            'Arrow Flight request duration',
            ['method'],
            registry=self.registry
        )

        # Start system metrics collection
        self._start_system_metrics()

    def _start_system_metrics(self):
        """Start background system metrics collection."""
        def collect_system_metrics():
            while True:
                try:
                    # CPU usage
                    self.system_cpu_percent.set(psutil.cpu_percent(interval=1))

                    # Memory usage
                    memory = psutil.virtual_memory()
                    self.system_memory_percent.set(memory.percent)

                    # Network I/O
                    net = psutil.net_io_counters()
                    self.system_network_bytes.labels(direction='sent')._value_set(net.bytes_sent)
                    self.system_network_bytes.labels(direction='recv')._value_set(net.bytes_recv)

                    time.sleep(5)  # Update every 5 seconds

                except Exception as e:
                    logger.error(f"Error collecting system metrics: {e}")
                    time.sleep(5)

        thread = threading.Thread(target=collect_system_metrics, daemon=True)
        thread.start()

    def record_message_processed(self, agent: str, topic: str, processing_time: float = None):
        """Record a processed message."""
        self.messages_processed.labels(agent=agent, topic=topic).inc()

        # Record in FastRedis observability if available
        if self.observability:
            self.observability.record_operation(
                f"agent_{agent}",
                processing_time * 1000 if processing_time else 1.0,  # Convert to ms
                success=True,
                metadata={"topic": topic, "operation": "message_processed"}
            )

        if processing_time is not None:
            self.message_processing_time.labels(agent=agent, topic=topic).observe(processing_time)

    def record_agent_error(self, agent: str, error_type: str = "unknown"):
        """Record an agent error."""
        self.agent_errors.labels(agent=agent, error_type=error_type).inc()

        # Record in FastRedis observability
        if self.observability:
            self.observability.record_operation(
                f"agent_{agent}",
                0.0,  # Error duration
                success=False,
                metadata={"error_type": error_type, "operation": "agent_error"}
            )

    def set_active_agents(self, count: int):
        """Set the number of active agents."""
        self.active_agents.set(count)

    def record_arrow_operation(self, operation_type: str, duration: float = None):
        """Record an Arrow operation."""
        self.arrow_operations.labels(operation_type=operation_type).inc()

        # Record in FastRedis observability
        if self.observability:
            self.observability.record_operation(
                f"arrow_{operation_type}",
                duration * 1000 if duration else 1.0,  # Convert to ms
                success=True,
                metadata={"operation": operation_type, "type": "arrow"}
            )

        if duration is not None:
            self.arrow_operation_time.labels(operation_type=operation_type).observe(duration)

    def set_arrow_memory_usage(self, bytes_used: int):
        """Set Arrow memory usage."""
        self.arrow_memory_usage.set(bytes_used)

    def record_pipeline_operation(self, pipeline_name: str, operation: str):
        """Record a pipeline operation."""
        self.pipeline_operations.labels(pipeline_name=pipeline_name, operation=operation).inc()

    def record_pipeline_execution(self, pipeline_name: str, duration: float):
        """Record pipeline execution time."""
        self.pipeline_execution_time.labels(pipeline_name=pipeline_name).observe(duration)

        # Record in FastRedis observability
        if self.observability:
            self.observability.record_operation(
                f"pipeline_{pipeline_name}",
                duration * 1000,  # Convert to ms
                success=True,
                metadata={"pipeline": pipeline_name, "operation": "pipeline_execution"}
            )

    def record_flight_request(self, method: str, status: str, duration: float = None):
        """Record an Arrow Flight request."""
        self.flight_requests.labels(method=method, status=status).inc()

        if duration is not None:
            self.flight_request_time.labels(method=method).observe(duration)

    def get_metrics_text(self) -> str:
        """Get metrics in Prometheus text format."""
        return generate_latest(self.registry).decode('utf-8')

    def get_metrics_dict(self) -> Dict[str, Any]:
        """Get metrics as a dictionary for JSON serialization."""
        metrics_dict = {
            'messages_processed': self.messages_processed._value,
            'active_agents': self.active_agents._value,
            'arrow_memory_usage': self.arrow_memory_usage._value,
            'system_cpu_percent': self.system_cpu_percent._value,
            'system_memory_percent': self.system_memory_percent._value,
        }

        # Add FastRedis observability metrics if available
        if self.observability:
            try:
                fastredis_metrics = self.observability.get_all_metrics()
                metrics_dict['fastredis'] = fastredis_metrics

                # Add health check
                health = self.observability.health_check()
                metrics_dict['health'] = health

            except Exception as e:
                logger.warning(f"Failed to get FastRedis metrics: {e}")

        return metrics_dict


# Global metrics instance
_metrics_instance: Optional[SabotMetrics] = None


def get_metrics(redis_client=None) -> SabotMetrics:
    """Get the global metrics instance.

    Args:
        redis_client: Optional FastRedis client for observability features
    """
    global _metrics_instance
    if _metrics_instance is None:
        _metrics_instance = SabotMetrics(redis_client=redis_client)
    return _metrics_instance


def start_metrics_server(port: int = 9090, host: str = "0.0.0.0"):
    """Start Prometheus metrics HTTP server."""
    from prometheus_client import start_http_server
    try:
        start_http_server(port, addr=host)
        logger.info(f"Started metrics server on http://{host}:{port}/metrics")
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")


# Convenience functions for easy use
def record_message_processed(agent: str, topic: str, processing_time: float = None):
    """Convenience function to record message processing."""
    get_metrics().record_message_processed(agent, topic, processing_time)


def record_agent_error(agent: str, error_type: str = "unknown"):
    """Convenience function to record agent errors."""
    get_metrics().record_agent_error(agent, error_type)


def record_arrow_operation(operation_type: str, duration: float = None):
    """Convenience function to record Arrow operations."""
    get_metrics().record_arrow_operation(operation_type, duration)


def record_pipeline_operation(pipeline_name: str, operation: str):
    """Convenience function to record pipeline operations."""
    get_metrics().record_pipeline_operation(pipeline_name, operation)
