#!/usr/bin/env python3
"""
Enhanced Metrics Collector for Sabot

Provides comprehensive metrics collection with:
- High-performance counters and gauges
- Histogram-based latency tracking
- Resource usage monitoring
- Custom metric definitions
- Prometheus export capabilities
- Real-time alerting integration
"""

import time
import threading
import logging
import psutil
from typing import Dict, List, Any, Optional, Callable, Union
from dataclasses import dataclass, field
from collections import defaultdict, deque
from enum import Enum
import statistics

try:
    from prometheus_client import (
        Counter, Gauge, Histogram, CollectorRegistry, generate_latest,
        push_to_gateway, exposition
    )
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
    push_to_gateway = lambda *args, **kwargs: None
    exposition = None

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of metrics supported."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


@dataclass
class MonitoringConfig:
    """Configuration for the monitoring system."""
    enabled: bool = True
    collection_interval: float = 10.0  # seconds
    retention_period: float = 3600.0  # 1 hour
    max_samples: int = 10000
    enable_prometheus: bool = True
    prometheus_push_gateway: Optional[str] = None
    enable_resource_monitoring: bool = True
    enable_custom_metrics: bool = True
    alert_check_interval: float = 30.0


@dataclass
class MetricDefinition:
    """Definition of a custom metric."""
    name: str
    type: MetricType
    description: str
    labels: List[str] = field(default_factory=list)
    buckets: Optional[List[float]] = None  # For histograms


@dataclass
class MetricSample:
    """Individual metric measurement."""
    timestamp: float
    value: Union[int, float]
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class TimeSeries:
    """Time series data for a metric."""
    name: str
    samples: deque = field(default_factory=lambda: deque(maxlen=1000))
    labels: Dict[str, str] = field(default_factory=dict)

    def add_sample(self, value: Union[int, float], timestamp: Optional[float] = None,
                   labels: Optional[Dict[str, str]] = None):
        """Add a sample to the time series."""
        if timestamp is None:
            timestamp = time.time()

        sample_labels = {**self.labels}
        if labels:
            sample_labels.update(labels)

        sample = MetricSample(timestamp, value, sample_labels)
        self.samples.append(sample)

    def get_recent_samples(self, seconds: float) -> List[MetricSample]:
        """Get samples from the last N seconds."""
        cutoff = time.time() - seconds
        return [s for s in self.samples if s.timestamp >= cutoff]

    def calculate_stats(self, seconds: Optional[float] = None) -> Dict[str, Any]:
        """Calculate statistics for the time series."""
        samples = self.get_recent_samples(seconds) if seconds else list(self.samples)

        if not samples:
            return {}

        values = [s.value for s in samples]

        return {
            'count': len(values),
            'sum': sum(values),
            'min': min(values),
            'max': max(values),
            'mean': statistics.mean(values) if values else 0,
            'median': statistics.median(values) if values else 0,
            'stddev': statistics.stdev(values) if len(values) > 1 else 0,
            'latest': values[-1] if values else 0,
            'rate_per_second': len(values) / (seconds or 60) if seconds else 0
        }


class MetricsCollector:
    """
    Enhanced metrics collector with comprehensive monitoring capabilities.

    Features:
    - High-performance metric collection
    - Time-series data storage
    - Resource monitoring
    - Prometheus integration
    - Alerting hooks
    """

    def __init__(self, config: MonitoringConfig):
        self.config = config
        self._lock = threading.RLock()

        # Core metrics storage
        self._counters: Dict[str, TimeSeries] = {}
        self._gauges: Dict[str, TimeSeries] = {}
        self._histograms: Dict[str, Dict[str, TimeSeries]] = defaultdict(dict)

        # Custom metrics
        self._custom_metrics: Dict[str, MetricDefinition] = {}
        self._custom_timeseries: Dict[str, TimeSeries] = {}

        # Resource monitoring
        self._resource_metrics = {}

        # Prometheus integration
        self._prometheus_registry = CollectorRegistry() if PROMETHEUS_AVAILABLE else None
        self._prometheus_metrics: Dict[str, Any] = {}

        # Alert callbacks
        self._alert_callbacks: List[Callable] = []

        # Background monitoring
        self._monitoring_thread: Optional[threading.Thread] = None
        self._running = False

        # Initialize built-in metrics
        self._initialize_builtin_metrics()

        # Start background monitoring if enabled
        if config.enabled:
            self.start()

    def _initialize_builtin_metrics(self):
        """Initialize built-in system metrics."""
        # Stream processing metrics
        self.create_counter('sabot_messages_processed_total', 'Total messages processed')
        self.create_counter('sabot_messages_failed_total', 'Total messages failed')
        self.create_gauge('sabot_active_streams', 'Number of active streams')
        self.create_gauge('sabot_active_agents', 'Number of active agents')
        self.create_histogram('sabot_message_processing_duration', 'Message processing duration',
                            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0])

        # Resource metrics
        self.create_gauge('sabot_cpu_usage_percent', 'CPU usage percentage')
        self.create_gauge('sabot_memory_usage_bytes', 'Memory usage in bytes')
        self.create_gauge('sabot_memory_usage_percent', 'Memory usage percentage')
        self.create_gauge('sabot_disk_usage_bytes', 'Disk usage in bytes')
        self.create_gauge('sabot_network_bytes_sent', 'Network bytes sent')
        self.create_gauge('sabot_network_bytes_received', 'Network bytes received')

        # State management metrics
        self.create_counter('sabot_state_operations_total', 'Total state operations')
        self.create_gauge('sabot_state_tables_active', 'Number of active state tables')
        self.create_histogram('sabot_state_operation_duration', 'State operation duration')

        # Error metrics
        self.create_counter('sabot_errors_total', 'Total errors by type', labels=['error_type'])
        self.create_gauge('sabot_health_status', 'Health status (0=unhealthy, 1=healthy)')

    def create_counter(self, name: str, description: str, labels: Optional[List[str]] = None) -> None:
        """Create a counter metric."""
        with self._lock:
            if name not in self._counters:
                self._counters[name] = TimeSeries(name)

                if PROMETHEUS_AVAILABLE and self._prometheus_registry:
                    self._prometheus_metrics[name] = Counter(
                        name, description, labelnames=labels or [],
                        registry=self._prometheus_registry
                    )

    def create_gauge(self, name: str, description: str, labels: Optional[List[str]] = None) -> None:
        """Create a gauge metric."""
        with self._lock:
            if name not in self._gauges:
                self._gauges[name] = TimeSeries(name)

                if PROMETHEUS_AVAILABLE and self._prometheus_registry:
                    self._prometheus_metrics[name] = Gauge(
                        name, description, labelnames=labels or [],
                        registry=self._prometheus_registry
                    )

    def create_histogram(self, name: str, description: str,
                        buckets: Optional[List[float]] = None,
                        labels: Optional[List[str]] = None) -> None:
        """Create a histogram metric."""
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = {}

                if PROMETHEUS_AVAILABLE and self._prometheus_registry:
                    self._prometheus_metrics[name] = Histogram(
                        name, description,
                        labelnames=labels or [],
                        buckets=buckets or [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0],
                        registry=self._prometheus_registry
                    )

    def increment_counter(self, name: str, value: float = 1.0,
                         labels: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter."""
        with self._lock:
            if name in self._counters:
                self._counters[name].add_sample(value, labels=labels)

                if PROMETHEUS_AVAILABLE and name in self._prometheus_metrics:
                    prom_metric = self._prometheus_metrics[name]
                    if labels:
                        prom_metric = prom_metric.labels(**labels)
                    prom_metric.inc(value)

    def set_gauge(self, name: str, value: Union[int, float],
                  labels: Optional[Dict[str, str]] = None) -> None:
        """Set a gauge value."""
        with self._lock:
            if name in self._gauges:
                self._gauges[name].add_sample(value, labels=labels)

                if PROMETHEUS_AVAILABLE and name in self._prometheus_metrics:
                    prom_metric = self._prometheus_metrics[name]
                    if labels:
                        prom_metric = prom_metric.labels(**labels)
                    prom_metric.set(value)

    def observe_histogram(self, name: str, value: Union[int, float],
                         labels: Optional[Dict[str, str]] = None) -> None:
        """Observe a value in a histogram."""
        with self._lock:
            if name in self._histograms:
                # Store in our time series
                if 'observations' not in self._histograms[name]:
                    self._histograms[name]['observations'] = TimeSeries(f"{name}_observations")

                self._histograms[name]['observations'].add_sample(value, labels=labels)

                if PROMETHEUS_AVAILABLE and name in self._prometheus_metrics:
                    prom_metric = self._prometheus_metrics[name]
                    if labels:
                        prom_metric = prom_metric.labels(**labels)
                    prom_metric.observe(value)

    def record_message_processed(self, stream_id: str, processing_time: Optional[float] = None) -> None:
        """Record a message processing event."""
        self.increment_counter('sabot_messages_processed_total',
                             labels={'stream_id': stream_id})

        if processing_time is not None:
            self.observe_histogram('sabot_message_processing_duration',
                                 processing_time,
                                 labels={'stream_id': stream_id})

    def record_message_failed(self, stream_id: str, error_type: str = 'unknown') -> None:
        """Record a message processing failure."""
        self.increment_counter('sabot_messages_failed_total',
                             labels={'stream_id': stream_id, 'error_type': error_type})
        self.increment_counter('sabot_errors_total',
                             labels={'error_type': error_type})

    def record_state_operation(self, operation: str, duration: Optional[float] = None) -> None:
        """Record a state operation."""
        self.increment_counter('sabot_state_operations_total',
                             labels={'operation': operation})

        if duration is not None:
            self.observe_histogram('sabot_state_operation_duration',
                                 duration,
                                 labels={'operation': operation})

    def update_resource_metrics(self) -> None:
        """Update system resource metrics."""
        if not self.config.enable_resource_monitoring:
            return

        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=None)
            self.set_gauge('sabot_cpu_usage_percent', cpu_percent)

            # Memory usage
            memory = psutil.virtual_memory()
            self.set_gauge('sabot_memory_usage_bytes', memory.used)
            self.set_gauge('sabot_memory_usage_percent', memory.percent)

            # Disk usage (root filesystem)
            disk = psutil.disk_usage('/')
            self.set_gauge('sabot_disk_usage_bytes', disk.used)

            # Network I/O (since boot)
            net = psutil.net_io_counters()
            if net:
                self.set_gauge('sabot_network_bytes_sent', net.bytes_sent)
                self.set_gauge('sabot_network_bytes_received', net.bytes_recv)

        except Exception as e:
            logger.error(f"Failed to update resource metrics: {e}")

    def get_metric_stats(self, name: str, time_window: Optional[float] = None) -> Dict[str, Any]:
        """Get statistics for a specific metric."""
        with self._lock:
            if name in self._counters:
                return self._counters[name].calculate_stats(time_window)
            elif name in self._gauges:
                stats = self._gauges[name].calculate_stats(time_window)
                # For gauges, also include current value
                recent = self._gauges[name].get_recent_samples(60)  # Last minute
                if recent:
                    stats['current'] = recent[-1].value
                return stats
            elif name in self._histograms:
                # For histograms, return observation stats
                if 'observations' in self._histograms[name]:
                    return self._histograms[name]['observations'].calculate_stats(time_window)

        return {}

    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all metrics data."""
        with self._lock:
            result = {
                'counters': {},
                'gauges': {},
                'histograms': {},
                'custom': {},
                'resources': self._resource_metrics.copy(),
                'timestamp': time.time()
            }

            # Counters
            for name, ts in self._counters.items():
                result['counters'][name] = ts.calculate_stats()

            # Gauges
            for name, ts in self._gauges.items():
                stats = ts.calculate_stats()
                recent = ts.get_recent_samples(60)
                if recent:
                    stats['current'] = recent[-1].value
                result['gauges'][name] = stats

            # Histograms
            for name, hist_data in self._histograms.items():
                if 'observations' in hist_data:
                    result['histograms'][name] = hist_data['observations'].calculate_stats()

            # Custom metrics
            for name, ts in self._custom_timeseries.items():
                result['custom'][name] = ts.calculate_stats()

            return result

    def get_prometheus_metrics(self) -> bytes:
        """Get metrics in Prometheus format."""
        if PROMETHEUS_AVAILABLE and self._prometheus_registry:
            return generate_latest(self._prometheus_registry)
        return b""

    def push_to_prometheus_gateway(self) -> None:
        """Push metrics to Prometheus push gateway."""
        if (PROMETHEUS_AVAILABLE and self.config.prometheus_push_gateway and
            self._prometheus_registry):
            try:
                push_to_gateway(
                    self.config.prometheus_push_gateway,
                    job='sabot',
                    registry=self._prometheus_registry
                )
            except Exception as e:
                logger.error(f"Failed to push metrics to Prometheus: {e}")

    def add_alert_callback(self, callback: Callable) -> None:
        """Add a callback for alert notifications."""
        self._alert_callbacks.append(callback)

    def _monitoring_loop(self) -> None:
        """Background monitoring loop."""
        while self._running:
            try:
                # Update resource metrics
                self.update_resource_metrics()

                # Push to Prometheus if configured
                if self.config.prometheus_push_gateway:
                    self.push_to_prometheus_gateway()

                # Sleep for collection interval
                time.sleep(self.config.collection_interval)

            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(5)  # Brief pause before retrying

    def start(self) -> None:
        """Start the metrics collector."""
        if self._running:
            return

        self._running = True
        self._monitoring_thread = threading.Thread(
            target=self._monitoring_loop,
            daemon=True,
            name="metrics-collector"
        )
        self._monitoring_thread.start()
        logger.info("Metrics collector started")

    def stop(self) -> None:
        """Stop the metrics collector."""
        self._running = False
        if self._monitoring_thread:
            self._monitoring_thread.join(timeout=5.0)
        logger.info("Metrics collector stopped")

    def get_health_status(self) -> Dict[str, Any]:
        """Get overall health status based on metrics."""
        try:
            # Check recent error rates
            error_stats = self.get_metric_stats('sabot_errors_total', time_window=300)  # Last 5 minutes
            error_rate = error_stats.get('rate_per_second', 0)

            # Check message processing success rate
            processed_stats = self.get_metric_stats('sabot_messages_processed_total', time_window=300)
            failed_stats = self.get_metric_stats('sabot_messages_failed_total', time_window=300)

            total_processed = processed_stats.get('sum', 0)
            total_failed = failed_stats.get('sum', 0)

            success_rate = 1.0
            if total_processed + total_failed > 0:
                success_rate = total_processed / (total_processed + total_failed)

            # Check resource usage
            memory_percent = self.get_metric_stats('sabot_memory_usage_percent', time_window=60)
            current_memory = memory_percent.get('current', 0)

            # Determine health status
            is_healthy = (
                error_rate < 1.0 and  # Less than 1 error per second
                success_rate > 0.95 and  # >95% success rate
                current_memory < 90.0  # <90% memory usage
            )

            return {
                'healthy': is_healthy,
                'error_rate': error_rate,
                'success_rate': success_rate,
                'memory_usage_percent': current_memory,
                'checks': {
                    'error_rate_ok': error_rate < 1.0,
                    'success_rate_ok': success_rate > 0.95,
                    'memory_ok': current_memory < 90.0
                }
            }

        except Exception as e:
            logger.error(f"Failed to determine health status: {e}")
            return {
                'healthy': False,
                'error': str(e),
                'checks': {}
            }
