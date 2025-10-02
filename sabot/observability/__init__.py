#!/usr/bin/env python3
"""
Sabot Observability Module - OpenTelemetry Integration

Provides comprehensive OpenTelemetry integration across all Sabot components
for enterprise-grade observability and monitoring.
"""

import os
import logging
from typing import Optional, Dict, Any, Callable
from contextlib import contextmanager

# OpenTelemetry imports with graceful fallback
try:
    import opentelemetry as otel
    from opentelemetry import trace, metrics
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
    from opentelemetry.exporter.jaeger.thrift import JaegerExporter
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.semconv.resource import ResourceAttributes
    from opentelemetry.trace.status import Status, StatusCode
    OPENTELEMETRY_AVAILABLE = True
except ImportError:
    OPENTELEMETRY_AVAILABLE = False
    # Create mock objects for graceful degradation
    class MockTracerProvider:
        def add_span_processor(self, processor): pass

    class MockMeterProvider:
        pass

    class MockTracer:
        def start_as_current_span(self, name, **kwargs):
            return MockSpan()

    class MockSpan:
        def set_attribute(self, key, value): pass
        def add_event(self, name, attributes=None): pass
        def set_status(self, status): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass

    class MockMeter:
        def create_counter(self, *args, **kwargs): return MockCounter()
        def create_histogram(self, *args, **kwargs): return MockHistogram()
        def create_gauge(self, *args, **kwargs): return MockGauge()

    class MockCounter:
        def add(self, value, attributes=None): pass

    class MockHistogram:
        def record(self, value, attributes=None): pass

    class MockGauge:
        def set(self, value, attributes=None): pass

    trace = type('MockTrace', (), {
        'get_tracer_provider': lambda: MockTracerProvider(),
        'set_tracer_provider': lambda x: None,
        'get_tracer': lambda name: MockTracer(),
        'Status': type('Status', (), {'StatusCode': type('StatusCode', (), {'OK': 'OK', 'ERROR': 'ERROR'})})(),
        'StatusCode': type('StatusCode', (), {'OK': 'OK', 'ERROR': 'ERROR'})()
    })()

    metrics = type('MockMetrics', (), {
        'set_meter_provider': lambda x: None,
        'get_meter': lambda name: MockMeter()
    })()

    otel = None

logger = logging.getLogger(__name__)


class SabotObservability:
    """
    Centralized observability manager for Sabot.

    Provides unified tracing and metrics across all components with
    configurable backends and sampling.
    """

    def __init__(self, enabled: bool = None, service_name: str = "sabot"):
        """
        Initialize observability.

        Args:
            enabled: Whether to enable telemetry (auto-detects if None)
            service_name: Service name for telemetry
        """
        self.enabled = enabled if enabled is not None else self._auto_detect_enabled()
        self.service_name = service_name

        if self.enabled and OPENTELEMETRY_AVAILABLE:
            self._setup_telemetry()
        elif self.enabled and not OPENTELEMETRY_AVAILABLE:
            logger.warning("OpenTelemetry enabled but not available. Install with: pip install opentelemetry-distro")
            self.enabled = False

        # Global instances
        self._tracer = None
        self._meter = None
        self._counters = {}
        self._histograms = {}
        self._gauges = {}

    def _auto_detect_enabled(self) -> bool:
        """Auto-detect if telemetry should be enabled."""
        # Check environment variables
        env_enabled = os.getenv('SABOT_TELEMETRY_ENABLED', '').lower()
        if env_enabled in ('true', '1', 'yes'):
            return True
        if env_enabled in ('false', '0', 'no'):
            return False

        # Check for OpenTelemetry endpoints
        if os.getenv('OTEL_TRACES_EXPORTER') or os.getenv('OTEL_METRICS_EXPORTER'):
            return True

        # Default to disabled for development
        return False

    def _setup_telemetry(self):
        """Setup OpenTelemetry providers and exporters."""
        resource = Resource.create({
            ResourceAttributes.SERVICE_NAME: self.service_name,
            ResourceAttributes.SERVICE_VERSION: "1.0.0",
            ResourceAttributes.SERVICE_INSTANCE_ID: f"{self.service_name}-{os.getpid()}"
        })

        # Setup tracing
        trace.set_tracer_provider(TracerProvider(resource=resource))

        # Configure exporters based on environment
        self._setup_trace_exporters()

        # Setup metrics
        self._setup_metrics()

        logger.info(f"OpenTelemetry telemetry enabled for service: {self.service_name}")

    def _setup_trace_exporters(self):
        """Setup trace exporters."""
        tracer_provider = trace.get_tracer_provider()

        # Jaeger exporter
        jaeger_endpoint = os.getenv('SABOT_JAEGER_ENDPOINT', 'http://localhost:14268/api/traces')
        if jaeger_endpoint:
            try:
                jaeger_exporter = JaegerExporter(
                    agent_host_name=jaeger_endpoint.split('://')[1].split(':')[0],
                    agent_port=int(jaeger_endpoint.split(':')[-1].split('/')[0]),
                )
                tracer_provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))
                logger.info(f"Jaeger exporter configured: {jaeger_endpoint}")
            except Exception as e:
                logger.warning(f"Failed to configure Jaeger exporter: {e}")

        # OTLP exporter
        otlp_endpoint = os.getenv('SABOT_OTLP_ENDPOINT', 'http://localhost:4317')
        if otlp_endpoint:
            try:
                otlp_exporter = OTLPSpanExporter(
                    endpoint=otlp_endpoint,
                    insecure=True,
                )
                tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
                logger.info(f"OTLP exporter configured: {otlp_endpoint}")
            except Exception as e:
                logger.warning(f"Failed to configure OTLP exporter: {e}")

        # Console exporter for development
        if os.getenv('SABOT_CONSOLE_TRACING', '').lower() in ('true', '1'):
            console_exporter = ConsoleSpanExporter()
            tracer_provider.add_span_processor(BatchSpanProcessor(console_exporter))
            logger.info("Console tracing enabled")

    def _setup_metrics(self):
        """Setup metrics collection."""
        # Prometheus exporter
        prometheus_port = int(os.getenv('SABOT_PROMETHEUS_PORT', '8000'))
        try:
            prometheus_reader = PrometheusMetricReader()
            meter_provider = MeterProvider(metric_readers=[prometheus_reader])
            metrics.set_meter_provider(meter_provider)
            logger.info(f"Prometheus metrics enabled on port {prometheus_port}")
        except Exception as e:
            logger.warning(f"Failed to configure Prometheus metrics: {e}")

    @property
    def tracer(self):
        """Get the tracer instance."""
        if not self._tracer:
            if self.enabled and OPENTELEMETRY_AVAILABLE:
                self._tracer = trace.get_tracer(f"{self.service_name}.sabot")
            else:
                self._tracer = trace.get_tracer("mock")
        return self._tracer

    @property
    def meter(self):
        """Get the meter instance."""
        if not self._meter:
            if self.enabled and OPENTELEMETRY_AVAILABLE:
                self._meter = metrics.get_meter(f"{self.service_name}.sabot")
            else:
                self._meter = metrics.get_meter("mock")
        return self._meter

    def get_counter(self, name: str, description: str = "", unit: str = "1") -> Any:
        """Get or create a counter metric."""
        if name not in self._counters:
            self._counters[name] = self.meter.create_counter(
                name=name,
                description=description,
                unit=unit
            )
        return self._counters[name]

    def get_histogram(self, name: str, description: str = "", unit: str = "s") -> Any:
        """Get or create a histogram metric."""
        if name not in self._histograms:
            self._histograms[name] = self.meter.create_histogram(
                name=name,
                description=description,
                unit=unit
            )
        return self._histograms[name]

    def get_gauge(self, name: str, description: str = "", unit: str = "1") -> Any:
        """Get or create a gauge metric."""
        if name not in self._gauges:
            self._gauges[name] = self.meter.create_gauge(
                name=name,
                description=description,
                unit=unit
            )
        return self._gauges[name]

    @contextmanager
    def trace_operation(self, operation_name: str, attributes: Dict[str, Any] = None):
        """
        Context manager for tracing operations.

        Usage:
            with observability.trace_operation("process_message", {"stream": "orders"}):
                # Your code here
                pass
        """
        if not self.enabled:
            yield
            return

        with self.tracer.start_as_current_span(operation_name) as span:
            if attributes:
                for key, value in attributes.items():
                    span.set_attribute(key, value)
            try:
                yield span
            except Exception as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.add_event("error", {"error.message": str(e)})
                raise
            else:
                span.set_status(Status(StatusCode.OK))

    def record_operation(self, operation: str, duration: float, attributes: Dict[str, Any] = None):
        """Record an operation with timing."""
        if not self.enabled:
            return

        histogram = self.get_histogram(
            f"sabot_{operation}_duration",
            f"Duration of {operation} operations",
            "s"
        )
        histogram.record(duration, attributes or {})

        counter = self.get_counter(
            f"sabot_{operation}_total",
            f"Total number of {operation} operations"
        )
        counter.add(1, attributes or {})

    def record_metric(self, name: str, value: float, attributes: Dict[str, Any] = None, metric_type: str = "histogram"):
        """Record a custom metric."""
        if not self.enabled:
            return

        if metric_type == "counter":
            counter = self.get_counter(f"sabot_{name}", f"Custom counter: {name}")
            counter.add(int(value), attributes or {})
        elif metric_type == "gauge":
            gauge = self.get_gauge(f"sabot_{name}", f"Custom gauge: {name}")
            gauge.set(value, attributes or {})
        else:  # histogram
            histogram = self.get_histogram(f"sabot_{name}", f"Custom histogram: {name}")
            histogram.record(value, attributes or {})


# Global observability instance
_observability = None

def get_observability() -> SabotObservability:
    """Get the global observability instance."""
    global _observability
    if _observability is None:
        _observability = SabotObservability()
    return _observability

def init_observability(enabled: bool = None, service_name: str = "sabot") -> SabotObservability:
    """Initialize global observability."""
    global _observability
    _observability = SabotObservability(enabled=enabled, service_name=service_name)
    return _observability

def trace_operation(operation_name: str, attributes: Dict[str, Any] = None):
    """Decorator for tracing operations."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            obs = get_observability()
            with obs.trace_operation(operation_name, attributes):
                return func(*args, **kwargs)
        return wrapper
    return decorator

# Convenience functions
def start_span(name: str, attributes: Dict[str, Any] = None):
    """Start a new span."""
    obs = get_observability()
    return obs.tracer.start_as_current_span(name, attributes=attributes or {})

def record_operation(operation: str, duration: float, attributes: Dict[str, Any] = None):
    """Record an operation."""
    obs = get_observability()
    obs.record_operation(operation, duration, attributes)

def record_metric(name: str, value: float, attributes: Dict[str, Any] = None, metric_type: str = "histogram"):
    """Record a metric."""
    obs = get_observability()
    obs.record_metric(name, value, attributes, metric_type)
