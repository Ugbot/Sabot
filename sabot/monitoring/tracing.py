#!/usr/bin/env python3
"""
Distributed Tracing Support for Sabot

Provides distributed tracing capabilities with:
- Request tracing across components
- Performance profiling
- Trace correlation
- Integration with tracing backends (Jaeger, Zipkin, etc.)
"""

import time
import uuid
import logging
from typing import Dict, List, Any, Optional, Callable, ContextManager
from dataclasses import dataclass, field
from contextvars import ContextVar
from enum import Enum

try:
    import opentelemetry as otel
    import opentelemetry.trace as trace
    import opentelemetry.sdk.trace as sdk_trace
    import opentelemetry.sdk.trace.export as trace_export
    import opentelemetry.exporter.jaeger as jaeger_exporter
    import opentelemetry.exporter.zipkin as zipkin_exporter
    OPENTELEMETRY_AVAILABLE = True
except ImportError:
    OPENTELEMETRY_AVAILABLE = False
    otel = None
    trace = None
    sdk_trace = None
    trace_export = None
    jaeger_exporter = None
    zipkin_exporter = None

logger = logging.getLogger(__name__)


class TracingBackend(Enum):
    """Supported tracing backends."""
    JAEGER = "jaeger"
    ZIPKIN = "zipkin"
    CONSOLE = "console"
    NONE = "none"


@dataclass
class TracingConfig:
    """Configuration for distributed tracing."""
    enabled: bool = True
    backend: TracingBackend = TracingBackend.CONSOLE
    service_name: str = "sabot"
    jaeger_endpoint: Optional[str] = None
    zipkin_endpoint: Optional[str] = None
    sample_rate: float = 1.0  # 1.0 = sample all requests
    max_spans_per_trace: int = 1000


@dataclass
class Span:
    """A tracing span."""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    name: str
    start_time: float
    end_time: Optional[float] = None
    attributes: Dict[str, Any] = field(default_factory=dict)
    events: List[Dict[str, Any]] = field(default_factory=list)
    status: str = "ok"

    @property
    def duration(self) -> Optional[float]:
        """Get span duration in seconds."""
        if self.end_time is None:
            return None
        return self.end_time - self.start_time

    def add_event(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
        """Add an event to the span."""
        self.events.append({
            "name": name,
            "timestamp": time.time(),
            "attributes": attributes or {}
        })

    def set_attribute(self, key: str, value: Any) -> None:
        """Set a span attribute."""
        self.attributes[key] = value

    def finish(self, status: str = "ok") -> None:
        """Finish the span."""
        self.end_time = time.time()
        self.status = status


@dataclass
class Trace:
    """A complete trace containing multiple spans."""
    trace_id: str
    spans: List[Span] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None

    @property
    def duration(self) -> Optional[float]:
        """Get trace duration in seconds."""
        if self.end_time is None:
            return None
        return self.end_time - self.start_time

    def add_span(self, span: Span) -> None:
        """Add a span to the trace."""
        self.spans.append(span)

    def finish(self) -> None:
        """Finish the trace."""
        self.end_time = time.time()


class TracingManager:
    """
    Manages distributed tracing for Sabot applications.

    Provides tracing capabilities across all components with support
    for multiple backends and automatic context propagation.
    """

    def __init__(self, config: TracingConfig):
        self.config = config
        self.traces: Dict[str, Trace] = {}
        self.active_spans: Dict[str, Span] = {}

        # Context variables for trace/span propagation
        self._current_trace_id: ContextVar[Optional[str]] = ContextVar('trace_id', default=None)
        self._current_span_id: ContextVar[Optional[str]] = ContextVar('span_id', default=None)

        # OpenTelemetry setup
        self._tracer = None
        if OPENTELEMETRY_AVAILABLE and config.enabled:
            self._setup_opentelemetry()

    def _setup_opentelemetry(self) -> None:
        """Setup OpenTelemetry tracing."""
        try:
            # Create tracer provider
            tracer_provider = sdk_trace.TracerProvider()

            # Configure exporters
            if self.config.backend == TracingBackend.JAEGER and self.config.jaeger_endpoint:
                jaeger_exporter_inst = jaeger_exporter.JaegerExporter(
                    agent_host_name=self.config.jaeger_endpoint.split(':')[0],
                    agent_port=int(self.config.jaeger_endpoint.split(':')[1])
                )
                span_processor = sdk_trace.SimpleSpanProcessor(jaeger_exporter_inst)

            elif self.config.backend == TracingBackend.ZIPKIN and self.config.zipkin_endpoint:
                zipkin_exporter_inst = zipkin_exporter.ZipkinExporter(
                    endpoint=self.config.zipkin_endpoint
                )
                span_processor = sdk_trace.SimpleSpanProcessor(zipkin_exporter_inst)

            else:
                # Console exporter
                span_processor = sdk_trace.SimpleSpanProcessor(trace_export.ConsoleSpanExporter())

            tracer_provider.add_span_processor(span_processor)
            trace.set_tracer_provider(tracer_provider)

            self._tracer = trace.get_tracer(__name__)

        except Exception as e:
            logger.error(f"Failed to setup OpenTelemetry: {e}")

    def start_trace(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> str:
        """
        Start a new trace.

        Returns:
            Trace ID
        """
        if not self.config.enabled:
            return ""

        trace_id = str(uuid.uuid4())
        trace_obj = Trace(trace_id=trace_id)
        self.traces[trace_id] = trace_obj

        # Set context
        self._current_trace_id.set(trace_id)

        # Start root span
        self.start_span(name, attributes or {})

        return trace_id

    def end_trace(self, trace_id: str) -> None:
        """End a trace."""
        if trace_id in self.traces:
            self.traces[trace_id].finish()

            # Clean up old traces (keep last 1000)
            if len(self.traces) > 1000:
                # Remove oldest traces
                sorted_traces = sorted(self.traces.items(),
                                     key=lambda x: x[1].start_time)
                for old_trace_id, _ in sorted_traces[:-1000]:
                    del self.traces[old_trace_id]

    def start_span(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> str:
        """
        Start a new span in the current trace.

        Returns:
            Span ID
        """
        if not self.config.enabled:
            return ""

        current_trace_id = self._current_trace_id.get()
        current_span_id = self._current_span_id.get()

        if not current_trace_id:
            # Start a new trace if none exists
            current_trace_id = self.start_trace("auto_trace")

        span_id = str(uuid.uuid4())

        span = Span(
            trace_id=current_trace_id,
            span_id=span_id,
            parent_span_id=current_span_id,
            name=name,
            start_time=time.time(),
            attributes=attributes or {}
        )

        self.active_spans[span_id] = span

        # Add to trace
        if current_trace_id in self.traces:
            self.traces[current_trace_id].add_span(span)

        # Set context
        self._current_span_id.set(span_id)

        # OpenTelemetry span
        if self._tracer:
            otel_span = self._tracer.start_span(name, attributes=attributes)
            # Store reference for later finishing
            span.attributes['_otel_span'] = otel_span

        return span_id

    def end_span(self, span_id: str, status: str = "ok") -> None:
        """End a span."""
        if span_id in self.active_spans:
            span = self.active_spans[span_id]
            span.finish(status)
            del self.active_spans[span_id]

            # Clear context if this was the current span
            current_span_id = self._current_span_id.get()
            if current_span_id == span_id:
                self._current_span_id.set(span.parent_span_id)

            # Finish OpenTelemetry span
            if self._tracer and '_otel_span' in span.attributes:
                otel_span = span.attributes['_otel_span']
                otel_span.set_status(trace.Status(trace.StatusCode.OK if status == "ok" else trace.StatusCode.ERROR))
                otel_span.end()

    def add_span_event(self, span_id: str, name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
        """Add an event to a span."""
        if span_id in self.active_spans:
            self.active_spans[span_id].add_event(name, attributes)

            # OpenTelemetry event
            if self._tracer and '_otel_span' in self.active_spans[span_id].attributes:
                otel_span = self.active_spans[span_id].attributes['_otel_span']
                otel_span.add_event(name, attributes or {})

    def set_span_attribute(self, span_id: str, key: str, value: Any) -> None:
        """Set an attribute on a span."""
        if span_id in self.active_spans:
            self.active_spans[span_id].set_attribute(key, value)

            # OpenTelemetry attribute
            if self._tracer and '_otel_span' in self.active_spans[span_id].attributes:
                otel_span = self.active_spans[span_id].attributes['_otel_span']
                otel_span.set_attribute(key, value)

    def get_current_trace_id(self) -> Optional[str]:
        """Get the current trace ID."""
        return self._current_trace_id.get()

    def get_current_span_id(self) -> Optional[str]:
        """Get the current span ID."""
        return self._current_span_id.get()

    def get_trace(self, trace_id: str) -> Optional[Trace]:
        """Get a trace by ID."""
        return self.traces.get(trace_id)

    def get_active_spans(self) -> List[Span]:
        """Get all active spans."""
        return list(self.active_spans.values())

    def get_trace_summary(self, trace_id: str) -> Optional[Dict[str, Any]]:
        """Get a summary of a trace."""
        trace = self.get_trace(trace_id)
        if not trace:
            return None

        return {
            'trace_id': trace.trace_id,
            'duration': trace.duration,
            'span_count': len(trace.spans),
            'start_time': trace.start_time,
            'end_time': trace.end_time,
            'spans': [
                {
                    'span_id': span.span_id,
                    'name': span.name,
                    'duration': span.duration,
                    'status': span.status,
                    'attributes': span.attributes
                }
                for span in trace.spans
            ]
        }

    def create_span_context(self, trace_id: str, span_id: str) -> Dict[str, str]:
        """Create context for span propagation."""
        return {
            'trace_id': trace_id,
            'span_id': span_id
        }

    def restore_span_context(self, context: Dict[str, str]) -> None:
        """Restore span context."""
        self._current_trace_id.set(context.get('trace_id'))
        self._current_span_id.set(context.get('span_id'))

    def instrument_function(self, func_name: str) -> Callable:
        """Decorator to instrument a function with tracing."""
        def decorator(func: Callable) -> Callable:
            def wrapper(*args, **kwargs):
                if not self.config.enabled:
                    return func(*args, **kwargs)

                span_id = self.start_span(func_name, {
                    'function': func.__name__,
                    'module': func.__module__
                })

                try:
                    result = func(*args, **kwargs)
                    self.end_span(span_id, "ok")
                    return result
                except Exception as e:
                    self.set_span_attribute(span_id, 'error', str(e))
                    self.end_span(span_id, "error")
                    raise

            return wrapper
        return decorator

    def get_tracing_stats(self) -> Dict[str, Any]:
        """Get tracing statistics."""
        total_traces = len(self.traces)
        active_spans = len(self.active_spans)

        # Calculate average trace duration
        trace_durations = [
            trace.duration for trace in self.traces.values()
            if trace.duration is not None
        ]
        avg_trace_duration = sum(trace_durations) / len(trace_durations) if trace_durations else 0

        return {
            'total_traces': total_traces,
            'active_spans': active_spans,
            'avg_trace_duration': avg_trace_duration,
            'backend': self.config.backend.value,
            'enabled': self.config.enabled,
            'sample_rate': self.config.sample_rate
        }


# Context manager for spans
class span_context:
    """Context manager for tracing spans."""

    def __init__(self, tracer: TracingManager, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.tracer = tracer
        self.name = name
        self.attributes = attributes or {}
        self.span_id: Optional[str] = None

    def __enter__(self):
        self.span_id = self.tracer.start_span(self.name, self.attributes)
        return self.span_id

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.span_id:
            status = "error" if exc_type else "ok"
            if exc_val:
                self.tracer.set_span_attribute(self.span_id, 'error', str(exc_val))
            self.tracer.end_span(self.span_id, status)
