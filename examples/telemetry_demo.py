#!/usr/bin/env python3
"""
OpenTelemetry Integration Demo for Sabot

This demo showcases how to integrate OpenTelemetry tracing and metrics
with Sabot streaming applications for end-to-end observability.

Features demonstrated:
- Distributed tracing across agent operations
- Custom spans for processing stages
- Metrics collection (counters, histograms, gauges)
- Integration with observability backends

Prerequisites:
- Kafka/Redpanda running: docker compose up -d
- Sabot installed: pip install -e .
- OpenTelemetry: pip install opentelemetry-api opentelemetry-sdk

Usage:
    # Start the worker with telemetry
    sabot -A examples.telemetry_demo:app worker

    # Send test data (separate terminal)
    python -c "
    from confluent_kafka import Producer
    import json, random, time

    producer = Producer({'bootstrap.servers': 'localhost:19092'})

    for i in range(30):
        msg = {
            'id': f'msg_{i}',
            'type': random.choice(['order', 'event', 'metric']),
            'user_id': f'user_{random.randint(1, 100)}',
            'value': random.randint(10, 1000),
            'timestamp': time.time()
        }
        producer.produce('messages', value=json.dumps(msg).encode())
        producer.flush()
        print(f'Sent: {msg}')
        time.sleep(0.3)
    "

Note:
    For production, configure exporters for Jaeger, Prometheus, or OTLP.
"""

import sabot as sb
import time
import random

# OpenTelemetry imports (with fallbacks)
try:
    from opentelemetry import trace, metrics as otel_metrics
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader, ConsoleMetricExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.semconv.resource import ResourceAttributes
    OPENTELEMETRY_AVAILABLE = True
except ImportError:
    print("⚠️  OpenTelemetry not available. Install with:")
    print("    pip install opentelemetry-api opentelemetry-sdk")
    print("    Running without telemetry...")
    OPENTELEMETRY_AVAILABLE = False

# Create Sabot application
app = sb.App(
    'telemetry-demo',
    broker='kafka://localhost:19092',
    value_serializer='json'
)

# Setup OpenTelemetry
tracer = None
meter = None
message_counter = None
processing_duration = None

if OPENTELEMETRY_AVAILABLE:
    # Setup resource
    resource = Resource.create({
        ResourceAttributes.SERVICE_NAME: "sabot-telemetry-demo",
        ResourceAttributes.SERVICE_VERSION: "1.0.0",
    })

    # Setup tracing
    trace.set_tracer_provider(TracerProvider(resource=resource))
    tracer_provider = trace.get_tracer_provider()

    # Console exporter for demo (use Jaeger/OTLP in production)
    console_span_processor = BatchSpanProcessor(ConsoleSpanExporter())
    tracer_provider.add_span_processor(console_span_processor)

    tracer = trace.get_tracer(__name__)

    # Setup metrics
    console_metric_reader = PeriodicExportingMetricReader(
        exporter=ConsoleMetricExporter(),
        export_interval_millis=10000,  # Export every 10 seconds
    )

    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[console_metric_reader],
    )
    otel_metrics.set_meter_provider(meter_provider)

    meter = otel_metrics.get_meter(__name__)

    # Create metrics
    message_counter = meter.create_counter(
        name="sabot_messages_processed_total",
        description="Total number of messages processed",
        unit="1"
    )

    processing_duration = meter.create_histogram(
        name="sabot_message_processing_duration",
        description="Time spent processing messages",
        unit="s"
    )

    print("✅ OpenTelemetry telemetry configured")
    print("📊 Metrics: sabot_messages_processed_total, sabot_message_processing_duration")
    print("🔍 Traces: Console output (configure Jaeger for visualization)\n")


@app.agent('messages')
async def process_with_telemetry(stream):
    """
    Process messages with full OpenTelemetry instrumentation.

    Each message gets:
    - Distributed trace with spans
    - Metrics (counters, histograms)
    - Custom attributes and events
    """
    print("📡 Telemetry Agent started")
    if not OPENTELEMETRY_AVAILABLE:
        print("⚠️  Running without OpenTelemetry instrumentation\n")

    processed = 0

    async for message in stream:
        start_time = time.time()

        try:
            if OPENTELEMETRY_AVAILABLE and tracer:
                # Create distributed trace span
                with tracer.start_as_current_span("process_message") as span:
                    span.set_attribute("message.id", message.get("id"))
                    span.set_attribute("message.type", message.get("type"))
                    span.set_attribute("user.id", message.get("user_id"))

                    # Add event
                    span.add_event("message_received", {
                        "timestamp": message.get("timestamp", time.time())
                    })

                    # Validation span
                    with tracer.start_as_current_span("validate_message") as val_span:
                        val_span.set_attribute("validation.type", "schema")
                        is_valid = all(k in message for k in ["id", "type", "user_id"])
                        val_span.set_attribute("validation.result", "valid" if is_valid else "invalid")
                        val_span.add_event("validation_complete")

                        if not is_valid:
                            span.add_event("error", {"message": "Invalid schema"})
                            continue

                    # Business logic span
                    with tracer.start_as_current_span("business_logic") as logic_span:
                        logic_span.set_attribute("operation", "process")
                        msg_type = message.get("type")
                        value = message.get("value", 0)

                        # Simulate processing
                        if msg_type == "order":
                            logic_span.set_attribute("order.value", value)
                            result = f"Order processed: ${value}"
                        elif msg_type == "event":
                            logic_span.set_attribute("event.type", "custom")
                            result = f"Event logged: {message.get('id')}"
                        else:
                            result = f"Message processed: {msg_type}"

                        logic_span.add_event("processing_complete", {"result": result})

                    # State update span (simulated)
                    with tracer.start_as_current_span("update_state") as state_span:
                        state_span.set_attribute("store.type", "memory")
                        state_span.set_attribute("operation", "update")
                        # Simulate state update
                        await asyncio_sleep(random.uniform(0.001, 0.005))
                        state_span.add_event("state_updated")

                    processed += 1
                    duration = time.time() - start_time

                    # Record metrics
                    message_counter.add(1, {"message_type": msg_type})
                    processing_duration.record(duration, {"operation": "full_process"})

                    print(f"✅ Processed {message.get('id')} ({msg_type}) "
                          f"in {duration*1000:.2f}ms [trace_id: {span.get_span_context().trace_id}]")

                    yield {
                        "message_id": message.get("id"),
                        "processed": True,
                        "duration": duration,
                        "trace_id": hex(span.get_span_context().trace_id)
                    }

            else:
                # Process without telemetry
                processed += 1
                print(f"✅ Processed {message.get('id')} (no telemetry)")

                yield {
                    "message_id": message.get("id"),
                    "processed": True
                }

            # Periodic stats
            if processed % 10 == 0:
                print(f"\n📊 Processed {processed} messages total\n")

        except Exception as e:
            if OPENTELEMETRY_AVAILABLE and tracer:
                with tracer.start_as_current_span("error_handler") as error_span:
                    error_span.set_attribute("error.type", type(e).__name__)
                    error_span.set_attribute("error.message", str(e))
                    error_span.add_event("error_occurred")

            print(f"❌ Error processing message: {e}")
            continue


# Helper for simulated async sleep
async def asyncio_sleep(seconds):
    """Helper for async sleep."""
    import asyncio
    await asyncio.sleep(seconds)


if __name__ == "__main__":
    print(__doc__)
    print("\n" + "="*60)
    print("TELEMETRY CONFIGURATION:")
    print("="*60)
    print("""
📊 Metrics Available:
- sabot_messages_processed_total (counter)
- sabot_message_processing_duration (histogram)

🔍 Traces Available:
- process_message (root span)
  - validate_message (validation)
  - business_logic (processing)
  - update_state (state updates)

🚀 Production Configuration:

# Jaeger Tracing
from opentelemetry.exporter.jaeger import JaegerExporter
jaeger_exporter = JaegerExporter(
    agent_host_name='localhost',
    agent_port=6831,
)

# Prometheus Metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
prometheus_reader = PrometheusMetricReader()

# OTLP (OpenTelemetry Protocol)
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4317")

📈 View Traces:
1. Start Jaeger: docker run -d -p 16686:16686 -p 6831:6831/udp jaegertracing/all-in-one
2. Open: http://localhost:16686
3. Search for service: sabot-telemetry-demo

📉 View Metrics:
1. Start Prometheus: docker run -d -p 9090:9090 prom/prometheus
2. Open: http://localhost:9090
3. Query: sabot_messages_processed_total
""")
