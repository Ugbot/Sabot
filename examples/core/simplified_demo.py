#!/usr/bin/env python3
"""
Simplified Sabot Demo

This is a simplified demonstration of Sabot concepts that doesn't require
the full Sabot installation. It shows the core ideas and patterns using
the new CLI-compatible agent model.

This demo runs standalone and doesn't require Kafka, but shows you the
patterns you'll use with real Sabot applications.

Usage:
    python examples/core/simplified_demo.py
"""

import asyncio
import random
import time
from typing import AsyncGenerator, Dict, Any, List


# Simplified Sabot-like classes for demonstration

class MockApp:
    """Mock Sabot app demonstrating the @app.agent() pattern."""

    def __init__(self, name: str, broker: str = 'mock://'):
        self.name = name
        self.broker = broker
        self.agents = {}
        print(f"🚀 Created app: {name} (broker: {broker})")

    def agent(self, topic: str):
        """Decorator to register an agent (matches real Sabot API)."""
        def decorator(func):
            self.agents[topic] = func
            print(f"🤖 Registered agent for topic: {topic}")
            return func
        return decorator

    async def send_to_topic(self, topic: str, data: Dict[str, Any]):
        """Send data to a topic (mock implementation)."""
        if topic in self.agents:
            await asyncio.sleep(0.001)  # Tiny delay for realism
            print(f"📨 Sent data to {topic}")


async def generate_sensor_data() -> AsyncGenerator[Dict[str, Any], None]:
    """Generate simulated sensor readings."""
    sensors = [f"sensor_{i}" for i in range(1, 6)]

    while True:
        yield {
            "sensor_id": random.choice(sensors),
            "temperature": round(random.uniform(20.0, 35.0), 2),
            "humidity": round(random.uniform(30.0, 80.0), 2),
            "timestamp": time.time()
        }
        await asyncio.sleep(0.1)


async def demo_basic_agent_pattern():
    """Demonstrate the @app.agent() pattern (matches real Sabot)."""
    print("\n🤖 Agent Pattern Demo (Real Sabot Pattern)")
    print("-" * 46)

    # This is exactly how you'd create a real Sabot app
    app = MockApp("sensor-pipeline", broker='kafka://localhost:19092')

    processed_count = 0
    hot_readings = 0

    # Define agent using decorator (EXACTLY like real Sabot)
    @app.agent('sensor-readings')
    async def process_sensors(stream):
        """Process sensor data and alert on high temperatures."""
        nonlocal processed_count, hot_readings

        print("📡 Sensor processing agent started (simulated stream)")

        async for record in stream:
            processed_count += 1

            # Filter: Only hot readings (> 25°C)
            if record["temperature"] > 25.0:
                hot_readings += 1

                # Transform: Add derived fields
                record['temp_celsius'] = record['temperature']
                record['temp_fahrenheit'] = round(record['temperature'] * 9/5 + 32, 2)
                record['alert_level'] = "HIGH" if record['temperature'] > 30.0 else "MEDIUM"

                # Output
                print(f"🚨 ALERT [{record['alert_level']}]: "
                      f"{record['sensor_id']} - {record['temp_celsius']}°C")

                # Yield for downstream processing
                yield record

    # Simulate streaming data
    print("🔄 Simulating data stream...")
    data_generator = generate_sensor_data()

    # Manually feed data to agent (in real Sabot, Kafka does this)
    mock_stream = []
    for i in range(20):
        sensor_data = await anext(data_generator)
        mock_stream.append(sensor_data)

    # Process through agent
    results = [r async for r in process_sensors(mock_stream)]

    print(f"✅ Processed {processed_count} sensor readings")
    print(f"🔥 Found {hot_readings} hot readings (> 25°C)")
    print(f"📊 Generated {len(results)} alerts")


async def demo_fraud_detection_agent():
    """Demonstrate fraud detection agent pattern."""
    print("\n🚨 Fraud Detection Agent Demo")
    print("-" * 31)

    app = MockApp("fraud-detection", broker='kafka://localhost:19092')

    fraud_alerts = 0

    @app.agent('transactions')
    async def detect_fraud(stream):
        """Detect fraudulent transactions."""
        nonlocal fraud_alerts

        print("🔍 Fraud detection agent started")

        async for transaction in stream:
            # Simple fraud rules
            is_fraud = False
            reasons = []

            if transaction["amount"] > 500:
                is_fraud = True
                reasons.append("high_amount")

            if transaction["amount"] > 900:
                reasons.append("very_high_amount")

            if is_fraud:
                fraud_alerts += 1
                print(f"🚨 FRAUD ALERT: ${transaction['amount']} "
                      f"from {transaction['user_id']} "
                      f"(reasons: {', '.join(reasons)})")
                yield {
                    "transaction": transaction,
                    "fraud_reasons": reasons,
                    "flagged_at": time.time()
                }

    # Generate test transactions
    transactions = []
    for i in range(20):
        transactions.append({
            "txn_id": f"txn_{i}",
            "user_id": f"user_{random.randint(1, 10)}",
            "amount": round(random.uniform(10, 1000), 2),
            "merchant": f"merchant_{random.randint(1, 20)}",
            "timestamp": time.time()
        })

    # Process transactions
    print("💳 Processing transactions...")
    alerts = [a async for a in detect_fraud(transactions)]

    print(f"✅ Processed {len(transactions)} transactions")
    print(f"🚨 Generated {fraud_alerts} fraud alerts")


async def demo_multi_agent_coordination():
    """Demonstrate multiple agents working together."""
    print("\n🤝 Multi-Agent Coordination Demo")
    print("-" * 34)

    app = MockApp("customer-pipeline", broker='kafka://localhost:19092')

    # Agent 1: Data validation
    @app.agent('raw-events')
    async def validate_events(stream):
        """First agent: Validate incoming events."""
        print("✅ Validation agent started")

        async for event in stream:
            # Validate
            if all(k in event for k in ['event_id', 'user_id', 'event_type']):
                event['validated'] = True
                event['validation_time'] = time.time()
                yield event

    # Agent 2: Enrichment
    @app.agent('validated-events')
    async def enrich_events(stream):
        """Second agent: Enrich validated events."""
        print("🎨 Enrichment agent started")

        async for event in stream:
            # Add enrichment
            event['enriched'] = True
            event['category'] = random.choice(['premium', 'standard', 'trial'])
            event['priority'] = 'high' if event['category'] == 'premium' else 'normal'
            yield event

    # Agent 3: Analytics
    @app.agent('enriched-events')
    async def analyze_events(stream):
        """Third agent: Generate analytics."""
        print("📊 Analytics agent started")

        high_priority = 0
        async for event in stream:
            if event.get('priority') == 'high':
                high_priority += 1
            yield {"analysis": f"Processed {event['event_id']}", "priority": event['priority']}

    # Simulate multi-agent pipeline
    print("🔄 Simulating 3-agent pipeline: Validation → Enrichment → Analytics")

    raw_events = [
        {"event_id": f"evt_{i}", "user_id": f"user_{i%5}", "event_type": "click"}
        for i in range(15)
    ]

    # Process through pipeline
    validated = [e async for e in validate_events(raw_events)]
    enriched = [e async for e in enrich_events(validated)]
    analyzed = [a async for a in analyze_events(enriched)]

    print(f"✅ Pipeline complete:")
    print(f"   Raw events: {len(raw_events)}")
    print(f"   Validated: {len(validated)}")
    print(f"   Enriched: {len(enriched)}")
    print(f"   Analyzed: {len(analyzed)}")


async def demo_arrow_operations():
    """Demonstrate Arrow operation concepts."""
    print("\n🏹 Arrow Operations Demo")
    print("-" * 24)

    try:
        import pyarrow as pa
        print("✅ PyArrow available - demonstrating columnar operations")

        # Create sample data
        data = {
            'user_id': ['alice', 'bob', 'charlie', 'alice', 'bob'],
            'product': ['widget_a', 'widget_b', 'widget_a', 'widget_c', 'widget_a'],
            'amount': [29.99, 19.99, 29.99, 49.99, 29.99]
        }

        # Create Arrow table
        table = pa.table(data)
        print(f"📊 Created Arrow table: {table.num_rows} rows, {table.num_columns} columns")

        # Filter high-value purchases
        filtered = table.filter(pa.compute.greater(table['amount'], 25.0))
        print(f"🔍 Filtered to high-value purchases: {filtered.num_rows} rows")

        # Aggregate
        total_revenue = pa.compute.sum(filtered['amount']).as_py()
        print(f"💰 Total revenue from high-value purchases: ${total_revenue:.2f}")

        print("📈 Data preview:")
        print(filtered.to_pandas().to_string(index=False))

    except ImportError:
        print("⚠️  PyArrow not available - showing conceptual demo")
        print("📊 Arrow operations provide:")
        print("   • Columnar data storage")
        print("   • SIMD-accelerated operations")
        print("   • Zero-copy data sharing")
        print("   • Efficient memory usage")


async def main():
    """Run all demos."""
    print("🎭 Sabot Concepts Demonstration")
    print("=" * 35)
    print("This demo shows core Sabot patterns without requiring full installation")
    print()

    await demo_basic_agent_pattern()
    await demo_fraud_detection_agent()
    await demo_multi_agent_coordination()
    await demo_arrow_operations()

    print("\n" + "=" * 60)
    print("🎯 Real Sabot Usage (CLI-based deployment):")
    print()
    print("1️⃣  Create your app with @app.agent() decorator:")
    print("    import sabot as sb")
    print("    app = sb.App('myapp', broker='kafka://localhost:19092')")
    print()
    print("    @app.agent('my-topic')")
    print("    async def process_events(stream):")
    print("        async for event in stream:")
    print("            yield processed_event")
    print()
    print("2️⃣  Start infrastructure:")
    print("    docker compose up -d")
    print()
    print("3️⃣  Run with CLI:")
    print("    sabot -A myapp:app worker")
    print()
    print("💡 Full examples: examples/fraud_app.py, examples/core/basic_pipeline.py")


if __name__ == "__main__":
    asyncio.run(main())
