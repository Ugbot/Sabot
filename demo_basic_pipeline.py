#!/usr/bin/env python3
"""
Demo: End-to-end test of basic_pipeline example.

This demonstrates that the converted example works correctly by:
1. Importing the app with registered agents
2. Simulating streaming data
3. Processing through the agent
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from examples.core.basic_pipeline import app, process_sensors


class MockStream:
    """Mock stream for testing."""

    def __init__(self, data):
        self.data = data

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.data:
            raise StopAsyncIteration
        return self.data.pop(0)


async def main():
    """Run basic pipeline demo."""
    print("🎬 Basic Pipeline Demo")
    print("=" * 60)

    # Create test data
    test_data = [
        {"sensor_id": "sensor_0", "temperature": 22.5, "humidity": 55.0, "timestamp": 1234567890},
        {"sensor_id": "sensor_1", "temperature": 28.0, "humidity": 60.0, "timestamp": 1234567891},
        {"sensor_id": "sensor_2", "temperature": 31.5, "humidity": 65.0, "timestamp": 1234567892},
        {"sensor_id": "sensor_3", "temperature": 24.0, "humidity": 50.0, "timestamp": 1234567893},
        {"sensor_id": "sensor_4", "temperature": 33.0, "humidity": 70.0, "timestamp": 1234567894},
    ]

    print(f"\n📊 Processing {len(test_data)} sensor readings...")
    print()

    # Create mock stream
    mock_stream = MockStream(test_data.copy())

    # Process through agent
    results = []
    async for result in process_sensors(mock_stream):
        results.append(result)

    print(f"\n✅ Processed {len(results)} alerts")
    print("\n📋 Results:")
    for i, result in enumerate(results, 1):
        alert_emoji = "🔥" if result.get("alert_level") == "HIGH" else "⚠️"
        print(f"   {i}. {alert_emoji} {result['sensor_id']}: {result['temperature']}°C "
              f"(Alert: {result.get('alert_level', 'NONE')})")

    print("\n" + "=" * 60)
    print("✅ Demo completed successfully!")
    print("\n📝 What happened:")
    print("   - Imported example with @app.agent() decorator")
    print("   - Simulated streaming data")
    print("   - Processed through agent function")
    print("   - Agent filtered and added alert levels")
    print("   - Results yielded back")

    print("\n🚀 To run with real Kafka:")
    print("   1. python -m sabot -A examples.core.basic_pipeline:app worker")
    print("   2. Send data to 'sensor-readings' topic")
    print("   3. Agent will process and yield results")


if __name__ == "__main__":
    asyncio.run(main())
