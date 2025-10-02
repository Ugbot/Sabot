#!/usr/bin/env python3
import sys
import os

# Add sabot to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

"""
Arrow-Focused Windowing Demo for Sabot

This example demonstrates Sabot's Cython-optimized windowing system
with Arrow integration for high-performance time-based aggregations.
"""

import asyncio
import time
from typing import Dict, Any

import sabot as sb


async def generate_sensor_data(app: sb.App) -> None:
    """Generate streaming sensor data."""
    print("🔄 Generating sensor data...")

    # Create a topic for sensor data
    sensor_topic = app.topic("sensor-readings")

    # Simulate sensor readings
    sensor_id = 0
    while True:
        # Generate batch of sensor readings
        batch = []
        for i in range(10):  # 10 readings per batch
            reading = {
                "sensor_id": f"sensor_{sensor_id % 3}",  # 3 different sensors
                "temperature": 20.0 + (sensor_id % 10),  # Varying temperature
                "humidity": 50.0 + (sensor_id % 20),     # Varying humidity
                "timestamp": time.time(),
                "batch_id": sensor_id // 10
            }
            batch.append(reading)
            sensor_id += 1

        # Send batch to topic
        await sensor_topic.send(batch)
        print(f"📤 Sent batch of {len(batch)} sensor readings")

        await asyncio.sleep(1.0)  # 1 batch per second


async def tumbling_window_demo(app: sb.App) -> None:
    """Demonstrate tumbling windows for periodic aggregations."""
    print("\n🏗️  Tumbling Window Demo (10-second windows)")

    # Get windowed stream factory
    windowed = app.windowed_stream()

    # Create a stream from sensor topic
    sensor_topic = app.topic("sensor-readings")
    sensor_stream = app.stream(sensor_topic)

    # Apply tumbling window (10-second fixed windows)
    tumbling_stream = windowed.tumbling(
        stream=sensor_stream,
        size_seconds=10.0,  # 10-second windows
        key_field="sensor_id",  # Group by sensor
        aggregations={
            "temperature": "mean",  # Average temperature per sensor
            "humidity": "mean",     # Average humidity per sensor
            "sensor_id": "count"    # Count readings per sensor
        }
    )

    # Process windowed results
    window_count = 0
    async for window in tumbling_stream:
        window_count += 1
        print(f"\n🏁 Window {window_count} completed:")
        print(f"   Sensor: {window.get('key', 'unknown')}")
        print(".1f")
        print(".1f")
        print(f"   Reading count: {window.get('aggregations', {}).get('sensor_id', 0)}")
        print(f"   Window start: {time.ctime(window['start_time'])}")

        # Stop after 3 windows for demo
        if window_count >= 3:
            break


async def sliding_window_demo(app: sb.App) -> None:
    """Demonstrate sliding windows for rolling aggregations."""
    print("\n🔄 Sliding Window Demo (30-second windows, 10-second slide)")

    windowed = app.windowed_stream()
    sensor_topic = app.topic("sensor-readings")
    sensor_stream = app.stream(sensor_topic)

    # Sliding window: 30-second windows that slide every 10 seconds
    sliding_stream = windowed.sliding(
        stream=sensor_stream,
        size_seconds=30.0,    # 30-second window size
        slide_seconds=10.0,   # Slide every 10 seconds
        key_field="sensor_id",
        aggregations={
            "temperature": "max",   # Max temperature in window
            "humidity": "min",      # Min humidity in window
            "sensor_id": "count"
        }
    )

    window_count = 0
    async for window in sliding_stream:
        window_count += 1
        print(f"\n🌊 Sliding Window {window_count}:")
        print(f"   Sensor: {window.get('key', 'unknown')}")
        print(".1f")
        print(".1f")
        print(f"   Reading count: {window.get('aggregations', {}).get('sensor_id', 0)}")

        if window_count >= 5:  # Show more sliding windows
            break


async def hopping_window_demo(app: sb.App) -> None:
    """Demonstrate hopping windows for sampled aggregations."""
    print("\n🏃 Hopping Window Demo (20-second windows, 5-second hops)")

    windowed = app.windowed_stream()
    sensor_topic = app.topic("sensor-readings")
    sensor_stream = app.stream(sensor_topic)

    # Hopping window: 20-second windows that hop every 5 seconds
    hopping_stream = windowed.hopping(
        stream=sensor_stream,
        size_seconds=20.0,    # 20-second window size
        hop_seconds=5.0,      # Hop every 5 seconds (overlapping)
        key_field="sensor_id",
        aggregations={
            "temperature": "sum",   # Sum of temperatures
            "humidity": "mean",     # Average humidity
            "sensor_id": "count"
        }
    )

    window_count = 0
    async for window in hopping_stream:
        window_count += 1
        print(f"\n🏃 Hopping Window {window_count}:")
        print(f"   Sensor: {window.get('key', 'unknown')}")
        print(".1f")
        print(".1f")
        print(f"   Reading count: {window.get('aggregations', {}).get('sensor_id', 0)}")

        if window_count >= 4:
            break


async def session_window_demo(app: sb.App) -> None:
    """Demonstrate session windows for activity-based grouping."""
    print("\n🎭 Session Window Demo (5-second timeout)")

    windowed = app.windowed_stream()
    sensor_topic = app.topic("sensor-readings")
    sensor_stream = app.stream(sensor_topic)

    # Session window: Group readings by sensor with 5-second inactivity timeout
    session_stream = windowed.session(
        stream=sensor_stream,
        timeout_seconds=5.0,   # 5-second session timeout
        key_field="sensor_id",
        aggregations={
            "temperature": "mean",
            "humidity": "mean",
            "sensor_id": "count"
        }
    )

    window_count = 0
    async for window in session_stream:
        window_count += 1
        print(f"\n🎭 Session Window {window_count}:")
        print(f"   Sensor: {window.get('key', 'unknown')}")
        print(".1f")
        print(".1f")
        print(f"   Session duration: {(window['end_time'] - window['start_time']):.1f}s")
        print(f"   Reading count: {window.get('aggregations', {}).get('sensor_id', 0)}")

        if window_count >= 6:  # Show session behavior
            break


async def performance_comparison_demo(app: sb.App) -> None:
    """Compare performance of different window types."""
    print("\n⚡ Performance Comparison")

    # Get window stats
    windowed = app.windowed_stream()
    stats = await windowed.get_stats()

    print("Window Manager Stats:")
    for key, value in stats.items():
        print(f"  {key}: {value}")

    # Show Cython availability
    try:
        from sabot.windows import CYTHON_AVAILABLE
        print(f"  Cython Available: {CYTHON_AVAILABLE}")
    except ImportError:
        print("  Cython Available: False")


async def main():
    """Run all windowing demos."""
    print("🪟 Sabot Arrow-Focused Windowing System Demo")
    print("=" * 50)

    # Create app
    app = sb.create_app("windowing-demo")

    try:
        await app.start()

        # Create tasks for concurrent execution
        tasks = []

        # Data generator (runs continuously)
        tasks.append(asyncio.create_task(generate_sensor_data(app)))

        # Wait a bit for data to start flowing
        await asyncio.sleep(2)

        # Run window demos sequentially
        await tumbling_window_demo(app)
        await asyncio.sleep(2)

        await sliding_window_demo(app)
        await asyncio.sleep(2)

        await hopping_window_demo(app)
        await asyncio.sleep(2)

        await session_window_demo(app)
        await asyncio.sleep(2)

        await performance_comparison_demo(app)

        # Cancel data generator
        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

    except KeyboardInterrupt:
        print("\n🛑 Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await app.stop()

    print("\n✅ Windowing demo completed!")
    print("\nKey Features Demonstrated:")
    print("• 🏗️  Tumbling windows - Fixed-size, non-overlapping")
    print("• 🔄 Sliding windows - Fixed-size, overlapping")
    print("• 🏃 Hopping windows - Custom hop intervals")
    print("• 🎭 Session windows - Activity-based grouping")
    print("• ⚡ Cython optimization for high performance")
    print("• 📊 Arrow compute integration for aggregations")


if __name__ == "__main__":
    asyncio.run(main())
