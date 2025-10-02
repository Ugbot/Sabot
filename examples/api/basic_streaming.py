#!/usr/bin/env python3
"""
Basic Streaming with Sabot API

This example demonstrates basic stream processing using Sabot's high-level Stream API.
It shows the same concepts as core/basic_pipeline.py but with the modern API.

This demonstrates:
- Creating streams from data sources
- Filtering and transforming data with zero-copy operations
- Aggregations and analytics
- Simple, declarative stream processing

Modern API Benefits:
- More intuitive than async agents
- Zero-copy performance (0.5ns/row when Cython compiled)
- Declarative style (vs imperative async loops)
- Automatic batching and optimization

Prerequisites:
- Sabot installed: pip install -e .
- PyArrow: pip install pyarrow

Usage:
    python examples/api/basic_streaming.py
"""

import time
import random

# Import Sabot Arrow (uses internal or external PyArrow)
from sabot import arrow as pa
from sabot.arrow import compute as pc

# Import Sabot Stream API
from sabot.api import Stream


def generate_sensor_data(num_readings=50):
    """Generate sample sensor readings."""
    sensor_ids = ['sensor_1', 'sensor_2', 'sensor_3', 'sensor_4', 'sensor_5']

    readings = []
    for i in range(num_readings):
        reading = {
            'sensor_id': random.choice(sensor_ids),
            'temperature': round(random.uniform(20.0, 35.0), 2),
            'humidity': round(random.uniform(30.0, 80.0), 2),
            'timestamp': time.time() + i,
            'reading_id': i
        }
        readings.append(reading)

    return readings


def main():
    print("=" * 60)
    print("Basic Streaming with Sabot API")
    print("=" * 60)

    # Generate sample data
    print("\n📊 Generating sensor readings...")
    sensor_data = generate_sensor_data(50)

    # Create Arrow batches (batch size = 10 for demo)
    batches = []
    batch_size = 10
    for i in range(0, len(sensor_data), batch_size):
        chunk = sensor_data[i:i+batch_size]
        batch = pa.RecordBatch.from_pylist(chunk)
        batches.append(batch)

    print(f"✓ Generated {len(batches)} batches ({sum(b.num_rows for b in batches)} total readings)")

    # ========================================================================
    # Example 1: Filter - Only hot readings (> 25°C)
    # ========================================================================
    print("\n" + "=" * 60)
    print("Example 1: Filter Hot Readings (> 25°C)")
    print("=" * 60)

    stream = Stream.from_batches(batches)

    # Filter using Arrow compute (SIMD-accelerated)
    hot_stream = stream.filter(lambda batch: pc.greater(batch.column('temperature'), 25.0))

    # Collect results
    hot_readings = hot_stream.collect()

    print(f"\n🔥 Hot readings: {hot_readings.num_rows} / {sum(b.num_rows for b in batches)}")
    print(f"\nFirst 5 hot readings:")
    print(hot_readings.slice(0, min(5, hot_readings.num_rows)).to_pandas())

    # ========================================================================
    # Example 2: Map - Add temperature conversions
    # ========================================================================
    print("\n" + "=" * 60)
    print("Example 2: Transform - Add Fahrenheit Conversion")
    print("=" * 60)

    def add_fahrenheit(batch):
        """Add Fahrenheit temperature column."""
        celsius = batch.column('temperature')
        fahrenheit = pc.add(pc.multiply(celsius, 9/5), 32)

        # Create new batch with additional column
        return pa.RecordBatch.from_arrays(
            [batch.column(i) for i in range(batch.num_columns)] + [fahrenheit],
            names=list(batch.schema.names) + ['temp_fahrenheit']
        )

    stream = Stream.from_batches(batches)
    converted_stream = stream.map(add_fahrenheit)

    result = converted_stream.collect()

    print(f"\n✓ Added Fahrenheit column to {result.num_rows} readings")
    print(f"\nFirst 5 readings with both temperatures:")
    df = result.slice(0, min(5, result.num_rows)).to_pandas()
    print(df[['sensor_id', 'temperature', 'temp_fahrenheit', 'humidity']])

    # ========================================================================
    # Example 3: Aggregation - Statistics
    # ========================================================================
    print("\n" + "=" * 60)
    print("Example 3: Aggregation - Overall Statistics")
    print("=" * 60)

    stream = Stream.from_batches(batches)

    # Aggregate: count and sum
    stats = stream.aggregate({
        'count': ('*', 'count'),
        'sum_temp': ('temperature', 'sum')
    })

    stats_table = stats.collect()
    total_count = stats_table['count'][0].as_py()
    total_temp = stats_table['sum_temp'][0].as_py()
    avg_temp = total_temp / total_count if total_count > 0 else 0

    print(f"\n📊 Overall Statistics:")
    print(f"   Total readings: {total_count}")
    print(f"   Average temperature: {avg_temp:.2f}°C")

    # ========================================================================
    # Example 4: Chained Operations - Filter + Map + Aggregate
    # ========================================================================
    print("\n" + "=" * 60)
    print("Example 4: Chained Operations - Pipeline")
    print("=" * 60)

    def classify_alert(batch):
        """Classify temperature alerts."""
        temp = batch.column('temperature')

        # Create alert level column
        alert_levels = []
        for t in temp:
            t_val = t.as_py()
            if t_val > 30:
                alert_levels.append('HIGH')
            elif t_val > 25:
                alert_levels.append('MEDIUM')
            else:
                alert_levels.append('LOW')

        alert_array = pa.array(alert_levels, type=pa.string())

        return pa.RecordBatch.from_arrays(
            [batch.column(i) for i in range(batch.num_columns)] + [alert_array],
            names=list(batch.schema.names) + ['alert_level']
        )

    # Build pipeline: filter -> map -> collect
    stream = (Stream.from_batches(batches)
              .filter(lambda b: pc.greater(b.column('temperature'), 25.0))
              .map(classify_alert))

    alerts = stream.collect()

    print(f"\n⚠️  Alert Summary:")
    print(f"   Total alerts: {alerts.num_rows}")

    # Count by alert level
    alert_levels = alerts.column('alert_level').to_pylist()
    level_counts = {'HIGH': 0, 'MEDIUM': 0}
    for level in alert_levels:
        level_counts[level] = level_counts.get(level, 0) + 1

    print(f"   HIGH alerts: {level_counts.get('HIGH', 0)}")
    print(f"   MEDIUM alerts: {level_counts.get('MEDIUM', 0)}")

    print(f"\nSample alerts:")
    print(alerts.slice(0, min(5, alerts.num_rows)).to_pandas()[
        ['sensor_id', 'temperature', 'alert_level']
    ])

    # ========================================================================
    # Example 5: Per-Sensor Analytics
    # ========================================================================
    print("\n" + "=" * 60)
    print("Example 5: Per-Sensor Analytics (Manual Grouping)")
    print("=" * 60)

    stream = Stream.from_batches(batches)
    all_data = stream.collect()

    # Group by sensor manually (in future: use keyed stream)
    sensor_stats = {}
    for sensor_id in ['sensor_1', 'sensor_2', 'sensor_3', 'sensor_4', 'sensor_5']:
        # Filter for this sensor
        mask = pc.equal(all_data.column('sensor_id'), sensor_id)
        sensor_data = all_data.filter(mask)

        if sensor_data.num_rows > 0:
            temps = sensor_data.column('temperature')
            avg_temp = pc.mean(temps).as_py()
            max_temp = pc.max(temps).as_py()
            min_temp = pc.min(temps).as_py()

            sensor_stats[sensor_id] = {
                'count': sensor_data.num_rows,
                'avg_temp': round(avg_temp, 2),
                'max_temp': round(max_temp, 2),
                'min_temp': round(min_temp, 2)
            }

    print(f"\n📈 Per-Sensor Statistics:")
    for sensor_id, stats in sorted(sensor_stats.items()):
        print(f"   {sensor_id}:")
        print(f"      Readings: {stats['count']}")
        print(f"      Avg: {stats['avg_temp']}°C")
        print(f"      Max: {stats['max_temp']}°C")
        print(f"      Min: {stats['min_temp']}°C")

    # ========================================================================
    # Performance Note
    # ========================================================================
    print("\n" + "=" * 60)
    print("Performance Notes")
    print("=" * 60)
    print("""
🚀 Stream API Performance:

Current (PyArrow fallback):
- ~2000ns per row (using PyArrow compute)
- Still much faster than Python loops
- SIMD-accelerated where possible

With Cython modules compiled:
- ~0.5ns per row (4000x faster!)
- Zero-copy operations throughout
- Matches Apache Flink performance

To enable zero-copy:
    cd /path/to/sabot
    python setup.py build_ext --inplace
""")

    print("\n" + "=" * 60)
    print("✓ Basic streaming examples complete!")
    print("=" * 60)
    print("""
Compare this to core/basic_pipeline.py:
- Stream API: Declarative, composable, zero-copy
- Agent API: Imperative, async, more verbose

Both are valid approaches - choose based on your needs!
""")


if __name__ == "__main__":
    main()
