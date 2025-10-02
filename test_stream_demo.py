#!/usr/bin/env python3
"""
Quick Stream Processing Demo - No pandas required
"""
import sys
import time
import random

sys.path.insert(0, '/Users/bengamble/PycharmProjects/pythonProject/sabot')

from sabot import arrow as pa
from sabot.arrow import compute as pc
from sabot.api import Stream

print("=" * 70)
print("Sabot Stream Processing Demo")
print("=" * 70)

# Generate sensor data
print("\n📊 Generating sensor readings...")
sensor_data = []
for i in range(50):
    sensor_data.append({
        'sensor_id': f'sensor_{random.randint(1, 5)}',
        'temperature': round(random.uniform(15.0, 35.0), 2),
        'humidity': round(random.uniform(30.0, 80.0), 2),
        'timestamp': time.time() + i * 0.1
    })

# Create batches
batches = []
batch_size = 10
for i in range(0, len(sensor_data), batch_size):
    chunk = sensor_data[i:i+batch_size]
    batch = pa.RecordBatch.from_pylist(chunk)
    batches.append(batch)

print(f"✓ Generated {len(batches)} batches ({len(sensor_data)} readings)")

# Example 1: Filter hot readings
print("\n" + "=" * 70)
print("Example 1: Filter Hot Readings (> 25°C)")
print("=" * 70)

stream = Stream.from_batches(batches, batches[0].schema)
hot_stream = stream.filter(lambda b: pc.greater(b.column('temperature'), 25.0))
hot_readings = hot_stream.collect()

print(f"\n🔥 Hot readings: {hot_readings.num_rows} / {len(sensor_data)}")
print(f"\nFirst 5 hot readings:")
for i in range(min(5, hot_readings.num_rows)):
    sensor = hot_readings.column('sensor_id')[i].as_py()
    temp = hot_readings.column('temperature')[i].as_py()
    humidity = hot_readings.column('humidity')[i].as_py()
    print(f"   {sensor}: {temp}°C, {humidity}% humidity")

# Example 2: Map transformation (add Fahrenheit)
print("\n" + "=" * 70)
print("Example 2: Add Fahrenheit Conversion")
print("=" * 70)

def add_fahrenheit(batch):
    celsius = batch.column('temperature')
    fahrenheit = pc.add(pc.multiply(celsius, 1.8), 32)

    return pa.RecordBatch.from_arrays(
        [batch.column('sensor_id'), celsius, fahrenheit, batch.column('humidity'), batch.column('timestamp')],
        names=['sensor_id', 'celsius', 'fahrenheit', 'humidity', 'timestamp']
    )

stream = Stream.from_batches(batches, batches[0].schema)
converted_stream = stream.map(add_fahrenheit)
converted = converted_stream.collect()

print(f"\n✓ Converted {converted.num_rows} readings")
print(f"\nFirst 5 with Fahrenheit:")
for i in range(min(5, converted.num_rows)):
    sensor = converted.column('sensor_id')[i].as_py()
    celsius = converted.column('celsius')[i].as_py()
    fahrenheit = converted.column('fahrenheit')[i].as_py()
    print(f"   {sensor}: {celsius}°C = {fahrenheit:.2f}°F")

# Example 3: Chained operations
print("\n" + "=" * 70)
print("Example 3: Chained Operations (filter → map)")
print("=" * 70)

stream = Stream.from_batches(batches, batches[0].schema)
result = stream \
    .filter(lambda b: pc.greater(b.column('temperature'), 25.0)) \
    .map(add_fahrenheit) \
    .collect()

print(f"\n✓ Processed {result.num_rows} hot readings with Fahrenheit")
print(f"\nFirst 3 results:")
for i in range(min(3, result.num_rows)):
    sensor = result.column('sensor_id')[i].as_py()
    celsius = result.column('celsius')[i].as_py()
    fahrenheit = result.column('fahrenheit')[i].as_py()
    humidity = result.column('humidity')[i].as_py()
    print(f"   {sensor}: {celsius}°C ({fahrenheit:.2f}°F), {humidity}% humidity")

# Example 4: Aggregations
print("\n" + "=" * 70)
print("Example 4: Per-Sensor Aggregations")
print("=" * 70)

all_data = Stream.from_batches(batches, batches[0].schema).collect()

sensors = set(all_data.column('sensor_id').to_pylist())
print(f"\n📊 Statistics by sensor:")

for sensor in sorted(sensors):
    # Filter for this sensor
    mask = pc.equal(all_data.column('sensor_id'), sensor)
    sensor_data = all_data.filter(mask)

    # Calculate stats
    temps = sensor_data.column('temperature')
    avg_temp = pc.mean(temps).as_py()
    max_temp = pc.max(temps).as_py()
    min_temp = pc.min(temps).as_py()
    count = sensor_data.num_rows

    print(f"\n   {sensor} ({count} readings):")
    print(f"      Avg: {avg_temp:.2f}°C")
    print(f"      Max: {max_temp:.2f}°C")
    print(f"      Min: {min_temp:.2f}°C")

# Summary
print("\n" + "=" * 70)
print("Summary")
print("=" * 70)
print("""
✓ Stream API working with sabot.arrow module
✓ Filter operations using Arrow compute
✓ Map transformations working
✓ Chained operations functional
✓ Aggregations with pc.mean/max/min working

Performance characteristics:
- Using external PyArrow (USING_EXTERNAL=True)
- Zero-copy Arrow operations
- All operations are Arrow-native
""")

print("=" * 70)
print("✓ Demo complete!")
print("=" * 70)
