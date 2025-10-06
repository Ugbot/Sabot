#!/usr/bin/env python3
"""
Windowed Stream Processing Demo - Real-time analytics simulation
"""
import sys
import time
import random
from datetime import datetime

sys.path.insert(0, '/Users/bengamble/PycharmProjects/pythonProject/sabot')

from sabot import cyarrow as pa
from sabot.cyarrow import compute as pc
from sabot.api import Stream

def format_time(ts):
    """Format timestamp for display."""
    return datetime.fromtimestamp(ts).strftime('%H:%M:%S')

print("=" * 70)
print("Windowed Stream Processing Demo")
print("=" * 70)

# Generate time-series data (API requests over 60 seconds)
print("\n📊 Generating API request data...")
base_time = time.time()
requests = []

for i in range(120):
    # Spread requests across 60 seconds
    timestamp = base_time + (60 * i / 120)

    # Simulate varying load
    if i % 20 < 10:  # Normal load
        response_time = random.uniform(0.1, 0.5)
    else:  # Higher load
        response_time = random.uniform(0.5, 2.0)

    # Simulate occasional errors
    status_code = 200 if random.random() > 0.05 else random.choice([400, 500])

    requests.append({
        'request_id': f'req_{i}',
        'endpoint': random.choice(['/api/users', '/api/orders', '/api/products']),
        'response_time': round(response_time, 3),
        'status_code': status_code,
        'timestamp': timestamp,
        'bytes_sent': random.randint(100, 5000)
    })

# Create batches
batches = []
batch_size = 10
for i in range(0, len(requests), batch_size):
    chunk = requests[i:i+batch_size]
    batch = pa.RecordBatch.from_pylist(chunk)
    batches.append(batch)

print(f"✓ Generated {len(batches)} batches ({len(requests)} requests)")
print(f"   Time range: {format_time(requests[0]['timestamp'])} - {format_time(requests[-1]['timestamp'])}")

# Collect all data for windowed analysis
stream = Stream.from_batches(batches, batches[0].schema)
all_data = stream.collect()

# Example 1: Tumbling Windows (10-second intervals)
print("\n" + "=" * 70)
print("Example 1: Tumbling Windows (10-second intervals)")
print("=" * 70)

print("\n📌 Non-overlapping time windows for request analytics")

min_ts = pc.min(all_data.column('timestamp')).as_py()
max_ts = pc.max(all_data.column('timestamp')).as_py()

window_size = 10.0
current_window_start = int(min_ts / window_size) * window_size

print(f"\n📊 Request metrics per 10-second window:")

window_num = 1
while current_window_start <= max_ts:
    window_end = current_window_start + window_size

    # Filter data in this window
    in_window = pc.and_(
        pc.greater_equal(all_data.column('timestamp'), current_window_start),
        pc.less(all_data.column('timestamp'), window_end)
    )
    window_data = all_data.filter(in_window)

    if window_data.num_rows > 0:
        # Calculate metrics
        request_count = window_data.num_rows
        avg_response_time = pc.mean(window_data.column('response_time')).as_py()
        total_bytes = pc.sum(window_data.column('bytes_sent')).as_py()

        # Count errors
        errors = pc.greater_equal(window_data.column('status_code'), 400)
        error_count = pc.sum(pc.cast(errors, pa.int64())).as_py()
        error_rate = (error_count / request_count * 100) if request_count > 0 else 0

        print(f"\n   Window {window_num} [{format_time(current_window_start)} - {format_time(window_end)})")
        print(f"      Requests: {request_count}")
        print(f"      Avg Response: {avg_response_time:.3f}s")
        print(f"      Total Bytes: {total_bytes:,}")
        print(f"      Errors: {error_count} ({error_rate:.1f}%)")

        window_num += 1

    current_window_start = window_end

# Example 2: Per-Endpoint Analytics
print("\n" + "=" * 70)
print("Example 2: Per-Endpoint Analytics (10-second windows)")
print("=" * 70)

endpoints = list(set(all_data.column('endpoint').to_pylist()))

for endpoint in sorted(endpoints):
    print(f"\n📍 Endpoint: {endpoint}")

    # Filter for this endpoint
    endpoint_mask = pc.equal(all_data.column('endpoint'), endpoint)
    endpoint_data = all_data.filter(endpoint_mask)

    if endpoint_data.num_rows == 0:
        print(f"   (No data)")
        continue

    # Windowed analysis
    current_window_start = int(min_ts / window_size) * window_size
    window_count = 0

    while current_window_start <= max_ts and window_count < 3:  # Show first 3 windows
        window_end = current_window_start + window_size

        in_window = pc.and_(
            pc.greater_equal(endpoint_data.column('timestamp'), current_window_start),
            pc.less(endpoint_data.column('timestamp'), window_end)
        )
        window_data = endpoint_data.filter(in_window)

        if window_data.num_rows > 0:
            count = window_data.num_rows
            avg_rt = pc.mean(window_data.column('response_time')).as_py()

            errors = pc.greater_equal(window_data.column('status_code'), 400)
            error_count = pc.sum(pc.cast(errors, pa.int64())).as_py()

            print(f"   [{format_time(current_window_start)}-{format_time(window_end)}): "
                  f"{count} reqs, Avg: {avg_rt:.3f}s, Errors: {error_count}")
            window_count += 1

        current_window_start = window_end

# Example 3: Real-time Dashboard Metrics
print("\n" + "=" * 70)
print("Example 3: Real-Time Dashboard (20-second sliding window)")
print("=" * 70)

print("\n📌 Overlapping windows for trend analysis")

window_size = 20.0
slide = 10.0
current_window_start = int(min_ts / slide) * slide

update_count = 0
print(f"\n📊 Dashboard updates (every {slide}s showing last {window_size}s):")

while current_window_start <= max_ts and update_count < 4:  # Show 4 updates
    window_end = current_window_start + window_size

    in_window = pc.and_(
        pc.greater_equal(all_data.column('timestamp'), current_window_start),
        pc.less(all_data.column('timestamp'), window_end)
    )
    window_data = all_data.filter(in_window)

    if window_data.num_rows > 0:
        total_requests = window_data.num_rows

        errors = pc.greater_equal(window_data.column('status_code'), 400)
        error_count = pc.sum(pc.cast(errors, pa.int64())).as_py()
        error_rate = (error_count / total_requests) * 100

        avg_response_time = pc.mean(window_data.column('response_time')).as_py()
        max_response_time = pc.max(window_data.column('response_time')).as_py()

        print(f"\n   ⏰ Update {update_count + 1} @ {format_time(window_end)}")
        print(f"   ╔════════════════════════════════════╗")
        print(f"   ║  📊 Dashboard (last {window_size:.0f}s)         ║")
        print(f"   ╠════════════════════════════════════╣")
        print(f"   ║  Requests: {total_requests:21d} ║")
        print(f"   ║  Error Rate: {error_rate:18.1f}% ║")
        print(f"   ║  Avg Response: {avg_response_time:15.3f}s ║")
        print(f"   ║  Max Response: {max_response_time:15.3f}s ║")
        print(f"   ╚════════════════════════════════════╝")

        update_count += 1

    current_window_start += slide

# Summary
print("\n" + "=" * 70)
print("Summary")
print("=" * 70)
print("""
✓ Tumbling windows: Non-overlapping intervals
✓ Per-group analytics: Endpoint-level metrics
✓ Sliding windows: Overlapping time ranges for trends
✓ Real-time aggregations: Count, mean, sum, max

Window Types Demonstrated:
- Tumbling (10s): Each event in exactly one window
- Sliding (20s/10s): Overlapping for rolling metrics
- Per-key: Group by endpoint within windows

This uses Arrow compute for SIMD-accelerated aggregations
achieving 10-100x speedup vs Python loops.
""")

print("=" * 70)
print("✓ Windowed analytics demo complete!")
print("=" * 70)
