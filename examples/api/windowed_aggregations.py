#!/usr/bin/env python3
"""
Windowed Aggregations with Sabot API

This example demonstrates windowed analytics using Sabot's high-level Window API.
It shows the same concepts as streaming/windowed_analytics.py but with the modern API.

This demonstrates:
- Tumbling windows (non-overlapping, fixed intervals)
- Sliding windows (overlapping, rolling calculations)
- Session windows (gap-based, dynamic)
- Time-based aggregations
- Real-time analytics dashboards

Modern API Benefits:
- Declarative window specifications
- Automatic window assignment and lifecycle
- Built-in watermark handling (when enabled)
- Zero-copy aggregations

Prerequisites:
- Sabot installed: pip install -e .
- PyArrow: pip install pyarrow

Usage:
    python examples/api/windowed_aggregations.py
"""

import time
import random
from datetime import datetime

# Import Sabot Arrow (uses internal or external PyArrow)
from sabot import arrow as pa
from sabot.arrow import compute as pc

# Import Sabot API
from sabot.api import Stream, tumbling, sliding, session


def generate_api_requests(num_requests=100, duration_seconds=30):
    """Generate sample API request data over a time period."""
    endpoints = ["/api/users", "/api/orders", "/api/products", "/api/analytics"]
    methods = ["GET", "POST", "PUT", "DELETE"]
    users = [f"user_{i}" for i in range(1, 21)]

    base_time = time.time()
    requests = []

    for i in range(num_requests):
        # Spread requests across duration
        timestamp = base_time + (duration_seconds * i / num_requests)

        # Simulate different response times
        if random.random() < 0.1:  # 10% slow
            response_time = random.uniform(2.0, 10.0)
        else:
            response_time = random.uniform(0.1, 1.0)

        # Simulate occasional errors
        status_code = 200
        if random.random() < 0.05:  # 5% error rate
            status_code = random.choice([400, 404, 500])

        request = {
            'request_id': f"req_{int(timestamp * 1000)}_{i}",
            'endpoint': random.choice(endpoints),
            'method': random.choice(methods),
            'user_id': random.choice(users),
            'response_time': round(response_time, 3),
            'status_code': status_code,
            'timestamp': timestamp,
            'bytes_sent': random.randint(100, 10000)
        }
        requests.append(request)

    return requests


def format_time(ts):
    """Format timestamp for display."""
    return datetime.fromtimestamp(ts).strftime('%H:%M:%S')


def main():
    print("=" * 70)
    print("Windowed Aggregations with Sabot API")
    print("=" * 70)

    # Generate sample data
    print("\n📊 Generating API request data (30 seconds worth)...")
    requests = generate_api_requests(num_requests=100, duration_seconds=30)

    # Create Arrow batches
    batches = []
    batch_size = 10
    for i in range(0, len(requests), batch_size):
        chunk = requests[i:i+batch_size]
        batch = pa.RecordBatch.from_pylist(chunk)
        batches.append(batch)

    print(f"✓ Generated {len(batches)} batches ({sum(b.num_rows for b in batches)} requests)")
    print(f"   Time range: {format_time(requests[0]['timestamp'])} - {format_time(requests[-1]['timestamp'])}")

    # ========================================================================
    # Example 1: Tumbling Windows (10-second intervals)
    # ========================================================================
    print("\n" + "=" * 70)
    print("Example 1: Tumbling Windows (10-second intervals)")
    print("=" * 70)

    print("\n📌 Tumbling windows partition time into non-overlapping intervals.")
    print("   Each event belongs to exactly one window.")

    # Create stream
    stream = Stream.from_batches(batches)

    # Apply 10-second tumbling window
    window_spec = tumbling(seconds=10)
    print(f"\n🕐 Window specification: {window_spec}")

    # For demonstration, manually group by time windows
    # (In production, this would be automatic with event-time processing)
    all_data = stream.collect()

    # Calculate window boundaries
    min_ts = pc.min(all_data.column('timestamp')).as_py()
    max_ts = pc.max(all_data.column('timestamp')).as_py()

    print(f"\n📊 Analyzing data across tumbling windows:")

    window_size = 10.0
    current_window_start = int(min_ts / window_size) * window_size

    while current_window_start <= max_ts:
        window_end = current_window_start + window_size

        # Filter data in this window
        in_window = pc.and_(
            pc.greater_equal(all_data.column('timestamp'), current_window_start),
            pc.less(all_data.column('timestamp'), window_end)
        )
        window_data = all_data.filter(in_window)

        if window_data.num_rows > 0:
            # Aggregate window data
            request_count = window_data.num_rows
            avg_response_time = pc.mean(window_data.column('response_time')).as_py()
            total_bytes = pc.sum(window_data.column('bytes_sent')).as_py()

            # Count errors
            errors = pc.greater_equal(window_data.column('status_code'), 400)
            error_count = pc.sum(pc.cast(errors, pa.int64())).as_py()

            print(f"   🕐 [{format_time(current_window_start)} - {format_time(window_end)})")
            print(f"      Requests: {request_count}")
            print(f"      Avg Response: {avg_response_time:.2f}s")
            print(f"      Total Bytes: {total_bytes:,}")
            print(f"      Errors: {error_count}")

        current_window_start = window_end

    # ========================================================================
    # Example 2: Sliding Windows (20s window, 10s slide)
    # ========================================================================
    print("\n" + "=" * 70)
    print("Example 2: Sliding Windows (20s window, 10s slide)")
    print("=" * 70)

    print("\n📌 Sliding windows overlap - events can appear in multiple windows.")
    print("   Good for rolling averages and trend analysis.")

    window_spec = sliding(size_seconds=20, slide_seconds=10)
    print(f"\n🕐 Window specification: {window_spec}")

    print(f"\n📊 Rolling window analysis:")

    window_size = 20.0
    slide = 10.0
    current_window_start = int(min_ts / slide) * slide

    while current_window_start <= max_ts:
        window_end = current_window_start + window_size

        # Filter data in this window
        in_window = pc.and_(
            pc.greater_equal(all_data.column('timestamp'), current_window_start),
            pc.less(all_data.column('timestamp'), window_end)
        )
        window_data = all_data.filter(in_window)

        if window_data.num_rows > 0:
            # Calculate rolling metrics
            request_count = window_data.num_rows
            avg_response_time = pc.mean(window_data.column('response_time')).as_py()

            # Error rate
            errors = pc.greater_equal(window_data.column('status_code'), 400)
            error_count = pc.sum(pc.cast(errors, pa.int64())).as_py()
            error_rate = (error_count / request_count) * 100 if request_count > 0 else 0

            # Unique users
            unique_users = len(set(window_data.column('user_id').to_pylist()))

            print(f"   🕐 [{format_time(current_window_start)} - {format_time(window_end)})")
            print(f"      Requests: {request_count}")
            print(f"      Avg Response: {avg_response_time:.2f}s")
            print(f"      Error Rate: {error_rate:.1f}%")
            print(f"      Unique Users: {unique_users}")

        current_window_start += slide

    # ========================================================================
    # Example 3: Per-Endpoint Analytics with Windows
    # ========================================================================
    print("\n" + "=" * 70)
    print("Example 3: Per-Endpoint Tumbling Windows (15s)")
    print("=" * 70)

    print("\n📌 Combining grouping with windowing for detailed analytics.")

    window_size = 15.0
    current_window_start = int(min_ts / window_size) * window_size

    endpoints = ["/api/users", "/api/orders", "/api/products", "/api/analytics"]

    print(f"\n📊 Per-endpoint window analysis:")

    for endpoint in endpoints:
        print(f"\n   🌐 Endpoint: {endpoint}")

        # Filter for this endpoint
        endpoint_mask = pc.equal(all_data.column('endpoint'), endpoint)
        endpoint_data = all_data.filter(endpoint_mask)

        if endpoint_data.num_rows == 0:
            print(f"      (No data)")
            continue

        # Analyze windows for this endpoint
        ws = current_window_start
        while ws <= max_ts:
            we = ws + window_size

            in_window = pc.and_(
                pc.greater_equal(endpoint_data.column('timestamp'), ws),
                pc.less(endpoint_data.column('timestamp'), we)
            )
            window_data = endpoint_data.filter(in_window)

            if window_data.num_rows > 0:
                count = window_data.num_rows
                avg_rt = pc.mean(window_data.column('response_time')).as_py()

                errors = pc.greater_equal(window_data.column('status_code'), 400)
                error_count = pc.sum(pc.cast(errors, pa.int64())).as_py()

                print(f"      [{format_time(ws)} - {format_time(we)}): "
                      f"{count} reqs, "
                      f"Avg RT: {avg_rt:.2f}s, "
                      f"Errors: {error_count}")

            ws = we

    # ========================================================================
    # Example 4: Dashboard Metrics with Sliding Windows
    # ========================================================================
    print("\n" + "=" * 70)
    print("Example 4: Real-Time Dashboard (30s sliding window, 5s updates)")
    print("=" * 70)

    print("\n📌 Simulating real-time dashboard updates with overlapping windows.")

    window_size = 30.0
    slide = 5.0

    print(f"\n📊 Dashboard updates every {slide}s (showing last {window_size}s):")

    current_window_start = int(min_ts / slide) * slide
    update_count = 0

    while current_window_start <= max_ts and update_count < 6:  # Show first 6 updates
        window_end = current_window_start + window_size

        in_window = pc.and_(
            pc.greater_equal(all_data.column('timestamp'), current_window_start),
            pc.less(all_data.column('timestamp'), window_end)
        )
        window_data = all_data.filter(in_window)

        if window_data.num_rows > 0:
            # Calculate dashboard metrics
            total_requests = window_data.num_rows

            errors = pc.greater_equal(window_data.column('status_code'), 400)
            error_count = pc.sum(pc.cast(errors, pa.int64())).as_py()
            error_rate = (error_count / total_requests) * 100

            avg_response_time = pc.mean(window_data.column('response_time')).as_py()
            active_users = len(set(window_data.column('user_id').to_pylist()))

            print(f"\n   ⏰ Update {update_count + 1} @ {format_time(current_window_start + window_size)}")
            print(f"   ╔═══════════════════════════════════════╗")
            print(f"   ║  📊 Dashboard Metrics                ║")
            print(f"   ╠═══════════════════════════════════════╣")
            print(f"   ║  📈 Total Requests: {total_requests:17d} ║")
            print(f"   ║  ❌ Error Rate: {error_rate:20.1f}% ║")
            print(f"   ║  ⏱️  Avg Response: {avg_response_time:17.2f}s ║")
            print(f"   ║  👥 Active Users: {active_users:19d} ║")
            print(f"   ╚═══════════════════════════════════════╝")

            update_count += 1

        current_window_start += slide

    # ========================================================================
    # Example 5: Session Windows (concept demonstration)
    # ========================================================================
    print("\n" + "=" * 70)
    print("Example 5: Session Windows (5-second inactivity gap)")
    print("=" * 70)

    print("\n📌 Session windows group events with <5s gaps into sessions.")
    print("   Windows close after 5 seconds of inactivity.")

    gap_threshold = 5.0

    # Sort data by timestamp
    sorted_data = all_data.sort_by([('timestamp', 'ascending')])

    print(f"\n📊 Detected sessions:")

    session_start = sorted_data.column('timestamp')[0].as_py()
    session_events = []
    session_num = 1

    for i in range(sorted_data.num_rows):
        event_time = sorted_data.column('timestamp')[i].as_py()

        # Check gap from previous event
        if session_events:
            prev_time = sorted_data.column('timestamp')[i-1].as_py()
            gap = event_time - prev_time

            if gap > gap_threshold:
                # Session ended - emit current session
                session_end = prev_time

                print(f"   🔷 Session {session_num}:")
                print(f"      Duration: [{format_time(session_start)} - {format_time(session_end)}] "
                      f"({session_end - session_start:.1f}s)")
                print(f"      Events: {len(session_events)}")

                # Start new session
                session_num += 1
                session_start = event_time
                session_events = []

        session_events.append(i)

    # Emit final session
    if session_events:
        session_end = sorted_data.column('timestamp')[sorted_data.num_rows - 1].as_py()
        print(f"   🔷 Session {session_num}:")
        print(f"      Duration: [{format_time(session_start)} - {format_time(session_end)}] "
              f"({session_end - session_start:.1f}s)")
        print(f"      Events: {len(session_events)}")

    # ========================================================================
    # Summary
    # ========================================================================
    print("\n" + "=" * 70)
    print("Window API Summary")
    print("=" * 70)
    print("""
🕐 Window Types Available:

1. Tumbling Windows - tumbling(seconds=10)
   • Non-overlapping, fixed intervals
   • Each event in exactly one window
   • Use for: periodic reports, aggregations

2. Sliding Windows - sliding(size_seconds=20, slide_seconds=10)
   • Overlapping windows
   • Events can appear in multiple windows
   • Use for: rolling averages, trends

3. Session Windows - session(gap_seconds=5)
   • Dynamic windows based on inactivity
   • Windows close after gap
   • Use for: user sessions, activity bursts

🚀 With Event-Time Processing (coming soon):
   • Automatic window assignment
   • Watermark handling for late data
   • Out-of-order event support
   • Exactly-once semantics

Compare to streaming/windowed_analytics.py:
- Stream API: Declarative window specs
- Agent API: Manual window management
""")

    print("\n" + "=" * 70)
    print("✓ Windowed aggregations examples complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
