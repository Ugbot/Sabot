#!/usr/bin/env python3
"""
Windowed Analytics Example

This example demonstrates Sabot's windowed analytics capabilities with the CLI model.
It shows:
- Tumbling windows for periodic aggregations
- Sliding windows for rolling calculations
- Real-time dashboard metrics
- Time-based data processing

Prerequisites:
- Kafka/Redpanda running: docker compose up -d
- Sabot installed: pip install -e .

Usage:
    # Start the worker
    sabot -A examples.streaming.windowed_analytics:app worker

    # Send test data (separate terminal - run the producer at end of this file)
    python examples/streaming/windowed_analytics_producer.py
"""

import sabot as sb
import time

# Create Sabot application
app = sb.App(
    'api-analytics',
    broker='kafka://localhost:19092',
    value_serializer='json'
)


# Metrics dashboard
class MetricsDashboard:
    """Simple in-memory metrics dashboard."""

    def __init__(self):
        self.metrics = {
            "total_requests": 0,
            "error_rate": 0.0,
            "avg_response_time": 0.0,
            "active_users": 0
        }

    def update(self, window_data):
        """Update dashboard with windowed data."""
        self.metrics.update(window_data)
        self.display()

    def display(self):
        """Display current metrics."""
        print("📊 Dashboard Metrics:")
        print(f"📈 Total Requests: {self.metrics['total_requests']}")
        print(f"❌ Error Rate: {self.metrics['error_rate']:.1f}%")
        print(f"⏱️  Avg Response Time: {self.metrics['avg_response_time']:.2f}s")
        print(f"👥 Active Users: {self.metrics['active_users']}")
        print("-" * 30)


dashboard = MetricsDashboard()


@app.agent('api-requests')
async def tumbling_window_analytics(stream):
    """
    Analyze API requests with tumbling windows (10-second intervals).

    Only processes successful requests (status < 400).
    """
    print("🕐 Tumbling window agent started (10-second windows)")

    # Simple tumbling window implementation
    window_size = 10.0  # 10 seconds
    window_start = time.time()
    window_data = []

    async for request in stream:
        try:
            # Filter: Only successful requests
            if request.get("status_code", 500) < 400:
                window_data.append(request)

            current_time = time.time()

            # Check if window should close
            if current_time - window_start >= window_size:
                if window_data:
                    # Aggregate window data
                    request_count = len(window_data)
                    avg_response_time = sum(r.get("response_time", 0) for r in window_data) / request_count
                    total_bytes = sum(r.get("bytes_sent", 0) for r in window_data)
                    unique_users = len(set(r.get("user_id") for r in window_data if r.get("user_id")))

                    result = {
                        "window_type": "tumbling",
                        "request_count": request_count,
                        "avg_response_time": round(avg_response_time, 2),
                        "total_bytes": total_bytes,
                        "unique_users": unique_users,
                        "window_start": window_start,
                        "window_end": current_time
                    }

                    print(f"🕐 Tumbling Window: {request_count} requests, "
                          f"Avg RT: {avg_response_time:.2f}s, "
                          f"Users: {unique_users}")

                    yield result

                # Reset window
                window_data = []
                window_start = current_time

        except Exception as e:
            print(f"❌ Error in tumbling window: {e}")
            continue


@app.agent('api-requests')
async def sliding_window_analytics(stream):
    """
    Analyze API requests with sliding windows (30s window, 10s slide).

    Calculates error rates and updates dashboard.
    """
    print("📈 Sliding window agent started (30s window, 10s slide)")

    # Sliding window implementation
    window_size = 30.0  # 30 seconds
    slide_interval = 10.0  # 10 seconds

    all_events = []  # Store all events with timestamps
    last_slide = time.time()

    async for request in stream:
        try:
            current_time = time.time()

            # Add event with timestamp
            event_with_time = {**request, "_internal_time": current_time}
            all_events.append(event_with_time)

            # Remove events outside window
            cutoff_time = current_time - window_size
            all_events = [e for e in all_events if e["_internal_time"] >= cutoff_time]

            # Check if should slide window
            if current_time - last_slide >= slide_interval:
                if all_events:
                    # Calculate metrics
                    total_requests = len(all_events)
                    error_count = sum(1 for e in all_events if e.get("status_code", 500) >= 400)
                    error_rate = (error_count / total_requests) * 100 if total_requests > 0 else 0

                    avg_response_time = sum(e.get("response_time", 0) for e in all_events) / total_requests
                    active_users = len(set(e.get("user_id") for e in all_events if e.get("user_id")))

                    # Update dashboard
                    dashboard.update({
                        "total_requests": total_requests,
                        "error_rate": error_rate,
                        "avg_response_time": avg_response_time,
                        "active_users": active_users
                    })

                    result = {
                        "window_type": "sliding",
                        "total_requests": total_requests,
                        "error_count": error_count,
                        "error_rate": error_rate,
                        "avg_response_time": round(avg_response_time, 2),
                        "active_users": active_users
                    }

                    yield result

                last_slide = current_time

        except Exception as e:
            print(f"❌ Error in sliding window: {e}")
            continue


@app.agent('api-requests')
async def endpoint_analytics(stream):
    """
    Per-endpoint analytics with 15-second tumbling windows.

    Groups by endpoint and calculates metrics for each.
    """
    print("📍 Endpoint analytics agent started (15s windows)")

    # State for each endpoint
    endpoint_windows = {}
    window_size = 15.0

    async for request in stream:
        try:
            endpoint = request.get("endpoint", "unknown")
            current_time = time.time()

            # Initialize endpoint window if needed
            if endpoint not in endpoint_windows:
                endpoint_windows[endpoint] = {
                    "data": [],
                    "window_start": current_time
                }

            # Add to endpoint window
            endpoint_windows[endpoint]["data"].append(request)

            # Check if window should close for this endpoint
            window_start = endpoint_windows[endpoint]["window_start"]
            if current_time - window_start >= window_size:
                data = endpoint_windows[endpoint]["data"]

                if data:
                    # Calculate endpoint metrics
                    requests = len(data)
                    avg_response_time = sum(r.get("response_time", 0) for r in data) / requests
                    errors = sum(1 for r in data if r.get("status_code", 500) >= 400)

                    result = {
                        "endpoint": endpoint,
                        "requests": requests,
                        "avg_response_time": round(avg_response_time, 2),
                        "errors": errors,
                        "window_start": window_start,
                        "window_end": current_time
                    }

                    print(f"🌐 {endpoint}: {requests} reqs, "
                          f"Avg RT: {avg_response_time:.2f}s, "
                          f"Errors: {errors}")

                    yield result

                # Reset endpoint window
                endpoint_windows[endpoint] = {
                    "data": [],
                    "window_start": current_time
                }

        except Exception as e:
            print(f"❌ Error in endpoint analytics: {e}")
            continue


if __name__ == "__main__":
    print(__doc__)
    print("\n" + "="*60)
    print("PRODUCER CODE - Save as windowed_analytics_producer.py:")
    print("="*60)
    print("""
import random
import time
import json
from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:19092'})

endpoints = ["/api/users", "/api/orders", "/api/products", "/api/analytics"]
methods = ["GET", "POST", "PUT", "DELETE"]
users = [f"user_{i}" for i in range(1, 21)]

print("🚀 Sending API request data to Kafka...")

for i in range(100):
    endpoint = random.choice(endpoints)

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
        "request_id": f"req_{int(time.time() * 1000)}_{i}",
        "endpoint": endpoint,
        "method": random.choice(methods),
        "user_id": random.choice(users),
        "response_time": round(response_time, 3),
        "status_code": status_code,
        "timestamp": time.time(),
        "bytes_sent": random.randint(100, 10000)
    }

    producer.produce('api-requests', value=json.dumps(request).encode())
    producer.flush()

    if i % 10 == 0:
        print(f"📨 Sent {i} requests...")

    time.sleep(0.1)  # 10 requests/sec

print("✅ Producer complete!")
""")
