#!/usr/bin/env python3
"""
Join Operations Demo for Sabot

This example demonstrates streaming joins with Sabot's CLI-compatible agent model.
It shows:
- Stream-table joins (enriching streams with reference data)
- Stream-stream joins (correlating two streams)
- Using state to maintain join state
- Real-time data enrichment

Prerequisites:
- Kafka/Redpanda running: docker compose up -d
- Sabot installed: pip install -e .

Usage:
    # Start the worker
    sabot -A examples.data.joins_demo:app worker

    # Send test data (separate terminal - run producers below)
    python -c "
    from confluent_kafka import Producer
    import json, time

    producer = Producer({'bootstrap.servers': 'localhost:19092'})

    # Send user events
    users = ['alice', 'bob', 'charlie', 'diana']
    events = ['login', 'click', 'view', 'purchase']

    for i in range(20):
        user_event = {
            'user_id': users[i % len(users)],
            'event_type': events[i % len(events)],
            'timestamp': time.time(),
            'session_id': f'session_{i}',
            'page': f'/page/{i % 5}'
        }
        producer.produce('user-events', value=json.dumps(user_event).encode())
        producer.flush()
        print(f'Sent user event: {user_event}')
        time.sleep(0.3)
    "
"""

import sabot as sb
import time

# Create Sabot application
app = sb.App(
    'joins-demo',
    broker='kafka://localhost:19092',
    value_serializer='json'
)

# Reference data: User profiles (simulating a dimension table)
USER_PROFILES = {
    "alice": {"user_id": "alice", "name": "Alice Johnson", "age": 28, "city": "New York", "vip": True},
    "bob": {"user_id": "bob", "name": "Bob Smith", "age": 34, "city": "San Francisco", "vip": False},
    "charlie": {"user_id": "charlie", "name": "Charlie Brown", "age": 25, "city": "Chicago", "vip": False},
    "diana": {"user_id": "diana", "name": "Diana Prince", "age": 31, "city": "Boston", "vip": True},
}

# State for stream-stream joins
order_state = {}  # Store orders keyed by user_id


@app.agent('user-events')
async def stream_table_join_agent(stream):
    """
    Perform stream-table join: enrich user events with profile data.

    Join Type: LEFT OUTER JOIN
    Stream: user-events
    Table: USER_PROFILES (in-memory reference data)
    """
    print("🔗 Stream-Table Join Agent started")
    print(f"📊 Reference table loaded: {len(USER_PROFILES)} user profiles\n")

    event_count = 0

    async for event in stream:
        try:
            user_id = event.get('user_id')
            event_type = event.get('event_type')
            event_count += 1

            # Perform join: look up user profile
            profile = USER_PROFILES.get(user_id)

            if profile:
                # Enrich event with profile data
                enriched_event = {
                    **event,
                    'user_name': profile['name'],
                    'user_age': profile['age'],
                    'user_city': profile['city'],
                    'user_vip': profile['vip'],
                    'enriched_at': time.time()
                }

                vip_badge = "⭐ VIP" if profile['vip'] else ""
                print(f"✅ Enriched Event #{event_count}: "
                      f"{profile['name']} {vip_badge} from {profile['city']} "
                      f"→ {event_type}")

                yield enriched_event
            else:
                # User not in reference table (NULL in SQL terms)
                print(f"⚠️  Event #{event_count}: Unknown user {user_id} → {event_type}")

                enriched_event = {
                    **event,
                    'user_name': None,
                    'user_age': None,
                    'user_city': None,
                    'user_vip': False,
                    'enriched_at': time.time()
                }
                yield enriched_event

        except Exception as e:
            print(f"❌ Error in stream-table join: {e}")
            continue


@app.agent('order-events')
async def order_buffering_agent(stream):
    """
    Buffer orders for stream-stream join.

    This agent stores orders in state so they can be joined
    with user events.
    """
    print("📦 Order Buffering Agent started\n")

    order_count = 0

    async for order in stream:
        try:
            order_id = order.get('order_id')
            user_id = order.get('user_id')
            amount = order.get('amount', 0)
            status = order.get('status')

            order_count += 1

            # Store order in join state (keyed by user_id for joining)
            if user_id not in order_state:
                order_state[user_id] = []

            order_state[user_id].append({
                'order_id': order_id,
                'amount': amount,
                'status': status,
                'timestamp': order.get('timestamp', time.time())
            })

            # Keep only recent orders (last 10 per user)
            if len(order_state[user_id]) > 10:
                order_state[user_id] = order_state[user_id][-10:]

            print(f"📦 Buffered Order #{order_count}: "
                  f"{order_id} → {user_id} (${amount}) [{status}]")

            yield order

        except Exception as e:
            print(f"❌ Error buffering order: {e}")
            continue


@app.agent('user-events')
async def stream_stream_join_agent(stream):
    """
    Perform stream-stream join: correlate user events with their orders.

    Join Type: INNER JOIN (only users with orders)
    Left Stream: user-events
    Right Stream: order-events (buffered in state)

    This demonstrates time-windowed stream-stream joins.
    """
    print("🔄 Stream-Stream Join Agent started")
    print("💡 Correlating user events with recent orders\n")

    correlation_count = 0

    async for event in stream:
        try:
            user_id = event.get('user_id')
            event_type = event.get('event_type')
            event_time = event.get('timestamp', time.time())

            # Perform join: look up user's orders
            user_orders = order_state.get(user_id, [])

            if user_orders:
                # Found matching orders - perform join
                for order in user_orders:
                    # Time-windowed join: only correlate recent orders (within 60 seconds)
                    time_diff = event_time - order['timestamp']
                    if time_diff < 60:  # 60 second window
                        correlation_count += 1

                        joined_event = {
                            'user_id': user_id,
                            'event_type': event_type,
                            'event_timestamp': event_time,
                            'order_id': order['order_id'],
                            'order_amount': order['amount'],
                            'order_status': order['status'],
                            'order_timestamp': order['timestamp'],
                            'time_diff_seconds': round(time_diff, 2),
                            'correlation_id': f"corr_{correlation_count}"
                        }

                        print(f"🔗 Correlation #{correlation_count}: "
                              f"User {user_id} {event_type} event "
                              f"linked to order {order['order_id']} (${order['amount']}) "
                              f"[{time_diff:.1f}s apart]")

                        yield joined_event

        except Exception as e:
            print(f"❌ Error in stream-stream join: {e}")
            continue


@app.agent('user-events')
async def join_stats_agent(stream):
    """
    Track join statistics and report periodically.
    """
    print("📊 Join Statistics Agent started\n")

    stats = {
        "events_processed": 0,
        "events_with_profiles": 0,
        "events_with_orders": 0,
        "unique_users": set()
    }

    async for event in stream:
        try:
            user_id = event.get('user_id')
            stats["events_processed"] += 1
            stats["unique_users"].add(user_id)

            # Check if enriched
            if event.get('user_name'):
                stats["events_with_profiles"] += 1

            # Check if user has orders
            if order_state.get(user_id):
                stats["events_with_orders"] += 1

            # Report every 10 events
            if stats["events_processed"] % 10 == 0:
                print(f"\n📊 Join Statistics:")
                print(f"   Events processed: {stats['events_processed']}")
                print(f"   Events with profiles: {stats['events_with_profiles']}")
                print(f"   Events with orders: {stats['events_with_orders']}")
                print(f"   Unique users: {len(stats['unique_users'])}")
                print(f"   Users with buffered orders: {len(order_state)}\n")

        except Exception as e:
            print(f"❌ Error in stats: {e}")
            continue


if __name__ == "__main__":
    print(__doc__)
    print("\n" + "="*60)
    print("JOIN TYPES DEMONSTRATED:")
    print("="*60)
    print("""
🔗 Stream-Table Join (Enrichment):
   - Join user events with reference data (profiles)
   - LEFT OUTER JOIN semantics
   - Fast lookups from in-memory table
   - Use case: Enriching streams with dimension data

🔄 Stream-Stream Join (Correlation):
   - Join user events with order events
   - INNER JOIN with time window (60 seconds)
   - Buffered state for right stream
   - Use case: Correlating related events across streams

📊 Join Statistics:
   - Track join effectiveness
   - Monitor match rates
   - Report periodically

💡 Production Patterns:

# For large reference tables, use state backend:
config = sb.BackendConfig(backend_type="rocksdb", path="./state")
backend = sb.RocksDBBackend(config)
profile_state = sb.MapState(backend, "profiles")

# For stream-stream joins with large windows:
# Use watermarks and event time
# Buffer in RocksDB for durability
# Configure TTL to expire old state

🚀 Send Test Data:

# Terminal 2: Send user events
python -c "see producer code in docstring above"

# Terminal 3: Send orders
python -c "
from confluent_kafka import Producer
import json, time

producer = Producer({'bootstrap.servers': 'localhost:19092'})

orders = [
    {'order_id': '1001', 'user_id': 'alice', 'amount': 99.99, 'status': 'pending'},
    {'order_id': '1002', 'user_id': 'bob', 'amount': 149.99, 'status': 'paid'},
    {'order_id': '1003', 'user_id': 'alice', 'amount': 79.99, 'status': 'shipped'},
]

for order in orders:
    order['timestamp'] = time.time()
    producer.produce('order-events', value=json.dumps(order).encode())
    producer.flush()
    print(f'Sent order: {order}')
    time.sleep(1)
"
""")
