#!/usr/bin/env python3
"""
Pluggable Store Backends Demo

This example demonstrates Sabot's pluggable storage backends for state management.
Shows how to use different storage engines with the CLI-based agent model.

Storage backends demonstrated:
- Memory backend (fast, ephemeral)
- RocksDB backend (persistent, disk-based)
- Redis backend (distributed, shared state)

Prerequisites:
- Kafka/Redpanda running: docker compose up -d
- Sabot installed: pip install -e .
- Optional: Redis for distributed state

Usage:
    # Run with memory backend
    sabot -A examples.storage.pluggable_backends:app worker

    # Send test data (separate terminal)
    python -c "
    from confluent_kafka import Producer
    import json, time

    producer = Producer({'bootstrap.servers': 'localhost:19092'})

    # Send user events
    for i in range(20):
        event = {
            'user_id': f'user_{i % 5}',
            'action': ['login', 'purchase', 'logout'][i % 3],
            'timestamp': time.time(),
            'value': i * 10
        }
        producer.produce('user-events', value=json.dumps(event).encode())
        producer.flush()
        print(f'Sent: {event}')
        time.sleep(0.5)
    "
"""

import sabot as sb

# Create Sabot application
app = sb.App(
    'storage-demo',
    broker='kafka://localhost:19092',
    value_serializer='json'
)

# Configure memory backend (fast, not persistent)
memory_config = sb.BackendConfig(
    backend_type="memory",
    max_size=100000,
    ttl_seconds=300.0  # 5 minute TTL
)
memory_backend = sb.MemoryBackend(memory_config)

# Create state objects for different use cases
user_profiles = sb.MapState(memory_backend, "user_profiles")
event_counter = sb.ValueState(memory_backend, "event_counter")
recent_events = sb.ListState(memory_backend, "recent_events")


@app.agent('user-events')
async def process_user_events(stream):
    """
    Process user events and maintain state in pluggable backends.

    Demonstrates:
    - MapState for user profiles
    - ValueState for counters
    - ListState for recent events
    """
    print("📦 Storage Backend Agent started")
    print(f"🗄️  Using backend: {memory_config.backend_type}")
    print(f"⏱️  TTL: {memory_config.ttl_seconds}s")
    print(f"📊 Max size: {memory_config.max_size:,} entries\n")

    # Initialize counter
    count = await event_counter.value()
    if count is None:
        await event_counter.update(0)
        count = 0

    async for event in stream:
        try:
            user_id = event.get("user_id")
            action = event.get("action")
            value = event.get("value", 0)
            timestamp = event.get("timestamp")

            # Update event counter
            count += 1
            await event_counter.update(count)

            # Update user profile in MapState
            profile = await user_profiles.get(user_id)
            if profile is None:
                profile = {
                    "user_id": user_id,
                    "total_events": 0,
                    "total_value": 0,
                    "actions": []
                }

            profile["total_events"] += 1
            profile["total_value"] += value
            profile["actions"].append(action)
            profile["last_seen"] = timestamp

            # Keep only last 10 actions
            if len(profile["actions"]) > 10:
                profile["actions"] = profile["actions"][-10:]

            await user_profiles.put(user_id, profile)

            # Add to recent events list
            event_summary = {
                "user_id": user_id,
                "action": action,
                "value": value,
                "timestamp": timestamp
            }

            recent = await recent_events.get()
            if recent is None:
                recent = []
            recent.append(event_summary)

            # Keep only last 20 events
            if len(recent) > 20:
                recent = recent[-20:]

            await recent_events.update(recent)

            print(f"✅ Processed {action} for {user_id}: "
                  f"total_events={profile['total_events']}, "
                  f"total_value=${profile['total_value']}")

            # Periodic stats
            if count % 10 == 0:
                all_profiles = await user_profiles.items()
                print(f"\n📊 Storage Stats:")
                print(f"   Total events: {count}")
                print(f"   Unique users: {len(all_profiles)}")
                print(f"   Recent events in buffer: {len(recent)}")
                print()

            yield {
                "event_count": count,
                "user_profile": profile
            }

        except Exception as e:
            print(f"❌ Error processing event: {e}")
            continue


@app.agent('user-events')
async def analytics_agent(stream):
    """
    Secondary agent demonstrating shared state access.

    Both agents can access the same state backends.
    """
    print("📈 Analytics Agent started (shares same state)\n")

    processed = 0

    async for event in stream:
        try:
            processed += 1

            if processed % 15 == 0:
                # Read state from shared backend
                count = await event_counter.value()
                all_profiles = await user_profiles.items()

                print(f"\n📈 Analytics Report:")
                print(f"   System event count: {count}")
                print(f"   Users tracked: {len(all_profiles)}")

                # Find most active user
                if all_profiles:
                    most_active = max(all_profiles,
                                     key=lambda x: x[1].get('total_events', 0))
                    user_id, profile = most_active
                    print(f"   Most active user: {user_id} "
                          f"({profile['total_events']} events)")
                print()

        except Exception as e:
            print(f"❌ Error in analytics: {e}")
            continue


if __name__ == "__main__":
    print(__doc__)
    print("\n" + "="*60)
    print("BACKEND CONFIGURATION OPTIONS:")
    print("="*60)
    print("""
# Memory Backend (fast, ephemeral)
memory_config = sb.BackendConfig(
    backend_type="memory",
    max_size=100000,
    ttl_seconds=300.0
)
backend = sb.MemoryBackend(memory_config)

# RocksDB Backend (persistent, disk-based)
rocksdb_config = sb.BackendConfig(
    backend_type="rocksdb",
    path="./state/rocksdb",
    compression="snappy"
)
backend = sb.RocksDBBackend(rocksdb_config)

# State Types Available:
- sb.ValueState(backend, "key")      # Single value
- sb.MapState(backend, "key")        # Key-value map
- sb.ListState(backend, "key")       # Ordered list
- sb.ReducingState(backend, "key")   # Aggregations
- sb.AggregatingState(backend, "key") # Complex aggregations
""")
