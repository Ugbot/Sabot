#!/usr/bin/env python3
"""
Stateful Processing and Joins with Sabot API

This example demonstrates stateful stream processing and join operations
using Sabot's high-level State and Stream APIs. It shows the same concepts
as data/joins_demo.py but with the modern API.

This demonstrates:
- Stream-table joins (enriching streams with reference data)
- Stateful stream processing with ValueState
- ListState for accumulating events
- MapState for per-key features
- Keyed streams for partitioned processing

Modern API Benefits:
- Clean state management abstractions
- Type-safe state operations
- Automatic state backends (in-memory, Tonbo)
- Zero-copy where possible

Prerequisites:
- Sabot installed: pip install -e .
- PyArrow: pip install pyarrow

Usage:
    python examples/api/stateful_joins.py
"""

import time
import random

# Import Sabot Arrow (uses internal or external PyArrow)
from sabot import cyarrow as pa
from sabot.cyarrow import compute as pc

# Import Sabot API
from sabot.api import Stream, ValueState, ListState, MapState


# ============================================================================
# Reference Data
# ============================================================================

USER_PROFILES = {
    "alice": {
        "user_id": "alice",
        "name": "Alice Johnson",
        "age": 28,
        "city": "New York",
        "vip": True,
        "credit_limit": 5000.0
    },
    "bob": {
        "user_id": "bob",
        "name": "Bob Smith",
        "age": 34,
        "city": "San Francisco",
        "vip": False,
        "credit_limit": 1000.0
    },
    "charlie": {
        "user_id": "charlie",
        "name": "Charlie Brown",
        "age": 25,
        "city": "Chicago",
        "vip": False,
        "credit_limit": 2000.0
    },
    "diana": {
        "user_id": "diana",
        "name": "Diana Prince",
        "age": 31,
        "city": "Boston",
        "vip": True,
        "credit_limit": 8000.0
    },
}


def generate_user_events(num_events=40):
    """Generate user activity events."""
    users = list(USER_PROFILES.keys())
    event_types = ['login', 'view', 'click', 'purchase', 'logout']
    pages = ['/home', '/products', '/cart', '/checkout', '/profile']

    events = []
    for i in range(num_events):
        event = {
            'user_id': random.choice(users),
            'event_type': random.choice(event_types),
            'page': random.choice(pages),
            'timestamp': time.time() + i * 0.5,
            'session_id': f"session_{random.randint(1, 10)}",
            'event_id': i
        }
        events.append(event)

    return events


def generate_order_events(num_orders=20):
    """Generate order/purchase events."""
    users = list(USER_PROFILES.keys())
    products = ['widget_a', 'widget_b', 'widget_c']
    prices = {'widget_a': 29.99, 'widget_b': 19.99, 'widget_c': 49.99}

    orders = []
    for i in range(num_orders):
        product = random.choice(products)
        quantity = random.randint(1, 5)

        order = {
            'user_id': random.choice(users),
            'order_id': f"order_{i}",
            'product_id': product,
            'quantity': quantity,
            'amount': prices[product] * quantity,
            'timestamp': time.time() + i * 1.0,
        }
        orders.append(order)

    return orders


def main():
    print("=" * 70)
    print("Stateful Processing and Joins with Sabot API")
    print("=" * 70)

    print(f"\n📊 Reference Data:")
    print(f"   User Profiles: {len(USER_PROFILES)} users")

    # ========================================================================
    # Example 1: Stream-Table Join (Enrichment with ValueState)
    # ========================================================================
    print("\n" + "=" * 70)
    print("Example 1: Stream-Table Join using ValueState")
    print("=" * 70)

    print("\n📌 Enrich user events with profile data from state.")

    # Load user profiles into state
    profile_state = ValueState('user_profile')

    for user_id, profile in USER_PROFILES.items():
        profile_state.update(user_id, profile)

    print(f"✓ Loaded {len(USER_PROFILES)} user profiles into state")

    # Generate events
    user_events = generate_user_events(20)
    event_batches = []
    batch_size = 5
    for i in range(0, len(user_events), batch_size):
        chunk = user_events[i:i+batch_size]
        batch = pa.RecordBatch.from_pylist(chunk)
        event_batches.append(batch)

    # Enrich events with profile data
    def enrich_with_profile(batch):
        """Enrich events with user profile from state."""
        enriched_events = []

        for i in range(batch.num_rows):
            event = {col: batch.column(col)[i].as_py() for col in batch.schema.names}
            user_id = event['user_id']

            # Lookup profile in state
            profile = profile_state.value(user_id)

            if profile:
                # Merge event with profile
                enriched = {
                    **event,
                    'user_name': profile['name'],
                    'user_city': profile['city'],
                    'user_vip': profile['vip'],
                }
                enriched_events.append(enriched)

        if enriched_events:
            return pa.RecordBatch.from_pylist(enriched_events)
        else:
            return batch

    stream = Stream.from_batches(event_batches)
    enriched_stream = stream.map(enrich_with_profile)

    enriched = enriched_stream.collect()

    print(f"\n✓ Enriched {enriched.num_rows} events")
    print(f"\nSample enriched events:")
    print(enriched.slice(0, min(5, enriched.num_rows)).to_pandas()[
        ['user_id', 'event_type', 'user_name', 'user_vip']
    ])

    # ========================================================================
    # Example 2: Stateful Aggregation (Per-User Counters)
    # ========================================================================
    print("\n" + "=" * 70)
    print("Example 2: Per-User Event Counters using ValueState")
    print("=" * 70)

    print("\n📌 Track event counts per user with stateful counters.")

    # Event counter state
    event_counter = ValueState('event_count')

    stream = Stream.from_batches(event_batches)

    # Process events and update counters
    for batch in stream._execute_iterator():
        for i in range(batch.num_rows):
            user_id = batch.column('user_id')[i].as_py()
            event_type = batch.column('event_type')[i].as_py()

            # Get current count
            current_count = event_counter.value(user_id) or 0

            # Increment
            new_count = current_count + 1
            event_counter.update(user_id, new_count)

    # Display final counts
    print(f"\n📊 Per-User Event Counts:")
    for user_id in USER_PROFILES.keys():
        count = event_counter.value(user_id) or 0
        if count > 0:
            print(f"   {user_id}: {count} events")

    # ========================================================================
    # Example 3: ListState - Accumulating Events Per User
    # ========================================================================
    print("\n" + "=" * 70)
    print("Example 3: Accumulate User Events with ListState")
    print("=" * 70)

    print("\n📌 Store all events per user in ListState.")

    # Event history state
    event_history = ListState('event_history')

    stream = Stream.from_batches(event_batches)

    # Accumulate events
    event_count_by_user = {}
    for batch in stream._execute_iterator():
        for i in range(batch.num_rows):
            user_id = batch.column('user_id')[i].as_py()
            event_type = batch.column('event_type')[i].as_py()

            # Create single-event batch
            event_batch = pa.RecordBatch.from_pydict({
                'event_type': [event_type],
                'timestamp': [batch.column('timestamp')[i].as_py()]
            })

            # Add to history
            event_history.add(user_id, event_batch)
            event_count_by_user[user_id] = event_count_by_user.get(user_id, 0) + 1

    # Display history
    print(f"\n📊 Event History Per User:")
    for user_id in sorted(event_count_by_user.keys()):
        count = event_count_by_user[user_id]
        print(f"   {user_id}: {count} events stored in ListState")

        # Get events from state
        user_events = event_history.get(user_id)
        print(f"      Retrieved {len(user_events)} batches from state")

    # ========================================================================
    # Example 4: MapState - Per-User Features
    # ========================================================================
    print("\n" + "=" * 70)
    print("Example 4: User Feature Store with MapState")
    print("=" * 70)

    print("\n📌 Store multiple features per user using MapState.")

    # Feature state
    user_features = MapState('user_features')

    stream = Stream.from_batches(event_batches)

    # Track features
    for batch in stream._execute_iterator():
        for i in range(batch.num_rows):
            user_id = batch.column('user_id')[i].as_py()
            event_type = batch.column('event_type')[i].as_py()
            page = batch.column('page')[i].as_py()

            # Update feature: last_event_type
            user_features.put(user_id, 'last_event_type', event_type)

            # Update feature: last_page
            user_features.put(user_id, 'last_page', page)

            # Increment feature: total_events
            current_total = user_features.get(user_id, 'total_events') or 0
            user_features.put(user_id, 'total_events', current_total + 1)

            # Update feature: timestamp
            user_features.put(user_id, 'last_seen', batch.column('timestamp')[i].as_py())

    # Display features
    print(f"\n📊 User Features (MapState):")
    for user_id in sorted(USER_PROFILES.keys()):
        features = user_features.items(user_id)
        if features:
            print(f"\n   {user_id}:")
            for feature_name, feature_value in features:
                print(f"      {feature_name}: {feature_value}")

    # ========================================================================
    # Example 5: Stream-Stream Join (Orders + Events)
    # ========================================================================
    print("\n" + "=" * 70)
    print("Example 5: Stream-Stream Join using State")
    print("=" * 70)

    print("\n📌 Correlate orders with recent user activity.")

    # Generate order events
    orders = generate_order_events(15)
    order_batches = []
    for i in range(0, len(orders), 3):
        chunk = orders[i:i+3]
        batch = pa.RecordBatch.from_pylist(chunk)
        order_batches.append(batch)

    print(f"✓ Generated {len(orders)} orders")

    # Use features from previous example
    print(f"\n💰 Orders with User Context:")

    for batch in order_batches:
        for i in range(batch.num_rows):
            user_id = batch.column('user_id')[i].as_py()
            order_id = batch.column('order_id')[i].as_py()
            amount = batch.column('amount')[i].as_py()
            product = batch.column('product_id')[i].as_py()

            # Get user context from state
            last_event = user_features.get(user_id, 'last_event_type')
            last_page = user_features.get(user_id, 'last_page')
            total_events = user_features.get(user_id, 'total_events') or 0

            profile = profile_state.value(user_id)
            user_name = profile['name'] if profile else user_id
            is_vip = profile['vip'] if profile else False

            print(f"\n   Order: {order_id}")
            print(f"      User: {user_name} ({'VIP' if is_vip else 'Regular'})")
            print(f"      Amount: ${amount:.2f} ({product})")
            print(f"      Recent Activity: {last_event} on {last_page}")
            print(f"      Total Events: {total_events}")

    # ========================================================================
    # Example 6: Stateful Fraud Detection
    # ========================================================================
    print("\n" + "=" * 70)
    print("Example 6: Real-Time Fraud Detection with State")
    print("=" * 70)

    print("\n📌 Detect suspicious patterns using stateful tracking.")

    # Fraud detection state
    purchase_tracker = ValueState('purchase_total')
    purchase_count_tracker = ValueState('purchase_count')

    # Process orders and detect fraud
    suspicious_orders = []

    for batch in order_batches:
        for i in range(batch.num_rows):
            user_id = batch.column('user_id')[i].as_py()
            amount = batch.column('amount')[i].as_py()
            order_id = batch.column('order_id')[i].as_py()

            # Get current totals
            total_spent = purchase_tracker.value(user_id) or 0.0
            purchase_count = purchase_count_tracker.value(user_id) or 0

            # Update totals
            new_total = total_spent + amount
            new_count = purchase_count + 1

            purchase_tracker.update(user_id, new_total)
            purchase_count_tracker.update(user_id, new_count)

            # Fraud detection rules
            profile = profile_state.value(user_id)
            credit_limit = profile['credit_limit'] if profile else 1000.0

            is_suspicious = False
            reasons = []

            # Rule 1: Exceeds credit limit
            if new_total > credit_limit:
                is_suspicious = True
                reasons.append(f"Exceeds credit limit (${new_total:.2f} > ${credit_limit:.2f})")

            # Rule 2: Too many purchases in short time
            if new_count > 5:
                is_suspicious = True
                reasons.append(f"High purchase frequency ({new_count} purchases)")

            # Rule 3: Large single purchase
            if amount > 100:
                is_suspicious = True
                reasons.append(f"Large single purchase (${amount:.2f})")

            if is_suspicious:
                suspicious_orders.append({
                    'order_id': order_id,
                    'user_id': user_id,
                    'amount': amount,
                    'total_spent': new_total,
                    'reasons': reasons
                })

    # Display suspicious orders
    print(f"\n🚨 Suspicious Orders Detected: {len(suspicious_orders)}")
    for order in suspicious_orders[:5]:  # Show first 5
        print(f"\n   ⚠️  Order {order['order_id']} - {order['user_id']}")
        print(f"      Amount: ${order['amount']:.2f}")
        print(f"      Total Spent: ${order['total_spent']:.2f}")
        print(f"      Flags:")
        for reason in order['reasons']:
            print(f"         - {reason}")

    # ========================================================================
    # Summary
    # ========================================================================
    print("\n" + "=" * 70)
    print("State API Summary")
    print("=" * 70)
    print("""
🗄️ State Types:

1. ValueState - Single value per key
   • user_state.update('alice', value)
   • value = user_state.value('alice')
   • Use for: counters, flags, last values

2. ListState - List of values per key
   • events.add('alice', event_batch)
   • batches = events.get('alice')
   • Use for: event history, accumulation

3. MapState - Nested key-value per key
   • features.put('alice', 'feature1', value)
   • value = features.get('alice', 'feature1')
   • Use for: feature stores, multi-field state

🔄 Join Patterns:

- Stream-Table: Enrich stream with reference data
- Stream-Stream: Correlate two event streams
- Temporal: Join based on time windows

🚀 State Backends:

Current: In-memory (dict-based)
Production: Tonbo LSM tree (columnar, persistent)
Performance: <100ns (cache), <10μs (disk)

Compare to data/joins_demo.py:
- State API: Clean abstractions
- Agent API: Manual state management
""")

    print("\n" + "=" * 70)
    print("✓ Stateful processing and joins examples complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
