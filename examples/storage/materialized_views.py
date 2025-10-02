#!/usr/bin/env python3
"""
Materialized Views Example

This example demonstrates Sabot's materialized views and state management with CLI.
It shows:
- Creating and maintaining materialized views
- Real-time aggregations with RocksDB persistence
- Live queries on materialized data
- State management across restarts

Prerequisites:
- Kafka/Redpanda running: docker compose up -d
- Sabot installed: pip install -e .

Usage:
    # Start the worker
    sabot -A examples.storage.materialized_views:app worker

    # Send test data (separate terminal)
    python -c "
    from confluent_kafka import Producer
    import json, time, random

    producer = Producer({'bootstrap.servers': 'localhost:19092'})

    users = [f'user_{i}' for i in range(1, 51)]
    products = [
        {'id': 'prod_1', 'name': 'Wireless Headphones', 'category': 'Electronics', 'price': 199.99},
        {'id': 'prod_2', 'name': 'Running Shoes', 'category': 'Sports', 'price': 129.99},
        {'id': 'prod_3', 'name': 'Coffee Maker', 'category': 'Kitchen', 'price': 89.99},
        {'id': 'prod_4', 'name': 'Yoga Mat', 'category': 'Sports', 'price': 39.99},
        {'id': 'prod_5', 'name': 'Bluetooth Speaker', 'category': 'Electronics', 'price': 79.99},
    ]

    event_types = ['view', 'add_to_cart', 'purchase', 'remove_from_cart']

    for i in range(100):
        user_id = random.choice(users)
        product = random.choice(products)
        event_type = random.choice(event_types)

        # Weight purchases to be less frequent
        if event_type == 'purchase' and random.random() > 0.3:
            event_type = random.choice(['view', 'add_to_cart'])

        event = {
            'event_id': f'evt_{int(time.time() * 1000)}_{random.randint(1000, 9999)}',
            'user_id': user_id,
            'product_id': product['id'],
            'product_name': product['name'],
            'category': product['category'],
            'price': product['price'],
            'event_type': event_type,
            'timestamp': time.time(),
            'session_id': f'session_{random.randint(10000, 99999)}',
            'source': random.choice(['web', 'mobile', 'api'])
        }

        producer.produce('ecommerce-events', value=json.dumps(event).encode())
        producer.flush()
        print(f'Sent {event_type}: {product[\"name\"]} by {user_id}')
        time.sleep(0.1)
    "
"""

import sabot as sb
import random
import time

# Create Sabot application
app = sb.App(
    'ecommerce-analytics',
    broker='kafka://localhost:19092',
    value_serializer='json'
)

# Set up materialized views manager with RocksDB
# This creates persistent aggregation views backed by RocksDB
print("🗄️  Initializing Materialized Views with RocksDB...")

# Configure RocksDB backend for materialized views
mv_backend_config = sb.BackendConfig(
    backend_type="rocksdb",
    path="./rocksdb_ecommerce",
    compression="snappy"
)
mv_backend = sb.RocksDBBackend(mv_backend_config)

# Create state objects for aggregations
user_behavior_state = sb.MapState(mv_backend, "user_behavior")
product_analytics_state = sb.MapState(mv_backend, "product_analytics")


@app.agent('ecommerce-events')
async def user_behavior_aggregator(stream):
    """
    Maintain user behavior materialized view.

    Aggregates:
    - Total events per user
    - Views, cart adds, purchases counts
    - Total spending per user
    - Conversion rate
    """
    print("👤 User Behavior Aggregator started")
    print("📊 Maintaining real-time user engagement metrics\n")

    processed = 0

    async for event in stream:
        try:
            user_id = event.get("user_id")
            event_type = event.get("event_type")
            price = event.get("price", 0)

            # Get current user metrics
            user_metrics = await user_behavior_state.get(user_id)
            if user_metrics is None:
                user_metrics = {
                    "user_id": user_id,
                    "total_events": 0,
                    "views": 0,
                    "cart_adds": 0,
                    "purchases": 0,
                    "total_spent": 0.0,
                    "last_event": None
                }

            # Update metrics
            user_metrics["total_events"] += 1
            user_metrics["last_event"] = event_type

            if event_type == "view":
                user_metrics["views"] += 1
            elif event_type == "add_to_cart":
                user_metrics["cart_adds"] += 1
            elif event_type == "purchase":
                user_metrics["purchases"] += 1
                user_metrics["total_spent"] += price

            # Calculate conversion rate
            total_funnel = user_metrics["views"] + user_metrics["cart_adds"]
            user_metrics["conversion_rate"] = (
                (user_metrics["purchases"] / max(1, total_funnel)) * 100
                if total_funnel > 0 else 0.0
            )

            # Save updated metrics
            await user_behavior_state.put(user_id, user_metrics)

            processed += 1

            # Log purchases
            if event_type == "purchase":
                print(f"💰 Purchase: ${price:.2f} by {user_id} "
                      f"(total: ${user_metrics['total_spent']:.2f})")

            # Periodic stats
            if processed % 50 == 0:
                all_users = await user_behavior_state.items()
                total_purchases = sum(u[1]["purchases"] for u in all_users)
                total_revenue = sum(u[1]["total_spent"] for u in all_users)
                print(f"\n📈 User Stats:")
                print(f"   Active users: {len(all_users)}")
                print(f"   Total purchases: {total_purchases}")
                print(f"   Total revenue: ${total_revenue:.2f}\n")

            yield user_metrics

        except Exception as e:
            print(f"❌ Error aggregating user behavior: {e}")
            continue


@app.agent('ecommerce-events')
async def product_analytics_aggregator(stream):
    """
    Maintain product analytics materialized view.

    Aggregates:
    - Total events per product
    - Views, cart adds, purchases counts
    - Revenue per product
    - Conversion rate
    """
    print("📦 Product Analytics Aggregator started")
    print("📊 Maintaining real-time product performance metrics\n")

    processed = 0

    async for event in stream:
        try:
            product_id = event.get("product_id")
            product_name = event.get("product_name")
            category = event.get("category")
            event_type = event.get("event_type")
            price = event.get("price", 0)

            # Get current product metrics
            product_metrics = await product_analytics_state.get(product_id)
            if product_metrics is None:
                product_metrics = {
                    "product_id": product_id,
                    "product_name": product_name,
                    "category": category,
                    "total_events": 0,
                    "views": 0,
                    "cart_adds": 0,
                    "purchases": 0,
                    "revenue": 0.0,
                }

            # Update metrics
            product_metrics["total_events"] += 1

            if event_type == "view":
                product_metrics["views"] += 1
            elif event_type == "add_to_cart":
                product_metrics["cart_adds"] += 1
            elif event_type == "purchase":
                product_metrics["purchases"] += 1
                product_metrics["revenue"] += price

            # Calculate conversion rate
            total_funnel = product_metrics["views"] + product_metrics["cart_adds"]
            product_metrics["conversion_rate"] = (
                (product_metrics["purchases"] / max(1, total_funnel)) * 100
                if total_funnel > 0 else 0.0
            )

            # Save updated metrics
            await product_analytics_state.put(product_id, product_metrics)

            processed += 1

            # Periodic product performance report
            if processed % 50 == 0:
                all_products = await product_analytics_state.items()
                print(f"\n📊 Product Performance:")
                for prod_id, metrics in sorted(all_products,
                                               key=lambda x: x[1]["revenue"],
                                               reverse=True)[:3]:
                    print(f"   {metrics['product_name']}: "
                          f"${metrics['revenue']:.2f} revenue, "
                          f"{metrics['purchases']} purchases, "
                          f"{metrics['conversion_rate']:.1f}% conversion")
                print()

            yield product_metrics

        except Exception as e:
            print(f"❌ Error aggregating product analytics: {e}")
            continue


@app.agent('ecommerce-events')
async def category_analytics_aggregator(stream):
    """
    Maintain category analytics materialized view.

    Aggregates metrics by product category.
    """
    print("📂 Category Analytics Aggregator started\n")

    # In-memory category state (could also use RocksDB)
    category_metrics = {}

    processed = 0

    async for event in stream:
        try:
            category = event.get("category")
            event_type = event.get("event_type")
            price = event.get("price", 0)

            if category not in category_metrics:
                category_metrics[category] = {
                    "category": category,
                    "total_events": 0,
                    "views": 0,
                    "cart_adds": 0,
                    "purchases": 0,
                    "revenue": 0.0
                }

            # Update category metrics
            cat = category_metrics[category]
            cat["total_events"] += 1

            if event_type == "view":
                cat["views"] += 1
            elif event_type == "add_to_cart":
                cat["cart_adds"] += 1
            elif event_type == "purchase":
                cat["purchases"] += 1
                cat["revenue"] += price

            processed += 1

            # Periodic category report
            if processed % 75 == 0:
                print(f"\n📂 Category Performance:")
                for cat_name, metrics in sorted(category_metrics.items(),
                                               key=lambda x: x[1]["revenue"],
                                               reverse=True):
                    print(f"   {cat_name}: ${metrics['revenue']:.2f} revenue, "
                          f"{metrics['purchases']} purchases")
                print()

            yield category_metrics[category]

        except Exception as e:
            print(f"❌ Error aggregating category analytics: {e}")
            continue


if __name__ == "__main__":
    print(__doc__)
    print("\n" + "=" * 60)
    print("MATERIALIZED VIEWS ARCHITECTURE:")
    print("=" * 60)
    print("""
💎 Materialized Views Benefits:

1. **Real-Time Aggregations**
   - Continuous updates as events arrive
   - No batch ETL pipelines needed
   - Sub-millisecond query latency

2. **RocksDB Persistence**
   - Views survive process restarts
   - Efficient disk storage with compression
   - Point lookups and range scans

3. **Multiple Aggregation Strategies**
   - User behavior (per-user metrics)
   - Product analytics (per-product metrics)
   - Category analytics (per-category metrics)
   - Custom aggregations with business logic

4. **Scalability**
   - Parallel agent processing
   - Shared state via RocksDB
   - Horizontal scaling with partitioning

🔍 Query Patterns:

# Query user metrics
user_metrics = await user_behavior_state.get("user_123")
print(f"Total spent: ${user_metrics['total_spent']}")

# Query product performance
product_metrics = await product_analytics_state.get("prod_1")
print(f"Revenue: ${product_metrics['revenue']}")

# Get top users by spending
all_users = await user_behavior_state.items()
top_spenders = sorted(all_users,
                     key=lambda x: x[1]['total_spent'],
                     reverse=True)[:10]

🎯 Production Configuration:

# Use RocksDB for large-scale views
rocksdb_config = sb.BackendConfig(
    backend_type="rocksdb",
    path="/data/materialized_views",
    compression="snappy",
    cache_size=512 * 1024 * 1024  # 512MB cache
)

# Enable checkpointing for fault tolerance
app = sb.App(
    'ecommerce-analytics',
    broker='kafka://localhost:19092',
    checkpoint_interval=60.0  # Checkpoint every 60s
)
""")
