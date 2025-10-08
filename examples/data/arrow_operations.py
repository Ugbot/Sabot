#!/usr/bin/env python3
"""
Arrow-Native Operations Example

This example demonstrates Sabot's Arrow-native columnar operations.
It shows:
- Processing Arrow RecordBatches in streams
- Columnar data transformations
- Zero-copy operations
- SIMD-accelerated filtering and aggregations

Prerequisites:
- Kafka/Redpanda running: docker compose up -d
- Sabot installed: pip install -e .
- PyArrow: pip install pyarrow

Usage:
    # Start the worker
    sabot -A examples.data.arrow_operations:app worker

    # Send test data (this example demonstrates Arrow concepts)
    # In production, you'd send Arrow RecordBatches to Kafka
"""

import sabot as sb
from sabot import cyarrow as ca
from sabot import cyarrow as pa  # For specialized types (vendored Arrow)
from sabot.cyarrow import compute as pc  # For compute functions (vendored Arrow)
import time

# Create Sabot application
app = sb.App(
    'arrow-analytics',
    broker='kafka://localhost:19092',
    value_serializer='json'  # In production, use 'arrow' for zero-copy
)

# Static reference data as Arrow tables
user_profiles = pa.table({
    'user_id': ['alice', 'bob', 'charlie', 'diana', 'eve'],
    'name': ['Alice Johnson', 'Bob Smith', 'Charlie Brown', 'Diana Prince', 'Eve Wilson'],
    'signup_date': ['2023-01-15', '2023-02-20', '2023-03-10', '2023-04-05', '2023-05-12'],
    'tier': ['premium', 'basic', 'premium', 'basic', 'premium']
})

product_catalog = pa.table({
    'product_id': ['widget_a', 'widget_b', 'widget_c'],
    'name': ['Premium Widget A', 'Basic Widget B', 'Deluxe Widget C'],
    'price': [29.99, 19.99, 49.99],
    'category': ['electronics', 'home', 'electronics']
})


@app.agent('user-activity')
async def process_activity(stream):
    """
    Process user activity using Arrow columnar operations.

    Demonstrates:
    - Converting JSON to Arrow RecordBatches
    - Columnar filtering
    - Joining with reference data
    - Zero-copy transformations
    """
    print("🏹 Arrow Operations Agent started")
    print(f"📊 User profiles loaded: {user_profiles.num_rows} users")
    print(f"📦 Product catalog loaded: {product_catalog.num_rows} products\n")

    # Batch processing for Arrow efficiency
    batch_size = 10
    batch_buffer = []
    processed_count = 0

    async for activity in stream:
        try:
            batch_buffer.append(activity)

            # Process in batches for Arrow efficiency
            if len(batch_buffer) >= batch_size:
                # Convert batch to Arrow Table
                activity_table = pa.Table.from_pylist(batch_buffer)

                # Arrow compute operations (SIMD-accelerated)
                print(f"\n📊 Processing Arrow batch: {activity_table.num_rows} records")

                # Filter: Only mobile activities (columnar operation)
                is_mobile = pc.equal(activity_table['device_type'], 'mobile')
                mobile_activities = activity_table.filter(is_mobile)
                print(f"   📱 Mobile activities: {mobile_activities.num_rows}")

                # Filter: Only purchases
                is_purchase = pc.equal(activity_table['activity_type'], 'purchase')
                purchases = activity_table.filter(is_purchase)
                print(f"   💰 Purchases: {purchases.num_rows}")

                # Join with user profiles (for enrichment)
                for i in range(activity_table.num_rows):
                    user_id = activity_table['user_id'][i].as_py()
                    activity_type = activity_table['activity_type'][i].as_py()

                    # Look up user in reference table
                    user_mask = pc.equal(user_profiles['user_id'], user_id)
                    user_data = user_profiles.filter(user_mask)

                    if user_data.num_rows > 0:
                        tier = user_data['tier'][0].as_py()
                        name = user_data['name'][0].as_py()

                        processed_count += 1
                        print(f"   ✅ {name} ({tier}): {activity_type}")

                # Yield enriched batch results
                yield {
                    "batch_size": activity_table.num_rows,
                    "mobile_count": mobile_activities.num_rows,
                    "purchase_count": purchases.num_rows,
                    "processed_count": processed_count
                }

                # Clear batch
                batch_buffer = []

        except Exception as e:
            print(f"❌ Error processing activity: {e}")
            continue


@app.agent('orders')
async def process_orders(stream):
    """
    Process orders with Arrow-based product catalog joins.

    Demonstrates:
    - Streaming joins with static Arrow tables
    - Columnar aggregations
    - Price calculations
    """
    print("📦 Order Processing Agent started\n")

    order_count = 0
    total_revenue = 0.0

    async for order in stream:
        try:
            product_id = order.get('product_id')
            quantity = order.get('quantity', 1)
            user_id = order.get('user_id')

            # Look up product in Arrow catalog
            product_mask = pc.equal(product_catalog['product_id'], product_id)
            product_data = product_catalog.filter(product_mask)

            if product_data.num_rows > 0:
                product_name = product_data['name'][0].as_py()
                price = product_data['price'][0].as_py()
                category = product_data['category'][0].as_py()

                # Calculate order total
                order_total = price * quantity
                total_revenue += order_total

                order_count += 1

                print(f"💰 Order #{order_count}: {user_id} bought {quantity}x {product_name} "
                      f"(${price:.2f} each) = ${order_total:.2f}")

                # Periodic stats
                if order_count % 5 == 0:
                    print(f"\n📊 Order Stats:")
                    print(f"   Total orders: {order_count}")
                    print(f"   Total revenue: ${total_revenue:.2f}")
                    print(f"   Avg order value: ${total_revenue/order_count:.2f}\n")

                yield {
                    "order_id": order.get('order_id'),
                    "user_id": user_id,
                    "product_name": product_name,
                    "quantity": quantity,
                    "order_total": order_total,
                    "category": category
                }

        except Exception as e:
            print(f"❌ Error processing order: {e}")
            continue


@app.agent('user-activity')
async def analytics_agent(stream):
    """
    Perform columnar analytics on activity streams.

    Demonstrates:
    - Batch aggregations
    - Columnar group-by operations
    - SIMD-accelerated computations
    """
    print("📈 Analytics Agent started\n")

    # Collect data for periodic analysis
    analysis_buffer = []
    analysis_interval = 15  # Analyze every 15 records

    async for activity in stream:
        try:
            analysis_buffer.append(activity)

            if len(analysis_buffer) >= analysis_interval:
                # Convert to Arrow for columnar analysis
                analysis_table = pa.Table.from_pylist(analysis_buffer)

                print("\n📊 Analytics Report (Arrow-accelerated):")

                # Count by activity type (columnar aggregation)
                activity_types = analysis_table['activity_type'].to_pylist()
                type_counts = {}
                for at in activity_types:
                    type_counts[at] = type_counts.get(at, 0) + 1

                print(f"   Activity breakdown:")
                for activity_type, count in sorted(type_counts.items()):
                    print(f"      {activity_type}: {count}")

                # Count by device type
                device_types = analysis_table['device_type'].to_pylist()
                device_counts = {}
                for dt in device_types:
                    device_counts[dt] = device_counts.get(dt, 0) + 1

                print(f"   Device breakdown:")
                for device_type, count in sorted(device_counts.items()):
                    print(f"      {device_type}: {count}")

                print()

                # Clear buffer
                analysis_buffer = []

        except Exception as e:
            print(f"❌ Error in analytics: {e}")
            continue


if __name__ == "__main__":
    print(__doc__)
    print("\n" + "="*60)
    print("ARROW OPERATIONS BENEFITS:")
    print("="*60)
    print("""
🏹 Arrow Columnar Format Benefits:

1. **Zero-Copy Operations**
   - No serialization/deserialization overhead
   - Direct memory access
   - 10-100x faster than Python objects

2. **SIMD Acceleration**
   - Vectorized operations on columnar data
   - CPU-level parallel processing
   - Massive speedup for filters/aggregations

3. **Memory Efficiency**
   - Columnar layout = better compression
   - Lower memory footprint
   - Cache-friendly data access

4. **Language Interop**
   - Same format across Python, Java, C++, Rust
   - No conversion overhead
   - True zero-copy between systems

Example Arrow Operations:
-------------------------

# Create table
table = pa.table({'col': [1, 2, 3, 4, 5]})

# Filter (SIMD-accelerated)
filtered = table.filter(pc.greater(table['col'], 2))

# Aggregate
total = pc.sum(table['col']).as_py()

# Zero-copy slice
subset = table.slice(0, 3)  # No data copied!
""")
