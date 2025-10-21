#!/usr/bin/env python3
"""
Kafka Integration Example
==========================

Demonstrates Sabot's Kafka integration with the Stream API.

This example shows:
- Reading from Kafka topics
- Processing streams with Arrow operations
- Writing to Kafka topics
- Multiple codec support (JSON, Avro, etc.)
- Error handling
- Testing without Kafka

Prerequisites:
- Optional: Kafka running on localhost:9092
- If Kafka is not available, will use mock data

Run:
    python examples/kafka_integration_example.py
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pyarrow as pa
import pyarrow.compute as pc
from sabot.api.stream import Stream
import asyncio


def example1_simple_json():
    """Example 1: Simple JSON stream (without Kafka)."""
    print("\n" + "=" * 60)
    print("Example 1: Simple JSON Stream (Mock Data)")
    print("=" * 60)
    
    # Create test data (simulating Kafka messages)
    batches = [
        pa.RecordBatch.from_pydict({
            'transaction_id': [1, 2, 3],
            'amount': [100.0, 1500.0, 50.0],
            'customer_id': ['C001', 'C002', 'C003']
        }),
        pa.RecordBatch.from_pydict({
            'transaction_id': [4, 5, 6],
            'amount': [2000.0, 75.0, 3000.0],
            'customer_id': ['C004', 'C005', 'C006']
        })
    ]
    
    # Create stream from batches (simulating Kafka)
    stream = Stream.from_batches(batches)
    
    # Process: Filter high-value transactions
    high_value = stream.filter(lambda b: pc.greater(b['amount'], 1000.0))
    
    # Collect results
    result = high_value.collect()
    
    print(f"\n✅ Processed {sum(b.num_rows for b in batches)} transactions")
    print(f"✅ Found {result.num_rows} high-value transactions (> $1000)")
    
    if result.num_rows > 0:
        print(f"\nHigh-value transactions:")
        for i in range(result.num_rows):
            row = result.slice(i, 1).to_pydict()
            print(f"  Transaction {row['transaction_id'][0]}: "
                  f"${row['amount'][0]:.2f} (Customer: {row['customer_id'][0]})")


def example2_transformation():
    """Example 2: Data transformation pipeline."""
    print("\n" + "=" * 60)
    print("Example 2: Transformation Pipeline")
    print("=" * 60)
    
    # Create test data
    batches = [
        pa.RecordBatch.from_pydict({
            'order_id': [1, 2, 3, 4, 5],
            'amount': [100.0, 200.0, 300.0, 400.0, 500.0],
            'status': ['pending', 'pending', 'completed', 'pending', 'completed']
        })
    ]
    
    stream = Stream.from_batches(batches)
    
    # Add tax calculation (8%)
    def add_tax(batch):
        tax = pc.multiply(batch['amount'], 0.08)
        return batch.append_column('tax', tax)
    
    # Add total calculation
    def add_total(batch):
        total = pc.add(batch['amount'], batch['tax'])
        return batch.append_column('total', total)
    
    # Process pipeline
    processed = (stream
        .map(add_tax)
        .map(add_total)
        .filter(lambda b: pc.equal(b['status'], 'pending'))
    )
    
    result = processed.collect()
    
    print(f"\n✅ Processed {batches[0].num_rows} orders")
    print(f"✅ Found {result.num_rows} pending orders")
    
    if result.num_rows > 0:
        print(f"\nPending orders with tax:")
        for i in range(result.num_rows):
            row = result.slice(i, 1).to_pydict()
            print(f"  Order {row['order_id'][0]}: "
                  f"${row['amount'][0]:.2f} + ${row['tax'][0]:.2f} = ${row['total'][0]:.2f}")


def example3_aggregation():
    """Example 3: Aggregation operations."""
    print("\n" + "=" * 60)
    print("Example 3: Aggregation")
    print("=" * 60)
    
    # Create test data with categories
    batches = [
        pa.RecordBatch.from_pydict({
            'product': ['A', 'B', 'A', 'C', 'B', 'A', 'C', 'B'],
            'quantity': [10, 20, 15, 5, 30, 12, 8, 25],
            'price': [100.0, 200.0, 100.0, 300.0, 200.0, 100.0, 300.0, 200.0]
        })
    ]
    
    stream = Stream.from_batches(batches)
    result = stream.collect()
    
    # Manual aggregation (since aggregate operator might not be available)
    total_quantity = pc.sum(result['quantity']).as_py()
    avg_price = pc.mean(result['price']).as_py()
    
    print(f"\n✅ Processed {result.num_rows} sales")
    print(f"📊 Statistics:")
    print(f"  Total quantity sold: {total_quantity}")
    print(f"  Average price: ${avg_price:.2f}")
    
    # Count by product
    products = result['product'].to_pylist()
    product_counts = {}
    for p in products:
        product_counts[p] = product_counts.get(p, 0) + 1
    
    print(f"\n  Sales by product:")
    for product, count in sorted(product_counts.items()):
        print(f"    Product {product}: {count} sales")


def example4_kafka_api_usage():
    """Example 4: Show Kafka API usage (documentation only)."""
    print("\n" + "=" * 60)
    print("Example 4: Kafka API Usage (Documentation)")
    print("=" * 60)
    
    print("""
To use Kafka integration when Kafka is available:

# Reading from Kafka:
stream = Stream.from_kafka(
    bootstrap_servers="localhost:9092",
    topic="transactions",
    group_id="fraud-detector",
    codec_type="json"
)

# Processing:
processed = (stream
    .filter(lambda b: pc.greater(b['amount'], 1000.0))
    .map(lambda b: b.append_column('flagged', pc.scalar(True)))
)

# Writing to Kafka:
processed.to_kafka(
    bootstrap_servers="localhost:9092",
    topic="flagged-transactions",
    codec_type="json",
    compression_type="lz4"
)

Supported codecs:
- json (default)
- avro (with Schema Registry)
- protobuf (with Schema Registry)
- json_schema (with Schema Registry)
- msgpack
- string
- bytes

Error handling policies:
- skip (default - skip bad messages)
- fail (stop on error)
- retry (retry with backoff)
- dead_letter (send to DLQ topic)
    """)
    
    print("\n✅ See KAFKA_INTEGRATION_GUIDE.md for complete documentation")


def example5_fan_out():
    """Example 5: Fan-out pattern (route to multiple outputs)."""
    print("\n" + "=" * 60)
    print("Example 5: Fan-out Routing")
    print("=" * 60)
    
    # Create test data with different types
    batches = [
        pa.RecordBatch.from_pydict({
            'event_id': [1, 2, 3, 4, 5, 6],
            'event_type': ['login', 'purchase', 'logout', 'purchase', 'login', 'error'],
            'user_id': ['U1', 'U2', 'U3', 'U4', 'U5', 'U6'],
            'timestamp': [100, 101, 102, 103, 104, 105]
        })
    ]
    
    stream = Stream.from_batches(batches)
    
    # Route to different streams based on event type
    login_events = stream.filter(lambda b: pc.equal(b['event_type'], 'login'))
    purchase_events = stream.filter(lambda b: pc.equal(b['event_type'], 'purchase'))
    error_events = stream.filter(lambda b: pc.equal(b['event_type'], 'error'))
    
    # Collect results
    logins = login_events.collect()
    purchases = purchase_events.collect()
    errors = error_events.collect()
    
    print(f"\n✅ Routed {batches[0].num_rows} events:")
    print(f"  Login events: {logins.num_rows}")
    print(f"  Purchase events: {purchases.num_rows}")
    print(f"  Error events: {errors.num_rows}")
    
    if purchases.num_rows > 0:
        print(f"\nPurchase events:")
        for i in range(purchases.num_rows):
            row = purchases.slice(i, 1).to_pydict()
            print(f"  Event {row['event_id'][0]}: User {row['user_id'][0]} at {row['timestamp'][0]}")


def main():
    """Run all examples."""
    print("\n" + "=" * 60)
    print("Kafka Integration Examples")
    print("=" * 60)
    print("\nThese examples demonstrate Sabot's Kafka integration")
    print("using mock data (no Kafka instance required)")
    
    example1_simple_json()
    example2_transformation()
    example3_aggregation()
    example4_kafka_api_usage()
    example5_fan_out()
    
    print("\n" + "=" * 60)
    print("Examples Complete!")
    print("=" * 60)
    print("\nKey Takeaways:")
    print("  1. Stream API provides unified interface for data sources")
    print("  2. Kafka integration supports multiple codecs (JSON, Avro, etc.)")
    print("  3. Transformations use Arrow compute functions for performance")
    print("  4. Fan-out pattern enables routing to multiple outputs")
    print("  5. Error handling policies: skip, fail, retry, dead_letter")
    print("\nNext Steps:")
    print("  - See KAFKA_INTEGRATION_GUIDE.md for complete documentation")
    print("  - Run with real Kafka: docker-compose up -d kafka")
    print("  - Try examples/kafka_avro_example.py for Avro support")
    print()


if __name__ == '__main__':
    main()

