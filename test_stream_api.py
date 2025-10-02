#!/usr/bin/env python3
"""
Test high-level Stream API with Cython operators.

Demonstrates user-friendly API that automatically uses Cython acceleration.
"""

import pyarrow as pa
import pyarrow.compute as pc
import time
from sabot.api.stream import Stream


def generate_test_data(num_rows=100000):
    """Generate test data as list of dicts."""
    return [
        {
            'id': i,
            'customer_id': i % 1000,
            'price': 100.0 + (i % 100),
            'quantity': 10 + (i % 50),
            'side': 'BUY' if i % 2 == 0 else 'SELL',
            'timestamp': 1000000 + i * 1000,
        }
        for i in range(num_rows)
    ]


def test_simple_pipeline():
    """Test simple filter → map → select pipeline."""
    print("\n=== Testing Simple Pipeline ===")

    # Generate data
    data = generate_test_data(100000)

    # Create stream
    stream = Stream.from_dicts(data, batch_size=10000)

    # Chain operations (lazy)
    result = (stream
        .filter(lambda b: pc.greater(b.column('price'), 120))
        .map(lambda b: b.append_column('total',
            pc.multiply(b.column('price'), b.column('quantity'))))
        .select('id', 'price', 'quantity', 'total')
    )

    # Execute (consume)
    start = time.perf_counter()
    total_rows = 0
    batch_count = 0
    for batch in result:
        total_rows += batch.num_rows
        batch_count += 1

    elapsed = time.perf_counter() - start

    print(f"  Input rows: {len(data):,}")
    print(f"  Output rows: {total_rows:,}")
    print(f"  Output batches: {batch_count}")
    print(f"  Time: {elapsed:.4f}s")
    print(f"  Throughput: {len(data) / elapsed / 1_000_000:.2f}M rows/sec")

    return total_rows


def test_aggregation():
    """Test global aggregation."""
    print("\n=== Testing Aggregation ===")

    # Generate data
    data = generate_test_data(100000)

    # Create stream and aggregate
    stream = Stream.from_dicts(data, batch_size=10000)

    start = time.perf_counter()
    result = stream.aggregate({
        'total_price': ('price', 'sum'),
        'avg_price': ('price', 'mean'),
        'max_quantity': ('quantity', 'max'),
        'count': ('*', 'count'),
    })

    # Collect result
    result_table = result.collect()
    elapsed = time.perf_counter() - start

    print(f"  Input rows: {len(data):,}")
    print(f"  Time: {elapsed:.4f}s")
    print(f"  Throughput: {len(data) / elapsed / 1_000_000:.2f}M rows/sec")
    print(f"  Results:")
    for name in result_table.schema.names:
        print(f"    {name}: {result_table.column(name)[0].as_py()}")

    return result_table


def test_distinct():
    """Test distinct operation."""
    print("\n=== Testing Distinct ===")

    # Generate data
    data = generate_test_data(100000)

    # Create stream and get distinct customer_ids
    stream = Stream.from_dicts(data, batch_size=10000)

    start = time.perf_counter()
    result = stream.distinct('customer_id')

    # Count unique
    unique_count = 0
    for batch in result:
        unique_count += batch.num_rows

    elapsed = time.perf_counter() - start

    print(f"  Input rows: {len(data):,}")
    print(f"  Unique customers: {unique_count:,}")
    print(f"  Time: {elapsed:.4f}s")
    print(f"  Throughput: {len(data) / elapsed / 1_000_000:.2f}M rows/sec")
    print(f"  Reduction: {(1 - unique_count / len(data)) * 100:.1f}%")

    return unique_count


def test_join():
    """Test hash join."""
    print("\n=== Testing Hash Join ===")

    # Generate left side (orders)
    orders_data = generate_test_data(50000)
    orders = Stream.from_dicts(orders_data, batch_size=10000)

    # Generate right side (customers)
    customers_data = [
        {'customer_id': i, 'name': f'Customer_{i}', 'region': 'US' if i % 3 == 0 else 'EU'}
        for i in range(1000)
    ]
    customers = Stream.from_dicts(customers_data, batch_size=1000)

    # Join
    start = time.perf_counter()
    result = orders.join(
        customers,
        left_keys=['customer_id'],
        right_keys=['customer_id'],
        how='inner'
    )

    # Consume
    total_rows = 0
    for batch in result:
        total_rows += batch.num_rows

    elapsed = time.perf_counter() - start

    print(f"  Left rows: {len(orders_data):,}")
    print(f"  Right rows: {len(customers_data):,}")
    print(f"  Joined rows: {total_rows:,}")
    print(f"  Time: {elapsed:.4f}s")
    print(f"  Throughput: {len(orders_data) / elapsed / 1_000_000:.2f}M rows/sec")

    return total_rows


def test_complex_pipeline():
    """Test complex multi-stage pipeline."""
    print("\n=== Testing Complex Pipeline ===")

    # Generate data
    orders_data = generate_test_data(50000)
    customers_data = [
        {'customer_id': i, 'name': f'Customer_{i}', 'region': 'US' if i % 3 == 0 else 'EU'}
        for i in range(1000)
    ]

    # Build complex pipeline
    orders = Stream.from_dicts(orders_data, batch_size=10000)
    customers = Stream.from_dicts(customers_data, batch_size=1000)

    start = time.perf_counter()

    result = (orders
        # Filter high-value orders
        .filter(lambda b: pc.greater(b.column('price'), 120))
        # Add total column
        .map(lambda b: b.append_column('total',
            pc.multiply(b.column('price'), b.column('quantity'))))
        # Join with customers
        .join(customers,
            left_keys=['customer_id'],
            right_keys=['customer_id'],
            how='left')
        # Select final columns
        .select('id', 'name', 'region', 'total', 'side')
    )

    # Consume
    total_rows = 0
    batch_count = 0
    for batch in result:
        total_rows += batch.num_rows
        batch_count += 1

    elapsed = time.perf_counter() - start

    print(f"  Input rows: {len(orders_data):,}")
    print(f"  Output rows: {total_rows:,}")
    print(f"  Output batches: {batch_count}")
    print(f"  Pipeline stages: filter → map → join → select")
    print(f"  Time: {elapsed:.4f}s")
    print(f"  Throughput: {len(orders_data) / elapsed / 1_000_000:.2f}M rows/sec")

    return total_rows


def test_terminal_operations():
    """Test terminal operations (collect, count, etc.)."""
    print("\n=== Testing Terminal Operations ===")

    data = generate_test_data(10000)
    stream = Stream.from_dicts(data, batch_size=2000)

    # Count
    count = stream.count()
    print(f"  count(): {count:,}")

    # Collect to table
    stream = Stream.from_dicts(data, batch_size=2000)
    table = stream.collect()
    print(f"  collect(): {table.num_rows:,} rows, {table.num_columns} columns")

    # Take first N batches
    stream = Stream.from_dicts(data, batch_size=2000)
    batches = stream.take(3)
    print(f"  take(3): {len(batches)} batches, {sum(b.num_rows for b in batches):,} rows")

    # Convert to pylist
    stream = Stream.from_dicts(data[:100], batch_size=50)
    pylist = stream.to_pylist()
    print(f"  to_pylist(): {len(pylist)} dicts")

    return count


def main():
    """Run all Stream API tests."""
    print("=" * 60)
    print("Sabot High-Level Stream API - Tests")
    print("=" * 60)

    try:
        # Test operations
        test_simple_pipeline()
        test_aggregation()
        test_distinct()
        test_join()
        test_complex_pipeline()
        test_terminal_operations()

        print("\n" + "=" * 60)
        print("All Stream API tests passed! ✅")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
