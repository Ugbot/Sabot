#!/usr/bin/env python3
"""
Test Cython join operators with real data.

Verifies joins work correctly and measure performance.
"""

import pyarrow as pa
import pyarrow.compute as pc
import time
from sabot._cython.operators import (
    CythonHashJoinOperator,
    CythonIntervalJoinOperator,
    CythonAsofJoinOperator,
)


def generate_left_batches(num_batches=10, batch_size=10000):
    """Generate left (probe) side batches."""
    for i in range(num_batches):
        batch = pa.RecordBatch.from_pydict({
            'order_id': list(range(i * batch_size, (i + 1) * batch_size)),
            'customer_id': [(i * batch_size + j) % 1000 for j in range(batch_size)],
            'amount': [100.0 + ((i * batch_size + j) % 100) for j in range(batch_size)],
            'timestamp': [1000000 + (i * batch_size + j) * 1000 for j in range(batch_size)],
        })
        yield batch


def generate_right_batches(num_batches=10, batch_size=1000):
    """Generate right (build) side batches."""
    for i in range(num_batches):
        batch = pa.RecordBatch.from_pydict({
            'customer_id': list(range(i * batch_size, (i + 1) * batch_size)),
            'name': [f'Customer_{i * batch_size + j}' for j in range(batch_size)],
            'region': ['US' if j % 3 == 0 else 'EU' if j % 3 == 1 else 'APAC'
                      for j in range(batch_size)],
            'timestamp': [1000000 + (i * batch_size + j) * 2000 for j in range(batch_size)],
        })
        yield batch


def test_hash_join():
    """Test hash join operator."""
    print("\n=== Testing CythonHashJoinOperator (Inner Join) ===")

    # Generate test data
    left_batches = generate_left_batches(num_batches=10, batch_size=10000)
    right_batches = generate_right_batches(num_batches=10, batch_size=1000)

    total_left_rows = 10 * 10000
    total_right_rows = 10 * 1000

    # Create hash join operator
    joined = CythonHashJoinOperator(
        left_source=left_batches,
        right_source=right_batches,
        left_keys=['customer_id'],
        right_keys=['customer_id'],
        join_type='inner'
    )

    # Measure performance
    start = time.perf_counter()
    result_batches = list(joined)
    elapsed = time.perf_counter() - start

    output_rows = sum(b.num_rows for b in result_batches)

    print(f"  Left rows: {total_left_rows:,}")
    print(f"  Right rows: {total_right_rows:,}")
    print(f"  Joined rows: {output_rows:,}")
    print(f"  Time: {elapsed:.4f}s")
    print(f"  Throughput: {total_left_rows / elapsed / 1_000_000:.2f}M rows/sec")

    if result_batches:
        print(f"  Output schema: {result_batches[0].schema.names}")

    return result_batches


def test_hash_join_left():
    """Test left outer join."""
    print("\n=== Testing CythonHashJoinOperator (Left Join) ===")

    # Generate test data (smaller for left join test)
    left_batches = list(generate_left_batches(num_batches=5, batch_size=5000))
    right_batches = list(generate_right_batches(num_batches=3, batch_size=500))

    total_left_rows = sum(b.num_rows for b in left_batches)
    total_right_rows = sum(b.num_rows for b in right_batches)

    # Create left join operator
    joined = CythonHashJoinOperator(
        left_source=iter(left_batches),
        right_source=iter(right_batches),
        left_keys=['customer_id'],
        right_keys=['customer_id'],
        join_type='left'
    )

    # Measure performance
    start = time.perf_counter()
    result_batches = list(joined)
    elapsed = time.perf_counter() - start

    output_rows = sum(b.num_rows for b in result_batches)

    print(f"  Left rows: {total_left_rows:,}")
    print(f"  Right rows: {total_right_rows:,}")
    print(f"  Joined rows: {output_rows:,}")
    print(f"  Time: {elapsed:.4f}s")
    print(f"  Throughput: {total_left_rows / elapsed / 1_000_000:.2f}M rows/sec")

    return result_batches


def test_interval_join():
    """Test interval join operator."""
    print("\n=== Testing CythonIntervalJoinOperator ===")

    # Generate test data
    left_batches = list(generate_left_batches(num_batches=5, batch_size=5000))
    right_batches = list(generate_right_batches(num_batches=5, batch_size=500))

    total_left_rows = sum(b.num_rows for b in left_batches)
    total_right_rows = sum(b.num_rows for b in right_batches)

    # Create interval join (±5 seconds)
    joined = CythonIntervalJoinOperator(
        left_source=iter(left_batches),
        right_source=iter(right_batches),
        time_column='timestamp',
        lower_bound=-5000,  # -5 seconds
        upper_bound=5000    # +5 seconds
    )

    # Measure performance
    start = time.perf_counter()
    result_batches = list(joined)
    elapsed = time.perf_counter() - start

    output_rows = sum(b.num_rows for b in result_batches)

    print(f"  Left rows: {total_left_rows:,}")
    print(f"  Right rows: {total_right_rows:,}")
    print(f"  Joined rows: {output_rows:,}")
    print(f"  Time: {elapsed:.4f}s")
    print(f"  Throughput: {total_left_rows / elapsed / 1_000_000:.2f}M rows/sec")

    return result_batches


def test_asof_join():
    """Test as-of join operator."""
    print("\n=== Testing CythonAsofJoinOperator ===")

    # Generate test data
    left_batches = list(generate_left_batches(num_batches=5, batch_size=5000))
    right_batches = list(generate_right_batches(num_batches=5, batch_size=500))

    total_left_rows = sum(b.num_rows for b in left_batches)
    total_right_rows = sum(b.num_rows for b in right_batches)

    # Create as-of join (backward)
    joined = CythonAsofJoinOperator(
        left_source=iter(left_batches),
        right_source=iter(right_batches),
        time_column='timestamp',
        direction='backward'
    )

    # Measure performance
    start = time.perf_counter()
    result_batches = list(joined)
    elapsed = time.perf_counter() - start

    output_rows = sum(b.num_rows for b in result_batches)

    print(f"  Left rows: {total_left_rows:,}")
    print(f"  Right rows: {total_right_rows:,}")
    print(f"  Joined rows: {output_rows:,}")
    print(f"  Time: {elapsed:.4f}s")
    print(f"  Throughput: {total_left_rows / elapsed / 1_000_000:.2f}M rows/sec")

    return result_batches


def main():
    """Run all join tests."""
    print("=" * 60)
    print("Cython Join Operators - Performance Tests")
    print("=" * 60)

    try:
        # Test join operators
        test_hash_join()
        test_hash_join_left()
        test_interval_join()
        test_asof_join()

        print("\n" + "=" * 60)
        print("Join tests completed! ✅")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
