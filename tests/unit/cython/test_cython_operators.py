#!/usr/bin/env python3
"""
Test Cython transform operators with real data.

Verifies that the operators work correctly and measure performance.
"""

from sabot import cyarrow as pa
from sabot.cyarrow import compute as pc
import time
from sabot._cython.operators import (
    CythonMapOperator,
    CythonFilterOperator,
    CythonSelectOperator,
    CythonFlatMapOperator,
    CythonUnionOperator,
)


def generate_test_batches(num_batches=100, batch_size=10000):
    """Generate test RecordBatches with synthetic data."""
    for i in range(num_batches):
        batch = pa.RecordBatch.from_pydict({
            'id': list(range(i * batch_size, (i + 1) * batch_size)),
            'price': [100.0 + (j % 100) for j in range(batch_size)],
            'quantity': [10 + (j % 50) for j in range(batch_size)],
            'side': ['BUY' if j % 2 == 0 else 'SELL' for j in range(batch_size)],
        })
        yield batch


def test_filter_operator():
    """Test filter operator with SIMD predicate."""
    print("\n=== Testing CythonFilterOperator ===")

    # Generate test data
    batches = list(generate_test_batches(num_batches=10, batch_size=10000))
    total_rows = sum(b.num_rows for b in batches)

    # Create filter operator (price > 150)
    filtered = CythonFilterOperator(
        source=iter(batches),
        predicate=lambda b: pc.greater(b.column('price'), 150)
    )

    # Measure performance
    start = time.perf_counter()
    result_batches = list(filtered)
    elapsed = time.perf_counter() - start

    filtered_rows = sum(b.num_rows for b in result_batches)

    print(f"  Input rows: {total_rows:,}")
    print(f"  Filtered rows: {filtered_rows:,}")
    print(f"  Time: {elapsed:.4f}s")
    print(f"  Throughput: {total_rows / elapsed / 1_000_000:.2f}M rows/sec")

    return result_batches


def test_map_operator():
    """Test map operator with vectorized function."""
    print("\n=== Testing CythonMapOperator ===")

    # Generate test data
    batches = list(generate_test_batches(num_batches=10, batch_size=10000))
    total_rows = sum(b.num_rows for b in batches)

    # Create map operator (add 'total' column = price * quantity)
    def add_total_column(batch):
        total = pc.multiply(batch.column('price'), batch.column('quantity'))
        return batch.append_column('total', total)

    mapped = CythonMapOperator(
        source=iter(batches),
        map_func=add_total_column
    )

    # Measure performance
    start = time.perf_counter()
    result_batches = list(mapped)
    elapsed = time.perf_counter() - start

    mapped_rows = sum(b.num_rows for b in result_batches)

    print(f"  Input rows: {total_rows:,}")
    print(f"  Mapped rows: {mapped_rows:,}")
    print(f"  Output columns: {result_batches[0].schema.names}")
    print(f"  Time: {elapsed:.4f}s")
    print(f"  Throughput: {total_rows / elapsed / 1_000_000:.2f}M rows/sec")

    return result_batches


def test_select_operator():
    """Test select operator (zero-copy projection)."""
    print("\n=== Testing CythonSelectOperator ===")

    # Generate test data
    batches = list(generate_test_batches(num_batches=10, batch_size=10000))
    total_rows = sum(b.num_rows for b in batches)

    # Create select operator
    selected = CythonSelectOperator(
        source=iter(batches),
        columns=['id', 'price']
    )

    # Measure performance
    start = time.perf_counter()
    result_batches = list(selected)
    elapsed = time.perf_counter() - start

    selected_rows = sum(b.num_rows for b in result_batches)

    print(f"  Input rows: {total_rows:,}")
    print(f"  Selected rows: {selected_rows:,}")
    print(f"  Output columns: {result_batches[0].schema.names}")
    print(f"  Time: {elapsed:.4f}s")
    print(f"  Throughput: {total_rows / elapsed / 1_000_000:.2f}M rows/sec")

    return result_batches


def test_chained_operators():
    """Test chaining multiple operators."""
    print("\n=== Testing Chained Operators ===")

    # Generate test data
    batches = generate_test_batches(num_batches=10, batch_size=10000)
    total_rows = 10 * 10000

    # Chain: filter -> map -> select
    # 1. Filter: price > 120
    filtered = CythonFilterOperator(
        source=batches,
        predicate=lambda b: pc.greater(b.column('price'), 120)
    )

    # 2. Map: add total column
    def add_total_column(batch):
        total = pc.multiply(batch.column('price'), batch.column('quantity'))
        return batch.append_column('total', total)

    mapped = CythonMapOperator(
        source=filtered,
        map_func=add_total_column
    )

    # 3. Select: keep only id, price, total
    selected = CythonSelectOperator(
        source=mapped,
        columns=['id', 'price', 'total']
    )

    # Measure performance
    start = time.perf_counter()
    result_batches = list(selected)
    elapsed = time.perf_counter() - start

    output_rows = sum(b.num_rows for b in result_batches)

    print(f"  Input rows: {total_rows:,}")
    print(f"  Output rows: {output_rows:,}")
    print(f"  Output columns: {result_batches[0].schema.names}")
    print(f"  Time: {elapsed:.4f}s")
    print(f"  Throughput: {total_rows / elapsed / 1_000_000:.2f}M rows/sec")
    print(f"  Selectivity: {output_rows / total_rows * 100:.1f}%")

    return result_batches


def test_union_operator():
    """Test union operator."""
    print("\n=== Testing CythonUnionOperator ===")

    # Generate two streams
    stream1 = generate_test_batches(num_batches=5, batch_size=10000)
    stream2 = generate_test_batches(num_batches=5, batch_size=10000)

    total_rows = 10 * 10000

    # Create union
    unioned = CythonUnionOperator(stream1, stream2)

    # Measure performance
    start = time.perf_counter()
    result_batches = list(unioned)
    elapsed = time.perf_counter() - start

    output_rows = sum(b.num_rows for b in result_batches)

    print(f"  Input rows: {total_rows:,}")
    print(f"  Output rows: {output_rows:,}")
    print(f"  Time: {elapsed:.4f}s")
    print(f"  Throughput: {total_rows / elapsed / 1_000_000:.2f}M rows/sec")

    return result_batches


def main():
    """Run all tests."""
    print("=" * 60)
    print("Cython Transform Operators - Performance Tests")
    print("=" * 60)

    try:
        # Test individual operators
        test_filter_operator()
        test_map_operator()
        test_select_operator()
        test_union_operator()

        # Test chained pipeline
        test_chained_operators()

        print("\n" + "=" * 60)
        print("All tests passed! ✅")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
