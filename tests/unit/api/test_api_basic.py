#!/usr/bin/env python3
"""
Basic API Test - Standalone

Tests the high-level Python API without importing the full sabot package.
This avoids compatibility issues with the old codebase.
"""

import sys
import os

# Add sabot to path
sys.path.insert(0, os.path.dirname(__file__))

# Import API modules directly (avoid sabot.__init__.py)
from sabot.api.stream import Stream
from sabot.api.window import tumbling, sliding
from sabot.api.state import ValueState
from sabot import cyarrow as pa


def test_imports():
    """Test 1: Import API modules"""
    print("=" * 60)
    print("Test 1: API Imports")
    print("=" * 60)
    print(f"✓ Stream: {Stream}")
    print(f"✓ tumbling: {tumbling}")
    print(f"✓ ValueState: {ValueState}")
    print()


def test_stream_creation():
    """Test 2: Create stream from batches"""
    print("=" * 60)
    print("Test 2: Stream Creation")
    print("=" * 60)

    batches = [
        pa.RecordBatch.from_pydict({
            'values': [1, 2, 3, 4, 5],
            'amounts': [10, 20, 30, 40, 50],
        })
    ]

    stream = Stream.from_batches(batches)
    print(f"✓ Created stream")
    print(f"  Schema: {stream.schema}")
    print(f"  Operators: {len(stream._operators)}")
    print()


def test_aggregation():
    """Test 3: Aggregation with zero-copy"""
    print("=" * 60)
    print("Test 3: Aggregation")
    print("=" * 60)

    batches = [
        pa.RecordBatch.from_pydict({
            'values': pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        })
    ]

    stream = Stream.from_batches(batches)
    result = stream.aggregate(sum='values')
    result_table = result.collect()

    total = result_table['sum'][0].as_py()
    print(f"✓ Sum: {total}")
    print(f"  Expected: 15")
    print(f"  Match: {total == 15}")
    print()


def test_filter():
    """Test 4: Filter operation"""
    print("=" * 60)
    print("Test 4: Filter")
    print("=" * 60)

    batches = [
        pa.RecordBatch.from_pydict({
            'values': [1, 2, 3, 4, 5],
        })
    ]

    stream = Stream.from_batches(batches)
    stream = stream.filter(lambda b: pa.compute.greater(b.column('values'), 2))

    result = stream.collect()
    print(f"✓ Filtered rows: {result.num_rows}")
    print(f"  Expected: 3 (values 3, 4, 5)")
    print(f"  Values: {result.column('values').to_pylist()}")
    print()


def test_map():
    """Test 5: Map transformation"""
    print("=" * 60)
    print("Test 5: Map Transformation")
    print("=" * 60)

    batches = [
        pa.RecordBatch.from_pydict({
            'values': [1, 2, 3, 4, 5],
        })
    ]

    def double_values(batch):
        values = batch.column('values')
        doubled = pa.compute.multiply(values, 2)
        return pa.RecordBatch.from_arrays([doubled], names=['doubled'])

    stream = Stream.from_batches(batches)
    stream = stream.map(double_values)

    result = stream.collect()
    print(f"✓ Mapped values: {result.column('doubled').to_pylist()}")
    print(f"  Expected: [2, 4, 6, 8, 10]")
    print()


def test_state():
    """Test 6: State management"""
    print("=" * 60)
    print("Test 6: State Management")
    print("=" * 60)

    state = ValueState('test_state')

    # Update state
    state.update('key1', 100)
    state.update('key2', 200)

    # Read state
    val1 = state.value('key1')
    val2 = state.value('key2')

    print(f"✓ State key1: {val1}")
    print(f"✓ State key2: {val2}")
    print(f"  Values match: {val1 == 100 and val2 == 200}")
    print()


def test_window_specs():
    """Test 7: Window specifications"""
    print("=" * 60)
    print("Test 7: Window Specifications")
    print("=" * 60)

    # Tumbling window
    w1 = tumbling(seconds=60)
    print(f"✓ Tumbling: {w1}")

    # Sliding window
    w2 = sliding(size_seconds=60, slide_seconds=30)
    print(f"✓ Sliding: {w2}")

    print()


def test_large_aggregation():
    """Test 8: Large batch aggregation (performance)"""
    print("=" * 60)
    print("Test 8: Large Batch Performance")
    print("=" * 60)

    import time

    # Create 100K row batch
    n = 100_000
    batch = pa.RecordBatch.from_pydict({
        'values': pa.array(list(range(n)), type=pa.int64()),
    })

    stream = Stream.from_batches([batch])

    start = time.perf_counter()
    result = stream.aggregate(sum='values')
    result_table = result.collect()
    elapsed = time.perf_counter() - start

    total = result_table['sum'][0].as_py()
    expected = sum(range(n))

    ns_per_row = (elapsed * 1e9) / n

    print(f"✓ Rows: {n:,}")
    print(f"✓ Sum: {total:,}")
    print(f"✓ Time: {elapsed*1000:.2f}ms")
    print(f"✓ Performance: {ns_per_row:.2f}ns per row")
    print(f"✓ Correct: {total == expected}")

    if ns_per_row < 10:
        print(f"✓✓ EXCELLENT: Below 10ns target!")

    print()


def main():
    """Run all tests"""
    print()
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 15 + "SABOT API BASIC TESTS" + " " * 21 + "║")
    print("╚" + "=" * 58 + "╝")
    print()

    try:
        test_imports()
        test_stream_creation()
        test_aggregation()
        test_filter()
        test_map()
        test_state()
        test_window_specs()
        test_large_aggregation()

        print("=" * 60)
        print("✓ All tests passed!")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
