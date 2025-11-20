#!/usr/bin/env python3
"""Test zero-copy hash_join integration."""

import sys
sys.path.insert(0, '.')

# Use vendored cyarrow
from sabot import cyarrow as pa

# Test 1: Direct hash_join function
print("=" * 60)
print("Test 1: Direct hash_join function")
print("=" * 60)

try:
    from sabot._cython.joins_ql.hash_join import hash_join

    # Create test data
    left = pa.table({
        'id': [1, 2, 3, 4],
        'name': ['Alice', 'Bob', 'Charlie', 'David']
    })

    right = pa.table({
        'id': [2, 3, 4, 5],
        'value': [10, 20, 30, 40]
    })

    print(f"Left table: {left.num_rows} rows")
    print(left.to_pydict())
    print(f"\nRight table: {right.num_rows} rows")
    print(right.to_pydict())

    # Execute zero-copy hash join
    print("\nExecuting zero-copy hash join...")
    result = hash_join(left, right, ['id'], ['id'], join_type='inner')

    print(f"\nResult: {result.num_rows} rows")
    print(result.to_pydict())

    # Verify correctness
    assert result.num_rows == 3, f"Expected 3 rows, got {result.num_rows}"
    result_ids = result.column('id').to_pylist()
    assert sorted(result_ids) == [2, 3, 4], f"Expected [2, 3, 4], got {sorted(result_ids)}"

    print("\n✅ Direct hash_join test PASSED")

except Exception as e:
    print(f"\n❌ Direct hash_join test FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 2: Via sabot.joins module
print("\n" + "=" * 60)
print("Test 2: Via sabot.joins module")
print("=" * 60)

try:
    from sabot.joins import hash_join, HASH_JOIN_AVAILABLE

    print(f"HASH_JOIN_AVAILABLE: {HASH_JOIN_AVAILABLE}")

    if not HASH_JOIN_AVAILABLE:
        print("❌ hash_join not available via sabot.joins")
        sys.exit(1)

    # Same test data
    left = pa.table({
        'user_id': [1, 2, 3],
        'username': ['alice', 'bob', 'charlie']
    })

    right = pa.table({
        'user_id': [2, 3, 4],
        'score': [100, 200, 300]
    })

    print(f"Left: {left.num_rows} rows, Right: {right.num_rows} rows")

    result = hash_join(left, right, ['user_id'], ['user_id'], join_type='inner')

    print(f"Result (inner join): {result.num_rows} rows")
    print(result.to_pydict())

    # Inner join should have 2 matches (user_id 2 and 3)
    assert result.num_rows == 2, f"Expected 2 rows, got {result.num_rows}"

    print("\n✅ sabot.joins module test PASSED")

except Exception as e:
    print(f"\n❌ sabot.joins module test FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 3: Large data test (performance check)
print("\n" + "=" * 60)
print("Test 3: Large data performance test")
print("=" * 60)

try:
    import time

    # Create larger tables
    n_left = 100_000
    n_right = 50_000

    left_data = {
        'id': list(range(n_left)),
        'data': [f'left_{i}' for i in range(n_left)]
    }

    right_data = {
        'id': list(range(n_right)),
        'value': list(range(n_right))
    }

    left = pa.table(left_data)
    right = pa.table(right_data)

    print(f"Left: {left.num_rows:,} rows")
    print(f"Right: {right.num_rows:,} rows")

    start = time.time()
    result = hash_join(left, right, ['id'], ['id'], join_type='inner')
    elapsed = time.time() - start

    print(f"\nResult: {result.num_rows:,} rows")
    print(f"Time: {elapsed:.3f}s")
    print(f"Throughput: {result.num_rows / elapsed:,.0f} rows/sec")

    # Verify correctness
    assert result.num_rows == n_right, f"Expected {n_right} rows, got {result.num_rows}"

    print("\n✅ Large data test PASSED")

except Exception as e:
    print(f"\n❌ Large data test FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "=" * 60)
print("ALL TESTS PASSED ✅")
print("=" * 60)
print("\nZero-copy hash_join integration is working correctly!")
print("Ready for TPC-H benchmarks.")
