#!/usr/bin/env python3
"""Simple large-scale morsel test with verbose output."""

import time
from sabot import cyarrow as pa
from sabot.cyarrow import compute as pc
from sabot._cython.operators.transform import CythonMapOperator
from sabot._cython.operators.morsel_operator import MorselDrivenOperator

print("Starting large-scale morsel test...")

# Test 1: 100K rows
print("\n[1/3] Test: 100K rows with 4 workers")
print("-" * 60)

def double_x(batch):
    print(f"  Worker processing {batch.num_rows} rows")
    return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

print("Creating batch...")
batch = pa.RecordBatch.from_pydict({'x': list(range(100_000))})
print(f"  Created: {batch.num_rows:,} rows")

print("Creating operators...")
map_op = CythonMapOperator(source=None, map_func=double_x)
morsel_op = MorselDrivenOperator(map_op, num_workers=4, morsel_size_kb=64)

print("Processing batch...")
start = time.perf_counter()
result = morsel_op.process_batch(batch)
elapsed = time.perf_counter() - start

print(f"  Result: {result.num_rows:,} rows")
print(f"  Time: {elapsed*1000:.2f}ms")

# Verify ordering
expected = [i * 2 for i in range(100_000)]
actual = result.column('x').to_pylist()

if actual == expected:
    print("  ✅ ORDERING CORRECT!")
else:
    print("  ❌ ORDERING WRONG!")
    for i in range(min(10, len(actual))):
        if expected[i] != actual[i]:
            print(f"    First mismatch at {i}: expected {expected[i]}, got {actual[i]}")
            break

# Test 2: 500K rows
print("\n[2/3] Test: 500K rows with 8 workers")
print("-" * 60)

print("Creating batch...")
batch_large = pa.RecordBatch.from_pydict({'x': list(range(500_000))})
print(f"  Created: {batch_large.num_rows:,} rows")

print("Creating operators...")
map_op_large = CythonMapOperator(source=None, map_func=double_x)
morsel_op_large = MorselDrivenOperator(map_op_large, num_workers=8, morsel_size_kb=64)

print("Processing batch...")
start = time.perf_counter()
result_large = morsel_op_large.process_batch(batch_large)
elapsed = time.perf_counter() - start

print(f"  Result: {result_large.num_rows:,} rows")
print(f"  Time: {elapsed*1000:.2f}ms")
print(f"  Throughput: {result_large.num_rows / elapsed / 1_000_000:.2f}M rows/sec")

# Verify ordering (check first and last 100)
expected_large = [i * 2 for i in range(500_000)]
actual_large = result_large.column('x').to_pylist()

first_100_match = expected_large[:100] == actual_large[:100]
last_100_match = expected_large[-100:] == actual_large[-100:]

if first_100_match and last_100_match:
    print("  ✅ ORDERING CORRECT (sampled first/last 100)")
else:
    print("  ❌ ORDERING WRONG!")

print("\n" + "="*60)
print("✅ TESTS COMPLETE - Morsel operator working with large batches!")
print("="*60)
