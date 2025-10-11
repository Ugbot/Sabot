#!/usr/bin/env python3
"""Standalone test of MorselDrivenOperator (no pytest)."""

from sabot import cyarrow as pa
from sabot.cyarrow import compute as pc
from sabot._cython.operators.transform import CythonMapOperator
from sabot._cython.operators.morsel_operator import MorselDrivenOperator

print("Testing MorselDrivenOperator without pytest...")

# Test 1: Small batch (should bypass morsels)
print("\n1. Testing small batch...")
def double_x(batch):
    return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

map_op = CythonMapOperator(source=None, map_func=double_x)
morsel_op = MorselDrivenOperator(map_op, num_workers=2)

small_batch = pa.RecordBatch.from_pydict({'x': list(range(100))})
result = morsel_op.process_batch(small_batch)
assert result.num_rows == 100
print(f"   ✓ Small batch: {result.num_rows} rows")

# Test 2: Large batch (should use morsels)
print("\n2. Testing large batch with morsels...")
large_batch = pa.RecordBatch.from_pydict({'x': list(range(15000))})
print(f"   Processing {large_batch.num_rows} rows...")

result = morsel_op.process_batch(large_batch)
assert result.num_rows == 15000
assert result.column('x').to_pylist()[0] == 0
assert result.column('x').to_pylist()[100] == 200  # 100 * 2
print(f"   ✓ Large batch: {result.num_rows} rows processed correctly")

print("\n✅ All tests passed!")
