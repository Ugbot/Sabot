#!/usr/bin/env python3
"""Test with fresh morsel operator."""

from sabot import cyarrow as pa
from sabot.cyarrow import compute as pc
from sabot._cython.operators.transform import CythonMapOperator
from sabot._cython.operators.morsel_operator import MorselDrivenOperator

print("Test 1: Large batch only...")

def double_x(batch):
    return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

map_op = CythonMapOperator(source=None, map_func=double_x)
morsel_op = MorselDrivenOperator(map_op, num_workers=2)

large_batch = pa.RecordBatch.from_pydict({'x': list(range(15000))})
print(f"Processing {large_batch.num_rows} rows...")

result = morsel_op.process_batch(large_batch)
print(f"✅ Result: {result.num_rows} rows")
