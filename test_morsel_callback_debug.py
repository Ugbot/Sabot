#!/usr/bin/env python3
"""Debug morsel operator callbacks."""

import sys
from sabot import cyarrow as pa
from sabot.cyarrow import compute as pc
from sabot._cython.operators.transform import CythonMapOperator
from sabot._cython.operators.morsel_operator import MorselDrivenOperator

print("Testing MorselDrivenOperator callback...")

# Create wrapped operator with debug callback
call_count = [0]

def debug_double_x(batch):
    call_count[0] += 1
    print(f"   [callback {call_count[0]}] batch with {batch.num_rows} rows", flush=True)
    result = batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))
    print(f"   [callback {call_count[0]}] returning {result.num_rows} rows", flush=True)
    return result

print("1. Creating operators...")
map_op = CythonMapOperator(source=None, map_func=debug_double_x)
morsel_op = MorselDrivenOperator(map_op, num_workers=2)
print("   ✓ Operators created")

print("\n2. Creating batch...")
batch = pa.RecordBatch.from_pydict({'x': list(range(15000))})
print(f"   ✓ Created {batch.num_rows} rows")

print("\n3. Processing with morsels...")
print("   [BEFORE process_batch]", flush=True)
sys.stdout.flush()

try:
    result = morsel_op.process_batch(batch)
    print(f"   [AFTER process_batch] Got {result.num_rows} rows")
    print(f"   Total callbacks: {call_count[0]}")
    print("   ✅ Success!")
except Exception as e:
    print(f"   ✗ Error: {e}")
    import traceback
    traceback.print_exc()
