#!/usr/bin/env python3
"""Simplest possible morsel test."""

from sabot import cyarrow as pa
from sabot.cyarrow import compute as pc
from sabot._cython.operators.transform import CythonMapOperator
from sabot._cython.operators.morsel_operator import MorselDrivenOperator

print("Test: Simple morsel processing")

def double_x(batch):
    print(f"  Processing {batch.num_rows} rows")
    return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

map_op = CythonMapOperator(source=None, map_func=double_x)
morsel_op = MorselDrivenOperator(map_op, num_workers=2)

batch = pa.RecordBatch.from_pydict({'x': list(range(15000))})
print(f"Created batch: {batch.num_rows} rows")

print("Calling process_batch...")
result = morsel_op.process_batch(batch)
print(f"✅ Result: {result.num_rows} rows")

# Explicit cleanup
print("Cleaning up...")
from sabot.cluster.agent_context import AgentContext
ctx = AgentContext.get_instance()
if ctx.task_slot_manager:
    print("  Shutting down TaskSlotManager...")
    ctx.task_slot_manager.shutdown()
    ctx.task_slot_manager = None

print("✅ Done!")
