#!/usr/bin/env python3
"""Debug script to test TaskSlotManager directly."""

import sys
from sabot import cyarrow as pa
from sabot.cyarrow import compute as pc

print("Starting TaskSlotManager debug test...")

# Test 1: Create TaskSlotManager directly
print("\n1. Creating TaskSlotManager...")
from sabot._c.task_slot_manager import TaskSlotManager
manager = TaskSlotManager(num_slots=2)
print(f"   ✓ Created with {manager.num_slots} slots")

# Test 2: Create test batch
print("\n2. Creating test batch...")
batch = pa.RecordBatch.from_pydict({'x': list(range(1000))})
print(f"   ✓ Created batch with {batch.num_rows} rows")

# Test 3: Create morsels
print("\n3. Creating morsels...")
morsels = []
morsel_size = 500
for start in range(0, batch.num_rows, morsel_size):
    num_rows = min(morsel_size, batch.num_rows - start)
    morsels.append((batch, start, num_rows, 0))
print(f"   ✓ Created {len(morsels)} morsels")

# Test 4: Define processor
print("\n4. Defining processor...")
def double_x(b):
    print(f"   [callback] Processing batch with {b.num_rows} rows")
    return b.set_column(0, 'x', pc.multiply(b.column('x'), 2))
print("   ✓ Processor defined")

# Test 5: Execute morsels
print("\n5. Executing morsels...")
print("   [BEFORE execute_morsels call]")
sys.stdout.flush()

try:
    results = manager.execute_morsels(morsels, double_x)
    print(f"   ✓ Got {len(results)} results")
except Exception as e:
    print(f"   ✗ Error: {e}")
    import traceback
    traceback.print_exc()

print("\n6. Shutting down...")
manager.shutdown()
print("   ✓ Shutdown complete")

print("\n✅ Test complete!")
