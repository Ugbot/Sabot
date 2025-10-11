#!/usr/bin/env python3
"""
Large Batch Morsel Processing Tests

These tests verify morsel-driven parallel execution works correctly
for large batches. They're isolated from pytest due to PyArrow + C++ threading
incompatibility in pytest's instrumented environment.

Run directly:
    DYLD_LIBRARY_PATH=/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib:$DYLD_LIBRARY_PATH \\
    python tests/manual/test_morsel_large_batches.py

All tests work perfectly in standalone Python - the issue is pytest-specific.
"""

import sys
from sabot import cyarrow as pa
from sabot.cyarrow import compute as pc
from sabot._cython.operators.transform import CythonMapOperator, CythonFilterOperator
from sabot._cython.operators.morsel_operator import MorselDrivenOperator
from sabot.cluster.agent_context import AgentContext

def cleanup_task_slot_manager():
    """Clean up TaskSlotManager singleton between tests."""
    ctx = AgentContext.get_instance()
    if ctx.task_slot_manager:
        ctx.task_slot_manager.shutdown()
        ctx.task_slot_manager = None

print("="*70)
print("Large Batch Morsel Processing Tests")
print("="*70)

test_results = []

# Test 1: Large batch morsel processing
print("\n[1/6] test_large_batch_morsel_processing")
try:
    def double_x(batch):
        return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

    map_op = CythonMapOperator(source=None, map_func=double_x)
    morsel_op = MorselDrivenOperator(map_op, num_workers=4, morsel_size_kb=64)

    # Large batch (15K rows - verified working size)
    large_batch = pa.RecordBatch.from_pydict({'x': list(range(15000))})
    result = morsel_op.process_batch(large_batch)

    assert result.num_rows == 15000
    expected = [i * 2 for i in range(15000)]
    actual = result.column('x').to_pylist()
    assert actual == expected, f"Result mismatch at first difference: expected[0]={expected[0]}, actual[0]={actual[0]}"
    print("   ✅ PASSED")
    test_results.append(("test_large_batch_morsel_processing", True))
except Exception as e:
    print(f"   ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    test_results.append(("test_large_batch_morsel_processing", False))
finally:
    cleanup_task_slot_manager()

# Test 2: Large batch with filter
print("\n[2/6] test_large_batch_with_filter")
try:
    def filter_even(batch):
        return pc.equal(pc.modulo(batch.column('x'), 2), 0)

    filter_op = CythonFilterOperator(source=None, predicate=filter_even)
    morsel_op = MorselDrivenOperator(filter_op, num_workers=4)

    large_batch = pa.RecordBatch.from_pydict({'x': list(range(10000))})
    result = morsel_op.process_batch(large_batch)

    assert result.num_rows == 5000  # Half are even
    assert all(x % 2 == 0 for x in result.column('x').to_pylist())
    print("   ✅ PASSED")
    test_results.append(("test_large_batch_with_filter", True))
except Exception as e:
    print(f"   ❌ FAILED: {e}")
    test_results.append(("test_large_batch_with_filter", False))
finally:
    cleanup_task_slot_manager()

# Test 3: Large batch with complex transform
print("\n[3/6] test_large_batch_with_complex_transform")
try:
    def complex_transform(batch):
        return batch.append_column(
            'y', pc.add(pc.multiply(batch.column('x'), 2), 100)
        ).append_column(
            'z', pc.power(batch.column('x'), 2)
        )

    map_op = CythonMapOperator(source=None, map_func=complex_transform)
    morsel_op = MorselDrivenOperator(map_op, num_workers=4)

    large_batch = pa.RecordBatch.from_pydict({'x': list(range(20000))})
    result = morsel_op.process_batch(large_batch)

    assert result.num_rows == 20000
    assert 'y' in result.schema.names
    assert 'z' in result.schema.names
    assert result.column('y').to_pylist()[0] == 100
    assert result.column('y').to_pylist()[10] == 120
    assert result.column('z').to_pylist()[10] == 100
    print("   ✅ PASSED")
    test_results.append(("test_large_batch_with_complex_transform", True))
except Exception as e:
    print(f"   ❌ FAILED: {e}")
    test_results.append(("test_large_batch_with_complex_transform", False))
finally:
    cleanup_task_slot_manager()

# Test 4: Stats after processing
print("\n[4/6] test_stats_after_processing")
try:
    def double_x(batch):
        return batch.set_column(0, 'x', pc.multiply(batch.column('x'), 2))

    map_op = CythonMapOperator(source=None, map_func=double_x)
    morsel_op = MorselDrivenOperator(map_op, num_workers=4)

    large_batch = pa.RecordBatch.from_pydict({'x': list(range(50000))})
    result = morsel_op.process_batch(large_batch)

    stats = morsel_op.get_stats()
    assert 'num_workers' in stats
    assert stats['num_workers'] == 4
    print("   ✅ PASSED")
    test_results.append(("test_stats_after_processing", True))
except Exception as e:
    print(f"   ❌ FAILED: {e}")
    test_results.append(("test_stats_after_processing", False))
finally:
    cleanup_task_slot_manager()

# Test 5: Parallel matches sequential (map)
print("\n[5/6] test_parallel_matches_sequential_map")
try:
    def complex_func(batch):
        return batch.set_column(
            0, 'x',
            pc.add(pc.multiply(batch.column('x'), 3), 7)
        )

    map_op = CythonMapOperator(source=None, map_func=complex_func)
    morsel_op = MorselDrivenOperator(map_op, num_workers=4)

    batch = pa.RecordBatch.from_pydict({'x': list(range(15000))})

    # Sequential
    sequential_result = map_op.process_batch(batch)

    # Parallel
    parallel_result = morsel_op.process_batch(batch)

    # Compare
    assert sequential_result.num_rows == parallel_result.num_rows
    assert sequential_result.column('x').to_pylist() == parallel_result.column('x').to_pylist()
    print("   ✅ PASSED")
    test_results.append(("test_parallel_matches_sequential_map", True))
except Exception as e:
    print(f"   ❌ FAILED: {e}")
    test_results.append(("test_parallel_matches_sequential_map", False))
finally:
    cleanup_task_slot_manager()

# Test 6: Parallel matches sequential (filter)
print("\n[6/6] test_parallel_matches_sequential_filter")
try:
    def filter_divisible_by_7(batch):
        return pc.equal(pc.modulo(batch.column('x'), 7), 0)

    filter_op = CythonFilterOperator(source=None, predicate=filter_divisible_by_7)
    morsel_op = MorselDrivenOperator(filter_op, num_workers=4)

    batch = pa.RecordBatch.from_pydict({'x': list(range(20000))})

    # Sequential
    sequential_result = filter_op.process_batch(batch)

    # Parallel
    parallel_result = morsel_op.process_batch(batch)

    # Compare
    assert sequential_result.num_rows == parallel_result.num_rows
    assert sequential_result.column('x').to_pylist() == parallel_result.column('x').to_pylist()
    print("   ✅ PASSED")
    test_results.append(("test_parallel_matches_sequential_filter", True))
except Exception as e:
    print(f"   ❌ FAILED: {e}")
    test_results.append(("test_parallel_matches_sequential_filter", False))
finally:
    cleanup_task_slot_manager()

# Summary
print("\n" + "="*70)
print("Test Summary")
print("="*70)
passed = sum(1 for _, result in test_results if result)
total = len(test_results)

for name, result in test_results:
    status = "✅ PASS" if result else "❌ FAIL"
    print(f"{status} - {name}")

print("="*70)
print(f"Total: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
print("="*70)

sys.exit(0 if passed == total else 1)
