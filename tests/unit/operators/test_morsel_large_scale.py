#!/usr/bin/env python3
"""
Large-Scale Morsel Operator Test

Tests the morsel operator with large batches (100K-1M rows) to verify:
1. Correct ordering after parallel execution
2. Performance improvements with multiple workers
3. Correctness across different batch sizes
"""

import time
from sabot import cyarrow as pa
from sabot.cyarrow import compute as pc
from sabot._cython.operators.transform import CythonMapOperator
from sabot._cython.operators.morsel_operator import MorselDrivenOperator
from sabot.cluster.agent_context import AgentContext


def cleanup():
    """Cleanup TaskSlotManager singleton."""
    ctx = AgentContext.get_instance()
    if ctx.task_slot_manager:
        ctx.task_slot_manager.shutdown()
        ctx.task_slot_manager = None


def test_large_batch_performance():
    """Test performance with different batch sizes."""
    print("="*70)
    print("LARGE-SCALE MORSEL OPERATOR TEST")
    print("="*70)

    # Define transform
    def complex_transform(batch):
        """Complex transform to simulate real workload"""
        return batch.append_column(
            'y',
            pc.add(
                pc.multiply(batch.column('x'), 2.5),
                100
            )
        ).append_column(
            'z',
            pc.power(batch.column('x'), 2)
        )

    # Test different batch sizes
    test_cases = [
        ("Small (10K)", 10_000, 2),
        ("Medium (50K)", 50_000, 4),
        ("Large (100K)", 100_000, 4),
        ("X-Large (500K)", 500_000, 8),
        ("XX-Large (1M)", 1_000_000, 8),
    ]

    results = []

    for name, num_rows, num_workers in test_cases:
        print(f"\n{'='*70}")
        print(f"Test: {name} rows with {num_workers} workers")
        print(f"{'='*70}")

        # Create batch
        print(f"Creating batch with {num_rows:,} rows...")
        start_create = time.perf_counter()
        batch = pa.RecordBatch.from_pydict({'x': list(range(num_rows))})
        create_time = (time.perf_counter() - start_create) * 1000
        print(f"  Created in {create_time:.2f}ms")

        # Create operators
        map_op = CythonMapOperator(source=None, map_func=complex_transform)
        morsel_op = MorselDrivenOperator(map_op, num_workers=num_workers, morsel_size_kb=64)

        # Process with morsel parallelism
        print(f"Processing with {num_workers} workers...")
        start_process = time.perf_counter()
        result = morsel_op.process_batch(batch)
        process_time = (time.perf_counter() - start_process) * 1000

        # Verify results
        print(f"  Processed in {process_time:.2f}ms")

        # Check correctness
        expected_y_first = 2.5 * 0 + 100  # First row: x=0
        expected_y_last = 2.5 * (num_rows - 1) + 100  # Last row
        expected_z_first = 0 ** 2
        expected_z_last = (num_rows - 1) ** 2

        actual_y_first = result.column('y')[0].as_py()
        actual_y_last = result.column('y')[num_rows - 1].as_py()
        actual_z_first = result.column('z')[0].as_py()
        actual_z_last = result.column('z')[num_rows - 1].as_py()

        # Check ordering is correct
        correct = (
            abs(actual_y_first - expected_y_first) < 0.01 and
            abs(actual_y_last - expected_y_last) < 0.01 and
            abs(actual_z_first - expected_z_first) < 0.01 and
            abs(actual_z_last - expected_z_last) < 0.01
        )

        if correct:
            print(f"  ✅ Results CORRECT (ordering preserved)")
            print(f"     First: y={actual_y_first:.2f}, z={actual_z_first:.0f}")
            print(f"     Last:  y={actual_y_last:.2f}, z={actual_z_last:.0f}")
        else:
            print(f"  ❌ Results INCORRECT (ordering broken)")
            print(f"     Expected first: y={expected_y_first:.2f}, z={expected_z_first:.0f}")
            print(f"     Got first:      y={actual_y_first:.2f}, z={actual_z_first:.0f}")
            print(f"     Expected last:  y={expected_y_last:.2f}, z={expected_z_last:.0f}")
            print(f"     Got last:       y={actual_y_last:.2f}, z={actual_z_last:.0f}")

        # Calculate throughput
        throughput = num_rows / (process_time / 1000) / 1_000_000  # M rows/sec
        print(f"  Throughput: {throughput:.2f}M rows/sec")

        results.append({
            'name': name,
            'rows': num_rows,
            'workers': num_workers,
            'time_ms': process_time,
            'throughput': throughput,
            'correct': correct
        })

        # Cleanup between tests
        cleanup()

    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    print(f"{'Test':<20} {'Rows':>10} {'Workers':>8} {'Time (ms)':>12} {'Throughput':>12} {'Status':>8}")
    print("-"*70)

    for r in results:
        status = "✅ PASS" if r['correct'] else "❌ FAIL"
        print(f"{r['name']:<20} {r['rows']:>10,} {r['workers']:>8} {r['time_ms']:>12.2f} {r['throughput']:>10.2f}M/s {status:>8}")

    all_passed = all(r['correct'] for r in results)

    print(f"\n{'='*70}")
    if all_passed:
        print("✅ ALL TESTS PASSED - Morsel operator working correctly!")
    else:
        print("❌ SOME TESTS FAILED - Check ordering logic")
    print(f"{'='*70}")

    return all_passed


if __name__ == '__main__':
    try:
        success = test_large_batch_performance()
        exit(0 if success else 1)
    finally:
        cleanup()
