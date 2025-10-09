#!/usr/bin/env python3
"""
Simple Task Slot Manager and Morsel Executor Tests

Tests the newly built C++ modules directly without full Sabot imports.
"""

import sys
import time


def test_morsel_executor():
    """Test MorselExecutor C++ module."""
    print("\n=== Test 1: MorselExecutor ===")

    try:
        from sabot._c.morsel_executor import MorselExecutor
        import pyarrow as pa

        # Create executor
        executor = MorselExecutor(num_threads=4)
        print(f"✓ Created MorselExecutor with {executor.num_threads} threads")

        # Create test batch
        batch = pa.RecordBatch.from_pydict({
            'x': list(range(50000)),
            'y': list(range(50000, 100000))
        })
        print(f"✓ Created test batch: {batch.num_rows:,} rows")

        # Define processing function
        def double_x(b):
            import pyarrow.compute as pc
            return b.set_column(0, 'x', pc.multiply(b.column('x'), 2))

        # Execute with morsels
        start = time.perf_counter()
        results = executor.execute_local(batch, 64*1024, double_x)
        elapsed = time.perf_counter() - start

        print(f"✓ Processed {len(results)} result batches in {elapsed*1000:.2f}ms")

        # Get statistics
        stats = executor.get_stats()
        print(f"  Morsels created: {stats['total_morsels_created']}")
        print(f"  Morsels processed: {stats['total_morsels_processed']}")
        print(f"  Batches executed: {stats['total_batches_executed']}")

        # Shutdown
        executor.shutdown()
        print("✓ Executor shutdown successful")

        print("✅ Test 1 PASSED\n")
        return True

    except Exception as e:
        print(f"❌ Test 1 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_task_slot_manager():
    """Test TaskSlotManager C++ module."""
    print("\n=== Test 2: TaskSlotManager ===")

    try:
        from sabot._c.task_slot_manager import TaskSlotManager, Morsel, MorselSource
        import pyarrow as pa
        import pyarrow.compute as pc

        # Create manager
        manager = TaskSlotManager(num_slots=4)
        print(f"✓ Created TaskSlotManager with {manager.get_num_slots()} slots")
        print(f"  Available slots: {manager.get_available_slots()}")

        # Create test batch
        batch = pa.RecordBatch.from_pydict({
            'value': list(range(30000))
        })
        print(f"✓ Created test batch: {batch.num_rows:,} rows")

        # Create morsels manually
        morsel_size = 10000
        morsels = []
        for start in range(0, batch.num_rows, morsel_size):
            length = min(morsel_size, batch.num_rows - start)
            morsel = Morsel()
            morsel.batch = batch
            morsel.start_row = start
            morsel.num_rows = length
            morsel.source = MorselSource.LOCAL
            morsel.worker_id = -1
            morsels.append(morsel)

        print(f"✓ Created {len(morsels)} morsels")

        # Define processing function
        def increment(b):
            return b.set_column(0, 'value', pc.add(b.column('value'), 1))

        # Execute morsels
        start = time.perf_counter()
        results = manager.execute_morsels(morsels, increment)
        elapsed = time.perf_counter() - start

        print(f"✓ Processed {len(results)} results in {elapsed*1000:.2f}ms")
        print(f"  Queue depth: {manager.get_queue_depth()}")

        # Get slot statistics
        slot_stats = manager.get_slot_stats()
        print(f"  Total slots: {len(slot_stats)}")
        for stat in slot_stats[:2]:  # Show first 2 slots
            print(f"    Slot {stat['slot_id']}: {stat['morsels_processed']} morsels, "
                  f"{stat['total_processing_time_us']/1000:.2f}ms")

        # Test elastic scaling - add slots
        manager.add_slots(2)
        print(f"✓ Added 2 slots, now have {manager.get_num_slots()} total")

        # Test elastic scaling - remove slots
        manager.remove_slots(1)
        print(f"✓ Removed 1 slot, now have {manager.get_num_slots()} total")

        # Shutdown
        manager.shutdown()
        print("✓ Manager shutdown successful")

        print("✅ Test 2 PASSED\n")
        return True

    except Exception as e:
        print(f"❌ Test 2 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_agent_context():
    """Test AgentContext integration."""
    print("\n=== Test 3: AgentContext ===")

    try:
        from sabot.cluster.agent_context import AgentContext

        # Get singleton instance
        context = AgentContext.get_instance()
        print("✓ Got AgentContext singleton instance")

        # Initialize with task slots
        context.initialize(
            agent_id="test-agent-001",
            num_slots=4,
            host="127.0.0.1",
            port=8816
        )
        print(f"✓ Initialized agent: {context.agent_id}")
        print(f"  Task slots: {context.task_slot_manager.get_num_slots()}")
        print(f"  Shuffle transport: {context.shuffle_transport is not None}")

        # Verify shared infrastructure
        assert context.task_slot_manager is not None
        assert context.shuffle_transport is not None
        print("✓ Shared infrastructure verified")

        # Get slot statistics
        slot_stats = context.task_slot_manager.get_slot_stats()
        print(f"✓ Can access slot stats: {len(slot_stats)} slots")

        # Test getting same instance
        context2 = AgentContext.get_instance()
        assert context is context2
        print("✓ Singleton pattern verified")

        # Shutdown
        if context.task_slot_manager:
            context.task_slot_manager.shutdown()
        if context.shuffle_transport:
            context.shuffle_transport.stop()
        print("✓ Agent shutdown successful")

        print("✅ Test 3 PASSED\n")
        return True

    except Exception as e:
        print(f"❌ Test 3 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("="*70)
    print("Task Slot System - Direct C++ Module Tests")
    print("="*70)

    results = []

    # Test 1: MorselExecutor
    results.append(("MorselExecutor", test_morsel_executor()))

    # Test 2: TaskSlotManager
    results.append(("TaskSlotManager", test_task_slot_manager()))

    # Test 3: AgentContext
    results.append(("AgentContext", test_agent_context()))

    # Summary
    print("="*70)
    print("Test Summary:")
    print("="*70)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {name}")

    print("="*70)
    print(f"Total: {passed}/{total} tests passed")
    print("="*70)

    return 0 if passed == total else 1


if __name__ == '__main__':
    sys.exit(main())
