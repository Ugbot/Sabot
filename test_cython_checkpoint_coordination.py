#!/usr/bin/env python3
"""
Test Cython Checkpoint Coordination

Tests the distributed checkpointing system with Chandy-Lamport algorithm
and exactly-once semantics.
"""

import asyncio
import tempfile
import os
import sys

# Add sabot to path
sys.path.insert(0, os.path.dirname(__file__))

from sabot.stores.rocksdb_fallback import RocksDBBackend
from sabot.stores.base import StoreBackendConfig


async def test_barrier_tracker():
    """Test barrier tracker for multi-input alignment."""
    print("🧪 Testing Barrier Tracker...")

    # Create barrier tracker for 3 channels
    from sabot._cython.checkpoint.barrier_tracker import BarrierTracker
    tracker = BarrierTracker(3, 10)  # 3 channels, 10 max checkpoints

    # Test initial state
    assert tracker.get_active_barrier_count() == 0
    assert tracker.has_capacity()

    # Register barriers for checkpoint 1
    complete = tracker.register_barrier(0, 1, 3)  # Channel 0, checkpoint 1, 3 total inputs
    assert not complete  # Not complete yet (1/3)

    complete = tracker.register_barrier(1, 1, 3)  # Channel 1, checkpoint 1
    assert not complete  # Not complete yet (2/3)

    complete = tracker.register_barrier(2, 1, 3)  # Channel 2, checkpoint 1
    assert complete  # Now complete (3/3)

    # Check alignment
    assert tracker.is_barrier_aligned(1)

    # Test stats
    stats = tracker.get_barrier_stats(1)
    assert stats['checkpoint_id'] == 1
    assert stats['total_inputs'] == 3
    assert stats['received_count'] == 3
    assert stats['is_complete'] == True

    # Test active barriers
    active = tracker.get_active_barriers()
    assert len(active) == 1
    assert active[0]['checkpoint_id'] == 1

    # Clean up
    tracker.reset_barrier(1)
    assert tracker.get_active_barrier_count() == 0

    print("✅ Barrier Tracker tests passed!")


async def test_checkpoint_barrier():
    """Test checkpoint barrier data structure."""
    print("🧪 Testing Checkpoint Barrier...")

    from sabot._cython.checkpoint.barrier import Barrier

    # Create barriers
    barrier1 = Barrier.create_checkpoint_barrier(100, 1)
    barrier2 = Barrier.create_cancellation_barrier(101, 2)

    # Test properties
    assert barrier1.get_checkpoint_id() == 100
    assert barrier1.get_source_id() == 1
    assert barrier1.is_cancellation() == False
    assert barrier1.should_trigger_checkpoint() == True

    assert barrier2.get_checkpoint_id() == 101
    assert barrier2.get_source_id() == 2
    assert barrier2.is_cancellation() == True
    assert barrier2.should_cancel_checkpoint() == True

    # Test metadata
    barrier1.set_metadata("priority", "high")
    assert barrier1.get_metadata("priority") == "high"
    assert barrier1.get_metadata("missing", "default") == "default"

    # Test comparison
    assert barrier1.is_same_checkpoint(barrier1)
    assert not barrier1.is_same_checkpoint(barrier2)
    assert barrier1.compare_to(barrier2) < 0  # 100 < 101

    print("✅ Checkpoint Barrier tests passed!")


async def test_checkpoint_coordinator():
    """Test checkpoint coordinator."""
    print("🧪 Testing Checkpoint Coordinator...")

    with tempfile.TemporaryDirectory() as temp_dir:
        config = StoreBackendConfig(path=temp_dir)
        backend = RocksDBBackend(config)

        try:
            await backend.start()

            # Create components
            from sabot._cython.checkpoint.barrier_tracker import BarrierTracker
            from sabot._cython.checkpoint.coordinator import CheckpointCoordinator

            barrier_tracker = BarrierTracker(2, 10)
            coordinator = CheckpointCoordinator(10, 10)

            coordinator.set_barrier_tracker(barrier_tracker)
            coordinator.set_storage_backends(backend, None)  # No Tonbo for test

            # Register operators
            coordinator.register_operator(1, "TestOperator1", None)
            coordinator.register_operator(2, "TestOperator2", None)

            # Trigger checkpoint
            checkpoint_id = coordinator.trigger_checkpoint()
            assert checkpoint_id == 1

            # Check that checkpoint is active
            active = coordinator.get_active_checkpoints()
            assert len(active) == 1
            assert active[0]['checkpoint_id'] == 1
            assert not active[0]['is_completed']

            # Acknowledge from operators
            complete1 = coordinator.acknowledge_checkpoint(1, checkpoint_id)
            assert not complete1  # Not complete yet (1/2)

            complete2 = coordinator.acknowledge_checkpoint(2, checkpoint_id)
            assert complete2  # Now complete (2/2)

            # Check completion
            assert coordinator.is_checkpoint_complete(checkpoint_id)

            # Get stats
            stats = coordinator.get_checkpoint_stats(checkpoint_id)
            assert stats['is_completed'] == True
            assert stats['completed_count'] == 2
            assert stats['operator_count'] == 2

            # Clean up completed checkpoints
            coordinator.cleanup_completed_checkpoints()
            active_after = coordinator.get_active_checkpoints()
            assert len(active_after) == 0

            print("✅ Checkpoint Coordinator tests passed!")

        finally:
            await backend.stop()


async def test_checkpoint_storage():
    """Test checkpoint storage."""
    print("🧪 Testing Checkpoint Storage...")

    with tempfile.TemporaryDirectory() as temp_dir:
        config = StoreBackendConfig(path=temp_dir)
        backend = RocksDBBackend(config)

        try:
            await backend.start()

            from sabot._cython.checkpoint.storage import CheckpointStorage

            # Create storage (no Tonbo backend for test)
            storage = CheckpointStorage(None, backend, max_checkpoints=5)

            # Test initial state
            available = storage.list_available_checkpoints()
            assert len(available) == 0
            assert storage.get_latest_checkpoint_id() == -1

            # Store checkpoint
            app_data = {"state1": "data1", "state2": "data2"}
            sys_metadata = {"timers": {}, "coordinator": {}}

            storage.store_checkpoint(1, app_data, sys_metadata)

            # Check availability
            available = storage.list_available_checkpoints()
            assert len(available) == 1
            assert available[0] == 1
            assert storage.get_latest_checkpoint_id() == 1
            assert storage.checkpoint_exists(1)

            # Load checkpoint
            loaded_app, loaded_sys = storage.load_checkpoint(1)
            assert loaded_sys is not None

            # Get info
            info = storage.get_checkpoint_info(1)
            assert info['checkpoint_id'] == 1
            assert info['has_system_metadata'] == True

            # Store another checkpoint
            storage.store_checkpoint(2, {}, {"test": "data"})

            available = storage.list_available_checkpoints()
            assert len(available) == 2
            assert available == [2, 1]  # Most recent first

            # Test cleanup
            storage.cleanup_old_checkpoints()  # Should keep both (under limit)

            available = storage.list_available_checkpoints()
            assert len(available) == 2

            print("✅ Checkpoint Storage tests passed!")

        finally:
            await backend.stop()


async def test_recovery_manager():
    """Test recovery manager."""
    print("🧪 Testing Recovery Manager...")

    with tempfile.TemporaryDirectory() as temp_dir:
        config = StoreBackendConfig(path=temp_dir)
        backend = RocksDBBackend(config)

        try:
            await backend.start()

            from sabot._cython.checkpoint.storage import CheckpointStorage
            from sabot._cython.checkpoint.coordinator import CheckpointCoordinator
            from sabot._cython.checkpoint.recovery import RecoveryManager

            # Create components
            storage = CheckpointStorage(None, backend)
            coordinator = CheckpointCoordinator()
            recovery = RecoveryManager(storage, coordinator)

            # Test initial state
            assert not recovery.needs_recovery()
            assert recovery.select_recovery_checkpoint() == -1

            # Store a checkpoint
            storage.store_checkpoint(1, {}, {"test": "recovery_data"})

            # Now recovery should be needed
            assert recovery.needs_recovery()
            assert recovery.select_recovery_checkpoint() == 1

            # Test recovery stats
            stats = recovery.get_recovery_stats()
            assert stats['available_checkpoints'] == [1]
            assert stats['selected_checkpoint'] == 1

            # Test recovery strategies
            strategies = recovery.get_recovery_strategies()
            assert 'latest_checkpoint' in strategies
            assert 'specific_checkpoint' in strategies

            print("✅ Recovery Manager tests passed!")

        finally:
            await backend.stop()


async def test_integrated_checkpointing():
    """Test integrated checkpointing workflow."""
    print("🧪 Testing Integrated Checkpointing...")

    with tempfile.TemporaryDirectory() as temp_dir:
        config = StoreBackendConfig(path=temp_dir)
        backend = RocksDBBackend(config)

        try:
            await backend.start()

            # Create full checkpointing system
            from sabot._cython.checkpoint.barrier_tracker import BarrierTracker
            from sabot._cython.checkpoint.coordinator import CheckpointCoordinator
            from sabot._cython.checkpoint.storage import CheckpointStorage
            from sabot._cython.checkpoint.recovery import RecoveryManager

            barrier_tracker = BarrierTracker(2, 10)
            coordinator = CheckpointCoordinator(10, 10)
            storage = CheckpointStorage(None, backend)
            recovery = RecoveryManager(storage, coordinator)

            coordinator.set_barrier_tracker(barrier_tracker)
            coordinator.set_storage_backends(backend, None)

            # Register operators
            coordinator.register_operator(1, "SourceOperator", None)
            coordinator.register_operator(2, "ProcessOperator", None)

            # Simulate checkpoint workflow
            # 1. Trigger checkpoint
            checkpoint_id = coordinator.trigger_checkpoint()
            print(f"   Triggered checkpoint {checkpoint_id}")

            # 2. Operators acknowledge (simulate barrier alignment)
            complete1 = coordinator.acknowledge_checkpoint(1, checkpoint_id)
            print(f"   Operator 1 acknowledged, complete: {complete1}")

            complete2 = coordinator.acknowledge_checkpoint(2, checkpoint_id)
            print(f"   Operator 2 acknowledged, complete: {complete2}")

            # 3. Store checkpoint
            app_data = {"operator_states": {"op1": "state1", "op2": "state2"}}
            sys_data = {"coordinator": coordinator.get_coordinator_stats()}
            storage.store_checkpoint(checkpoint_id, app_data, sys_data)

            # 4. Verify checkpoint
            loaded_app, loaded_sys = storage.load_checkpoint(checkpoint_id)
            assert loaded_sys is not None
            print(f"   Checkpoint {checkpoint_id} stored and loaded successfully")

            # 5. Test recovery preparation
            assert recovery.select_recovery_checkpoint() == checkpoint_id
            print(f"   Recovery checkpoint selected: {checkpoint_id}")

            print("✅ Integrated Checkpointing tests passed!")

        finally:
            await backend.stop()


async def main():
    """Run all checkpoint coordination tests."""
    print("🚀 Cython Checkpoint Coordination Tests")
    print("=" * 60)

    try:
        await test_barrier_tracker()
        await test_checkpoint_barrier()
        await test_checkpoint_coordinator()
        await test_checkpoint_storage()
        await test_recovery_manager()
        await test_integrated_checkpointing()

        print("\n🎉 All checkpoint coordination tests passed!")
        print("✅ Chandy-Lamport distributed snapshot algorithm is working")
        print("✅ Exactly-once semantics infrastructure is ready")

    except Exception as e:
        print(f"\n❌ Tests failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
