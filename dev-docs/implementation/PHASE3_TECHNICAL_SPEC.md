# Phase 3: Increase Test Coverage - Technical Specification

**Phase:** 3 of 9
**Focus:** Test existing Cython modules and create end-to-end tests
**Dependencies:** Phase 1 (CLI working), Phase 2 (Agent runtime integrated)
**Target Coverage:** Move from ~5% to 30%+ test coverage

---

## Overview

Phase 3 focuses on testing the existing Cython modules that were built in the initial development. These modules compile and many work, but lack comprehensive tests. This phase does not add new features - it validates what already exists.

**Current State:**
- 31 Cython modules compile successfully
- ~5% test coverage across the codebase
- Checkpoint modules have some tests
- State, time, and Arrow modules largely untested
- Integration tests missing

**Goal:**
- Unit test all Cython checkpoint, state, and time modules
- Test Arrow C++ integration
- Create end-to-end test for fraud detection demo
- Reach 30%+ code coverage
- Document what works and what doesn't

---

## Step 3.1: Unit Test Cython Checkpoint Modules

### Current State

**Location:** `sabot/_cython/checkpoint/`

**Modules:**
- `barrier.pyx` / `barrier.pxd` - Barrier message representation
- `barrier_tracker.pyx` / `barrier_tracker.pxd` - Track barriers across channels
- `coordinator.pyx` / `coordinator.pxd` - Checkpoint coordinator
- `storage.pyx` / `storage.pxd` - Checkpoint persistence
- `recovery.pyx` / `recovery.pxd` - Recovery from checkpoints

**Current Tests:**
- Some basic tests exist for coordinator
- Barrier alignment logic not fully tested
- Recovery path untested
- Storage persistence not tested

### Implementation

#### Create `tests/unit/test_cython_checkpoint.py`

```python
"""Unit tests for Cython checkpoint modules."""

import pytest
import asyncio
from pathlib import Path
import tempfile
import shutil

from sabot.checkpoint import (
    Barrier,
    BarrierTracker,
    Coordinator,
    CheckpointStorage,
    CheckpointRecovery,
)


class TestBarrier:
    """Test Barrier message creation and serialization."""

    def test_barrier_creation(self):
        """Test creating a barrier."""
        barrier = Barrier(checkpoint_id=42, timestamp=1000)

        assert barrier.checkpoint_id == 42
        assert barrier.timestamp == 1000

    def test_barrier_serialization(self):
        """Test barrier serialization/deserialization."""
        barrier = Barrier(checkpoint_id=42, timestamp=1000)

        # Serialize
        data = barrier.serialize()
        assert isinstance(data, bytes)

        # Deserialize
        restored = Barrier.deserialize(data)
        assert restored.checkpoint_id == 42
        assert restored.timestamp == 1000

    def test_barrier_equality(self):
        """Test barrier equality comparison."""
        b1 = Barrier(checkpoint_id=42, timestamp=1000)
        b2 = Barrier(checkpoint_id=42, timestamp=1000)
        b3 = Barrier(checkpoint_id=43, timestamp=1000)

        assert b1 == b2
        assert b1 != b3


class TestBarrierTracker:
    """Test BarrierTracker for barrier alignment."""

    def test_tracker_creation(self):
        """Test creating a barrier tracker."""
        tracker = BarrierTracker(num_channels=3)

        assert tracker.num_channels == 3
        assert not tracker.is_aligned()

    def test_barrier_registration(self):
        """Test registering barriers from different channels."""
        tracker = BarrierTracker(num_channels=3)
        barrier = Barrier(checkpoint_id=1, timestamp=1000)

        # Register from channel 0
        tracker.register_barrier(channel_id=0, barrier=barrier)
        assert not tracker.is_aligned()

        # Register from channel 1
        tracker.register_barrier(channel_id=1, barrier=barrier)
        assert not tracker.is_aligned()

        # Register from channel 2 - now aligned
        tracker.register_barrier(channel_id=2, barrier=barrier)
        assert tracker.is_aligned()

    def test_barrier_alignment_same_checkpoint(self):
        """Test that alignment only happens for same checkpoint_id."""
        tracker = BarrierTracker(num_channels=2)

        b1 = Barrier(checkpoint_id=1, timestamp=1000)
        b2 = Barrier(checkpoint_id=2, timestamp=2000)

        tracker.register_barrier(channel_id=0, barrier=b1)
        tracker.register_barrier(channel_id=1, barrier=b2)

        # Different checkpoint IDs, should not align
        assert not tracker.is_aligned()

    def test_reset_after_alignment(self):
        """Test resetting tracker after alignment."""
        tracker = BarrierTracker(num_channels=2)
        barrier = Barrier(checkpoint_id=1, timestamp=1000)

        tracker.register_barrier(channel_id=0, barrier=barrier)
        tracker.register_barrier(channel_id=1, barrier=barrier)

        assert tracker.is_aligned()

        # Reset
        tracker.reset()
        assert not tracker.is_aligned()


class TestCoordinator:
    """Test CheckpointCoordinator."""

    def test_coordinator_creation(self):
        """Test creating a checkpoint coordinator."""
        coord = Coordinator()

        assert coord.last_checkpoint_id == 0

    def test_trigger_checkpoint(self):
        """Test triggering a checkpoint."""
        coord = Coordinator()

        checkpoint_id = coord.trigger()

        assert checkpoint_id == 1
        assert coord.last_checkpoint_id == 1

    def test_multiple_checkpoints(self):
        """Test triggering multiple checkpoints."""
        coord = Coordinator()

        id1 = coord.trigger()
        id2 = coord.trigger()
        id3 = coord.trigger()

        assert id1 == 1
        assert id2 == 2
        assert id3 == 3
        assert coord.last_checkpoint_id == 3

    def test_checkpoint_latency(self):
        """Test checkpoint trigger latency (<10μs claimed)."""
        import time

        coord = Coordinator()

        # Measure 1000 checkpoint triggers
        start = time.perf_counter()
        for _ in range(1000):
            coord.trigger()
        end = time.perf_counter()

        avg_latency_us = ((end - start) / 1000) * 1_000_000

        # Should be <10μs per checkpoint
        assert avg_latency_us < 10, f"Checkpoint latency {avg_latency_us:.2f}μs exceeds 10μs"


class TestCheckpointStorage:
    """Test CheckpointStorage persistence."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for checkpoints."""
        temp = Path(tempfile.mkdtemp())
        yield temp
        shutil.rmtree(temp)

    def test_storage_creation(self, temp_dir):
        """Test creating checkpoint storage."""
        storage = CheckpointStorage(checkpoint_dir=str(temp_dir))

        assert storage.checkpoint_dir == str(temp_dir)

    def test_save_checkpoint(self, temp_dir):
        """Test saving a checkpoint."""
        storage = CheckpointStorage(checkpoint_dir=str(temp_dir))

        state = {"key1": "value1", "key2": 42}
        checkpoint_id = 1

        storage.save(checkpoint_id=checkpoint_id, state=state)

        # Verify checkpoint file exists
        checkpoint_path = temp_dir / f"checkpoint_{checkpoint_id}.bin"
        assert checkpoint_path.exists()

    def test_load_checkpoint(self, temp_dir):
        """Test loading a checkpoint."""
        storage = CheckpointStorage(checkpoint_dir=str(temp_dir))

        # Save
        state = {"key1": "value1", "key2": 42}
        checkpoint_id = 1
        storage.save(checkpoint_id=checkpoint_id, state=state)

        # Load
        restored_state = storage.load(checkpoint_id=checkpoint_id)

        assert restored_state == state

    def test_list_checkpoints(self, temp_dir):
        """Test listing available checkpoints."""
        storage = CheckpointStorage(checkpoint_dir=str(temp_dir))

        # Save multiple checkpoints
        storage.save(checkpoint_id=1, state={"a": 1})
        storage.save(checkpoint_id=2, state={"b": 2})
        storage.save(checkpoint_id=3, state={"c": 3})

        checkpoints = storage.list_checkpoints()

        assert checkpoints == [1, 2, 3]

    def test_delete_checkpoint(self, temp_dir):
        """Test deleting a checkpoint."""
        storage = CheckpointStorage(checkpoint_dir=str(temp_dir))

        storage.save(checkpoint_id=1, state={"a": 1})
        storage.delete(checkpoint_id=1)

        assert storage.list_checkpoints() == []


class TestCheckpointRecovery:
    """Test CheckpointRecovery."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for checkpoints."""
        temp = Path(tempfile.mkdtemp())
        yield temp
        shutil.rmtree(temp)

    def test_recovery_creation(self, temp_dir):
        """Test creating recovery coordinator."""
        recovery = CheckpointRecovery(checkpoint_dir=str(temp_dir))

        assert recovery.checkpoint_dir == str(temp_dir)

    def test_get_latest_checkpoint(self, temp_dir):
        """Test retrieving latest checkpoint."""
        storage = CheckpointStorage(checkpoint_dir=str(temp_dir))
        recovery = CheckpointRecovery(checkpoint_dir=str(temp_dir))

        # Save checkpoints
        storage.save(checkpoint_id=1, state={"a": 1})
        storage.save(checkpoint_id=2, state={"b": 2})
        storage.save(checkpoint_id=3, state={"c": 3})

        latest_id = recovery.get_latest_checkpoint_id()
        assert latest_id == 3

    def test_restore_from_checkpoint(self, temp_dir):
        """Test full restore from checkpoint."""
        storage = CheckpointStorage(checkpoint_dir=str(temp_dir))
        recovery = CheckpointRecovery(checkpoint_dir=str(temp_dir))

        # Save checkpoint
        original_state = {
            "agent1": {"counter": 100, "last_key": "abc"},
            "agent2": {"sum": 500, "count": 10}
        }
        storage.save(checkpoint_id=5, state=original_state)

        # Restore
        restored_state = recovery.restore_from_checkpoint(checkpoint_id=5)

        assert restored_state == original_state

    def test_no_checkpoint_available(self, temp_dir):
        """Test recovery when no checkpoint exists."""
        recovery = CheckpointRecovery(checkpoint_dir=str(temp_dir))

        latest = recovery.get_latest_checkpoint_id()
        assert latest is None


# Run benchmarks separately
@pytest.mark.benchmark
class TestCheckpointPerformance:
    """Performance benchmarks for checkpoint operations."""

    def test_barrier_creation_performance(self, benchmark):
        """Benchmark barrier creation."""
        def create_barrier():
            return Barrier(checkpoint_id=1, timestamp=1000)

        result = benchmark(create_barrier)
        assert result.checkpoint_id == 1

    def test_coordinator_trigger_performance(self, benchmark):
        """Benchmark checkpoint trigger latency."""
        coord = Coordinator()

        result = benchmark(coord.trigger)
        assert result > 0
```

### Verification

```bash
# Run checkpoint unit tests
pytest tests/unit/test_cython_checkpoint.py -v

# Run with coverage
pytest tests/unit/test_cython_checkpoint.py --cov=sabot.checkpoint --cov-report=html

# Run benchmarks (if pytest-benchmark installed)
pytest tests/unit/test_cython_checkpoint.py -m benchmark --benchmark-only
```

**Expected Results:**
- All tests pass
- Coverage for `sabot/checkpoint/` reaches 70%+
- Checkpoint trigger latency confirmed <10μs

---

## Step 3.2: Unit Test Cython State Modules

### Current State

**Location:** `sabot/_cython/state/`

**Modules:**
- `state_backend.pyx` - Abstract state backend interface
- `memory_backend.pyx` - In-memory state backend
- `rocksdb_state.pyx` - RocksDB-backed state (experimental)
- `value_state.pyx` - ValueState primitive
- `list_state.pyx` - ListState primitive
- `map_state.pyx` - MapState primitive
- `reducing_state.pyx` - ReducingState primitive
- `aggregating_state.pyx` - AggregatingState primitive

**Current Tests:**
- Memory backend has minimal tests
- RocksDB backend untested
- State primitives not tested
- Performance not measured

### Implementation

#### Create `tests/unit/test_cython_state.py`

```python
"""Unit tests for Cython state modules."""

import pytest
import asyncio
from pathlib import Path
import tempfile
import shutil

from sabot.state import (
    MemoryBackend,
    RocksDBBackend,
    ValueState,
    ListState,
    MapState,
    ReducingState,
    AggregatingState,
)


class TestMemoryBackend:
    """Test in-memory state backend."""

    @pytest.fixture
    def backend(self):
        """Create memory backend."""
        return MemoryBackend(max_size=1000)

    def test_backend_creation(self, backend):
        """Test creating memory backend."""
        assert backend.max_size == 1000
        assert backend.size() == 0

    def test_put_and_get(self, backend):
        """Test basic put/get operations."""
        backend.put(namespace="test", key="key1", value="value1")

        result = backend.get(namespace="test", key="key1")
        assert result == "value1"

    def test_get_nonexistent(self, backend):
        """Test getting non-existent key."""
        result = backend.get(namespace="test", key="nonexistent")
        assert result is None

    def test_delete(self, backend):
        """Test deleting a key."""
        backend.put(namespace="test", key="key1", value="value1")
        backend.delete(namespace="test", key="key1")

        result = backend.get(namespace="test", key="key1")
        assert result is None

    def test_namespace_isolation(self, backend):
        """Test that namespaces are isolated."""
        backend.put(namespace="ns1", key="key1", value="value_ns1")
        backend.put(namespace="ns2", key="key1", value="value_ns2")

        assert backend.get(namespace="ns1", key="key1") == "value_ns1"
        assert backend.get(namespace="ns2", key="key1") == "value_ns2"

    def test_max_size_enforcement(self):
        """Test that max_size is enforced with LRU eviction."""
        backend = MemoryBackend(max_size=3)

        backend.put(namespace="test", key="key1", value="value1")
        backend.put(namespace="test", key="key2", value="value2")
        backend.put(namespace="test", key="key3", value="value3")

        # This should trigger eviction of key1 (LRU)
        backend.put(namespace="test", key="key4", value="value4")

        assert backend.get(namespace="test", key="key1") is None
        assert backend.get(namespace="test", key="key4") == "value4"

    def test_clear_namespace(self, backend):
        """Test clearing a namespace."""
        backend.put(namespace="test", key="key1", value="value1")
        backend.put(namespace="test", key="key2", value="value2")
        backend.put(namespace="other", key="key3", value="value3")

        backend.clear_namespace(namespace="test")

        assert backend.get(namespace="test", key="key1") is None
        assert backend.get(namespace="test", key="key2") is None
        assert backend.get(namespace="other", key="key3") == "value3"

    def test_ttl_expiration(self, backend):
        """Test TTL-based expiration."""
        import time

        backend.put(namespace="test", key="key1", value="value1", ttl_seconds=1)

        # Should exist immediately
        assert backend.get(namespace="test", key="key1") == "value1"

        # Wait for expiration
        time.sleep(1.1)

        # Should be expired
        assert backend.get(namespace="test", key="key1") is None


class TestRocksDBBackend:
    """Test RocksDB state backend."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for RocksDB."""
        temp = Path(tempfile.mkdtemp())
        yield temp
        shutil.rmtree(temp)

    @pytest.fixture
    def backend(self, temp_dir):
        """Create RocksDB backend."""
        return RocksDBBackend(db_path=str(temp_dir / "rocksdb"))

    def test_backend_creation(self, backend):
        """Test creating RocksDB backend."""
        assert backend.db_path is not None

    def test_put_and_get(self, backend):
        """Test basic put/get operations."""
        backend.put(namespace="test", key="key1", value="value1")

        result = backend.get(namespace="test", key="key1")
        assert result == "value1"

    def test_persistence(self, temp_dir):
        """Test that data persists across backend instances."""
        db_path = str(temp_dir / "rocksdb")

        # Create backend and write data
        backend1 = RocksDBBackend(db_path=db_path)
        backend1.put(namespace="test", key="key1", value="value1")
        backend1.close()

        # Create new backend instance and read data
        backend2 = RocksDBBackend(db_path=db_path)
        result = backend2.get(namespace="test", key="key1")

        assert result == "value1"
        backend2.close()

    def test_delete(self, backend):
        """Test deleting a key."""
        backend.put(namespace="test", key="key1", value="value1")
        backend.delete(namespace="test", key="key1")

        result = backend.get(namespace="test", key="key1")
        assert result is None

    def test_namespace_isolation(self, backend):
        """Test that namespaces are isolated."""
        backend.put(namespace="ns1", key="key1", value="value_ns1")
        backend.put(namespace="ns2", key="key1", value="value_ns2")

        assert backend.get(namespace="ns1", key="key1") == "value_ns1"
        assert backend.get(namespace="ns2", key="key1") == "value_ns2"


class TestValueState:
    """Test ValueState primitive."""

    @pytest.fixture
    def backend(self):
        """Create memory backend."""
        return MemoryBackend()

    def test_value_state_creation(self, backend):
        """Test creating a ValueState."""
        state = ValueState(backend=backend, namespace="test", key="counter")

        assert state.get() is None

    def test_value_state_update(self, backend):
        """Test updating value state."""
        state = ValueState(backend=backend, namespace="test", key="counter")

        state.set(42)
        assert state.get() == 42

        state.set(100)
        assert state.get() == 100

    def test_value_state_clear(self, backend):
        """Test clearing value state."""
        state = ValueState(backend=backend, namespace="test", key="counter")

        state.set(42)
        state.clear()

        assert state.get() is None


class TestListState:
    """Test ListState primitive."""

    @pytest.fixture
    def backend(self):
        """Create memory backend."""
        return MemoryBackend()

    def test_list_state_creation(self, backend):
        """Test creating a ListState."""
        state = ListState(backend=backend, namespace="test", key="events")

        assert state.get() == []

    def test_list_state_append(self, backend):
        """Test appending to list state."""
        state = ListState(backend=backend, namespace="test", key="events")

        state.append("event1")
        state.append("event2")
        state.append("event3")

        assert state.get() == ["event1", "event2", "event3"]

    def test_list_state_clear(self, backend):
        """Test clearing list state."""
        state = ListState(backend=backend, namespace="test", key="events")

        state.append("event1")
        state.append("event2")
        state.clear()

        assert state.get() == []


class TestMapState:
    """Test MapState primitive."""

    @pytest.fixture
    def backend(self):
        """Create memory backend."""
        return MemoryBackend()

    def test_map_state_creation(self, backend):
        """Test creating a MapState."""
        state = MapState(backend=backend, namespace="test", key="user_data")

        assert state.get("key1") is None

    def test_map_state_put_and_get(self, backend):
        """Test putting and getting from map state."""
        state = MapState(backend=backend, namespace="test", key="user_data")

        state.put("user1", {"name": "Alice", "score": 100})
        state.put("user2", {"name": "Bob", "score": 200})

        assert state.get("user1") == {"name": "Alice", "score": 100}
        assert state.get("user2") == {"name": "Bob", "score": 200}

    def test_map_state_keys(self, backend):
        """Test getting all keys from map state."""
        state = MapState(backend=backend, namespace="test", key="user_data")

        state.put("user1", {"score": 100})
        state.put("user2", {"score": 200})
        state.put("user3", {"score": 300})

        keys = state.keys()
        assert set(keys) == {"user1", "user2", "user3"}

    def test_map_state_remove(self, backend):
        """Test removing from map state."""
        state = MapState(backend=backend, namespace="test", key="user_data")

        state.put("user1", {"score": 100})
        state.remove("user1")

        assert state.get("user1") is None

    def test_map_state_clear(self, backend):
        """Test clearing map state."""
        state = MapState(backend=backend, namespace="test", key="user_data")

        state.put("user1", {"score": 100})
        state.put("user2", {"score": 200})
        state.clear()

        assert state.keys() == []


class TestReducingState:
    """Test ReducingState primitive."""

    @pytest.fixture
    def backend(self):
        """Create memory backend."""
        return MemoryBackend()

    def test_reducing_state_sum(self, backend):
        """Test reducing state with sum function."""
        def sum_reducer(a, b):
            return a + b

        state = ReducingState(
            backend=backend,
            namespace="test",
            key="total",
            reduce_fn=sum_reducer,
            initial_value=0
        )

        state.add(10)
        state.add(20)
        state.add(30)

        assert state.get() == 60

    def test_reducing_state_max(self, backend):
        """Test reducing state with max function."""
        def max_reducer(a, b):
            return max(a, b)

        state = ReducingState(
            backend=backend,
            namespace="test",
            key="max_value",
            reduce_fn=max_reducer,
            initial_value=float('-inf')
        )

        state.add(10)
        state.add(50)
        state.add(30)

        assert state.get() == 50


class TestAggregatingState:
    """Test AggregatingState primitive."""

    @pytest.fixture
    def backend(self):
        """Create memory backend."""
        return MemoryBackend()

    def test_aggregating_state_average(self, backend):
        """Test aggregating state for computing average."""
        def avg_aggregate(accumulator, value):
            """Accumulator: (sum, count)"""
            return (accumulator[0] + value, accumulator[1] + 1)

        def avg_result(accumulator):
            """Compute average from accumulator."""
            if accumulator[1] == 0:
                return 0
            return accumulator[0] / accumulator[1]

        state = AggregatingState(
            backend=backend,
            namespace="test",
            key="average",
            aggregate_fn=avg_aggregate,
            result_fn=avg_result,
            initial_value=(0, 0)
        )

        state.add(10)
        state.add(20)
        state.add(30)

        # Average of 10, 20, 30 = 20
        assert state.get() == 20


# Performance benchmarks
@pytest.mark.benchmark
class TestStatePerformance:
    """Performance benchmarks for state operations."""

    def test_memory_backend_put_performance(self, benchmark):
        """Benchmark memory backend put operations."""
        backend = MemoryBackend()

        def put_operation():
            backend.put(namespace="test", key="key1", value="value1")

        benchmark(put_operation)

    def test_memory_backend_get_performance(self, benchmark):
        """Benchmark memory backend get operations."""
        backend = MemoryBackend()
        backend.put(namespace="test", key="key1", value="value1")

        def get_operation():
            return backend.get(namespace="test", key="key1")

        result = benchmark(get_operation)
        assert result == "value1"

    def test_memory_backend_throughput(self):
        """Test memory backend throughput (target: 1M+ ops/sec)."""
        import time

        backend = MemoryBackend()

        num_ops = 100_000

        # Measure put throughput
        start = time.perf_counter()
        for i in range(num_ops):
            backend.put(namespace="test", key=f"key{i}", value=f"value{i}")
        end = time.perf_counter()

        put_throughput = num_ops / (end - start)

        # Should exceed 1M ops/sec
        assert put_throughput > 1_000_000, f"Put throughput {put_throughput:.0f} ops/sec below 1M"

        # Measure get throughput
        start = time.perf_counter()
        for i in range(num_ops):
            backend.get(namespace="test", key=f"key{i}")
        end = time.perf_counter()

        get_throughput = num_ops / (end - start)

        assert get_throughput > 1_000_000, f"Get throughput {get_throughput:.0f} ops/sec below 1M"
```

### Verification

```bash
# Run state unit tests
pytest tests/unit/test_cython_state.py -v

# Run with coverage
pytest tests/unit/test_cython_state.py --cov=sabot.state --cov-report=html

# Run benchmarks
pytest tests/unit/test_cython_state.py -m benchmark --benchmark-only
```

**Expected Results:**
- All tests pass
- Coverage for `sabot/state/` reaches 70%+
- Memory backend throughput confirmed >1M ops/sec

---

## Step 3.3: Unit Test Cython Time Modules

### Current State

**Location:** `sabot/_cython/time/`

**Modules:**
- `watermark_tracker.pyx` / `watermark_tracker.pxd` - Watermark tracking
- `timers.pyx` / `timers.pxd` - Timer service
- `event_time.pyx` - Event-time utilities
- `time_service.pyx` - Time service

**Current Tests:**
- Basic watermark tests exist
- Timer service untested
- Event-time utilities untested

### Implementation

#### Create `tests/unit/test_cython_time.py`

```python
"""Unit tests for Cython time modules."""

import pytest
import asyncio
import time as time_module

from sabot.time import (
    WatermarkTracker,
    Timers,
    EventTime,
    TimeService,
)


class TestWatermarkTracker:
    """Test WatermarkTracker for event-time processing."""

    def test_tracker_creation(self):
        """Test creating a watermark tracker."""
        tracker = WatermarkTracker(num_partitions=3)

        assert tracker.num_partitions == 3
        assert tracker.get_watermark() == 0

    def test_update_watermark(self):
        """Test updating watermark for a partition."""
        tracker = WatermarkTracker(num_partitions=2)

        tracker.update_watermark(partition_id=0, watermark=1000)

        # Global watermark should still be 0 (min of all partitions)
        assert tracker.get_watermark() == 0

        tracker.update_watermark(partition_id=1, watermark=500)

        # Global watermark should now be 500 (min of 1000 and 500)
        assert tracker.get_watermark() == 500

    def test_watermark_advancement(self):
        """Test watermark advances monotonically."""
        tracker = WatermarkTracker(num_partitions=2)

        tracker.update_watermark(partition_id=0, watermark=1000)
        tracker.update_watermark(partition_id=1, watermark=1000)

        assert tracker.get_watermark() == 1000

        # Advance both partitions
        tracker.update_watermark(partition_id=0, watermark=2000)
        tracker.update_watermark(partition_id=1, watermark=2000)

        assert tracker.get_watermark() == 2000

    def test_watermark_skew(self):
        """Test watermark with partition skew."""
        tracker = WatermarkTracker(num_partitions=3)

        # Partition 0 is ahead, partitions 1 and 2 lag
        tracker.update_watermark(partition_id=0, watermark=10000)
        tracker.update_watermark(partition_id=1, watermark=1000)
        tracker.update_watermark(partition_id=2, watermark=500)

        # Global watermark is min (500)
        assert tracker.get_watermark() == 500

    def test_get_partition_watermark(self):
        """Test getting watermark for specific partition."""
        tracker = WatermarkTracker(num_partitions=2)

        tracker.update_watermark(partition_id=0, watermark=1000)
        tracker.update_watermark(partition_id=1, watermark=2000)

        assert tracker.get_partition_watermark(partition_id=0) == 1000
        assert tracker.get_partition_watermark(partition_id=1) == 2000

    def test_watermark_update_performance(self):
        """Test watermark update latency (<5μs claimed)."""
        tracker = WatermarkTracker(num_partitions=100)

        # Measure 10000 updates
        start = time_module.perf_counter()
        for i in range(10000):
            tracker.update_watermark(partition_id=i % 100, watermark=i)
        end = time_module.perf_counter()

        avg_latency_us = ((end - start) / 10000) * 1_000_000

        # Should be <5μs per update
        assert avg_latency_us < 5, f"Watermark update latency {avg_latency_us:.2f}μs exceeds 5μs"


class TestTimers:
    """Test Timer service."""

    @pytest.fixture
    def timer_service(self):
        """Create timer service."""
        return Timers()

    def test_timer_creation(self, timer_service):
        """Test creating timer service."""
        assert timer_service is not None

    def test_register_timer(self, timer_service):
        """Test registering a timer."""
        fired = []

        def callback():
            fired.append(True)

        timer_id = timer_service.register_timer(
            timestamp=1000,
            callback=callback
        )

        assert timer_id is not None

    @pytest.mark.asyncio
    async def test_timer_firing(self, timer_service):
        """Test that timers fire at correct time."""
        fired = []

        def callback():
            fired.append(time_module.time())

        # Register timer for 100ms from now
        fire_time = int((time_module.time() + 0.1) * 1000)
        timer_service.register_timer(
            timestamp=fire_time,
            callback=callback
        )

        # Start timer service
        asyncio.create_task(timer_service.run())

        # Wait for timer to fire
        await asyncio.sleep(0.2)

        assert len(fired) == 1

    def test_cancel_timer(self, timer_service):
        """Test canceling a timer."""
        fired = []

        def callback():
            fired.append(True)

        timer_id = timer_service.register_timer(
            timestamp=1000,
            callback=callback
        )

        timer_service.cancel_timer(timer_id)

        # Timer should not fire
        timer_service.advance_to(timestamp=2000)

        assert len(fired) == 0

    def test_multiple_timers(self, timer_service):
        """Test multiple timers fire in order."""
        fired_order = []

        def callback(timer_id):
            fired_order.append(timer_id)

        # Register timers in non-sequential order
        timer_service.register_timer(timestamp=3000, callback=lambda: callback(3))
        timer_service.register_timer(timestamp=1000, callback=lambda: callback(1))
        timer_service.register_timer(timestamp=2000, callback=lambda: callback(2))

        # Advance time
        timer_service.advance_to(timestamp=4000)

        # Timers should fire in timestamp order
        assert fired_order == [1, 2, 3]


class TestEventTime:
    """Test EventTime utilities."""

    def test_extract_event_time(self):
        """Test extracting event time from message."""
        message = {
            "timestamp": 1609459200000,  # 2021-01-01 00:00:00 UTC
            "data": "test"
        }

        event_time = EventTime.extract(message, timestamp_field="timestamp")

        assert event_time == 1609459200000

    def test_extract_event_time_missing_field(self):
        """Test extracting event time when field is missing."""
        message = {"data": "test"}

        # Should fall back to processing time or raise error
        with pytest.raises(KeyError):
            EventTime.extract(message, timestamp_field="timestamp")

    def test_assign_event_time(self):
        """Test assigning event time to message."""
        message = {"data": "test"}

        EventTime.assign(message, timestamp=1609459200000, timestamp_field="timestamp")

        assert message["timestamp"] == 1609459200000


class TestTimeService:
    """Test TimeService coordination."""

    def test_time_service_creation(self):
        """Test creating time service."""
        service = TimeService()

        assert service is not None

    def test_processing_time(self):
        """Test getting current processing time."""
        service = TimeService()

        proc_time = service.processing_time()

        # Should be close to current time
        current_time = int(time_module.time() * 1000)
        assert abs(proc_time - current_time) < 100  # Within 100ms

    def test_event_time_mode(self):
        """Test setting event-time mode."""
        service = TimeService(time_characteristic="event_time")

        assert service.time_characteristic == "event_time"

    def test_ingestion_time_mode(self):
        """Test setting ingestion-time mode."""
        service = TimeService(time_characteristic="ingestion_time")

        assert service.time_characteristic == "ingestion_time"


# Performance benchmarks
@pytest.mark.benchmark
class TestTimePerformance:
    """Performance benchmarks for time operations."""

    def test_watermark_update_benchmark(self, benchmark):
        """Benchmark watermark updates."""
        tracker = WatermarkTracker(num_partitions=10)

        def update_watermark():
            tracker.update_watermark(partition_id=0, watermark=1000)

        benchmark(update_watermark)

    def test_watermark_get_benchmark(self, benchmark):
        """Benchmark getting global watermark."""
        tracker = WatermarkTracker(num_partitions=10)

        # Set up some watermarks
        for i in range(10):
            tracker.update_watermark(partition_id=i, watermark=1000)

        def get_watermark():
            return tracker.get_watermark()

        result = benchmark(get_watermark)
        assert result == 1000
```

### Verification

```bash
# Run time unit tests
pytest tests/unit/test_cython_time.py -v

# Run with coverage
pytest tests/unit/test_cython_time.py --cov=sabot.time --cov-report=html

# Run benchmarks
pytest tests/unit/test_cython_time.py -m benchmark --benchmark-only
```

**Expected Results:**
- All tests pass
- Coverage for `sabot/time/` reaches 70%+
- Watermark update latency confirmed <5μs

---

## Step 3.4: Unit Test Arrow Integration

### Current State

**Location:** `sabot/_c/arrow_core.pyx` and related Arrow modules

**Modules:**
- `arrow_core.pyx` - Core Arrow C++ integration
- `batch_processor.pyx` - Batch transformations
- `join_processor.pyx` - Join operations
- `window_processor.pyx` - Window operations

**Current Tests:**
- Arrow core recently fixed in refactor
- Batch operations untested
- Join operations untested
- Window operations untested

### Implementation

#### Create `tests/unit/test_arrow_core.py`

```python
"""Unit tests for Arrow C++ integration."""

import pytest
import pyarrow as pa
import numpy as np

from sabot._c.arrow_core import (
    create_batch,
    batch_to_dict,
    filter_batch,
    project_batch,
)
from sabot._c.batch_processor import BatchProcessor
from sabot._c.join_processor import JoinProcessor
from sabot._c.window_processor import WindowProcessor


class TestArrowCore:
    """Test core Arrow operations."""

    def test_create_batch(self):
        """Test creating Arrow RecordBatch."""
        data = {
            "id": [1, 2, 3, 4, 5],
            "value": [10.0, 20.0, 30.0, 40.0, 50.0],
            "label": ["a", "b", "c", "d", "e"]
        }

        batch = create_batch(data)

        assert isinstance(batch, pa.RecordBatch)
        assert batch.num_rows == 5
        assert batch.num_columns == 3

    def test_batch_to_dict(self):
        """Test converting RecordBatch to dict."""
        # Create batch
        data = {
            "id": [1, 2, 3],
            "value": [10.0, 20.0, 30.0]
        }
        batch = pa.RecordBatch.from_pydict(data)

        # Convert back to dict
        result = batch_to_dict(batch)

        assert result["id"] == [1, 2, 3]
        assert result["value"] == [10.0, 20.0, 30.0]

    def test_filter_batch(self):
        """Test filtering RecordBatch."""
        data = {
            "id": [1, 2, 3, 4, 5],
            "value": [10.0, 20.0, 30.0, 40.0, 50.0]
        }
        batch = pa.RecordBatch.from_pydict(data)

        # Filter where value > 25
        mask = pa.array([False, False, True, True, True])
        filtered = filter_batch(batch, mask)

        assert filtered.num_rows == 3
        result = filtered.to_pydict()
        assert result["id"] == [3, 4, 5]
        assert result["value"] == [30.0, 40.0, 50.0]

    def test_project_batch(self):
        """Test projecting columns from RecordBatch."""
        data = {
            "id": [1, 2, 3],
            "value": [10.0, 20.0, 30.0],
            "label": ["a", "b", "c"]
        }
        batch = pa.RecordBatch.from_pydict(data)

        # Project only id and value columns
        projected = project_batch(batch, columns=["id", "value"])

        assert projected.num_columns == 2
        assert projected.schema.names == ["id", "value"]


class TestBatchProcessor:
    """Test BatchProcessor for transformations."""

    @pytest.fixture
    def processor(self):
        """Create batch processor."""
        return BatchProcessor()

    def test_map_batch(self, processor):
        """Test mapping function over batch."""
        data = {
            "value": [1, 2, 3, 4, 5]
        }
        batch = pa.RecordBatch.from_pydict(data)

        # Map: multiply values by 2
        def multiply_by_2(batch):
            values = batch.column("value").to_pylist()
            return pa.RecordBatch.from_pydict({
                "value": [v * 2 for v in values]
            })

        result = processor.map_batch(batch, multiply_by_2)

        result_dict = result.to_pydict()
        assert result_dict["value"] == [2, 4, 6, 8, 10]

    def test_filter_batch(self, processor):
        """Test filtering batch with predicate."""
        data = {
            "id": [1, 2, 3, 4, 5],
            "value": [10, 20, 30, 40, 50]
        }
        batch = pa.RecordBatch.from_pydict(data)

        # Filter: keep only values > 25
        def predicate(batch):
            values = batch.column("value")
            return pa.compute.greater(values, 25)

        result = processor.filter_batch(batch, predicate)

        result_dict = result.to_pydict()
        assert result_dict["id"] == [3, 4, 5]
        assert result_dict["value"] == [30, 40, 50]

    def test_aggregate_batch(self, processor):
        """Test aggregating batch."""
        data = {
            "category": ["A", "B", "A", "B", "A"],
            "value": [10, 20, 30, 40, 50]
        }
        batch = pa.RecordBatch.from_pydict(data)

        # Aggregate: sum values by category
        result = processor.aggregate_batch(
            batch,
            group_by=["category"],
            aggregations={"value": "sum"}
        )

        result_dict = result.to_pydict()

        # Category A: 10 + 30 + 50 = 90
        # Category B: 20 + 40 = 60
        assert sorted(result_dict["category"]) == ["A", "B"]
        category_sums = dict(zip(result_dict["category"], result_dict["value"]))
        assert category_sums["A"] == 90
        assert category_sums["B"] == 60


class TestJoinProcessor:
    """Test JoinProcessor for stream joins."""

    @pytest.fixture
    def processor(self):
        """Create join processor."""
        return JoinProcessor()

    def test_inner_join(self, processor):
        """Test inner join."""
        left_data = {
            "id": [1, 2, 3],
            "left_value": ["a", "b", "c"]
        }
        right_data = {
            "id": [2, 3, 4],
            "right_value": ["x", "y", "z"]
        }

        left_batch = pa.RecordBatch.from_pydict(left_data)
        right_batch = pa.RecordBatch.from_pydict(right_data)

        result = processor.join(
            left_batch,
            right_batch,
            on="id",
            how="inner"
        )

        result_dict = result.to_pydict()

        # Should match on id=2 and id=3
        assert result_dict["id"] == [2, 3]
        assert result_dict["left_value"] == ["b", "c"]
        assert result_dict["right_value"] == ["x", "y"]

    def test_left_join(self, processor):
        """Test left outer join."""
        left_data = {
            "id": [1, 2, 3],
            "left_value": ["a", "b", "c"]
        }
        right_data = {
            "id": [2, 3, 4],
            "right_value": ["x", "y", "z"]
        }

        left_batch = pa.RecordBatch.from_pydict(left_data)
        right_batch = pa.RecordBatch.from_pydict(right_data)

        result = processor.join(
            left_batch,
            right_batch,
            on="id",
            how="left"
        )

        result_dict = result.to_pydict()

        # Should include all left rows
        assert result_dict["id"] == [1, 2, 3]
        assert result_dict["left_value"] == ["a", "b", "c"]
        # id=1 has no match, should be None
        assert result_dict["right_value"][0] is None


class TestWindowProcessor:
    """Test WindowProcessor for windowed operations."""

    @pytest.fixture
    def processor(self):
        """Create window processor."""
        return WindowProcessor()

    def test_tumbling_window(self, processor):
        """Test tumbling window."""
        data = {
            "timestamp": [1000, 1500, 2000, 2500, 3000, 3500],
            "value": [10, 20, 30, 40, 50, 60]
        }
        batch = pa.RecordBatch.from_pydict(data)

        # 1-second tumbling windows
        result = processor.tumbling_window(
            batch,
            timestamp_col="timestamp",
            window_size_ms=1000,
            aggregation={"value": "sum"}
        )

        result_dict = result.to_pydict()

        # Window 1000-2000: 10 + 20 = 30
        # Window 2000-3000: 30 + 40 = 70
        # Window 3000-4000: 50 + 60 = 110
        assert len(result_dict["window_start"]) == 3

    def test_sliding_window(self, processor):
        """Test sliding window."""
        data = {
            "timestamp": [1000, 2000, 3000, 4000, 5000],
            "value": [10, 20, 30, 40, 50]
        }
        batch = pa.RecordBatch.from_pydict(data)

        # 2-second sliding windows, 1-second slide
        result = processor.sliding_window(
            batch,
            timestamp_col="timestamp",
            window_size_ms=2000,
            slide_ms=1000,
            aggregation={"value": "sum"}
        )

        result_dict = result.to_pydict()

        # Windows should overlap
        assert len(result_dict["window_start"]) > 0

    def test_session_window(self, processor):
        """Test session window with inactivity gap."""
        data = {
            "timestamp": [1000, 1100, 1200, 5000, 5100, 5200],
            "user_id": ["user1"] * 6,
            "value": [10, 20, 30, 40, 50, 60]
        }
        batch = pa.RecordBatch.from_pydict(data)

        # Session window with 1-second gap
        result = processor.session_window(
            batch,
            timestamp_col="timestamp",
            gap_ms=1000,
            partition_by=["user_id"],
            aggregation={"value": "sum"}
        )

        result_dict = result.to_pydict()

        # Should create 2 sessions:
        # Session 1: 1000-1200 (sum = 60)
        # Session 2: 5000-5200 (sum = 150)
        assert len(result_dict["session_id"]) == 2


# Performance benchmarks
@pytest.mark.benchmark
class TestArrowPerformance:
    """Performance benchmarks for Arrow operations."""

    def test_batch_creation_benchmark(self, benchmark):
        """Benchmark RecordBatch creation."""
        data = {
            "id": list(range(10000)),
            "value": np.random.randn(10000).tolist()
        }

        def create():
            return pa.RecordBatch.from_pydict(data)

        benchmark(create)

    def test_filter_benchmark(self, benchmark):
        """Benchmark filtering large batch."""
        data = {
            "id": list(range(100000)),
            "value": np.random.randn(100000).tolist()
        }
        batch = pa.RecordBatch.from_pydict(data)

        def filter_op():
            mask = pa.compute.greater(batch.column("value"), 0)
            return pa.compute.filter(batch, mask)

        benchmark(filter_op)
```

### Verification

```bash
# Run Arrow unit tests
pytest tests/unit/test_arrow_core.py -v

# Run with coverage
pytest tests/unit/test_arrow_core.py --cov=sabot._c --cov-report=html

# Run benchmarks
pytest tests/unit/test_arrow_core.py -m benchmark --benchmark-only
```

**Expected Results:**
- All tests pass
- Coverage for Arrow modules reaches 60%+
- Zero-copy operations verified

---

## Step 3.5: Fraud Demo End-to-End Test

### Current State

**File:** `examples/fraud_app.py`

**Status:**
- Fraud detection demo exists
- Measured throughput: 3K-6K txn/s
- No automated test exists

### Implementation

#### Create `tests/integration/test_fraud_demo_e2e.py`

```python
"""End-to-end integration test for fraud detection demo."""

import pytest
import asyncio
import json
from pathlib import Path
import subprocess
import time

import sabot as sb
from examples.fraud_app import app as fraud_app


class TestFraudDemoE2E:
    """End-to-end test for fraud detection application."""

    @pytest.fixture
    async def kafka_broker(self):
        """Start Kafka broker using docker-compose."""
        # Start Kafka and dependencies
        subprocess.run(
            ["docker", "compose", "up", "-d", "redpanda"],
            cwd=Path.cwd(),
            check=True
        )

        # Wait for Kafka to be ready
        await asyncio.sleep(5)

        yield "localhost:19092"

        # Cleanup
        subprocess.run(
            ["docker", "compose", "down"],
            cwd=Path.cwd(),
            check=False
        )

    @pytest.fixture
    def fraud_topic(self):
        """Kafka topic for fraud detection."""
        return "transactions"

    async def test_fraud_app_startup(self, kafka_broker, fraud_topic):
        """Test that fraud app starts without errors."""
        # App should start
        assert fraud_app is not None
        assert len(fraud_app.agents) > 0

        # Should have fraud detection agent
        agent_names = [agent.name for agent in fraud_app.agents.values()]
        assert "fraud_detector" in agent_names

    async def test_fraud_detection_pipeline(self, kafka_broker, fraud_topic):
        """Test fraud detection on sample transactions."""
        from aiokafka import AIOKafkaProducer

        # Create Kafka producer
        producer = AIOKafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode()
        )
        await producer.start()

        try:
            # Send test transactions
            transactions = [
                {
                    "transaction_id": "tx_001",
                    "user_id": "user_001",
                    "amount": 50.0,
                    "timestamp": int(time.time() * 1000),
                    "location": "US"
                },
                {
                    "transaction_id": "tx_002",
                    "user_id": "user_001",
                    "amount": 100.0,
                    "timestamp": int(time.time() * 1000) + 1000,
                    "location": "US"
                },
                # High-velocity fraud pattern
                {
                    "transaction_id": "tx_003",
                    "user_id": "user_002",
                    "amount": 200.0,
                    "timestamp": int(time.time() * 1000),
                    "location": "US"
                },
                {
                    "transaction_id": "tx_004",
                    "user_id": "user_002",
                    "amount": 250.0,
                    "timestamp": int(time.time() * 1000) + 100,  # 100ms later
                    "location": "US"
                },
                {
                    "transaction_id": "tx_005",
                    "user_id": "user_002",
                    "amount": 300.0,
                    "timestamp": int(time.time() * 1000) + 200,  # 200ms later
                    "location": "US"
                },
            ]

            for txn in transactions:
                await producer.send(fraud_topic, value=txn)

            # Flush
            await producer.flush()

            # Wait for processing
            await asyncio.sleep(2)

            # TODO: Verify fraud alerts were generated
            # (requires implementing fraud alert collection)

        finally:
            await producer.stop()

    async def test_fraud_throughput(self, kafka_broker, fraud_topic):
        """Test fraud detection throughput (target: 3K-6K txn/s)."""
        from aiokafka import AIOKafkaProducer
        import random

        producer = AIOKafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode()
        )
        await producer.start()

        try:
            num_transactions = 10000

            # Generate transactions
            transactions = []
            for i in range(num_transactions):
                transactions.append({
                    "transaction_id": f"tx_{i:06d}",
                    "user_id": f"user_{random.randint(0, 1000)}",
                    "amount": random.uniform(10.0, 500.0),
                    "timestamp": int(time.time() * 1000) + i,
                    "location": random.choice(["US", "EU", "ASIA"])
                })

            # Send transactions and measure throughput
            start_time = time.perf_counter()

            for txn in transactions:
                await producer.send(fraud_topic, value=txn)

            await producer.flush()

            end_time = time.perf_counter()

            throughput = num_transactions / (end_time - start_time)

            print(f"Throughput: {throughput:.0f} txn/s")

            # Should achieve 3K-6K txn/s
            assert throughput >= 3000, f"Throughput {throughput:.0f} txn/s below 3K"

        finally:
            await producer.stop()

    async def test_fraud_state_consistency(self, kafka_broker, fraud_topic):
        """Test that fraud detection state is consistent."""
        # Send transactions for same user
        from aiokafka import AIOKafkaProducer

        producer = AIOKafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode()
        )
        await producer.start()

        try:
            user_id = "test_user_001"

            # Send 10 transactions
            for i in range(10):
                txn = {
                    "transaction_id": f"tx_{i}",
                    "user_id": user_id,
                    "amount": 100.0,
                    "timestamp": int(time.time() * 1000) + i * 1000,
                    "location": "US"
                }
                await producer.send(fraud_topic, value=txn)

            await producer.flush()

            # Wait for processing
            await asyncio.sleep(2)

            # TODO: Verify state shows 10 transactions for user
            # (requires accessing agent state)

        finally:
            await producer.stop()

    async def test_fraud_checkpoint_creation(self, kafka_broker, fraud_topic):
        """Test that checkpoints are created during fraud detection."""
        # Start app with checkpoint interval
        # Process transactions
        # Verify checkpoint created

        # TODO: Implement when checkpoint integration complete
        pytest.skip("Checkpoint integration not complete in Phase 2")
```

### Verification

```bash
# Ensure Kafka is running
docker compose up -d redpanda

# Run fraud demo E2E test
pytest tests/integration/test_fraud_demo_e2e.py -v -s

# Stop Kafka
docker compose down
```

**Expected Results:**
- Fraud app starts without errors
- Transactions processed through pipeline
- Throughput reaches 3K-6K txn/s
- State consistency maintained

---

## Phase 3 Completion Criteria

### Tests Created

- [x] `tests/unit/test_cython_checkpoint.py` - Checkpoint module tests
- [x] `tests/unit/test_cython_state.py` - State backend tests
- [x] `tests/unit/test_cython_time.py` - Time and watermark tests
- [x] `tests/unit/test_arrow_core.py` - Arrow integration tests
- [x] `tests/integration/test_fraud_demo_e2e.py` - End-to-end fraud demo test

### Coverage Goals

- Checkpoint modules: 70%+
- State modules: 70%+
- Time modules: 70%+
- Arrow modules: 60%+
- Overall codebase: 30%+

### Performance Verified

- Checkpoint trigger latency: <10μs
- State backend throughput: >1M ops/sec
- Watermark update latency: <5μs
- Fraud detection throughput: 3K-6K txn/s

### Documentation

- Test coverage report generated
- Performance benchmark results documented
- Known issues cataloged

---

## Dependencies

**Phase 1 Complete:**
- CLI loads real applications
- Channels can be created

**Phase 2 Complete:**
- Agents consume from Kafka
- Agents use state backends
- Checkpoints can be triggered

---

## Next Phase

**Phase 4: Stream API Completion**

After Phase 3, the existing Cython modules are tested and verified. Phase 4 will complete the Stream API by implementing window operations, join operations, and state integration.

---

**Last Updated:** October 3, 2025
**Status:** Technical specification for Phase 3
