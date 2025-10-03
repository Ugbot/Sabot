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
