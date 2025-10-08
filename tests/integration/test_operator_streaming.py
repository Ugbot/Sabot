"""
Operator-to-Operator Streaming Tests

Simple, focused tests for streaming data between operators using lock-free shuffle.
Tests the actual data flow paths that will be used in production.
"""

import pytest
import time
from sabot import cyarrow as pa

from sabot._cython.shuffle.atomic_partition_store import AtomicPartitionStore
from sabot._cython.shuffle.lock_free_queue import SPSCRingBuffer, MPSCQueue


# ============================================================================
# Test Schema and Data
# ============================================================================

@pytest.fixture
def sensor_schema():
    """Schema for sensor data stream."""
    return pa.schema([
        ('sensor_id', pa.int32()),
        ('timestamp_ms', pa.int64()),
        ('temperature', pa.float64()),
        ('humidity', pa.float64()),
        ('location', pa.string()),
    ])


@pytest.fixture
def create_sensor_batch():
    """Factory to create sensor data batches."""
    def _create(sensor_id, start_ts, num_readings=100):
        return pa.RecordBatch.from_arrays([
            pa.array([sensor_id] * num_readings, type=pa.int32()),
            pa.array([start_ts + i * 1000 for i in range(num_readings)], type=pa.int64()),
            pa.array([20.0 + (i % 10) * 0.5 for i in range(num_readings)], type=pa.float64()),
            pa.array([45.0 + (i % 15) * 0.3 for i in range(num_readings)], type=pa.float64()),
            pa.array([f'zone_{sensor_id % 3}' for _ in range(num_readings)], type=pa.string()),
        ], schema=sensor_schema())
    return _create


# ============================================================================
# Simple Operator Streaming (In-Process)
# ============================================================================

class TestOperatorStreaming:
    """Test streaming between operators using lock-free primitives."""

    def test_filter_to_map_pipeline(self, sensor_schema, create_sensor_batch):
        """
        Test: Filter -> Map pipeline

        Filter operator: Keep only high temperature readings
        Map operator: Convert Celsius to Fahrenheit
        """
        # Step 1: Filter operator produces filtered batches
        source_batch = create_sensor_batch(sensor_id=1, start_ts=1000000, num_readings=200)

        # Filter: temperature > 22.0
        mask = pa.compute.greater(source_batch.column('temperature'), pa.scalar(22.0))
        filtered_batch = source_batch.filter(mask)

        print(f"Filter operator: {source_batch.num_rows} -> {filtered_batch.num_rows} rows")

        # Step 2: Use SPSC queue to stream filtered batch to Map operator
        queue = SPSCRingBuffer(capacity=16)

        # Filter pushes
        success = queue.push(1, 0, filtered_batch)  # shuffle_hash=1, partition=0
        assert success

        # Map pops
        result = queue.pop()
        assert result is not None

        shuffle_hash, partition_id, received_batch = result
        assert received_batch.num_rows == filtered_batch.num_rows

        # Map operator: Convert C to F
        temp_c = received_batch.column('temperature')
        temp_f = pa.compute.multiply(temp_c, pa.scalar(9.0/5.0))
        temp_f = pa.compute.add(temp_f, pa.scalar(32.0))

        mapped_batch = received_batch.set_column(
            2,  # temperature column index
            'temperature_f',
            temp_f
        )

        print(f"Map operator: Converted {mapped_batch.num_rows} readings to Fahrenheit")
        print(f"Sample temp F: {mapped_batch.column('temperature_f')[0].as_py():.1f}")

    def test_map_to_aggregate_shuffle(self, sensor_schema, create_sensor_batch):
        """
        Test: Map -> Shuffle -> Aggregate pipeline

        Map operator partitions by sensor_id
        Aggregate operator combines readings per sensor
        """
        num_sensors = 4
        store = AtomicPartitionStore(size=128)

        # Step 1: Map operator creates partitions by sensor_id
        for sensor_id in range(num_sensors):
            batch = create_sensor_batch(sensor_id=sensor_id, start_ts=1000000 + sensor_id * 10000)

            # Hash by sensor_id for shuffle
            shuffle_hash = hash(f"sensor_agg_{sensor_id}")

            # Store in partition store (lock-free)
            success = store.insert(shuffle_hash, sensor_id, batch)
            assert success

        print(f"Map operator produced {num_sensors} partitions")

        # Step 2: Aggregate operator reads partitions and aggregates
        total_readings = 0
        avg_temps = []

        for sensor_id in range(num_sensors):
            shuffle_hash = hash(f"sensor_agg_{sensor_id}")

            # Fetch partition (lock-free)
            batch = store.get(shuffle_hash, sensor_id)
            assert batch is not None

            # Aggregate: compute average temperature
            temp_array = batch.column('temperature')
            avg_temp = pa.compute.mean(temp_array).as_py()

            total_readings += batch.num_rows
            avg_temps.append(avg_temp)

        print(f"Aggregate operator processed {total_readings} readings")
        print(f"Average temperatures: {[f'{t:.2f}' for t in avg_temps]}")

        assert total_readings == num_sensors * 100
        assert len(avg_temps) == num_sensors

    def test_multi_producer_aggregate(self, sensor_schema, create_sensor_batch):
        """
        Test: Multiple producers -> MPSC queue -> Single aggregator

        Simulates multiple upstream operators sending to one aggregator.
        """
        num_producers = 8
        queue = MPSCQueue(capacity=128)

        # Step 1: Multiple producers push concurrently
        import threading

        def producer(producer_id):
            batch = create_sensor_batch(sensor_id=producer_id, start_ts=1000000 + producer_id * 10000, num_readings=50)

            # Push to MPSC queue (lock-free with CAS)
            success = queue.push_mpsc(producer_id, producer_id, batch)
            assert success

        threads = []
        for p_id in range(num_producers):
            thread = threading.Thread(target=producer, args=(p_id,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        print(f"Producers sent {num_producers} batches to queue")

        # Step 2: Single consumer aggregates all
        total_readings = 0
        received_producers = set()

        for _ in range(num_producers):
            result = queue.pop_mpsc()
            assert result is not None

            shuffle_hash, partition_id, batch = result
            total_readings += batch.num_rows
            received_producers.add(shuffle_hash)

        print(f"Aggregator received {total_readings} total readings from {len(received_producers)} producers")

        assert len(received_producers) == num_producers
        assert total_readings == num_producers * 50

    def test_windowed_aggregation_pipeline(self, sensor_schema, create_sensor_batch):
        """
        Test: Windowed aggregation with shuffle

        Window operator: Group readings into time windows
        Shuffle: Partition by window_id
        Aggregate: Compute window statistics
        """
        window_size_ms = 5000  # 5 second windows
        num_windows = 4
        store = AtomicPartitionStore(size=256)

        # Step 1: Window operator assigns readings to windows
        for window_id in range(num_windows):
            # Create batch with timestamps in this window
            start_ts = window_id * window_size_ms
            batch = create_sensor_batch(sensor_id=1, start_ts=start_ts, num_readings=100)

            # Store partition by window_id
            shuffle_hash = hash(f"window_{window_id}")
            success = store.insert(shuffle_hash, window_id, batch)
            assert success

        print(f"Window operator created {num_windows} windows")

        # Step 2: Aggregate operator processes each window
        window_stats = []

        for window_id in range(num_windows):
            shuffle_hash = hash(f"window_{window_id}")

            # Fetch window data (lock-free)
            batch = store.get(shuffle_hash, window_id)
            assert batch is not None

            # Compute window statistics
            temp_array = batch.column('temperature')
            humidity_array = batch.column('humidity')

            stats = {
                'window_id': window_id,
                'count': batch.num_rows,
                'avg_temp': pa.compute.mean(temp_array).as_py(),
                'min_temp': pa.compute.min(temp_array).as_py(),
                'max_temp': pa.compute.max(temp_array).as_py(),
                'avg_humidity': pa.compute.mean(humidity_array).as_py(),
            }

            window_stats.append(stats)

        print(f"\nWindow Statistics:")
        for stats in window_stats:
            print(f"  Window {stats['window_id']}: {stats['count']} readings, "
                  f"temp {stats['min_temp']:.1f}-{stats['max_temp']:.1f}°C (avg {stats['avg_temp']:.1f}°C)")

        assert len(window_stats) == num_windows


# ============================================================================
# Shuffle Hash Partitioning
# ============================================================================

class TestShufflePartitioning:
    """Test hash partitioning for shuffle operations."""

    def test_hash_partition_by_key(self, sensor_schema, create_sensor_batch):
        """Test hash partitioning by key field (location)."""
        num_partitions = 4
        store = AtomicPartitionStore(size=256)

        # Create batch with multiple locations
        batch = pa.RecordBatch.from_arrays([
            pa.array([1, 1, 1, 2, 2, 2, 3, 3, 3] * 10, type=pa.int32()),
            pa.array(list(range(90)), type=pa.int64()),
            pa.array([20.0 + i * 0.1 for i in range(90)], type=pa.float64()),
            pa.array([45.0 + i * 0.1 for i in range(90)], type=pa.float64()),
            pa.array(['zone_0', 'zone_1', 'zone_2'] * 30, type=pa.string()),
        ], schema=sensor_schema)

        # Partition by location (hash partitioning)
        location_array = batch.column('location')

        partition_batches = {}

        for row_idx in range(batch.num_rows):
            location = location_array[row_idx].as_py()

            # Hash location to partition
            partition_id = hash(location) % num_partitions

            if partition_id not in partition_batches:
                partition_batches[partition_id] = []

            # Collect row indices for this partition
            partition_batches[partition_id].append(row_idx)

        # Create partitioned batches
        for partition_id, row_indices in partition_batches.items():
            indices = pa.array(row_indices, type=pa.int32())
            partition_batch = batch.take(indices)

            # Store partition (lock-free)
            shuffle_hash = 12345
            success = store.insert(shuffle_hash, partition_id, partition_batch)
            assert success

        print(f"\nHash partitioning results:")
        print(f"  Total rows: {batch.num_rows}")
        print(f"  Partitions created: {len(partition_batches)}")
        for partition_id in sorted(partition_batches.keys()):
            print(f"  Partition {partition_id}: {len(partition_batches[partition_id])} rows")

        # Verify we can fetch partitions
        for partition_id in partition_batches.keys():
            shuffle_hash = 12345
            fetched = store.get(shuffle_hash, partition_id)
            assert fetched is not None
            assert fetched.num_rows == len(partition_batches[partition_id])

    def test_range_partition_by_timestamp(self, sensor_schema, create_sensor_batch):
        """Test range partitioning by timestamp field."""
        num_partitions = 4
        store = AtomicPartitionStore(size=256)

        # Create batch spanning timestamp range
        batch = create_sensor_batch(sensor_id=1, start_ts=0, num_readings=1000)

        # Get timestamp range
        ts_array = batch.column('timestamp_ms')
        min_ts = pa.compute.min(ts_array).as_py()
        max_ts = pa.compute.max(ts_array).as_py()
        range_size = (max_ts - min_ts) / num_partitions

        # Range partition by timestamp
        partition_batches = {}

        for row_idx in range(batch.num_rows):
            ts = ts_array[row_idx].as_py()

            # Determine partition by range
            partition_id = min(int((ts - min_ts) / range_size), num_partitions - 1)

            if partition_id not in partition_batches:
                partition_batches[partition_id] = []

            partition_batches[partition_id].append(row_idx)

        # Create and store partitions
        for partition_id, row_indices in partition_batches.items():
            indices = pa.array(row_indices, type=pa.int32())
            partition_batch = batch.take(indices)

            shuffle_hash = 99999
            success = store.insert(shuffle_hash, partition_id, partition_batch)
            assert success

        print(f"\nRange partitioning results:")
        print(f"  Total rows: {batch.num_rows}")
        print(f"  Timestamp range: {min_ts} - {max_ts}")
        print(f"  Range per partition: {range_size:.0f}ms")
        for partition_id in sorted(partition_batches.keys()):
            print(f"  Partition {partition_id}: {len(partition_batches[partition_id])} rows")


# ============================================================================
# Performance Micro-Benchmarks
# ============================================================================

class TestOperatorPerformance:
    """Performance tests for operator streaming."""

    def test_queue_throughput(self, sensor_schema, create_sensor_batch):
        """Measure queue throughput for operator-to-operator streaming."""
        queue = SPSCRingBuffer(capacity=1024)
        num_batches = 1000

        batch = create_sensor_batch(sensor_id=1, start_ts=0, num_readings=100)

        # Measure push throughput
        start = time.perf_counter()
        for i in range(num_batches):
            while not queue.push(i, i, batch):
                pass
        push_elapsed = time.perf_counter() - start

        # Measure pop throughput
        start = time.perf_counter()
        for _ in range(num_batches):
            while queue.pop() is None:
                pass
        pop_elapsed = time.perf_counter() - start

        push_throughput = num_batches / push_elapsed
        pop_throughput = num_batches / pop_elapsed
        total_rows = num_batches * 100

        print(f"\nQueue Throughput:")
        print(f"  Push: {push_throughput:.0f} batches/sec ({total_rows/push_elapsed:.0f} rows/sec)")
        print(f"  Pop: {pop_throughput:.0f} batches/sec ({total_rows/pop_elapsed:.0f} rows/sec)")
        print(f"  Latency: {push_elapsed*1e6/num_batches:.1f}μs per batch")

    def test_shuffle_store_throughput(self, sensor_schema, create_sensor_batch):
        """Measure atomic store throughput for shuffle operations."""
        store = AtomicPartitionStore(size=16384)
        num_partitions = 1000

        batch = create_sensor_batch(sensor_id=1, start_ts=0, num_readings=100)

        # Measure insert throughput
        start = time.perf_counter()
        for i in range(num_partitions):
            store.insert(i, 0, batch)
        insert_elapsed = time.perf_counter() - start

        # Measure get throughput
        start = time.perf_counter()
        for i in range(num_partitions):
            store.get(i, 0)
        get_elapsed = time.perf_counter() - start

        insert_throughput = num_partitions / insert_elapsed
        get_throughput = num_partitions / get_elapsed

        print(f"\nShuffle Store Throughput:")
        print(f"  Insert: {insert_throughput:.0f} partitions/sec ({insert_elapsed*1e6/num_partitions:.1f}μs/op)")
        print(f"  Get: {get_throughput:.0f} partitions/sec ({get_elapsed*1e6/num_partitions:.1f}μs/op)")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
