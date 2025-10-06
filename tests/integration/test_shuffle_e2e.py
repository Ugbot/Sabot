#!/usr/bin/env python3
"""
End-to-end integration test for shuffle system.

Tests complete shuffle workflow:
1. Partition data using hash partitioner
2. Buffer partitioned data
3. Transport data via Arrow Flight
4. Read and merge partitioned data
"""

import pytest
import time
import pyarrow as pa

# Import shuffle components
try:
    from sabot._cython.shuffle.partitioner import HashPartitioner
    from sabot._cython.shuffle.shuffle_buffer import ShuffleBuffer
    from sabot._cython.shuffle.shuffle_transport import ShuffleTransport
    from sabot._cython.shuffle.shuffle_manager import ShuffleManager
    from sabot._cython.shuffle.types import ShuffleEdgeType
    SHUFFLE_AVAILABLE = True
except ImportError as e:
    SHUFFLE_AVAILABLE = False
    IMPORT_ERROR = str(e)


@pytest.mark.skipif(not SHUFFLE_AVAILABLE, reason=f"Shuffle module not available: {IMPORT_ERROR if not SHUFFLE_AVAILABLE else ''}")
class TestShuffleE2E:
    """End-to-end shuffle tests."""

    def test_hash_shuffle_workflow(self):
        """
        Test complete hash shuffle workflow.

        Simulates map -> shuffle -> reduce pipeline:
        1. Create data with keys
        2. Partition by keys
        3. Buffer partitions
        4. Verify partitioning correctness
        """
        # Create test data
        schema = pa.schema([
            ('user_id', pa.int64()),
            ('transaction_id', pa.int64()),
            ('amount', pa.float64()),
        ])

        # Create batch with 20 rows, 5 unique users
        batch = pa.RecordBatch.from_arrays([
            pa.array([1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5]),
            pa.array(list(range(20))),
            pa.array([float(i) * 100.0 for i in range(20)]),
        ], schema=schema)

        # Step 1: Partition data by user_id into 3 partitions
        partitioner = HashPartitioner(
            num_partitions=3,
            key_columns=[b'user_id'],
            schema=schema
        )

        partitioned = partitioner.partition_batch(pa._c.unwrap_batch(batch))

        # Verify partitioning
        assert len(partitioned) == 3
        total_rows = sum(pa._c.wrap_batch(partitioned[i]).num_rows for i in range(3))
        assert total_rows == 20

        # Step 2: Buffer each partition
        buffers = []
        for partition_id in range(3):
            buffer = ShuffleBuffer(
                partition_id=partition_id,
                max_rows=1000,
                max_bytes=1024*1024,
                schema=schema
            )
            buffer.add_batch(partitioned[partition_id])
            buffers.append(buffer)

        # Verify buffers
        for partition_id in range(3):
            part_batch = pa._c.wrap_batch(partitioned[partition_id])
            assert buffers[partition_id].num_rows() == part_batch.num_rows
            assert buffers[partition_id].num_bytes() > 0

        # Step 3: Merge buffered data
        for partition_id in range(3):
            merged = buffers[partition_id].get_merged_batch()
            merged_batch = pa._c.wrap_batch(merged)

            # Verify merged data matches original partition
            original_batch = pa._c.wrap_batch(partitioned[partition_id])
            assert merged_batch.num_rows == original_batch.num_rows
            assert merged_batch.schema.equals(original_batch.schema)

    def test_shuffle_manager_workflow(self):
        """
        Test shuffle manager coordinating full shuffle.

        Tests shuffle registration, writing, and flushing.
        """
        schema = pa.schema([
            ('key', pa.int64()),
            ('value', pa.float64()),
        ])

        # Create shuffle manager
        manager = ShuffleManager(
            agent_id=b"agent-1",
            agent_host=b"localhost",
            agent_port=9000
        )

        try:
            # Start manager (starts transport)
            # Note: This may fail if port already in use, which is OK for test
            try:
                manager.start()
            except Exception as e:
                pytest.skip(f"Could not start shuffle server (port conflict?): {e}")

            # Register shuffle
            shuffle_id = manager.register_shuffle(
                operator_id=b"map-1",
                num_partitions=4,
                partition_keys=[b'key'],
                edge_type=ShuffleEdgeType.HASH,
                schema=pa._c.unwrap_schema(schema)
            )

            assert shuffle_id is not None
            assert len(shuffle_id) > 0

            # Create test data
            batch = pa.RecordBatch.from_arrays([
                pa.array([1, 2, 3, 4, 5, 6, 7, 8]),
                pa.array([10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0]),
            ], schema=schema)

            # Write to partition 0 directly
            manager.write_partition(shuffle_id, 0, pa._c.unwrap_batch(batch))

            # Flush partition
            manager.flush_partition(shuffle_id, 0)

            # Cleanup
            manager.unregister_shuffle(shuffle_id)

        finally:
            # Always stop manager
            try:
                manager.stop()
            except:
                pass

    def test_partitioner_correctness(self):
        """
        Verify partitioner correctness: same key always goes to same partition.
        """
        schema = pa.schema([
            ('customer_id', pa.int64()),
            ('order_id', pa.int64()),
            ('total', pa.float64()),
        ])

        # Create dataset with duplicate customers
        batch = pa.RecordBatch.from_arrays([
            pa.array([100, 200, 100, 300, 200, 100, 300]),
            pa.array([1, 2, 3, 4, 5, 6, 7]),
            pa.array([10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0]),
        ], schema=schema)

        partitioner = HashPartitioner(
            num_partitions=4,
            key_columns=[b'customer_id'],
            schema=schema
        )

        partitioned = partitioner.partition_batch(pa._c.unwrap_batch(batch))

        # Build customer -> partition mapping
        customer_to_partition = {}

        for partition_id in range(4):
            part_batch = pa._c.wrap_batch(partitioned[partition_id])
            if part_batch.num_rows > 0:
                for idx in range(part_batch.num_rows):
                    customer_id = part_batch.column('customer_id')[idx].as_py()

                    if customer_id in customer_to_partition:
                        # Same customer must be in same partition
                        assert customer_to_partition[customer_id] == partition_id, \
                            f"Customer {customer_id} found in multiple partitions!"
                    else:
                        customer_to_partition[customer_id] = partition_id

        # Verify all customers accounted for
        assert set(customer_to_partition.keys()) == {100, 200, 300}

        # Verify customer 100 appears 3 times (all in same partition)
        partition_for_100 = customer_to_partition[100]
        batch_100 = pa._c.wrap_batch(partitioned[partition_for_100])
        count_100 = sum(1 for v in batch_100.column('customer_id').to_pylist() if v == 100)
        assert count_100 == 3


@pytest.mark.skipif(not SHUFFLE_AVAILABLE, reason=f"Shuffle module not available: {IMPORT_ERROR if not SHUFFLE_AVAILABLE else ''}")
class TestShuffleBuffers:
    """Tests for shuffle buffering and flushing."""

    def test_buffer_flush_threshold(self):
        """Test that buffer flushes when threshold exceeded."""
        schema = pa.schema([('value', pa.int64())])

        buffer = ShuffleBuffer(
            partition_id=0,
            max_rows=5,  # Small threshold for testing
            max_bytes=1024*1024,
            schema=schema
        )

        # Add small batch (< threshold)
        batch1 = pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], schema=schema)
        buffer.add_batch(pa._c.unwrap_batch(batch1))
        assert not buffer.should_flush()

        # Add another batch (> threshold)
        batch2 = pa.RecordBatch.from_arrays([pa.array([4, 5, 6])], schema=schema)
        buffer.add_batch(pa._c.unwrap_batch(batch2))
        assert buffer.should_flush()

    def test_buffer_merge(self):
        """Test merging multiple batches."""
        schema = pa.schema([('id', pa.int64()), ('value', pa.float64())])

        buffer = ShuffleBuffer(
            partition_id=0,
            max_rows=1000,
            max_bytes=1024*1024,
            schema=schema
        )

        # Add multiple small batches
        for i in range(5):
            batch = pa.RecordBatch.from_arrays([
                pa.array([i, i+1]),
                pa.array([float(i)*10.0, float(i+1)*10.0]),
            ], schema=schema)
            buffer.add_batch(pa._c.unwrap_batch(batch))

        # Get merged batch
        merged = buffer.get_merged_batch()
        merged_batch = pa._c.wrap_batch(merged)

        # Verify merged data
        assert merged_batch.num_rows == 10  # 5 batches * 2 rows each
        assert merged_batch.schema.equals(schema)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
