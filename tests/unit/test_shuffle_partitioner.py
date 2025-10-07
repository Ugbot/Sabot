#!/usr/bin/env python3
"""
Unit tests for shuffle partitioner.

Tests hash/range/round-robin partitioning with Arrow RecordBatches.
"""

import pytest
# Use Sabot's vendored Arrow (NOT pip pyarrow)
from sabot import cyarrow as pa
try:
    from sabot.cyarrow import compute as pc
except (ImportError, AttributeError):
    pc = None

# Import shuffle components
try:
    from sabot._cython.shuffle.partitioner import (
        HashPartitioner,
        RangePartitioner,
        RoundRobinPartitioner,
        create_partitioner,
    )
    from sabot._cython.shuffle.types import ShuffleEdgeType  # Python enum wrapper
    SHUFFLE_AVAILABLE = True
except ImportError as e:
    SHUFFLE_AVAILABLE = False
    IMPORT_ERROR = str(e)


@pytest.mark.skipif(not SHUFFLE_AVAILABLE, reason=f"Shuffle module not available: {IMPORT_ERROR if not SHUFFLE_AVAILABLE else ''}")
class TestHashPartitioner:
    """Tests for hash-based partitioning."""

    def test_hash_partitioner_single_key(self):
        """Test hash partitioning with single key column."""
        # Create test schema
        schema = pa.schema([
            ('user_id', pa.int64()),
            ('amount', pa.float64()),
        ])

        # Create test data
        batch = pa.RecordBatch.from_arrays([
            pa.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            pa.array([10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]),
        ], schema=schema)

        # Create partitioner (4 partitions, partition by user_id)
        partitioner = HashPartitioner(
            num_partitions=4,
            key_columns=[b'user_id'],
            schema=schema
        )

        # Partition the batch
        partitioned = partitioner.partition(batch)

        # Verify we got 4 batches
        assert len(partitioned) == 4

        # Verify total rows match
        total_rows = sum(partitioned[i].num_rows for i in range(4))
        assert total_rows == 10

        # Verify same user_id goes to same partition
        user_to_partition = {}
        for partition_id in range(4):
            part_batch = partitioned[partition_id]
            if part_batch.num_rows > 0:
                for user_id in part_batch.column('user_id').to_pylist():
                    if user_id in user_to_partition:
                        # Same user should always go to same partition
                        assert user_to_partition[user_id] == partition_id
                    else:
                        user_to_partition[user_id] = partition_id

    def test_hash_partitioner_multiple_keys(self):
        """Test hash partitioning with multiple key columns."""
        schema = pa.schema([
            ('user_id', pa.int64()),
            ('product_id', pa.int64()),
            ('amount', pa.float64()),
        ])

        batch = pa.RecordBatch.from_arrays([
            pa.array([1, 1, 2, 2, 3]),
            pa.array([10, 20, 10, 20, 10]),
            pa.array([100.0, 200.0, 300.0, 400.0, 500.0]),
        ], schema=schema)

        partitioner = HashPartitioner(
            num_partitions=3,
            key_columns=[b'user_id', b'product_id'],
            schema=schema
        )

        partitioned = partitioner.partition(batch)

        assert len(partitioned) == 3
        total_rows = sum(partitioned[i].num_rows for i in range(3))
        assert total_rows == 5


@pytest.mark.skipif(not SHUFFLE_AVAILABLE, reason=f"Shuffle module not available: {IMPORT_ERROR if not SHUFFLE_AVAILABLE else ''}")
class TestRoundRobinPartitioner:
    """Tests for round-robin partitioning."""

    def test_round_robin_partitioner(self):
        """Test round-robin distributes evenly."""
        schema = pa.schema([
            ('id', pa.int64()),
            ('value', pa.float64()),
        ])

        batch = pa.RecordBatch.from_arrays([
            pa.array(list(range(12))),
            pa.array([float(i) * 10.0 for i in range(12)]),
        ], schema=schema)

        partitioner = RoundRobinPartitioner(
            num_partitions=3,
            key_columns=[],  # No keys needed for RR
            schema=schema
        )

        partitioned = partitioner.partition(batch)

        # Verify we got 3 partitions
        assert len(partitioned) == 3

        # Verify even distribution (12 rows / 3 partitions = 4 rows each)
        for i in range(3):
            part_batch = partitioned[i]
            assert part_batch.num_rows == 4

        # Total rows preserved
        total_rows = sum(partitioned[i].num_rows for i in range(3))
        assert total_rows == 12


@pytest.mark.skipif(not SHUFFLE_AVAILABLE, reason=f"Shuffle module not available: {IMPORT_ERROR if not SHUFFLE_AVAILABLE else ''}")
class TestRangePartitioner:
    """Tests for range-based partitioning."""

    def test_range_partitioner(self):
        """Test range partitioning with boundaries."""
        schema = pa.schema([
            ('timestamp', pa.int64()),
            ('value', pa.float64()),
        ])

        batch = pa.RecordBatch.from_arrays([
            pa.array([50, 150, 250, 75, 175]),
            pa.array([1.0, 2.0, 3.0, 4.0, 5.0]),
        ], schema=schema)

        partitioner = RangePartitioner(
            num_partitions=3,
            key_columns=[b'timestamp'],
            schema=schema
        )

        # Set boundaries: [100, 200]
        # Partition 0: < 100
        # Partition 1: [100, 200)
        # Partition 2: >= 200
        partitioner.set_ranges([100, 200])

        partitioned = partitioner.partition(batch)

        assert len(partitioned) == 3

        # Verify partition 0: < 100 (should have 50, 75)
        part0 = partitioned[0]
        assert part0.num_rows == 2
        assert set(part0.column('timestamp').to_pylist()) == {50, 75}

        # Verify partition 1: [100, 200) (should have 150, 175)
        part1 = partitioned[1]
        assert part1.num_rows == 2
        assert set(part1.column('timestamp').to_pylist()) == {150, 175}

        # Verify partition 2: >= 200 (should have 250)
        part2 = partitioned[2]
        assert part2.num_rows == 1
        assert part2.column('timestamp').to_pylist() == [250]


@pytest.mark.skipif(not SHUFFLE_AVAILABLE, reason=f"Shuffle module not available: {IMPORT_ERROR if not SHUFFLE_AVAILABLE else ''}")
class TestPartitionerFactory:
    """Tests for partitioner factory function."""

    def test_create_hash_partitioner(self):
        """Test factory creates hash partitioner."""
        schema = pa.schema([('key', pa.int64())])

        partitioner = create_partitioner(
            ShuffleEdgeType.HASH,
            4,
            [b'key'],
            schema
        )

        assert isinstance(partitioner, HashPartitioner)
        assert partitioner.num_partitions == 4

    def test_create_rebalance_partitioner(self):
        """Test factory creates round-robin partitioner."""
        schema = pa.schema([('key', pa.int64())])

        partitioner = create_partitioner(
            ShuffleEdgeType.REBALANCE,
            3,
            [],
            schema
        )

        assert isinstance(partitioner, RoundRobinPartitioner)
        assert partitioner.num_partitions == 3

    def test_create_range_partitioner(self):
        """Test factory creates range partitioner."""
        schema = pa.schema([('key', pa.int64())])

        partitioner = create_partitioner(
            ShuffleEdgeType.RANGE,
            2,
            [b'key'],
            schema
        )

        assert isinstance(partitioner, RangePartitioner)
        assert partitioner.num_partitions == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
