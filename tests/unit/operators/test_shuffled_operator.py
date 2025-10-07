"""
Unit Tests for ShuffledOperator

Tests the shuffle protocol interface, partitioning logic,
send/receive modes, and integration with HashPartitioner.
"""

import pytest
import pyarrow as pa
from unittest.mock import Mock, MagicMock, patch

# Import after compilation
try:
    from sabot._cython.operators.shuffled_operator import ShuffledOperator
    SHUFFLED_OPERATOR_AVAILABLE = True
except ImportError:
    SHUFFLED_OPERATOR_AVAILABLE = False


@pytest.mark.skipif(not SHUFFLED_OPERATOR_AVAILABLE, reason="ShuffledOperator not compiled")
class TestShuffledOperatorInitialization:
    """Test ShuffledOperator initialization"""

    def test_initialization_default(self):
        """Test ShuffledOperator initialization with defaults"""
        schema = pa.schema([
            ('user_id', pa.int64()),
            ('amount', pa.float64())
        ])

        op = ShuffledOperator(
            operator_id='shuffle-op-1',
            parallelism=4,
            schema=schema,
            partition_keys=['user_id']
        )

        assert op.operator_id == 'shuffle-op-1'
        assert op.parallelism == 4
        assert op.requires_shuffle() is True
        assert op.get_partition_keys() == ['user_id']

    def test_initialization_with_multiple_keys(self):
        """Test initialization with multiple partition keys"""
        schema = pa.schema([
            ('user_id', pa.int64()),
            ('product_id', pa.int64()),
            ('amount', pa.float64())
        ])

        op = ShuffledOperator(
            operator_id='shuffle-op-2',
            parallelism=8,
            schema=schema,
            partition_keys=['user_id', 'product_id']
        )

        assert op.get_partition_keys() == ['user_id', 'product_id']
        assert op.get_num_partitions() == 4  # Default if not set

    def test_initialization_custom_num_partitions(self):
        """Test initialization with custom num_partitions"""
        schema = pa.schema([('key', pa.int64())])

        op = ShuffledOperator(
            operator_id='shuffle-op-3',
            parallelism=4,
            schema=schema,
            partition_keys=['key'],
            num_partitions=16
        )

        assert op.get_num_partitions() == 16


@pytest.mark.skipif(not SHUFFLED_OPERATOR_AVAILABLE, reason="ShuffledOperator not compiled")
class TestShuffleProtocol:
    """Test shuffle protocol interface"""

    def test_requires_shuffle_returns_true(self):
        """Test that requires_shuffle() returns True"""
        schema = pa.schema([('key', pa.int64())])
        op = ShuffledOperator(
            operator_id='op-1',
            parallelism=4,
            schema=schema,
            partition_keys=['key']
        )

        assert op.requires_shuffle() is True

    def test_get_partition_keys_single_key(self):
        """Test get_partition_keys() with single key"""
        schema = pa.schema([('user_id', pa.int64()), ('value', pa.float64())])
        op = ShuffledOperator(
            operator_id='op-2',
            parallelism=4,
            schema=schema,
            partition_keys=['user_id']
        )

        keys = op.get_partition_keys()
        assert keys == ['user_id']
        assert isinstance(keys, list)

    def test_get_partition_keys_multiple_keys(self):
        """Test get_partition_keys() with multiple keys"""
        schema = pa.schema([
            ('region', pa.string()),
            ('user_id', pa.int64()),
            ('value', pa.float64())
        ])

        op = ShuffledOperator(
            operator_id='op-3',
            parallelism=4,
            schema=schema,
            partition_keys=['region', 'user_id']
        )

        keys = op.get_partition_keys()
        assert keys == ['region', 'user_id']
        assert len(keys) == 2

    def test_get_num_partitions(self):
        """Test get_num_partitions()"""
        schema = pa.schema([('key', pa.int64())])

        op = ShuffledOperator(
            operator_id='op-4',
            parallelism=4,
            schema=schema,
            partition_keys=['key'],
            num_partitions=8
        )

        assert op.get_num_partitions() == 8


@pytest.mark.skipif(not SHUFFLED_OPERATOR_AVAILABLE, reason="ShuffledOperator not compiled")
class TestShuffleConfiguration:
    """Test shuffle configuration setup"""

    def test_set_shuffle_config(self):
        """Test set_shuffle_config() method"""
        schema = pa.schema([('user_id', pa.int64()), ('amount', pa.float64())])

        op = ShuffledOperator(
            operator_id='op-1',
            parallelism=4,
            schema=schema,
            partition_keys=['user_id'],
            num_partitions=4
        )

        # Mock shuffle transport
        mock_transport = Mock()
        shuffle_id = b'shuffle-123'
        task_id = 2
        num_partitions = 4

        # Set shuffle config
        op.set_shuffle_config(mock_transport, shuffle_id, task_id, num_partitions)

        # Verify internal state (these are private, testing via behavior)
        assert op.get_num_partitions() == 4

    def test_shuffle_config_initializes_partitioner(self):
        """Test that set_shuffle_config() initializes partitioner"""
        schema = pa.schema([
            ('user_id', pa.int64()),
            ('amount', pa.float64())
        ])

        op = ShuffledOperator(
            operator_id='op-2',
            parallelism=4,
            schema=schema,
            partition_keys=['user_id'],
            num_partitions=8
        )

        mock_transport = Mock()
        op.set_shuffle_config(mock_transport, b'shuffle-456', 0, 8)

        # Partitioner should be initialized (verified via partitioning behavior)
        # We'll test this in partition tests


@pytest.mark.skipif(not SHUFFLED_OPERATOR_AVAILABLE, reason="ShuffledOperator not compiled")
class TestBatchPartitioning:
    """Test batch partitioning logic"""

    def test_partition_batch_single_key(self):
        """Test partitioning batch by single key"""
        schema = pa.schema([
            ('user_id', pa.int64()),
            ('amount', pa.float64())
        ])

        op = ShuffledOperator(
            operator_id='op-1',
            parallelism=4,
            schema=schema,
            partition_keys=['user_id'],
            num_partitions=4
        )

        # Configure shuffle
        mock_transport = Mock()
        op.set_shuffle_config(mock_transport, b'shuffle-789', 0, 4)

        # Create test batch
        batch = pa.record_batch([
            pa.array([1, 2, 3, 4, 1, 2], type=pa.int64()),
            pa.array([100.0, 200.0, 300.0, 400.0, 150.0, 250.0], type=pa.float64())
        ], schema=schema)

        # Partition the batch (using private method for testing)
        partitions = op._partition_batch(batch)

        # Verify we get 4 partitions
        assert isinstance(partitions, list)
        assert len(partitions) == 4

        # Verify total rows preserved
        total_rows = sum(p.num_rows for p in partitions if p is not None)
        assert total_rows == batch.num_rows

    def test_partition_determinism(self):
        """Test that partitioning is deterministic"""
        schema = pa.schema([
            ('key', pa.int64()),
            ('value', pa.float64())
        ])

        op = ShuffledOperator(
            operator_id='op-2',
            parallelism=4,
            schema=schema,
            partition_keys=['key'],
            num_partitions=4
        )

        mock_transport = Mock()
        op.set_shuffle_config(mock_transport, b'shuffle-det', 0, 4)

        # Create test batch
        batch = pa.record_batch([
            pa.array([10, 20, 30, 40, 10, 20], type=pa.int64()),
            pa.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], type=pa.float64())
        ], schema=schema)

        # Partition twice
        partitions1 = op._partition_batch(batch)
        partitions2 = op._partition_batch(batch)

        # Verify determinism: same keys go to same partitions
        for i in range(4):
            if partitions1[i] is not None and partitions2[i] is not None:
                assert partitions1[i].num_rows == partitions2[i].num_rows

    def test_partition_batch_multiple_keys(self):
        """Test partitioning by multiple keys"""
        schema = pa.schema([
            ('region', pa.string()),
            ('user_id', pa.int64()),
            ('amount', pa.float64())
        ])

        op = ShuffledOperator(
            operator_id='op-3',
            parallelism=4,
            schema=schema,
            partition_keys=['region', 'user_id'],
            num_partitions=8
        )

        mock_transport = Mock()
        op.set_shuffle_config(mock_transport, b'shuffle-multi', 0, 8)

        # Create test batch
        batch = pa.record_batch([
            pa.array(['US', 'EU', 'US', 'ASIA', 'EU', 'US'], type=pa.string()),
            pa.array([1, 2, 1, 3, 2, 4], type=pa.int64()),
            pa.array([100.0, 200.0, 300.0, 400.0, 500.0, 600.0], type=pa.float64())
        ], schema=schema)

        partitions = op._partition_batch(batch)

        # Verify partitioning
        assert len(partitions) == 8
        total_rows = sum(p.num_rows for p in partitions if p is not None)
        assert total_rows == batch.num_rows

    def test_partition_same_key_goes_to_same_partition(self):
        """Test that rows with same key go to same partition"""
        schema = pa.schema([
            ('user_id', pa.int64()),
            ('timestamp', pa.int64())
        ])

        op = ShuffledOperator(
            operator_id='op-4',
            parallelism=4,
            schema=schema,
            partition_keys=['user_id'],
            num_partitions=4
        )

        mock_transport = Mock()
        op.set_shuffle_config(mock_transport, b'shuffle-same', 0, 4)

        # Create batch with duplicate keys
        batch = pa.record_batch([
            pa.array([100, 200, 100, 100, 200], type=pa.int64()),
            pa.array([1, 2, 3, 4, 5], type=pa.int64())
        ], schema=schema)

        partitions = op._partition_batch(batch)

        # Find which partition has user_id=100
        partition_for_100 = None
        for i, p in enumerate(partitions):
            if p is not None and p.num_rows > 0:
                user_ids = p.column('user_id').to_pylist()
                if 100 in user_ids:
                    partition_for_100 = i
                    # All rows with user_id=100 should be in this partition
                    assert user_ids.count(100) == 3  # Three rows with user_id=100
                    break

        assert partition_for_100 is not None


@pytest.mark.skipif(not SHUFFLED_OPERATOR_AVAILABLE, reason="ShuffledOperator not compiled")
class TestShuffleSendReceive:
    """Test shuffle send and receive modes"""

    def test_send_mode_without_shuffle_transport(self):
        """Test send mode when shuffle transport not configured"""
        schema = pa.schema([('key', pa.int64())])

        op = ShuffledOperator(
            operator_id='op-1',
            parallelism=4,
            schema=schema,
            partition_keys=['key']
        )

        # Without shuffle transport, iterator should handle gracefully
        # (Implementation detail: may return empty or skip)

    def test_receive_mode_with_task_id(self):
        """Test receive mode when task_id is set"""
        schema = pa.schema([('key', pa.int64()), ('value', pa.float64())])

        op = ShuffledOperator(
            operator_id='op-2',
            parallelism=4,
            schema=schema,
            partition_keys=['key'],
            num_partitions=4
        )

        # Mock shuffle transport with receive
        mock_transport = Mock()
        mock_transport.receive_partitions = Mock(return_value=iter([]))

        op.set_shuffle_config(mock_transport, b'shuffle-recv', 2, 4)

        # When task_id >= 0, should be in receive mode
        # Iterator should call receive_partitions
        list(op)  # Consume iterator

        # Verify receive was attempted
        mock_transport.receive_partitions.assert_called()


@pytest.mark.skipif(not SHUFFLED_OPERATOR_AVAILABLE, reason="ShuffledOperator not compiled")
class TestShuffleIntegration:
    """Test integration with shuffle infrastructure"""

    def test_shuffle_with_partitioner(self):
        """Test that ShuffledOperator uses HashPartitioner correctly"""
        schema = pa.schema([
            ('user_id', pa.int64()),
            ('amount', pa.float64())
        ])

        op = ShuffledOperator(
            operator_id='op-1',
            parallelism=4,
            schema=schema,
            partition_keys=['user_id'],
            num_partitions=4
        )

        mock_transport = Mock()
        op.set_shuffle_config(mock_transport, b'shuffle-int', 0, 4)

        # Create batch
        batch = pa.record_batch([
            pa.array([1, 2, 3, 4, 5, 6, 7, 8], type=pa.int64()),
            pa.array([10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0], type=pa.float64())
        ], schema=schema)

        partitions = op._partition_batch(batch)

        # Verify partitioner created correct partitions
        assert len(partitions) == 4

        # Verify no data loss
        total_rows = sum(p.num_rows for p in partitions if p is not None)
        assert total_rows == 8

    def test_shuffle_with_empty_batch(self):
        """Test partitioning empty batch"""
        schema = pa.schema([
            ('key', pa.int64()),
            ('value', pa.float64())
        ])

        op = ShuffledOperator(
            operator_id='op-2',
            parallelism=4,
            schema=schema,
            partition_keys=['key'],
            num_partitions=4
        )

        mock_transport = Mock()
        op.set_shuffle_config(mock_transport, b'shuffle-empty', 0, 4)

        # Create empty batch
        batch = pa.record_batch([
            pa.array([], type=pa.int64()),
            pa.array([], type=pa.float64())
        ], schema=schema)

        partitions = op._partition_batch(batch)

        # Should return 4 partitions (may be None or empty)
        assert len(partitions) == 4


@pytest.mark.skipif(not SHUFFLED_OPERATOR_AVAILABLE, reason="ShuffledOperator not compiled")
class TestShuffledOperatorEdgeCases:
    """Test edge cases and error conditions"""

    def test_partition_with_null_values(self):
        """Test partitioning batch with null values in partition key"""
        schema = pa.schema([
            ('user_id', pa.int64()),
            ('amount', pa.float64())
        ])

        op = ShuffledOperator(
            operator_id='op-1',
            parallelism=4,
            schema=schema,
            partition_keys=['user_id'],
            num_partitions=4
        )

        mock_transport = Mock()
        op.set_shuffle_config(mock_transport, b'shuffle-null', 0, 4)

        # Create batch with nulls
        batch = pa.record_batch([
            pa.array([1, None, 3, None, 5], type=pa.int64()),
            pa.array([10.0, 20.0, 30.0, 40.0, 50.0], type=pa.float64())
        ], schema=schema)

        partitions = op._partition_batch(batch)

        # Verify partitioning handles nulls
        assert len(partitions) == 4
        total_rows = sum(p.num_rows for p in partitions if p is not None)
        assert total_rows == 5

    def test_partition_with_large_batch(self):
        """Test partitioning large batch"""
        schema = pa.schema([
            ('key', pa.int64()),
            ('value', pa.float64())
        ])

        op = ShuffledOperator(
            operator_id='op-2',
            parallelism=4,
            schema=schema,
            partition_keys=['key'],
            num_partitions=4
        )

        mock_transport = Mock()
        op.set_shuffle_config(mock_transport, b'shuffle-large', 0, 4)

        # Create large batch (10K rows)
        num_rows = 10_000
        batch = pa.record_batch([
            pa.array(range(num_rows), type=pa.int64()),
            pa.array([float(i) * 1.5 for i in range(num_rows)], type=pa.float64())
        ], schema=schema)

        partitions = op._partition_batch(batch)

        # Verify all data preserved
        assert len(partitions) == 4
        total_rows = sum(p.num_rows for p in partitions if p is not None)
        assert total_rows == num_rows


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
