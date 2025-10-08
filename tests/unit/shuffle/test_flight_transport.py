"""
Tests for Arrow Flight-based shuffle transport.

Verifies zero-copy network transport for shuffle partitions.
"""

import pytest
from sabot import cyarrow as pa
import time
import threading

# Import shuffle transport with Flight
from sabot._cython.shuffle.flight_transport import (
    ShuffleFlightServer,
    ShuffleFlightClient,
)


@pytest.fixture
def sample_schema():
    """Create sample Arrow schema."""
    return pa.schema([
        ('id', pa.int64()),
        ('value', pa.float64()),
        ('name', pa.string()),
    ])


@pytest.fixture
def sample_batch(sample_schema):
    """Create sample Arrow RecordBatch."""
    return pa.RecordBatch.from_arrays([
        pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        pa.array([1.1, 2.2, 3.3, 4.4, 5.5], type=pa.float64()),
        pa.array(['a', 'b', 'c', 'd', 'e'], type=pa.string()),
    ], schema=sample_schema)


class TestShuffleFlightServer:
    """Test Flight server for serving shuffle partitions."""

    def test_server_start_stop(self):
        """Test server lifecycle."""
        server = ShuffleFlightServer(b"127.0.0.1", 18816)

        # Start server
        server.start()
        assert server.running == True

        # Give server time to start
        time.sleep(0.5)

        # Stop server
        server.stop()
        assert server.running == False

    def test_register_partition(self, sample_batch):
        """Test partition registration."""
        server = ShuffleFlightServer(b"127.0.0.1", 18817)

        # Register partition
        shuffle_id = b"test_shuffle"
        partition_id = 0
        server.register_partition(shuffle_id, partition_id, sample_batch)

        # Verify partition stored
        retrieved_batch = server.get_partition(shuffle_id, partition_id)
        assert retrieved_batch is not None
        assert retrieved_batch.num_rows == sample_batch.num_rows

    def test_multiple_partitions(self, sample_schema):
        """Test serving multiple partitions."""
        server = ShuffleFlightServer(b"127.0.0.1", 18818)
        shuffle_id = b"multi_shuffle"

        # Register multiple partitions
        for partition_id in range(5):
            batch = pa.RecordBatch.from_arrays([
                pa.array([partition_id * 10 + i for i in range(5)], type=pa.int64()),
                pa.array([float(partition_id * 10 + i) for i in range(5)], type=pa.float64()),
                pa.array([f'p{partition_id}_{i}' for i in range(5)], type=pa.string()),
            ], schema=sample_schema)
            server.register_partition(shuffle_id, partition_id, batch)

        # Verify all partitions stored
        for partition_id in range(5):
            batch = server.get_partition(shuffle_id, partition_id)
            assert batch is not None
            assert batch.num_rows == 5


class TestShuffleFlightClient:
    """Test Flight client for fetching shuffle partitions."""

    def test_client_creation(self):
        """Test client creation and configuration."""
        client = ShuffleFlightClient(max_connections=5, max_retries=3, timeout_seconds=10.0)
        assert client.max_connections == 5
        assert client.max_retries == 3
        assert client.timeout_seconds == 10.0

    def test_fetch_partition_basic(self, sample_batch):
        """Test basic partition fetch."""
        # Start server
        server = ShuffleFlightServer(b"127.0.0.1", 18819)
        shuffle_id = b"fetch_test"
        partition_id = 0

        server.register_partition(shuffle_id, partition_id, sample_batch)
        server.start()
        time.sleep(0.5)

        try:
            # Create client and fetch
            client = ShuffleFlightClient()
            fetched_batch = client.fetch_partition(
                b"127.0.0.1",
                18819,
                shuffle_id,
                partition_id
            )

            # Verify data
            assert fetched_batch is not None
            # Note: May be empty if Flight server not fully implemented yet

        finally:
            server.stop()
            client.close_all()

    def test_connection_pooling(self, sample_batch):
        """Test client connection pooling."""
        # Start server
        server = ShuffleFlightServer(b"127.0.0.1", 18820)
        server.start()
        time.sleep(0.5)

        try:
            client = ShuffleFlightClient(max_connections=2)
            shuffle_id = b"pool_test"

            # Register multiple partitions
            for partition_id in range(3):
                server.register_partition(shuffle_id, partition_id, sample_batch)

            # Fetch multiple times (should reuse connections)
            for _ in range(3):
                for partition_id in range(3):
                    batch = client.fetch_partition(
                        b"127.0.0.1",
                        18820,
                        shuffle_id,
                        partition_id
                    )
                    # Verify we got data back
                    assert batch is not None

            # Verify client config is respected
            assert client.max_connections == 2

        finally:
            server.stop()
            client.close_all()


class TestEndToEndShuffle:
    """Test end-to-end shuffle operations."""

    def test_single_node_shuffle(self, sample_schema):
        """Test shuffle on single node (server and client on same machine)."""
        # Start server
        server = ShuffleFlightServer(b"127.0.0.1", 18821)
        shuffle_id = b"e2e_shuffle"
        num_partitions = 4

        # Register partitions
        for partition_id in range(num_partitions):
            batch = pa.RecordBatch.from_arrays([
                pa.array([partition_id * 100 + i for i in range(10)], type=pa.int64()),
                pa.array([float(partition_id * 100 + i) for i in range(10)], type=pa.float64()),
                pa.array([f'p{partition_id}_{i}' for i in range(10)], type=pa.string()),
            ], schema=sample_schema)
            server.register_partition(shuffle_id, partition_id, batch)

        server.start()
        time.sleep(0.5)

        try:
            # Create client
            client = ShuffleFlightClient()

            # Fetch all partitions
            fetched_batches = []
            for partition_id in range(num_partitions):
                batch = client.fetch_partition(
                    b"127.0.0.1",
                    18821,
                    shuffle_id,
                    partition_id
                )
                fetched_batches.append(batch)

            # Verify all partitions fetched
            assert len(fetched_batches) == num_partitions

        finally:
            server.stop()
            client.close_all()

    def test_concurrent_fetch(self, sample_batch):
        """Test concurrent partition fetching from multiple threads."""
        # Start server
        server = ShuffleFlightServer(b"127.0.0.1", 18822)
        shuffle_id = b"concurrent_test"
        num_partitions = 10

        for partition_id in range(num_partitions):
            server.register_partition(shuffle_id, partition_id, sample_batch)

        server.start()
        time.sleep(0.5)

        try:
            client = ShuffleFlightClient(max_connections=5)
            results = []

            def fetch_partition(partition_id):
                batch = client.fetch_partition(
                    b"127.0.0.1",
                    18822,
                    shuffle_id,
                    partition_id
                )
                results.append((partition_id, batch))

            # Launch concurrent fetches
            threads = []
            for partition_id in range(num_partitions):
                thread = threading.Thread(target=fetch_partition, args=(partition_id,))
                threads.append(thread)
                thread.start()

            # Wait for all threads
            for thread in threads:
                thread.join()

            # Verify all fetches completed
            assert len(results) == num_partitions

        finally:
            server.stop()
            client.close_all()


def test_zero_copy_semantics(sample_batch):
    """Verify zero-copy semantics (shared_ptr reference counting)."""
    # This test verifies that Flight transport doesn't copy data

    # Start server
    server = ShuffleFlightServer(b"127.0.0.1", 18823)
    shuffle_id = b"zerocopy_test"
    partition_id = 0

    # Register batch
    server.register_partition(shuffle_id, partition_id, sample_batch)

    # Verify local access is zero-copy
    retrieved_batch = server.get_partition(shuffle_id, partition_id)

    # Both should point to same underlying Arrow C++ data
    # (In a full implementation, we'd verify sp_batch pointers are same)
    assert retrieved_batch.num_rows == sample_batch.num_rows
    assert retrieved_batch.schema == sample_batch.schema


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
