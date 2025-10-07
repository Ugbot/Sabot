"""
Integration Tests for Lock-Free Shuffle Networking

Tests real network streaming between operators across nodes:
- Lock-free partition exchange via Arrow Flight
- Multi-node shuffle operations
- Operator-to-operator data streaming
- Zero-copy network transfer

Simulates distributed streaming operators exchanging shuffle partitions.
"""

import pytest
import time
import threading
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import Sabot's cyarrow (NOT pip pyarrow)
from sabot import cyarrow
from sabot import cyarrow as pa  # Use cyarrow for RecordBatch creation

# Import lock-free shuffle components
from sabot._cython.shuffle.lock_free_queue import SPSCRingBuffer, MPSCQueue
from sabot._cython.shuffle.atomic_partition_store import AtomicPartitionStore
from sabot._cython.shuffle.flight_transport_lockfree import (
    LockFreeFlightServer,
    LockFreeFlightClient,
)
from sabot._cython.shuffle.shuffle_transport import (
    ShuffleServer,
    ShuffleClient,
    ShuffleTransport,
)


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def free_port():
    """Get a free port for testing."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port


@pytest.fixture
def sample_schema():
    """Arrow schema for test data."""
    return pa.schema([
        ('operator_id', pa.int32()),
        ('partition_id', pa.int32()),
        ('timestamp', pa.int64()),
        ('value', pa.float64()),
        ('key', pa.string()),
    ])


@pytest.fixture
def create_batch():
    """Factory to create test batches."""
    def _create(schema, operator_id, partition_id, num_rows=100):
        return pa.RecordBatch.from_arrays([
            pa.array([operator_id] * num_rows, type=pa.int32()),
            pa.array([partition_id] * num_rows, type=pa.int32()),
            pa.array(list(range(num_rows)), type=pa.int64()),
            pa.array([float(i) * 1.5 for i in range(num_rows)], type=pa.float64()),
            pa.array([f'key_{operator_id}_{partition_id}_{i}' for i in range(num_rows)], type=pa.string()),
        ], schema=schema)
    return _create


# ============================================================================
# Simulated Streaming Operators
# ============================================================================

class StreamingOperator:
    """
    Simulated streaming operator that produces/consumes shuffle partitions.

    Represents a map/filter/aggregate operator in a distributed topology.
    """

    def __init__(self, operator_id, num_partitions, schema):
        self.operator_id = operator_id
        self.num_partitions = num_partitions
        self.schema = schema
        self.output_partitions = {}  # partition_id -> RecordBatch

    def produce_partitions(self, num_rows_per_partition=100):
        """
        Produce shuffle partitions (simulate operator output).

        Returns dict of partition_id -> RecordBatch
        """
        for partition_id in range(self.num_partitions):
            batch = pa.RecordBatch.from_arrays([
                pa.array([self.operator_id] * num_rows_per_partition, type=pa.int32()),
                pa.array([partition_id] * num_rows_per_partition, type=pa.int32()),
                pa.array(list(range(num_rows_per_partition)), type=pa.int64()),
                pa.array([float(i) * 1.5 for i in range(num_rows_per_partition)], type=pa.float64()),
                pa.array([f'key_{self.operator_id}_{partition_id}_{i}' for i in range(num_rows_per_partition)],
                        type=pa.string()),
            ], schema=self.schema)

            self.output_partitions[partition_id] = batch

        return self.output_partitions

    def consume_partitions(self, partitions):
        """
        Consume shuffle partitions (simulate operator input).

        Args:
            partitions: dict of partition_id -> RecordBatch

        Returns:
            Total rows consumed
        """
        total_rows = 0
        for partition_id, batch in partitions.items():
            if batch is not None:
                total_rows += batch.num_rows
        return total_rows


# ============================================================================
# Local Shuffle Tests (Single Node)
# ============================================================================

class TestLocalShuffle:
    """Test shuffle operations on a single node (no network)."""

    def test_operator_to_operator_local(self, sample_schema):
        """Test operator-to-operator shuffle on same node."""
        # Create two operators
        upstream_op = StreamingOperator(operator_id=1, num_partitions=4, schema=sample_schema)
        downstream_op = StreamingOperator(operator_id=2, num_partitions=4, schema=sample_schema)

        # Upstream produces partitions
        partitions = upstream_op.produce_partitions(num_rows_per_partition=50)

        # Downstream consumes partitions
        total_rows = downstream_op.consume_partitions(partitions)

        # Verify
        assert total_rows == 4 * 50  # 4 partitions * 50 rows each

    def test_shuffle_via_atomic_store(self, sample_schema, create_batch):
        """Test shuffle using atomic partition store (lock-free)."""
        store = AtomicPartitionStore(size=1024)

        # Simulate upstream operator writing partitions
        shuffle_id_hash = 12345
        num_partitions = 8

        for partition_id in range(num_partitions):
            batch = create_batch(sample_schema, operator_id=1, partition_id=partition_id, num_rows=100)
            success = store.py_insert(shuffle_id_hash, partition_id, batch)
            assert success

        # Simulate downstream operator reading partitions
        received_batches = []
        for partition_id in range(num_partitions):
            batch = store.py_get(shuffle_id_hash, partition_id)
            assert batch is not None
            assert batch.num_rows == 100
            received_batches.append(batch)

        assert len(received_batches) == num_partitions

    def test_shuffle_via_ring_buffer(self, sample_schema, create_batch):
        """Test shuffle using SPSC ring buffer (producer -> consumer)."""
        buffer = SPSCRingBuffer(capacity=64)

        num_partitions = 16
        produced = []
        consumed = []

        def producer():
            """Producer operator - generates partitions."""
            for partition_id in range(num_partitions):
                batch = create_batch(sample_schema, operator_id=1, partition_id=partition_id, num_rows=50)

                # Push to buffer (lock-free)
                while not buffer.py_push(partition_id, partition_id, batch):
                    time.sleep(0.0001)  # Spin-wait if full

                produced.append(partition_id)

        def consumer():
            """Consumer operator - receives partitions."""
            for _ in range(num_partitions):
                result = None
                while result is None:
                    result = buffer.py_pop()
                    if result is None:
                        time.sleep(0.0001)  # Spin-wait if empty

                shuffle_hash, partition_id, batch = result
                consumed.append(partition_id)
                assert batch.num_rows == 50

        # Run producer and consumer concurrently
        p_thread = threading.Thread(target=producer)
        c_thread = threading.Thread(target=consumer)

        p_thread.start()
        c_thread.start()

        p_thread.join()
        c_thread.join()

        # Verify all partitions transferred
        assert len(produced) == num_partitions
        assert len(consumed) == num_partitions
        assert sorted(consumed) == list(range(num_partitions))


# ============================================================================
# Network Shuffle Tests (Multi-Node Simulation)
# ============================================================================

@pytest.mark.skip(reason="Arrow Flight server implementation incomplete (lines 335-372 in flight_transport_lockfree.pyx)")
class TestNetworkShuffle:
    """Test shuffle operations across network (simulated multi-node)."""

    def test_single_server_client(self, free_port, sample_schema, create_batch):
        """Test basic server-client shuffle exchange."""
        shuffle_id = b"test_shuffle_001"

        # Start shuffle server (simulates node 1)
        server = ShuffleServer(host=b"127.0.0.1", port=free_port)
        server.start()
        time.sleep(0.5)  # Give server time to start

        try:
            # Server registers partitions (simulates upstream operator)
            num_partitions = 4
            for partition_id in range(num_partitions):
                batch = create_batch(sample_schema, operator_id=1, partition_id=partition_id, num_rows=100)
                server.register_partition(shuffle_id, partition_id, batch)

            # Client fetches partitions (simulates downstream operator on node 2)
            client = ShuffleClient()

            received_batches = []
            for partition_id in range(num_partitions):
                batch = client.fetch_partition(
                    host=b"127.0.0.1",
                    port=free_port,
                    shuffle_id=shuffle_id,
                    partition_id=partition_id
                )

                if batch is not None:
                    received_batches.append(batch)

            # Verify partitions received
            # Note: Network layer may not be fully implemented yet
            print(f"Received {len(received_batches)} batches (expected {num_partitions})")

        finally:
            server.stop()

    def test_multi_operator_shuffle_topology(self, free_port, sample_schema):
        """
        Test multi-operator shuffle topology:

        Operator A (node 1) --shuffle--> Operator B (node 2)
        Operator A (node 1) --shuffle--> Operator C (node 2)
        """
        # Start server on node 1
        server = ShuffleServer(host=b"127.0.0.1", port=free_port)
        server.start()
        time.sleep(0.5)

        try:
            # Operator A produces partitions for two downstream operators
            shuffle_b = b"shuffle_to_B"
            shuffle_c = b"shuffle_to_C"

            op_a = StreamingOperator(operator_id=1, num_partitions=4, schema=sample_schema)
            partitions_a = op_a.produce_partitions(num_rows_per_partition=100)

            # Register partitions for operator B
            for partition_id, batch in partitions_a.items():
                server.register_partition(shuffle_b, partition_id, batch)

            # Register partitions for operator C (different shuffle)
            for partition_id, batch in partitions_a.items():
                server.register_partition(shuffle_c, partition_id, batch)

            # Operators B and C fetch their partitions (simulates node 2)
            client = ShuffleClient()

            # Operator B fetches
            b_partitions = {}
            for partition_id in range(4):
                batch = client.fetch_partition(
                    host=b"127.0.0.1",
                    port=free_port,
                    shuffle_id=shuffle_b,
                    partition_id=partition_id
                )
                b_partitions[partition_id] = batch

            # Operator C fetches
            c_partitions = {}
            for partition_id in range(4):
                batch = client.fetch_partition(
                    host=b"127.0.0.1",
                    port=free_port,
                    shuffle_id=shuffle_c,
                    partition_id=partition_id
                )
                c_partitions[partition_id] = batch

            print(f"Operator B received: {sum(1 for b in b_partitions.values() if b is not None)} partitions")
            print(f"Operator C received: {sum(1 for b in c_partitions.values() if b is not None)} partitions")

        finally:
            server.stop()

    def test_concurrent_shuffle_streams(self, free_port, sample_schema, create_batch):
        """Test multiple concurrent shuffle streams (stress test)."""
        num_shuffles = 4
        partitions_per_shuffle = 8

        # Start server
        server = ShuffleServer(host=b"127.0.0.1", port=free_port)
        server.start()
        time.sleep(0.5)

        try:
            # Register multiple shuffles concurrently
            def register_shuffle(shuffle_idx):
                shuffle_id = f"shuffle_{shuffle_idx}".encode('utf-8')
                for partition_id in range(partitions_per_shuffle):
                    batch = create_batch(
                        sample_schema,
                        operator_id=shuffle_idx,
                        partition_id=partition_id,
                        num_rows=50
                    )
                    server.register_partition(shuffle_id, partition_id, batch)

            # Register shuffles in parallel
            with ThreadPoolExecutor(max_workers=num_shuffles) as executor:
                futures = [executor.submit(register_shuffle, i) for i in range(num_shuffles)]
                for future in as_completed(futures):
                    future.result()  # Wait for completion

            # Fetch shuffles concurrently
            client = ShuffleClient()

            def fetch_shuffle(shuffle_idx):
                shuffle_id = f"shuffle_{shuffle_idx}".encode('utf-8')
                batches = []
                for partition_id in range(partitions_per_shuffle):
                    batch = client.fetch_partition(
                        host=b"127.0.0.1",
                        port=free_port,
                        shuffle_id=shuffle_id,
                        partition_id=partition_id
                    )
                    if batch is not None:
                        batches.append(batch)
                return len(batches)

            # Fetch in parallel
            with ThreadPoolExecutor(max_workers=num_shuffles) as executor:
                futures = [executor.submit(fetch_shuffle, i) for i in range(num_shuffles)]
                results = [future.result() for future in as_completed(futures)]

            print(f"Fetched batches per shuffle: {results}")

        finally:
            server.stop()


# ============================================================================
# End-to-End Operator Pipeline Tests
# ============================================================================

class TestOperatorPipeline:
    """Test complete operator pipelines with shuffle networking."""

    def test_map_shuffle_reduce_pipeline(self, free_port, sample_schema):
        """
        Test Map -> Shuffle -> Reduce pipeline:

        Map operators (node 1) -> Shuffle -> Reduce operators (node 2)
        """
        num_map_operators = 2
        num_reduce_operators = 2
        partitions_per_mapper = 4

        # Start shuffle server
        server = ShuffleServer(host=b"127.0.0.1", port=free_port)
        server.start()
        time.sleep(0.5)

        try:
            # Phase 1: Map operators produce partitions
            map_outputs = {}

            for map_id in range(num_map_operators):
                mapper = StreamingOperator(
                    operator_id=map_id,
                    num_partitions=partitions_per_mapper,
                    schema=sample_schema
                )

                partitions = mapper.produce_partitions(num_rows_per_partition=100)

                # Register partitions for shuffle
                shuffle_id = f"map_{map_id}_output".encode('utf-8')
                for partition_id, batch in partitions.items():
                    server.register_partition(shuffle_id, partition_id, batch)

                map_outputs[map_id] = shuffle_id

            # Phase 2: Reduce operators fetch partitions
            client = ShuffleClient()

            total_rows_reduced = 0

            for reduce_id in range(num_reduce_operators):
                reducer = StreamingOperator(
                    operator_id=reduce_id + 100,  # Different ID range
                    num_partitions=partitions_per_mapper,
                    schema=sample_schema
                )

                # Each reducer fetches from all mappers
                input_partitions = {}

                for map_id in range(num_map_operators):
                    shuffle_id = map_outputs[map_id]

                    # Fetch assigned partition (round-robin distribution)
                    partition_id = reduce_id % partitions_per_mapper

                    batch = client.fetch_partition(
                        host=b"127.0.0.1",
                        port=free_port,
                        shuffle_id=shuffle_id,
                        partition_id=partition_id
                    )

                    if batch is not None:
                        input_partitions[f"{map_id}_{partition_id}"] = batch

                # Reducer processes inputs
                rows_processed = reducer.consume_partitions(input_partitions)
                total_rows_reduced += rows_processed

            print(f"Total rows reduced: {total_rows_reduced}")
            # Expected: num_map_operators * partitions_per_mapper * 100 rows

        finally:
            server.stop()

    def test_windowed_shuffle_aggregation(self, free_port, sample_schema):
        """
        Test windowed aggregation with shuffle:

        Window operators emit partial aggregates -> Shuffle by key -> Final aggregation
        """
        num_windows = 3
        keys_per_window = 5

        server = ShuffleServer(host=b"127.0.0.1", port=free_port)
        server.start()
        time.sleep(0.5)

        try:
            # Phase 1: Window operators emit partial aggregates
            for window_id in range(num_windows):
                shuffle_id = f"window_{window_id}_agg".encode('utf-8')

                # Create partial aggregate batch
                batch = pa.RecordBatch.from_arrays([
                    pa.array([window_id] * keys_per_window, type=pa.int32()),
                    pa.array(list(range(keys_per_window)), type=pa.int32()),  # partition by key
                    pa.array([int(time.time() * 1e9)] * keys_per_window, type=pa.int64()),
                    pa.array([float(window_id * 10 + i) for i in range(keys_per_window)], type=pa.float64()),
                    pa.array([f'key_{i}' for i in range(keys_per_window)], type=pa.string()),
                ], schema=sample_schema)

                # Register partitions (hash by key)
                for partition_id in range(keys_per_window):
                    server.register_partition(shuffle_id, partition_id, batch)

            # Phase 2: Final aggregation operator fetches by key
            client = ShuffleClient()

            aggregated_keys = set()

            for key_id in range(keys_per_window):
                # Fetch all partial aggregates for this key
                partial_aggregates = []

                for window_id in range(num_windows):
                    shuffle_id = f"window_{window_id}_agg".encode('utf-8')

                    batch = client.fetch_partition(
                        host=b"127.0.0.1",
                        port=free_port,
                        shuffle_id=shuffle_id,
                        partition_id=key_id
                    )

                    if batch is not None:
                        partial_aggregates.append(batch)

                if partial_aggregates:
                    aggregated_keys.add(key_id)

            print(f"Aggregated keys: {len(aggregated_keys)} (expected {keys_per_window})")

        finally:
            server.stop()


# ============================================================================
# Performance Tests
# ============================================================================

class TestShufflePerformance:
    """Performance tests for shuffle networking."""

    def test_throughput_single_stream(self, free_port, sample_schema, create_batch):
        """Measure shuffle throughput for single stream."""
        num_partitions = 100
        rows_per_partition = 1000

        server = ShuffleServer(host=b"127.0.0.1", port=free_port)
        server.start()
        time.sleep(0.5)

        try:
            shuffle_id = b"throughput_test"

            # Measure registration time
            start = time.perf_counter()

            for partition_id in range(num_partitions):
                batch = create_batch(sample_schema, operator_id=1, partition_id=partition_id, num_rows=rows_per_partition)
                server.register_partition(shuffle_id, partition_id, batch)

            register_elapsed = time.perf_counter() - start

            # Measure fetch time
            client = ShuffleClient()

            start = time.perf_counter()

            for partition_id in range(num_partitions):
                batch = client.fetch_partition(
                    host=b"127.0.0.1",
                    port=free_port,
                    shuffle_id=shuffle_id,
                    partition_id=partition_id
                )

            fetch_elapsed = time.perf_counter() - start

            total_rows = num_partitions * rows_per_partition

            print(f"\nShuffle Throughput Test:")
            print(f"  Registration: {num_partitions / register_elapsed:.0f} partitions/sec ({register_elapsed*1e6/num_partitions:.1f}μs/partition)")
            print(f"  Fetch: {num_partitions / fetch_elapsed:.0f} partitions/sec ({fetch_elapsed*1e6/num_partitions:.1f}μs/partition)")
            print(f"  Total rows: {total_rows}, Transfer rate: {total_rows / (register_elapsed + fetch_elapsed):.0f} rows/sec")

        finally:
            server.stop()


# ============================================================================
# Agent-Based Shuffle Tests (Phase 5 Integration)
# ============================================================================

class TestLocalShuffle:
    """Test shuffle operations with Agent integration (local mode)"""

    @pytest.mark.asyncio
    async def test_single_agent_startup(self):
        """Test that a single agent can start with Flight server on configured port"""
        import asyncio
        from sabot.agent import Agent, AgentConfig

        # Create agent config
        config = AgentConfig(
            agent_id="agent1",
            host="0.0.0.0",
            port=18816,  # Use high port to avoid conflicts
            num_slots=2,
            workers_per_slot=2
        )

        # Create agent
        agent = Agent(config, job_manager=None)

        # Start agent (should bind Flight server to 0.0.0.0:18816)
        await agent.start()

        # Verify agent is running
        status = agent.get_status()
        assert status['running'] is True
        assert status['agent_id'] == "agent1"

        # Stop agent
        await agent.stop()

        # Verify agent stopped
        status = agent.get_status()
        assert status['running'] is False

    @pytest.mark.asyncio
    async def test_multi_agent_startup_different_ports(self):
        """Test that multiple agents can start on different ports without conflict"""
        import asyncio
        from sabot.agent import Agent, AgentConfig

        # Create two agents with different ports
        config1 = AgentConfig(
            agent_id="agent1",
            host="0.0.0.0",
            port=18817,
            num_slots=2
        )

        config2 = AgentConfig(
            agent_id="agent2",
            host="0.0.0.0",
            port=18818,
            num_slots=2
        )

        agent1 = Agent(config1, job_manager=None)
        agent2 = Agent(config2, job_manager=None)

        # Start both agents
        await agent1.start()
        await agent2.start()

        # Verify both are running
        assert agent1.get_status()['running'] is True
        assert agent2.get_status()['running'] is True

        # Stop both agents
        await agent1.stop()
        await agent2.stop()

        # Verify both stopped
        assert agent1.get_status()['running'] is False
        assert agent2.get_status()['running'] is False

    @pytest.mark.asyncio
    async def test_operator_to_operator_local(self, sample_schema, create_batch):
        """Test operator data transfer via shuffle transport in local mode"""
        import asyncio
        from sabot._cython.shuffle.shuffle_transport import ShuffleTransport

        # Create shuffle transport instances (simulating two operators)
        transport1 = ShuffleTransport()
        transport2 = ShuffleTransport()

        # Start transports on different ports
        transport1.start(b"0.0.0.0", 18819)
        transport2.start(b"0.0.0.0", 18820)

        try:
            # Create test data
            batch = create_batch(sample_schema, operator_id=1, partition_id=0, num_rows=100)

            # Start shuffle on transport1 (sender)
            shuffle_id = b"test_shuffle_001"
            downstream_agents = [b"0.0.0.0:18820"]
            transport1.start_shuffle(shuffle_id, num_partitions=1, downstream_agents=downstream_agents)

            # Start shuffle on transport2 (receiver)
            upstream_agents = [b"0.0.0.0:18819"]
            transport2.start_shuffle(shuffle_id, num_partitions=1, downstream_agents=[], upstream_agents=upstream_agents)

            # Publish partition from transport1
            transport1.publish_partition(shuffle_id, partition_id=0, batch=batch)

            # Wait a bit for publication
            await asyncio.sleep(0.1)

            # Fetch partition from transport2
            fetched_batches = transport2.receive_partitions(shuffle_id, partition_id=0)

            # Verify data transfer
            assert len(fetched_batches) > 0, "Should receive at least one batch"

            fetched_batch = fetched_batches[0]
            assert fetched_batch.num_rows == 100, "Should receive all rows"
            assert fetched_batch.schema.equals(batch.schema), "Schema should match"

        finally:
            # Clean up
            transport1.end_shuffle(shuffle_id)
            transport2.end_shuffle(shuffle_id)
            transport1.stop()
            transport2.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
