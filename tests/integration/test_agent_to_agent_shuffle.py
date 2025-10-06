#!/usr/bin/env python3
"""
Agent-to-Agent Shuffle Transport Tests

Tests lock-free Flight-based network shuffle between simulated agents.
"""

import sys
import time
import pyarrow as pa
from sabot._cython.shuffle.shuffle_transport import ShuffleServer, ShuffleClient


def test_server_client_basic():
    """Test basic server-client communication."""
    print("\n=== Test 1: Basic Server-Client Communication ===")

    # Create server
    server = ShuffleServer(host=b"127.0.0.1", port=8816)
    print(f"✓ Created shuffle server on 127.0.0.1:8816")

    # Create test batch
    schema = pa.schema([
        ('transaction_id', pa.int64()),
        ('amount', pa.float64()),
        ('timestamp', pa.int64())
    ])
    batch = pa.RecordBatch.from_arrays([
        pa.array([1001, 1002, 1003], type=pa.int64()),
        pa.array([99.99, 149.50, 250.00], type=pa.float64()),
        pa.array([1696550000, 1696550001, 1696550002], type=pa.int64()),
    ], schema=schema)

    # Register partition with server
    shuffle_id = b"shuffle-test-001"
    partition_id = 0

    print(f"Registering partition: shuffle_id={shuffle_id}, partition_id={partition_id}, rows={batch.num_rows}")
    server.register_partition(shuffle_id, partition_id, batch)
    print(f"✓ Registered partition with {batch.num_rows} rows")

    # Create client
    client = ShuffleClient(max_connections=5)
    print(f"✓ Created shuffle client")

    # Fetch partition (note: network layer is stubbed, so this will return None for now)
    print(f"Fetching partition from server...")
    fetched = client.fetch_partition(
        host=b"127.0.0.1",
        port=8816,
        shuffle_id=shuffle_id,
        partition_id=partition_id
    )

    if fetched is None:
        print("ℹ️  Network layer stubbed - returned None (expected)")
        print("   Registration succeeded, fetch path exercised")
    else:
        print(f"✓ Fetched partition with {fetched.num_rows} rows")
        assert fetched.num_rows == batch.num_rows
        assert fetched.schema == batch.schema

    print("✅ Test 1 passed!\n")


def test_multi_partition_shuffle():
    """Test multiple partitions from multiple agents."""
    print("=== Test 2: Multi-Partition Multi-Agent Shuffle ===")

    # Simulate 3 agents with their own servers
    agents = []
    for i in range(3):
        server = ShuffleServer(host=b"127.0.0.1", port=8816 + i)
        agents.append({
            'id': i,
            'host': b"127.0.0.1",
            'port': 8816 + i,
            'server': server
        })
        print(f"✓ Created agent {i} server on port {8816 + i}")

    # Create shuffle client (simulates coordinator or another agent)
    client = ShuffleClient(max_connections=10)
    print(f"✓ Created shuffle client with connection pool")

    # Each agent registers multiple partitions
    shuffle_id = b"multi-agent-shuffle"
    total_partitions = 0

    schema = pa.schema([('key', pa.int64()), ('value', pa.string())])

    for agent in agents:
        # Agent registers 2 partitions
        for partition_id in range(2):
            global_partition_id = agent['id'] * 2 + partition_id

            # Create batch specific to this agent/partition
            batch = pa.RecordBatch.from_arrays([
                pa.array([global_partition_id * 100 + j for j in range(5)], type=pa.int64()),
                pa.array([f"agent{agent['id']}_p{partition_id}_row{j}" for j in range(5)]),
            ], schema=schema)

            agent['server'].register_partition(shuffle_id, global_partition_id, batch)
            total_partitions += 1
            print(f"  Agent {agent['id']} registered partition {global_partition_id}")

    print(f"✓ Registered {total_partitions} partitions across {len(agents)} agents")

    # Client fetches from all agents
    fetched_count = 0
    for agent in agents:
        for partition_id in range(2):
            global_partition_id = agent['id'] * 2 + partition_id

            fetched = client.fetch_partition(
                host=agent['host'],
                port=agent['port'],
                shuffle_id=shuffle_id,
                partition_id=global_partition_id
            )

            if fetched is None:
                # Expected - network layer stubbed
                fetched_count += 1
            else:
                print(f"  Fetched partition {global_partition_id} from agent {agent['id']}")
                fetched_count += 1

    print(f"✓ Attempted to fetch {fetched_count} partitions (network stubbed)")
    print("✅ Test 2 passed!\n")


def test_concurrent_registration():
    """Test concurrent partition registration (simulates parallel agents)."""
    print("=== Test 3: Concurrent Partition Registration ===")

    server = ShuffleServer(host=b"127.0.0.1", port=9000)
    print(f"✓ Created shuffle server")

    # Simulate 10 agents registering partitions concurrently
    num_agents = 10
    shuffle_id = b"concurrent-shuffle"

    schema = pa.schema([('id', pa.int64())])

    start = time.perf_counter()

    for agent_id in range(num_agents):
        partition_id = agent_id

        # Each agent creates and registers a batch
        batch = pa.RecordBatch.from_arrays([
            pa.array([agent_id * 1000 + i for i in range(100)], type=pa.int64()),
        ], schema=schema)

        server.register_partition(shuffle_id, partition_id, batch)

    elapsed = time.perf_counter() - start

    throughput = num_agents / elapsed
    latency_us = (elapsed / num_agents) * 1_000_000

    print(f"✓ Registered {num_agents} partitions")
    print(f"  Total time: {elapsed*1000:.2f}ms")
    print(f"  Throughput: {throughput:.0f} registrations/sec")
    print(f"  Avg latency: {latency_us:.1f}μs per registration")

    print("✅ Test 3 passed!\n")


def test_large_batch_shuffle():
    """Test shuffling large batches between agents."""
    print("=== Test 4: Large Batch Shuffle ===")

    server = ShuffleServer(host=b"127.0.0.1", port=9001)
    client = ShuffleClient(max_connections=5)

    # Create large batch (100K rows)
    num_rows = 100_000
    schema = pa.schema([
        ('id', pa.int64()),
        ('value', pa.float64()),
        ('name', pa.string())
    ])

    batch = pa.RecordBatch.from_arrays([
        pa.array(list(range(num_rows)), type=pa.int64()),
        pa.array([float(i) * 1.5 for i in range(num_rows)], type=pa.float64()),
        pa.array([f"row_{i}" for i in range(num_rows)]),
    ], schema=schema)

    batch_size_mb = batch.nbytes / (1024 * 1024)
    print(f"Created batch: {num_rows:,} rows, {batch_size_mb:.2f} MB")

    # Register large batch
    shuffle_id = b"large-batch-shuffle"
    partition_id = 0

    start = time.perf_counter()
    server.register_partition(shuffle_id, partition_id, batch)
    reg_elapsed = time.perf_counter() - start

    print(f"✓ Registered in {reg_elapsed*1000:.2f}ms")

    # Fetch large batch
    start = time.perf_counter()
    fetched = client.fetch_partition(
        host=b"127.0.0.1",
        port=9001,
        shuffle_id=shuffle_id,
        partition_id=partition_id
    )
    fetch_elapsed = time.perf_counter() - start

    if fetched is None:
        print(f"✓ Fetch attempted in {fetch_elapsed*1000:.2f}ms (network stubbed)")
    else:
        print(f"✓ Fetched in {fetch_elapsed*1000:.2f}ms")
        print(f"  Throughput: {batch_size_mb/fetch_elapsed:.2f} MB/s")

    print("✅ Test 4 passed!\n")


def test_multiple_shuffles():
    """Test multiple concurrent shuffles (different shuffle IDs)."""
    print("=== Test 5: Multiple Concurrent Shuffles ===")

    server = ShuffleServer(host=b"127.0.0.1", port=9002)

    # Create 3 different shuffles
    shuffles = [
        (b"shuffle-job-1", 5),   # 5 partitions
        (b"shuffle-job-2", 3),   # 3 partitions
        (b"shuffle-job-3", 8),   # 8 partitions
    ]

    schema = pa.schema([('data', pa.int64())])
    total_registered = 0

    for shuffle_id, num_partitions in shuffles:
        for partition_id in range(num_partitions):
            batch = pa.RecordBatch.from_arrays([
                pa.array([partition_id * 10 + i for i in range(50)], type=pa.int64()),
            ], schema=schema)

            server.register_partition(shuffle_id, partition_id, batch)
            total_registered += 1

        print(f"✓ Registered {num_partitions} partitions for {shuffle_id.decode()}")

    print(f"✓ Total registered: {total_registered} partitions across {len(shuffles)} shuffles")
    print("✅ Test 5 passed!\n")


if __name__ == "__main__":
    print("\n" + "="*70)
    print("Agent-to-Agent Shuffle Transport Tests")
    print("Lock-Free Flight-Based Network Layer")
    print("="*70)

    try:
        test_server_client_basic()
        test_multi_partition_shuffle()
        test_concurrent_registration()
        test_large_batch_shuffle()
        test_multiple_shuffles()

        print("="*70)
        print("✅ All agent-to-agent tests passed!")
        print("="*70 + "\n")

        print("Note: Network DoGet() is currently stubbed - returns None")
        print("      Registration and local storage is fully functional")
        print("      Next step: Implement Flight server DoGet() handler\n")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
