#!/usr/bin/env python3
"""
Simple test of lock-free primitives without pytest dependency.
"""

import sys
import time
from sabot import cyarrow as pa

# Import lock-free primitives
from sabot._cython.shuffle.lock_free_queue import SPSCRingBuffer, MPSCQueue
from sabot._cython.shuffle.atomic_partition_store import AtomicPartitionStore


def test_spsc_basic():
    """Test SPSC ring buffer basic operations."""
    print("\n=== Testing SPSC Ring Buffer ===")

    queue = SPSCRingBuffer(capacity=16)

    # Create test batch
    schema = pa.schema([('id', pa.int64()), ('value', pa.float64())])
    batch = pa.RecordBatch.from_arrays([
        pa.array([1, 2, 3], type=pa.int64()),
        pa.array([1.0, 2.0, 3.0], type=pa.float64()),
    ], schema=schema)

    # Push batch
    print("Pushing batch...")
    success = queue.push_py(shuffle_hash=123, partition_id=0, batch=batch)
    assert success, "Push failed"
    print(f"  ✓ Pushed batch with {batch.num_rows} rows")

    # Pop batch
    print("Popping batch...")
    result = queue.pop_py()
    assert result is not None, "Pop returned None"

    shuffle_hash, partition_id, received_batch = result
    assert shuffle_hash == 123
    assert partition_id == 0
    assert received_batch.num_rows == 3
    print(f"  ✓ Popped batch: shuffle_hash={shuffle_hash}, partition_id={partition_id}, rows={received_batch.num_rows}")

    print("✅ SPSC test passed!\n")


def test_atomic_store_basic():
    """Test atomic partition store basic operations."""
    print("=== Testing Atomic Partition Store ===")

    store = AtomicPartitionStore(size=128)

    # Create test batch
    schema = pa.schema([('id', pa.int64()), ('value', pa.float64())])
    batch = pa.RecordBatch.from_arrays([
        pa.array([10, 20, 30], type=pa.int64()),
        pa.array([10.0, 20.0, 30.0], type=pa.float64()),
    ], schema=schema)

    # Insert batch
    print("Inserting batch...")
    success = store.insert(shuffle_id_hash=456, partition_id=5, batch=batch)
    assert success, "Insert failed"
    print(f"  ✓ Inserted batch with {batch.num_rows} rows")

    # Get batch
    print("Getting batch...")
    retrieved = store.get(shuffle_id_hash=456, partition_id=5)
    assert retrieved is not None, "Get returned None"
    assert retrieved.num_rows == 3
    print(f"  ✓ Retrieved batch: rows={retrieved.num_rows}")

    # Verify size
    size = store.size()
    print(f"  Store size: {size}")

    print("✅ Atomic store test passed!\n")


def test_performance():
    """Simple performance test."""
    print("=== Performance Test ===")

    queue = SPSCRingBuffer(capacity=1024)

    # Create test batch
    schema = pa.schema([('id', pa.int64())])
    batch = pa.RecordBatch.from_arrays([
        pa.array(list(range(100)), type=pa.int64()),
    ], schema=schema)

    num_iterations = 10000

    # Measure push throughput
    print(f"Pushing {num_iterations} batches...")
    start = time.perf_counter()

    for i in range(num_iterations):
        while not queue.push_py(i, i, batch):
            pass

    push_elapsed = time.perf_counter() - start

    # Measure pop throughput
    print(f"Popping {num_iterations} batches...")
    start = time.perf_counter()

    for _ in range(num_iterations):
        while queue.pop_py() is None:
            pass

    pop_elapsed = time.perf_counter() - start

    push_throughput = num_iterations / push_elapsed
    pop_throughput = num_iterations / pop_elapsed
    total_rows = num_iterations * 100

    print(f"\nResults:")
    print(f"  Push: {push_throughput:.0f} batches/sec ({total_rows/push_elapsed:.0f} rows/sec)")
    print(f"  Pop:  {pop_throughput:.0f} batches/sec ({total_rows/pop_elapsed:.0f} rows/sec)")
    print(f"  Latency: {push_elapsed*1e6/num_iterations:.1f}μs per batch")

    print("✅ Performance test completed!\n")


if __name__ == "__main__":
    print("\n" + "="*60)
    print("Lock-Free Shuffle Transport - Basic Tests")
    print("="*60)

    try:
        test_spsc_basic()
        test_atomic_store_basic()
        test_performance()

        print("="*60)
        print("✅ All tests passed!")
        print("="*60 + "\n")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
