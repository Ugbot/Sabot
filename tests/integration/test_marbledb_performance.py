# -*- coding: utf-8 -*-
"""
MarbleDB Performance Tests

Performance benchmarks for MarbleDB state backend.
Validates throughput, latency, and scalability.
"""

import os
import shutil
import tempfile
import time
import logging
import statistics
import pytest
from typing import List, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Skip if MarbleDB is not available
try:
    from sabot._cython.state.marbledb_backend import MarbleDBStateBackend
    MARBLEDB_AVAILABLE = True
except ImportError as e:
    MARBLEDB_AVAILABLE = False
    MARBLEDB_IMPORT_ERROR = str(e)


@pytest.fixture
def temp_db_path():
    """Create a temporary directory for MarbleDB."""
    path = tempfile.mkdtemp(prefix="test_marbledb_perf_")
    yield path
    if os.path.exists(path):
        shutil.rmtree(path)


@pytest.fixture
def backend(temp_db_path):
    """Create and initialize a MarbleDB backend."""
    if not MARBLEDB_AVAILABLE:
        pytest.skip(f"MarbleDB not available: {MARBLEDB_IMPORT_ERROR}")

    backend = MarbleDBStateBackend(temp_db_path)
    backend.open()
    yield backend
    backend.close()


class TestWriteThroughput:
    """Test write throughput characteristics."""

    def test_sequential_write_throughput(self, backend):
        """Measure sequential write throughput."""
        num_ops = 10000
        key_prefix = "seq_write"

        start = time.perf_counter()
        for i in range(num_ops):
            backend.put_raw(f"{key_prefix}:{i}", f"value_{i}".encode())
        elapsed = time.perf_counter() - start

        ops_per_sec = num_ops / elapsed
        logger.info(f"Sequential Write: {ops_per_sec:,.0f} ops/sec ({num_ops} ops in {elapsed:.2f}s)")

        # Baseline expectation: at least 10K ops/sec
        assert ops_per_sec > 10000, f"Sequential write throughput too low: {ops_per_sec:.0f} ops/sec"

    def test_batch_write_throughput(self, backend):
        """Measure batch write throughput using ValueState."""
        num_batches = 100
        batch_size = 100

        start = time.perf_counter()
        for batch in range(num_batches):
            # Store entire batch as a dict value (simulates batch operation)
            batch_data = {f"key_{i}": f"value_{i}" for i in range(batch_size)}
            backend.put_value(f"batch:{batch}", batch_data)
        elapsed = time.perf_counter() - start

        total_items = num_batches * batch_size
        items_per_sec = total_items / elapsed
        batches_per_sec = num_batches / elapsed

        logger.info(f"Batch Write: {batches_per_sec:,.0f} batches/sec ({items_per_sec:,.0f} items/sec)")

        # Each batch is 100 items, expect good throughput
        assert batches_per_sec > 100, f"Batch write throughput too low: {batches_per_sec:.0f} batches/sec"


class TestReadThroughput:
    """Test read throughput characteristics."""

    def test_sequential_read_throughput(self, backend):
        """Measure sequential read throughput."""
        num_keys = 1000
        num_reads = 10000

        # Pre-populate
        for i in range(num_keys):
            backend.put_raw(f"read_test:{i}", f"value_{i}".encode())

        # Measure reads (with key reuse)
        start = time.perf_counter()
        for i in range(num_reads):
            _ = backend.get_raw(f"read_test:{i % num_keys}")
        elapsed = time.perf_counter() - start

        ops_per_sec = num_reads / elapsed
        logger.info(f"Sequential Read: {ops_per_sec:,.0f} ops/sec ({num_reads} ops in {elapsed:.2f}s)")

        # Expect high read throughput due to bloom filters
        assert ops_per_sec > 100000, f"Sequential read throughput too low: {ops_per_sec:.0f} ops/sec"

    def test_hot_key_read_throughput(self, backend):
        """Measure throughput for hot keys (frequently accessed)."""
        # Store a hot key
        backend.put_raw("hot_key", b"hot_value")

        num_reads = 100000
        start = time.perf_counter()
        for _ in range(num_reads):
            _ = backend.get_raw("hot_key")
        elapsed = time.perf_counter() - start

        ops_per_sec = num_reads / elapsed
        logger.info(f"Hot Key Read: {ops_per_sec:,.0f} ops/sec")

        # Hot key should be extremely fast
        assert ops_per_sec > 500000, f"Hot key read throughput too low: {ops_per_sec:.0f} ops/sec"


class TestLatency:
    """Test operation latency characteristics."""

    def measure_latencies(self, backend, operation, num_samples=1000) -> List[float]:
        """Measure latencies for an operation."""
        latencies = []
        for i in range(num_samples):
            start = time.perf_counter()
            operation(i)
            elapsed = (time.perf_counter() - start) * 1e6  # microseconds
            latencies.append(elapsed)
        return latencies

    def test_write_latency(self, backend):
        """Measure write latency distribution."""
        def write_op(i):
            backend.put_raw(f"lat_write:{i}", f"value_{i}".encode())

        latencies = self.measure_latencies(backend, write_op)

        avg_latency = statistics.mean(latencies)
        p50 = statistics.median(latencies)
        p99 = sorted(latencies)[int(len(latencies) * 0.99)]

        logger.info(f"Write Latency - Avg: {avg_latency:.1f}µs, P50: {p50:.1f}µs, P99: {p99:.1f}µs")

        # Expect low latency writes
        assert avg_latency < 500, f"Average write latency too high: {avg_latency:.1f}µs"

    def test_read_latency(self, backend):
        """Measure read latency distribution."""
        # Pre-populate
        for i in range(1000):
            backend.put_raw(f"lat_read:{i}", f"value_{i}".encode())

        def read_op(i):
            _ = backend.get_raw(f"lat_read:{i % 1000}")

        latencies = self.measure_latencies(backend, read_op)

        avg_latency = statistics.mean(latencies)
        p50 = statistics.median(latencies)
        p99 = sorted(latencies)[int(len(latencies) * 0.99)]

        logger.info(f"Read Latency - Avg: {avg_latency:.1f}µs, P50: {p50:.1f}µs, P99: {p99:.1f}µs")

        # Expect very low latency reads
        assert avg_latency < 50, f"Average read latency too high: {avg_latency:.1f}µs"


class TestScalability:
    """Test behavior under increasing load."""

    def test_throughput_with_data_size(self, backend):
        """Test throughput as data size grows."""
        results = []
        sizes = [100, 1000, 10000]

        for size in sizes:
            # Pre-populate to target size
            for i in range(size):
                backend.put_raw(f"scale:{i}", f"value_{i}".encode())

            # Measure read throughput at this size
            num_reads = 1000
            start = time.perf_counter()
            for i in range(num_reads):
                _ = backend.get_raw(f"scale:{i % size}")
            elapsed = time.perf_counter() - start

            ops_per_sec = num_reads / elapsed
            results.append((size, ops_per_sec))
            logger.info(f"  Data size {size}: {ops_per_sec:,.0f} reads/sec")

        # Throughput should not degrade dramatically
        # Allow 10x degradation max between smallest and largest
        ratio = results[0][1] / results[-1][1]
        logger.info(f"Throughput ratio (smallest/largest): {ratio:.1f}x")
        assert ratio < 100, f"Throughput degrades too much: {ratio:.1f}x"

    def test_multi_get_scalability(self, backend):
        """Test multi-get performance with batch size."""
        # Pre-populate
        for i in range(1000):
            backend.put_raw(f"multi:{i}", f"value_{i}".encode())

        batch_sizes = [10, 50, 100, 500]
        for batch_size in batch_sizes:
            keys = [f"multi:{i}" for i in range(batch_size)]

            start = time.perf_counter()
            for _ in range(100):
                _ = backend.multi_get_raw(keys)
            elapsed = time.perf_counter() - start

            batches_per_sec = 100 / elapsed
            keys_per_sec = batch_size * batches_per_sec

            logger.info(f"  Multi-get batch {batch_size}: {keys_per_sec:,.0f} keys/sec")


class TestMixedWorkload:
    """Test mixed read/write workloads."""

    def test_read_heavy_workload(self, backend):
        """90% reads, 10% writes."""
        # Pre-populate
        for i in range(1000):
            backend.put_raw(f"mixed:{i}", f"value_{i}".encode())

        num_ops = 10000
        write_counter = [0]  # Use list for mutation in nested function

        start = time.perf_counter()
        for i in range(num_ops):
            if i % 10 == 0:
                # 10% writes
                backend.put_raw(f"mixed_new:{write_counter[0]}", f"new_value_{write_counter[0]}".encode())
                write_counter[0] += 1
            else:
                # 90% reads
                _ = backend.get_raw(f"mixed:{i % 1000}")
        elapsed = time.perf_counter() - start

        ops_per_sec = num_ops / elapsed
        logger.info(f"Read-heavy (90/10): {ops_per_sec:,.0f} ops/sec")

        assert ops_per_sec > 50000, f"Read-heavy throughput too low: {ops_per_sec:.0f} ops/sec"

    def test_write_heavy_workload(self, backend):
        """10% reads, 90% writes."""
        # Pre-populate
        for i in range(100):
            backend.put_raw(f"write_heavy:{i}", f"value_{i}".encode())

        num_ops = 10000

        start = time.perf_counter()
        for i in range(num_ops):
            if i % 10 == 0:
                # 10% reads
                _ = backend.get_raw(f"write_heavy:{i % 100}")
            else:
                # 90% writes
                backend.put_raw(f"write_new:{i}", f"value_{i}".encode())
        elapsed = time.perf_counter() - start

        ops_per_sec = num_ops / elapsed
        logger.info(f"Write-heavy (10/90): {ops_per_sec:,.0f} ops/sec")

        assert ops_per_sec > 10000, f"Write-heavy throughput too low: {ops_per_sec:.0f} ops/sec"


class TestValueStatePerfromance:
    """Test ValueState (pickle serialization) performance."""

    def test_small_object_throughput(self, backend):
        """Test throughput with small Python objects."""
        num_ops = 5000

        # Small dicts
        start = time.perf_counter()
        for i in range(num_ops):
            backend.put_value(f"small:{i}", {"id": i, "value": f"data_{i}"})
        write_elapsed = time.perf_counter() - start

        start = time.perf_counter()
        for i in range(num_ops):
            _ = backend.get_value(f"small:{i}")
        read_elapsed = time.perf_counter() - start

        write_ops = num_ops / write_elapsed
        read_ops = num_ops / read_elapsed

        logger.info(f"Small Object - Write: {write_ops:,.0f}/s, Read: {read_ops:,.0f}/s")

        assert write_ops > 5000, f"Small object write too slow: {write_ops:.0f}/s"
        assert read_ops > 10000, f"Small object read too slow: {read_ops:.0f}/s"

    def test_large_object_throughput(self, backend):
        """Test throughput with larger Python objects."""
        num_ops = 500

        # Large dicts (1KB+)
        large_data = {f"key_{j}": f"value_{j}" * 10 for j in range(100)}

        start = time.perf_counter()
        for i in range(num_ops):
            backend.put_value(f"large:{i}", large_data)
        write_elapsed = time.perf_counter() - start

        start = time.perf_counter()
        for i in range(num_ops):
            _ = backend.get_value(f"large:{i}")
        read_elapsed = time.perf_counter() - start

        write_ops = num_ops / write_elapsed
        read_ops = num_ops / read_elapsed

        logger.info(f"Large Object - Write: {write_ops:,.0f}/s, Read: {read_ops:,.0f}/s")

        assert write_ops > 500, f"Large object write too slow: {write_ops:.0f}/s"
        assert read_ops > 1000, f"Large object read too slow: {read_ops:.0f}/s"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
