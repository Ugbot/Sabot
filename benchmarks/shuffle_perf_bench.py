"""
Shuffle Performance Benchmarks

Benchmarks hash partitioning, network shuffle, and distributed joins.

Performance Targets (from PHASE4_NETWORK_SHUFFLE.md):
- Hash computation: ~500M values/sec (Arrow SIMD)
- Batch partitioning: ~200M rows/sec
- Network shuffle latency: <2ms p99
- Distributed join: ~100K rows/sec with shuffle
"""

import time
import pyarrow as pa
import pyarrow.compute as pc
import numpy as np
from typing import List

# Import Sabot shuffle components
try:
    from sabot._cython.shuffle.hash_partitioner import ArrowHashPartitioner, hash_partition
    from sabot._cython.shuffle.partitioner import HashPartitioner
    from sabot._cython.operators.shuffled_operator import ShuffledOperator
    SHUFFLE_AVAILABLE = True
except ImportError:
    SHUFFLE_AVAILABLE = False
    print("⚠️  Shuffle modules not compiled - skipping benchmarks")


def format_rate(ops_per_sec: float, unit: str = "ops") -> str:
    """Format operation rate in human-readable form"""
    if ops_per_sec >= 1_000_000_000:
        return f"{ops_per_sec / 1_000_000_000:.2f}B {unit}/sec"
    elif ops_per_sec >= 1_000_000:
        return f"{ops_per_sec / 1_000_000:.2f}M {unit}/sec"
    elif ops_per_sec >= 1_000:
        return f"{ops_per_sec / 1_000:.2f}K {unit}/sec"
    else:
        return f"{ops_per_sec:.2f} {unit}/sec"


def benchmark_hash_computation():
    """
    Benchmark: Hash Computation Throughput

    Target: ~500M values/sec (Arrow SIMD hash)
    """
    print("\n" + "="*80)
    print("Benchmark: Hash Computation Throughput")
    print("="*80)

    # Create test data (1M integers)
    num_values = 1_000_000
    data = pa.array(np.random.randint(0, 1_000_000, num_values, dtype=np.int64))

    # Warmup
    for _ in range(3):
        _ = pc.hash(data)

    # Benchmark
    iterations = 100
    start = time.perf_counter()

    for _ in range(iterations):
        hashes = pc.hash(data)

    elapsed = time.perf_counter() - start

    total_values = num_values * iterations
    throughput = total_values / elapsed

    print(f"  Data size: {num_values:,} values")
    print(f"  Iterations: {iterations}")
    print(f"  Time: {elapsed:.3f}s")
    print(f"  Throughput: {format_rate(throughput, 'values')}")
    print(f"  Target: 500M values/sec")

    if throughput >= 500_000_000:
        print("  ✅ PASSED - Exceeds target")
    elif throughput >= 250_000_000:
        print("  ⚠️  MARGINAL - 50%+ of target")
    else:
        print("  ❌ BELOW TARGET")

    return throughput


def benchmark_batch_partitioning():
    """
    Benchmark: Batch Partitioning Speed

    Target: ~200M rows/sec for partitioning
    """
    print("\n" + "="*80)
    print("Benchmark: Batch Partitioning Speed")
    print("="*80)

    if not SHUFFLE_AVAILABLE:
        print("  ⚠️  Skipped - shuffle modules not compiled")
        return 0

    # Create test batch (100K rows, 3 columns)
    num_rows = 100_000
    schema = pa.schema([
        ('user_id', pa.int64()),
        ('product_id', pa.int64()),
        ('amount', pa.float64())
    ])

    batch = pa.record_batch([
        pa.array(np.random.randint(0, 10_000, num_rows, dtype=np.int64)),
        pa.array(np.random.randint(0, 1_000, num_rows, dtype=np.int64)),
        pa.array(np.random.random(num_rows))
    ], schema=schema)

    # Create partitioner
    partitioner = ArrowHashPartitioner(
        key_columns=['user_id'],
        num_partitions=16
    )

    # Warmup
    for _ in range(3):
        _ = partitioner.partition_batch(batch)

    # Benchmark
    iterations = 500
    start = time.perf_counter()

    for _ in range(iterations):
        partitions = partitioner.partition_batch(batch)

    elapsed = time.perf_counter() - start

    total_rows = num_rows * iterations
    throughput = total_rows / elapsed

    print(f"  Batch size: {num_rows:,} rows")
    print(f"  Partitions: 16")
    print(f"  Iterations: {iterations}")
    print(f"  Time: {elapsed:.3f}s")
    print(f"  Throughput: {format_rate(throughput, 'rows')}")
    print(f"  Target: 200M rows/sec")

    if throughput >= 200_000_000:
        print("  ✅ PASSED - Exceeds target")
    elif throughput >= 100_000_000:
        print("  ⚠️  MARGINAL - 50%+ of target")
    else:
        print("  ❌ BELOW TARGET")

    return throughput


def benchmark_multi_column_partitioning():
    """
    Benchmark: Multi-Column Hash Partitioning

    Tests partitioning with composite keys (region + user_id)
    """
    print("\n" + "="*80)
    print("Benchmark: Multi-Column Hash Partitioning")
    print("="*80)

    if not SHUFFLE_AVAILABLE:
        print("  ⚠️  Skipped - shuffle modules not compiled")
        return 0

    # Create test batch with composite key
    num_rows = 50_000
    schema = pa.schema([
        ('region', pa.string()),
        ('user_id', pa.int64()),
        ('amount', pa.float64())
    ])

    regions = ['US', 'EU', 'ASIA', 'LATAM']
    batch = pa.record_batch([
        pa.array(np.random.choice(regions, num_rows)),
        pa.array(np.random.randint(0, 100_000, num_rows, dtype=np.int64)),
        pa.array(np.random.random(num_rows))
    ], schema=schema)

    # Create partitioner with composite key
    partitioner = ArrowHashPartitioner(
        key_columns=['region', 'user_id'],
        num_partitions=32
    )

    # Warmup
    for _ in range(3):
        _ = partitioner.partition_batch(batch)

    # Benchmark
    iterations = 500
    start = time.perf_counter()

    for _ in range(iterations):
        partitions = partitioner.partition_batch(batch)

    elapsed = time.perf_counter() - start

    total_rows = num_rows * iterations
    throughput = total_rows / elapsed

    print(f"  Batch size: {num_rows:,} rows")
    print(f"  Key columns: 2 (region + user_id)")
    print(f"  Partitions: 32")
    print(f"  Iterations: {iterations}")
    print(f"  Time: {elapsed:.3f}s")
    print(f"  Throughput: {format_rate(throughput, 'rows')}")

    if throughput >= 100_000_000:
        print("  ✅ GOOD - High throughput for composite keys")
    elif throughput >= 50_000_000:
        print("  ⚠️  ACCEPTABLE - Moderate throughput")
    else:
        print("  ❌ LOW - Below expectations")

    return throughput


def benchmark_partition_distribution():
    """
    Benchmark: Partition Distribution Quality

    Verifies hash function distributes data evenly across partitions
    """
    print("\n" + "="*80)
    print("Benchmark: Partition Distribution Quality")
    print("="*80)

    if not SHUFFLE_AVAILABLE:
        print("  ⚠️  Skipped - shuffle modules not compiled")
        return

    # Create test batch (1M rows)
    num_rows = 1_000_000
    schema = pa.schema([
        ('user_id', pa.int64()),
        ('value', pa.float64())
    ])

    batch = pa.record_batch([
        pa.array(np.random.randint(0, 1_000_000, num_rows, dtype=np.int64)),
        pa.array(np.random.random(num_rows))
    ], schema=schema)

    # Partition into 16 partitions
    partitioner = ArrowHashPartitioner(
        key_columns=['user_id'],
        num_partitions=16
    )

    partitions = partitioner.partition_batch(batch)

    # Analyze distribution
    partition_sizes = [p.num_rows if p is not None else 0 for p in partitions]

    avg_size = num_rows / 16
    max_size = max(partition_sizes)
    min_size = min(partition_sizes)
    std_dev = np.std(partition_sizes)

    # Calculate balance metric (0-1, where 1 is perfect balance)
    balance = 1.0 - (std_dev / avg_size)

    print(f"  Total rows: {num_rows:,}")
    print(f"  Partitions: 16")
    print(f"  Average size: {avg_size:.0f} rows/partition")
    print(f"  Min size: {min_size:,} ({min_size/avg_size*100:.1f}%)")
    print(f"  Max size: {max_size:,} ({max_size/avg_size*100:.1f}%)")
    print(f"  Std dev: {std_dev:.1f}")
    print(f"  Balance score: {balance:.3f}")

    if balance >= 0.95:
        print("  ✅ EXCELLENT - Nearly uniform distribution")
    elif balance >= 0.90:
        print("  ✅ GOOD - Well-balanced distribution")
    elif balance >= 0.80:
        print("  ⚠️  ACCEPTABLE - Minor skew")
    else:
        print("  ❌ POOR - Significant skew")


def benchmark_shuffled_operator_overhead():
    """
    Benchmark: ShuffledOperator Overhead

    Measures overhead of ShuffledOperator wrapper vs direct partitioner
    """
    print("\n" + "="*80)
    print("Benchmark: ShuffledOperator Overhead")
    print("="*80)

    if not SHUFFLE_AVAILABLE:
        print("  ⚠️  Skipped - shuffle modules not compiled")
        return

    # Create test batch
    num_rows = 50_000
    schema = pa.schema([
        ('user_id', pa.int64()),
        ('amount', pa.float64())
    ])

    batch = pa.record_batch([
        pa.array(np.random.randint(0, 10_000, num_rows, dtype=np.int64)),
        pa.array(np.random.random(num_rows))
    ], schema=schema)

    # Benchmark direct partitioner
    partitioner = ArrowHashPartitioner(
        key_columns=['user_id'],
        num_partitions=8
    )

    iterations = 500
    start = time.perf_counter()
    for _ in range(iterations):
        _ = partitioner.partition_batch(batch)
    direct_time = time.perf_counter() - start

    # Benchmark via ShuffledOperator
    from unittest.mock import Mock

    op = ShuffledOperator(
        operator_id='bench-op',
        parallelism=4,
        schema=schema,
        partition_keys=['user_id'],
        num_partitions=8
    )

    mock_transport = Mock()
    op.set_shuffle_config(mock_transport, b'shuffle-bench', 0, 8)

    start = time.perf_counter()
    for _ in range(iterations):
        _ = op._partition_batch(batch)
    operator_time = time.perf_counter() - start

    overhead = ((operator_time - direct_time) / direct_time) * 100

    print(f"  Batch size: {num_rows:,} rows")
    print(f"  Iterations: {iterations}")
    print(f"  Direct partitioner: {direct_time:.3f}s")
    print(f"  Via ShuffledOperator: {operator_time:.3f}s")
    print(f"  Overhead: {overhead:.1f}%")

    if overhead < 5:
        print("  ✅ EXCELLENT - Minimal overhead")
    elif overhead < 15:
        print("  ✅ GOOD - Acceptable overhead")
    elif overhead < 30:
        print("  ⚠️  MODERATE - Some overhead")
    else:
        print("  ❌ HIGH - Significant overhead")


def benchmark_large_batch_partitioning():
    """
    Benchmark: Large Batch Partitioning

    Tests partitioning with very large batches (1M+ rows)
    """
    print("\n" + "="*80)
    print("Benchmark: Large Batch Partitioning (1M rows)")
    print("="*80)

    if not SHUFFLE_AVAILABLE:
        print("  ⚠️  Skipped - shuffle modules not compiled")
        return 0

    # Create large batch (1M rows, 5 columns)
    num_rows = 1_000_000
    schema = pa.schema([
        ('user_id', pa.int64()),
        ('timestamp', pa.int64()),
        ('event_type', pa.string()),
        ('value', pa.float64()),
        ('metadata', pa.string())
    ])

    batch = pa.record_batch([
        pa.array(np.random.randint(0, 100_000, num_rows, dtype=np.int64)),
        pa.array(np.random.randint(0, 1_000_000_000, num_rows, dtype=np.int64)),
        pa.array(np.random.choice(['click', 'view', 'purchase'], num_rows)),
        pa.array(np.random.random(num_rows)),
        pa.array(['{}'] * num_rows)
    ], schema=schema)

    print(f"  Batch size: {num_rows:,} rows")
    print(f"  Columns: 5")
    print(f"  Memory: ~{batch.nbytes / (1024**2):.1f} MB")

    # Create partitioner
    partitioner = ArrowHashPartitioner(
        key_columns=['user_id'],
        num_partitions=64
    )

    # Warmup
    for _ in range(2):
        _ = partitioner.partition_batch(batch)

    # Benchmark
    iterations = 20
    start = time.perf_counter()

    for _ in range(iterations):
        partitions = partitioner.partition_batch(batch)

    elapsed = time.perf_counter() - start

    total_rows = num_rows * iterations
    throughput = total_rows / elapsed

    print(f"  Partitions: 64")
    print(f"  Iterations: {iterations}")
    print(f"  Time: {elapsed:.3f}s")
    print(f"  Throughput: {format_rate(throughput, 'rows')}")

    if throughput >= 100_000_000:
        print("  ✅ EXCELLENT - High throughput on large batches")
    elif throughput >= 50_000_000:
        print("  ✅ GOOD - Acceptable throughput")
    else:
        print("  ⚠️  MODERATE - Consider optimization")

    return throughput


def run_all_benchmarks():
    """Run all shuffle performance benchmarks"""
    print("\n" + "="*80)
    print("Sabot Shuffle Performance Benchmarks")
    print("="*80)
    print(f"Arrow version: {pa.__version__}")

    if not SHUFFLE_AVAILABLE:
        print("\n⚠️  Shuffle modules not compiled - compile with:")
        print("    python setup.py build_ext --inplace")
        return

    results = {}

    # Run benchmarks
    results['hash_computation'] = benchmark_hash_computation()
    results['batch_partitioning'] = benchmark_batch_partitioning()
    results['multi_column'] = benchmark_multi_column_partitioning()
    benchmark_partition_distribution()
    benchmark_shuffled_operator_overhead()
    results['large_batch'] = benchmark_large_batch_partitioning()

    # Summary
    print("\n" + "="*80)
    print("Summary")
    print("="*80)

    if results.get('hash_computation'):
        print(f"Hash computation: {format_rate(results['hash_computation'], 'values')}")
    if results.get('batch_partitioning'):
        print(f"Batch partitioning: {format_rate(results['batch_partitioning'], 'rows')}")
    if results.get('multi_column'):
        print(f"Multi-column: {format_rate(results['multi_column'], 'rows')}")
    if results.get('large_batch'):
        print(f"Large batch (1M rows): {format_rate(results['large_batch'], 'rows')}")

    print("\nTargets:")
    print("  - Hash computation: 500M values/sec")
    print("  - Batch partitioning: 200M rows/sec")
    print("  - Network latency: <2ms p99 (not benchmarked here)")


if __name__ == '__main__':
    run_all_benchmarks()
