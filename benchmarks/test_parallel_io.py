#!/usr/bin/env python3
"""
Test parallel I/O performance
"""

import time
from sabot.api.stream import Stream

print()
print("="*70)
print("PARALLEL I/O PERFORMANCE TEST")
print("="*70)
print()

data_path = "benchmarks/polars-benchmark/data/tables/scale-0.1/lineitem.parquet"

# Test 1: Single-threaded I/O
print("TEST 1: Single-threaded I/O")
print("-"*70)
start = time.time()

stream = Stream.from_parquet(data_path, parallel=False)
count = 0
for batch in stream:
    count += batch.num_rows

elapsed_single = time.time() - start
print(f"Time:       {elapsed_single:.3f}s")
print(f"Rows:       {count:,}")
print(f"Throughput: {count/elapsed_single/1_000_000:.2f}M rows/sec")
print()

# Test 2: Parallel I/O (4 threads)
print("TEST 2: Parallel I/O (4 threads)")
print("-"*70)
start = time.time()

stream = Stream.from_parquet(data_path, parallel=True, num_threads=4)
count = 0
for batch in stream:
    count += batch.num_rows

elapsed_parallel = time.time() - start
print(f"Time:       {elapsed_parallel:.3f}s")
print(f"Rows:       {count:,}")
print(f"Throughput: {count/elapsed_parallel/1_000_000:.2f}M rows/sec")
print()

# Comparison
print("="*70)
print("COMPARISON")
print("="*70)
print()
print(f"Single-threaded: {elapsed_single:.3f}s")
print(f"Parallel (4):    {elapsed_parallel:.3f}s")
print(f"Speedup:         {elapsed_single/elapsed_parallel:.2f}x")
print()

if elapsed_single / elapsed_parallel > 1.3:
    print("✅ Parallel I/O provides significant speedup!")
elif elapsed_single / elapsed_parallel > 1.0:
    print("✓ Parallel I/O provides modest speedup")
else:
    print("⚠️  Parallel I/O not faster (possible overhead or single row group)")

print()
print("="*70)
