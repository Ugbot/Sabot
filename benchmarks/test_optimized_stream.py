#!/usr/bin/env python3
"""
Test optimized Stream.from_parquet with date conversion
"""

import time
from sabot.api.stream import Stream
from sabot import cyarrow as ca

print()
print("="*70)
print("OPTIMIZED STREAM.FROM_PARQUET TEST")
print("="*70)
print()

data_path = "benchmarks/polars-benchmark/data/tables/scale-0.1/lineitem.parquet"

# Test 1: WITH optimization (default)
print("TEST 1: With date optimization (optimize_dates=True)")
print("-"*70)
start = time.time()

stream = Stream.from_parquet(data_path, optimize_dates=True)

# TPC-H Q1 style filter
def date_filter(batch):
    pc = ca.compute
    # If date is already date32, this is FAST (SIMD int32 comparison)
    # If still string, this is SLOW (string comparison)
    cutoff = ca.scalar("1998-09-02")
    if str(batch.schema.field('l_shipdate').type) == 'date32':
        # Fast path: int32 comparison
        cutoff = ca.scalar(ca.date32().cast(cutoff))
    return pc.less_equal(batch['l_shipdate'], cutoff)

filtered = stream.filter(date_filter)

# Simple aggregation
count = 0
for batch in filtered:
    count += batch.num_rows

elapsed = time.time() - start

print(f"Time:       {elapsed:.3f}s")
print(f"Rows:       {count:,}")
print(f"Throughput: {600_572/elapsed/1_000_000:.2f}M rows/sec")
print()

# Test 2: Compare to profiler baseline
print("COMPARISON TO BASELINE:")
print("-"*70)
print(f"Profiler (string dates):  0.144s  (4.17M rows/sec)")
print(f"Optimized (date32):       {elapsed:.3f}s  ({600_572/elapsed/1_000_000:.2f}M rows/sec)")
if elapsed < 0.144:
    speedup = 0.144 / elapsed
    print(f"Speedup:                  {speedup:.2f}x faster! 🚀")
else:
    print(f"(Still slower, may need debugging)")
print()

print("="*70)
