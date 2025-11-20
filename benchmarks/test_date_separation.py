#!/usr/bin/env python3
"""
Proper test: Conversion happens ONCE at read, then filters are FAST
"""

import time
from sabot import cyarrow as ca
from datetime import date

print()
print("="*70)
print("DATE OPTIMIZATION - Conversion Once, Filter Many Times")
print("="*70)
print()

data_path = "benchmarks/polars-benchmark/data/tables/scale-0.1/lineitem.parquet"

# Scenario: Read data once, filter multiple times (realistic use case)

print("SCENARIO 1: String Dates (Current)")
print("="*70)

# Read
with open(data_path, 'rb') as f:
    import pyarrow.parquet as pq
    table_string = pq.read_table(f)

print(f"Read complete. Schema: {table_string.schema.field('l_shipdate').type}")

# Filter 5 times (simulating multiple queries/filters)
pc = ca.compute
total_filter_time = 0

for i in range(5):
    start = time.time()
    mask = pc.less_equal(table_string['l_shipdate'], ca.scalar("1998-09-02"))
    filtered = table_string.filter(mask)
    total_filter_time += (time.time() - start)

avg_filter_time = total_filter_time / 5
print(f"Average filter time (string): {avg_filter_time:.4f}s")
print(f"Filter throughput: {table_string.num_rows/avg_filter_time/1_000_000:.2f}M rows/sec")
print()

print("SCENARIO 2: Date32 (Optimized)")
print("="*70)

# Read and convert (ONE TIME COST)
with open(data_path, 'rb') as f:
    table_optimized = pq.read_table(f)

start_convert = time.time()
ts = pc.strptime(table_optimized['l_shipdate'], format='%Y-%m-%d', unit='s')
shipdate_date32 = pc.cast(ts, ca.date32())
table_optimized = table_optimized.set_column(
    table_optimized.schema.get_field_index('l_shipdate'),
    'l_shipdate',
    shipdate_date32
)
convert_time = time.time() - start_convert

print(f"Conversion time (ONE TIME): {convert_time:.4f}s")
print(f"Schema after: {table_optimized.schema.field('l_shipdate').type}")

# Filter 5 times (FAST because dates are already int32)
total_filter_time = 0

for i in range(5):
    start = time.time()
    cutoff = ca.scalar(date(1998, 9, 2), type=ca.date32())
    mask = pc.less_equal(table_optimized['l_shipdate'], cutoff)
    filtered = table_optimized.filter(mask)
    total_filter_time += (time.time() - start)

avg_filter_time_opt = total_filter_time / 5
print(f"Average filter time (date32): {avg_filter_time_opt:.4f}s")
print(f"Filter throughput: {table_optimized.num_rows/avg_filter_time_opt/1_000_000:.2f}M rows/sec")
print()

print("="*70)
print("RESULTS")
print("="*70)
print()
print(f"String filter:  {avg_filter_time:.4f}s per query")
print(f"Date32 filter:  {avg_filter_time_opt:.4f}s per query")
print(f"Speedup:        {avg_filter_time/avg_filter_time_opt:.2f}x faster per filter")
print()
print(f"Conversion cost: {convert_time:.4f}s (amortized over many queries)")
print(f"Break-even:      After {convert_time/(avg_filter_time - avg_filter_time_opt):.1f} queries")
print()

# Calculate TPC-H Q1 impact
print("="*70)
print("IMPACT ON TPC-H Q1")
print("="*70)
print()
q1_io = 0.055
q1_filter_string = 0.075
q1_groupby = 0.015

q1_filter_date32 = avg_filter_time_opt
q1_total_opt = q1_io + convert_time + q1_filter_date32 + q1_groupby

print(f"Baseline:")
print(f"  I/O:     {q1_io:.3f}s")
print(f"  Filter:  {q1_filter_string:.3f}s (string)")
print(f"  GroupBy: {q1_groupby:.3f}s")
print(f"  Total:   {q1_io + q1_filter_string + q1_groupby:.3f}s")
print()
print(f"Optimized:")
print(f"  I/O:     {q1_io:.3f}s")
print(f"  Convert: {convert_time:.3f}s (once)")
print(f"  Filter:  {q1_filter_date32:.3f}s (date32)")
print(f"  GroupBy: {q1_groupby:.3f}s")
print(f"  Total:   {q1_total_opt:.3f}s")
print()

if q1_total_opt < (q1_io + q1_filter_string + q1_groupby):
    speedup = (q1_io + q1_filter_string + q1_groupby) / q1_total_opt
    print(f"✅ Speedup: {speedup:.2f}x faster!")
else:
    print(f"⚠️  Slightly slower ({q1_total_opt:.3f}s vs {q1_io + q1_filter_string + q1_groupby:.3f}s)")
    print(f"   But filters after first query are {avg_filter_time/avg_filter_time_opt:.2f}x faster!")

print()
print("="*70)
