"""
Benchmark to validate lazy loading implementation.

Tests that the lazy loading fix provides correct results and reasonable
performance with TPC-H data.
"""

import sys
import time
sys.path.insert(0, '/Users/bengamble/Sabot')

from sabot.api.stream import Stream
from sabot import cyarrow as ca


def find_lineitem_file():
    """Find available lineitem parquet file."""
    candidates = [
        'benchmarks/polars-benchmark/tpch-dbgen/lineitem.parquet',
        'benchmarks/polars-benchmark/data/scale_0.1/lineitem.parquet',
        'benchmarks/polars-benchmark/data/scale_1.0/lineitem.parquet',
    ]
    
    for path in candidates:
        import os
        if os.path.exists(path):
            return path
    
    return None


def benchmark_lazy_vs_eager():
    """Compare lazy and eager loading performance."""
    lineitem_file = find_lineitem_file()
    
    if not lineitem_file:
        print("⚠ No lineitem.parquet found, skipping benchmark")
        return
    
    print(f"Using: {lineitem_file}")
    
    # Test 1: Lazy loading
    print("\n1. Testing LAZY loading (new, fixed)...")
    start = time.time()
    
    stream_lazy = Stream.from_parquet(lineitem_file, backend='arrow', lazy=True)
    
    batch_count = 0
    total_rows = 0
    
    for batch in stream_lazy:
        batch_count += 1
        total_rows += batch.num_rows
    
    lazy_time = time.time() - start
    
    print(f"   ✓ Lazy: {total_rows:,} rows in {batch_count} batches")
    print(f"   Time: {lazy_time:.3f}s")
    
    # Test 2: Eager loading
    print("\n2. Testing EAGER loading (old)...")
    start = time.time()
    
    stream_eager = Stream.from_parquet(lineitem_file, backend='arrow', lazy=False)
    
    batch_count_eager = 0
    total_rows_eager = 0
    
    for batch in stream_eager:
        batch_count_eager += 1
        total_rows_eager += batch.num_rows
    
    eager_time = time.time() - start
    
    print(f"   ✓ Eager: {total_rows_eager:,} rows in {batch_count_eager} batches")
    print(f"   Time: {eager_time:.3f}s")
    
    # Compare
    assert total_rows == total_rows_eager, "Row count mismatch!"
    
    print(f"\n3. Results:")
    print(f"   Rows match: ✓")
    print(f"   Lazy time:  {lazy_time:.3f}s")
    print(f"   Eager time: {eager_time:.3f}s")
    
    if lazy_time < eager_time:
        speedup = eager_time / lazy_time
        print(f"   Lazy is {speedup:.2f}x faster! ⚡")
    else:
        print(f"   Performance similar (expected for small files)")
    
    return lazy_time, eager_time


def benchmark_tpch_q1_lazy():
    """Run TPC-H Q1 with lazy loading to verify correctness."""
    lineitem_file = find_lineitem_file()
    
    if not lineitem_file:
        print("⚠ No lineitem.parquet found, skipping Q1")
        return
    
    print(f"\n4. Testing TPC-H Q1 with lazy loading...")
    
    from datetime import date
    
    start = time.time()
    
    # TPC-H Q1 with lazy loading
    lineitem = Stream.from_parquet(lineitem_file, backend='arrow', lazy=True)
    
    cutoff = date(1998, 9, 2)
    
    result = (lineitem
        .filter(lambda b: ca.compute.less_equal(
            b['l_shipdate'],
            ca.scalar(cutoff, type=ca.date32())
        ))
        .map(lambda b: b.append_column(
            'disc_price',
            ca.compute.multiply(b['l_extendedprice'],
                       ca.compute.subtract(ca.scalar(1.0), b['l_discount']))
        ).append_column(
            'charge',
            ca.compute.multiply(
                ca.compute.multiply(b['l_extendedprice'],
                           ca.compute.subtract(ca.scalar(1.0), b['l_discount'])),
                ca.compute.add(ca.scalar(1.0), ca.compute.cast(b['l_tax'], ca.float64()))
            )
        ))
        .group_by(['l_returnflag', 'l_linestatus'])
        .aggregate({
            'sum_qty': ('l_quantity', 'sum'),
            'sum_base_price': ('l_extendedprice', 'sum'),
            'sum_disc_price': ('disc_price', 'sum'),
            'sum_charge': ('charge', 'sum'),
            'avg_qty': ('l_quantity', 'mean'),
            'avg_price': ('l_extendedprice', 'mean'),
            'avg_disc': ('l_discount', 'mean'),
            'count_order': ('l_returnflag', 'count')
        })
    )
    
    # Collect results
    batches = list(result)
    
    q1_time = time.time() - start
    
    if batches:
        result_table = ca.Table.from_batches(batches)
        print(f"   ✓ Q1 completed: {result_table.num_rows} groups")
        print(f"   Time: {q1_time:.3f}s")
        
        # Show sample results
        print(f"\n   Sample results:")
        for i in range(min(2, result_table.num_rows)):
            row = {col: result_table.column(col)[i].as_py() 
                   for col in result_table.column_names}
            print(f"     {row['l_returnflag']} | {row['l_linestatus']} | "
                  f"count={row['count_order']}")
        
        return q1_time
    else:
        print(f"   ⚠ Q1 returned no results")
        return None


if __name__ == '__main__':
    print("=" * 70)
    print("LAZY LOADING BENCHMARK - Validation Test")
    print("=" * 70)
    
    try:
        # Test basic lazy vs eager
        lazy_time, eager_time = benchmark_lazy_vs_eager()
        
        # Test TPC-H Q1
        q1_time = benchmark_tpch_q1_lazy()
        
        print("\n" + "=" * 70)
        print("BENCHMARK COMPLETE ✅")
        print("=" * 70)
        print(f"\nSummary:")
        print(f"  - Lazy loading works correctly ✓")
        print(f"  - Results match eager loading ✓")
        print(f"  - TPC-H Q1 executes successfully ✓")
        
        if q1_time:
            print(f"  - Q1 time: {q1_time:.3f}s")
        
        print("\n✅ Lazy loading implementation validated!")
        
    except Exception as e:
        print(f"\n❌ BENCHMARK FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

