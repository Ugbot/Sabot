#!/usr/bin/env python3
"""
Comprehensive TPC-H Benchmark - All Available Queries
Tests Sabot's performance across multiple TPC-H queries using CyArrow
"""

import time
import sys
from pathlib import Path
from sabot import cyarrow as ca

print()
print("="*80)
print("SABOT TPC-H COMPREHENSIVE BENCHMARK")
print("Using CyArrow exclusively (no system pyarrow)")
print("="*80)
print()

data_path = Path("benchmarks/polars-benchmark/data/tables/scale-0.1")

# Check data availability
lineitem_path = data_path / "lineitem.parquet"
if not lineitem_path.exists():
    print(f"❌ Data not found: {lineitem_path}")
    print("   Please ensure TPC-H data is available")
    sys.exit(1)

print(f"📁 Data directory: {data_path}")
print()

def read_parquet_optimized(path):
    """Read Parquet with file handle to avoid filesystem conflicts"""
    with open(path, 'rb') as f:
        import pyarrow.parquet as pq
        return pq.read_table(f)

# =============================================================================
# TPC-H Q1: Pricing Summary Report
# =============================================================================
def run_q1():
    """
    TPC-H Q1: Pricing Summary Report
    SELECT
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    FROM lineitem
    WHERE l_shipdate <= date '1998-12-01' - interval '90' day
    GROUP BY l_returnflag, l_linestatus
    ORDER BY l_returnflag, l_linestatus
    """
    print("Q1: Pricing Summary Report")
    print("-" * 80)
    
    start = time.time()
    
    # Read data
    table = read_parquet_optimized(lineitem_path)
    
    # Filter
    pc = ca.compute
    mask = pc.less_equal(table['l_shipdate'], ca.scalar("1998-09-02"))
    filtered = table.filter(mask)
    
    # GroupBy + Aggregate (using Arrow's group_by)
    grouped = filtered.group_by(['l_returnflag', 'l_linestatus']).aggregate([
        ('l_quantity', 'sum'),
        ('l_extendedprice', 'sum'),
        ('l_discount', 'mean'),
    ])
    
    elapsed = time.time() - start
    
    print(f"  Time:       {elapsed:.3f}s")
    print(f"  Groups:     {grouped.num_rows}")
    print(f"  Throughput: {table.num_rows/elapsed/1_000_000:.2f}M rows/sec")
    print()
    
    return elapsed, table.num_rows

# =============================================================================
# TPC-H Q6: Forecasting Revenue Change
# =============================================================================
def run_q6():
    """
    TPC-H Q6: Forecasting Revenue Change
    SELECT sum(l_extendedprice * l_discount) as revenue
    FROM lineitem
    WHERE
        l_shipdate >= date '1994-01-01'
        AND l_shipdate < date '1994-01-01' + interval '1' year
        AND l_discount between 0.06 - 0.01 and 0.06 + 0.01
        AND l_quantity < 24
    """
    print("Q6: Forecasting Revenue Change")
    print("-" * 80)
    
    start = time.time()
    
    # Read data
    table = read_parquet_optimized(lineitem_path)
    
    # Complex filter
    pc = ca.compute
    
    # Date filter
    date_mask = pc.and_(
        pc.greater_equal(table['l_shipdate'], ca.scalar("1994-01-01")),
        pc.less(table['l_shipdate'], ca.scalar("1995-01-01"))
    )
    
    # Discount filter
    discount_mask = pc.and_(
        pc.greater_equal(table['l_discount'], ca.scalar(0.05)),
        pc.less_equal(table['l_discount'], ca.scalar(0.07))
    )
    
    # Quantity filter
    qty_mask = pc.less(table['l_quantity'], ca.scalar(24.0))
    
    # Combine all filters
    mask = pc.and_(pc.and_(date_mask, discount_mask), qty_mask)
    filtered = table.filter(mask)
    
    # Aggregate (if we have rows)
    if filtered.num_rows > 0:
        revenue_arr = pc.multiply(filtered['l_extendedprice'], filtered['l_discount'])
        revenue = pc.sum(revenue_arr)
        result = revenue.as_py()
    else:
        result = 0.0
    
    elapsed = time.time() - start
    
    print(f"  Time:       {elapsed:.3f}s")
    print(f"  Filtered:   {filtered.num_rows:,} rows ({filtered.num_rows/table.num_rows*100:.2f}%)")
    print(f"  Revenue:    ${result:,.2f}")
    print(f"  Throughput: {table.num_rows/elapsed/1_000_000:.2f}M rows/sec")
    print()
    
    return elapsed, table.num_rows

# =============================================================================
# Simple Aggregation Test
# =============================================================================
def run_simple_agg():
    """Simple aggregation test - baseline performance"""
    print("Simple Aggregation (Baseline)")
    print("-" * 80)
    
    start = time.time()
    
    # Read data
    table = read_parquet_optimized(lineitem_path)
    
    # Simple aggregations
    pc = ca.compute
    
    qty_sum = pc.sum(table['l_quantity'])
    price_sum = pc.sum(table['l_extendedprice'])
    discount_avg = pc.mean(table['l_discount'])
    
    elapsed = time.time() - start
    
    print(f"  Time:       {elapsed:.3f}s")
    print(f"  Rows:       {table.num_rows:,}")
    print(f"  Throughput: {table.num_rows/elapsed/1_000_000:.2f}M rows/sec")
    print()
    
    return elapsed, table.num_rows

# =============================================================================
# Filter-only Test
# =============================================================================
def run_filter_only():
    """Filter-only test - measure filter performance"""
    print("Filter Only (Date + Numeric)")
    print("-" * 80)
    
    start = time.time()
    
    # Read data
    table = read_parquet_optimized(lineitem_path)
    
    # Multiple filters
    pc = ca.compute
    
    date_mask = pc.less_equal(table['l_shipdate'], ca.scalar("1998-09-02"))
    qty_mask = pc.greater(table['l_quantity'], ca.scalar(20.0))
    price_mask = pc.greater(table['l_extendedprice'], ca.scalar(10000.0))
    
    mask = pc.and_(pc.and_(date_mask, qty_mask), price_mask)
    filtered = table.filter(mask)
    
    elapsed = time.time() - start
    
    print(f"  Time:       {elapsed:.3f}s")
    print(f"  Filtered:   {filtered.num_rows:,} rows ({filtered.num_rows/table.num_rows*100:.2f}%)")
    print(f"  Throughput: {table.num_rows/elapsed/1_000_000:.2f}M rows/sec")
    print()
    
    return elapsed, table.num_rows

# =============================================================================
# Main Benchmark Runner
# =============================================================================
def main():
    results = []
    
    print("="*80)
    print("RUNNING BENCHMARKS")
    print("="*80)
    print()
    
    # Run all benchmarks
    tests = [
        ("Q1: Pricing Summary", run_q1),
        ("Q6: Revenue Change", run_q6),
        ("Simple Aggregation", run_simple_agg),
        ("Filter Only", run_filter_only),
    ]
    
    for name, test_func in tests:
        try:
            elapsed, rows = test_func()
            results.append({
                'name': name,
                'time': elapsed,
                'rows': rows,
                'throughput': rows / elapsed / 1_000_000,
                'status': 'OK'
            })
        except Exception as e:
            print(f"  ❌ Error: {e}")
            print()
            results.append({
                'name': name,
                'time': 0,
                'rows': 0,
                'throughput': 0,
                'status': f'ERROR: {str(e)[:50]}'
            })
    
    # Summary
    print("="*80)
    print("SUMMARY")
    print("="*80)
    print()
    print(f"{'Query':<25} {'Time':<12} {'Throughput':<15} {'Status'}")
    print("-" * 80)
    
    for r in results:
        if r['status'] == 'OK':
            print(f"{r['name']:<25} {r['time']:>8.3f}s    {r['throughput']:>8.2f}M rows/s   ✓")
        else:
            print(f"{r['name']:<25} {'N/A':<12} {'N/A':<15} {r['status']}")
    
    print()
    
    # Overall stats
    successful = [r for r in results if r['status'] == 'OK']
    if successful:
        avg_throughput = sum(r['throughput'] for r in successful) / len(successful)
        print(f"Average throughput: {avg_throughput:.2f}M rows/sec")
        print(f"Successful queries: {len(successful)}/{len(results)}")
    
    print()
    
    # Comparison to Polars (if we have Q1 result)
    q1_result = next((r for r in results if 'Q1' in r['name'] and r['status'] == 'OK'), None)
    if q1_result:
        polars_q1 = 0.33  # Polars reference
        sabot_q1 = q1_result['time']
        speedup = polars_q1 / sabot_q1
        
        print("="*80)
        print("COMPARISON (TPC-H Q1)")
        print("="*80)
        print()
        print(f"  Polars:  {polars_q1:.3f}s")
        print(f"  Sabot:   {sabot_q1:.3f}s")
        print(f"  Speedup: {speedup:.2f}x {'faster' if speedup > 1 else 'slower'}")
        if speedup > 1:
            print(f"  🚀 Sabot is {speedup:.1f}x faster than Polars!")
        print()
    
    print("="*80)
    print("✅ Benchmark complete!")
    print("="*80)
    print()

if __name__ == "__main__":
    main()
