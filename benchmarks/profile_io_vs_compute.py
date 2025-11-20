#!/usr/bin/env python3
"""
Profile Sabot: I/O vs Compute Breakdown

Measures where time is actually spent:
- Ingress (reading Parquet)
- Processing (filters, aggregations, etc.)
- Egress (writing results)

This identifies if we're I/O bound or compute bound.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, '/Users/bengamble/Sabot')

from sabot import cyarrow as ca
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc


class DetailedTimer:
    """Hierarchical timer for detailed profiling."""
    
    def __init__(self):
        self.timings = {}
        self.current = []
    
    def time(self, name):
        return TimedSection(self, name)
    
    def record(self, name, elapsed):
        self.timings[name] = elapsed
    
    def print_summary(self, total_time):
        print("\nTime Breakdown:")
        print("="*70)
        print(f"{'Operation':<40} {'Time':<10} {'% of Total'}")
        print("-"*70)
        
        for name, time_val in sorted(self.timings.items(), key=lambda x: x[1], reverse=True):
            pct = (time_val / total_time * 100) if total_time > 0 else 0
            print(f"{name:<40} {time_val:>6.3f}s   {pct:>6.1f}%")
        
        print("-"*70)
        print(f"{'TOTAL':<40} {total_time:>6.3f}s   100.0%")
        

class TimedSection:
    def __init__(self, timer, name):
        self.timer = timer
        self.name = name
        self.start = None
    
    def __enter__(self):
        self.start = time.perf_counter()
        return self
    
    def __exit__(self, *args):
        elapsed = time.perf_counter() - self.start
        self.timer.record(self.name, elapsed)


def profile_tpch_q6():
    """
    Profile TPC-H Q6 with detailed I/O vs Compute breakdown.
    
    Q6: Filter on date, discount, quantity → aggregate sum
    """
    print("="*80)
    print("SABOT PROFILING: TPC-H Q6")
    print("I/O vs Compute Breakdown")
    print("="*80)
    print()
    
    data_path = "benchmarks/polars-benchmark/data/tables/scale-0.1/lineitem.parquet"
    
    timer = DetailedTimer()
    overall_start = time.perf_counter()
    
    # PHASE 1: INGRESS (I/O)
    with timer.time("1. Parquet Read (Ingress)"):
        table = pq.read_table(data_path)
        print(f"  Read {table.num_rows:,} rows, {table.nbytes/1024/1024:.1f} MB")
    
    batches = table.to_batches(max_chunksize=100000)
    
    # PHASE 2: PROCESSING (Compute)
    
    # Filter 1: Date range
    filtered_batches_1 = []
    with timer.time("2a. Filter: Date (strptime + compare)"):
        for batch in batches:
            # This is expensive - parsing strings!
            dates = pc.strptime(batch['l_shipdate'], '%Y-%m-%d', 'date32')
            mask = pc.and_(
                pc.greater_equal(dates, ca.scalar('1994-01-01')),
                pc.less(dates, ca.scalar('1995-01-01'))
            )
            filtered = batch.filter(mask)
            if filtered.num_rows > 0:
                filtered_batches_1.append(filtered)
    
    print(f"  After date filter: {sum(b.num_rows for b in filtered_batches_1):,} rows")
    
    # Filter 2: Discount range
    filtered_batches_2 = []
    with timer.time("2b. Filter: Discount (numeric compare)"):
        for batch in filtered_batches_1:
            mask = pc.and_(
                pc.greater_equal(batch['l_discount'], ca.scalar(0.05)),
                pc.less_equal(batch['l_discount'], ca.scalar(0.07))
            )
            filtered = batch.filter(mask)
            if filtered.num_rows > 0:
                filtered_batches_2.append(filtered)
    
    print(f"  After discount filter: {sum(b.num_rows for b in filtered_batches_2):,} rows")
    
    # Filter 3: Quantity
    filtered_batches_3 = []
    with timer.time("2c. Filter: Quantity (numeric compare)"):
        for batch in filtered_batches_2:
            mask = pc.less(batch['l_quantity'], ca.scalar(24.0))
            filtered = batch.filter(mask)
            if filtered.num_rows > 0:
                filtered_batches_3.append(filtered)
    
    print(f"  After quantity filter: {sum(b.num_rows for b in filtered_batches_3):,} rows")
    
    # Compute revenue
    revenue_batches = []
    with timer.time("2d. Compute: Revenue (multiply)"):
        for batch in filtered_batches_3:
            revenue = pc.multiply(batch['l_extendedprice'], batch['l_discount'])
            revenue_batches.append(revenue)
    
    # Aggregate
    with timer.time("2e. Aggregate: Sum"):
        total_revenue = 0.0
        for revenue_arr in revenue_batches:
            total_revenue += pc.sum(revenue_arr).as_py()
    
    print(f"  Total revenue: {total_revenue:,.2f}")
    
    # PHASE 3: EGRESS (Output)
    with timer.time("3. Result Collection (Egress)"):
        # Create result table
        result = pa.table({'revenue': [total_revenue]})
    
    overall_time = time.perf_counter() - overall_start
    
    # Print breakdown
    timer.print_summary(overall_time)
    
    print()
    print("="*80)
    print("ANALYSIS")
    print("="*80)
    print()
    
    # Calculate categories
    ingress_time = timer.timings.get("1. Parquet Read (Ingress)", 0)
    compute_time = sum(v for k, v in timer.timings.items() if k.startswith("2"))
    egress_time = timer.timings.get("3. Result Collection (Egress)", 0)
    
    print(f"Ingress (I/O):     {ingress_time:6.3f}s  ({ingress_time/overall_time*100:5.1f}%)")
    print(f"Processing:        {compute_time:6.3f}s  ({compute_time/overall_time*100:5.1f}%)")
    print(f"Egress (Output):   {egress_time:6.3f}s  ({egress_time/overall_time*100:5.1f}%)")
    print(f"Overhead:          {overall_time - ingress_time - compute_time - egress_time:6.3f}s")
    print()
    
    # Identify bottleneck
    if ingress_time > compute_time:
        print("⚠️ I/O BOUND - Ingress dominates")
        print("   Optimization: Better Parquet reader, parallel I/O")
    elif compute_time > ingress_time * 2:
        print("⚠️ COMPUTE BOUND - Processing dominates")
        print("   Optimization: Better operators, SIMD, reduce overhead")
    else:
        print("⚠️ BALANCED - Both I/O and compute matter")
    
    print()
    print("Specific bottleneck:")
    max_op = max(timer.timings.items(), key=lambda x: x[1])
    print(f"  {max_op[0]}: {max_op[1]:.3f}s ({max_op[1]/overall_time*100:.1f}%)")
    print()
    
    # Compare with Polars
    print(f"Sabot total: {overall_time:.3f}s")
    print(f"Polars:      0.580s (from benchmark)")
    print(f"Gap:         {overall_time/0.58:.1f}x")
    print()
    print("Where to focus optimization:")
    print("  1. Fix the specific bottleneck identified above")
    print("  2. Then tackle next slowest operation")
    print("  3. Iterate until competitive")


def profile_tpch_q1():
    """
    Profile TPC-H Q1 with I/O vs Compute breakdown.
    
    Q1: Filter by date → GroupBy → Multiple aggregations → Sort
    """
    print("="*80)
    print("SABOT PROFILING: TPC-H Q1")
    print("I/O vs Compute Breakdown")
    print("="*80)
    print()
    
    data_path = "benchmarks/polars-benchmark/data/tables/scale-0.1/lineitem.parquet"
    
    timer = DetailedTimer()
    overall_start = time.perf_counter()
    
    # PHASE 1: INGRESS
    with timer.time("1. Parquet Read"):
        table = pq.read_table(data_path)
        print(f"  Read {table.num_rows:,} rows")
    
    batches = table.to_batches(max_chunksize=100000)
    
    # PHASE 2: PROCESSING
    
    # Filter
    filtered_batches = []
    with timer.time("2a. Filter: Date"):
        for batch in batches:
            dates = pc.strptime(batch['l_shipdate'], '%Y-%m-%d', 'date32')
            mask = pc.less_equal(dates, ca.scalar('1998-09-02'))
            filtered = batch.filter(mask)
            if filtered.num_rows > 0:
                filtered_batches.append(filtered)
    
    print(f"  After filter: {sum(b.num_rows for b in filtered_batches):,} rows")
    
    # Combine batches
    with timer.time("2b. Combine Batches"):
        combined_table = pa.Table.from_batches(filtered_batches)
    
    # GroupBy + Aggregate (THE BOTTLENECK)
    with timer.time("2c. GroupBy + Aggregate"):
        grouped = combined_table.group_by(['l_returnflag', 'l_linestatus'])
        
        result = grouped.aggregate([
            ('l_quantity', 'sum'),
            ('l_extendedprice', 'sum'),
            ('l_quantity', 'mean'),
            ('l_extendedprice', 'mean'),
            ('l_discount', 'mean'),
            ('l_returnflag', 'count')
        ])
    
    print(f"  Groups: {result.num_rows}")
    
    # Sort
    with timer.time("2d. Sort"):
        sorted_table = result.sort_by([
            ('l_returnflag', 'ascending'),
            ('l_linestatus', 'ascending')
        ])
    
    # PHASE 3: EGRESS
    with timer.time("3. Result Collection"):
        final_result = sorted_table.to_pandas()
    
    overall_time = time.perf_counter() - overall_start
    
    # Print breakdown
    timer.print_summary(overall_time)
    
    print()
    print("="*80)
    print("ANALYSIS")
    print("="*80)
    print()
    
    ingress = timer.timings.get("1. Parquet Read", 0)
    compute = sum(v for k, v in timer.timings.items() if k.startswith("2"))
    egress = timer.timings.get("3. Result Collection", 0)
    
    print(f"Ingress (I/O):     {ingress:6.3f}s  ({ingress/overall_time*100:5.1f}%)")
    print(f"Processing:        {compute:6.3f}s  ({compute/overall_time*100:5.1f}%)")
    print(f"  - Date filter:   {timer.timings.get('2a. Filter: Date', 0):6.3f}s")
    print(f"  - Combine:       {timer.timings.get('2b. Combine Batches', 0):6.3f}s")
    print(f"  - GroupBy:       {timer.timings.get('2c. GroupBy + Aggregate', 0):6.3f}s")
    print(f"  - Sort:          {timer.timings.get('2d. Sort', 0):6.3f}s")
    print(f"Egress (Output):   {egress:6.3f}s  ({egress/overall_time*100:5.1f}%)")
    print()
    
    print(f"Sabot: {overall_time:.3f}s")
    print(f"Polars: 0.330s")
    print(f"Gap: {overall_time/0.33:.1f}x")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--query', choices=['q1', 'q6', 'both'], default='both')
    args = parser.parse_args()
    
    if args.query in ['q6', 'both']:
        profile_tpch_q6()
        print("\n")
    
    if args.query in ['q1', 'both']:
        profile_tpch_q1()

