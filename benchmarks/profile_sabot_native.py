#!/usr/bin/env python3
"""
Profile Sabot Native Performance

Detailed timing breakdown to find bottlenecks and optimization opportunities.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent / 'polars-benchmark'))

from sabot.api.stream import Stream
from sabot import cyarrow as ca
import pyarrow.compute as pc
from datetime import date


class Timer:
    """Context manager for timing operations."""
    
    def __init__(self, name):
        self.name = name
        self.start = None
        
    def __enter__(self):
        self.start = time.perf_counter()
        return self
        
    def __exit__(self, *args):
        elapsed = time.perf_counter() - self.start
        print(f"{self.name:30} {elapsed:8.3f}s")


def profile_q6():
    """Profile TPC-H Q6 with detailed timing."""
    print("="*80)
    print("Sabot Native Q6 - Detailed Profile")
    print("="*80)
    print()
    
    data_path = "benchmarks/polars-benchmark/data/tables/scale-0.1/lineitem.parquet"
    
    # Overall timing
    total_start = time.perf_counter()
    
    # 1. I/O
    with Timer("1. Parquet read"):
        stream = Stream.from_parquet(data_path)
    
    # 2. Filter - date range
    with Timer("2. Filter: date range"):
        stream = stream.filter(lambda b: pc.and_(
            pc.greater_equal(
                pc.strptime(b['l_shipdate'], '%Y-%m-%d', 'date32'),
                ca.scalar(date(1994, 1, 1))
            ),
            pc.less(
                pc.strptime(b['l_shipdate'], '%Y-%m-%d', 'date32'),
                ca.scalar(date(1995, 1, 1))
            )
        ))
    
    # 3. Filter - discount
    with Timer("3. Filter: discount range"):
        stream = stream.filter(lambda b: pc.and_(
            pc.greater_equal(b['l_discount'], ca.scalar(0.05)),
            pc.less_equal(b['l_discount'], ca.scalar(0.07))
        ))
    
    # 4. Filter - quantity
    with Timer("4. Filter: quantity"):
        stream = stream.filter(lambda b: pc.less(b['l_quantity'], ca.scalar(24)))
    
    # 5. Compute revenue
    with Timer("5. Compute: revenue"):
        stream = stream.map(lambda b: b.append_column(
            'revenue',
            pc.multiply(b['l_extendedprice'], b['l_discount'])
        ))
    
    # 6. Aggregate
    with Timer("6. Aggregate: sum"):
        total_revenue = 0
        for batch in stream:
            total_revenue += pc.sum(batch['revenue']).as_py()
    
    total_elapsed = time.perf_counter() - total_start
    
    print()
    print(f"{'TOTAL':30} {total_elapsed:8.3f}s")
    print()
    print(f"Result: Revenue = {total_revenue:,.2f}")
    print()
    
    # Compare with Polars
    print("="*80)
    print("ANALYSIS")
    print("="*80)
    print()
    print(f"Sabot Native:  {total_elapsed:.3f}s")
    print(f"Polars:        0.580s (from benchmark)")
    print(f"Gap:           {total_elapsed/0.58:.1f}x")
    print()
    
    print("Where is time spent:")
    print("  - Check individual operation timings above")
    print("  - Identify bottlenecks")
    print("  - Focus optimization efforts")


def profile_q1():
    """Profile TPC-H Q1 with detailed timing."""
    print("="*80)
    print("Sabot Native Q1 - Detailed Profile")
    print("="*80)
    print()
    
    data_path = "benchmarks/polars-benchmark/data/tables/scale-0.1/lineitem.parquet"
    
    total_start = time.perf_counter()
    
    # 1. I/O
    with Timer("1. Parquet read"):
        stream = Stream.from_parquet(data_path)
    
    # 2. Filter
    with Timer("2. Filter: shipdate"):
        stream = stream.filter(lambda b: pc.less_equal(
            pc.strptime(b['l_shipdate'], '%Y-%m-%d', 'date32'),
            ca.scalar(date(1998, 9, 2))
        ))
    
    # 3. Compute derived columns
    with Timer("3. Compute: derived cols"):
        stream = stream.map(lambda b: b.append_column(
            'disc_price',
            pc.multiply(b['l_extendedprice'],
                       pc.subtract(ca.scalar(1.0), b['l_discount']))
        ).append_column(
            'charge',
            pc.multiply(
                pc.multiply(b['l_extendedprice'],
                           pc.subtract(ca.scalar(1.0), b['l_discount'])),
                pc.add(ca.scalar(1.0), pc.cast(b['l_tax'], ca.float64()))
            )
        ))
    
    # 4. GroupBy
    with Timer("4. GroupBy"):
        grouped = stream.group_by(['l_returnflag', 'l_linestatus'])
    
    # 5. Aggregate
    with Timer("5. Aggregate"):
        result = grouped.aggregate({
            'sum_qty': ('l_quantity', 'sum'),
            'sum_base_price': ('l_extendedprice', 'sum'),
            'sum_disc_price': ('disc_price', 'sum'),
            'sum_charge': ('charge', 'sum'),
            'avg_qty': ('l_quantity', 'mean'),
            'avg_price': ('l_extendedprice', 'mean'),
            'avg_disc': ('l_discount', 'mean'),
            'count_order': ('l_returnflag', 'count')
        })
    
    # 6. Collect
    with Timer("6. Collect results"):
        batches = list(result)
        if batches:
            final = ca.Table.from_batches(batches)
            print(f"  → {final.num_rows} groups")
    
    total_elapsed = time.perf_counter() - total_start
    
    print()
    print(f"{'TOTAL':30} {total_elapsed:8.3f}s")
    print()
    
    print("="*80)
    print("ANALYSIS")
    print("="*80)
    print()
    print(f"Sabot Native:  {total_elapsed:.3f}s")
    print(f"Polars:        0.330s")
    print(f"Gap:           {total_elapsed/0.33:.1f}x")
    print()


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--query', choices=['q1', 'q6', 'both'], default='both')
    args = parser.parse_args()
    
    if args.query in ['q6', 'both']:
        profile_q6()
        print()
    
    if args.query in ['q1', 'both']:
        profile_q1()


if __name__ == "__main__":
    main()

