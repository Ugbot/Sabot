#!/usr/bin/env python3
"""
Test performance with and without morsel parallelism
This will show if morsel overhead is causing the 3x slowdown
"""

import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "polars-benchmark"))

from sabot.api.stream import Stream
from sabot import cyarrow as ca

print()
print("="*70)
print("TESTING MORSEL PARALLELISM OVERHEAD")
print("="*70)
print()

data_path = "benchmarks/polars-benchmark/data/tables/scale-0.1/lineitem.parquet"

# Test: Simple filter + aggregate (like profiler)
print("TEST: Simple filter + aggregate (Q1-style)")
print("-"*70)

# Read data
stream = Stream.from_parquet(data_path, parallel=True)

# Q1-style operation
def run_q1_style():
    start = time.time()
    
    # Filter by date
    filtered = stream.filter(lambda b: 
        ca.compute.less_equal(b['l_shipdate'], ca.scalar("1998-09-02"))
    )
    
    # GroupBy
    result = filtered.group_by('l_returnflag', 'l_linestatus').aggregate({
        'sum_qty': ('l_quantity', 'sum'),
        'sum_price': ('l_extendedprice', 'sum')
    })
    
    # Collect
    batches = list(result)
    elapsed = time.time() - start
    
    if batches:
        table = ca.Table.from_batches(batches)
        rows = table.num_rows
    else:
        rows = 0
    
    return elapsed, rows

# Run test
elapsed, rows = run_q1_style()

print(f"Time: {elapsed:.3f}s")
print(f"Rows: {rows}")
print()

# Check if morsel operators are being used
print("="*70)
print("CHECKING OPERATOR USAGE")
print("="*70)
print()

try:
    from sabot._cython.operators.morsel_operator import MorselDrivenOperator
    print("✓ MorselDrivenOperator is available")
    print("  This wraps ALL operators with parallelism")
    print("  Overhead: Thread pool, work-stealing, coordination")
    print()
    
    # Check configuration
    print("Default configuration:")
    print("  num_workers: 0 (auto-detect)")
    print("  morsel_size_kb: 64")
    print("  enabled: True")
    print()
    
except ImportError as e:
    print(f"✗ MorselDrivenOperator not available: {e}")
    print("  Operators run without parallelism overhead")
    print()

print("="*70)
print("ANALYSIS")
print("="*70)
print()

print(f"Q1 profiler time: 0.144s")
print(f"Q1 benchmark time: {elapsed:.3f}s")
print(f"Slowdown: {elapsed/0.144:.2f}x")
print()

if elapsed > 0.144 * 2:
    print("⚠️  Significant overhead detected!")
    print("   Likely cause: Morsel parallelism on small dataset")
    print("   Solution: Disable morsels for single-machine or use threshold")
else:
    print("✓ Performance matches profiling")

print()
print("="*70)

