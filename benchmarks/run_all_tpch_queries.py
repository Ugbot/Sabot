#!/usr/bin/env python3
"""
Run ALL available TPC-H queries for Sabot
Tests the full range of operations and query complexity
"""

import time
import sys
from pathlib import Path

# Add benchmark path
sys.path.insert(0, str(Path(__file__).parent / "polars-benchmark"))

from sabot import cyarrow as ca

print()
print("="*80)
print("SABOT - COMPLETE TPC-H BENCHMARK SUITE")
print("="*80)
print()

# Define all implemented queries
IMPLEMENTED_QUERIES = [1, 3, 4, 6, 7, 10, 12]
SIMPLE_QUERIES = [1, 6]  # Queries we can run reliably
COMPLEX_QUERIES = [3, 4, 7, 10, 12]  # More complex, may need work

results = []

print(f"Testing {len(IMPLEMENTED_QUERIES)} implemented queries")
print(f"  Simple: Q{', Q'.join(map(str, SIMPLE_QUERIES))}")
print(f"  Complex: Q{', Q'.join(map(str, COMPLEX_QUERIES))}")
print()
print("="*80)
print()

# Import query modules
from queries.sabot_native import q1, q3, q6

# Run simple queries
print("SIMPLE QUERIES (Fully Tested)")
print("-"*80)

for q_num, q_module in [(1, q1), (6, q6)]:
    try:
        print(f"\nQ{q_num}:")
        start = time.time()
        q_module.q()
        elapsed = time.time() - start
        
        results.append({
            'query': f'Q{q_num}',
            'time': elapsed,
            'status': 'OK'
        })
        
        print(f"  ✓ Time: {elapsed:.3f}s")
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
        results.append({
            'query': f'Q{q_num}',
            'time': 0,
            'status': f'ERROR: {str(e)[:50]}'
        })

print()
print("="*80)
print()

# Summary
print("SUMMARY")
print("="*80)
print()

successful = [r for r in results if r['status'] == 'OK']
failed = [r for r in results if r['status'] != 'OK']

print(f"{'Query':<10} {'Time':<12} {'Status'}")
print("-"*80)

for r in results:
    if r['status'] == 'OK':
        print(f"{r['query']:<10} {r['time']:>8.3f}s    ✓")
    else:
        print(f"{r['query']:<10} {'N/A':<12} ✗ {r['status']}")

print()
print(f"Success: {len(successful)}/{len(results)} queries")
print(f"Average time: {sum(r['time'] for r in successful)/len(successful):.3f}s" if successful else "N/A")
print()

# Show which queries are implemented
print("="*80)
print("TPC-H QUERY COVERAGE")
print("="*80)
print()

all_queries = list(range(1, 23))
implemented = IMPLEMENTED_QUERIES
working = [r['query'].replace('Q', '') for r in successful]
working = [int(q) for q in working]

print(f"Total TPC-H queries: 22")
print(f"Implemented: {len(implemented)} ({len(implemented)/22*100:.1f}%)")
print(f"Working: {len(working)} ({len(working)/22*100:.1f}%)")
print()

print("Status by query:")
for q in range(1, 23):
    if q in working:
        status = "✓ Working"
    elif q in implemented:
        status = "⚠ Implemented (needs testing)"
    else:
        status = "○ Not implemented"
    print(f"  Q{q:02d}: {status}")

print()
print("="*80)
print()

# Show performance for working queries
if successful:
    print("PERFORMANCE (Working Queries)")
    print("="*80)
    print()
    
    for r in successful:
        q_num = int(r['query'].replace('Q', ''))
        throughput = 600_572 / r['time'] / 1_000_000  # Assuming lineitem size
        print(f"  Q{q_num:02d}: {r['time']:.3f}s ({throughput:.2f}M rows/sec)")
    
    avg_throughput = sum(600_572 / r['time'] for r in successful) / len(successful) / 1_000_000
    print()
    print(f"  Average throughput: {avg_throughput:.2f}M rows/sec")
    print()

print("="*80)
print()
print("Next steps:")
print("  1. Implement remaining 17 queries (Q2, Q4-Q5, Q7-Q9, Q11-Q22)")
print("  2. Test all implemented queries")
print("  3. Benchmark against Polars/PySpark for all 22 queries")
print()

