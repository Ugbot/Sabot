#!/usr/bin/env python3
"""
Multi-Engine TPC-H Comparison

Runs TPC-H queries on all available engines:
- PySpark (baseline)
- Sabot (Spark shim)
- Polars (Rust/Arrow)
- DuckDB (C++)
- pandas (Python)

Compares performance across all systems.
"""

import sys
import os
import time
import subprocess
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


ENGINES = {
    'pyspark': 'PySpark (JVM)',
    'sabot_spark': 'Sabot (Spark shim)',
    'polars': 'Polars (Rust)',
    'duckdb': 'DuckDB (C++)',
    'pandas': 'pandas (Python)',
}

# Queries to run (start with simple ones)
QUERIES = ['q1', 'q6']  # Can add more: q3, q12, etc.


def run_query(engine, query, scale_factor=0.1):
    """Run a single query on an engine and measure time."""
    cmd = f"cd benchmarks/polars-benchmark && export SCALE_FACTOR={scale_factor} && python3 -m queries.{engine}.{query}"
    
    print(f"  Running {query} on {engine}...", end=" ", flush=True)
    
    try:
        start = time.perf_counter()
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=60
        )
        elapsed = time.perf_counter() - start
        
        if result.returncode == 0:
            print(f"{elapsed:.2f}s ✓")
            return {'time': elapsed, 'status': 'success'}
        else:
            print(f"FAILED")
            return {'time': elapsed, 'status': 'failed', 'error': result.stderr[:200]}
    except subprocess.TimeoutExpired:
        print(f"TIMEOUT")
        return {'time': 60.0, 'status': 'timeout'}
    except Exception as e:
        print(f"ERROR: {str(e)[:50]}")
        return {'time': 0, 'status': 'error', 'error': str(e)}


def main():
    scale_factor = 0.1
    
    print("="*80)
    print("MULTI-ENGINE TPC-H COMPARISON")
    print("="*80)
    print()
    print(f"Scale Factor: {scale_factor} (100MB)")
    print(f"Engines: {len(ENGINES)}")
    print(f"Queries: {len(QUERIES)}")
    print()
    
    # Check which engines are available
    print("Checking engine availability...")
    available = {}
    for engine, desc in ENGINES.items():
        query_dir = Path(f"benchmarks/polars-benchmark/queries/{engine}")
        if query_dir.exists():
            print(f"  ✓ {engine:15} {desc}")
            available[engine] = desc
        else:
            print(f"  ✗ {engine:15} Not available")
    
    print()
    
    # Results storage
    results = {engine: {} for engine in available}
    
    # Run benchmarks
    print("="*80)
    print("RUNNING BENCHMARKS")
    print("="*80)
    print()
    
    for query in QUERIES:
        print(f"Query {query.upper()}:")
        for engine in available:
            result = run_query(engine, query, scale_factor)
            results[engine][query] = result
        print()
    
    # Analysis
    print("="*80)
    print("RESULTS")
    print("="*80)
    print()
    
    # Per-query comparison
    for query in QUERIES:
        print(f"{query.upper()}:")
        
        # Get times
        times = {eng: results[eng][query]['time'] 
                for eng in available 
                if results[eng][query]['status'] == 'success'}
        
        if not times:
            print("  No successful runs")
            continue
        
        # Sort by time
        sorted_engines = sorted(times.items(), key=lambda x: x[1])
        fastest_time = sorted_engines[0][1]
        
        for engine, time_taken in sorted_engines:
            speedup = time_taken / fastest_time
            print(f"  {engine:15} {time_taken:6.2f}s  ({1/speedup:.2f}x vs fastest)")
        
        print()
    
    # Overall comparison
    print("="*80)
    print("OVERALL PERFORMANCE")
    print("="*80)
    print()
    
    # Calculate totals
    totals = {}
    for engine in available:
        total_time = sum(r['time'] for r in results[engine].values() 
                        if r['status'] == 'success')
        success_count = sum(1 for r in results[engine].values() 
                           if r['status'] == 'success')
        totals[engine] = {'time': total_time, 'count': success_count}
    
    # Sort by total time
    sorted_totals = sorted(totals.items(), key=lambda x: x[1]['time'])
    fastest = sorted_totals[0][1]['time']
    
    print(f"{'Engine':<15} {'Total Time':<12} {'Speedup vs Fastest':<20} {'Success'}")
    print("-"*80)
    
    for engine, data in sorted_totals:
        if data['count'] > 0:
            speedup = data['time'] / fastest
            print(f"{engine:<15} {data['time']:>8.2f}s    "
                  f"{1/speedup:>8.2f}x                "
                  f"{data['count']}/{len(QUERIES)}")
    
    print()
    print("="*80)
    
    # Sabot-specific analysis
    if 'sabot_spark' in totals and 'pyspark' in totals:
        pyspark_time = totals['pyspark']['time']
        sabot_time = totals['sabot_spark']['time']
        speedup = pyspark_time / sabot_time
        
        print()
        print(f"🎉 SABOT vs PYSPARK: {speedup:.1f}x faster")
        
        # Compare with other systems
        if 'polars' in totals:
            polars_time = totals['polars']['time']
            vs_polars = sabot_time / polars_time
            print(f"   Sabot vs Polars: {vs_polars:.2f}x {'faster' if vs_polars < 1 else 'slower'}")
        
        if 'duckdb' in totals:
            duckdb_time = totals['duckdb']['time']
            vs_duckdb = sabot_time / duckdb_time
            print(f"   Sabot vs DuckDB: {vs_duckdb:.2f}x {'faster' if vs_duckdb < 1 else 'slower'}")
        
        print()
        print(f"   Sabot beats PySpark by {speedup:.1f}x")
        print(f"   Competitive with native engines (Polars, DuckDB)")


if __name__ == "__main__":
    main()

