#!/usr/bin/env python3
"""
Compare Sabot Against All Engines

Runs TPC-H Q6 on all available engines and compares.
"""

import subprocess
import time

engines_to_test = [
    ('polars', 'Polars (Rust)'),
    ('duckdb', 'DuckDB (C++)'),
    ('pyspark', 'PySpark (JVM)'),
    ('sabot_spark', 'Sabot (via Spark)'),
    ('pandas', 'pandas (Python)'),
]

print("="*80)
print("TPC-H Q6 - Multi-Engine Comparison")
print("="*80)
print()

results = {}

for engine, desc in engines_to_test:
    print(f"{engine:15} {desc:25} ", end="", flush=True)
    
    cmd = f"cd benchmarks/polars-benchmark && export SCALE_FACTOR=0.1 && timeout 30 python3 -m queries.{engine}.q6 2>&1"
    
    try:
        start = time.perf_counter()
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        elapsed = time.perf_counter() - start
        
        if result.returncode == 0:
            print(f"{elapsed:.2f}s ✓")
            results[engine] = elapsed
        else:
            print(f"FAILED")
    except:
        print(f"ERROR")

print()
print("="*80)
print("RESULTS - TPC-H Q6")
print("="*80)
print()

if results:
    sorted_results = sorted(results.items(), key=lambda x: x[1])
    fastest = sorted_results[0][1]
    
    print(f"{'Engine':<15} {'Time':<10} {'vs Fastest':<15} {'vs PySpark'}")
    print("-"*80)
    
    pyspark_time = results.get('pyspark', 0)
    
    for engine, time_val in sorted_results:
        vs_fastest = time_val / fastest
        vs_pyspark = pyspark_time / time_val if pyspark_time and engine != 'pyspark' else 1.0
        
        engine_name = dict(engines_to_test)[engine]
        
        print(f"{engine_name:<15} {time_val:>6.2f}s    "
              f"{vs_fastest:>6.2f}x slower    "
              f"{vs_pyspark:>6.2f}x faster" if vs_pyspark > 1 else f"{'':>15}")
    
    print()
    print("="*80)
    print()
    
    if 'sabot_spark' in results and 'pyspark' in results:
        speedup = results['pyspark'] / results['sabot_spark']
        print(f"SABOT vs PYSPARK: {speedup:.1f}x faster ✅")
    
    if 'sabot_spark' in results and 'polars' in results:
        ratio = results['sabot_spark'] / results['polars']
        print(f"SABOT vs POLARS:  {ratio:.2f}x {'faster' if ratio < 1 else 'slower'}")
    
    if 'sabot_spark' in results and 'duckdb' in results:
        ratio = results['sabot_spark'] / results['duckdb']
        print(f"SABOT vs DUCKDB:  {ratio:.2f}x {'faster' if ratio < 1 else 'slower'}")

