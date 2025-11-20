#!/usr/bin/env python3
"""
Complete TPC-H Benchmark - All Engines, All Queries
Compares Sabot vs Polars vs DuckDB vs PySpark on all 22 TPC-H queries
"""

import time
import sys
import json
from pathlib import Path
from datetime import datetime
import importlib

# Add benchmark path
sys.path.insert(0, str(Path(__file__).parent / "polars-benchmark"))

print()
print("="*80)
print("COMPLETE TPC-H BENCHMARK - ALL ENGINES")
print("Testing: Sabot, Polars, DuckDB, PySpark")
print("Queries: 1-22 (complete suite)")
print("="*80)
print()

# Configuration
SCALE = 0.1  # Dataset scale factor
ALL_QUERIES = list(range(1, 23))
ENGINES = ['sabot_native', 'polars', 'duckdb']  # PySpark requires setup

results = {
    'scale': SCALE,
    'timestamp': datetime.now().isoformat(),
    'engines': {}
}

def run_engine_queries(engine_name, query_list):
    """Run all queries for a given engine"""
    engine_results = []
    
    print(f"\n{'='*80}")
    print(f"ENGINE: {engine_name.upper()}")
    print(f"{'='*80}\n")
    
    for q_num in query_list:
        try:
            module_name = f"queries.{engine_name}.q{q_num}"
            module = importlib.import_module(module_name)
            
            print(f"Q{q_num:02d}: ", end='', flush=True)
            start = time.time()
            
            try:
                # Run the query
                if engine_name == 'sabot_native':
                    result_stream = module.q()
                    batches = list(result_stream)
                    elapsed = time.time() - start
                    
                    if batches:
                        from sabot import cyarrow as ca
                        table = ca.Table.from_batches(batches)
                        rows = table.num_rows
                    else:
                        rows = 0
                else:
                    # Polars/DuckDB
                    result = module.q()
                    if hasattr(result, 'collect'):
                        df = result.collect()
                        elapsed = time.time() - start
                        rows = len(df)
                    else:
                        elapsed = time.time() - start
                        rows = 0
                
                print(f"{elapsed:.3f}s ({rows} rows) ✓")
                
                engine_results.append({
                    'query': q_num,
                    'time': elapsed,
                    'rows': rows,
                    'status': 'OK'
                })
                
            except Exception as e:
                elapsed = time.time() - start
                error = str(e)[:50]
                print(f"✗ {error}")
                
                engine_results.append({
                    'query': q_num,
                    'time': elapsed,
                    'rows': 0,
                    'status': f'ERROR: {error}'
                })
        
        except ImportError:
            print(f"Q{q_num:02d}: ✗ Not implemented")
            engine_results.append({
                'query': q_num,
                'time': 0,
                'rows': 0,
                'status': 'NOT_IMPL'
            })
    
    return engine_results

# Run benchmarks for each engine
for engine in ENGINES:
    results['engines'][engine] = run_engine_queries(engine, ALL_QUERIES)

# Summary
print()
print("="*80)
print("SUMMARY - ALL ENGINES")
print("="*80)
print()

# Comparison table
print(f"{'Query':<8}", end='')
for engine in ENGINES:
    print(f"{engine.upper():<20}", end='')
print("Winner")
print("-"*80)

for q_num in ALL_QUERIES:
    print(f"Q{q_num:02d}      ", end='')
    
    times = {}
    for engine in ENGINES:
        engine_data = results['engines'][engine]
        q_result = next((r for r in engine_data if r['query'] == q_num), None)
        
        if q_result and q_result['status'] == 'OK' and q_result['time'] > 0:
            times[engine] = q_result['time']
            print(f"{q_result['time']:>6.3f}s          ", end='')
        else:
            print(f"{'N/A':<18}", end='')
    
    # Determine winner
    if times:
        winner = min(times.keys(), key=lambda k: times[k])
        print(f"  {winner}")
    else:
        print(f"  -")

print()

# Statistics per engine
print("="*80)
print("ENGINE STATISTICS")
print("="*80)
print()

for engine in ENGINES:
    engine_data = results['engines'][engine]
    successful = [r for r in engine_data if r['status'] == 'OK' and r['time'] > 0]
    
    if successful:
        avg_time = sum(r['time'] for r in successful) / len(successful)
        total_time = sum(r['time'] for r in successful)
        
        print(f"{engine.upper()}:")
        print(f"  Success: {len(successful)}/{len(ALL_QUERIES)} ({len(successful)/22*100:.1f}%)")
        print(f"  Average: {avg_time:.3f}s")
        print(f"  Total:   {total_time:.3f}s")
        print()

# Save results to JSON
output_file = Path(__file__).parent / f"tpch_comparison_scale_{SCALE}.json"
with open(output_file, 'w') as f:
    json.dump(results, f, indent=2)

print(f"Results saved to: {output_file}")
print()

# Winner analysis
print("="*80)
print("WINNER ANALYSIS")
print("="*80)
print()

wins = {engine: 0 for engine in ENGINES}
for q_num in ALL_QUERIES:
    times = {}
    for engine in ENGINES:
        engine_data = results['engines'][engine]
        q_result = next((r for r in engine_data if r['query'] == q_num), None)
        if q_result and q_result['status'] == 'OK' and q_result['time'] > 0:
            times[engine] = q_result['time']
    
    if times:
        winner = min(times.keys(), key=lambda k: times[k])
        wins[winner] += 1

for engine in ENGINES:
    print(f"{engine.upper()}: {wins[engine]}/22 wins ({wins[engine]/22*100:.1f}%)")

print()
print("="*80)

