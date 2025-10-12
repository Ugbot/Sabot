"""
REAL Benchmark - Using Actual Sabot Cython Operators

This benchmark uses the ACTUAL Sabot operators:
- CythonHashJoinOperator (104M rows/sec)
- CythonGroupByOperator
- CyArrow (optimized Arrow with zero-copy)

NO SIMULATION - This is the real deal!
"""

import sys
import time
import os
from pathlib import Path

# Set library path for imports
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib:/Users/bengamble/Sabot/vendor/tonbo/tonbo-ffi/target/release/deps:' + os.environ.get('DYLD_LIBRARY_PATH', '')

print("="*70)
print("🚀 REAL SABOT OPERATOR BENCHMARK")
print("="*70)
print()

# Import real Sabot operators
print("Importing Sabot operators...")
try:
    from sabot._cython.operators.joins import CythonHashJoinOperator
    from sabot._cython.operators.aggregations import CythonGroupByOperator
    from sabot._cython.operators.transform import CythonFilterOperator, CythonMapOperator
    from sabot import cyarrow as ca
    print("✅ All Sabot operators imported successfully!")
    print(f"✅ CyArrow zero-copy: {ca.USING_ZERO_COPY}")
    print()
except ImportError as e:
    print(f"❌ Failed to import Sabot operators: {e}")
    sys.exit(1)

def create_test_data(size=100000):
    """Create test data using cyarrow"""
    print(f"Creating test data ({size:,} rows)...")
    
    # Create left table
    left_data = {
        'id': list(range(size)),
        'value': list(range(size)),
        'category': [f'cat_{i % 10}' for i in range(size)]
    }
    left_table = ca.table(left_data)
    
    # Create right table (smaller)
    right_size = size // 2
    right_data = {
        'id': list(range(right_size)),
        'data': list(range(right_size)),
        'status': [f'status_{i % 5}' for i in range(right_size)]
    }
    right_table = ca.table(right_data)
    
    print(f"✅ Created left table: {left_table.num_rows:,} rows")
    print(f"✅ Created right table: {right_table.num_rows:,} rows")
    print()
    
    return left_table, right_table

def benchmark_hash_join(left_table, right_table):
    """Benchmark the REAL CythonHashJoinOperator"""
    print("🔗 Benchmarking CythonHashJoinOperator...")
    
    # Convert to batches for the operator
    left_batches = list(left_table.to_batches())
    right_batches = list(right_table.to_batches())
    
    # Create the REAL operator
    join_op = CythonHashJoinOperator(
        left_source=iter(left_batches),
        right_source=iter(right_batches),
        left_keys=['id'],
        right_keys=['id']
    )
    
    # Benchmark execution
    start_time = time.perf_counter()
    
    result_batches = []
    for batch in join_op:
        result_batches.append(batch)
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    
    # Calculate results
    total_rows = left_table.num_rows + right_table.num_rows
    throughput = total_rows / elapsed / 1e6  # M rows/sec
    
    print(f"✅ Hash join completed in {elapsed*1000:.1f}ms")
    print(f"✅ Throughput: {throughput:.1f}M rows/sec")
    print(f"✅ Result batches: {len(result_batches)}")
    
    if result_batches:
        total_result_rows = sum(batch.num_rows for batch in result_batches)
        print(f"✅ Result rows: {total_result_rows:,}")
    
    print()
    return elapsed, throughput

def benchmark_groupby(table):
    """Benchmark the REAL CythonGroupByOperator"""
    print("📊 Benchmarking CythonGroupByOperator...")
    
    # Convert to batches
    batches = list(table.to_batches())
    
    # Create the REAL operator
    groupby_op = CythonGroupByOperator(
        source=iter(batches),
        group_keys=['category'],
        agg_expressions=['sum(value)']
    )
    
    # Benchmark execution
    start_time = time.perf_counter()
    
    result_batches = []
    for batch in groupby_op:
        result_batches.append(batch)
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    
    # Calculate results
    throughput = table.num_rows / elapsed / 1e6  # M rows/sec
    
    print(f"✅ GroupBy completed in {elapsed*1000:.1f}ms")
    print(f"✅ Throughput: {throughput:.1f}M rows/sec")
    print(f"✅ Result batches: {len(result_batches)}")
    
    if result_batches:
        total_result_rows = sum(batch.num_rows for batch in result_batches)
        print(f"✅ Result rows: {total_result_rows:,}")
    
    print()
    return elapsed, throughput

def benchmark_filter(table):
    """Benchmark the REAL CythonFilterOperator"""
    print("🔍 Benchmarking CythonFilterOperator...")
    
    # Convert to batches
    batches = list(table.to_batches())
    
    # Create the REAL operator
    filter_op = CythonFilterOperator(
        source=iter(batches),
        predicate="value > 50000"  # Filter condition
    )
    
    # Benchmark execution
    start_time = time.perf_counter()
    
    result_batches = []
    for batch in filter_op:
        result_batches.append(batch)
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    
    # Calculate results
    throughput = table.num_rows / elapsed / 1e6  # M rows/sec
    
    print(f"✅ Filter completed in {elapsed*1000:.1f}ms")
    print(f"✅ Throughput: {throughput:.1f}M rows/sec")
    print(f"✅ Result batches: {len(result_batches)}")
    
    if result_batches:
        total_result_rows = sum(batch.num_rows for batch in result_batches)
        print(f"✅ Result rows: {total_result_rows:,}")
    
    print()
    return elapsed, throughput

def main():
    """Run comprehensive benchmark"""
    print("Starting REAL Sabot operator benchmark...")
    print()
    
    # Test different sizes
    sizes = [10000, 100000, 1000000]
    
    results = {}
    
    for size in sizes:
        print(f"📏 Testing with {size:,} rows")
        print("-" * 50)
        
        # Create test data
        left_table, right_table = create_test_data(size)
        
        # Run benchmarks
        join_time, join_throughput = benchmark_hash_join(left_table, right_table)
        groupby_time, groupby_throughput = benchmark_groupby(left_table)
        filter_time, filter_throughput = benchmark_filter(left_table)
        
        # Store results
        results[size] = {
            'hash_join': {'time': join_time, 'throughput': join_throughput},
            'groupby': {'time': groupby_time, 'throughput': groupby_throughput},
            'filter': {'time': filter_time, 'throughput': filter_throughput}
        }
        
        print("="*50)
        print()
    
    # Print summary
    print("📊 BENCHMARK SUMMARY")
    print("="*70)
    print(f"{'Size':>10} {'Hash Join':>15} {'GroupBy':>15} {'Filter':>15}")
    print(f"{'':>10} {'(M rows/sec)':>15} {'(M rows/sec)':>15} {'(M rows/sec)':>15}")
    print("-"*70)
    
    for size in sizes:
        r = results[size]
        print(f"{size:>10,} {r['hash_join']['throughput']:>15.1f} {r['groupby']['throughput']:>15.1f} {r['filter']['throughput']:>15.1f}")
    
    print("="*70)
    print()
    
    # Performance analysis
    print("🎯 PERFORMANCE ANALYSIS")
    print("="*70)
    
    # Find best performance
    best_join = max(results[s]['hash_join']['throughput'] for s in sizes)
    best_groupby = max(results[s]['groupby']['throughput'] for s in sizes)
    best_filter = max(results[s]['filter']['throughput'] for s in sizes)
    
    print(f"🚀 Best Hash Join: {best_join:.1f}M rows/sec")
    print(f"🚀 Best GroupBy: {best_groupby:.1f}M rows/sec")
    print(f"🚀 Best Filter: {best_filter:.1f}M rows/sec")
    print()
    
    # Compare to expected performance
    expected_join = 104.0  # M rows/sec from previous benchmarks
    expected_groupby = 50.0  # Estimated
    expected_filter = 200.0  # Estimated
    
    print("📈 Performance vs Expected:")
    print(f"   Hash Join: {best_join/expected_join*100:.1f}% of expected")
    print(f"   GroupBy: {best_groupby/expected_groupby*100:.1f}% of expected")
    print(f"   Filter: {best_filter/expected_filter*100:.1f}% of expected")
    print()
    
    print("✅ REAL BENCHMARK COMPLETE!")
    print("This used actual Sabot Cython operators - no simulation!")

if __name__ == "__main__":
    main()
