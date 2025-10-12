"""
SabotSQL vs DuckDB Benchmark

This benchmark compares Sabot's SQL pipeline using real Cython operators
against DuckDB's native execution engine.

Uses:
- Real Sabot operators (CythonHashJoinOperator, CythonGroupByOperator, etc.)
- CyArrow for optimized Arrow processing
- DuckDB for SQL parsing and optimization
- Real performance measurement (no simulation)
"""

import sys
import time
import os
import numpy as np
from pathlib import Path

# Set library path for Sabot imports
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib:/Users/bengamble/Sabot/vendor/tonbo/tonbo-ffi/target/release/deps:' + os.environ.get('DYLD_LIBRARY_PATH', '')

print("="*80)
print("🚀 SABOT SQL vs DUCKDB BENCHMARK")
print("="*80)
print()

# Import Sabot operators
try:
    from sabot._cython.operators.joins import CythonHashJoinOperator
    from sabot._cython.operators.aggregations import CythonGroupByOperator
    from sabot._cython.operators.transform import CythonFilterOperator, CythonMapOperator
    from sabot import cyarrow as ca
    print("✅ Sabot operators imported successfully!")
    print(f"✅ CyArrow zero-copy: {ca.USING_ZERO_COPY}")
    print()
except ImportError as e:
    print(f"❌ Failed to import Sabot operators: {e}")
    sys.exit(1)

# Import DuckDB for comparison
try:
    import duckdb
    print("✅ DuckDB imported successfully!")
    print()
except ImportError as e:
    print(f"❌ Failed to import DuckDB: {e}")
    print("Install with: pip install duckdb")
    sys.exit(1)

def create_test_data(size=1000000):
    """Create realistic test data"""
    print(f"Creating test data ({size:,} rows)...")
    
    # Create customers table
    customers_data = {
        'customer_id': list(range(size)),
        'name': [f'Customer_{i}' for i in range(size)],
        'city': [f'City_{i % 100}' for i in range(size)],
        'segment': [f'Segment_{i % 5}' for i in range(size)]
    }
    customers_table = ca.table(customers_data)
    
    # Create orders table (smaller, realistic ratio)
    orders_size = size // 10
    orders_data = {
        'order_id': list(range(orders_size)),
        'customer_id': np.random.randint(0, size, orders_size).tolist(),
        'amount': np.random.uniform(10, 1000, orders_size).tolist(),
        'status': [f'Status_{i % 3}' for i in range(orders_size)]
    }
    orders_table = ca.table(orders_data)
    
    print(f"✅ Created customers table: {customers_table.num_rows:,} rows")
    print(f"✅ Created orders table: {orders_table.num_rows:,} rows")
    print()
    
    return customers_table, orders_table

def benchmark_sabot_hash_join(left_table, right_table):
    """Benchmark Sabot's CythonHashJoinOperator"""
    print("🔗 Benchmarking Sabot Hash Join...")
    
    # Convert to batches
    left_batches = list(left_table.to_batches())
    right_batches = list(right_table.to_batches())
    
    # Create operator with correct signature
    join_op = CythonHashJoinOperator(
        source=iter(left_batches),  # BaseOperator expects 'source'
        left_source=iter(left_batches),
        right_source=iter(right_batches),
        left_keys=['customer_id'],
        right_keys=['customer_id'],
        join_type='inner',
        partition_keys=['customer_id'],  # ShuffledOperator expects this
        num_partitions=4
    )
    
    # Benchmark execution
    start_time = time.perf_counter()
    
    result_batches = []
    for batch in join_op:
        result_batches.append(batch)
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    
    # Calculate throughput
    total_rows = left_table.num_rows + right_table.num_rows
    throughput = total_rows / elapsed / 1e6  # M rows/sec
    
    result_rows = sum(batch.num_rows for batch in result_batches)
    
    print(f"✅ Sabot Hash Join: {elapsed*1000:.1f}ms")
    print(f"✅ Throughput: {throughput:.1f}M rows/sec")
    print(f"✅ Result rows: {result_rows:,}")
    print()
    
    return elapsed, throughput, result_rows

def benchmark_sabot_groupby(table):
    """Benchmark Sabot's CythonGroupByOperator"""
    print("📊 Benchmarking Sabot GroupBy...")
    
    # Convert to batches
    batches = list(table.to_batches())
    
    # Create operator with correct signature
    groupby_op = CythonGroupByOperator(
        source=iter(batches),
        keys=['status'],  # Group by status column
        aggregations={
            'count': ('order_id', 'count'),
            'total_amount': ('amount', 'sum')
        },
        partition_keys=['status'],  # ShuffledOperator expects this
        num_partitions=4
    )
    
    # Benchmark execution
    start_time = time.perf_counter()
    
    result_batches = []
    for batch in groupby_op:
        result_batches.append(batch)
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    
    # Calculate throughput
    throughput = table.num_rows / elapsed / 1e6  # M rows/sec
    result_rows = sum(batch.num_rows for batch in result_batches)
    
    print(f"✅ Sabot GroupBy: {elapsed*1000:.1f}ms")
    print(f"✅ Throughput: {throughput:.1f}M rows/sec")
    print(f"✅ Result rows: {result_rows:,}")
    print()
    
    return elapsed, throughput, result_rows

def benchmark_duckdb_join(customers_table, orders_table):
    """Benchmark DuckDB's native hash join"""
    print("🔗 Benchmarking DuckDB Hash Join...")
    
    # Create DuckDB connection
    conn = duckdb.connect()
    
    # Register tables
    conn.register('customers', customers_table)
    conn.register('orders', orders_table)
    
    # Benchmark execution
    start_time = time.perf_counter()
    
    result = conn.execute("""
        SELECT c.customer_id, c.name, o.order_id, o.amount
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
    """).fetchall()
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    
    # Calculate throughput
    total_rows = customers_table.num_rows + orders_table.num_rows
    throughput = total_rows / elapsed / 1e6  # M rows/sec
    result_rows = len(result)
    
    print(f"✅ DuckDB Hash Join: {elapsed*1000:.1f}ms")
    print(f"✅ Throughput: {throughput:.1f}M rows/sec")
    print(f"✅ Result rows: {result_rows:,}")
    print()
    
    conn.close()
    return elapsed, throughput, result_rows

def benchmark_duckdb_groupby(orders_table):
    """Benchmark DuckDB's native group by"""
    print("📊 Benchmarking DuckDB GroupBy...")
    
    # Create DuckDB connection
    conn = duckdb.connect()
    
    # Register table
    conn.register('orders', orders_table)
    
    # Benchmark execution
    start_time = time.perf_counter()
    
    result = conn.execute("""
        SELECT status, COUNT(*) as count, SUM(amount) as total_amount
        FROM orders
        GROUP BY status
    """).fetchall()
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    
    # Calculate throughput
    throughput = orders_table.num_rows / elapsed / 1e6  # M rows/sec
    result_rows = len(result)
    
    print(f"✅ DuckDB GroupBy: {elapsed*1000:.1f}ms")
    print(f"✅ Throughput: {throughput:.1f}M rows/sec")
    print(f"✅ Result rows: {result_rows:,}")
    print()
    
    conn.close()
    return elapsed, throughput, result_rows

def benchmark_sabot_filter(table):
    """Benchmark Sabot's CythonFilterOperator"""
    print("🔍 Benchmarking Sabot Filter...")
    
    # Convert to batches
    batches = list(table.to_batches())
    
    # Create filter predicate (amount > 500)
    def filter_predicate(batch):
        # Simple filter: keep rows where amount > 500
        amount_col = batch.column('amount')
        if hasattr(amount_col, 'to_pylist'):
            amounts = amount_col.to_pylist()
            mask = [amount > 500 for amount in amounts]
        else:
            # Fallback for different Arrow types
            mask = [True] * batch.num_rows
        return mask
    
    # Create operator with correct signature
    filter_op = CythonFilterOperator(
        source=iter(batches),
        predicate=filter_predicate
    )
    
    # Benchmark execution
    start_time = time.perf_counter()
    
    result_batches = []
    for batch in filter_op:
        result_batches.append(batch)
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    
    # Calculate throughput
    throughput = table.num_rows / elapsed / 1e6  # M rows/sec
    result_rows = sum(batch.num_rows for batch in result_batches)
    
    print(f"✅ Sabot Filter: {elapsed*1000:.1f}ms")
    print(f"✅ Throughput: {throughput:.1f}M rows/sec")
    print(f"✅ Result rows: {result_rows:,}")
    print()
    
    return elapsed, throughput, result_rows

def benchmark_duckdb_filter(orders_table):
    """Benchmark DuckDB's native filter"""
    print("🔍 Benchmarking DuckDB Filter...")
    
    # Create DuckDB connection
    conn = duckdb.connect()
    
    # Register table
    conn.register('orders', orders_table)
    
    # Benchmark execution
    start_time = time.perf_counter()
    
    result = conn.execute("""
        SELECT *
        FROM orders
        WHERE amount > 500
    """).fetchall()
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    
    # Calculate throughput
    throughput = orders_table.num_rows / elapsed / 1e6  # M rows/sec
    result_rows = len(result)
    
    print(f"✅ DuckDB Filter: {elapsed*1000:.1f}ms")
    print(f"✅ Throughput: {throughput:.1f}M rows/sec")
    print(f"✅ Result rows: {result_rows:,}")
    print()
    
    conn.close()
    return elapsed, throughput, result_rows

def main():
    """Run comprehensive benchmark"""
    print("Starting SabotSQL vs DuckDB benchmark...")
    print()
    
    # Test different sizes
    sizes = [100000, 1000000, 5000000]
    
    results = {}
    
    for size in sizes:
        print(f"📏 Testing with {size:,} rows")
        print("-" * 60)
        
        # Create test data
        customers_table, orders_table = create_test_data(size)
        
        # Run benchmarks
        print("🔗 HASH JOIN BENCHMARKS")
        print("-" * 30)
        sabot_join_time, sabot_join_throughput, sabot_join_rows = benchmark_sabot_hash_join(customers_table, orders_table)
        duckdb_join_time, duckdb_join_throughput, duckdb_join_rows = benchmark_duckdb_join(customers_table, orders_table)
        
        print("📊 GROUP BY BENCHMARKS")
        print("-" * 30)
        sabot_groupby_time, sabot_groupby_throughput, sabot_groupby_rows = benchmark_sabot_groupby(orders_table)
        duckdb_groupby_time, duckdb_groupby_throughput, duckdb_groupby_rows = benchmark_duckdb_groupby(orders_table)
        
        print("🔍 FILTER BENCHMARKS")
        print("-" * 30)
        sabot_filter_time, sabot_filter_throughput, sabot_filter_rows = benchmark_sabot_filter(orders_table)
        duckdb_filter_time, duckdb_filter_throughput, duckdb_filter_rows = benchmark_duckdb_filter(orders_table)
        
        # Store results
        results[size] = {
            'sabot': {
                'hash_join': {'time': sabot_join_time, 'throughput': sabot_join_throughput, 'rows': sabot_join_rows},
                'groupby': {'time': sabot_groupby_time, 'throughput': sabot_groupby_throughput, 'rows': sabot_groupby_rows},
                'filter': {'time': sabot_filter_time, 'throughput': sabot_filter_throughput, 'rows': sabot_filter_rows}
            },
            'duckdb': {
                'hash_join': {'time': duckdb_join_time, 'throughput': duckdb_join_throughput, 'rows': duckdb_join_rows},
                'groupby': {'time': duckdb_groupby_time, 'throughput': duckdb_groupby_throughput, 'rows': duckdb_groupby_rows},
                'filter': {'time': duckdb_filter_time, 'throughput': duckdb_filter_throughput, 'rows': duckdb_filter_rows}
            }
        }
        
        print("="*60)
        print()
    
    # Print summary
    print("📊 BENCHMARK SUMMARY")
    print("="*80)
    print(f"{'Size':>10} {'Operation':>12} {'Sabot':>15} {'DuckDB':>15} {'Speedup':>15}")
    print(f"{'':>10} {'':>12} {'(M rows/sec)':>15} {'(M rows/sec)':>15} {'(x)':>15}")
    print("-"*80)
    
    for size in sizes:
        r = results[size]
        for op in ['hash_join', 'groupby', 'filter']:
            sabot_tput = r['sabot'][op]['throughput']
            duckdb_tput = r['duckdb'][op]['throughput']
            speedup = sabot_tput / duckdb_tput if duckdb_tput > 0 else 0
            
            print(f"{size:>10,} {op:>12} {sabot_tput:>15.1f} {duckdb_tput:>15.1f} {speedup:>15.2f}")
    
    print("="*80)
    print()
    
    # Performance analysis
    print("🎯 PERFORMANCE ANALYSIS")
    print("="*80)
    
    # Calculate average speedups
    avg_speedups = {}
    for op in ['hash_join', 'groupby', 'filter']:
        speedups = []
        for size in sizes:
            sabot_tput = results[size]['sabot'][op]['throughput']
            duckdb_tput = results[size]['duckdb'][op]['throughput']
            if duckdb_tput > 0:
                speedups.append(sabot_tput / duckdb_tput)
        avg_speedups[op] = np.mean(speedups) if speedups else 0
    
    print("Average Speedup (Sabot vs DuckDB):")
    for op, speedup in avg_speedups.items():
        status = "🚀 FASTER" if speedup > 1.0 else "🐌 SLOWER"
        print(f"  {op:>12}: {speedup:.2f}x {status}")
    
    print()
    
    # Best performance
    best_sabot = max(max(results[s]['sabot'][op]['throughput'] for op in ['hash_join', 'groupby', 'filter']) for s in sizes)
    best_duckdb = max(max(results[s]['duckdb'][op]['throughput'] for op in ['hash_join', 'groupby', 'filter']) for s in sizes)
    
    print(f"🚀 Best Sabot Performance: {best_sabot:.1f}M rows/sec")
    print(f"🚀 Best DuckDB Performance: {best_duckdb:.1f}M rows/sec")
    print(f"🚀 Best Speedup: {best_sabot/best_duckdb:.2f}x")
    print()
    
    print("✅ BENCHMARK COMPLETE!")
    print("This used REAL Sabot Cython operators - no simulation!")

if __name__ == "__main__":
    main()
