"""
Sabot Arrow vs DuckDB Benchmark

This benchmark compares Sabot's CyArrow (optimized Arrow) against DuckDB's native execution.
Uses real Arrow operations instead of complex operator initialization.

Focuses on:
- CyArrow vs PyArrow performance
- Arrow compute kernels vs DuckDB native operations
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
print("🚀 SABOT ARROW vs DUCKDB BENCHMARK")
print("="*80)
print()

# Import Sabot CyArrow
try:
    from sabot import cyarrow as ca
    print("✅ Sabot CyArrow imported successfully!")
    print(f"✅ CyArrow zero-copy: {ca.USING_ZERO_COPY}")
    print()
except ImportError as e:
    print(f"❌ Failed to import Sabot CyArrow: {e}")
    sys.exit(1)

# Import PyArrow for comparison
try:
    import pyarrow as pa
    import pyarrow.compute as pc
    print("✅ PyArrow imported successfully!")
    print()
except ImportError as e:
    print(f"❌ Failed to import PyArrow: {e}")
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

def benchmark_sabot_arrow_filter(table):
    """Benchmark Sabot's CyArrow filter operations"""
    print("🔍 Benchmarking Sabot CyArrow Filter...")
    
    # Benchmark execution
    start_time = time.perf_counter()
    
    # Use Arrow compute for filtering
    mask = ca.compute.greater(table['amount'], ca.scalar(500))
    filtered_table = table.filter(mask)
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    
    # Calculate throughput
    throughput = table.num_rows / elapsed / 1e6  # M rows/sec
    result_rows = filtered_table.num_rows
    
    print(f"✅ Sabot CyArrow Filter: {elapsed*1000:.1f}ms")
    print(f"✅ Throughput: {throughput:.1f}M rows/sec")
    print(f"✅ Result rows: {result_rows:,}")
    print()
    
    return elapsed, throughput, result_rows

def benchmark_pyarrow_filter(table):
    """Benchmark PyArrow filter operations"""
    print("🔍 Benchmarking PyArrow Filter...")
    
    # Convert to PyArrow table
    pyarrow_table = pa.table({
        'order_id': table['order_id'].to_pylist(),
        'customer_id': table['customer_id'].to_pylist(),
        'amount': table['amount'].to_pylist(),
        'status': table['status'].to_pylist()
    })
    
    # Benchmark execution
    start_time = time.perf_counter()
    
    # Use Arrow compute for filtering
    mask = pc.greater(pyarrow_table['amount'], pa.scalar(500))
    filtered_table = pyarrow_table.filter(mask)
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    
    # Calculate throughput
    throughput = pyarrow_table.num_rows / elapsed / 1e6  # M rows/sec
    result_rows = filtered_table.num_rows
    
    print(f"✅ PyArrow Filter: {elapsed*1000:.1f}ms")
    print(f"✅ Throughput: {throughput:.1f}M rows/sec")
    print(f"✅ Result rows: {result_rows:,}")
    print()
    
    return elapsed, throughput, result_rows

def benchmark_sabot_arrow_groupby(table):
    """Benchmark Sabot's CyArrow group by operations"""
    print("📊 Benchmarking Sabot CyArrow GroupBy...")
    
    # Benchmark execution
    start_time = time.perf_counter()
    
    # Use Arrow compute for grouping
    grouped = table.group_by('status').aggregate([
        ('order_id', 'count'),
        ('amount', 'sum')
    ])
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    
    # Calculate throughput
    throughput = table.num_rows / elapsed / 1e6  # M rows/sec
    result_rows = grouped.num_rows
    
    print(f"✅ Sabot CyArrow GroupBy: {elapsed*1000:.1f}ms")
    print(f"✅ Throughput: {throughput:.1f}M rows/sec")
    print(f"✅ Result rows: {result_rows:,}")
    print()
    
    return elapsed, throughput, result_rows

def benchmark_pyarrow_groupby(table):
    """Benchmark PyArrow group by operations"""
    print("📊 Benchmarking PyArrow GroupBy...")
    
    # Convert to PyArrow table
    pyarrow_table = pa.table({
        'order_id': table['order_id'].to_pylist(),
        'customer_id': table['customer_id'].to_pylist(),
        'amount': table['amount'].to_pylist(),
        'status': table['status'].to_pylist()
    })
    
    # Benchmark execution
    start_time = time.perf_counter()
    
    # Use Arrow compute for grouping
    grouped = pyarrow_table.group_by('status').aggregate([
        ('order_id', 'count'),
        ('amount', 'sum')
    ])
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    
    # Calculate throughput
    throughput = pyarrow_table.num_rows / elapsed / 1e6  # M rows/sec
    result_rows = grouped.num_rows
    
    print(f"✅ PyArrow GroupBy: {elapsed*1000:.1f}ms")
    print(f"✅ Throughput: {throughput:.1f}M rows/sec")
    print(f"✅ Result rows: {result_rows:,}")
    print()
    
    return elapsed, throughput, result_rows

def benchmark_sabot_arrow_join(left_table, right_table):
    """Benchmark Sabot's CyArrow join operations"""
    print("🔗 Benchmarking Sabot CyArrow Join...")
    
    # Benchmark execution
    start_time = time.perf_counter()
    
    # Use Arrow compute for joining
    joined = left_table.join(right_table, 'customer_id', 'customer_id', 'inner')
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    
    # Calculate throughput
    total_rows = left_table.num_rows + right_table.num_rows
    throughput = total_rows / elapsed / 1e6  # M rows/sec
    result_rows = joined.num_rows
    
    print(f"✅ Sabot CyArrow Join: {elapsed*1000:.1f}ms")
    print(f"✅ Throughput: {throughput:.1f}M rows/sec")
    print(f"✅ Result rows: {result_rows:,}")
    print()
    
    return elapsed, throughput, result_rows

def benchmark_pyarrow_join(left_table, right_table):
    """Benchmark PyArrow join operations"""
    print("🔗 Benchmarking PyArrow Join...")
    
    # Convert to PyArrow tables
    left_pyarrow = pa.table({
        'customer_id': left_table['customer_id'].to_pylist(),
        'name': left_table['name'].to_pylist(),
        'city': left_table['city'].to_pylist(),
        'segment': left_table['segment'].to_pylist()
    })
    
    right_pyarrow = pa.table({
        'order_id': right_table['order_id'].to_pylist(),
        'customer_id': right_table['customer_id'].to_pylist(),
        'amount': right_table['amount'].to_pylist(),
        'status': right_table['status'].to_pylist()
    })
    
    # Benchmark execution
    start_time = time.perf_counter()
    
    # Use Arrow compute for joining
    joined = left_pyarrow.join(right_pyarrow, 'customer_id', 'customer_id', 'inner')
    
    end_time = time.perf_counter()
    elapsed = end_time - start_time
    
    # Calculate throughput
    total_rows = left_pyarrow.num_rows + right_pyarrow.num_rows
    throughput = total_rows / elapsed / 1e6  # M rows/sec
    result_rows = joined.num_rows
    
    print(f"✅ PyArrow Join: {elapsed*1000:.1f}ms")
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
    print("Starting Sabot Arrow vs DuckDB benchmark...")
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
        print("🔗 JOIN BENCHMARKS")
        print("-" * 30)
        sabot_join_time, sabot_join_throughput, sabot_join_rows = benchmark_sabot_arrow_join(customers_table, orders_table)
        pyarrow_join_time, pyarrow_join_throughput, pyarrow_join_rows = benchmark_pyarrow_join(customers_table, orders_table)
        duckdb_join_time, duckdb_join_throughput, duckdb_join_rows = benchmark_duckdb_join(customers_table, orders_table)
        
        print("📊 GROUP BY BENCHMARKS")
        print("-" * 30)
        sabot_groupby_time, sabot_groupby_throughput, sabot_groupby_rows = benchmark_sabot_arrow_groupby(orders_table)
        pyarrow_groupby_time, pyarrow_groupby_throughput, pyarrow_groupby_rows = benchmark_pyarrow_groupby(orders_table)
        duckdb_groupby_time, duckdb_groupby_throughput, duckdb_groupby_rows = benchmark_duckdb_groupby(orders_table)
        
        print("🔍 FILTER BENCHMARKS")
        print("-" * 30)
        sabot_filter_time, sabot_filter_throughput, sabot_filter_rows = benchmark_sabot_arrow_filter(orders_table)
        pyarrow_filter_time, pyarrow_filter_throughput, pyarrow_filter_rows = benchmark_pyarrow_filter(orders_table)
        duckdb_filter_time, duckdb_filter_throughput, duckdb_filter_rows = benchmark_duckdb_filter(orders_table)
        
        # Store results
        results[size] = {
            'sabot': {
                'hash_join': {'time': sabot_join_time, 'throughput': sabot_join_throughput, 'rows': sabot_join_rows},
                'groupby': {'time': sabot_groupby_time, 'throughput': sabot_groupby_throughput, 'rows': sabot_groupby_rows},
                'filter': {'time': sabot_filter_time, 'throughput': sabot_filter_throughput, 'rows': sabot_filter_rows}
            },
            'pyarrow': {
                'hash_join': {'time': pyarrow_join_time, 'throughput': pyarrow_join_throughput, 'rows': pyarrow_join_rows},
                'groupby': {'time': pyarrow_groupby_time, 'throughput': pyarrow_groupby_throughput, 'rows': pyarrow_groupby_rows},
                'filter': {'time': pyarrow_filter_time, 'throughput': pyarrow_filter_throughput, 'rows': pyarrow_filter_rows}
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
    print("="*100)
    print(f"{'Size':>10} {'Operation':>12} {'Sabot':>15} {'PyArrow':>15} {'DuckDB':>15} {'Sabot/PyArrow':>15} {'Sabot/DuckDB':>15}")
    print(f"{'':>10} {'':>12} {'(M rows/sec)':>15} {'(M rows/sec)':>15} {'(M rows/sec)':>15} {'(x)':>15} {'(x)':>15}")
    print("-"*100)
    
    for size in sizes:
        r = results[size]
        for op in ['hash_join', 'groupby', 'filter']:
            sabot_tput = r['sabot'][op]['throughput']
            pyarrow_tput = r['pyarrow'][op]['throughput']
            duckdb_tput = r['duckdb'][op]['throughput']
            
            sabot_pyarrow_ratio = sabot_tput / pyarrow_tput if pyarrow_tput > 0 else 0
            sabot_duckdb_ratio = sabot_tput / duckdb_tput if duckdb_tput > 0 else 0
            
            print(f"{size:>10,} {op:>12} {sabot_tput:>15.1f} {pyarrow_tput:>15.1f} {duckdb_tput:>15.1f} {sabot_pyarrow_ratio:>15.2f} {sabot_duckdb_ratio:>15.2f}")
    
    print("="*100)
    print()
    
    # Performance analysis
    print("🎯 PERFORMANCE ANALYSIS")
    print("="*80)
    
    # Calculate average speedups
    avg_speedups_sabot_pyarrow = {}
    avg_speedups_sabot_duckdb = {}
    
    for op in ['hash_join', 'groupby', 'filter']:
        speedups_sabot_pyarrow = []
        speedups_sabot_duckdb = []
        
        for size in sizes:
            sabot_tput = results[size]['sabot'][op]['throughput']
            pyarrow_tput = results[size]['pyarrow'][op]['throughput']
            duckdb_tput = results[size]['duckdb'][op]['throughput']
            
            if pyarrow_tput > 0:
                speedups_sabot_pyarrow.append(sabot_tput / pyarrow_tput)
            if duckdb_tput > 0:
                speedups_sabot_duckdb.append(sabot_tput / duckdb_tput)
        
        avg_speedups_sabot_pyarrow[op] = np.mean(speedups_sabot_pyarrow) if speedups_sabot_pyarrow else 0
        avg_speedups_sabot_duckdb[op] = np.mean(speedups_sabot_duckdb) if speedups_sabot_duckdb else 0
    
    print("Average Speedup (Sabot vs PyArrow):")
    for op, speedup in avg_speedups_sabot_pyarrow.items():
        status = "🚀 FASTER" if speedup > 1.0 else "🐌 SLOWER"
        print(f"  {op:>12}: {speedup:.2f}x {status}")
    
    print()
    print("Average Speedup (Sabot vs DuckDB):")
    for op, speedup in avg_speedups_sabot_duckdb.items():
        status = "🚀 FASTER" if speedup > 1.0 else "🐌 SLOWER"
        print(f"  {op:>12}: {speedup:.2f}x {status}")
    
    print()
    
    # Best performance
    best_sabot = max(max(results[s]['sabot'][op]['throughput'] for op in ['hash_join', 'groupby', 'filter']) for s in sizes)
    best_pyarrow = max(max(results[s]['pyarrow'][op]['throughput'] for op in ['hash_join', 'groupby', 'filter']) for s in sizes)
    best_duckdb = max(max(results[s]['duckdb'][op]['throughput'] for op in ['hash_join', 'groupby', 'filter']) for s in sizes)
    
    print(f"🚀 Best Sabot Performance: {best_sabot:.1f}M rows/sec")
    print(f"🚀 Best PyArrow Performance: {best_pyarrow:.1f}M rows/sec")
    print(f"🚀 Best DuckDB Performance: {best_duckdb:.1f}M rows/sec")
    print(f"🚀 Sabot vs PyArrow: {best_sabot/best_pyarrow:.2f}x")
    print(f"🚀 Sabot vs DuckDB: {best_sabot/best_duckdb:.2f}x")
    print()
    
    print("✅ BENCHMARK COMPLETE!")
    print("This used REAL Sabot CyArrow operations - no simulation!")

if __name__ == "__main__":
    main()
