#!/usr/bin/env python3
"""
SabotSQL Benchmark with 1M Rows

This benchmark tests SabotSQL performance with 1 million rows of data
to validate scalability and performance characteristics.
"""

import os
import sys
import time
import subprocess
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from typing import Dict, List, Any, Optional

# Add Sabot to path
sys.path.insert(0, '/Users/bengamble/Sabot')

def create_large_dataset(num_rows: int = 1000000) -> pa.Table:
    """Create a large dataset for benchmarking"""
    print(f"Creating dataset with {num_rows:,} rows...")
    
    np.random.seed(42)
    
    # Generate data in chunks to avoid memory issues
    chunk_size = 100000
    chunks = []
    
    for i in range(0, num_rows, chunk_size):
        current_chunk_size = min(chunk_size, num_rows - i)
        
        data = {
            'id': np.arange(i, i + current_chunk_size),
            'user_id': np.random.randint(1, 10000, current_chunk_size),
            'product_id': np.random.randint(1, 1000, current_chunk_size),
            'price': np.random.uniform(10.0, 1000.0, current_chunk_size),
            'quantity': np.random.randint(1, 10, current_chunk_size),
            'category': np.random.choice(['electronics', 'clothing', 'books', 'home', 'sports'], current_chunk_size),
            'rating': np.random.uniform(1.0, 5.0, current_chunk_size),
            'timestamp': np.random.randint(1609459200, 1640995200, current_chunk_size)  # 2021-2022
        }
        
        chunk = pa.Table.from_pydict(data)
        chunks.append(chunk)
        
        if (i + chunk_size) % 500000 == 0:
            print(f"  Generated {i + chunk_size:,} rows...")
    
    # Combine chunks
    print("Combining chunks...")
    table = pa.concat_tables(chunks)
    
    print(f"✅ Created dataset: {table.num_rows:,} rows, {table.num_columns} columns")
    return table

def benchmark_sabot_sql_cpp(table: pa.Table) -> Dict[str, Any]:
    """Benchmark SabotSQL C++ implementation"""
    print("\n🔧 Benchmarking SabotSQL C++ Implementation")
    print("-" * 50)
    
    # Save table to Parquet for C++ access
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        pq.write_table(table, f.name)
        parquet_file = f.name
    
    try:
        # Test basic SabotSQL
        start_time = time.time()
        
        result = subprocess.run([
            'DYLD_LIBRARY_PATH=./sabot_sql/build:./vendor/arrow/cpp/build/install/lib:$DYLD_LIBRARY_PATH',
            './test_sabot_sql_comprehensive'
        ], shell=True, capture_output=True, text=True, timeout=300)
        
        end_time = time.time()
        
        if result.returncode == 0:
            print("✅ SabotSQL C++ test passed")
            print(f"   Execution time: {end_time - start_time:.3f}s")
            return {
                'status': 'success',
                'execution_time': end_time - start_time,
                'output': result.stdout
            }
        else:
            print(f"❌ SabotSQL C++ test failed: {result.stderr}")
            return {
                'status': 'error',
                'error': result.stderr
            }
    
    except subprocess.TimeoutExpired:
        print("❌ SabotSQL C++ test timed out")
        return {
            'status': 'timeout',
            'error': 'Test timed out after 300 seconds'
        }
    
    finally:
        # Cleanup
        if os.path.exists(parquet_file):
            os.unlink(parquet_file)

def benchmark_sabot_sql_python(table: pa.Table) -> Dict[str, Any]:
    """Benchmark SabotSQL Python implementation"""
    print("\n🐍 Benchmarking SabotSQL Python Implementation")
    print("-" * 50)
    
    try:
        from sabot_sql import SabotSQLOrchestrator
        
        # Create orchestrator
        orchestrator = SabotSQLOrchestrator()
        
        # Add agents
        num_agents = 4
        for i in range(num_agents):
            orchestrator.add_agent(f"agent_{i+1}")
        
        # Distribute data
        print("Distributing data across agents...")
        start_time = time.time()
        orchestrator.distribute_table("sales", table, strategy="round_robin")
        distribution_time = time.time() - start_time
        print(f"✅ Data distributed in {distribution_time:.3f}s")
        
        # Test queries
        test_queries = [
            "SELECT COUNT(*) FROM sales",
            "SELECT category, COUNT(*) FROM sales GROUP BY category",
            "SELECT user_id, AVG(price) FROM sales GROUP BY user_id LIMIT 100",
            "SELECT * FROM sales WHERE price > 500 ORDER BY price DESC LIMIT 1000"
        ]
        
        results = {}
        
        for i, query in enumerate(test_queries, 1):
            print(f"\nQuery {i}: {query}")
            print("-" * 40)
            
            start_time = time.time()
            query_results = orchestrator.execute_distributed_query(query)
            end_time = time.time()
            
            execution_time = end_time - start_time
            
            # Count successful results
            successful = sum(1 for r in query_results if r['status'] == 'success')
            total = len(query_results)
            
            print(f"✅ Query {i} completed in {execution_time:.3f}s")
            print(f"   Successful agents: {successful}/{total}")
            
            results[f'query_{i}'] = {
                'query': query,
                'execution_time': execution_time,
                'successful_agents': successful,
                'total_agents': total
            }
        
        # Get orchestrator stats
        stats = orchestrator.get_orchestrator_stats()
        
        return {
            'status': 'success',
            'distribution_time': distribution_time,
            'queries': results,
            'stats': stats
        }
        
    except Exception as e:
        print(f"❌ SabotSQL Python test failed: {e}")
        import traceback
        traceback.print_exc()
        return {
            'status': 'error',
            'error': str(e)
        }

def benchmark_duckdb_comparison(table: pa.Table) -> Dict[str, Any]:
    """Benchmark DuckDB for comparison"""
    print("\n🦆 Benchmarking DuckDB for Comparison")
    print("-" * 50)
    
    try:
        import duckdb
        
        # Convert Arrow table to DuckDB
        print("Converting Arrow table to DuckDB...")
        start_time = time.time()
        
        # Save to Parquet and load with DuckDB
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            pq.write_table(table, f.name)
            parquet_file = f.name
        
        conn = duckdb.connect()
        conn.execute(f"CREATE TABLE sales AS SELECT * FROM read_parquet('{parquet_file}')")
        
        conversion_time = time.time() - start_time
        print(f"✅ Data converted in {conversion_time:.3f}s")
        
        # Test queries
        test_queries = [
            "SELECT COUNT(*) FROM sales",
            "SELECT category, COUNT(*) FROM sales GROUP BY category",
            "SELECT user_id, AVG(price) FROM sales GROUP BY user_id LIMIT 100",
            "SELECT * FROM sales WHERE price > 500 ORDER BY price DESC LIMIT 1000"
        ]
        
        results = {}
        
        for i, query in enumerate(test_queries, 1):
            print(f"\nQuery {i}: {query}")
            print("-" * 40)
            
            start_time = time.time()
            result = conn.execute(query).fetchall()
            end_time = time.time()
            
            execution_time = end_time - start_time
            result_count = len(result)
            
            print(f"✅ Query {i} completed in {execution_time:.3f}s")
            print(f"   Result rows: {result_count}")
            
            results[f'query_{i}'] = {
                'query': query,
                'execution_time': execution_time,
                'result_rows': result_count
            }
        
        conn.close()
        
        return {
            'status': 'success',
            'conversion_time': conversion_time,
            'queries': results
        }
        
    except Exception as e:
        print(f"❌ DuckDB test failed: {e}")
        import traceback
        traceback.print_exc()
        return {
            'status': 'error',
            'error': str(e)
        }
    
    finally:
        # Cleanup
        if os.path.exists(parquet_file):
            os.unlink(parquet_file)

def benchmark_arrow_compute(table: pa.Table) -> Dict[str, Any]:
    """Benchmark Arrow compute operations"""
    print("\n🏹 Benchmarking Arrow Compute Operations")
    print("-" * 50)
    
    try:
        import pyarrow.compute as pc
        
        # Test operations
        operations = [
            ("count", lambda t: pc.count(t['id'])),
            ("sum", lambda t: pc.sum(t['price'])),
            ("mean", lambda t: pc.mean(t['price'])),
            ("filter", lambda t: pc.filter(t['price'], pc.greater(t['price'], 500.0)))
        ]
        
        results = {}
        
        for op_name, operation in operations:
            print(f"\nOperation: {op_name}")
            print("-" * 20)
            
            start_time = time.time()
            result = operation(table)
            end_time = time.time()
            
            execution_time = end_time - start_time
            
            print(f"✅ {op_name} completed in {execution_time:.3f}s")
            
            results[op_name] = {
                'execution_time': execution_time,
                'result': str(result)
            }
        
        return {
            'status': 'success',
            'operations': results
        }
        
    except Exception as e:
        print(f"❌ Arrow compute test failed: {e}")
        import traceback
        traceback.print_exc()
        return {
            'status': 'error',
            'error': str(e)
        }

def main():
    """Main benchmark function"""
    print("🚀 SabotSQL 1M Row Benchmark")
    print("=" * 50)
    
    # Create large dataset
    table = create_large_dataset(1000000)
    
    # Run benchmarks
    benchmarks = {
        "SabotSQL C++": lambda: benchmark_sabot_sql_cpp(table),
        "SabotSQL Python": lambda: benchmark_sabot_sql_python(table),
        "DuckDB": lambda: benchmark_duckdb_comparison(table),
        "Arrow Compute": lambda: benchmark_arrow_compute(table)
    }
    
    results = {}
    
    for name, benchmark_func in benchmarks.items():
        print(f"\n{'='*60}")
        print(f"Running {name} Benchmark")
        print(f"{'='*60}")
        
        try:
            result = benchmark_func()
            results[name] = result
            
            if result['status'] == 'success':
                print(f"✅ {name} benchmark completed successfully")
            else:
                print(f"❌ {name} benchmark failed: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            print(f"❌ {name} benchmark crashed: {e}")
            results[name] = {
                'status': 'crashed',
                'error': str(e)
            }
    
    # Print summary
    print(f"\n{'='*60}")
    print("BENCHMARK SUMMARY")
    print(f"{'='*60}")
    
    for name, result in results.items():
        print(f"\n{name}:")
        print(f"  Status: {result['status']}")
        
        if result['status'] == 'success':
            if 'queries' in result:
                total_time = sum(q['execution_time'] for q in result['queries'].values())
                print(f"  Total execution time: {total_time:.3f}s")
                print(f"  Average per query: {total_time/len(result['queries']):.3f}s")
            
            if 'operations' in result:
                total_time = sum(op['execution_time'] for op in result['operations'].values())
                print(f"  Total execution time: {total_time:.3f}s")
                print(f"  Average per operation: {total_time/len(result['operations']):.3f}s")
        else:
            print(f"  Error: {result.get('error', 'Unknown error')}")
    
    print(f"\n🎉 Benchmark completed!")
    print(f"Dataset: {table.num_rows:,} rows, {table.num_columns} columns")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
