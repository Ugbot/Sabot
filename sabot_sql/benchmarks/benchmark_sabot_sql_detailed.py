#!/usr/bin/env python3
"""
Detailed SabotSQL Benchmark

This benchmark provides detailed performance analysis of SabotSQL
with various dataset sizes and query types.
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
import json

# Add Sabot to path
sys.path.insert(0, '/Users/bengamble/Sabot')

def create_dataset(num_rows: int) -> pa.Table:
    """Create a dataset for benchmarking"""
    print(f"Creating dataset with {num_rows:,} rows...")
    
    np.random.seed(42)
    
    # Generate data in chunks to avoid memory issues
    chunk_size = min(100000, num_rows)
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
            'timestamp': np.random.randint(1609459200, 1640995200, current_chunk_size)
        }
        
        chunk = pa.Table.from_pydict(data)
        chunks.append(chunk)
    
    # Combine chunks
    table = pa.concat_tables(chunks)
    print(f"✅ Created dataset: {table.num_rows:,} rows, {table.num_columns} columns")
    return table

def benchmark_sabot_sql_cpp_detailed(table: pa.Table) -> Dict[str, Any]:
    """Detailed benchmark of SabotSQL C++ implementation"""
    print("\n🔧 Detailed SabotSQL C++ Benchmark")
    print("-" * 50)
    
    # Save table to Parquet for C++ access
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        pq.write_table(table, f.name)
        parquet_file = f.name
    
    try:
        # Test different aspects
        tests = {
            "basic_functionality": "./test_sabot_sql_simple",
            "flink_extensions": "./test_flink_sql_extension", 
            "comprehensive": "./test_sabot_sql_comprehensive"
        }
        
        results = {}
        
        for test_name, test_command in tests.items():
            print(f"\nRunning {test_name}...")
            
            start_time = time.time()
            result = subprocess.run([
                f'DYLD_LIBRARY_PATH=./sabot_sql/build:./vendor/arrow/cpp/build/install/lib:$DYLD_LIBRARY_PATH',
                test_command
            ], shell=True, capture_output=True, text=True, timeout=60)
            end_time = time.time()
            
            execution_time = end_time - start_time
            
            if result.returncode == 0:
                print(f"✅ {test_name} passed in {execution_time:.3f}s")
                results[test_name] = {
                    'status': 'success',
                    'execution_time': execution_time,
                    'output': result.stdout
                }
            else:
                print(f"❌ {test_name} failed: {result.stderr}")
                results[test_name] = {
                    'status': 'error',
                    'execution_time': execution_time,
                    'error': result.stderr
                }
        
        return {
            'status': 'success',
            'tests': results,
            'dataset_size': table.num_rows
        }
    
    except subprocess.TimeoutExpired:
        print("❌ SabotSQL C++ test timed out")
        return {
            'status': 'timeout',
            'error': 'Test timed out after 60 seconds'
        }
    
    finally:
        # Cleanup
        if os.path.exists(parquet_file):
            os.unlink(parquet_file)

def benchmark_sabot_sql_python_detailed(table: pa.Table) -> Dict[str, Any]:
    """Detailed benchmark of SabotSQL Python implementation"""
    print("\n🐍 Detailed SabotSQL Python Benchmark")
    print("-" * 50)
    
    try:
        from sabot_sql import SabotSQLOrchestrator
        
        # Test different agent configurations
        agent_configs = [1, 2, 4, 8]
        results = {}
        
        for num_agents in agent_configs:
            print(f"\nTesting with {num_agents} agents...")
            
            # Create orchestrator
            orchestrator = SabotSQLOrchestrator()
            
            # Add agents
            for i in range(num_agents):
                orchestrator.add_agent(f"agent_{i+1}")
            
            # Distribute data
            start_time = time.time()
            orchestrator.distribute_table("sales", table, strategy="round_robin")
            distribution_time = time.time() - start_time
            
            # Test queries
            test_queries = [
                "SELECT COUNT(*) FROM sales",
                "SELECT category, COUNT(*) FROM sales GROUP BY category",
                "SELECT user_id, AVG(price) FROM sales GROUP BY user_id LIMIT 100"
            ]
            
            query_results = {}
            
            for i, query in enumerate(test_queries, 1):
                start_time = time.time()
                results_list = orchestrator.execute_distributed_query(query)
                end_time = time.time()
                
                execution_time = end_time - start_time
                successful = sum(1 for r in results_list if r['status'] == 'success')
                
                query_results[f'query_{i}'] = {
                    'query': query,
                    'execution_time': execution_time,
                    'successful_agents': successful,
                    'total_agents': num_agents
                }
            
            # Get orchestrator stats
            stats = orchestrator.get_orchestrator_stats()
            
            results[f'{num_agents}_agents'] = {
                'distribution_time': distribution_time,
                'queries': query_results,
                'stats': stats
            }
        
        return {
            'status': 'success',
            'configurations': results,
            'dataset_size': table.num_rows
        }
        
    except Exception as e:
        print(f"❌ SabotSQL Python test failed: {e}")
        import traceback
        traceback.print_exc()
        return {
            'status': 'error',
            'error': str(e)
        }

def benchmark_scalability():
    """Benchmark scalability across different dataset sizes"""
    print("\n📈 Scalability Benchmark")
    print("-" * 50)
    
    dataset_sizes = [10000, 100000, 500000, 1000000]
    results = {}
    
    for size in dataset_sizes:
        print(f"\nTesting with {size:,} rows...")
        
        # Create dataset
        table = create_dataset(size)
        
        # Benchmark SabotSQL Python
        python_result = benchmark_sabot_sql_python_detailed(table)
        
        # Benchmark SabotSQL C++
        cpp_result = benchmark_sabot_sql_cpp_detailed(table)
        
        results[f'{size}_rows'] = {
            'dataset_size': size,
            'python': python_result,
            'cpp': cpp_result
        }
    
    return results

def benchmark_query_types():
    """Benchmark different query types"""
    print("\n🔍 Query Types Benchmark")
    print("-" * 50)
    
    # Create 100K row dataset for query testing
    table = create_dataset(100000)
    
    try:
        from sabot_sql import SabotSQLOrchestrator
        
        # Create orchestrator with 4 agents
        orchestrator = SabotSQLOrchestrator()
        for i in range(4):
            orchestrator.add_agent(f"agent_{i+1}")
        
        # Distribute data
        orchestrator.distribute_table("sales", table, strategy="round_robin")
        
        # Test different query types
        query_types = {
            "count": "SELECT COUNT(*) FROM sales",
            "filter": "SELECT * FROM sales WHERE price > 500 LIMIT 1000",
            "group_by": "SELECT category, COUNT(*) FROM sales GROUP BY category",
            "aggregation": "SELECT user_id, AVG(price), SUM(quantity) FROM sales GROUP BY user_id LIMIT 100",
            "order_by": "SELECT * FROM sales ORDER BY price DESC LIMIT 1000",
            "complex": "SELECT category, AVG(price) FROM sales WHERE price > 100 GROUP BY category ORDER BY AVG(price) DESC"
        }
        
        results = {}
        
        for query_type, query in query_types.items():
            print(f"\nTesting {query_type} query...")
            
            # Run multiple times for average
            times = []
            for i in range(5):
                start_time = time.time()
                query_results = orchestrator.execute_distributed_query(query)
                end_time = time.time()
                times.append(end_time - start_time)
            
            avg_time = sum(times) / len(times)
            min_time = min(times)
            max_time = max(times)
            
            results[query_type] = {
                'query': query,
                'avg_time': avg_time,
                'min_time': min_time,
                'max_time': max_time,
                'times': times
            }
            
            print(f"✅ {query_type}: {avg_time:.3f}s avg ({min_time:.3f}s - {max_time:.3f}s)")
        
        return {
            'status': 'success',
            'query_types': results
        }
        
    except Exception as e:
        print(f"❌ Query types test failed: {e}")
        import traceback
        traceback.print_exc()
        return {
            'status': 'error',
            'error': str(e)
        }

def main():
    """Main benchmark function"""
    print("🚀 Detailed SabotSQL Benchmark Suite")
    print("=" * 60)
    
    # Run scalability benchmark
    scalability_results = benchmark_scalability()
    
    # Run query types benchmark
    query_types_results = benchmark_query_types()
    
    # Print detailed summary
    print(f"\n{'='*60}")
    print("DETAILED BENCHMARK SUMMARY")
    print(f"{'='*60}")
    
    print("\n📈 Scalability Results:")
    print("-" * 30)
    
    for size_key, result in scalability_results.items():
        size = result['dataset_size']
        print(f"\n{size:,} rows:")
        
        if result['python']['status'] == 'success':
            python_stats = result['python']['configurations']['4_agents']['stats']
            print(f"  Python: {python_stats['total_queries']} queries, {python_stats['total_execution_time']:.3f}s total")
        else:
            print(f"  Python: {result['python']['status']}")
        
        if result['cpp']['status'] == 'success':
            cpp_tests = result['cpp']['tests']
            total_time = sum(test['execution_time'] for test in cpp_tests.values() if test['status'] == 'success')
            print(f"  C++: {total_time:.3f}s total")
        else:
            print(f"  C++: {result['cpp']['status']}")
    
    print("\n🔍 Query Types Results:")
    print("-" * 30)
    
    if query_types_results['status'] == 'success':
        for query_type, result in query_types_results['query_types'].items():
            print(f"  {query_type}: {result['avg_time']:.3f}s avg")
    else:
        print(f"  Query types test failed: {query_types_results['error']}")
    
    # Save results to file
    all_results = {
        'scalability': scalability_results,
        'query_types': query_types_results,
        'timestamp': time.time()
    }
    
    with open('sabot_sql_detailed_benchmark_results.json', 'w') as f:
        json.dump(all_results, f, indent=2, default=str)
    
    print(f"\n🎉 Detailed benchmark completed!")
    print(f"Results saved to: sabot_sql_detailed_benchmark_results.json")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
