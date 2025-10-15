#!/usr/bin/env python3
"""
SabotCypher Simple Benchmark

Clean, focused benchmark for SabotCypher query engine.
"""

import sys
import os
import pyarrow as pa
import time
import json
from pathlib import Path

# Add integration path
sys.path.append(str(Path(__file__).parent.parent / "python_integration"))

from sabot_cypher_integration import SabotCypherIntegration


def create_test_graph(num_vertices=1000, num_edges=3000):
    """Create a test graph."""
    vertices = pa.table({
        'id': pa.array(list(range(1, num_vertices + 1)), type=pa.int64()),
        'name': pa.array([f"Person_{i}" for i in range(1, num_vertices + 1)]),
        'age': pa.array([20 + (i % 50) for i in range(1, num_vertices + 1)], type=pa.int32()),
        'city': pa.array(['NYC', 'LA', 'Chicago', 'Boston', 'Seattle'][i % 5] for i in range(1, num_vertices + 1))
    })
    
    edges = pa.table({
        'source': pa.array([(i % num_vertices) + 1 for i in range(num_edges)], type=pa.int64()),
        'target': pa.array([((i + 1) % num_vertices) + 1 for i in range(num_edges)], type=pa.int64()),
        'type': pa.array([['KNOWS', 'WORKS_WITH', 'FRIENDS'][i % 3] for i in range(num_edges)])
    })
    
    return vertices, edges


def benchmark_query(integration, query_id, query, vertices, edges, iterations=3):
    """Benchmark a single query."""
    print(f"  🔍 {query_id}: {query}")
    
    times = []
    success_count = 0
    
    for i in range(iterations):
        start_time = time.time()
        result = integration.execute_cypher(query, vertices, edges)
        end_time = time.time()
        
        if result['success']:
            times.append((end_time - start_time) * 1000)  # Convert to ms
            success_count += 1
    
    if times:
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        
        print(f"    ✅ {avg_time:.2f}ms avg ({min_time:.2f}-{max_time:.2f}ms), {success_count}/{iterations} success")
        
        return {
            'query_id': query_id,
            'query': query,
            'success': True,
            'avg_time_ms': avg_time,
            'min_time_ms': min_time,
            'max_time_ms': max_time,
            'success_count': success_count,
            'iterations': iterations
        }
    else:
        print(f"    ❌ Failed all {iterations} iterations")
        return {
            'query_id': query_id,
            'query': query,
            'success': False,
            'success_count': 0,
            'iterations': iterations
        }


def main():
    """Run SabotCypher benchmark."""
    print("SABOT_CYPHER BENCHMARK")
    print("=" * 50)
    
    # Initialize
    integration = SabotCypherIntegration()
    vertices, edges = create_test_graph(1000, 3000)
    print(f"📊 Test graph: {vertices.num_rows} vertices, {edges.num_rows} edges")
    
    # Q1-Q9 Queries
    q1_q9_queries = {
        'Q1': "MATCH (a:Person) RETURN a.name LIMIT 10",
        'Q2': "MATCH (a:Person) WITH a, count(*) as c RETURN c ORDER BY c LIMIT 5",
        'Q3': "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name LIMIT 10",
        'Q4': "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN count(*)",
        'Q5': "MATCH (a:Person) WITH a, count(*) as c WHERE c > 1 RETURN a.name ORDER BY c LIMIT 10",
        'Q6': "MATCH (a:Person) WHERE a.age > 30 RETURN a.name ORDER BY a.age LIMIT 10",
        'Q7': "MATCH (a:Person) WITH a, count(*) as c ORDER BY c LIMIT 5 RETURN a.name",
        'Q8': "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.city = 'NYC' RETURN a.name, b.name LIMIT 10",
        'Q9': "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) WHERE a.age > b.age RETURN count(*)"
    }
    
    # Operator Queries
    operator_queries = {
        'Scan': "MATCH (a:Person) RETURN a.id LIMIT 100",
        'Filter': "MATCH (a:Person) WHERE a.age > 25 AND a.age < 35 RETURN a.name LIMIT 50",
        'Project': "MATCH (a:Person) RETURN a.name, a.age, a.city LIMIT 100",
        'Aggregate': "MATCH (a:Person) RETURN count(*)",
        'OrderBy': "MATCH (a:Person) RETURN a.name ORDER BY a.age LIMIT 100",
        'Limit': "MATCH (a:Person) RETURN a.name LIMIT 1",
        'Match2Hop': "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.id, b.id LIMIT 50",
        'Match3Hop': "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN a.id, c.id LIMIT 20"
    }
    
    # Scalability Queries
    scalability_sizes = [
        (100, 300),
        (500, 1500),
        (1000, 3000),
        (2000, 6000)
    ]
    
    results = {
        'metadata': {
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'python_version': sys.version.split()[0],
            'pyarrow_version': pa.__version__
        },
        'q1_q9': {},
        'operators': {},
        'scalability': {}
    }
    
    # Q1-Q9 Benchmark
    print("\n" + "=" * 50)
    print("Q1-Q9 STANDARD BENCHMARK")
    print("=" * 50)
    
    for query_id, query in q1_q9_queries.items():
        result = benchmark_query(integration, query_id, query, vertices, edges)
        results['q1_q9'][query_id] = result
    
    # Operator Benchmark
    print("\n" + "=" * 50)
    print("OPERATOR BENCHMARK")
    print("=" * 50)
    
    for query_id, query in operator_queries.items():
        result = benchmark_query(integration, query_id, query, vertices, edges)
        results['operators'][query_id] = result
    
    # Scalability Benchmark
    print("\n" + "=" * 50)
    print("SCALABILITY BENCHMARK")
    print("=" * 50)
    
    test_query = "MATCH (a:Person) RETURN a.name LIMIT 10"
    
    for num_vertices, num_edges in scalability_sizes:
        print(f"\n  📊 Graph size: {num_vertices} vertices, {num_edges} edges")
        
        scale_vertices, scale_edges = create_test_graph(num_vertices, num_edges)
        result = benchmark_query(integration, f"Scale_{num_vertices}v", test_query, scale_vertices, scale_edges)
        
        if result['success']:
            vertices_per_ms = num_vertices / result['avg_time_ms'] if result['avg_time_ms'] > 0 else 0
            print(f"    📈 Performance: {vertices_per_ms:.0f} vertices/ms")
        
        results['scalability'][f"{num_vertices}v_{num_edges}e"] = {
            **result,
            'num_vertices': num_vertices,
            'num_edges': num_edges
        }
    
    # Summary
    print("\n" + "=" * 50)
    print("BENCHMARK SUMMARY")
    print("=" * 50)
    
    # Q1-Q9 Summary
    q1_q9_successful = [r for r in results['q1_q9'].values() if r['success']]
    q1_q9_avg_time = sum(r['avg_time_ms'] for r in q1_q9_successful) / len(q1_q9_successful) if q1_q9_successful else 0
    
    print(f"\n📊 Q1-Q9 Standard Benchmark:")
    print(f"   Queries: {len(results['q1_q9'])}")
    print(f"   Successful: {len(q1_q9_successful)}")
    print(f"   Success Rate: {len(q1_q9_successful)/len(results['q1_q9'])*100:.1f}%")
    print(f"   Avg Time: {q1_q9_avg_time:.2f}ms")
    
    # Operator Summary
    operator_successful = [r for r in results['operators'].values() if r['success']]
    operator_avg_time = sum(r['avg_time_ms'] for r in operator_successful) / len(operator_successful) if operator_successful else 0
    
    print(f"\n🔧 Operator Benchmark:")
    print(f"   Queries: {len(results['operators'])}")
    print(f"   Successful: {len(operator_successful)}")
    print(f"   Success Rate: {len(operator_successful)/len(results['operators'])*100:.1f}%")
    print(f"   Avg Time: {operator_avg_time:.2f}ms")
    
    # Scalability Summary
    scalability_successful = [r for r in results['scalability'].values() if r['success']]
    max_vertices = max([r['num_vertices'] for r in scalability_successful]) if scalability_successful else 0
    
    print(f"\n📈 Scalability:")
    print(f"   Max Vertices: {max_vertices:,}")
    print(f"   Successful Scales: {len(scalability_successful)}/{len(results['scalability'])}")
    
    # Overall Summary
    total_queries = len(results['q1_q9']) + len(results['operators'])
    total_successful = len(q1_q9_successful) + len(operator_successful)
    overall_success_rate = total_successful / total_queries * 100 if total_queries > 0 else 0
    
    print(f"\n🎯 Overall:")
    print(f"   Total Queries: {total_queries}")
    print(f"   Successful: {total_successful}")
    print(f"   Success Rate: {overall_success_rate:.1f}%")
    
    # Save results
    timestamp = time.strftime('%Y%m%d_%H%M%S')
    filename = f"sabot_cypher_benchmark_{timestamp}.json"
    filepath = Path(__file__).parent / filename
    
    with open(filepath, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n💾 Results saved to: {filepath}")
    print("\n" + "=" * 50)
    print("BENCHMARK COMPLETE!")
    print("=" * 50)
    
    return 0 if overall_success_rate >= 90 else 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
