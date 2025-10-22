#!/usr/bin/env python3
"""
SabotCypher Comprehensive Benchmark Suite

Complete benchmark testing for SabotCypher query engine including:
- Q1-Q9 standard benchmark queries
- Performance metrics (latency, throughput, memory)
- Scalability testing (different graph sizes)
- Operator-specific benchmarks
- Comparison with baseline implementations
"""

import sys
import os
import pyarrow as pa
import time
import json
import statistics
from pathlib import Path
from typing import Dict, List, Any, Tuple
import psutil
import gc

# Add integration path
sys.path.append(str(Path(__file__).parent.parent / "python_integration"))

from sabot_cypher_integration import SabotCypherIntegration


class SabotCypherBenchmark:
    """
    Comprehensive benchmark suite for SabotCypher.
    
    Tests performance across multiple dimensions:
    - Query execution latency
    - Memory usage
    - Scalability with graph size
    - Operator performance
    - Error rates
    """
    
    def __init__(self):
        """Initialize benchmark suite."""
        self.integration = SabotCypherIntegration()
        self.results = {}
        self.memory_baseline = 0
        
    def create_test_graph(self, num_vertices: int = 1000, num_edges: int = 3000) -> Tuple[pa.Table, pa.Table]:
        """Create a test graph with specified size."""
        # Create vertices
        vertices = pa.table({
            'id': pa.array(list(range(1, num_vertices + 1)), type=pa.int64()),
            'name': pa.array([f"Person_{i}" for i in range(1, num_vertices + 1)]),
            'age': pa.array([20 + (i % 50) for i in range(1, num_vertices + 1)], type=pa.int32()),
            'city': pa.array(['NYC', 'LA', 'Chicago', 'Boston', 'Seattle'][i % 5] for i in range(1, num_vertices + 1)),
            'company': pa.array([f"Company_{i % 100}" for i in range(1, num_vertices + 1)]),
            'salary': pa.array([50000 + (i % 100000) for i in range(1, num_vertices + 1)], type=pa.int32()),
            'department': pa.array(['Engineering', 'Sales', 'Marketing', 'HR', 'Finance'][i % 5] for i in range(1, num_vertices + 1))
        })
        
        # Create edges with realistic patterns
        edge_sources = []
        edge_targets = []
        edge_types = []
        edge_weights = []
        
        for i in range(num_edges):
            source = (i % num_vertices) + 1
            target = ((i + 1) % num_vertices) + 1
            edge_type = ['KNOWS', 'WORKS_WITH', 'FRIENDS', 'COLLEAGUE', 'MANAGES'][i % 5]
            weight = 0.1 + (i % 9) * 0.1
            
            edge_sources.append(source)
            edge_targets.append(target)
            edge_types.append(edge_type)
            edge_weights.append(weight)
        
        edges = pa.table({
            'source': pa.array(edge_sources, type=pa.int64()),
            'target': pa.array(edge_targets, type=pa.int64()),
            'type': pa.array(edge_types),
            'weight': pa.array(edge_weights, type=pa.float64())
        })
        
        return vertices, edges
    
    def get_q1_q9_queries(self) -> Dict[str, str]:
        """Get Q1-Q9 standard benchmark queries."""
        return {
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
    
    def get_operator_benchmark_queries(self) -> Dict[str, str]:
        """Get queries that test specific operators."""
        return {
            'Scan_Only': "MATCH (a:Person) RETURN a.id LIMIT 100",
            'Filter_Only': "MATCH (a:Person) WHERE a.age > 25 AND a.age < 35 RETURN a.name LIMIT 50",
            'Project_Only': "MATCH (a:Person) RETURN a.name, a.age, a.city LIMIT 100",
            'Aggregate_Count': "MATCH (a:Person) RETURN count(*)",
            'Aggregate_Avg': "MATCH (a:Person) RETURN avg(a.age)",
            'Aggregate_Sum': "MATCH (a:Person) RETURN sum(a.salary)",
            'OrderBy_Only': "MATCH (a:Person) RETURN a.name ORDER BY a.age LIMIT 100",
            'Limit_Only': "MATCH (a:Person) RETURN a.name LIMIT 1",
            'Match2Hop_Only': "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.id, b.id LIMIT 50",
            'Match3Hop_Only': "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN a.id, c.id LIMIT 20",
            'Complex_Query': "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 30 AND b.city = 'NYC' RETURN a.name, b.name ORDER BY a.age LIMIT 10"
        }
    
    def measure_memory_usage(self) -> float:
        """Measure current memory usage in MB."""
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024
    
    def benchmark_query(self, query_id: str, query: str, vertices: pa.Table, edges: pa.Table, 
                        iterations: int = 5) -> Dict[str, Any]:
        """Benchmark a single query with multiple iterations."""
        print(f"  🔍 {query_id}: {query}")
        
        # Warmup
        self.integration.execute_cypher(query, vertices, edges)
        
        # Measure memory before
        gc.collect()
        memory_before = self.measure_memory_usage()
        
        # Benchmark iterations
        execution_times = []
        success_count = 0
        
        for i in range(iterations):
            start_time = time.time()
            result = self.integration.execute_cypher(query, vertices, edges)
            end_time = time.time()
            
            if result['success']:
                execution_times.append((end_time - start_time) * 1000)  # Convert to ms
                success_count += 1
        
        # Measure memory after
        gc.collect()
        memory_after = self.measure_memory_usage()
        
        if execution_times:
            avg_time = statistics.mean(execution_times)
            min_time = min(execution_times)
            max_time = max(execution_times)
            std_dev = statistics.stdev(execution_times) if len(execution_times) > 1 else 0
        else:
            avg_time = min_time = max_time = std_dev = 0
        
        return {
            'query_id': query_id,
            'query': query,
            'iterations': iterations,
            'success_count': success_count,
            'success_rate': success_count / iterations * 100,
            'avg_time_ms': avg_time,
            'min_time_ms': min_time,
            'max_time_ms': max_time,
            'std_dev_ms': std_dev,
            'memory_before_mb': memory_before,
            'memory_after_mb': memory_after,
            'memory_delta_mb': memory_after - memory_before
        }
    
    def run_q1_q9_benchmark(self, vertices: pa.Table, edges: pa.Table) -> Dict[str, Any]:
        """Run Q1-Q9 standard benchmark."""
        print("\n" + "="*60)
        print("Q1-Q9 STANDARD BENCHMARK")
        print("="*60)
        
        queries = self.get_q1_q9_queries()
        results = {}
        
        for query_id, query in queries.items():
            result = self.benchmark_query(query_id, query, vertices, edges)
            results[query_id] = result
            
            if result['success_rate'] == 100:
                print(f"    ✅ {query_id}: {result['avg_time_ms']:.2f}ms avg, {result['success_count']}/{result['iterations']} success")
            else:
                print(f"    ❌ {query_id}: {result['success_rate']:.1f}% success rate")
        
        return results
    
    def run_operator_benchmark(self, vertices: pa.Table, edges: pa.Table) -> Dict[str, Any]:
        """Run operator-specific benchmark."""
        print("\n" + "="*60)
        print("OPERATOR-SPECIFIC BENCHMARK")
        print("="*60)
        
        queries = self.get_operator_benchmark_queries()
        results = {}
        
        for query_id, query in queries.items():
            result = self.benchmark_query(query_id, query, vertices, edges)
            results[query_id] = result
            
            if result['success_rate'] == 100:
                print(f"    ✅ {query_id}: {result['avg_time_ms']:.2f}ms avg")
            else:
                print(f"    ❌ {query_id}: {result['success_rate']:.1f}% success rate")
        
        return results
    
    def run_scalability_benchmark(self) -> Dict[str, Any]:
        """Run scalability benchmark with different graph sizes."""
        print("\n" + "="*60)
        print("SCALABILITY BENCHMARK")
        print("="*60)
        
        graph_sizes = [
            (100, 300),    # Small
            (500, 1500),   # Medium
            (1000, 3000),  # Large
            (2000, 6000),  # Very Large
            (5000, 15000)  # Extra Large
        ]
        
        results = {}
        test_query = "MATCH (a:Person) RETURN a.name LIMIT 10"
        
        for num_vertices, num_edges in graph_sizes:
            print(f"\n  📊 Graph size: {num_vertices} vertices, {num_edges} edges")
            
            vertices, edges = self.create_test_graph(num_vertices, num_edges)
            result = self.benchmark_query(f"Scale_{num_vertices}v", test_query, vertices, edges, iterations=3)
            
            results[f"{num_vertices}v_{num_edges}e"] = {
                **result,
                'num_vertices': num_vertices,
                'num_edges': num_edges,
                'vertices_per_ms': num_vertices / result['avg_time_ms'] if result['avg_time_ms'] > 0 else 0,
                'edges_per_ms': num_edges / result['avg_time_ms'] if result['avg_time_ms'] > 0 else 0
            }
            
            if result['success_rate'] == 100:
                print(f"    ✅ {result['avg_time_ms']:.2f}ms avg, {result['vertices_per_ms']:.0f} vertices/ms")
            else:
                print(f"    ❌ {result['success_rate']:.1f}% success rate")
        
        return results
    
    def run_comprehensive_benchmark(self) -> Dict[str, Any]:
        """Run comprehensive benchmark suite."""
        print("SABOT_CYPHER COMPREHENSIVE BENCHMARK")
        print("="*60)
        print(f"Python version: {sys.version}")
        print(f"PyArrow version: {pa.__version__}")
        print(f"System: {os.uname().sysname} {os.uname().release}")
        
        # Create test graph
        print(f"\n📊 Creating test graph...")
        vertices, edges = self.create_test_graph(1000, 3000)
        print(f"✅ Graph created: {vertices.num_rows} vertices, {edges.num_rows} edges")
        
        # Measure baseline memory
        self.memory_baseline = self.measure_memory_usage()
        print(f"📈 Baseline memory: {self.memory_baseline:.2f} MB")
        
        # Run benchmarks
        q1_q9_results = self.run_q1_q9_benchmark(vertices, edges)
        operator_results = self.run_operator_benchmark(vertices, edges)
        scalability_results = self.run_scalability_benchmark()
        
        # Compile results
        benchmark_results = {
            'metadata': {
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'python_version': sys.version,
                'pyarrow_version': pa.__version__,
                'system': f"{os.uname().sysname} {os.uname().release}",
                'baseline_memory_mb': self.memory_baseline
            },
            'q1_q9_benchmark': q1_q9_results,
            'operator_benchmark': operator_results,
            'scalability_benchmark': scalability_results,
            'summary': self._generate_summary(q1_q9_results, operator_results, scalability_results)
        }
        
        return benchmark_results
    
    def _generate_summary(self, q1_q9_results: Dict, operator_results: Dict, scalability_results: Dict) -> Dict[str, Any]:
        """Generate benchmark summary."""
        # Q1-Q9 summary
        q1_q9_successful = [r for r in q1_q9_results.values() if r['success_rate'] == 100]
        q1_q9_avg_time = statistics.mean([r['avg_time_ms'] for r in q1_q9_successful]) if q1_q9_successful else 0
        
        # Operator summary
        operator_successful = [r for r in operator_results.values() if r['success_rate'] == 100]
        operator_avg_time = statistics.mean([r['avg_time_ms'] for r in operator_successful]) if operator_successful else 0
        
        # Scalability summary
        scalability_successful = [r for r in scalability_results.values() if r['success_rate'] == 100]
        max_vertices = max([r['num_vertices'] for r in scalability_successful]) if scalability_successful else 0
        max_edges = max([r['num_edges'] for r in scalability_successful]) if scalability_successful else 0
        
        return {
            'q1_q9': {
                'total_queries': len(q1_q9_results),
                'successful_queries': len(q1_q9_successful),
                'success_rate': len(q1_q9_successful) / len(q1_q9_results) * 100 if q1_q9_results else 0,
                'avg_execution_time_ms': q1_q9_avg_time
            },
            'operators': {
                'total_queries': len(operator_results),
                'successful_queries': len(operator_successful),
                'success_rate': len(operator_successful) / len(operator_results) * 100 if operator_results else 0,
                'avg_execution_time_ms': operator_avg_time
            },
            'scalability': {
                'max_vertices_tested': max_vertices,
                'max_edges_tested': max_edges,
                'successful_scales': len(scalability_successful),
                'total_scales': len(scalability_results)
            },
            'overall': {
                'total_queries': len(q1_q9_results) + len(operator_results),
                'successful_queries': len(q1_q9_successful) + len(operator_successful),
                'overall_success_rate': (len(q1_q9_successful) + len(operator_successful)) / (len(q1_q9_results) + len(operator_results)) * 100 if (q1_q9_results or operator_results) else 0
            }
        }
    
    def print_summary(self, results: Dict[str, Any]):
        """Print benchmark summary."""
        summary = results['summary']
        
        print("\n" + "="*60)
        print("BENCHMARK SUMMARY")
        print("="*60)
        
        print(f"\n📊 Q1-Q9 Standard Benchmark:")
        print(f"   Queries: {summary['q1_q9']['total_queries']}")
        print(f"   Successful: {summary['q1_q9']['successful_queries']}")
        print(f"   Success Rate: {summary['q1_q9']['success_rate']:.1f}%")
        print(f"   Avg Time: {summary['q1_q9']['avg_execution_time_ms']:.2f}ms")
        
        print(f"\n🔧 Operator Benchmark:")
        print(f"   Queries: {summary['operators']['total_queries']}")
        print(f"   Successful: {summary['operators']['successful_queries']}")
        print(f"   Success Rate: {summary['operators']['success_rate']:.1f}%")
        print(f"   Avg Time: {summary['operators']['avg_execution_time_ms']:.2f}ms")
        
        print(f"\n📈 Scalability:")
        print(f"   Max Vertices: {summary['scalability']['max_vertices_tested']:,}")
        print(f"   Max Edges: {summary['scalability']['max_edges_tested']:,}")
        print(f"   Successful Scales: {summary['scalability']['successful_scales']}/{summary['scalability']['total_scales']}")
        
        print(f"\n🎯 Overall:")
        print(f"   Total Queries: {summary['overall']['total_queries']}")
        print(f"   Successful: {summary['overall']['successful_queries']}")
        print(f"   Success Rate: {summary['overall']['overall_success_rate']:.1f}%")
        
        print("\n" + "="*60)
        print("BENCHMARK COMPLETE!")
        print("="*60)
    
    def save_results(self, results: Dict[str, Any], filename: str = None):
        """Save benchmark results to JSON file."""
        if filename is None:
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            filename = f"sabot_cypher_benchmark_{timestamp}.json"
        
        filepath = Path(__file__).parent / filename
        
        with open(filepath, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\n💾 Results saved to: {filepath}")


def main():
    """Main benchmark function."""
    benchmark = SabotCypherBenchmark()
    
    try:
        results = benchmark.run_comprehensive_benchmark()
        benchmark.print_summary(results)
        benchmark.save_results(results)
        
        # Return exit code based on success rate
        overall_success_rate = results['summary']['overall']['overall_success_rate']
        return 0 if overall_success_rate >= 90 else 1
        
    except Exception as e:
        print(f"\n❌ Benchmark failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
