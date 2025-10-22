#!/usr/bin/env python3
"""
SabotCypher vs Kuzu Comparison Benchmark

Direct performance comparison between SabotCypher and Kuzu query engines.
"""

import sys
import os
import pyarrow as pa
import time
import json
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Tuple

# Add integration path
sys.path.append(str(Path(__file__).parent.parent / "python_integration"))

from sabot_cypher_integration import SabotCypherIntegration


class KuzuComparisonBenchmark:
    """
    Comparison benchmark between SabotCypher and Kuzu.
    
    Tests both engines on identical queries and graphs to measure:
    - Query execution latency
    - Memory usage
    - Scalability
    - Feature completeness
    """
    
    def __init__(self):
        """Initialize comparison benchmark."""
        self.sabot_cypher = SabotCypherIntegration()
        self.kuzu_available = self._check_kuzu_availability()
        self.results = {}
        
    def _check_kuzu_availability(self) -> bool:
        """Check if Kuzu is available."""
        try:
            import kuzu
            print("✅ Kuzu is available")
            return True
        except ImportError:
            print("❌ Kuzu not available. Install with: pip install kuzu")
            return False
    
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
    
    def setup_kuzu_database(self, vertices: pa.Table, edges: pa.Table) -> Any:
        """Setup Kuzu database with test data."""
        if not self.kuzu_available:
            return None
            
        try:
            import kuzu
            
            # Create in-memory database
            db = kuzu.Database(":memory:")
            conn = kuzu.Connection(db)
            
            # Create schema
            conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, age INT32, city STRING, company STRING, salary INT32, department STRING, PRIMARY KEY(id))")
            conn.execute("CREATE REL TABLE KNOWS(FROM Person TO Person, weight DOUBLE)")
            conn.execute("CREATE REL TABLE WORKS_WITH(FROM Person TO Person, weight DOUBLE)")
            conn.execute("CREATE REL TABLE FRIENDS(FROM Person TO Person, weight DOUBLE)")
            conn.execute("CREATE REL TABLE COLLEAGUE(FROM Person TO Person, weight DOUBLE)")
            conn.execute("CREATE REL TABLE MANAGES(FROM Person TO Person, weight DOUBLE)")
            
            # Insert vertices
            for i in range(vertices.num_rows):
                row = vertices.slice(i, 1)
                conn.execute(f"""
                    INSERT INTO Person VALUES (
                        {row['id'][0].as_py()},
                        '{row['name'][0].as_py()}',
                        {row['age'][0].as_py()},
                        '{row['city'][0].as_py()}',
                        '{row['company'][0].as_py()}',
                        {row['salary'][0].as_py()},
                        '{row['department'][0].as_py()}'
                    )
                """)
            
            # Insert edges
            for i in range(edges.num_rows):
                row = edges.slice(i, 1)
                edge_type = row['type'][0].as_py()
                conn.execute(f"""
                    INSERT INTO {edge_type} VALUES (
                        {row['source'][0].as_py()},
                        {row['target'][0].as_py()},
                        {row['weight'][0].as_py()}
                    )
                """)
            
            return conn
            
        except Exception as e:
            print(f"❌ Failed to setup Kuzu database: {e}")
            return None
    
    def get_comparison_queries(self) -> Dict[str, str]:
        """Get queries for comparison testing."""
        return {
            'Simple_Scan': "MATCH (a:Person) RETURN a.name LIMIT 10",
            'Filter_Query': "MATCH (a:Person) WHERE a.age > 30 RETURN a.name ORDER BY a.age LIMIT 10",
            'Aggregate_Count': "MATCH (a:Person) RETURN count(*)",
            'Aggregate_Avg': "MATCH (a:Person) RETURN avg(a.age)",
            'Two_Hop': "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name LIMIT 10",
            'Three_Hop': "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN count(*)",
            'Complex_Query': "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.city = 'NYC' RETURN a.name, b.name ORDER BY a.age LIMIT 10",
            'Multi_Aggregate': "MATCH (a:Person) RETURN count(*), avg(a.age), min(a.age), max(a.age)"
        }
    
    def benchmark_sabot_cypher(self, query_id: str, query: str, vertices: pa.Table, edges: pa.Table, iterations: int = 5) -> Dict[str, Any]:
        """Benchmark SabotCypher query."""
        print(f"  🔍 SabotCypher {query_id}: {query}")
        
        times = []
        success_count = 0
        
        for i in range(iterations):
            start_time = time.time()
            result = self.sabot_cypher.execute_cypher(query, vertices, edges)
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
                'engine': 'SabotCypher',
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
                'engine': 'SabotCypher',
                'query_id': query_id,
                'query': query,
                'success': False,
                'success_count': 0,
                'iterations': iterations
            }
    
    def benchmark_kuzu(self, query_id: str, query: str, conn: Any, iterations: int = 5) -> Dict[str, Any]:
        """Benchmark Kuzu query."""
        if not conn:
            return {
                'engine': 'Kuzu',
                'query_id': query_id,
                'query': query,
                'success': False,
                'error': 'Kuzu not available'
            }
        
        print(f"  🔍 Kuzu {query_id}: {query}")
        
        times = []
        success_count = 0
        
        for i in range(iterations):
            try:
                start_time = time.time()
                result = conn.execute(query)
                end_time = time.time()
                
                # Consume result to ensure execution
                list(result)
                
                times.append((end_time - start_time) * 1000)  # Convert to ms
                success_count += 1
                
            except Exception as e:
                print(f"    ❌ Kuzu query failed: {e}")
                continue
        
        if times:
            avg_time = sum(times) / len(times)
            min_time = min(times)
            max_time = max(times)
            
            print(f"    ✅ {avg_time:.2f}ms avg ({min_time:.2f}-{max_time:.2f}ms), {success_count}/{iterations} success")
            
            return {
                'engine': 'Kuzu',
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
                'engine': 'Kuzu',
                'query_id': query_id,
                'query': query,
                'success': False,
                'success_count': 0,
                'iterations': iterations
            }
    
    def run_comparison_benchmark(self) -> Dict[str, Any]:
        """Run comprehensive comparison benchmark."""
        print("SABOT_CYPHER vs KUZU COMPARISON BENCHMARK")
        print("=" * 60)
        
        # Create test graph
        vertices, edges = self.create_test_graph(1000, 3000)
        print(f"📊 Test graph: {vertices.num_rows} vertices, {edges.num_rows} edges")
        
        # Setup Kuzu database
        kuzu_conn = self.setup_kuzu_database(vertices, edges)
        
        # Get comparison queries
        queries = self.get_comparison_queries()
        
        results = {
            'metadata': {
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'python_version': sys.version.split()[0],
                'pyarrow_version': pa.__version__,
                'kuzu_available': self.kuzu_available,
                'test_graph_size': f"{vertices.num_rows} vertices, {edges.num_rows} edges"
            },
            'comparison_results': {},
            'summary': {}
        }
        
        # Run comparison tests
        print("\n" + "=" * 60)
        print("QUERY COMPARISON TESTS")
        print("=" * 60)
        
        sabot_results = []
        kuzu_results = []
        
        for query_id, query in queries.items():
            print(f"\n🔍 Testing {query_id}:")
            
            # Benchmark SabotCypher
            sabot_result = self.benchmark_sabot_cypher(query_id, query, vertices, edges)
            sabot_results.append(sabot_result)
            
            # Benchmark Kuzu
            kuzu_result = self.benchmark_kuzu(query_id, query, kuzu_conn)
            kuzu_results.append(kuzu_result)
            
            # Store results
            results['comparison_results'][query_id] = {
                'query': query,
                'sabot_cypher': sabot_result,
                'kuzu': kuzu_result
            }
        
        # Generate summary
        results['summary'] = self._generate_comparison_summary(sabot_results, kuzu_results)
        
        return results
    
    def _generate_comparison_summary(self, sabot_results: List[Dict], kuzu_results: List[Dict]) -> Dict[str, Any]:
        """Generate comparison summary."""
        # SabotCypher summary
        sabot_successful = [r for r in sabot_results if r['success']]
        sabot_avg_time = sum(r['avg_time_ms'] for r in sabot_successful) / len(sabot_successful) if sabot_successful else 0
        
        # Kuzu summary
        kuzu_successful = [r for r in kuzu_results if r['success']]
        kuzu_avg_time = sum(r['avg_time_ms'] for r in kuzu_successful) / len(kuzu_successful) if kuzu_successful else 0
        
        # Performance comparison
        performance_ratio = kuzu_avg_time / sabot_avg_time if sabot_avg_time > 0 and kuzu_avg_time > 0 else 0
        
        return {
            'sabot_cypher': {
                'total_queries': len(sabot_results),
                'successful_queries': len(sabot_successful),
                'success_rate': len(sabot_successful) / len(sabot_results) * 100 if sabot_results else 0,
                'avg_execution_time_ms': sabot_avg_time
            },
            'kuzu': {
                'total_queries': len(kuzu_results),
                'successful_queries': len(kuzu_successful),
                'success_rate': len(kuzu_successful) / len(kuzu_results) * 100 if kuzu_results else 0,
                'avg_execution_time_ms': kuzu_avg_time
            },
            'comparison': {
                'performance_ratio': performance_ratio,
                'sabot_faster': sabot_avg_time < kuzu_avg_time if sabot_avg_time > 0 and kuzu_avg_time > 0 else False,
                'speedup_factor': performance_ratio if performance_ratio > 1 else 1/performance_ratio if performance_ratio > 0 else 0
            }
        }
    
    def print_comparison_summary(self, results: Dict[str, Any]):
        """Print comparison summary."""
        summary = results['summary']
        
        print("\n" + "=" * 60)
        print("COMPARISON SUMMARY")
        print("=" * 60)
        
        print(f"\n📊 SabotCypher Results:")
        print(f"   Queries: {summary['sabot_cypher']['total_queries']}")
        print(f"   Successful: {summary['sabot_cypher']['successful_queries']}")
        print(f"   Success Rate: {summary['sabot_cypher']['success_rate']:.1f}%")
        print(f"   Avg Time: {summary['sabot_cypher']['avg_execution_time_ms']:.2f}ms")
        
        print(f"\n🔧 Kuzu Results:")
        print(f"   Queries: {summary['kuzu']['total_queries']}")
        print(f"   Successful: {summary['kuzu']['successful_queries']}")
        print(f"   Success Rate: {summary['kuzu']['success_rate']:.1f}%")
        print(f"   Avg Time: {summary['kuzu']['avg_execution_time_ms']:.2f}ms")
        
        print(f"\n⚡ Performance Comparison:")
        if summary['comparison']['performance_ratio'] > 0:
            if summary['comparison']['sabot_faster']:
                print(f"   SabotCypher is {summary['comparison']['speedup_factor']:.1f}x faster than Kuzu")
            else:
                print(f"   Kuzu is {summary['comparison']['speedup_factor']:.1f}x faster than SabotCypher")
        else:
            print("   Performance comparison not available")
        
        print("\n" + "=" * 60)
        print("COMPARISON COMPLETE!")
        print("=" * 60)
    
    def save_results(self, results: Dict[str, Any], filename: str = None):
        """Save comparison results to JSON file."""
        if filename is None:
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            filename = f"sabot_cypher_vs_kuzu_{timestamp}.json"
        
        filepath = Path(__file__).parent / filename
        
        with open(filepath, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\n💾 Results saved to: {filepath}")


def main():
    """Main comparison function."""
    benchmark = KuzuComparisonBenchmark()
    
    try:
        results = benchmark.run_comparison_benchmark()
        benchmark.print_comparison_summary(results)
        benchmark.save_results(results)
        
        # Return exit code based on success rate
        sabot_success_rate = results['summary']['sabot_cypher']['success_rate']
        return 0 if sabot_success_rate >= 90 else 1
        
    except Exception as e:
        print(f"\n❌ Comparison benchmark failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
