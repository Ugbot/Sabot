#!/usr/bin/env python3
"""
SabotCypher vs Kuzu Fair Comparison Benchmark

Fair comparison using Kuzu's Python API (not CLI) to eliminate overhead.
Based on the methodology from: https://github.com/prrao87/kuzudb-study
"""

import sys
import os
import pyarrow as pa
import time
import json
import tempfile
import shutil
from pathlib import Path
from typing import Dict, List, Any, Tuple

# Add integration path
sys.path.append(str(Path(__file__).parent.parent / "python_integration"))

from sabot_cypher_integration import SabotCypherIntegration

# Import Kuzu
try:
    import kuzu
    KUZU_AVAILABLE = True
except ImportError:
    KUZU_AVAILABLE = False
    print("❌ Kuzu Python API not available")


class FairKuzuComparison:
    """
    Fair comparison benchmark using Kuzu's Python API.
    
    Eliminates CLI overhead by using persistent database connections
    and pre-loaded data, matching the original Kuzu study methodology.
    """
    
    def __init__(self):
        """Initialize comparison benchmark."""
        self.sabot_cypher = SabotCypherIntegration()
        self.kuzu_available = KUZU_AVAILABLE
        self.kuzu_db_path = None
        self.kuzu_conn = None
        
    def create_social_network_graph(self, num_persons: int = 1000) -> Tuple[pa.Table, pa.Table]:
        """Create a social network graph similar to the Kuzu study."""
        # Create persons
        persons = pa.table({
            'id': pa.array(list(range(1, num_persons + 1)), type=pa.int64()),
            'name': pa.array([f"Person_{i}" for i in range(1, num_persons + 1)]),
            'age': pa.array([18 + (i % 65) for i in range(1, num_persons + 1)], type=pa.int32()),
            'gender': pa.array(['male', 'female'][i % 2] for i in range(1, num_persons + 1)),
            'city': pa.array([f"City_{(i % 20) + 1}" for i in range(1, num_persons + 1)]),
            'state': pa.array([f"State_{((i % 20) // 5) + 1}" for i in range(1, num_persons + 1)]),
            'country': pa.array(['United States', 'United Kingdom', 'Canada', 'Australia'][((i % 20) // 5)] for i in range(1, num_persons + 1))
        })
        
        # Create follows relationships
        follows_edges = []
        for i in range(num_persons):
            # Each person follows 3-5 other persons
            num_follows = 3 + (i % 3)
            for j in range(num_follows):
                target = ((i + j + 1) % num_persons) + 1
                follows_edges.append({
                    'source': i + 1,
                    'target': target,
                    'type': 'Follows'
                })
        
        edges = pa.table({
            'source': pa.array([e['source'] for e in follows_edges], type=pa.int64()),
            'target': pa.array([e['target'] for e in follows_edges], type=pa.int64()),
            'type': pa.array([e['type'] for e in follows_edges])
        })
        
        # Create vertices table
        vertices = persons
        
        return vertices, edges
    
    def setup_kuzu_database(self, vertices: pa.Table, edges: pa.Table) -> bool:
        """Setup Kuzu database with pre-loaded data."""
        if not self.kuzu_available:
            return False
            
        try:
            # Create temporary database directory
            temp_dir = tempfile.mkdtemp()
            self.kuzu_db_path = os.path.join(temp_dir, "benchmark.db")
            
            # Create database and connection
            db = kuzu.Database(self.kuzu_db_path)
            self.kuzu_conn = kuzu.Connection(db, num_threads=0)  # Use all available threads
            
            # Create schema
            self.kuzu_conn.execute("""
                CREATE NODE TABLE Person(
                    id INT64,
                    name STRING,
                    age INT32,
                    gender STRING,
                    city STRING,
                    state STRING,
                    country STRING,
                    PRIMARY KEY(id)
                )
            """)
            
            self.kuzu_conn.execute("""
                CREATE REL TABLE Follows(FROM Person TO Person)
            """)
            
            # Insert vertices using CREATE
            print(f"  Loading {vertices.num_rows} vertices into Kuzu...")
            for i in range(vertices.num_rows):
                row = vertices.slice(i, 1)
                self.kuzu_conn.execute(f"""
                    CREATE (p:Person {{
                        id: {row['id'][0].as_py()},
                        name: '{row['name'][0].as_py()}',
                        age: {row['age'][0].as_py()},
                        gender: '{row['gender'][0].as_py()}',
                        city: '{row['city'][0].as_py()}',
                        state: '{row['state'][0].as_py()}',
                        country: '{row['country'][0].as_py()}'
                    }})
                """)
            
            # Insert edges using CREATE
            print(f"  Loading {edges.num_rows} edges into Kuzu...")
            for i in range(edges.num_rows):
                row = edges.slice(i, 1)
                if row['type'][0].as_py() == 'Follows':
                    self.kuzu_conn.execute(f"""
                        MATCH (a:Person {{id: {row['source'][0].as_py()}}}), (b:Person {{id: {row['target'][0].as_py()}}})
                        CREATE (a)-[:Follows]->(b)
                    """)
            
            print(f"✅ Kuzu database created and loaded")
            return True
            
        except Exception as e:
            print(f"❌ Failed to setup Kuzu database: {e}")
            return False
    
    def cleanup_kuzu_database(self):
        """Cleanup Kuzu database."""
        if self.kuzu_db_path and os.path.exists(self.kuzu_db_path):
            shutil.rmtree(os.path.dirname(self.kuzu_db_path))
    
    def get_comparison_queries(self) -> Dict[str, str]:
        """Get queries for comparison."""
        return {
            'Q1': """
                MATCH (follower:Person)-[:Follows]->(person:Person)
                RETURN person.id AS personID, person.name AS name, count(follower.id) AS numFollowers
                ORDER BY numFollowers DESC LIMIT 3
            """,
            'Q2': """
                MATCH (follower:Person)-[:Follows]->(person:Person)
                WITH person, count(follower.id) as numFollowers
                ORDER BY numFollowers DESC LIMIT 1
                MATCH (person)
                RETURN person.name AS name, numFollowers
            """,
            'Q3': """
                MATCH (p:Person)
                WHERE p.country = 'United States'
                RETURN p.city AS city, avg(p.age) AS averageAge
                ORDER BY averageAge LIMIT 5
            """,
            'Q4': """
                MATCH (p:Person)
                WHERE p.age >= 30 AND p.age <= 40
                RETURN p.country AS countries, count(p) AS personCounts
                ORDER BY personCounts DESC LIMIT 3
            """,
            'Q5': """
                MATCH (p:Person)
                WHERE lower(p.gender) = 'male' AND p.city = 'City_1' AND p.country = 'United Kingdom'
                RETURN count(p) AS numPersons
            """,
            'Q6': """
                MATCH (p:Person)
                WHERE lower(p.gender) = 'female'
                RETURN count(p.id) AS numPersons, p.city AS city, p.country AS country
                ORDER BY numPersons DESC LIMIT 5
            """,
            'Q7': """
                MATCH (p:Person)
                WHERE p.age >= 23 AND p.age <= 30 AND p.country = 'United States'
                RETURN count(p.id) AS numPersons, p.state AS state, p.country AS country
                ORDER BY numPersons DESC LIMIT 1
            """,
            'Q8': """
                MATCH (a:Person)-[:Follows]->(b:Person)-[:Follows]->(c:Person)
                RETURN count(*) AS numPaths
            """,
            'Q9': """
                MATCH (a:Person)-[:Follows]->(b:Person)-[:Follows]->(c:Person)
                WHERE b.age < 50 AND c.age > 25
                RETURN count(*) as numPaths
            """
        }
    
    def benchmark_sabot_cypher(self, query_id: str, query: str, vertices: pa.Table, edges: pa.Table, iterations: int = 5) -> Dict[str, Any]:
        """Benchmark SabotCypher query."""
        print(f"  🔍 SabotCypher {query_id}: {query.strip()[:80]}...")
        
        # Warmup
        self.sabot_cypher.execute_cypher(query, vertices, edges)
        
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
            
            print(f"    ✅ {avg_time:.4f}ms avg ({min_time:.4f}-{max_time:.4f}ms), {success_count}/{iterations} success")
            
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
    
    def benchmark_kuzu(self, query_id: str, query: str, iterations: int = 5) -> Dict[str, Any]:
        """Benchmark Kuzu query using Python API."""
        if not self.kuzu_conn:
            return {
                'engine': 'Kuzu',
                'query_id': query_id,
                'query': query,
                'success': False,
                'error': 'Kuzu connection not available'
            }
        
        print(f"  🔍 Kuzu {query_id}: {query.strip()[:80]}...")
        
        # Warmup
        try:
            result = self.kuzu_conn.execute(query)
            _ = result.get_as_pl()
        except:
            pass
        
        times = []
        success_count = 0
        
        for i in range(iterations):
            try:
                start_time = time.time()
                result = self.kuzu_conn.execute(query)
                _ = result.get_as_pl()  # Materialize results
                end_time = time.time()
                
                times.append((end_time - start_time) * 1000)  # Convert to ms
                success_count += 1
                
            except Exception as e:
                print(f"    ❌ Kuzu query failed: {e}")
                continue
        
        if times:
            avg_time = sum(times) / len(times)
            min_time = min(times)
            max_time = max(times)
            
            print(f"    ✅ {avg_time:.4f}ms avg ({min_time:.4f}-{max_time:.4f}ms), {success_count}/{iterations} success")
            
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
    
    def run_fair_comparison(self) -> Dict[str, Any]:
        """Run fair comparison benchmark."""
        print("SABOT_CYPHER vs KUZU FAIR COMPARISON")
        print("=" * 60)
        print("Using Kuzu Python API (not CLI) for fair comparison")
        print("Methodology: https://github.com/prrao87/kuzudb-study")
        
        # Create social network graph
        vertices, edges = self.create_social_network_graph(1000)
        print(f"\n📊 Social network graph: {vertices.num_rows} vertices, {edges.num_rows} edges")
        
        # Setup Kuzu database with pre-loaded data
        print(f"\n📥 Setting up Kuzu database...")
        kuzu_ready = self.setup_kuzu_database(vertices, edges)
        
        if not kuzu_ready:
            print("❌ Kuzu setup failed, comparison cannot proceed")
            return None
        
        # Get comparison queries
        queries = self.get_comparison_queries()
        
        results = {
            'metadata': {
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'python_version': sys.version.split()[0],
                'pyarrow_version': pa.__version__,
                'kuzu_version': kuzu.__version__ if self.kuzu_available else 'N/A',
                'kuzu_available': self.kuzu_available,
                'test_graph_size': f"{vertices.num_rows} vertices, {edges.num_rows} edges",
                'comparison_type': 'fair_python_api',
                'study_reference': 'https://github.com/prrao87/kuzudb-study'
            },
            'comparison_results': {},
            'summary': {}
        }
        
        # Run comparison tests
        print("\n" + "=" * 60)
        print("FAIR QUERY COMPARISON")
        print("=" * 60)
        
        sabot_results = []
        kuzu_results = []
        
        for query_id, query in queries.items():
            print(f"\n🔍 Testing {query_id}:")
            
            # Benchmark SabotCypher
            sabot_result = self.benchmark_sabot_cypher(query_id, query, vertices, edges)
            sabot_results.append(sabot_result)
            
            # Benchmark Kuzu
            kuzu_result = self.benchmark_kuzu(query_id, query)
            kuzu_results.append(kuzu_result)
            
            # Calculate speedup
            if sabot_result['success'] and kuzu_result['success']:
                speedup = kuzu_result['avg_time_ms'] / sabot_result['avg_time_ms']
                print(f"    ⚡ Speedup: {speedup:.1f}x faster")
            
            # Store results
            results['comparison_results'][query_id] = {
                'query': query,
                'sabot_cypher': sabot_result,
                'kuzu': kuzu_result
            }
        
        # Generate summary
        results['summary'] = self._generate_comparison_summary(sabot_results, kuzu_results)
        
        # Cleanup
        self.cleanup_kuzu_database()
        
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
        print("FAIR COMPARISON SUMMARY")
        print("=" * 60)
        
        print(f"\n📊 SabotCypher Results:")
        print(f"   Queries: {summary['sabot_cypher']['total_queries']}")
        print(f"   Successful: {summary['sabot_cypher']['successful_queries']}")
        print(f"   Success Rate: {summary['sabot_cypher']['success_rate']:.1f}%")
        print(f"   Avg Time: {summary['sabot_cypher']['avg_execution_time_ms']:.4f}ms")
        
        print(f"\n🔧 Kuzu Results (Python API):")
        print(f"   Queries: {summary['kuzu']['total_queries']}")
        print(f"   Successful: {summary['kuzu']['successful_queries']}")
        print(f"   Success Rate: {summary['kuzu']['success_rate']:.1f}%")
        print(f"   Avg Time: {summary['kuzu']['avg_execution_time_ms']:.4f}ms")
        
        print(f"\n⚡ Performance Comparison:")
        if summary['comparison']['performance_ratio'] > 0:
            if summary['comparison']['sabot_faster']:
                print(f"   SabotCypher is {summary['comparison']['speedup_factor']:.1f}x faster than Kuzu")
            else:
                print(f"   Kuzu is {summary['comparison']['speedup_factor']:.1f}x faster than SabotCypher")
        else:
            print("   Performance comparison not available")
        
        print(f"\n📈 Comparison Notes:")
        print(f"   ✅ Fair comparison using Python APIs (not CLI)")
        print(f"   ✅ Pre-loaded data (no initialization overhead)")
        print(f"   ✅ Warmup runs performed")
        print(f"   ✅ Results materialized for both engines")
        
        print("\n" + "=" * 60)
        print("FAIR COMPARISON COMPLETE!")
        print("=" * 60)
    
    def save_results(self, results: Dict[str, Any], filename: str = None):
        """Save comparison results to JSON file."""
        if filename is None:
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            filename = f"sabot_cypher_vs_kuzu_fair_{timestamp}.json"
        
        filepath = Path(__file__).parent / filename
        
        with open(filepath, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\n💾 Results saved to: {filepath}")


def main():
    """Main comparison function."""
    benchmark = FairKuzuComparison()
    
    if not KUZU_AVAILABLE:
        print("\n❌ Kuzu Python API not available")
        print("Install with: python3 -m pip install --user kuzu")
        return 1
    
    try:
        results = benchmark.run_fair_comparison()
        
        if results:
            benchmark.print_comparison_summary(results)
            benchmark.save_results(results)
            
            # Return exit code based on success rate
            sabot_success_rate = results['summary']['sabot_cypher']['success_rate']
            return 0 if sabot_success_rate >= 90 else 1
        else:
            return 1
        
    except Exception as e:
        print(f"\n❌ Fair comparison benchmark failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
