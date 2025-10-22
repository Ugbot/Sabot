#!/usr/bin/env python3
"""
SabotCypher vs Kuzu Study Comparison Benchmark

Based on the Kuzu benchmark study from:
https://github.com/prrao87/kuzudb-study

This benchmark implements the same 9 queries used in the Kuzu study
to provide a fair comparison between SabotCypher and Kuzu.
"""

import sys
import os
import pyarrow as pa
import time
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, List, Any, Tuple

# Add integration path
sys.path.append(str(Path(__file__).parent.parent / "python_integration"))

from sabot_cypher_integration import SabotCypherIntegration


class KuzuStudyComparison:
    """
    Comparison benchmark based on the Kuzu study methodology.
    
    Implements the same 9 queries from the Kuzu benchmark study:
    https://github.com/prrao87/kuzudb-study
    """
    
    def __init__(self):
        """Initialize comparison benchmark."""
        self.sabot_cypher = SabotCypherIntegration()
        self.kuzu_available = self._check_kuzu_availability()
        
    def _check_kuzu_availability(self) -> bool:
        """Check if Kuzu CLI is available."""
        try:
            result = subprocess.run(['kuzu', '--version'], capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ Kuzu CLI available: {result.stdout.strip()}")
                return True
            else:
                print("❌ Kuzu CLI not working")
                return False
        except FileNotFoundError:
            print("❌ Kuzu CLI not found")
            return False
    
    def create_social_network_graph(self, num_persons: int = 1000) -> Tuple[pa.Table, pa.Table]:
        """Create a social network graph similar to the Kuzu study."""
        # Create persons
        persons = pa.table({
            'id': pa.array(list(range(1, num_persons + 1)), type=pa.int64()),
            'name': pa.array([f"Person_{i}" for i in range(1, num_persons + 1)]),
            'age': pa.array([18 + (i % 65) for i in range(1, num_persons + 1)], type=pa.int32()),
            'gender': pa.array(['male', 'female'][i % 2] for i in range(1, num_persons + 1))
        })
        
        # Create cities
        cities = pa.table({
            'id': pa.array(list(range(1, 21)), type=pa.int64()),
            'city': pa.array([f"City_{i}" for i in range(1, 21)]),
            'state': pa.array([f"State_{(i-1)//5 + 1}" for i in range(1, 21)]),
            'country': pa.array(['United States', 'United Kingdom', 'Canada', 'Australia'][(i-1)//5] for i in range(1, 21))
        })
        
        # Create interests
        interests = pa.table({
            'id': pa.array(list(range(1, 11)), type=pa.int64()),
            'interest': pa.array(['tennis', 'photography', 'fine dining', 'music', 'art', 'sports', 'travel', 'books', 'movies', 'cooking'])
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
        
        # Create lives_in relationships
        lives_in_edges = []
        for i in range(num_persons):
            city_id = (i % 20) + 1
            lives_in_edges.append({
                'source': i + 1,
                'target': city_id,
                'type': 'LivesIn'
            })
        
        # Create has_interest relationships
        has_interest_edges = []
        for i in range(num_persons):
            # Each person has 2-4 interests
            num_interests = 2 + (i % 3)
            for j in range(num_interests):
                interest_id = ((i + j) % 10) + 1
                has_interest_edges.append({
                    'source': i + 1,
                    'target': interest_id,
                    'type': 'HasInterest'
                })
        
        # Combine all edges
        all_edges = follows_edges + lives_in_edges + has_interest_edges
        
        edges = pa.table({
            'source': pa.array([e['source'] for e in all_edges], type=pa.int64()),
            'target': pa.array([e['target'] for e in all_edges], type=pa.int64()),
            'type': pa.array([e['type'] for e in all_edges])
        })
        
        # Combine all vertices
        all_vertices = []
        
        # Add persons
        for i in range(persons.num_rows):
            all_vertices.append({
                'id': persons['id'][i].as_py(),
                'name': persons['name'][i].as_py(),
                'age': persons['age'][i].as_py(),
                'gender': persons['gender'][i].as_py(),
                'type': 'Person'
            })
        
        # Add cities
        for i in range(cities.num_rows):
            all_vertices.append({
                'id': cities['id'][i].as_py() + 10000,  # Offset to avoid ID conflicts
                'name': cities['city'][i].as_py(),
                'age': None,
                'gender': None,
                'type': 'City'
            })
        
        # Add interests
        for i in range(interests.num_rows):
            all_vertices.append({
                'id': interests['id'][i].as_py() + 20000,  # Offset to avoid ID conflicts
                'name': interests['interest'][i].as_py(),
                'age': None,
                'gender': None,
                'type': 'Interest'
            })
        
        vertices = pa.table({
            'id': pa.array([v['id'] for v in all_vertices], type=pa.int64()),
            'name': pa.array([v['name'] for v in all_vertices]),
            'age': pa.array([v['age'] for v in all_vertices], type=pa.int32()),
            'gender': pa.array([v['gender'] for v in all_vertices]),
            'type': pa.array([v['type'] for v in all_vertices])
        })
        
        return vertices, edges
    
    def get_kuzu_study_queries(self) -> Dict[str, str]:
        """Get the 9 queries from the Kuzu study."""
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
                MATCH (person)-[:LivesIn]->(city:City)
                RETURN person.name AS name, numFollowers, city.city AS city, city.state AS state, city.country AS country
            """,
            'Q3': """
                MATCH (p:Person)-[:LivesIn]->(c:City)
                WHERE c.country = 'United States'
                RETURN c.city AS city, avg(p.age) AS averageAge
                ORDER BY averageAge LIMIT 5
            """,
            'Q4': """
                MATCH (p:Person)-[:LivesIn]->(c:City)
                WHERE p.age >= 30 AND p.age <= 40
                RETURN c.country AS countries, count(p) AS personCounts
                ORDER BY personCounts DESC LIMIT 3
            """,
            'Q5': """
                MATCH (p:Person)-[:HasInterest]->(i:Interest)
                WHERE lower(i.name) = 'fine dining'
                AND lower(p.gender) = 'male'
                WITH p, i
                MATCH (p)-[:LivesIn]->(c:City)
                WHERE c.name = 'City_1' AND c.country = 'United Kingdom'
                RETURN count(p) AS numPersons
            """,
            'Q6': """
                MATCH (p:Person)-[:HasInterest]->(i:Interest)
                WHERE lower(i.name) = 'tennis'
                AND lower(p.gender) = 'female'
                WITH p, i
                MATCH (p)-[:LivesIn]->(c:City)
                RETURN count(p.id) AS numPersons, c.name AS city, c.country AS country
                ORDER BY numPersons DESC LIMIT 5
            """,
            'Q7': """
                MATCH (p:Person)-[:LivesIn]->(c:City)
                WHERE p.age >= 23 AND p.age <= 30 AND c.country = 'United States'
                WITH p, c
                MATCH (p)-[:HasInterest]->(i:Interest)
                WHERE lower(i.name) = 'photography'
                RETURN count(p.id) AS numPersons, c.state AS state, c.country AS country
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
        print(f"  🔍 SabotCypher {query_id}: {query.strip()}")
        
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
    
    def benchmark_kuzu(self, query_id: str, query: str, iterations: int = 5) -> Dict[str, Any]:
        """Benchmark Kuzu query."""
        if not self.kuzu_available:
            return {
                'engine': 'Kuzu',
                'query_id': query_id,
                'query': query,
                'success': False,
                'error': 'Kuzu not available'
            }
        
        print(f"  🔍 Kuzu {query_id}: {query.strip()}")
        
        times = []
        success_count = 0
        
        for i in range(iterations):
            try:
                start_time = time.time()
                
                # Create input for Kuzu
                input_data = f"""
CREATE NODE TABLE Person(id INT64, name STRING, age INT32, gender STRING, PRIMARY KEY(id));
CREATE NODE TABLE City(id INT64, name STRING, state STRING, country STRING, PRIMARY KEY(id));
CREATE NODE TABLE Interest(id INT64, name STRING, PRIMARY KEY(id));
CREATE REL TABLE Follows(FROM Person TO Person);
CREATE REL TABLE LivesIn(FROM Person TO City);
CREATE REL TABLE HasInterest(FROM Person TO Interest);
{query};
"""
                
                result = subprocess.run([
                    'kuzu', ':memory:'
                ], input=input_data, capture_output=True, text=True, timeout=30)
                
                end_time = time.time()
                
                if result.returncode == 0 and "Error" not in result.stderr:
                    times.append((end_time - start_time) * 1000)  # Convert to ms
                    success_count += 1
                else:
                    print(f"    ❌ Kuzu query failed: {result.stderr[:100]}...")
                    
            except subprocess.TimeoutExpired:
                print(f"    ❌ Kuzu query timeout")
                continue
            except Exception as e:
                print(f"    ❌ Kuzu query error: {e}")
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
    
    def run_kuzu_study_comparison(self) -> Dict[str, Any]:
        """Run Kuzu study comparison benchmark."""
        print("SABOT_CYPHER vs KUZU STUDY COMPARISON")
        print("=" * 60)
        print("Based on: https://github.com/prrao87/kuzudb-study")
        
        # Create social network graph
        vertices, edges = self.create_social_network_graph(1000)
        print(f"📊 Social network graph: {vertices.num_rows} vertices, {edges.num_rows} edges")
        
        # Get Kuzu study queries
        queries = self.get_kuzu_study_queries()
        
        results = {
            'metadata': {
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'python_version': sys.version.split()[0],
                'pyarrow_version': pa.__version__,
                'kuzu_available': self.kuzu_available,
                'test_graph_size': f"{vertices.num_rows} vertices, {edges.num_rows} edges",
                'comparison_type': 'kuzu_study',
                'study_reference': 'https://github.com/prrao87/kuzudb-study'
            },
            'comparison_results': {},
            'summary': {}
        }
        
        # Run comparison tests
        print("\n" + "=" * 60)
        print("KUZU STUDY QUERIES COMPARISON")
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
        print("KUZU STUDY COMPARISON SUMMARY")
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
        
        print(f"\n📈 Kuzu Study Reference:")
        print(f"   Original study: https://github.com/prrao87/kuzudb-study")
        print(f"   Kuzu vs Neo4j speedup: 10.77x (Q1) to 374.45x (Q8)")
        print(f"   Our SabotCypher vs Kuzu: {summary['comparison']['speedup_factor']:.1f}x average")
        
        print("\n" + "=" * 60)
        print("KUZU STUDY COMPARISON COMPLETE!")
        print("=" * 60)
    
    def save_results(self, results: Dict[str, Any], filename: str = None):
        """Save comparison results to JSON file."""
        if filename is None:
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            filename = f"sabot_cypher_vs_kuzu_study_{timestamp}.json"
        
        filepath = Path(__file__).parent / filename
        
        with open(filepath, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\n💾 Results saved to: {filepath}")


def main():
    """Main comparison function."""
    benchmark = KuzuStudyComparison()
    
    try:
        results = benchmark.run_kuzu_study_comparison()
        benchmark.print_comparison_summary(results)
        benchmark.save_results(results)
        
        # Return exit code based on success rate
        sabot_success_rate = results['summary']['sabot_cypher']['success_rate']
        return 0 if sabot_success_rate >= 90 else 1
        
    except Exception as e:
        print(f"\n❌ Kuzu study comparison benchmark failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
