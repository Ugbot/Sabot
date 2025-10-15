#!/usr/bin/env python3
"""
Q1-Q9 Benchmark Validation

Validates SabotCypher against the standard graph benchmark queries.
"""

import sys
import os
import pyarrow as pa
import time
from pathlib import Path

# Add integration path
sys.path.append(str(Path(__file__).parent.parent / "python_integration"))

from sabot_cypher_integration import SabotCypherIntegration


class Q1Q9Validator:
    """
    Validator for Q1-Q9 benchmark queries.
    
    Tests SabotCypher against standard graph benchmark queries.
    """
    
    def __init__(self):
        """Initialize validator."""
        self.integration = SabotCypherIntegration()
        self.results = []
    
    def create_benchmark_graph(self):
        """Create a benchmark graph for testing."""
        # Create vertices (people, companies, etc.)
        vertices = pa.table({
            'id': pa.array(list(range(1, 1001)), type=pa.int64()),
            'name': pa.array([f"Person_{i}" for i in range(1, 1001)]),
            'age': pa.array([20 + (i % 50) for i in range(1, 1001)], type=pa.int32()),
            'city': pa.array(['NYC', 'LA', 'Chicago', 'Boston', 'Seattle'][i % 5] for i in range(1, 1001)),
            'company': pa.array([f"Company_{i % 100}" for i in range(1, 1001)])
        })
        
        # Create edges (relationships)
        edges = pa.table({
            'source': pa.array([i for i in range(1, 1001) for _ in range(3)], type=pa.int64()),
            'target': pa.array([(i + j) % 1000 + 1 for i in range(1, 1001) for j in [1, 2, 3]], type=pa.int64()),
            'type': pa.array(['KNOWS', 'WORKS_WITH', 'FRIENDS'][i % 3] for i in range(3000)),
            'weight': pa.array([0.1 + (i % 9) * 0.1 for i in range(3000)], type=pa.float64())
        })
        
        return vertices, edges
    
    def get_q1_q9_queries(self):
        """Get Q1-Q9 benchmark queries."""
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
    
    def validate_query(self, query_id: str, query: str, vertices: pa.Table, edges: pa.Table):
        """Validate a single query."""
        print(f"\n🔍 {query_id}: {query}")
        
        start_time = time.time()
        
        # Explain plan
        explanation = self.integration.explain_plan(query)
        plan_time = time.time() - start_time
        
        if not explanation['success']:
            print(f"   ❌ Plan failed: {explanation['error']}")
            return {
                'query_id': query_id,
                'query': query,
                'success': False,
                'error': explanation['error'],
                'plan_time_ms': plan_time * 1000
            }
        
        # Execute query
        exec_start = time.time()
        result = self.integration.execute_cypher(query, vertices, edges)
        exec_time = time.time() - exec_start
        
        if result['success']:
            print(f"   ✅ Success")
            print(f"      Plan: {explanation['summary']['total_operators']} operators")
            print(f"      Rows: {result['num_rows']}")
            print(f"      Time: {result['execution_time_ms']:.2f}ms")
            
            return {
                'query_id': query_id,
                'query': query,
                'success': True,
                'num_rows': result['num_rows'],
                'execution_time_ms': result['execution_time_ms'],
                'plan_time_ms': plan_time * 1000,
                'operators': explanation['summary']['total_operators'],
                'has_joins': explanation['summary']['has_joins'],
                'has_aggregates': explanation['summary']['has_aggregates'],
                'has_filters': explanation['summary']['has_filters']
            }
        else:
            print(f"   ❌ Execution failed: {result['error']}")
            return {
                'query_id': query_id,
                'query': query,
                'success': False,
                'error': result['error'],
                'execution_time_ms': exec_time * 1000,
                'plan_time_ms': plan_time * 1000
            }
    
    def run_validation(self):
        """Run Q1-Q9 validation."""
        print("Q1-Q9 Benchmark Validation")
        print("=" * 50)
        
        # Create benchmark graph
        print("Creating benchmark graph...")
        vertices, edges = self.create_benchmark_graph()
        print(f"✅ Graph created: {vertices.num_rows} vertices, {edges.num_rows} edges")
        
        # Get queries
        queries = self.get_q1_q9_queries()
        
        # Validate each query
        for query_id, query in queries.items():
            result = self.validate_query(query_id, query, vertices, edges)
            self.results.append(result)
        
        # Print summary
        self.print_summary()
        
        return self.results
    
    def print_summary(self):
        """Print validation summary."""
        print("\n" + "=" * 50)
        print("Q1-Q9 Validation Summary")
        print("=" * 50)
        
        successful = [r for r in self.results if r['success']]
        failed = [r for r in self.results if not r['success']]
        
        print(f"Total queries: {len(self.results)}")
        print(f"Successful: {len(successful)}")
        print(f"Failed: {len(failed)}")
        print(f"Success rate: {len(successful)/len(self.results)*100:.1f}%")
        
        if successful:
            avg_time = sum(r['execution_time_ms'] for r in successful) / len(successful)
            print(f"Average execution time: {avg_time:.2f}ms")
            
            print("\nSuccessful queries:")
            for r in successful:
                print(f"  {r['query_id']}: {r['num_rows']} rows, {r['execution_time_ms']:.2f}ms")
        
        if failed:
            print("\nFailed queries:")
            for r in failed:
                print(f"  {r['query_id']}: {r['error']}")
        
        print("\n" + "=" * 50)
        print("Validation complete!")


def main():
    """Main validation function."""
    validator = Q1Q9Validator()
    results = validator.run_validation()
    
    # Return exit code based on results
    successful = [r for r in results if r['success']]
    return 0 if len(successful) == len(results) else 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
