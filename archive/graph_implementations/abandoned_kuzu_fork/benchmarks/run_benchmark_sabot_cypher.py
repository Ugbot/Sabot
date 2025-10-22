#!/usr/bin/env python3
"""
SabotCypher Benchmark Runner

Runs Kuzu study queries Q1-Q9 using SabotCypher and compares with:
- Kuzu reference implementation
- Sabot code-mode (Python queries)

Based on: benchmarks/kuzu_study/run_benchmark.py
"""

import sys
import time
import pyarrow as pa
import pyarrow.compute as pc

# Add parent to path
sys.path.insert(0, '/Users/bengamble/Sabot')

# TODO: Import sabot_cypher when ready
# from sabot_cypher import SabotCypherBridge

# For now, use placeholder
class SabotCypherBridge:
    @staticmethod
    def create():
        raise NotImplementedError("SabotCypherBridge not yet implemented")

# Query definitions from kuzu_study
QUERIES = {
    "Q1": """
        MATCH (follower:Person)-[:Follows]->(person:Person)
        RETURN person.id AS personID, person.name AS name, count(follower.id) AS numFollowers
        ORDER BY numFollowers DESC LIMIT 3
    """,
    
    "Q2": """
        MATCH (follower:Person)-[:Follows]->(person:Person)
        WITH person, count(follower.id) as numFollowers
        ORDER BY numFollowers DESC LIMIT 1
        MATCH (person)-[:LivesIn]->(city:City)
        RETURN person.name AS name, numFollowers, city.city AS city
    """,
    
    "Q3": """
        MATCH (a:Person)-[:Follows*1..3]->(b:Person)
        WHERE lower(a.gender) = 'male' AND lower(b.gender) = 'female'
        AND a.age >= 23 AND a.age <= 30
        RETURN avg(b.age) AS avgAge
        ORDER BY avgAge DESC
    """,
    
    "Q4": """
        MATCH (a:Person)-[:Follows*1..3]->(b:Person)
        WHERE lower(a.gender) = 'male' AND lower(b.gender) = 'female'
        RETURN count(*) AS numPaths
    """,
    
    "Q5": """
        MATCH (p:Person)-[:HasInterest]->(i:Interest)
        WHERE lower(i.interest) = 'tennis'
        AND lower(p.gender) = 'male'
        WITH p, i
        MATCH (p)-[:LivesIn]->(c:City)
        WHERE c.city = 'Waterloo' AND c.country = 'Canada'
        RETURN count(p.id) AS numPersons
    """,
    
    "Q6": """
        MATCH (p:Person)-[:HasInterest]->(i:Interest)
        WHERE lower(i.interest) = 'fine dining'
        AND lower(p.gender) = 'male'
        WITH p, count(i.interest) AS numInterests
        WHERE numInterests >= 1
        MATCH (p)-[:LivesIn]->(c:City)
        RETURN p.name AS name, numInterests, c.city AS city
        ORDER BY numInterests DESC LIMIT 3
    """,
    
    "Q7": """
        MATCH (a:Person)-[:Follows]->(b:Person)-[:Follows]->(c:Person)
        WHERE a.age - b.age < 5 AND b.age - c.age < 5
        WITH a, count(*) AS numPaths
        ORDER BY numPaths DESC LIMIT 20
        MATCH (a)-[:LivesIn]->(c:City)
        RETURN a.name AS name, numPaths, c.city AS city
        ORDER BY numPaths DESC
    """,
    
    "Q8": """
        MATCH (a:Person)-[:Follows]->(b:Person)-[:Follows]->(c:Person)
        RETURN count(*) AS numPaths
    """,
    
    "Q9": """
        MATCH (a:Person)-[:Follows]->(b:Person)-[:Follows]->(c:Person)
        WHERE b.age < 50 AND c.age > 25
        RETURN count(*) AS numPaths
    """,
}


def load_data():
    """Load graph data from Kuzu study dataset."""
    import polars as pl
    
    print("Loading graph data...")
    
    # Load vertices
    persons = pl.read_parquet('/Users/bengamble/Sabot/benchmarks/kuzu_study/reference/data/person.parquet')
    cities = pl.read_parquet('/Users/bengamble/Sabot/benchmarks/kuzu_study/reference/data/city.parquet')
    interests = pl.read_parquet('/Users/bengamble/Sabot/benchmarks/kuzu_study/reference/data/interest.parquet')
    
    # Load edges
    follows = pl.read_parquet('/Users/bengamble/Sabot/benchmarks/kuzu_study/reference/data/follows.parquet')
    lives_in = pl.read_parquet('/Users/bengamble/Sabot/benchmarks/kuzu_study/reference/data/livesIn.parquet')
    has_interest = pl.read_parquet('/Users/bengamble/Sabot/benchmarks/kuzu_study/reference/data/hasInterest.parquet')
    
    # Convert to Arrow
    vertices_tables = [
        persons.to_arrow(),
        cities.to_arrow(),
        interests.to_arrow(),
    ]
    
    edges_tables = [
        follows.to_arrow(),
        lives_in.to_arrow(),
        has_interest.to_arrow(),
    ]
    
    # Unify schemas (add label/type columns)
    # TODO: Implement schema unification
    
    vertices = pa.concat_tables(vertices_tables, promote=True)
    edges = pa.concat_tables(edges_tables, promote=True)
    
    print(f"Loaded {vertices.num_rows:,} vertices and {edges.num_rows:,} edges")
    
    return vertices, edges


def run_benchmark():
    """Run all queries and report results."""
    print("=" * 70)
    print("SabotCypher Benchmark Runner")
    print("=" * 70)
    print()
    
    # Load data
    try:
        vertices, edges = load_data()
    except Exception as e:
        print(f"Error loading data: {e}")
        print("Please ensure Kuzu study dataset is available")
        return
    
    # Create bridge
    try:
        bridge = SabotCypherBridge.create()
        bridge.register_graph(vertices, edges)
        print("SabotCypher bridge initialized")
        print()
    except NotImplementedError:
        print("⚠️  SabotCypher not yet implemented")
        print("This is a placeholder benchmark runner")
        print()
        print("Once implementation is complete, this will run Q1-Q9 and report:")
        print("  - Execution time per query")
        print("  - Result correctness (vs Kuzu)")
        print("  - Performance comparison")
        print()
        return
    
    # Run queries
    results = {}
    total_time = 0
    
    for query_name, query_text in sorted(QUERIES.items()):
        print(f"Running {query_name}...")
        print(f"  {query_text.strip()[:60]}...")
        
        try:
            start = time.time()
            result = bridge.execute(query_text)
            elapsed = time.time() - start
            
            total_time += elapsed
            results[query_name] = {
                'time': elapsed,
                'rows': result.table.num_rows,
                'success': True,
            }
            
            print(f"  ✅ {result.table.num_rows} rows in {elapsed*1000:.2f}ms")
            
        except Exception as e:
            results[query_name] = {
                'success': False,
                'error': str(e),
            }
            print(f"  ❌ Error: {e}")
        
        print()
    
    # Summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    successful = sum(1 for r in results.values() if r.get('success'))
    print(f"Queries completed: {successful}/{len(QUERIES)}")
    print(f"Total time: {total_time*1000:.2f}ms")
    
    if successful > 0:
        avg_time = (total_time / successful) * 1000
        print(f"Average time: {avg_time:.2f}ms")
    
    print()
    
    # Detailed results
    print("Query Results:")
    print("-" * 70)
    for query_name in sorted(results.keys()):
        r = results[query_name]
        if r.get('success'):
            print(f"  {query_name}: {r['rows']:>6} rows  {r['time']*1000:>8.2f}ms")
        else:
            print(f"  {query_name}: FAILED - {r.get('error')}")
    
    print()


if __name__ == "__main__":
    run_benchmark()

