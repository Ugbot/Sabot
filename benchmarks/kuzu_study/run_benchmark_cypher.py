#!/usr/bin/env python3
"""
Sabot Graph Benchmark - Using Cypher Query Engine

This version uses Sabot's Cypher query engine to execute the 9 benchmark queries.

Note: Some queries require features not yet implemented in the Cypher engine:
- Aggregations (COUNT, SUM, AVG)
- Property access in RETURN (a.name, b.age)
- WHERE clause evaluation

For these queries, we show what WOULD be executed when fully implemented.

Usage:
    python run_benchmark_cypher.py [--iterations N] [--warmup N]
"""
import sys
import os
import time
import argparse
from pathlib import Path

# Add Sabot to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Set library path for Arrow
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib'

from data_loader import load_all_data
from sabot._cython.graph.engine import GraphQueryEngine
from sabot import cyarrow as ca
import pyarrow as pa


# The 9 Cypher queries from the Kuzu benchmark
CYPHER_QUERIES = {
    'query1': {
        'name': 'Top 3 most-followed persons',
        'cypher': """
            MATCH (follower:Person)-[:Follows]->(person:Person)
            RETURN person.id AS personID, person.name AS name, count(follower.id) AS numFollowers
            ORDER BY numFollowers DESC LIMIT 3
        """,
        'supported': False,
        'reason': 'Requires: COUNT aggregation, property access in RETURN, ORDER BY'
    },

    'query2': {
        'name': 'City where most-followed person lives',
        'cypher': """
            MATCH (follower:Person)-[:Follows]->(person:Person)
            WITH person, count(follower.id) as numFollowers
            ORDER BY numFollowers DESC LIMIT 1
            MATCH (person)-[:LivesIn]->(city:City)
            RETURN person.name AS name, numFollowers, city.city AS city, city.state AS state, city.country AS country
        """,
        'supported': False,
        'reason': 'Requires: WITH clause, COUNT, multiple MATCH, property access'
    },

    'query3': {
        'name': '5 cities with lowest avg age in US',
        'cypher': """
            MATCH (p:Person)-[:LivesIn]->(c:City)-[*1..2]->(co:Country)
            WHERE co.country = $country
            RETURN c.city AS city, avg(p.age) AS averageAge
            ORDER BY averageAge LIMIT 5
        """,
        'supported': False,
        'reason': 'Requires: Variable-length paths work, but AVG, WHERE, ORDER BY not implemented'
    },

    'query4': {
        'name': 'Persons aged 30-40 by country',
        'cypher': """
            MATCH (p:Person)-[:LivesIn]->(ci:City)-[*1..2]->(country:Country)
            WHERE p.age >= $age_lower AND p.age <= $age_upper
            RETURN country.country AS countries, count(country) AS personCounts
            ORDER BY personCounts DESC LIMIT 3
        """,
        'supported': False,
        'reason': 'Requires: WHERE on properties, COUNT, ORDER BY'
    },

    'query5': {
        'name': 'Men in London UK interested in fine dining',
        'cypher': """
            MATCH (p:Person)-[:HasInterest]->(i:Interest)
            WHERE lower(i.interest) = lower($interest)
            AND lower(p.gender) = lower($gender)
            WITH p, i
            MATCH (p)-[:LivesIn]->(c:City)
            WHERE c.city = $city AND c.country = $country
            RETURN count(p) AS numPersons
        """,
        'supported': False,
        'reason': 'Requires: WHERE clause evaluation, WITH, multiple MATCH, COUNT'
    },

    'query6': {
        'name': 'Cities with most women interested in tennis',
        'cypher': """
            MATCH (p:Person)-[:HasInterest]->(i:Interest)
            WHERE lower(i.interest) = lower($interest)
            AND lower(p.gender) = lower($gender)
            WITH p, i
            MATCH (p)-[:LivesIn]->(c:City)
            RETURN count(p.id) AS numPersons, c.city AS city, c.country AS country
            ORDER BY numPersons DESC LIMIT 5
        """,
        'supported': False,
        'reason': 'Requires: WHERE, WITH, COUNT, property access, ORDER BY'
    },

    'query7': {
        'name': 'US state with most 23-30yo interested in photography',
        'cypher': """
            MATCH (p:Person)-[:LivesIn]->(:City)-[:CityIn]->(s:State)
            WHERE p.age >= $age_lower AND p.age <= $age_upper AND s.country = $country
            WITH p, s
            MATCH (p)-[:HasInterest]->(i:Interest)
            WHERE lower(i.interest) = lower($interest)
            RETURN count(p.id) AS numPersons, s.state AS state, s.country AS country
            ORDER BY numPersons DESC LIMIT 1
        """,
        'supported': False,
        'reason': 'Requires: WHERE, WITH, multiple MATCH, COUNT, ORDER BY'
    },

    'query8': {
        'name': 'Count of 2-hop paths',
        'cypher': """
            MATCH (a:Person)-[r1:Follows]->(b:Person)-[r2:Follows]->(c:Person)
            RETURN count(*) AS numPaths
        """,
        'supported': True,  # Pattern matching works, but COUNT not supported
        'fallback': True,   # We'll use pattern matching + manual count
        'reason': 'Pattern matching works, but COUNT(*) not yet supported'
    },

    'query9': {
        'name': 'Filtered 2-hop paths',
        'cypher': """
            MATCH (a:Person)-[r1:Follows]->(b:Person)-[r2:Follows]->(c:Person)
            WHERE b.age < $age_1 AND c.age > $age_2
            RETURN count(*) as numPaths
        """,
        'supported': True,  # Pattern matching works, but WHERE + COUNT not supported
        'fallback': True,
        'reason': 'Pattern matching works, but WHERE clause + COUNT not yet supported'
    }
}


class CypherBenchmarkRunner:
    """Run Kuzu benchmark using Sabot's Cypher engine."""

    def __init__(self, data_dir: Path):
        """Initialize with data directory."""
        self.data_dir = data_dir
        self.data = None
        self.engine = None

    def load_data(self):
        """Load all graph data into Cypher engine."""
        print("="*70)
        print("LOADING DATA INTO CYPHER ENGINE")
        print("="*70)

        # Load raw data
        start = time.perf_counter()
        self.data = load_all_data(self.data_dir)
        load_time = time.perf_counter() - start
        print(f"✅ Raw data loaded in {load_time:.3f}s")
        print()

        # Create engine
        print("Creating GraphQueryEngine...")
        self.engine = GraphQueryEngine(state_store=None, enable_continuous=False)
        print("✅ Engine created")
        print()

        # Prepare vertices - for now, just use Person nodes
        # (Full multi-type vertex support requires schema unification)
        print("Preparing vertices...")

        # Use only Person nodes for this demo
        persons = self.data['person_nodes']

        # Add label column
        persons_labeled = persons.append_column(
            'label',
            ca.array(['Person'] * persons.num_rows, type=ca.string())
        )

        all_vertices = persons_labeled
        print(f"  Person vertices: {all_vertices.num_rows:,}")
        print(f"  Note: Other node types (City, State, etc.) require schema unification")

        # Prepare edges - for now, just use Follows edges
        print("Preparing edges...")

        # Follows edges
        follows = self.data['follows_edges']
        follows_labeled = follows.append_column(
            'label',
            ca.array(['Follows'] * follows.num_rows, type=ca.string())
        )

        all_edges = follows_labeled
        print(f"  Follows edges: {all_edges.num_rows:,}")
        print(f"  Note: Other edge types available but not loaded for this demo")
        print()

        # Load into engine
        print("Loading graph into engine...")
        start = time.perf_counter()
        self.engine.load_vertices(all_vertices, persist=False)
        self.engine.load_edges(all_edges, persist=False)
        load_time = time.perf_counter() - start

        stats = self.engine.get_graph_stats()
        print(f"✅ Graph loaded in {load_time:.3f}s")
        print(f"   Vertices: {stats['num_vertices']:,}")
        print(f"   Edges: {stats['num_edges']:,}")
        print()

    def show_query_support(self):
        """Show which queries are supported by the Cypher engine."""
        print("="*70)
        print("CYPHER ENGINE QUERY SUPPORT")
        print("="*70)
        print()

        supported_count = 0
        partial_count = 0
        unsupported_count = 0

        for query_id, query_info in CYPHER_QUERIES.items():
            name = query_info['name']
            supported = query_info['supported']
            fallback = query_info.get('fallback', False)
            reason = query_info['reason']

            if supported and not fallback:
                status = "✅ FULLY SUPPORTED"
                supported_count += 1
            elif supported and fallback:
                status = "⚠️  PARTIAL (pattern matching only)"
                partial_count += 1
            else:
                status = "❌ NOT SUPPORTED"
                unsupported_count += 1

            print(f"{query_id.upper()}: {name}")
            print(f"  Status: {status}")
            print(f"  Reason: {reason}")
            print()

        print("-"*70)
        print(f"Summary: {supported_count} fully supported, {partial_count} partial, {unsupported_count} not supported")
        print()

        print("What's Missing:")
        print("  🚧 Aggregations (COUNT, SUM, AVG)")
        print("  🚧 Property access in RETURN (a.name, b.age)")
        print("  🚧 WHERE clause evaluation")
        print("  🚧 ORDER BY")
        print("  🚧 WITH clause")
        print("  🚧 Multiple MATCH clauses")
        print()

        print("What Works:")
        print("  ✅ Pattern matching: (a)-[r]->(b)")
        print("  ✅ Node labels: (a:Person)")
        print("  ✅ Edge types: -[r:KNOWS]->")
        print("  ✅ Variable-length paths: -[r*1..3]->")
        print("  ✅ LIMIT clause")
        print()

    def run_supported_queries(self):
        """Run queries that are at least partially supported."""
        print("="*70)
        print("RUNNING SUPPORTED/PARTIAL QUERIES")
        print("="*70)
        print()

        # Query 8: 2-hop pattern matching
        print("[8/9] Query 8: Count of 2-hop paths")
        print("  Cypher: MATCH (a:Person)-[:Follows]->(b:Person)-[:Follows]->(c:Person)")

        try:
            # Use pattern matching directly
            query8_cypher = "MATCH (a:Person)-[:Follows]->(b:Person)-[:Follows]->(c:Person) RETURN a, b, c LIMIT 1000000"

            start = time.perf_counter()
            result = self.engine.query_cypher(query8_cypher)
            elapsed = (time.perf_counter() - start) * 1000

            num_paths = len(result) if result is not None else 0
            print(f"  ✅ Found {num_paths:,} paths in {elapsed:.2f}ms")

            if num_paths < 1000000:
                print(f"     Note: May be incomplete (limited by RETURN without aggregation)")

        except Exception as e:
            print(f"  ❌ Query failed: {e}")
            import traceback
            traceback.print_exc()

        print()

        # Query 9: Would need WHERE clause support
        print("[9/9] Query 9: Filtered 2-hop paths")
        print("  Status: ⚠️  Pattern matching works, but WHERE clause not yet supported")
        print("  Cypher: MATCH (a)-[:Follows]->(b)-[:Follows]->(c) WHERE b.age < 50 AND c.age > 25")
        print()


def main():
    parser = argparse.ArgumentParser(description="Run Sabot graph benchmark with Cypher engine")
    parser.add_argument('--iterations', type=int, default=5,
                        help='Number of timed iterations per query (default: 5)')
    parser.add_argument('--warmup', type=int, default=2,
                        help='Number of warmup iterations (default: 2)')
    args = parser.parse_args()

    # Data directory
    data_dir = Path(__file__).parent / "reference" / "data" / "output"

    if not data_dir.exists():
        print(f"❌ Error: Data directory not found: {data_dir}")
        print("   Run: cd reference/data && bash ../generate_data.sh 100000")
        return 1

    # Run benchmark
    runner = CypherBenchmarkRunner(data_dir)
    runner.load_data()
    runner.show_query_support()
    runner.run_supported_queries()

    print("="*70)
    print("CYPHER ENGINE STATUS")
    print("="*70)
    print()
    print("Current Status:")
    print("  ✅ Pattern matching core works")
    print("  ❌ Most benchmark queries need aggregations/filters")
    print()
    print("Next Steps to Support Full Benchmark:")
    print("  1. Implement COUNT, SUM, AVG aggregations")
    print("  2. Implement WHERE clause evaluation on properties")
    print("  3. Implement property access in RETURN (a.name)")
    print("  4. Implement ORDER BY")
    print("  5. Implement WITH clause for query composition")
    print("  6. Implement multiple MATCH clauses")
    print()
    print("Once implemented, the Cypher engine should match or exceed")
    print("the manual Python query performance (especially for complex queries)")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
