#!/usr/bin/env python3
"""
Sabot Real Cypher Benchmark - Using GraphQueryEngine

Tests the REAL working Cypher features against Kuzu benchmark queries.

Working Features (as of Oct 22, 2025):
- ✅ Node label filtering: (a:Person)
- ✅ Edge type filtering: -[:Follows]->
- ✅ Property access in RETURN: a.name, b.age
- ✅ 2-hop patterns: (a)-[r1]->(b)-[r2]->(c)
- ✅ LIMIT clause
- ✅ Variable naming

Not Yet Implemented:
- ❌ Aggregations (COUNT, SUM, AVG)
- ❌ WHERE clauses with property comparisons
- ❌ ORDER BY
- ❌ Variable-length paths (kernel exists, translator needs work)

Usage:
    python run_real_cypher_benchmark.py [--iterations N] [--warmup N]
"""
import sys
import os
import time
import argparse
from pathlib import Path

# Add Sabot to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

# Set library path for Arrow
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib'

from data_loader import load_all_data
from sabot._cython.graph.engine.query_engine import GraphQueryEngine
from sabot import cyarrow as pa


class CypherBenchmarkRunner:
    """Run benchmark using real Cypher query engine."""

    def __init__(self, data_dir: Path):
        """Initialize with data directory."""
        self.data_dir = data_dir
        self.engine = None
        self.data = None

    def load_data(self):
        """Load all graph data into GraphQueryEngine."""
        print("="*70)
        print("LOADING DATA INTO GRAPHQUERYENGINE")
        print("="*70)

        # Load raw data
        start = time.perf_counter()
        self.data = load_all_data(self.data_dir)
        elapsed = time.perf_counter() - start
        print(f"✅ Data loaded from Parquet in {elapsed:.3f}s")

        # Create graph engine
        self.engine = GraphQueryEngine()

        # Load vertices (combine all node types)
        print("\nLoading vertices...")
        all_vertices = []

        # Persons
        persons = self.data['person_nodes']
        person_vertices = pa.table({
            'id': persons.column('id'),
            'label': pa.array(['Person'] * persons.num_rows),
            'name': persons.column('name'),
            'age': persons.column('age'),
            'is_married': persons.column('isMarried'),
            'gender': persons.column('gender')
        })
        all_vertices.append(person_vertices)
        print(f"  - {persons.num_rows:,} Person nodes")

        # Cities
        cities = self.data['city_nodes']
        # Add missing columns to match schema
        city_vertices = pa.table({
            'id': cities.column('id'),
            'label': pa.array(['City'] * cities.num_rows),
            'name': cities.column('city'),  # Use 'city' as name
            'city': cities.column('city'),
            'state': cities.column('state'),
            'country': cities.column('country'),
            'lat': cities.column('lat'),
            'lon': cities.column('lon')
        })
        all_vertices.append(city_vertices)
        print(f"  - {cities.num_rows:,} City nodes")

        # Combine all vertices
        vertices = pa.concat_tables(all_vertices, promote=True)
        self.engine.load_vertices(vertices, persist=False)
        print(f"✅ Total vertices loaded: {vertices.num_rows:,}")

        # Load edges
        print("\nLoading edges...")
        all_edges = []

        # Follows edges
        follows = self.data['follows_edges']
        follows_edges = pa.table({
            'source': follows.column('source'),
            'target': follows.column('target'),
            'label': pa.array(['Follows'] * follows.num_rows)
        })
        all_edges.append(follows_edges)
        print(f"  - {follows.num_rows:,} Follows edges")

        # LivesIn edges
        lives_in = self.data['lives_in_edges']
        lives_in_edges = pa.table({
            'source': lives_in.column('source'),
            'target': lives_in.column('target'),
            'label': pa.array(['LivesIn'] * lives_in.num_rows)
        })
        all_edges.append(lives_in_edges)
        print(f"  - {lives_in.num_rows:,} LivesIn edges")

        # Combine all edges
        edges = pa.concat_tables(all_edges)
        self.engine.load_edges(edges, persist=False)
        print(f"✅ Total edges loaded: {edges.num_rows:,}\n")

    def run_query(self, query_name: str, cypher_query: str, iterations: int = 5, warmup: int = 2):
        """
        Run a Cypher query with warmup and timing.

        Args:
            query_name: Name of the query
            cypher_query: Cypher query string
            iterations: Number of timed iterations
            warmup: Number of warmup iterations

        Returns:
            Tuple of (result, avg_time_ms, min_time_ms, max_time_ms)
        """
        print(f"\n{query_name}")
        print("-" * 70)
        print(f"Cypher: {cypher_query.strip()}")

        # Warmup
        for _ in range(warmup):
            try:
                self.engine.query_cypher(cypher_query)
            except Exception as e:
                print(f"❌ Query failed during warmup: {e}")
                return None, 0, 0, 0

        # Timed iterations
        times = []
        result = None
        for _ in range(iterations):
            start = time.perf_counter()
            try:
                result = self.engine.query_cypher(cypher_query)
                elapsed = (time.perf_counter() - start) * 1000  # ms
                times.append(elapsed)
            except Exception as e:
                print(f"❌ Query failed: {e}")
                return None, 0, 0, 0

        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)

        print(f"Results: {result.table.num_rows} rows")
        print(f"Time: {avg_time:.2f}ms (min: {min_time:.2f}ms, max: {max_time:.2f}ms)")

        return result, avg_time, min_time, max_time

    def run_benchmarks(self, iterations: int = 5, warmup: int = 2):
        """Run all working Cypher queries."""
        if self.engine is None:
            raise ValueError("Data not loaded. Call load_data() first.")

        results = []

        print("="*70)
        print("RUNNING CYPHER BENCHMARKS (Working Features Only)")
        print("="*70)

        # Query 1: Simple pattern matching with LIMIT
        # Note: Can't do aggregation yet, so just get sample of followers
        q1_name = "Query 1: Find follower relationships (sample)"
        q1_cypher = "MATCH (follower:Person)-[:Follows]->(person:Person) RETURN follower, person LIMIT 100"
        r1 = self.run_query(q1_name, q1_cypher, iterations, warmup)
        results.append(('Q1_sample', *r1))

        # Query 1b: With property access
        q1b_name = "Query 1b: Follower names (property access)"
        q1b_cypher = "MATCH (follower:Person)-[:Follows]->(person:Person) RETURN follower.name, person.name LIMIT 100"
        r1b = self.run_query(q1b_name, q1b_cypher, iterations, warmup)
        results.append(('Q1b_properties', *r1b))

        # Query 2: LivesIn relationships
        q2_name = "Query 2: Person lives in city"
        q2_cypher = "MATCH (p:Person)-[:LivesIn]->(c:City) RETURN p.name, c LIMIT 100"
        r2 = self.run_query(q2_name, q2_cypher, iterations, warmup)
        results.append(('Q2_lives_in', *r2))

        # Query 3: 2-hop pattern (Person follows Person who lives in City)
        q3_name = "Query 3: 2-hop - Person->Person->City"
        q3_cypher = "MATCH (a:Person)-[:Follows]->(b:Person)-[:LivesIn]->(c:City) RETURN a.name, b.name, c LIMIT 1000"
        r3 = self.run_query(q3_name, q3_cypher, iterations, warmup)
        results.append(('Q3_2hop', *r3))

        # Query 4: Multiple edge types
        q4_name = "Query 4: All person-to-person follows"
        q4_cypher = "MATCH (a:Person)-[:Follows]->(b:Person) RETURN a, b LIMIT 10000"
        r4 = self.run_query(q4_name, q4_cypher, iterations, warmup)
        results.append(('Q4_all_follows', *r4))

        # Query 5: All LivesIn edges
        q5_name = "Query 5: All person-to-city LivesIn"
        q5_cypher = "MATCH (p:Person)-[:LivesIn]->(c:City) RETURN p, c"
        r5 = self.run_query(q5_name, q5_cypher, iterations, warmup)
        results.append(('Q5_all_lives_in', *r5))

        # Query 6: 2-hop without labels (pure pattern matching)
        q6_name = "Query 6: 2-hop pattern (no labels)"
        q6_cypher = "MATCH (a)-[:Follows]->(b)-[:LivesIn]->(c) RETURN a, b, c LIMIT 1000"
        r6 = self.run_query(q6_name, q6_cypher, iterations, warmup)
        results.append(('Q6_2hop_no_labels', *r6))

        # Summary
        print("\n" + "="*70)
        print("BENCHMARK SUMMARY")
        print("="*70)

        total_time = 0
        for query_name, result, avg_time, min_time, max_time in results:
            if result is not None:
                print(f"{query_name:25} {avg_time:8.2f}ms  ({result.table.num_rows:,} rows)")
                total_time += avg_time
            else:
                print(f"{query_name:25} FAILED")

        print("-" * 70)
        print(f"{'Total Time':25} {total_time:8.2f}ms")

        print("\n" + "="*70)
        print("FEATURES TESTED")
        print("="*70)
        print("✅ Node label filtering: (a:Person), (c:City)")
        print("✅ Edge type filtering: [:Follows], [:LivesIn]")
        print("✅ Property access in RETURN: a.name, b.name")
        print("✅ 2-hop patterns: (a)-[:Follows]->(b)-[:LivesIn]->(c)")
        print("✅ LIMIT clause")
        print("✅ Mixed label and unlabeled patterns")

        print("\n" + "="*70)
        print("NOT YET IMPLEMENTED (Use manual Arrow operations for now)")
        print("="*70)
        print("❌ Aggregations (COUNT, SUM, AVG)")
        print("❌ WHERE with property comparisons (a.age > 18)")
        print("❌ ORDER BY")
        print("❌ Variable-length paths (kernel exists)")
        print("❌ Multiple MATCH clauses")
        print("❌ WITH clause")

        return results


def main():
    """Run the benchmark."""
    parser = argparse.ArgumentParser(description='Run Sabot real Cypher benchmarks')
    parser.add_argument('--iterations', type=int, default=5,
                        help='Number of benchmark iterations (default: 5)')
    parser.add_argument('--warmup', type=int, default=2,
                        help='Number of warmup iterations (default: 2)')
    parser.add_argument('--data-dir', type=Path,
                        default=Path(__file__).parent / 'reference' / 'data' / 'output',
                        help='Path to data directory')

    args = parser.parse_args()

    # Check if data exists
    if not args.data_dir.exists():
        print(f"❌ Data directory not found: {args.data_dir}")
        print("\nTo generate data:")
        print("  cd benchmarks/studies/kuzu_study/reference/data")
        print("  bash ../generate_data.sh 100000")
        return 1

    print("SABOT REAL CYPHER BENCHMARK")
    print("="*70)
    print(f"Data directory: {args.data_dir}")
    print(f"Iterations: {args.iterations}")
    print(f"Warmup: {args.warmup}")
    print()

    # Create runner
    runner = CypherBenchmarkRunner(args.data_dir)

    # Load data
    runner.load_data()

    # Run benchmarks
    runner.run_benchmarks(iterations=args.iterations, warmup=args.warmup)

    return 0


if __name__ == '__main__':
    sys.exit(main())
