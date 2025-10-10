"""
Graph Query Engine Demo

Demonstrates the new GraphQueryEngine API for loading data into Sabot's
state store and querying it with SPARQL/Cypher.

This example shows:
1. Creating a GraphQueryEngine with state persistence
2. Loading vertices and edges from Arrow tables
3. Querying the graph (Cypher/SPARQL stubs for Phase 4.2-4.3)
4. Using pattern matching directly until compilers are implemented
5. Persisting and loading graph state

Status:
- Phase 4.1: ✅ Core API complete (this demo works)
- Phase 4.2: 🚧 SPARQL compiler (coming soon)
- Phase 4.3: 🚧 Cypher compiler (coming soon)
- Phase 4.6: 🚧 Continuous queries (coming soon)

Usage:
    python examples/graph_query_engine_demo.py
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set library path for Arrow
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib'

import pyarrow as pa
from sabot._cython.graph.engine import GraphQueryEngine

# For pattern matching until Cypher/SPARQL compilers are ready
from sabot._cython.graph.query import match_2hop, match_3hop


def create_social_network():
    """
    Create a simple social network graph for demonstration.

    Graph:
        Alice (0) --KNOWS--> Bob (1) --KNOWS--> Charlie (2)
              |                                       ^
              +---------------KNOWS-------------------+

    Returns:
        Tuple of (vertices, edges) as Arrow tables
    """
    # Create vertices
    vertices = pa.table({
        'id': pa.array([0, 1, 2], type=pa.int64()),
        'label': pa.array(['Person', 'Person', 'Person'], type=pa.string()),
        'name': pa.array(['Alice', 'Bob', 'Charlie'], type=pa.string()),
        'age': pa.array([25, 30, 28], type=pa.int64())
    })

    # Create edges
    edges = pa.table({
        'source': pa.array([0, 1, 0], type=pa.int64()),
        'target': pa.array([1, 2, 2], type=pa.int64()),
        'label': pa.array(['KNOWS', 'KNOWS', 'KNOWS'], type=pa.string()),
        'since': pa.array([2020, 2021, 2022], type=pa.int64())
    })

    return vertices, edges


def demo_basic_api():
    """
    Demonstrate basic GraphQueryEngine API.

    Shows:
    - Creating engine with state store
    - Loading vertices and edges
    - Getting graph statistics
    - Persisting and loading state
    """
    print("=" * 70)
    print("GRAPH QUERY ENGINE DEMO - Phase 4.1")
    print("=" * 70)
    print()

    # Step 1: Create engine with in-memory state (None = in-memory)
    print("Step 1: Create GraphQueryEngine with in-memory state")
    print("-" * 70)
    engine = GraphQueryEngine(
        state_store=None,  # None = in-memory backend
        num_vertices_hint=100,
        num_edges_hint=100,
        enable_continuous=True
    )
    print("✅ Engine created")
    print()

    # Step 2: Load data
    print("Step 2: Load vertices and edges")
    print("-" * 70)
    vertices, edges = create_social_network()

    print(f"Loading {len(vertices)} vertices:")
    print(vertices.to_pandas())
    print()

    print(f"Loading {len(edges)} edges:")
    print(edges.to_pandas())
    print()

    engine.load_vertices(vertices, persist=True)
    engine.load_edges(edges, persist=True)
    print("✅ Data loaded and persisted")
    print()

    # Step 3: Get graph statistics
    print("Step 3: Get graph statistics")
    print("-" * 70)
    stats = engine.get_graph_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    print()

    # Step 4: Try Cypher query (will show NotImplementedError with helpful message)
    print("Step 4: Try Cypher query (Phase 4.3 - not yet implemented)")
    print("-" * 70)
    try:
        result = engine.query_cypher(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"
        )
        print(result.to_pandas())
    except NotImplementedError as e:
        print(f"⚠️  Expected: {e}")
    print()

    # Step 5: Use pattern matching directly (this works now!)
    print("Step 5: Use pattern matching directly (works now!)")
    print("-" * 70)
    print("Executing 2-hop pattern: (a)-[:KNOWS]->(b)-[:KNOWS]->(c)")
    print()

    # Convert edges to simple format for pattern matching
    edge_table = pa.table({
        'source': edges.column('source'),
        'target': edges.column('target')
    })

    result = match_2hop(edge_table, edge_table)
    print(f"✅ Found {result.num_matches()} 2-hop paths:")
    print(result.result_table().to_pandas())
    print()

    # Step 6: Persist graph state
    print("Step 6: Persist graph state")
    print("-" * 70)
    engine.persist()
    print("✅ Graph state persisted to in-memory backend")
    print()

    # Step 7: Load graph state
    print("Step 7: Create new engine and load state")
    print("-" * 70)
    engine2 = GraphQueryEngine(
        state_store=engine.state_store,  # Reuse same backend
        enable_continuous=True
    )
    engine2.load()
    stats2 = engine2.get_graph_stats()
    print(f"✅ Loaded graph with {stats2['num_vertices']} vertices and {stats2['num_edges']} edges")
    print()

    # Step 8: Try SPARQL query (will show NotImplementedError)
    print("Step 8: Try SPARQL query (Phase 4.2 - not yet implemented)")
    print("-" * 70)
    try:
        result = engine.query_sparql("""
            SELECT ?person ?friend
            WHERE {
                ?person rdf:type :Person .
                ?person :knows ?friend .
            }
        """)
        print(result.to_pandas())
    except NotImplementedError as e:
        print(f"⚠️  Expected: {e}")
    print()

    # Step 9: Try continuous query (will show NotImplementedError)
    print("Step 9: Try continuous query (Phase 4.6 - not yet implemented)")
    print("-" * 70)
    try:
        def fraud_callback(result):
            print(f"Fraud detected: {result.to_pandas()}")

        query_id = engine.register_continuous_query(
            query="MATCH (a)-[:TRANSFER {amount: > 10000}]->(b) RETURN a, b",
            callback=fraud_callback
        )
        print(f"✅ Registered continuous query: {query_id}")
    except NotImplementedError as e:
        print(f"⚠️  Expected: {e}")
    print()

    # Cleanup
    engine.close()
    engine2.close()

    print("=" * 70)
    print("DEMO COMPLETE")
    print("=" * 70)
    print()
    print("Summary:")
    print("  ✅ Phase 4.1: Core Query Engine API - WORKING")
    print("  🚧 Phase 4.2: SPARQL Compiler - Coming in next phase")
    print("  🚧 Phase 4.3: Cypher Compiler - Coming in next phase")
    print("  🚧 Phase 4.6: Continuous Queries - Coming in next phase")
    print()
    print("For now, use pattern matching functions directly:")
    print("  - match_2hop(edges1, edges2)")
    print("  - match_3hop(edges1, edges2, edges3)")
    print("  - match_variable_length_path(edges, start, end, min_hops, max_hops)")
    print()


if __name__ == "__main__":
    demo_basic_api()
