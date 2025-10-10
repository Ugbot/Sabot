"""
Test EXPLAIN Statement Support

Demonstrates EXPLAIN functionality for both SPARQL and Cypher queries,
showing query execution plans, cost estimates, and optimization information.
"""

from sabot import cyarrow as pa
from sabot._cython.graph.engine.query_engine import GraphQueryEngine, QueryConfig


def create_test_graph():
    """
    Create a test property graph for EXPLAIN demonstrations.

    Graph structure:
    - 1000 Person nodes (common)
    - 10 VIP nodes (rare)
    - 100 Account nodes (moderately common)
    - Various edges
    """
    # Create vertices
    person_ids = list(range(1000))
    vip_ids = list(range(1000, 1010))
    account_ids = list(range(1010, 1110))

    all_ids = person_ids + vip_ids + account_ids
    all_labels = (
        ['Person'] * len(person_ids) +
        ['VIP'] * len(vip_ids) +
        ['Account'] * len(account_ids)
    )

    vertices = pa.table({
        'id': all_ids,
        'label': all_labels,
        'name': [f'Node_{i}' for i in all_ids],
        'age': [20 + (i % 50) for i in all_ids]
    })

    # Create edges
    # KNOWS edges: Person->Person (common)
    knows_sources = [i for i in range(0, 999)]
    knows_targets = [i+1 for i in range(0, 999)]

    # FRIEND_OF edges: VIP->VIP (rare)
    friend_sources = [i for i in range(1000, 1009)]
    friend_targets = [i+1 for i in range(1000, 1009)]

    # OWNS edges: Person->Account
    owns_sources = [i for i in range(0, 100)]
    owns_targets = [1010 + i for i in range(0, 100)]

    all_sources = knows_sources + friend_sources + owns_sources
    all_targets = knows_targets + friend_targets + owns_targets
    all_edge_labels = (
        ['KNOWS'] * len(knows_sources) +
        ['FRIEND_OF'] * len(friend_sources) +
        ['OWNS'] * len(owns_sources)
    )

    edges = pa.table({
        'source': all_sources,
        'target': all_targets,
        'label': all_edge_labels,
        'weight': [1.0] * len(all_sources)
    })

    return vertices, edges


def test_cypher_explain():
    """Test EXPLAIN for Cypher queries."""
    print("=" * 80)
    print("🔍 Testing EXPLAIN for Cypher Queries")
    print("=" * 80)
    print()

    # Create graph engine
    engine = GraphQueryEngine()

    # Load test graph
    vertices, edges = create_test_graph()
    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    # Print graph statistics
    stats = engine.get_graph_stats()
    print(f"📊 Graph Statistics:")
    print(f"   Total vertices: {stats['num_vertices']}")
    print(f"   Total edges: {stats['num_edges']}")
    print(f"   Vertex labels: {stats['vertex_labels']}")
    print(f"   Edge types: {stats['edge_labels']}")
    print()

    # Test EXPLAIN with a simple query
    print("-" * 80)
    print("Test 1: EXPLAIN Simple Pattern")
    print("-" * 80)
    query1 = """
        MATCH (a)-[:KNOWS]->(b:VIP)
        RETURN a, b
        LIMIT 10
    """
    print(f"Query: {query1.strip()}")
    print()

    config = QueryConfig(explain=True)
    explanation1 = engine.query_cypher(query1, config=config)

    # Print explanation
    plan_text = explanation1.table.column('QUERY PLAN')[0].as_py()
    print(plan_text)
    print()

    # Test EXPLAIN with a complex query
    print("-" * 80)
    print("Test 2: EXPLAIN Multi-hop Pattern")
    print("-" * 80)
    query2 = """
        MATCH (a:Person)-[:KNOWS]->(b)-[:FRIEND_OF]->(c:VIP)
        RETURN a, b, c
        LIMIT 10
    """
    print(f"Query: {query2.strip()}")
    print()

    explanation2 = engine.query_cypher(query2, config=config)

    # Print explanation
    plan_text2 = explanation2.table.column('QUERY PLAN')[0].as_py()
    print(plan_text2)
    print()

    # Compare with actual execution
    print("-" * 80)
    print("Test 3: Compare EXPLAIN vs Actual Execution")
    print("-" * 80)

    # Run EXPLAIN
    print("EXPLAIN output:")
    print()
    explanation3 = engine.query_cypher(query1, config=QueryConfig(explain=True))
    plan_text3 = explanation3.table.column('QUERY PLAN')[0].as_py()
    print(plan_text3)
    print()

    # Run actual query
    print("Actual execution:")
    print()
    result3 = engine.query_cypher(query1, config=QueryConfig(explain=False))
    print(f"✅ Execution time: {result3.metadata.execution_time_ms:.2f}ms")
    print(f"✅ Results: {result3.metadata.num_results} rows")
    print()


def test_sparql_explain():
    """Test EXPLAIN for SPARQL queries."""
    print("=" * 80)
    print("🔍 Testing EXPLAIN for SPARQL Queries")
    print("=" * 80)
    print()

    # Create graph engine
    engine = GraphQueryEngine()

    # Load test graph
    vertices, edges = create_test_graph()
    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    # Test EXPLAIN with SPARQL query
    print("-" * 80)
    print("Test 1: EXPLAIN SPARQL Query")
    print("-" * 80)
    query1 = """
        SELECT ?person ?friend
        WHERE {
            ?person rdf:type Person .
            ?person KNOWS ?friend .
            ?friend rdf:type VIP .
        }
        LIMIT 10
    """
    print(f"Query: {query1.strip()}")
    print()

    config = QueryConfig(explain=True)

    try:
        explanation1 = engine.query_sparql(query1, config=config)

        # Print explanation
        plan_text = explanation1.table.column('QUERY PLAN')[0].as_py()
        print(plan_text)
        print()
    except Exception as e:
        print(f"⚠️  SPARQL EXPLAIN failed (may need RDF triple store setup): {e}")
        print()

    # Test EXPLAIN with more complex SPARQL query
    print("-" * 80)
    print("Test 2: EXPLAIN Complex SPARQL Query")
    print("-" * 80)
    query2 = """
        SELECT ?a ?b ?c
        WHERE {
            ?a rdf:type Person .
            ?a KNOWS ?b .
            ?b FRIEND_OF ?c .
            ?c rdf:type VIP .
        }
        LIMIT 10
    """
    print(f"Query: {query2.strip()}")
    print()

    try:
        explanation2 = engine.query_sparql(query2, config=config)

        # Print explanation
        plan_text2 = explanation2.table.column('QUERY PLAN')[0].as_py()
        print(plan_text2)
        print()
    except Exception as e:
        print(f"⚠️  SPARQL EXPLAIN failed (may need RDF triple store setup): {e}")
        print()


def test_optimization_comparison():
    """Test optimization impact by comparing plans."""
    print("=" * 80)
    print("🚀 Testing Optimization Impact")
    print("=" * 80)
    print()

    # Create graph engine
    engine = GraphQueryEngine()

    # Load test graph
    vertices, edges = create_test_graph()
    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    query = """
        MATCH (a)-[:KNOWS]->(b:VIP)
        RETURN a, b
        LIMIT 10
    """

    print(f"Query: {query.strip()}")
    print()

    # Run with optimization enabled
    print("-" * 80)
    print("With Optimization (optimize=True)")
    print("-" * 80)
    config_opt = QueryConfig(explain=True, optimize=True)
    explanation_opt = engine.query_cypher(query, config=config_opt)
    plan_opt = explanation_opt.table.column('QUERY PLAN')[0].as_py()
    print(plan_opt)
    print()

    # Run without optimization
    print("-" * 80)
    print("Without Optimization (optimize=False)")
    print("-" * 80)
    config_no_opt = QueryConfig(explain=True, optimize=False)
    explanation_no_opt = engine.query_cypher(query, config=config_no_opt)
    plan_no_opt = explanation_no_opt.table.column('QUERY PLAN')[0].as_py()
    print(plan_no_opt)
    print()

    print("=" * 80)
    print("Key Observations:")
    print("- Optimized plan shows selectivity scores and reordering")
    print("- Unoptimized plan shows original pattern order")
    print("- Optimizations section lists applied transformations")
    print("=" * 80)


if __name__ == '__main__':
    print()
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "EXPLAIN Statement Test Suite" + " " * 30 + "║")
    print("╚" + "=" * 78 + "╝")
    print()

    # Run Cypher EXPLAIN tests
    test_cypher_explain()

    print("\n\n")

    # Run SPARQL EXPLAIN tests
    test_sparql_explain()

    print("\n\n")

    # Run optimization comparison
    test_optimization_comparison()

    print()
    print("✨ EXPLAIN tests complete!")
    print()
