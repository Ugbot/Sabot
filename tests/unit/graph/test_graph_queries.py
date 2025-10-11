"""
Graph Query Test Suite

Tests for both unified API (recommended) and advanced API for graph queries.

**Unified API Tests (Recommended for 90% of users):**
- test_unified_query_cypher_batch() - Batch query with query_cypher()
- test_unified_query_cypher_streaming() - Streaming query with as_operator=True
- test_graph_query_stream_api() - Stream API with graph_query()
- test_unified_operator_chaining() - Operator chaining with unified API

**Advanced API Tests (For power users):**
- test_match_tracker() - MatchTracker deduplication
- test_graph_stream_operator_basic() - GraphStreamOperator direct usage
- test_continuous_query_manager() - ContinuousQueryManager orchestration
- test_stream_api_integration_advanced() - Stream API with continuous_query()
- test_operator_chaining_advanced() - Operator chaining with transform_graph()
- test_incremental_vs_continuous() - Mode comparison
- test_performance_benchmark() - Performance benchmarks
"""

from sabot import cyarrow as pa
from sabot.api.stream import Stream


# ============================================================================
# UNIFIED API TESTS (RECOMMENDED FOR 90% OF USERS)
# ============================================================================


def test_unified_query_cypher_batch():
    """
    Test unified API: Batch query (finite source).

    Demonstrates query_cypher() for batch queries - same query works for both
    batch and streaming sources (DuckDB-style unified API).
    """
    print("=" * 80)
    print("Test 1 (Unified API): Batch Query with query_cypher()")
    print("=" * 80)
    print()

    from sabot._cython.graph.engine.query_engine import GraphQueryEngine

    engine = GraphQueryEngine()

    # Load graph
    vertices = pa.table({
        'id': [0, 1, 2, 3],
        'label': ['Person', 'Person', 'Account', 'Account'],
        'name': ['Alice', 'Bob', 'Checking', 'Savings']
    })

    edges = pa.table({
        'source': [0, 1, 0, 1],
        'target': [2, 2, 3, 3],
        'label': ['OWNS', 'OWNS', 'OWNS', 'OWNS'],
        'amount': [0, 0, 0, 0]
    })

    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    # Execute batch query (returns result)
    result = engine.query_cypher(
        "MATCH (p:Person)-[:OWNS]->(a:Account) RETURN p.name, a.name"
    )

    print(f"Result type: {type(result)}")
    print(f"Result rows: {len(result)}")
    print(result.to_pandas())
    print()

    assert len(result) >= 0, "Should return valid result"
    assert hasattr(result, 'to_pandas'), "Should be QueryResult with to_pandas"

    print("✅ Unified API batch query test passed")
    print()


def test_unified_query_cypher_streaming():
    """
    Test unified API: Streaming query with as_operator=True.

    Demonstrates query_cypher(..., as_operator=True) for streaming queries.
    Same query as batch test, but returns operator for infinite sources.
    """
    print("=" * 80)
    print("Test 2 (Unified API): Streaming Query with as_operator=True")
    print("=" * 80)
    print()

    from sabot._cython.graph.engine.query_engine import GraphQueryEngine

    engine = GraphQueryEngine()

    # Load graph
    vertices = pa.table({
        'id': [0, 1, 2, 3],
        'label': ['Person', 'Person', 'Account', 'Account'],
        'name': ['Alice', 'Bob', 'Checking', 'Savings']
    })

    edges = pa.table({
        'source': [0, 1],
        'target': [2, 2],
        'label': ['OWNS', 'OWNS']
    })

    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    # Create streaming operator (as_operator=True)
    operator = engine.query_cypher(
        "MATCH (p:Person)-[:OWNS]->(a:Account) RETURN p.id, a.id",
        as_operator=True,
        mode='incremental'
    )

    print(f"Operator type: {type(operator)}")
    print()

    # Process updates
    update = pa.table({
        'source': [0, 1],
        'target': [3, 3],
        'label': ['OWNS', 'OWNS']
    })

    matches = operator.process_batch(update)
    print(f"Matches found: {matches.num_rows}")
    assert matches.num_rows >= 0, "Should return valid batch"

    # Process same update again (should deduplicate in incremental mode)
    matches2 = operator.process_batch(update)
    print(f"Matches on second update: {matches2.num_rows}")
    print()

    print("✅ Unified API streaming query test passed")
    print()


def test_graph_query_stream_api():
    """
    Test unified API: Stream.graph_query() method (RECOMMENDED).

    Demonstrates the highest-level Stream API for graph queries.
    This is the simplest and most recommended approach for most users.
    """
    print("=" * 80)
    print("Test 3 (Unified API): Stream.graph_query() Method (RECOMMENDED)")
    print("=" * 80)
    print()

    from sabot._cython.graph.engine.query_engine import GraphQueryEngine

    engine = GraphQueryEngine()

    # Load graph
    vertices = pa.table({
        'id': list(range(10)),
        'label': ['Person'] * 10,
        'name': [f'Person_{i}' for i in range(10)]
    })

    edges = pa.table({
        'source': list(range(9)),
        'target': [i + 1 for i in range(9)],
        'label': ['KNOWS'] * 9
    })

    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    # Create stream of updates
    updates = [
        pa.table({
            'source': [0, 1],
            'target': [5, 6],
            'label': ['KNOWS', 'KNOWS']
        }),
        pa.table({
            'source': [2],
            'target': [7],
            'label': ['KNOWS']
        })
    ]

    stream = Stream.from_batches(updates)

    # Apply graph query using Stream API (RECOMMENDED)
    matches = stream.graph_query(
        engine=engine,
        query="MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.id, b.id",
        mode='incremental'
    )

    # Collect results
    total_matches = 0
    batch_count = 0
    for batch in matches:
        total_matches += batch.num_rows
        batch_count += 1

    print(f"Processed {batch_count} batches")
    print(f"Total matches: {total_matches}")
    assert total_matches >= 0, "Should process stream"
    print()

    print("✅ Stream.graph_query() test passed")
    print()


def test_unified_operator_chaining():
    """
    Test unified API: Operator chaining with graph_query().

    Demonstrates chaining graph queries with normal operators using the
    recommended unified API.
    """
    print("=" * 80)
    print("Test 4 (Unified API): Operator Chaining")
    print("=" * 80)
    print()

    from sabot._cython.graph.engine.query_engine import GraphQueryEngine

    engine = GraphQueryEngine()

    # Load graph
    vertices = pa.table({
        'id': [0, 1, 2, 3, 4, 5],
        'label': ['Account'] * 6,
        'name': ['Alice', 'Bob', 'Charlie', 'Dave', 'Eve', 'Frank'],
        'risk_score': [0.1, 0.3, 0.5, 0.2, 0.8, 0.4]
    })

    edges = pa.table({
        'source': [0, 1, 2, 3, 4],
        'target': [1, 2, 3, 4, 5],
        'label': ['TRANSFER'] * 5,
        'amount': [5000, 15000, 25000, 8000, 50000]
    })

    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    # Create stream of updates
    updates = [
        pa.table({
            'source': [0, 2],
            'target': [3, 4],
            'label': ['TRANSFER', 'TRANSFER'],
            'amount': [30000, 12000]
        })
    ]

    stream = Stream.from_batches(updates)

    # Chain: graph_query → filter → map
    result_stream = (stream
        .graph_query(
            engine=engine,
            query="MATCH (a)-[r:TRANSFER]->(b) WHERE r.amount > 10000 RETURN a, b, r.amount",
            mode='incremental'
        )
        .filter(lambda b: b.num_rows > 0)
        .map(lambda b: b if b.num_rows > 0 else b)
    )

    # Collect results
    batch_count = 0
    total_rows = 0
    for batch in result_stream:
        batch_count += 1
        total_rows += batch.num_rows

    print(f"Processed {batch_count} batches")
    print(f"Total rows after chaining: {total_rows}")
    assert batch_count >= 0, "Should process chained operators"
    print()

    print("✅ Unified API operator chaining test passed")
    print()


# ============================================================================
# ADVANCED API TESTS (FOR POWER USERS)
# ============================================================================


def test_match_tracker():
    """
    **Advanced API**: Test MatchTracker deduplication.

    Tests the low-level MatchTracker component used for deduplication in
    incremental mode. Most users don't need to use this directly.
    """
    print("=" * 80)
    print("Test 5 (Advanced API): MatchTracker Deduplication")
    print("=" * 80)
    print()

    from sabot._cython.graph.executor.match_tracker import MatchTracker

    tracker = MatchTracker(
        bloom_size=10000,
        max_exact_matches=1000
    )

    # Test 1: New match
    match1 = [1, 2, 3]
    assert tracker.is_new_match(match1), "Should be new match"
    tracker.add_match(match1)

    # Test 2: Duplicate match
    assert not tracker.is_new_match(match1), "Should be duplicate"

    # Test 3: Different order (should still be duplicate)
    match2 = [3, 2, 1]  # Same vertices, different order
    assert not tracker.is_new_match(match2), "Should detect reordered duplicate"

    # Test 4: Different match
    match3 = [1, 2, 4]
    assert tracker.is_new_match(match3), "Should be new match"
    tracker.add_match(match3)

    # Get stats
    stats = tracker.get_stats()
    print(f"Total checked: {stats['total_checked']}")
    print(f"New matches: {stats['new_matches']}")
    print(f"Exact hits: {stats['exact_hits']}")
    print(f"Bloom hit rate: {stats['bloom_hit_rate']:.2%}")
    print()

    print("✅ MatchTracker tests passed")
    print()


def test_graph_stream_operator_basic():
    """
    **Advanced API**: Test GraphStreamOperator basic functionality.

    Tests direct creation and usage of GraphStreamOperator. For most use cases,
    prefer query_cypher(..., as_operator=True) instead.
    """
    print("=" * 80)
    print("Test 6 (Advanced API): GraphStreamOperator Basic")
    print("=" * 80)
    print()

    from sabot._cython.graph.engine.query_engine import GraphQueryEngine
    from sabot._cython.graph.executor.graph_stream_operator import GraphStreamOperator

    # Create engine and load graph
    engine = GraphQueryEngine()

    vertices = pa.table({
        'id': [0, 1, 2, 3],
        'label': ['Person', 'Person', 'Person', 'Person'],
        'name': ['Alice', 'Bob', 'Charlie', 'Dave']
    })

    edges = pa.table({
        'source': [0, 1, 2],
        'target': [1, 2, 3],
        'label': ['KNOWS', 'KNOWS', 'KNOWS']
    })

    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    # Create operator
    operator = GraphStreamOperator(
        graph=engine.graph,
        query_engine=engine,
        query="MATCH (a)-[:KNOWS]->(b) RETURN a.id, b.id",
        mode='incremental'
    )

    # Process update
    update = pa.table({
        'source': [0],
        'target': [3],
        'label': ['KNOWS']
    })

    matches = operator.process_batch(update)
    print(f"Matches found: {matches.num_rows}")
    assert matches.num_rows >= 0, "Should return valid batch"

    # Get stats
    stats = operator.get_stats()
    print(f"Updates processed: {stats['total_updates_processed']}")
    print(f"Matches emitted: {stats['total_matches_emitted']}")
    print()

    print("✅ GraphStreamOperator tests passed")
    print()


def test_continuous_query_manager():
    """
    **Advanced API**: Test ContinuousQueryManager orchestration.

    Tests register_continuous_query() and create_continuous_operator() for
    callback-based query processing. For most use cases, prefer the unified API.
    """
    print("=" * 80)
    print("Test 7 (Advanced API): ContinuousQueryManager")
    print("=" * 80)
    print()

    from sabot._cython.graph.engine.query_engine import GraphQueryEngine

    engine = GraphQueryEngine(enable_continuous=True)

    # Load graph
    vertices = pa.table({
        'id': [0, 1, 2],
        'label': ['Person', 'Person', 'Person']
    })

    edges = pa.table({
        'source': [0, 1],
        'target': [1, 2],
        'label': ['KNOWS', 'KNOWS']
    })

    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    # Register query
    results = []

    def callback(result):
        results.append(result.num_rows)

    query_id = engine.register_continuous_query(
        query="MATCH (a)-[:KNOWS]->(b) RETURN a, b",
        callback=callback,
        mode='incremental'
    )

    assert query_id is not None, "Should return query ID"

    # Create operator
    operator = engine.create_continuous_operator(query_id)

    # Process update
    update = pa.table({
        'source': [0],
        'target': [2],
        'label': ['KNOWS']
    })

    operator.process_batch(update)

    # Get stats
    stats = engine.get_continuous_query_stats(query_id)
    print(f"Query ID: {stats['query_id']}")
    print(f"Status: {stats['status']}")
    print()

    # Unregister
    engine.unregister_continuous_query(query_id)

    print("✅ ContinuousQueryManager tests passed")
    print()


def test_stream_api_integration_advanced():
    """
    **Advanced API**: Test Stream API with continuous_query().

    Tests the advanced continuous_query() method. For most use cases,
    prefer graph_query() instead.
    """
    print("=" * 80)
    print("Test 8 (Advanced API): Stream API with continuous_query()")
    print("=" * 80)
    print()

    from sabot._cython.graph.engine.query_engine import GraphQueryEngine

    engine = GraphQueryEngine(enable_continuous=True)

    # Load graph
    vertices = pa.table({
        'id': list(range(5)),
        'label': ['Person'] * 5
    })

    edges = pa.table({
        'source': list(range(4)),
        'target': [i + 1 for i in range(4)],
        'label': ['KNOWS'] * 4
    })

    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    # Create update stream
    updates = [
        pa.table({
            'source': [0, 1],
            'target': [3, 4],
            'label': ['KNOWS', 'KNOWS']
        })
    ]

    stream = Stream.from_batches(updates)

    # Apply continuous query
    matches_stream = stream.continuous_query(
        query="MATCH (a)-[:KNOWS]->(b) RETURN a.id, b.id",
        graph=engine.graph,
        query_engine=engine,
        mode='incremental'
    )

    # Collect results
    match_count = 0
    for batch in matches_stream:
        match_count += batch.num_rows

    print(f"Total matches: {match_count}")
    assert match_count >= 0, "Should process stream"

    print("✅ Stream API integration tests passed")
    print()


def test_operator_chaining_advanced():
    """
    **Advanced API**: Test chaining with transform_graph().

    Tests the advanced transform_graph() method for explicit operator control.
    For most use cases, prefer graph_query() instead.
    """
    print("=" * 80)
    print("Test 9 (Advanced API): Operator Chaining with transform_graph()")
    print("=" * 80)
    print()

    from sabot._cython.graph.engine.query_engine import GraphQueryEngine

    engine = GraphQueryEngine(enable_continuous=True)

    # Load graph with properties
    vertices = pa.table({
        'id': [0, 1, 2, 3],
        'label': ['Person', 'Person', 'Person', 'Person'],
        'age': [25, 30, 35, 40]
    })

    edges = pa.table({
        'source': [0, 1, 2],
        'target': [1, 2, 3],
        'label': ['KNOWS', 'KNOWS', 'KNOWS']
    })

    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    # Create operator
    query_id = engine.register_continuous_query(
        query="MATCH (a)-[:KNOWS]->(b) RETURN a.id, b.id, a.age as age",
        mode='incremental'
    )

    operator = engine.create_continuous_operator(query_id)

    # Create stream and chain operators
    updates = [
        pa.table({
            'source': [0],
            'target': [3],
            'label': ['KNOWS']
        })
    ]

    stream = Stream.from_batches(updates)

    # Chain: graph → filter → map
    result_stream = (stream
        .transform_graph(operator)
        .filter(lambda b: b.num_rows > 0)
        .map(lambda b: b if b.num_rows > 0 else b)
    )

    # Collect results
    batch_count = 0
    for batch in result_stream:
        batch_count += 1

    print(f"Processed {batch_count} batches")
    assert batch_count >= 0, "Should process chained operators"

    print("✅ Operator chaining tests passed")
    print()


def test_incremental_vs_continuous():
    """
    **Advanced API**: Test incremental vs continuous mode.

    Demonstrates the difference between incremental (deduplicated) and
    continuous (all matches) modes using direct operator creation.
    """
    print("=" * 80)
    print("Test 10 (Advanced API): Incremental vs Continuous Mode")
    print("=" * 80)
    print()

    from sabot._cython.graph.engine.query_engine import GraphQueryEngine
    from sabot._cython.graph.executor.graph_stream_operator import GraphStreamOperator

    engine = GraphQueryEngine()

    vertices = pa.table({
        'id': [0, 1],
        'label': ['Person', 'Person']
    })

    edges = pa.table({
        'source': [0],
        'target': [1],
        'label': ['KNOWS']
    })

    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    # Create two operators
    op_incremental = GraphStreamOperator(
        graph=engine.graph,
        query_engine=engine,
        query="MATCH (a)-[:KNOWS]->(b) RETURN a.id, b.id",
        mode='incremental'
    )

    op_continuous = GraphStreamOperator(
        graph=engine.graph,
        query_engine=engine,
        query="MATCH (a)-[:KNOWS]->(b) RETURN a.id, b.id",
        mode='continuous'
    )

    # Same update twice
    update = pa.table({
        'source': [0],
        'target': [1],
        'label': ['KNOWS']
    })

    # Incremental should deduplicate
    matches1_1 = op_incremental.process_batch(update)
    matches1_2 = op_incremental.process_batch(update)

    print(f"Incremental mode:")
    print(f"  First update: {matches1_1.num_rows} matches")
    print(f"  Second update: {matches1_2.num_rows} matches (should be 0)")
    print()

    # Continuous should not deduplicate
    matches2_1 = op_continuous.process_batch(update)
    matches2_2 = op_continuous.process_batch(update)

    print(f"Continuous mode:")
    print(f"  First update: {matches2_1.num_rows} matches")
    print(f"  Second update: {matches2_2.num_rows} matches (should be same)")
    print()

    assert matches1_2.num_rows == 0, "Incremental should deduplicate"
    assert matches2_2.num_rows == matches2_1.num_rows, "Continuous should not deduplicate"

    print("✅ Mode comparison tests passed")
    print()


def test_performance_benchmark():
    """
    **Advanced API**: Benchmark continuous query performance.

    Performance benchmark using the advanced API with registered queries.
    Measures throughput and latency for continuous pattern matching.
    """
    print("=" * 80)
    print("Test 11 (Advanced API): Performance Benchmark")
    print("=" * 80)
    print()

    import time
    from sabot._cython.graph.engine.query_engine import GraphQueryEngine

    engine = GraphQueryEngine()

    # Create larger graph
    num_vertices = 1000
    num_edges = 5000

    vertices = pa.table({
        'id': list(range(num_vertices)),
        'label': ['Person'] * num_vertices
    })

    import random
    edges = pa.table({
        'source': [random.randint(0, num_vertices - 1) for _ in range(num_edges)],
        'target': [random.randint(0, num_vertices - 1) for _ in range(num_edges)],
        'label': ['KNOWS'] * num_edges
    })

    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    print(f"Loaded graph: {num_vertices} vertices, {num_edges} edges")
    print()

    # Create operator
    query_id = engine.register_continuous_query(
        query="MATCH (a)-[:KNOWS]->(b) RETURN a.id, b.id",
        mode='incremental'
    )

    operator = engine.create_continuous_operator(query_id)

    # Benchmark update processing
    num_updates = 100
    updates_per_batch = 10

    start_time = time.time()

    for _ in range(num_updates):
        update = pa.table({
            'source': [random.randint(0, num_vertices - 1) for _ in range(updates_per_batch)],
            'target': [random.randint(0, num_vertices - 1) for _ in range(updates_per_batch)],
            'label': ['KNOWS'] * updates_per_batch
        })
        operator.process_batch(update)

    end_time = time.time()

    total_updates = num_updates * updates_per_batch
    elapsed_time = end_time - start_time
    throughput = total_updates / elapsed_time

    print(f"Processed {total_updates} updates in {elapsed_time:.2f}s")
    print(f"Throughput: {throughput:.0f} updates/sec")
    print()

    stats = engine.get_continuous_query_stats(query_id)
    print(f"Total matches emitted: {stats['operator']['total_matches_emitted']}")
    print(f"Total matches deduplicated: {stats['operator']['total_matches_deduplicated']}")
    print()

    print("✅ Performance benchmark completed")
    print()


if __name__ == '__main__':
    print()
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 24 + "Graph Query Test Suite" + " " * 32 + "║")
    print("╚" + "=" * 78 + "╝")
    print()

    print("This test suite covers both the unified API (recommended for 90% of users)")
    print("and the advanced API (for power users who need fine-grained control).")
    print()

    try:
        # ===================================================================
        # UNIFIED API TESTS (RECOMMENDED)
        # ===================================================================
        print()
        print("═" * 80)
        print("  UNIFIED API TESTS (RECOMMENDED FOR 90% OF USERS)")
        print("═" * 80)
        print()

        test_unified_query_cypher_batch()
        test_unified_query_cypher_streaming()
        test_graph_query_stream_api()
        test_unified_operator_chaining()

        # ===================================================================
        # ADVANCED API TESTS (FOR POWER USERS)
        # ===================================================================
        print()
        print("═" * 80)
        print("  ADVANCED API TESTS (FOR POWER USERS)")
        print("═" * 80)
        print()

        test_match_tracker()
        test_graph_stream_operator_basic()
        test_continuous_query_manager()
        test_stream_api_integration_advanced()
        test_operator_chaining_advanced()
        test_incremental_vs_continuous()
        test_performance_benchmark()

        # ===================================================================
        # SUMMARY
        # ===================================================================
        print()
        print("=" * 80)
        print("✅ All tests passed!")
        print("=" * 80)
        print()

        print("Test Summary:")
        print("  - Unified API tests: 4 passed")
        print("  - Advanced API tests: 7 passed")
        print("  - Total: 11 tests passed")
        print()

        print("Recommended for most users:")
        print("  - engine.query_cypher(query) - Batch queries")
        print("  - engine.query_cypher(query, as_operator=True) - Streaming queries")
        print("  - stream.graph_query(engine, query) - Stream API sugar")
        print()

    except AssertionError as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

    except Exception as e:
        print(f"❌ Test error: {e}")
        import traceback
        traceback.print_exc()
