"""
Graph Query Demo - Unified Batch/Streaming API

Demonstrates DuckDB-style unified API for graph queries.
Same query works on both batch (finite) and streaming (infinite) sources.

**Main API (Recommended for 90% of users):**
- engine.query_cypher(query) - Batch query
- engine.query_cypher(query, as_operator=True) - Streaming query
- updates.graph_query(engine, query) - Stream API sugar

**Advanced API (For power users):**
- engine.register_continuous_query() - Register query with callback
- engine.create_continuous_operator() - Create operator from registered query
- updates.transform_graph() - Apply operator explicitly

Example:
    python examples/graph_query_demo.py
"""

from sabot import cyarrow as pa
from sabot.api.stream import Stream


def demo_unified_batch_streaming():
    """
    Demo 1: Unified batch/streaming API (RECOMMENDED).

    Same query works on batch or streaming data!
    """
    print("=" * 80)
    print("Demo 1: Unified Batch/Streaming API (RECOMMENDED)")
    print("=" * 80)
    print()

    from sabot._cython.graph.engine.query_engine import GraphQueryEngine

    engine = GraphQueryEngine()

    # Load graph
    vertices = pa.table({
        'id': [0, 1, 2, 3, 4],
        'label': ['Person', 'Person', 'Account', 'Account', 'Person'],
        'name': ['Alice', 'Bob', 'Checking', 'Savings', 'Charlie']
    })

    edges = pa.table({
        'source': [0, 1, 0, 1, 3, 4],
        'target': [2, 2, 3, 3, 4, 2],
        'label': ['OWNS', 'OWNS', 'OWNS', 'OWNS', 'TRANSFER', 'TRANSFER'],
        'amount': [0, 0, 0, 0, 15000, 12000]
    })

    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    print(f"Loaded graph: {engine.get_graph_stats()}")
    print()

    query = "MATCH (a)-[r:TRANSFER]->(b) WHERE r.amount > 10000 RETURN a, b, r.amount as amount"

    # Batch query (finite source)
    print("Batch query (finite source):")
    result = engine.query_cypher(query)
    print(result.to_pandas())
    print()

    # Streaming query (infinite source) - SAME QUERY!
    print("Streaming query (infinite source) - SAME QUERY!")
    operator = engine.query_cypher(query, as_operator=True, mode='incremental')
    print(f"Created operator: {operator}")
    print()

    # Simulate streaming updates
    updates = [
        pa.table({
            'source': [2, 0],
            'target': [3, 1],
            'label': ['TRANSFER', 'TRANSFER'],
            'amount': [25000, 500]
        }),
        pa.table({
            'source': [4],
            'target': [5],
            'label': ['TRANSFER'],
            'amount': [50000]
        })
    ]

    print("Processing streaming updates:")
    for i, update_batch in enumerate(updates):
        matches = operator.process_batch(update_batch)
        print(f"  Update {i + 1}: {matches.num_rows} new matches")
        if matches.num_rows > 0:
            print(matches.to_pandas())
    print()


def demo_stream_api_sugar():
    """
    Demo 2: Stream API sugar (RECOMMENDED).

    Even simpler - graph_query() does everything!
    """
    print("=" * 80)
    print("Demo 2: Stream API Sugar (RECOMMENDED)")
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

    print(f"Loaded graph: {engine.get_graph_stats()}")
    print()

    # Create stream of updates
    updates_data = []
    for i in range(5):
        updates_data.append({
            'source': [i],
            'target': [i + 5],
            'label': ['KNOWS']
        })

    updates_stream = Stream.from_dicts(updates_data, batch_size=2)

    # Apply graph query using simple Stream API
    print("Applying graph query via Stream API:")
    matches = updates_stream.graph_query(
        engine=engine,
        query="MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.id, b.id",
        mode='incremental'
    )

    # Chain with normal operators!
    result_stream = (matches
        .filter(lambda b: b.num_rows > 0)
        .map(lambda b: b)
    )

    print("Processing stream:")
    for i, batch in enumerate(result_stream):
        print(f"  Batch {i + 1}: {batch.num_rows} matches")
        if batch.num_rows > 0:
            print(batch.to_pandas().head())
    print()


def demo_operator_chaining():
    """
    Demo 3: Operator chaining.

    Graph operators chain seamlessly with normal operators!
    """
    print("=" * 80)
    print("Demo 3: Operator Chaining")
    print("=" * 80)
    print()

    from sabot._cython.graph.engine.query_engine import GraphQueryEngine

    engine = GraphQueryEngine()

    # Load financial graph
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

    print(f"Loaded graph: {engine.get_graph_stats()}")
    print()

    # Create stream of updates
    updates = Stream.from_batches([
        pa.table({
            'source': [0, 2],
            'target': [3, 4],
            'label': ['TRANSFER', 'TRANSFER'],
            'amount': [30000, 12000]
        })
    ])

    # Chain: graph query → filter → map
    print("Chaining: graph_query → filter → map")
    result = (updates
        .graph_query(
            engine=engine,
            query="MATCH (a)-[r:TRANSFER]->(b) WHERE r.amount > 10000 RETURN a, b, r.amount",
            mode='incremental'
        )
        .filter(lambda b: b.num_rows > 0)
        .map(lambda b: b.append_column(
            'alert', pa.array(['HIGH_VALUE'] * b.num_rows) if b.num_rows > 0 else pa.array([])
        ))
    )

    for batch in result:
        if batch.num_rows > 0:
            print("High-value transfers:")
            print(batch.to_pandas())
    print()


def demo_advanced_api_callbacks():
    """
    Demo 4: Advanced API with callbacks (for power users).

    Use this when you need fine-grained control.
    """
    print("=" * 80)
    print("Demo 4: Advanced API with Callbacks (Power Users)")
    print("=" * 80)
    print()

    from sabot._cython.graph.engine.query_engine import GraphQueryEngine

    engine = GraphQueryEngine(enable_continuous=True)

    # Load graph
    vertices = pa.table({
        'id': [0, 1, 2],
        'label': ['Person', 'Person', 'Person'],
        'name': ['Alice', 'Bob', 'Charlie']
    })

    edges = pa.table({
        'source': [0, 1],
        'target': [1, 2],
        'label': ['KNOWS', 'KNOWS']
    })

    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    print(f"Loaded graph: {engine.get_graph_stats()}")
    print()

    # Register query with callback (Advanced API)
    def alert(result):
        print(f"  Callback triggered: {result.num_rows} matches")

    query_id = engine.register_continuous_query(
        query="MATCH (a)-[:KNOWS]->(b) RETURN a, b",
        callback=alert,
        mode='incremental'
    )

    print(f"Registered query: {query_id}")
    print()

    # Create operator (Advanced API)
    operator = engine.create_continuous_operator(query_id)

    # Process updates
    update = pa.table({
        'source': [0],
        'target': [2],
        'label': ['KNOWS']
    })

    print("Processing update:")
    matches = operator.process_batch(update)
    print(f"Returned {matches.num_rows} matches")
    print()

    # Get stats (Advanced API)
    stats = engine.get_continuous_query_stats(query_id)
    print(f"Query stats: {stats}")
    print()


def demo_incremental_vs_continuous():
    """
    Demo 5: Incremental vs continuous mode.
    """
    print("=" * 80)
    print("Demo 5: Incremental vs Continuous Mode")
    print("=" * 80)
    print()

    from sabot._cython.graph.engine.query_engine import GraphQueryEngine

    engine = GraphQueryEngine()

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

    # Create operators (both modes)
    op_incremental = engine.query_cypher(
        "MATCH (a)-[:KNOWS]->(b) RETURN a, b",
        as_operator=True,
        mode='incremental'
    )

    op_continuous = engine.query_cypher(
        "MATCH (a)-[:KNOWS]->(b) RETURN a, b",
        as_operator=True,
        mode='continuous'
    )

    # Same update twice
    update = pa.table({
        'source': [0],
        'target': [2],
        'label': ['KNOWS']
    })

    print("Processing same update twice:")
    print()

    print("Incremental mode (deduplicates):")
    for i in range(2):
        matches = op_incremental.process_batch(update)
        print(f"  Update {i + 1}: {matches.num_rows} matches")
    print()

    print("Continuous mode (all matches):")
    for i in range(2):
        matches = op_continuous.process_batch(update)
        print(f"  Update {i + 1}: {matches.num_rows} matches")
    print()


if __name__ == '__main__':
    print()
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "Graph Query Demo - Unified API" + " " * 27 + "║")
    print("╚" + "=" * 78 + "╝")
    print()

    print("This demo shows the unified batch/streaming API for graph queries.")
    print("The RECOMMENDED way (90% of users) is shown in Demos 1-3.")
    print("The ADVANCED way (10% of users) is shown in Demo 4.")
    print()

    try:
        demo_unified_batch_streaming()
        demo_stream_api_sugar()
        demo_operator_chaining()
        demo_advanced_api_callbacks()
        demo_incremental_vs_continuous()

        print("=" * 80)
        print("✅ All demos completed successfully!")
        print("=" * 80)
        print()

        print("Key Takeaways:")
        print("  1. Same query works for batch or streaming (DuckDB-style)")
        print("  2. Use engine.query_cypher(query, as_operator=True) for streaming")
        print("  3. Use updates.graph_query(engine, query) for Stream API sugar")
        print("  4. Graph operators chain with normal operators (no special cases!)")
        print()

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
