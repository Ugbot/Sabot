"""
Execution Engine Demo

Demonstrates the Sabot-native execution infrastructure for graph queries.

Components:
- Query operators (BaseOperator-based)
- Pattern executor (bridges to C++ kernels)
- Physical plan builder (logical → physical conversion)
- Query executor (orchestrates execution)

Features:
- Morsel-based parallelism for large batches (≥10K rows)
- Shuffle-aware operators for distributed execution
- Integration with existing pattern matching (3-37M matches/sec)
"""

from sabot import cyarrow as pa
from sabot._cython.graph.engine.query_engine import GraphQueryEngine, QueryConfig


def demo_basic_execution():
    """
    Demonstrate basic query execution.

    Shows:
    - Loading graph data
    - Executing Cypher queries
    - Viewing results
    """
    print("=" * 80)
    print("Demo 1: Basic Query Execution")
    print("=" * 80)
    print()

    # Create graph engine
    engine = GraphQueryEngine()

    # Create sample graph
    vertices = pa.table({
        'id': [0, 1, 2, 3, 4],
        'label': ['Person', 'Person', 'Person', 'Account', 'Account'],
        'name': ['Alice', 'Bob', 'Charlie', 'Checking', 'Savings']
    })

    edges = pa.table({
        'source': [0, 1, 0, 2],
        'target': [1, 2, 3, 4],
        'label': ['KNOWS', 'KNOWS', 'OWNS', 'OWNS']
    })

    # Load graph
    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    print("Loaded graph:")
    print(f"  {len(vertices)} vertices")
    print(f"  {len(edges)} edges")
    print()

    # Execute query
    query = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        RETURN a.name AS from, b.name AS to
    """

    print(f"Executing query:")
    print(f"  {query.strip()}")
    print()

    result = engine.query_cypher(query)
    print("Results:")
    print(result.to_pandas())
    print()
    print(f"✅ Query completed in {result.metadata.execution_time_ms:.2f}ms")
    print()


def demo_query_optimization():
    """
    Demonstrate query optimization with EXPLAIN.

    Shows:
    - Pattern selectivity estimation
    - Execution plan generation
    - Cost estimates
    - Optimization decisions
    """
    print("=" * 80)
    print("Demo 2: Query Optimization")
    print("=" * 80)
    print()

    # Create larger graph
    engine = GraphQueryEngine()

    num_persons = 100
    vertices = pa.table({
        'id': list(range(num_persons)),
        'label': ['Person'] * num_persons,
        'name': [f'Person_{i}' for i in range(num_persons)],
        'age': [20 + (i % 50) for i in range(num_persons)]
    })

    # Create edges
    edges = pa.table({
        'source': [i for i in range(num_persons - 1)],
        'target': [i + 1 for i in range(num_persons - 1)],
        'label': ['KNOWS'] * (num_persons - 1)
    })

    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    # Show execution plan
    query = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        WHERE a.age > 25
        RETURN a.name, b.name
        LIMIT 10
    """

    print(f"Query:")
    print(f"  {query.strip()}")
    print()

    # Get EXPLAIN output
    explanation = engine.query_cypher(query, config=QueryConfig(explain=True))
    plan_text = explanation.table.column('QUERY PLAN')[0].as_py()

    print(plan_text)
    print()


def demo_execution_infrastructure():
    """
    Demonstrate the execution infrastructure conceptually.

    Shows:
    - How operators extend BaseOperator
    - Stateful vs stateless operators
    - Morsel-driven execution strategy
    - Shuffle-aware execution
    """
    print("=" * 80)
    print("Demo 3: Execution Infrastructure")
    print("=" * 80)
    print()

    print("Sabot Graph Query Execution Architecture:")
    print()
    print("1. Query Operators (BaseOperator-based)")
    print("   - PatternScanOperator: Scan using C++ pattern matching kernels")
    print("   - GraphFilterOperator: Apply filter predicates")
    print("   - GraphProjectOperator: Column selection")
    print("   - GraphLimitOperator: LIMIT/OFFSET")
    print()
    print("   Stateful operators (require shuffle):")
    print("   - GraphJoinOperator: Hash join for multi-hop patterns")
    print("   - GraphAggregateOperator: GROUP BY aggregations")
    print()

    print("2. Execution Strategy")
    print("   - Small batches (<10K rows): Direct execution")
    print("   - Large batches, stateless: Morsel parallelism (local C++ threads)")
    print("   - Large batches, stateful: Network shuffle (Arrow Flight)")
    print()

    print("3. Pattern Executor")
    print("   - Bridges to C++ pattern matching kernels")
    print("   - Local execution: 3-37M matches/sec")
    print("   - Distributed execution: Partition graph by vertex ID")
    print()

    print("4. Physical Plan Builder")
    print("   - Converts LogicalPlan → PhysicalPlan")
    print("   - Assigns operators to plan nodes")
    print("   - Annotates with stateful/stateless classification")
    print("   - Determines partition keys for shuffle")
    print()

    print("5. Query Executor")
    print("   - Orchestrates operator chain execution")
    print("   - Local mode: MorselDrivenOperator for parallelism")
    print("   - Distributed mode: ShuffleTransport for network shuffle")
    print()

    print("6. Integration Points")
    print("   - Existing translators use pattern matching directly")
    print("   - Executor infrastructure ready for:")
    print("     * Distributed graph query execution")
    print("     * Explicit operator pipelines")
    print("     * Multi-agent shuffle coordination")
    print()


if __name__ == '__main__':
    print()
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 23 + "Execution Engine Demo" + " " * 34 + "║")
    print("╚" + "=" * 78 + "╝")
    print()

    # Run demos
    demo_basic_execution()
    demo_query_optimization()
    demo_execution_infrastructure()

    print("=" * 80)
    print("✨ Demo complete!")
    print("=" * 80)
    print()
    print("Next steps:")
    print("- Compile Cython operators for morsel parallelism")
    print("- Integrate ShuffleTransport for distributed execution")
    print("- Implement continuous query support (Phase 4.6)")
    print()
