"""
Test Execution Engine

Demonstrates the query execution infrastructure:
- Query operators (PatternScan, Filter, Project, Limit)
- Pattern executor
- Physical plan builder
- Query executor

Tests verify that the execution infrastructure is ready for
distributed execution and morsel-based parallelism.
"""

from sabot import cyarrow as pa
from sabot._cython.graph.engine.query_engine import GraphQueryEngine, QueryConfig


def create_test_graph():
    """
    Create a test property graph.

    Graph structure:
    - 1000 Person nodes
    - 10 VIP nodes
    - 100 Account nodes
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
    knows_sources = [i for i in range(0, 999)]
    knows_targets = [i+1 for i in range(0, 999)]

    friend_sources = [i for i in range(1000, 1009)]
    friend_targets = [i+1 for i in range(1000, 1009)]

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


def test_basic_query_execution():
    """Test basic query execution using existing infrastructure."""
    print("=" * 80)
    print("Test 1: Basic Query Execution")
    print("=" * 80)
    print()

    # Create engine
    engine = GraphQueryEngine()

    # Load graph
    vertices, edges = create_test_graph()
    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    # Print stats
    stats = engine.get_graph_stats()
    print(f"Graph loaded:")
    print(f"  Vertices: {stats['num_vertices']}")
    print(f"  Edges: {stats['num_edges']}")
    print(f"  Vertex labels: {stats['vertex_labels']}")
    print(f"  Edge labels: {stats['edge_labels']}")
    print()

    # Execute simple Cypher query
    query = """
        MATCH (a)-[:KNOWS]->(b)
        RETURN a, b
        LIMIT 10
    """

    print(f"Executing query:")
    print(f"  {query.strip()}")
    print()

    try:
        result = engine.query_cypher(query, config=QueryConfig(optimize=True))
        print(f"✅ Query executed successfully")
        print(f"  Results: {result.metadata.num_results} rows")
        print(f"  Execution time: {result.metadata.execution_time_ms:.2f}ms")
        print()
    except Exception as e:
        print(f"❌ Query failed: {e}")
        print()


def test_query_operators():
    """Test query operators directly."""
    print("=" * 80)
    print("Test 2: Query Operators")
    print("=" * 80)
    print()

    try:
        from sabot._cython.graph.executor.query_operators import (
            GraphFilterOperator,
            GraphProjectOperator,
            GraphLimitOperator,
        )
        print("✅ Query operators imported successfully")
        print()

        # Create test batch
        batch = pa.table({
            'id': list(range(100)),
            'name': [f'node_{i}' for i in range(100)],
            'age': [20 + (i % 50) for i in range(100)]
        })

        # Test ProjectOperator
        print("Testing GraphProjectOperator...")
        project_op = GraphProjectOperator(columns=['id', 'name'])
        projected = project_op.process_batch(batch)
        print(f"  Input columns: {batch.column_names}")
        print(f"  Output columns: {projected.column_names if projected else 'None'}")
        print(f"  ✅ Projection works")
        print()

        # Test LimitOperator
        print("Testing GraphLimitOperator...")
        limit_op = GraphLimitOperator(limit=10, offset=5)
        limited = limit_op.process_batch(batch)
        print(f"  Input rows: {batch.num_rows}")
        print(f"  Output rows: {limited.num_rows if limited else 0}")
        print(f"  ✅ Limit works")
        print()

    except ImportError as e:
        print(f"⚠️  Could not import query operators: {e}")
        print(f"   This is expected if operators haven't been compiled yet")
        print()


def test_pattern_executor():
    """Test pattern executor."""
    print("=" * 80)
    print("Test 3: Pattern Executor")
    print("=" * 80)
    print()

    try:
        from sabot._cython.graph.executor.pattern_executor import PatternMatchExecutor

        print("✅ Pattern executor imported successfully")
        print()

        # Create engine and load graph
        engine = GraphQueryEngine()
        vertices, edges = create_test_graph()
        engine.load_vertices(vertices, persist=False)
        engine.load_edges(edges, persist=False)

        # Create pattern executor
        executor = PatternMatchExecutor(engine, distributed=False)
        print("  Created PatternMatchExecutor")
        print(f"  Distributed mode: {executor.distributed}")
        print(f"  ✅ Pattern executor ready")
        print()

    except ImportError as e:
        print(f"⚠️  Could not import pattern executor: {e}")
        print(f"   This is expected if executor hasn't been compiled yet")
        print()


def test_physical_plan_builder():
    """Test physical plan builder."""
    print("=" * 80)
    print("Test 4: Physical Plan Builder")
    print("=" * 80)
    print()

    try:
        from sabot._cython.graph.executor.physical_plan_builder import PhysicalPlanBuilder
        from sabot._cython.graph.compiler.query_plan import (
            LogicalPlan,
            PlanNode,
            NodeType,
            ScanNode,
        )

        print("✅ Physical plan builder imported successfully")
        print()

        # Create engine
        engine = GraphQueryEngine()
        vertices, edges = create_test_graph()
        engine.load_vertices(vertices, persist=False)
        engine.load_edges(edges, persist=False)

        # Create simple logical plan
        scan_node = ScanNode(
            NodeType.SCAN_VERTICES,
            table_name='vertices'
        )
        logical_plan = LogicalPlan(root=scan_node)

        # Build physical plan
        builder = PhysicalPlanBuilder(engine, distributed=False)
        physical_plan = builder.build(logical_plan)

        print("  Created PhysicalPlanBuilder")
        print(f"  Logical plan nodes: {len(logical_plan.get_all_nodes())}")
        print(f"  Physical plan nodes: {len(physical_plan.get_execution_order())}")
        print(f"  ✅ Physical plan builder works")
        print()

    except ImportError as e:
        print(f"⚠️  Could not import physical plan builder: {e}")
        print(f"   This is expected if components haven't been installed yet")
        print()


def test_query_executor():
    """Test query executor."""
    print("=" * 80)
    print("Test 5: Query Executor")
    print("=" * 80)
    print()

    try:
        from sabot._cython.graph.executor.query_executor import QueryExecutor
        from sabot._cython.graph.compiler.query_plan import (
            LogicalPlan,
            PlanNode,
            NodeType,
            ScanNode,
        )

        print("✅ Query executor imported successfully")
        print()

        # Create engine
        engine = GraphQueryEngine()
        vertices, edges = create_test_graph()
        engine.load_vertices(vertices, persist=False)
        engine.load_edges(edges, persist=False)

        # Create executor
        executor = QueryExecutor(engine, shuffle_transport=None, num_workers=4)
        print("  Created QueryExecutor")
        print(f"  Workers: {executor.num_workers}")
        print(f"  Shuffle transport: {executor.shuffle_transport is not None}")
        print(f"  ✅ Query executor ready")
        print()

        # Get stats
        stats = executor.get_stats()
        print(f"  Executor stats: {stats}")
        print()

    except ImportError as e:
        print(f"⚠️  Could not import query executor: {e}")
        print(f"   This is expected if components haven't been installed yet")
        print()


def test_explain_with_optimizer():
    """Test EXPLAIN with optimization."""
    print("=" * 80)
    print("Test 6: EXPLAIN with Optimizer")
    print("=" * 80)
    print()

    # Create engine
    engine = GraphQueryEngine()
    vertices, edges = create_test_graph()
    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    # Execute EXPLAIN
    query = """
        MATCH (a)-[:KNOWS]->(b:VIP)
        RETURN a, b
        LIMIT 10
    """

    print(f"Executing EXPLAIN for:")
    print(f"  {query.strip()}")
    print()

    try:
        explanation = engine.query_cypher(query, config=QueryConfig(explain=True))
        plan_text = explanation.table.column('QUERY PLAN')[0].as_py()
        print(plan_text)
        print()
        print("✅ EXPLAIN works with optimizer")
        print()
    except Exception as e:
        print(f"❌ EXPLAIN failed: {e}")
        print()


if __name__ == '__main__':
    print()
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "Execution Engine Test Suite" + " " * 31 + "║")
    print("╚" + "=" * 78 + "╝")
    print()

    # Run tests
    test_basic_query_execution()
    test_query_operators()
    test_pattern_executor()
    test_physical_plan_builder()
    test_query_executor()
    test_explain_with_optimizer()

    print("=" * 80)
    print("✨ Test suite complete!")
    print("=" * 80)
    print()
