"""
Test Cypher Query Execution

Verifies that Cypher queries execute correctly over Arrow-based graph storage.
"""

from sabot._cython.graph.engine.query_engine import GraphQueryEngine
from sabot import cyarrow as pa


def test_basic_edge_pattern():
    """Test basic edge pattern: MATCH (a)-[r]->(b)"""
    engine = GraphQueryEngine()

    vertices = pa.table({
        'id': [0, 1, 2],
        'label': ['Person', 'Person', 'Account'],
        'name': ['Alice', 'Bob', 'Checking']
    })
    engine.load_vertices(vertices, persist=False)

    edges = pa.table({
        'source': [0, 1],
        'target': [2, 2],
        'label': ['OWNS', 'OWNS']
    })
    engine.load_edges(edges, persist=False)

    result = engine.query_cypher('MATCH (a)-[r]->(b) RETURN a, b')

    assert result.table.num_rows == 2
    assert 'a' in result.table.column_names
    assert 'b' in result.table.column_names

    print("✅ test_basic_edge_pattern passed")


def test_edge_type_filter():
    """Test edge type filtering: MATCH (a)-[:OWNS]->(b)"""
    engine = GraphQueryEngine()

    vertices = pa.table({
        'id': [0, 1, 2, 3],
        'label': ['Person', 'Person', 'Account', 'Account'],
        'name': ['Alice', 'Bob', 'Checking', 'Savings']
    })
    engine.load_vertices(vertices, persist=False)

    edges = pa.table({
        'source': [0, 1, 2],
        'target': [2, 2, 0],
        'label': ['OWNS', 'OWNS', 'BELONGS_TO']
    })
    engine.load_edges(edges, persist=False)

    result = engine.query_cypher('MATCH (a)-[:OWNS]->(b) RETURN a, b')

    # Should only return OWNS edges (not BELONGS_TO)
    assert result.table.num_rows == 2

    print("✅ test_edge_type_filter passed")


def test_return_limit():
    """Test RETURN with LIMIT"""
    engine = GraphQueryEngine()

    vertices = pa.table({
        'id': list(range(10)),
        'label': ['Person'] * 10,
        'name': [f'Person_{i}' for i in range(10)]
    })
    engine.load_vertices(vertices, persist=False)

    edges = pa.table({
        'source': list(range(9)),
        'target': list(range(1, 10)),
        'label': ['KNOWS'] * 9
    })
    engine.load_edges(edges, persist=False)

    result = engine.query_cypher('MATCH (a)-[r]->(b) RETURN a, b LIMIT 3')

    # Should limit to 3 results
    assert result.table.num_rows == 3

    print("✅ test_return_limit passed")


def test_multiple_edge_types():
    """Test graph with multiple edge types"""
    engine = GraphQueryEngine()

    vertices = pa.table({
        'id': [0, 1, 2, 3, 4],
        'label': ['Person', 'Person', 'Company', 'Person', 'Company'],
        'name': ['Alice', 'Bob', 'Acme', 'Charlie', 'Globex']
    })
    engine.load_vertices(vertices, persist=False)

    edges = pa.table({
        'source': [0, 1, 0, 3],
        'target': [1, 0, 2, 4],
        'label': ['KNOWS', 'KNOWS', 'WORKS_AT', 'WORKS_AT']
    })
    engine.load_edges(edges, persist=False)

    # Query all edges
    result_all = engine.query_cypher('MATCH (a)-[r]->(b) RETURN a, b')
    assert result_all.table.num_rows == 4

    # Query only KNOWS edges
    result_knows = engine.query_cypher('MATCH (a)-[:KNOWS]->(b) RETURN a, b')
    assert result_knows.table.num_rows == 2

    # Query only WORKS_AT edges
    result_works = engine.query_cypher('MATCH (a)-[:WORKS_AT]->(b) RETURN a, b')
    assert result_works.table.num_rows == 2

    print("✅ test_multiple_edge_types passed")


def test_variable_naming():
    """Test custom variable names in pattern"""
    engine = GraphQueryEngine()

    vertices = pa.table({
        'id': [0, 1, 2],
        'label': ['Person', 'Person', 'Account'],
        'name': ['Alice', 'Bob', 'Checking']
    })
    engine.load_vertices(vertices, persist=False)

    edges = pa.table({
        'source': [0, 1],
        'target': [2, 2],
        'label': ['OWNS', 'OWNS']
    })
    engine.load_edges(edges, persist=False)

    # Use custom variable names
    result = engine.query_cypher('MATCH (person)-[relationship]->(account) RETURN person, account')

    assert result.table.num_rows == 2
    assert 'person' in result.table.column_names
    assert 'account' in result.table.column_names

    print("✅ test_variable_naming passed")


def test_empty_graph():
    """Test query on graph with no edges"""
    engine = GraphQueryEngine()

    vertices = pa.table({
        'id': [0, 1],
        'label': ['Person', 'Person'],
        'name': ['Alice', 'Bob']
    })
    engine.load_vertices(vertices, persist=False)

    # Load empty edges table (0 edges but table exists)
    edges = pa.table({
        'source': pa.array([], type=pa.int64()),
        'target': pa.array([], type=pa.int64()),
        'label': pa.array([], type=pa.string())
    })
    engine.load_edges(edges, persist=False)

    # Query should return empty result (not error)
    result = engine.query_cypher('MATCH (a)-[r]->(b) RETURN a, b')

    # Should return empty result
    assert result.table.num_rows == 0

    print("✅ test_empty_graph passed")


def run_all_tests():
    """Run all Cypher query tests"""
    print("=" * 60)
    print("Running Cypher Query Tests")
    print("=" * 60)
    print()

    tests = [
        test_basic_edge_pattern,
        test_edge_type_filter,
        test_return_limit,
        test_multiple_edge_types,
        test_variable_naming,
        test_empty_graph
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"❌ {test.__name__} failed: {e}")
            failed += 1
            import traceback
            traceback.print_exc()

    print()
    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)

    return failed == 0


if __name__ == '__main__':
    import sys
    success = run_all_tests()
    sys.exit(0 if success else 1)
