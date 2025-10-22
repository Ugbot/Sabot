"""
Test Cypher Property Access in RETURN Clauses

Verifies that property access (e.g., RETURN a.name, b.age) works correctly.
"""

from sabot._cython.graph.engine.query_engine import GraphQueryEngine
from sabot import cyarrow as pa


def test_simple_property_access():
    """Test simple property access: RETURN a.name"""
    engine = GraphQueryEngine()

    vertices = pa.table({
        'id': [0, 1, 2],
        'label': ['Person', 'Person', 'Account'],
        'name': ['Alice', 'Bob', 'Checking'],
        'age': [30, 25, None]
    })
    engine.load_vertices(vertices, persist=False)

    edges = pa.table({
        'source': [0, 1],
        'target': [2, 2],
        'label': ['OWNS', 'OWNS']
    })
    engine.load_edges(edges, persist=False)

    # Query with property access
    result = engine.query_cypher('MATCH (a)-[r]->(b) RETURN a.name')

    assert result.table.num_rows == 2
    assert 'a.name' in result.table.column_names

    # Check values
    names = result.table.column('a.name').to_pylist()
    assert set(names) == {'Alice', 'Bob'}

    print("✅ test_simple_property_access passed")


def test_multiple_property_access():
    """Test multiple properties: RETURN a.name, b.name"""
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

    # Query with multiple property access
    result = engine.query_cypher('MATCH (a)-[r]->(b) RETURN a.name, b.name')

    assert result.table.num_rows == 2
    assert 'a.name' in result.table.column_names
    assert 'b.name' in result.table.column_names

    # Check values
    a_names = result.table.column('a.name').to_pylist()
    b_names = result.table.column('b.name').to_pylist()
    assert set(a_names) == {'Alice', 'Bob'}
    assert set(b_names) == {'Checking'}

    print("✅ test_multiple_property_access passed")


def test_mixed_id_and_property():
    """Test mix of ID and property: RETURN a, b.name"""
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

    # Query with ID and property
    result = engine.query_cypher('MATCH (a)-[r]->(b) RETURN a, b.name')

    assert result.table.num_rows == 2
    assert 'a' in result.table.column_names
    assert 'b.name' in result.table.column_names

    # Check values
    a_ids = result.table.column('a').to_pylist()
    b_names = result.table.column('b.name').to_pylist()
    assert set(a_ids) == {0, 1}
    assert set(b_names) == {'Checking'}

    print("✅ test_mixed_id_and_property passed")


def test_property_with_edge_filter():
    """Test property access with edge type filter"""
    engine = GraphQueryEngine()

    vertices = pa.table({
        'id': [0, 1, 2, 3],
        'label': ['Person', 'Person', 'Account', 'Account'],
        'name': ['Alice', 'Bob', 'Checking', 'Savings'],
        'balance': [None, None, 1000, 5000]
    })
    engine.load_vertices(vertices, persist=False)

    edges = pa.table({
        'source': [0, 1, 2],
        'target': [2, 3, 0],
        'label': ['OWNS', 'OWNS', 'BELONGS_TO']
    })
    engine.load_edges(edges, persist=False)

    # Query with edge filter and property access
    result = engine.query_cypher('MATCH (a)-[:OWNS]->(b) RETURN a.name, b.balance')

    assert result.table.num_rows == 2
    assert 'a.name' in result.table.column_names
    assert 'b.balance' in result.table.column_names

    # Check values
    a_names = result.table.column('a.name').to_pylist()
    b_balances = result.table.column('b.balance').to_pylist()
    assert set(a_names) == {'Alice', 'Bob'}
    assert set(b_balances) == {1000, 5000}

    print("✅ test_property_with_edge_filter passed")


def test_property_with_label_filter():
    """Test property access with node label filter"""
    engine = GraphQueryEngine()

    vertices = pa.table({
        'id': [0, 1, 2, 3],
        'label': ['Person', 'Person', 'Account', 'Company'],
        'name': ['Alice', 'Bob', 'Checking', 'Acme Corp']
    })
    engine.load_vertices(vertices, persist=False)

    edges = pa.table({
        'source': [0, 1, 0],
        'target': [2, 2, 3],
        'label': ['OWNS', 'OWNS', 'WORKS_FOR']
    })
    engine.load_edges(edges, persist=False)

    # Query with label filter and property access
    result = engine.query_cypher('MATCH (a:Person)-[r]->(b:Account) RETURN a.name, b.name')

    assert result.table.num_rows == 2
    assert 'a.name' in result.table.column_names
    assert 'b.name' in result.table.column_names

    # Check values (should only include Person->Account edges, not Person->Company)
    a_names = result.table.column('a.name').to_pylist()
    b_names = result.table.column('b.name').to_pylist()
    assert set(a_names) == {'Alice', 'Bob'}
    assert set(b_names) == {'Checking'}

    print("✅ test_property_with_label_filter passed")


def test_property_with_limit():
    """Test property access with LIMIT"""
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

    # Query with property access and LIMIT
    result = engine.query_cypher('MATCH (a)-[r]->(b) RETURN a.name, b.name LIMIT 3')

    assert result.table.num_rows == 3
    assert 'a.name' in result.table.column_names
    assert 'b.name' in result.table.column_names

    print("✅ test_property_with_limit passed")


def run_all_tests():
    """Run all property access tests"""
    print("=" * 60)
    print("Running Property Access Tests")
    print("=" * 60)
    print()

    tests = [
        test_simple_property_access,
        test_multiple_property_access,
        test_mixed_id_and_property,
        test_property_with_edge_filter,
        test_property_with_label_filter,
        test_property_with_limit
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
