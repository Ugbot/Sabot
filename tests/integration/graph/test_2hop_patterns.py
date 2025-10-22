"""
Test Cypher 2-Hop Pattern Matching

Verifies that 2-hop patterns like (a)-[r1]->(b)-[r2]->(c) work correctly.
"""

from sabot._cython.graph.engine.query_engine import GraphQueryEngine
from sabot import cyarrow as pa


def test_basic_2hop_pattern():
    """Test basic 2-hop pattern: (a)-[r1]->(b)-[r2]->(c)"""
    engine = GraphQueryEngine()

    # Create a simple chain: 0 -> 1 -> 2 -> 3
    vertices = pa.table({
        'id': [0, 1, 2, 3],
        'label': ['Person', 'Person', 'Person', 'Person'],
        'name': ['Alice', 'Bob', 'Charlie', 'David']
    })
    engine.load_vertices(vertices, persist=False)

    edges = pa.table({
        'source': [0, 1, 2],
        'target': [1, 2, 3],
        'label': ['KNOWS', 'KNOWS', 'KNOWS']
    })
    engine.load_edges(edges, persist=False)

    # Query for 2-hop paths
    result = engine.query_cypher('MATCH (a)-[r1]->(b)-[r2]->(c) RETURN a, b, c')

    # Should find 2 paths:
    # 0 -> 1 -> 2
    # 1 -> 2 -> 3
    assert result.table.num_rows == 2
    assert 'a' in result.table.column_names
    assert 'b' in result.table.column_names
    assert 'c' in result.table.column_names

    # Check the paths
    a_ids = set(result.table.column('a').to_pylist())
    b_ids = set(result.table.column('b').to_pylist())
    c_ids = set(result.table.column('c').to_pylist())

    assert a_ids == {0, 1}
    assert b_ids == {1, 2}
    assert c_ids == {2, 3}

    print("✅ test_basic_2hop_pattern passed")


def test_2hop_with_edge_types():
    """Test 2-hop pattern with different edge types"""
    engine = GraphQueryEngine()

    vertices = pa.table({
        'id': [0, 1, 2, 3, 4],
        'label': ['Person', 'Person', 'Company', 'Person', 'Company'],
        'name': ['Alice', 'Bob', 'Acme', 'Charlie', 'Globex']
    })
    engine.load_vertices(vertices, persist=False)

    edges = pa.table({
        'source': [0, 1, 2, 3],
        'target': [1, 2, 3, 4],
        'label': ['KNOWS', 'WORKS_AT', 'EMPLOYS', 'WORKS_AT']
    })
    engine.load_edges(edges, persist=False)

    # Query for Person -> WORKS_AT -> Company -> EMPLOYS -> Person
    result = engine.query_cypher('MATCH (a)-[:WORKS_AT]->(b)-[:EMPLOYS]->(c) RETURN a, b, c')

    # Should find 1 path: 1 -> 2 -> 3
    assert result.table.num_rows == 1

    a_id = result.table.column('a')[0].as_py()
    b_id = result.table.column('b')[0].as_py()
    c_id = result.table.column('c')[0].as_py()

    assert a_id == 1
    assert b_id == 2
    assert c_id == 3

    print("✅ test_2hop_with_edge_types passed")


def test_2hop_with_node_labels():
    """Test 2-hop pattern with node label filtering"""
    engine = GraphQueryEngine()

    vertices = pa.table({
        'id': [0, 1, 2, 3],
        'label': ['Person', 'Account', 'Account', 'Person'],
        'name': ['Alice', 'Checking', 'Savings', 'Bob']
    })
    engine.load_vertices(vertices, persist=False)

    edges = pa.table({
        'source': [0, 0, 1],
        'target': [1, 2, 3],
        'label': ['OWNS', 'OWNS', 'BELONGS_TO']
    })
    engine.load_edges(edges, persist=False)

    # Query for Person -> Account -> Person paths
    result = engine.query_cypher('MATCH (a:Person)-[r1]->(b:Account)-[r2]->(c:Person) RETURN a, b, c')

    # Should find 1 path: 0 -> 1 -> 3 (Alice owns Checking which belongs to Bob)
    assert result.table.num_rows == 1

    a_id = result.table.column('a')[0].as_py()
    b_id = result.table.column('b')[0].as_py()
    c_id = result.table.column('c')[0].as_py()

    assert a_id == 0
    assert b_id == 1
    assert c_id == 3

    print("✅ test_2hop_with_node_labels passed")


def test_2hop_with_property_access():
    """Test 2-hop pattern with property access in RETURN"""
    engine = GraphQueryEngine()

    vertices = pa.table({
        'id': [0, 1, 2, 3],
        'label': ['Person', 'Person', 'Person', 'Person'],
        'name': ['Alice', 'Bob', 'Charlie', 'David']
    })
    engine.load_vertices(vertices, persist=False)

    edges = pa.table({
        'source': [0, 1, 2],
        'target': [1, 2, 3],
        'label': ['KNOWS', 'KNOWS', 'KNOWS']
    })
    engine.load_edges(edges, persist=False)

    # Query for 2-hop paths with property access
    result = engine.query_cypher('MATCH (a)-[r1]->(b)-[r2]->(c) RETURN a.name, b.name, c.name')

    assert result.table.num_rows == 2
    assert 'a.name' in result.table.column_names
    assert 'b.name' in result.table.column_names
    assert 'c.name' in result.table.column_names

    # Check the names
    # Path 1: Alice -> Bob -> Charlie
    # Path 2: Bob -> Charlie -> David
    a_names = result.table.column('a.name').to_pylist()
    b_names = result.table.column('b.name').to_pylist()
    c_names = result.table.column('c.name').to_pylist()

    assert set(a_names) == {'Alice', 'Bob'}
    assert set(b_names) == {'Bob', 'Charlie'}
    assert set(c_names) == {'Charlie', 'David'}

    print("✅ test_2hop_with_property_access passed")


def test_2hop_with_limit():
    """Test 2-hop pattern with LIMIT"""
    engine = GraphQueryEngine()

    # Create a longer chain
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

    # Query for 2-hop paths with LIMIT
    result = engine.query_cypher('MATCH (a)-[r1]->(b)-[r2]->(c) RETURN a, b, c LIMIT 3')

    # Should find 8 total paths (0->1->2, 1->2->3, ..., 7->8->9)
    # But LIMIT to 3
    assert result.table.num_rows == 3

    print("✅ test_2hop_with_limit passed")


def test_2hop_empty_graph():
    """Test 2-hop pattern on empty graph"""
    engine = GraphQueryEngine()

    vertices = pa.table({
        'id': [0, 1, 2],
        'label': ['Person', 'Person', 'Person'],
        'name': ['Alice', 'Bob', 'Charlie']
    })
    engine.load_vertices(vertices, persist=False)

    # Empty edges
    edges = pa.table({
        'source': pa.array([], type=pa.int64()),
        'target': pa.array([], type=pa.int64()),
        'label': pa.array([], type=pa.string())
    })
    engine.load_edges(edges, persist=False)

    # Query should return empty result
    result = engine.query_cypher('MATCH (a)-[r1]->(b)-[r2]->(c) RETURN a, b, c')

    assert result.table.num_rows == 0
    assert 'a' in result.table.column_names
    assert 'b' in result.table.column_names
    assert 'c' in result.table.column_names

    print("✅ test_2hop_empty_graph passed")


def test_2hop_triangle_pattern():
    """Test 2-hop pattern in a graph with triangles"""
    engine = GraphQueryEngine()

    vertices = pa.table({
        'id': [0, 1, 2],
        'label': ['Person', 'Person', 'Person'],
        'name': ['Alice', 'Bob', 'Charlie']
    })
    engine.load_vertices(vertices, persist=False)

    # Create a triangle: 0 -> 1 -> 2 -> 0
    edges = pa.table({
        'source': [0, 1, 2],
        'target': [1, 2, 0],
        'label': ['KNOWS', 'KNOWS', 'KNOWS']
    })
    engine.load_edges(edges, persist=False)

    # Query for 2-hop paths
    result = engine.query_cypher('MATCH (a)-[r1]->(b)-[r2]->(c) RETURN a, b, c')

    # Should find 3 paths:
    # 0 -> 1 -> 2
    # 1 -> 2 -> 0
    # 2 -> 0 -> 1
    assert result.table.num_rows == 3

    print("✅ test_2hop_triangle_pattern passed")


def run_all_tests():
    """Run all 2-hop pattern tests"""
    print("=" * 60)
    print("Running 2-Hop Pattern Tests")
    print("=" * 60)
    print()

    tests = [
        test_basic_2hop_pattern,
        test_2hop_with_edge_types,
        test_2hop_with_node_labels,
        test_2hop_with_property_access,
        test_2hop_with_limit,
        test_2hop_empty_graph,
        test_2hop_triangle_pattern
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
