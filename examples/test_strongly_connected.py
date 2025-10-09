"""
Test suite for strongly connected components (Tarjan's algorithm).

Strongly connected components (SCCs) are maximal sets of vertices where
every vertex is reachable from every other vertex.
"""

import sys
sys.path.insert(0, '/Users/bengamble/Sabot')

import pyarrow as pa
from sabot._cython.graph.traversal.strongly_connected_components import (
    strongly_connected_components,
    scc_membership,
    largest_scc,
    is_strongly_connected,
    find_source_sccs,
    find_sink_sccs
)


def create_directed_graph(edges, num_vertices):
    """Helper to create directed graph from edge list."""
    # Build CSR
    indptr_list = [0]
    indices_list = []

    for u in range(num_vertices):
        neighbors = [v for (src, v) in edges if src == u]
        indices_list.extend(neighbors)
        indptr_list.append(len(indices_list))

    indptr = pa.array(indptr_list, type=pa.int64())
    indices = pa.array(indices_list, type=pa.int64())

    return indptr, indices


def test_single_cycle():
    """
    Test: Directed cycle 0 -> 1 -> 2 -> 0
    Should find 1 SCC: {0, 1, 2}
    """
    print("\n=== Test 1: Single Cycle ===")

    edges = [(0, 1), (1, 2), (2, 0)]
    indptr, indices = create_directed_graph(edges, 3)

    result = strongly_connected_components(indptr, indices, 3)

    assert result.num_components() == 1, "Should have 1 SCC"
    assert result.largest_component_size() == 3, "Should include all 3 vertices"

    component_ids = result.component_ids().to_pylist()
    assert len(set(component_ids)) == 1, "All vertices should be in same SCC"

    # Test is_strongly_connected
    assert is_strongly_connected(indptr, indices, 3), "Graph should be strongly connected"

    print(f"✓ Found {result.num_components()} strongly connected component")
    print(f"✓ Component IDs: {component_ids}")
    print(f"✓ Graph is strongly connected")


def test_multiple_sccs():
    """
    Test: Multiple SCCs
    0 -> 1 -> 2 -> 0 (SCC 1)
    3 -> 4 (chain, 2 trivial SCCs)
    """
    print("\n=== Test 2: Multiple SCCs ===")

    edges = [(0, 1), (1, 2), (2, 0), (3, 4)]
    indptr, indices = create_directed_graph(edges, 5)

    result = strongly_connected_components(indptr, indices, 5)

    assert result.num_components() == 3, "Should have 3 SCCs"

    component_ids = result.component_ids().to_pylist()
    sizes = result.component_sizes().to_pylist()

    # One SCC of size 3, two of size 1
    assert 3 in sizes, "Should have SCC of size 3"
    assert sizes.count(1) == 2, "Should have 2 trivial SCCs"

    print(f"✓ Found {result.num_components()} SCCs")
    print(f"✓ Component IDs: {component_ids}")
    print(f"✓ Component sizes: {sizes}")


def test_dag():
    """
    Test: Directed Acyclic Graph (DAG)
    0 -> 1 -> 2
    All vertices are trivial SCCs
    """
    print("\n=== Test 3: DAG (all trivial SCCs) ===")

    edges = [(0, 1), (1, 2)]
    indptr, indices = create_directed_graph(edges, 3)

    result = strongly_connected_components(indptr, indices, 3)

    assert result.num_components() == 3, "DAG should have all trivial SCCs"
    sizes = result.component_sizes().to_pylist()
    assert all(s == 1 for s in sizes), "All SCCs should be size 1"

    assert not is_strongly_connected(indptr, indices, 3), "DAG is not strongly connected"

    print(f"✓ Found {result.num_components()} trivial SCCs (DAG)")
    print(f"✓ All components are size 1")


def test_source_sink_sccs():
    """
    Test: Find source and sink SCCs
    SCC 0 (source): {0, 1, 2} -> SCC 1 (sink): {3, 4, 5}
    """
    print("\n=== Test 4: Source and Sink SCCs ===")

    edges = [
        # SCC 0 (cycle)
        (0, 1), (1, 2), (2, 0),
        # SCC 1 (cycle)
        (3, 4), (4, 5), (5, 3),
        # Edge from SCC 0 to SCC 1
        (0, 3)
    ]
    indptr, indices = create_directed_graph(edges, 6)

    result = strongly_connected_components(indptr, indices, 6)

    assert result.num_components() == 2, "Should have 2 SCCs"

    # Find source and sink SCCs
    sources = find_source_sccs(result.component_ids(), indptr, indices, 6)
    sinks = find_sink_sccs(result.component_ids(), indptr, indices, 6)

    assert len(sources) == 1, "Should have 1 source SCC"
    assert len(sinks) == 1, "Should have 1 sink SCC"

    print(f"✓ Found {result.num_components()} SCCs")
    print(f"✓ Source SCCs: {sources.to_pylist()}")
    print(f"✓ Sink SCCs: {sinks.to_pylist()}")


def test_largest_scc():
    """
    Test: Get largest SCC
    """
    print("\n=== Test 5: Largest SCC ===")

    edges = [
        # Large SCC
        (0, 1), (1, 2), (2, 3), (3, 4), (4, 0),
        # Small SCC
        (5, 6), (6, 5)
    ]
    indptr, indices = create_directed_graph(edges, 7)

    result = strongly_connected_components(indptr, indices, 7)

    assert result.num_components() == 2, "Should have 2 SCCs"

    largest = largest_scc(result.component_ids(), result.component_sizes())
    assert len(largest) == 5, "Largest SCC should have 5 vertices"

    print(f"✓ Largest SCC has {len(largest)} vertices")
    print(f"✓ Vertices: {largest.to_pylist()}")


def test_complex_graph():
    """
    Test: Complex graph with multiple SCCs
    """
    print("\n=== Test 6: Complex Graph ===")

    edges = [
        # SCC 1: {0, 1, 2}
        (0, 1), (1, 2), (2, 0),
        # SCC 2: {3, 4}
        (3, 4), (4, 3),
        # SCC 3: {5} (trivial)
        # Edges between SCCs
        (2, 3),  # SCC 1 -> SCC 2
        (4, 5),  # SCC 2 -> SCC 3
    ]
    indptr, indices = create_directed_graph(edges, 6)

    result = strongly_connected_components(indptr, indices, 6)

    assert result.num_components() == 3, "Should have 3 SCCs"

    # Check topology
    sources = find_source_sccs(result.component_ids(), indptr, indices, 6)
    sinks = find_sink_sccs(result.component_ids(), indptr, indices, 6)

    assert len(sources) == 1, "Should have 1 source SCC"
    assert len(sinks) == 1, "Should have 1 sink SCC"

    print(f"✓ Found {result.num_components()} SCCs")
    print(f"✓ Source: {sources.to_pylist()} (should be SCC with {0,1,2})")
    print(f"✓ Sink: {sinks.to_pylist()} (should be SCC with {5})")


def test_self_loops():
    """
    Test: Graph with self-loops
    """
    print("\n=== Test 7: Self-Loops ===")

    edges = [
        (0, 0),  # Self-loop
        (1, 1),  # Self-loop
        (2, 2),  # Self-loop
    ]
    indptr, indices = create_directed_graph(edges, 3)

    result = strongly_connected_components(indptr, indices, 3)

    # Each vertex with self-loop is its own SCC
    assert result.num_components() == 3, "Should have 3 SCCs"

    print(f"✓ Found {result.num_components()} SCCs (each self-loop is an SCC)")


def test_scc_membership():
    """
    Test: Get SCC membership as RecordBatch
    """
    print("\n=== Test 8: SCC Membership ===")

    edges = [(0, 1), (1, 2), (2, 0), (3, 4)]
    indptr, indices = create_directed_graph(edges, 5)

    membership = scc_membership(indptr, indices, 5)

    assert membership.num_rows == 5, "Should have 5 rows"
    assert membership.num_columns == 2, "Should have 2 columns"
    assert membership.column_names == ['component_id', 'vertex_id'], "Column names should match"

    print(f"✓ SCC membership batch has {membership.num_rows} rows")
    print(f"✓ Columns: {membership.column_names}")


def test_empty_graph():
    """
    Test: Empty graph (no edges)
    """
    print("\n=== Test 9: Empty Graph ===")

    edges = []
    indptr, indices = create_directed_graph(edges, 5)

    result = strongly_connected_components(indptr, indices, 5)

    # Each vertex is its own trivial SCC
    assert result.num_components() == 5, "Should have 5 trivial SCCs"
    sizes = result.component_sizes().to_pylist()
    assert all(s == 1 for s in sizes), "All SCCs should be size 1"

    print(f"✓ Empty graph has {result.num_components()} trivial SCCs")


def test_complete_graph():
    """
    Test: Complete directed graph
    All vertices connect to all others
    """
    print("\n=== Test 10: Complete Directed Graph ===")

    n = 4
    edges = [(i, j) for i in range(n) for j in range(n) if i != j]
    indptr, indices = create_directed_graph(edges, n)

    result = strongly_connected_components(indptr, indices, n)

    assert result.num_components() == 1, "Complete graph should be one SCC"
    assert result.largest_component_size() == n, "SCC should include all vertices"
    assert is_strongly_connected(indptr, indices, n), "Should be strongly connected"

    print(f"✓ Complete graph is one SCC of size {n}")


def run_all_tests():
    """Run all SCC tests."""
    print("=" * 70)
    print("STRONGLY CONNECTED COMPONENTS TEST SUITE (Tarjan's Algorithm)")
    print("=" * 70)

    tests = [
        test_single_cycle,
        test_multiple_sccs,
        test_dag,
        test_source_sink_sccs,
        test_largest_scc,
        test_complex_graph,
        test_self_loops,
        test_scc_membership,
        test_empty_graph,
        test_complete_graph,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"\n❌ {test.__name__} FAILED: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("\n" + "=" * 70)
    print(f"RESULTS: {passed} passed, {failed} failed out of {len(tests)} tests")
    print("=" * 70)

    if failed == 0:
        print("\n✅ ALL TESTS PASSED!")
    else:
        print(f"\n❌ {failed} TEST(S) FAILED")

    return failed == 0


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
