"""
Test suite for weakly connected components.

Weakly connected components treat directed graphs as undirected
by ignoring edge direction.
"""

import sys
sys.path.insert(0, '/Users/bengamble/Sabot')

import pyarrow as pa
from sabot._cython.graph.storage.graph_storage import PyPropertyGraph
from sabot._cython.graph.traversal.connected_components import (
    weakly_connected_components,
    connected_components
)


def create_directed_graph(edges):
    """Helper to create directed graph from edge list."""
    # Count vertices
    vertices = set()
    for u, v in edges:
        vertices.add(u)
        vertices.add(v)
    num_vertices = max(vertices) + 1

    # Build CSR
    indptr_list = [0]
    indices_list = []

    for u in range(num_vertices):
        neighbors = [v for (src, v) in edges if src == u]
        indices_list.extend(neighbors)
        indptr_list.append(len(indices_list))

    indptr = pa.array(indptr_list, type=pa.int64())
    indices = pa.array(indices_list, type=pa.int64())

    return indptr, indices, num_vertices


def test_single_weakly_connected_component():
    """
    Test: Directed path 0 -> 1 -> 2 -> 3
    Should find 1 weakly connected component: {0, 1, 2, 3}
    """
    print("\n=== Test 1: Single Weakly Connected Component ===")

    edges = [(0, 1), (1, 2), (2, 3)]
    indptr, indices, num_vertices = create_directed_graph(edges)

    result = weakly_connected_components(indptr, indices, num_vertices)

    assert result.num_components() == 1, "Should have 1 weakly connected component"
    assert result.largest_component_size() == 4, "Largest component should have 4 vertices"

    component_ids = result.component_ids().to_pylist()
    assert len(set(component_ids)) == 1, "All vertices should be in same component"

    print(f"✓ Found {result.num_components()} weakly connected component")
    print(f"✓ Component IDs: {component_ids}")
    print(f"✓ Largest component size: {result.largest_component_size()}")


def test_two_weakly_connected_components():
    """
    Test: Two directed paths
    0 -> 1 -> 2
    3 -> 4
    Should find 2 weakly connected components
    """
    print("\n=== Test 2: Two Weakly Connected Components ===")

    edges = [(0, 1), (1, 2), (3, 4)]
    indptr, indices, num_vertices = create_directed_graph(edges)

    result = weakly_connected_components(indptr, indices, num_vertices)

    assert result.num_components() == 2, "Should have 2 weakly connected components"

    component_ids = result.component_ids().to_pylist()
    sizes = result.component_sizes().to_pylist()

    assert 3 in sizes, "Should have component of size 3"
    assert 2 in sizes, "Should have component of size 2"

    print(f"✓ Found {result.num_components()} weakly connected components")
    print(f"✓ Component IDs: {component_ids}")
    print(f"✓ Component sizes: {sizes}")


def test_directed_cycle_weakly_connected():
    """
    Test: Directed cycle 0 -> 1 -> 2 -> 0
    Should find 1 weakly connected component
    """
    print("\n=== Test 3: Directed Cycle (Weakly Connected) ===")

    edges = [(0, 1), (1, 2), (2, 0)]
    indptr, indices, num_vertices = create_directed_graph(edges)

    result = weakly_connected_components(indptr, indices, num_vertices)

    assert result.num_components() == 1, "Cycle should be weakly connected"
    assert result.largest_component_size() == 3, "Should include all 3 vertices"

    print(f"✓ Found {result.num_components()} weakly connected component")
    print(f"✓ Largest component: {result.largest_component_size()} vertices")


def test_weakly_vs_strongly_connected():
    """
    Test: Directed edges 0 -> 1 -> 2, 2 -> 0 (strongly connected)
          Plus 3 -> 4 (not strongly connected)

    Weakly: Should find 2 components {0,1,2} and {3,4}
    """
    print("\n=== Test 4: Weakly vs Strongly Connected ===")

    edges = [(0, 1), (1, 2), (2, 0), (3, 4)]
    indptr, indices, num_vertices = create_directed_graph(edges)

    result = weakly_connected_components(indptr, indices, num_vertices)

    assert result.num_components() == 2, "Should have 2 weakly connected components"

    component_ids = result.component_ids().to_pylist()

    # Vertices 0, 1, 2 should be in same component
    assert component_ids[0] == component_ids[1], "0 and 1 should be in same component"
    assert component_ids[1] == component_ids[2], "1 and 2 should be in same component"

    # Vertices 3, 4 should be in same component (different from 0,1,2)
    assert component_ids[3] == component_ids[4], "3 and 4 should be in same component"
    assert component_ids[0] != component_ids[3], "Components should be different"

    print(f"✓ Found {result.num_components()} weakly connected components")
    print(f"✓ Component IDs: {component_ids}")


def test_isolated_vertices_directed():
    """
    Test: Directed edges with isolated vertices
    0 -> 1
    2 (isolated)
    3 (isolated)
    Should find 3 weakly connected components
    """
    print("\n=== Test 5: Isolated Vertices (Directed) ===")

    edges = [(0, 1)]
    num_vertices = 4

    indptr_list = [0, 1, 1, 1, 1]  # 0 has 1 neighbor, others have 0
    indices_list = [1]

    indptr = pa.array(indptr_list, type=pa.int64())
    indices = pa.array(indices_list, type=pa.int64())

    result = weakly_connected_components(indptr, indices, num_vertices)

    assert result.num_components() == 3, "Should have 3 components"

    sizes = result.component_sizes().to_pylist()
    isolated_count = sum(1 for s in sizes if s == 1)
    assert isolated_count == 2, "Should have 2 isolated vertices"

    print(f"✓ Found {result.num_components()} weakly connected components")
    print(f"✓ Component sizes: {sizes}")
    print(f"✓ Isolated vertices: {isolated_count}")


def test_star_graph_directed():
    """
    Test: Directed star (all edges point to center)
    1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0
    Should be 1 weakly connected component
    """
    print("\n=== Test 6: Directed Star (All Point to Center) ===")

    edges = [(1, 0), (2, 0), (3, 0), (4, 0)]
    indptr, indices, num_vertices = create_directed_graph(edges)

    result = weakly_connected_components(indptr, indices, num_vertices)

    assert result.num_components() == 1, "Star should be weakly connected"
    assert result.largest_component_size() == 5, "Should include all 5 vertices"

    print(f"✓ Found {result.num_components()} weakly connected component")
    print(f"✓ All {num_vertices} vertices are weakly connected")


def test_directed_tree():
    """
    Test: Directed tree (parent -> children)
          0
         / \
        1   2
       / \
      3   4
    Edges: 0->1, 0->2, 1->3, 1->4
    Should be 1 weakly connected component
    """
    print("\n=== Test 7: Directed Tree ===")

    edges = [(0, 1), (0, 2), (1, 3), (1, 4)]
    indptr, indices, num_vertices = create_directed_graph(edges)

    result = weakly_connected_components(indptr, indices, num_vertices)

    assert result.num_components() == 1, "Tree should be weakly connected"
    assert result.largest_component_size() == 5, "Should include all 5 vertices"

    print(f"✓ Found {result.num_components()} weakly connected component")
    print(f"✓ Tree with {num_vertices} vertices is weakly connected")


def test_bidirectional_edges():
    """
    Test: Some edges bidirectional, some unidirectional
    0 <-> 1, 1 -> 2, 3 <-> 4
    Should find 2 weakly connected components
    """
    print("\n=== Test 8: Mixed Bidirectional/Unidirectional ===")

    edges = [
        (0, 1), (1, 0),  # 0 <-> 1
        (1, 2),          # 1 -> 2
        (3, 4), (4, 3)   # 3 <-> 4
    ]
    indptr, indices, num_vertices = create_directed_graph(edges)

    result = weakly_connected_components(indptr, indices, num_vertices)

    assert result.num_components() == 2, "Should have 2 weakly connected components"

    component_ids = result.component_ids().to_pylist()

    # 0, 1, 2 should be in same component
    assert component_ids[0] == component_ids[1] == component_ids[2]

    # 3, 4 should be in same component (different from above)
    assert component_ids[3] == component_ids[4]
    assert component_ids[0] != component_ids[3]

    print(f"✓ Found {result.num_components()} weakly connected components")
    print(f"✓ Component IDs: {component_ids}")


def test_long_directed_chain():
    """
    Test: Long directed chain
    0 -> 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7 -> 8 -> 9
    Should be 1 weakly connected component
    """
    print("\n=== Test 9: Long Directed Chain ===")

    edges = [(i, i+1) for i in range(9)]
    indptr, indices, num_vertices = create_directed_graph(edges)

    result = weakly_connected_components(indptr, indices, num_vertices)

    assert result.num_components() == 1, "Chain should be weakly connected"
    assert result.largest_component_size() == 10, "Should include all 10 vertices"

    print(f"✓ Found {result.num_components()} weakly connected component")
    print(f"✓ Chain of {num_vertices} vertices is weakly connected")


def test_comparison_directed_vs_undirected():
    """
    Test: Compare regular connected_components with weakly_connected_components
    on the same graph.

    For undirected graph, both should give same result.
    """
    print("\n=== Test 10: Comparison - Directed Input ===")

    # Directed graph: 0 -> 1 -> 2
    edges = [(0, 1), (1, 2)]
    indptr, indices, num_vertices = create_directed_graph(edges)

    # Try regular connected components (will see as disconnected)
    regular_result = connected_components(indptr, indices, num_vertices)

    # Try weakly connected components (will symmetrize edges)
    weakly_result = weakly_connected_components(indptr, indices, num_vertices)

    print(f"  Regular CC: {regular_result.num_components()} components")
    print(f"  Weakly CC:  {weakly_result.num_components()} components")

    # Regular CC will find more components (edges only go one direction)
    assert regular_result.num_components() >= weakly_result.num_components(), \
        "Weakly CC should never have more components than regular CC"

    # Weakly CC should find 1 component
    assert weakly_result.num_components() == 1, "Should be 1 weakly connected component"

    print(f"✓ Weakly CC correctly treats directed graph as undirected")


def run_all_tests():
    """Run all weakly connected components tests."""
    print("=" * 70)
    print("WEAKLY CONNECTED COMPONENTS TEST SUITE")
    print("=" * 70)

    tests = [
        test_single_weakly_connected_component,
        test_two_weakly_connected_components,
        test_directed_cycle_weakly_connected,
        test_weakly_vs_strongly_connected,
        test_isolated_vertices_directed,
        test_star_graph_directed,
        test_directed_tree,
        test_bidirectional_edges,
        test_long_directed_chain,
        test_comparison_directed_vs_undirected
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
