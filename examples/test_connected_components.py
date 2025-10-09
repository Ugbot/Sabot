"""
Connected Components Test Suite

Comprehensive tests for connected component finding algorithms.
"""

import pyarrow as pa
from sabot._cython.graph.storage.graph_storage import PyPropertyGraph, PyVertexTable, PyEdgeTable
from sabot._cython.graph.traversal.connected_components import (
    connected_components, connected_components_bfs, largest_component,
    component_statistics, count_isolated_vertices
)


def test_single_component():
    """Test graph with single connected component."""
    print("Test 1: Single Connected Component")
    print("-" * 50)

    # Triangle graph - all vertices connected
    vertices = pa.table({
        'id': [0, 1, 2],
        'label': pa.array(["Vertex"] * 3).dictionary_encode(),
    })

    edges = pa.table({
        'src': [0, 1, 1, 2, 2, 0],
        'dst': [1, 0, 2, 1, 0, 2],
        'type': pa.array(["Edge"] * 6).dictionary_encode(),
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    result = connected_components(csr.indptr, csr.indices, graph.num_vertices())

    print(f"Number of components: {result.num_components()}")
    print(f"Component IDs: {result.component_ids().to_pylist()}")

    assert result.num_components() == 1, "Should have 1 component"
    assert all(id == 0 for id in result.component_ids().to_pylist())

    print("✅ PASSED\n")


def test_two_components():
    """Test graph with two disconnected components."""
    print("Test 2: Two Disconnected Components")
    print("-" * 50)

    # Two triangles: {0,1,2} and {3,4,5}
    vertices = pa.table({
        'id': list(range(6)),
        'label': pa.array(["Vertex"] * 6).dictionary_encode(),
    })

    edges = pa.table({
        'src': [0, 1, 0, 2, 1, 2, 3, 4, 3, 5, 4, 5],
        'dst': [1, 0, 2, 0, 2, 1, 4, 3, 5, 3, 5, 4],
        'type': pa.array(["Edge"] * 12).dictionary_encode(),
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    result = connected_components(csr.indptr, csr.indices, graph.num_vertices())

    print(f"Number of components: {result.num_components()}")
    component_ids = result.component_ids().to_pylist()
    print(f"Component IDs: {component_ids}")

    assert result.num_components() == 2, "Should have 2 components"

    # Check that 0,1,2 are in one component
    assert component_ids[0] == component_ids[1] == component_ids[2]

    # Check that 3,4,5 are in another component
    assert component_ids[3] == component_ids[4] == component_ids[5]

    # Check that the two components are different
    assert component_ids[0] != component_ids[3]

    print("✅ PASSED\n")


def test_isolated_vertices():
    """Test graph with isolated vertices."""
    print("Test 3: Isolated Vertices")
    print("-" * 50)

    # 3 connected + 2 isolated
    vertices = pa.table({
        'id': list(range(5)),
        'label': pa.array(["Vertex"] * 5).dictionary_encode(),
    })

    # Only 0-1-2 connected
    edges = pa.table({
        'src': [0, 1, 0, 2, 1, 2],
        'dst': [1, 0, 2, 0, 2, 1],
        'type': pa.array(["Edge"] * 6).dictionary_encode(),
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    result = connected_components(csr.indptr, csr.indices, graph.num_vertices())

    print(f"Number of components: {result.num_components()}")
    print(f"Component sizes: {result.component_sizes().to_pylist()}")

    isolated_count = count_isolated_vertices(result.component_sizes())
    print(f"Isolated vertices: {isolated_count}")

    assert result.num_components() == 3, "Should have 3 components"
    assert isolated_count == 2, "Should have 2 isolated vertices"

    print("✅ PASSED\n")


def test_largest_component_extraction():
    """Test extracting the largest component."""
    print("Test 4: Largest Component Extraction")
    print("-" * 50)

    # Two components: size 4 and size 2
    vertices = pa.table({
        'id': list(range(6)),
        'label': pa.array(["Vertex"] * 6).dictionary_encode(),
    })

    # Component 1: 0,1,2,3 (size 4)
    # Component 2: 4,5 (size 2)
    edges = pa.table({
        'src': [0, 1, 0, 2, 1, 2, 1, 3, 2, 3, 4, 5],
        'dst': [1, 0, 2, 0, 2, 1, 3, 1, 3, 2, 5, 4],
        'type': pa.array(["Edge"] * 12).dictionary_encode(),
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    result = connected_components(csr.indptr, csr.indices, graph.num_vertices())

    largest = largest_component(result.component_ids(), result.component_sizes())

    print(f"Largest component size: {len(largest)}")
    print(f"Largest component vertices: {largest.to_pylist()}")

    assert len(largest) == 4, "Largest component should have 4 vertices"
    assert result.largest_component_size() == 4

    print("✅ PASSED\n")


def test_component_statistics():
    """Test component statistics."""
    print("Test 5: Component Statistics")
    print("-" * 50)

    # 3 components: size 3, 2, 1
    vertices = pa.table({
        'id': list(range(6)),
        'label': pa.array(["Vertex"] * 6).dictionary_encode(),
    })

    # Component 0: {0,1,2}, Component 1: {3,4}, Component 2: {5}
    edges = pa.table({
        'src': [0, 1, 0, 2, 1, 2, 3, 4],
        'dst': [1, 0, 2, 0, 2, 1, 4, 3],
        'type': pa.array(["Edge"] * 8).dictionary_encode(),
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    result = connected_components(csr.indptr, csr.indices, graph.num_vertices())

    stats = component_statistics(result.component_ids(), result.num_components())

    print("Component statistics:")
    for i in range(len(stats)):
        comp_id = stats['component_id'][i].as_py()
        size = stats['size'][i].as_py()
        isolated = stats['isolated'][i].as_py()
        print(f"  Component {comp_id}: size={size}, isolated={isolated}")

    # Check that one component is isolated
    isolated_count = sum(1 for i in range(len(stats)) if stats['isolated'][i].as_py())
    assert isolated_count == 1, "Should have 1 isolated component"

    print("✅ PASSED\n")


def test_union_find_vs_bfs():
    """Test that union-find and BFS give same results."""
    print("Test 6: Union-Find vs BFS Comparison")
    print("-" * 50)

    # Random graph
    vertices = pa.table({
        'id': list(range(8)),
        'label': pa.array(["Vertex"] * 8).dictionary_encode(),
    })

    # 3 components: {0,1,2}, {3,4,5}, {6,7}
    edges = pa.table({
        'src': [0, 1, 1, 2, 3, 4, 4, 5, 6, 7],
        'dst': [1, 0, 2, 1, 4, 3, 5, 4, 7, 6],
        'type': pa.array(["Edge"] * 10).dictionary_encode(),
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    result_uf = connected_components(csr.indptr, csr.indices, graph.num_vertices())
    result_bfs = connected_components_bfs(csr.indptr, csr.indices, graph.num_vertices())

    print(f"Union-Find components: {result_uf.num_components()}")
    print(f"BFS components: {result_bfs.num_components()}")

    assert result_uf.num_components() == result_bfs.num_components()

    # Component assignments might differ in numbering, but groupings should be same
    uf_groups = {}
    bfs_groups = {}

    for i in range(graph.num_vertices()):
        uf_id = result_uf.component_ids()[i].as_py()
        bfs_id = result_bfs.component_ids()[i].as_py()

        if uf_id not in uf_groups:
            uf_groups[uf_id] = []
        if bfs_id not in bfs_groups:
            bfs_groups[bfs_id] = []

        uf_groups[uf_id].append(i)
        bfs_groups[bfs_id].append(i)

    # Sort groupings
    uf_sorted = sorted([sorted(group) for group in uf_groups.values()])
    bfs_sorted = sorted([sorted(group) for group in bfs_groups.values()])

    print(f"Union-Find groupings: {uf_sorted}")
    print(f"BFS groupings: {bfs_sorted}")

    assert uf_sorted == bfs_sorted, "Both methods should produce same groupings"

    print("✅ PASSED\n")


def test_single_vertex():
    """Test graph with single vertex."""
    print("Test 7: Single Vertex Graph")
    print("-" * 50)

    vertices = pa.table({
        'id': [0],
        'label': pa.array(["Vertex"]).dictionary_encode(),
    })

    edges = pa.table({
        'src': [],
        'dst': [],
        'type': pa.array([]).dictionary_encode(),
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    result = connected_components(csr.indptr, csr.indices, graph.num_vertices())

    print(f"Number of components: {result.num_components()}")

    assert result.num_components() == 1
    assert result.largest_component_size() == 1

    print("✅ PASSED\n")


def test_chain_graph():
    """Test linear chain graph."""
    print("Test 8: Linear Chain Graph")
    print("-" * 50)

    # Chain: 0-1-2-3-4
    vertices = pa.table({
        'id': list(range(5)),
        'label': pa.array(["Vertex"] * 5).dictionary_encode(),
    })

    edges = pa.table({
        'src': [0, 1, 1, 2, 2, 3, 3, 4],
        'dst': [1, 0, 2, 1, 3, 2, 4, 3],
        'type': pa.array(["Edge"] * 8).dictionary_encode(),
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    result = connected_components(csr.indptr, csr.indices, graph.num_vertices())

    print(f"Number of components: {result.num_components()}")

    assert result.num_components() == 1, "Chain should be single component"

    print("✅ PASSED\n")


def test_star_graph():
    """Test star graph (hub and spokes)."""
    print("Test 9: Star Graph")
    print("-" * 50)

    # Center 0 connected to 1,2,3,4
    vertices = pa.table({
        'id': list(range(5)),
        'label': pa.array(["Vertex"] * 5).dictionary_encode(),
    })

    edges = pa.table({
        'src': [0, 1, 0, 2, 0, 3, 0, 4],
        'dst': [1, 0, 2, 0, 3, 0, 4, 0],
        'type': pa.array(["Edge"] * 8).dictionary_encode(),
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    result = connected_components(csr.indptr, csr.indices, graph.num_vertices())

    print(f"Number of components: {result.num_components()}")

    assert result.num_components() == 1, "Star should be single component"

    print("✅ PASSED\n")


def test_complete_graph():
    """Test complete graph K5."""
    print("Test 10: Complete Graph K5")
    print("-" * 50)

    vertices = pa.table({
        'id': list(range(5)),
        'label': pa.array(["Vertex"] * 5).dictionary_encode(),
    })

    # All pairs connected
    edges = pa.table({
        'src': [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4],
        'dst': [1, 2, 3, 4, 0, 2, 3, 4, 0, 1, 3, 4, 0, 1, 2, 4, 0, 1, 2, 3],
        'type': pa.array(["Edge"] * 20).dictionary_encode(),
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    result = connected_components(csr.indptr, csr.indices, graph.num_vertices())

    print(f"Number of components: {result.num_components()}")

    assert result.num_components() == 1, "Complete graph should be single component"
    assert result.largest_component_size() == 5

    print("✅ PASSED\n")


def main():
    print("\n" + "=" * 50)
    print("CONNECTED COMPONENTS TEST SUITE")
    print("=" * 50 + "\n")

    test_single_component()
    test_two_components()
    test_isolated_vertices()
    test_largest_component_extraction()
    test_component_statistics()
    test_union_find_vs_bfs()
    test_single_vertex()
    test_chain_graph()
    test_star_graph()
    test_complete_graph()

    print("=" * 50)
    print("✅ ALL 10 TESTS PASSED!")
    print("=" * 50)


if __name__ == "__main__":
    main()
