"""
Triangle Counting Test Suite

Comprehensive tests for triangle counting and clustering coefficient algorithms.
"""

import pyarrow as pa
from sabot._cython.graph.storage.graph_storage import PyPropertyGraph, PyVertexTable, PyEdgeTable
from sabot._cython.graph.traversal.triangle_counting import (
    count_triangles, count_triangles_weighted, top_k_triangle_counts,
    compute_transitivity
)


def test_triangle_graph():
    """Test basic triangle (3-clique) counting."""
    print("Test 1: Basic Triangle Graph")
    print("-" * 50)

    # Create simple triangle: 0-1-2-0
    vertices = pa.table({
        'id': [0, 1, 2],
        'label': pa.array(["Vertex"] * 3).dictionary_encode(),
    })

    # Undirected edges (both directions)
    edges = pa.table({
        'src': [0, 1, 1, 2, 2, 0],
        'dst': [1, 0, 2, 1, 0, 2],
        'type': pa.array(["Edge"] * 6).dictionary_encode(),
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    result = count_triangles(csr.indptr, csr.indices, graph.num_vertices())

    print(f"Total triangles: {result.total_triangles()}")
    print(f"Per-vertex triangles: {result.per_vertex_triangles().to_pylist()}")
    print(f"Clustering coefficients: {result.clustering_coefficient().to_pylist()}")
    print(f"Global clustering: {result.global_clustering_coefficient():.4f}")

    assert result.total_triangles() == 1, "Should find exactly 1 triangle"
    # Each vertex participates in 1 triangle
    assert result.per_vertex_triangles().to_pylist() == [1, 1, 1]
    # Perfect clustering (all neighbors are connected)
    assert all(c == 1.0 for c in result.clustering_coefficient().to_pylist())

    print("✅ PASSED\n")


def test_two_triangles():
    """Test graph with two triangles sharing an edge."""
    print("Test 2: Two Triangles Sharing Edge")
    print("-" * 50)

    # Graph: 0-1-2-0 and 1-2-3-1 (triangles sharing edge 1-2)
    vertices = pa.table({
        'id': [0, 1, 2, 3],
        'label': pa.array(["Vertex"] * 4).dictionary_encode(),
    })

    # Edges: 0-1, 0-2, 1-2 (triangle 1), 1-3, 2-3 (triangle 2)
    edges = pa.table({
        'src': [0, 1, 0, 2, 1, 2, 1, 3, 2, 3],
        'dst': [1, 0, 2, 0, 2, 1, 3, 1, 3, 2],
        'type': pa.array(["Edge"] * 10).dictionary_encode(),
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    result = count_triangles(csr.indptr, csr.indices, graph.num_vertices())

    print(f"Total triangles: {result.total_triangles()}")
    print(f"Per-vertex triangles: {result.per_vertex_triangles().to_pylist()}")

    assert result.total_triangles() == 2, "Should find 2 triangles"
    # Vertices 1 and 2 participate in both triangles
    triangle_counts = result.per_vertex_triangles().to_pylist()
    assert triangle_counts[0] == 1  # vertex 0 in one triangle
    assert triangle_counts[1] == 2  # vertex 1 in two triangles
    assert triangle_counts[2] == 2  # vertex 2 in two triangles
    assert triangle_counts[3] == 1  # vertex 3 in one triangle

    print("✅ PASSED\n")


def test_complete_graph():
    """Test complete graph K4 (all vertices connected)."""
    print("Test 3: Complete Graph K4")
    print("-" * 50)

    # K4 has 4 triangles
    vertices = pa.table({
        'id': [0, 1, 2, 3],
        'label': pa.array(["Vertex"] * 4).dictionary_encode(),
    })

    # All pairs connected (undirected)
    edges = pa.table({
        'src': [0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3],
        'dst': [1, 2, 3, 0, 2, 3, 0, 1, 3, 0, 1, 2],
        'type': pa.array(["Edge"] * 12).dictionary_encode(),
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    result = count_triangles(csr.indptr, csr.indices, graph.num_vertices())

    print(f"Total triangles: {result.total_triangles()}")
    print(f"Per-vertex triangles: {result.per_vertex_triangles().to_pylist()}")
    print(f"Global clustering: {result.global_clustering_coefficient():.4f}")

    # K4 has C(4,3) = 4 triangles
    assert result.total_triangles() == 4, "K4 should have 4 triangles"

    # Each vertex is in C(3,2) = 3 triangles
    assert all(count == 3 for count in result.per_vertex_triangles().to_pylist())

    # Perfect clustering (all neighbors are connected)
    assert result.global_clustering_coefficient() == 1.0

    print("✅ PASSED\n")


def test_star_graph():
    """Test star graph (no triangles)."""
    print("Test 4: Star Graph (No Triangles)")
    print("-" * 50)

    # Center vertex connected to 4 leaves, no triangles
    vertices = pa.table({
        'id': [0, 1, 2, 3, 4],
        'label': pa.array(["Vertex"] * 5).dictionary_encode(),
    })

    # Star: 0 is center, 1-4 are leaves
    edges = pa.table({
        'src': [0, 0, 0, 0, 1, 2, 3, 4],
        'dst': [1, 2, 3, 4, 0, 0, 0, 0],
        'type': pa.array(["Edge"] * 8).dictionary_encode(),
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    result = count_triangles(csr.indptr, csr.indices, graph.num_vertices())

    print(f"Total triangles: {result.total_triangles()}")
    print(f"Clustering coefficients: {result.clustering_coefficient().to_pylist()}")

    assert result.total_triangles() == 0, "Star graph has no triangles"

    # Center has degree 4 but clustering 0 (no triangles)
    clustering = result.clustering_coefficient().to_pylist()
    assert clustering[0] == 0.0, "Center should have 0 clustering"

    # Leaves have degree 1, clustering undefined (0.0)
    assert all(c == 0.0 for c in clustering[1:])

    print("✅ PASSED\n")


def test_weighted_triangle_counting():
    """Test weighted triangle counting (topology only)."""
    print("Test 5: Weighted Triangle Counting")
    print("-" * 50)

    # Triangle with weights (topology only)
    vertices = pa.table({
        'id': [0, 1, 2],
        'label': pa.array(["Vertex"] * 3).dictionary_encode(),
    })

    edges = pa.table({
        'src': [0, 1, 1, 2, 2, 0],
        'dst': [1, 0, 2, 1, 0, 2],
        'type': pa.array(["Edge"] * 6).dictionary_encode(),
        'weight': [1.0, 1.0, 2.0, 2.0, 0.5, 0.5],
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    # Extract weights (convert ChunkedArray to Array)
    weights_chunked = edges.column('weight')
    weights = pa.concat_arrays([chunk for chunk in weights_chunked.chunks])

    result = count_triangles_weighted(
        csr.indptr, csr.indices, weights, graph.num_vertices()
    )

    print(f"Total triangles: {result.total_triangles()}")

    # Topology unchanged, weights ignored
    assert result.total_triangles() == 1

    print("✅ PASSED\n")


def test_top_k_triangle_counts():
    """Test top-K triangle count extraction."""
    print("Test 6: Top-K Triangle Counts")
    print("-" * 50)

    # Graph with varying triangle participation
    vertices = pa.table({
        'id': list(range(6)),
        'label': pa.array(["Vertex"] * 6).dictionary_encode(),
    })

    # Create 3 triangles: (0,1,2), (1,2,3), (2,3,4)
    edges = pa.table({
        'src': [0, 1, 0, 2, 1, 2,  # triangle 1
                1, 3, 2, 3,        # triangle 2 (shares 1-2)
                2, 4, 3, 4],       # triangle 3 (shares 2-3)
        'dst': [1, 0, 2, 0, 2, 1,
                3, 1, 3, 2,
                4, 2, 4, 3],
        'type': pa.array(["Edge"] * 14).dictionary_encode(),
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    result = count_triangles(csr.indptr, csr.indices, graph.num_vertices())

    # Get top 3 by triangle count
    top_3 = top_k_triangle_counts(
        result.per_vertex_triangles(),
        result.clustering_coefficient(),
        k=3
    )

    print("Top 3 vertices by triangle count:")
    for i in range(len(top_3)):
        vertex_id = top_3['vertex_id'][i].as_py()
        count = top_3['triangle_count'][i].as_py()
        clustering = top_3['clustering_coefficient'][i].as_py()
        print(f"  Vertex {vertex_id}: {count} triangles, clustering={clustering:.4f}")

    # Vertex 2 participates in all 3 triangles
    assert top_3['vertex_id'][0].as_py() == 2
    assert top_3['triangle_count'][0].as_py() == 3

    print("✅ PASSED\n")


def test_transitivity():
    """Test transitivity (global clustering) computation."""
    print("Test 7: Transitivity Computation")
    print("-" * 50)

    # Complete graph K4 has transitivity 1.0
    vertices = pa.table({
        'id': [0, 1, 2, 3],
        'label': pa.array(["Vertex"] * 4).dictionary_encode(),
    })

    edges = pa.table({
        'src': [0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3],
        'dst': [1, 2, 3, 0, 2, 3, 0, 1, 3, 0, 1, 2],
        'type': pa.array(["Edge"] * 12).dictionary_encode(),
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    transitivity = compute_transitivity(csr.indptr, csr.indices, graph.num_vertices())

    print(f"Transitivity: {transitivity:.4f}")

    # K4: all triples close into triangles
    assert transitivity == 1.0, "K4 should have transitivity 1.0"

    print("✅ PASSED\n")


def test_isolated_vertices():
    """Test graph with isolated vertices (no edges)."""
    print("Test 8: Isolated Vertices")
    print("-" * 50)

    # 3 connected vertices + 2 isolated
    vertices = pa.table({
        'id': [0, 1, 2, 3, 4],
        'label': pa.array(["Vertex"] * 5).dictionary_encode(),
    })

    # Triangle on 0,1,2; vertices 3,4 isolated
    edges = pa.table({
        'src': [0, 1, 1, 2, 2, 0],
        'dst': [1, 0, 2, 1, 0, 2],
        'type': pa.array(["Edge"] * 6).dictionary_encode(),
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    result = count_triangles(csr.indptr, csr.indices, graph.num_vertices())

    print(f"Total triangles: {result.total_triangles()}")
    triangles = result.per_vertex_triangles().to_pylist()
    print(f"Per-vertex triangles: {triangles}")

    assert result.total_triangles() == 1
    assert triangles[0] == triangles[1] == triangles[2] == 1
    assert triangles[3] == triangles[4] == 0  # Isolated vertices

    print("✅ PASSED\n")


def test_chain_graph():
    """Test linear chain (no triangles)."""
    print("Test 9: Linear Chain Graph")
    print("-" * 50)

    # Chain: 0-1-2-3-4
    vertices = pa.table({
        'id': [0, 1, 2, 3, 4],
        'label': pa.array(["Vertex"] * 5).dictionary_encode(),
    })

    edges = pa.table({
        'src': [0, 1, 1, 2, 2, 3, 3, 4],
        'dst': [1, 0, 2, 1, 3, 2, 4, 3],
        'type': pa.array(["Edge"] * 8).dictionary_encode(),
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    result = count_triangles(csr.indptr, csr.indices, graph.num_vertices())

    print(f"Total triangles: {result.total_triangles()}")
    print(f"Clustering coefficients: {result.clustering_coefficient().to_pylist()}")

    assert result.total_triangles() == 0, "Chain has no triangles"

    # All vertices have 0 clustering
    clustering = result.clustering_coefficient().to_pylist()
    assert all(c == 0.0 for c in clustering)

    print("✅ PASSED\n")


def test_social_network_clustering():
    """Test realistic social network with communities."""
    print("Test 10: Social Network Clustering")
    print("-" * 50)

    # Two tight communities (triangles) connected by a bridge
    vertices = pa.table({
        'id': list(range(7)),
        'label': pa.array(["Person"] * 7).dictionary_encode(),
        'name': ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace"],
    })

    # Community 1: Alice, Bob, Carol (triangle)
    # Community 2: Eve, Frank, Grace (triangle)
    # Bridge: Dave connects both communities
    edges = pa.table({
        'src': [0, 1, 0, 2, 1, 2,  # Community 1 triangle
                4, 5, 4, 6, 5, 6,  # Community 2 triangle
                3, 0, 3, 4],       # Dave bridges both
        'dst': [1, 0, 2, 0, 2, 1,
                5, 4, 6, 4, 6, 5,
                0, 3, 4, 3],
        'type': pa.array(["Friend"] * 16).dictionary_encode(),
    })

    graph = PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges))
    csr = graph.csr()

    result = count_triangles(csr.indptr, csr.indices, graph.num_vertices())

    print(f"Total triangles: {result.total_triangles()}")

    triangles = result.per_vertex_triangles().to_pylist()
    clustering = result.clustering_coefficient().to_pylist()

    print("\nPer-person analysis:")
    for i in range(graph.num_vertices()):
        name = vertices['name'][i].as_py()
        print(f"  {name}: {triangles[i]} triangles, clustering={clustering[i]:.4f}")

    # Should find 2 triangles (one per community)
    assert result.total_triangles() == 2

    # Communities have high clustering
    assert clustering[0] > 0.9  # Alice
    assert clustering[4] > 0.9  # Eve

    # Bridge has low clustering (connects non-connected groups)
    assert clustering[3] == 0.0  # Dave (bridge)

    print("✅ PASSED\n")


def main():
    print("\n" + "=" * 50)
    print("TRIANGLE COUNTING TEST SUITE")
    print("=" * 50 + "\n")

    test_triangle_graph()
    test_two_triangles()
    test_complete_graph()
    test_star_graph()
    test_weighted_triangle_counting()
    test_top_k_triangle_counts()
    test_transitivity()
    test_isolated_vertices()
    test_chain_graph()
    test_social_network_clustering()

    print("=" * 50)
    print("✅ ALL 10 TESTS PASSED!")
    print("=" * 50)


if __name__ == "__main__":
    main()
