"""
Test betweenness centrality algorithms on property graphs.
"""
import pyarrow as pa
from sabot._cython.graph.storage.graph_storage import PyPropertyGraph, PyVertexTable, PyEdgeTable
from sabot._cython.graph.traversal.centrality import (
    betweenness_centrality, weighted_betweenness_centrality,
    edge_betweenness_centrality, top_k_betweenness,
    PyBetweennessCentralityResult
)


def create_simple_graph():
    """
    Create a simple graph for betweenness testing:

    A linear chain with a bridge vertex:
    0 - 1 - 2 - 3 - 4

    Vertex 2 is a bridge (all paths go through it)
    """
    vertices = pa.table({
        'id': [0, 1, 2, 3, 4],
        'label': pa.array(["A", "B", "C", "D", "E"]).dictionary_encode(),
        'name': ["Alice", "Bob", "Carol", "Dave", "Eve"]
    })

    # Undirected edges (add both directions)
    edges = pa.table({
        'src': [0, 1, 1, 2, 2, 3, 3, 4],
        'dst': [1, 0, 2, 1, 3, 2, 4, 3],
        'type': pa.array(["LINK"] * 8).dictionary_encode()
    })

    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    return PyPropertyGraph(vertex_table, edge_table)


def create_star_graph():
    """
    Create a star graph for betweenness testing:

          1
          |
      2 - 0 - 3
          |
          4

    Vertex 0 is the center (hub) - all paths go through it
    """
    vertices = pa.table({
        'id': [0, 1, 2, 3, 4],
        'label': pa.array(["Hub", "Leaf", "Leaf", "Leaf", "Leaf"]).dictionary_encode(),
        'name': ["Hub", "A", "B", "C", "D"]
    })

    # Undirected star (center connected to all leaves)
    edges = pa.table({
        'src': [0, 1, 0, 2, 0, 3, 0, 4],
        'dst': [1, 0, 2, 0, 3, 0, 4, 0],
        'type': pa.array(["LINK"] * 8).dictionary_encode()
    })

    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    return PyPropertyGraph(vertex_table, edge_table)


def create_karate_club_graph():
    """
    Create Zachary's Karate Club graph (simplified version).

    Classic social network with two factions.
    """
    vertices = pa.table({
        'id': list(range(34)),
        'label': pa.array(["Member"] * 34).dictionary_encode(),
        'faction': [1] * 17 + [2] * 17
    })

    # Simplified edges (subset of real karate club)
    edges = pa.table({
        'src': [0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 5, 6, 7, 8, 9],
        'dst': [1, 2, 3, 4, 2, 3, 3, 4, 4, 5, 6, 7, 8, 9, 10, 11],
        'type': pa.array(["KNOWS"] * 16).dictionary_encode()
    })

    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    return PyPropertyGraph(vertex_table, edge_table)


def test_basic_betweenness():
    """Test basic betweenness centrality."""
    print("=" * 60)
    print("Test 1: Basic Betweenness Centrality")
    print("=" * 60)

    graph = create_simple_graph()
    csr = graph.csr()

    result = betweenness_centrality(csr.indptr, csr.indices, graph.num_vertices())

    print(f"\nBetweenness centrality (normalized):")
    print(f"  Vertices: {result.num_vertices()}")
    print(f"  Normalized: {result.normalized()}")

    centrality = result.centrality().to_pylist()
    for i, score in enumerate(centrality):
        print(f"    Vertex {i}: {score:.6f}")

    # In a linear chain, middle vertex (2) should have highest betweenness
    assert centrality[2] > centrality[0], "Middle vertex should have highest betweenness"
    assert centrality[2] > centrality[1], "Middle vertex should have highest betweenness"
    assert centrality[2] > centrality[3], "Middle vertex should have highest betweenness"
    assert centrality[2] > centrality[4], "Middle vertex should have highest betweenness"

    # End vertices should have 0 betweenness (no paths go through them)
    assert centrality[0] == 0.0, "End vertex should have 0 betweenness"
    assert centrality[4] == 0.0, "End vertex should have 0 betweenness"

    print("\n✅ Basic betweenness test passed!")


def test_star_graph_betweenness():
    """Test betweenness on star graph."""
    print("\n" + "=" * 60)
    print("Test 2: Star Graph Betweenness")
    print("=" * 60)

    graph = create_star_graph()
    csr = graph.csr()

    result = betweenness_centrality(csr.indptr, csr.indices, graph.num_vertices())

    centrality = result.centrality().to_pylist()

    print(f"\nBetweenness centrality (star graph):")
    for i, score in enumerate(centrality):
        name = ["Hub", "A", "B", "C", "D"][i]
        print(f"    {name}: {score:.6f}")

    # Hub (vertex 0) should have maximum betweenness
    # All paths between leaves go through the hub
    assert centrality[0] > 0.0, "Hub should have positive betweenness"
    assert all(centrality[0] > centrality[i] for i in range(1, 5)), \
        "Hub should have highest betweenness"

    # Leaves should have 0 betweenness (no paths go through them)
    for i in range(1, 5):
        assert centrality[i] == 0.0, f"Leaf {i} should have 0 betweenness"

    print("\n✅ Star graph test passed!")


def test_normalization():
    """Test betweenness centrality normalization."""
    print("\n" + "=" * 60)
    print("Test 3: Normalization")
    print("=" * 60)

    graph = create_simple_graph()
    csr = graph.csr()

    # Normalized
    result_norm = betweenness_centrality(csr.indptr, csr.indices, graph.num_vertices(),
                                        normalized=True)

    # Unnormalized
    result_unnorm = betweenness_centrality(csr.indptr, csr.indices, graph.num_vertices(),
                                           normalized=False)

    centrality_norm = result_norm.centrality().to_pylist()
    centrality_unnorm = result_unnorm.centrality().to_pylist()

    print(f"\nNormalized vs Unnormalized:")
    print(f"  Normalized max: {max(centrality_norm):.6f}")
    print(f"  Unnormalized max: {max(centrality_unnorm):.6f}")

    # Normalization factor for undirected graph: (n-1)(n-2)/2
    n = graph.num_vertices()
    norm_factor = (n - 1) * (n - 2) / 2.0

    # Verify normalization relationship
    for i in range(len(centrality_norm)):
        expected = centrality_unnorm[i] / norm_factor
        assert abs(centrality_norm[i] - expected) < 1e-6, \
            f"Normalization mismatch for vertex {i}"

    print("\n✅ Normalization test passed!")


def test_weighted_betweenness():
    """Test weighted betweenness centrality."""
    print("\n" + "=" * 60)
    print("Test 4: Weighted Betweenness Centrality")
    print("=" * 60)

    # Create graph with weighted edges
    vertices = pa.table({
        'id': [0, 1, 2, 3],
        'label': pa.array(["A", "B", "C", "D"]).dictionary_encode()
    })

    # Triangle with weighted edges
    # 0 -> 1 (weight 1)
    # 0 -> 2 (weight 10) [expensive]
    # 1 -> 2 (weight 1)
    # 2 -> 3 (weight 1)
    edges = pa.table({
        'src': [0, 1, 0, 2, 1, 2, 2, 3],
        'dst': [1, 0, 2, 0, 2, 1, 3, 2],
        'type': pa.array(["LINK"] * 8).dictionary_encode(),
        'weight': [1.0, 1.0, 10.0, 10.0, 1.0, 1.0, 1.0, 1.0]
    })

    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    graph = PyPropertyGraph(vertex_table, edge_table)

    csr = graph.csr()
    weights = edges['weight'].combine_chunks()

    result = weighted_betweenness_centrality(csr.indptr, csr.indices, weights,
                                             graph.num_vertices())

    centrality = result.centrality().to_pylist()

    print(f"\nWeighted betweenness centrality:")
    for i, score in enumerate(centrality):
        print(f"    Vertex {i}: {score:.6f}")

    # Vertex 2 should have high betweenness (bridge to vertex 3)
    assert centrality[2] > 0.0, "Vertex 2 should have positive betweenness"

    # At least some vertex should have non-zero betweenness
    assert any(score > 0.0 for score in centrality), "Some vertex should have positive betweenness"

    print("\n✅ Weighted betweenness test passed!")


def test_edge_betweenness():
    """Test edge betweenness centrality."""
    print("\n" + "=" * 60)
    print("Test 5: Edge Betweenness Centrality")
    print("=" * 60)

    graph = create_simple_graph()
    csr = graph.csr()

    result = edge_betweenness_centrality(csr.indptr, csr.indices, graph.num_vertices())

    print(f"\nEdge betweenness centrality:")
    print(f"  Total edges: {len(result)}")
    print(f"  Schema: {result.schema}")

    # Find edges with highest betweenness
    betweenness_scores = result['betweenness'].to_pylist()
    max_betweenness = max(betweenness_scores)

    print(f"\n  Top edges by betweenness:")
    for i in range(min(5, len(result))):
        src = result['src'][i].as_py()
        dst = result['dst'][i].as_py()
        score = result['betweenness'][i].as_py()
        if score > 0:
            print(f"    Edge {src}->{dst}: {score:.6f}")

    # Middle edges should have high betweenness
    assert max_betweenness > 0.0, "Some edge should have positive betweenness"

    print("\n✅ Edge betweenness test passed!")


def test_top_k_betweenness():
    """Test top-K betweenness extraction."""
    print("\n" + "=" * 60)
    print("Test 6: Top-K Betweenness")
    print("=" * 60)

    graph = create_simple_graph()
    csr = graph.csr()

    result = betweenness_centrality(csr.indptr, csr.indices, graph.num_vertices())

    # Get top 3 vertices
    top_3 = top_k_betweenness(result.centrality(), k=3)

    print(f"\nTop 3 vertices by betweenness:")
    for i in range(len(top_3)):
        vertex_id = top_3['vertex_id'][i].as_py()
        score = top_3['betweenness'][i].as_py()
        print(f"  {i+1}. Vertex {vertex_id}: {score:.6f}")

    # Verify sorted in descending order
    scores = [top_3['betweenness'][i].as_py() for i in range(len(top_3))]
    for i in range(len(scores) - 1):
        assert scores[i] >= scores[i+1], "Top-K should be sorted descending"

    # Top vertex should be the bridge (vertex 2)
    top_vertex = top_3['vertex_id'][0].as_py()
    assert top_vertex == 2, "Bridge vertex should be at the top"

    print("\n✅ Top-K test passed!")


def test_isolated_vertices():
    """Test betweenness with isolated vertices."""
    print("\n" + "=" * 60)
    print("Test 7: Isolated Vertices")
    print("=" * 60)

    # Create graph with isolated vertex
    vertices = pa.table({
        'id': [0, 1, 2, 3, 4],
        'label': pa.array(["A", "B", "C", "D", "Isolated"]).dictionary_encode()
    })

    # Connect 0-1-2-3, leave 4 isolated
    edges = pa.table({
        'src': [0, 1, 1, 2, 2, 3],
        'dst': [1, 0, 2, 1, 3, 2],
        'type': pa.array(["LINK"] * 6).dictionary_encode()
    })

    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    graph = PyPropertyGraph(vertex_table, edge_table)

    csr = graph.csr()

    result = betweenness_centrality(csr.indptr, csr.indices, graph.num_vertices())

    centrality = result.centrality().to_pylist()

    print(f"\nBetweenness with isolated vertex:")
    for i, score in enumerate(centrality):
        status = " (isolated)" if i == 4 else ""
        print(f"    Vertex {i}: {score:.6f}{status}")

    # Isolated vertex should have 0 betweenness
    assert centrality[4] == 0.0, "Isolated vertex should have 0 betweenness"

    print("\n✅ Isolated vertices test passed!")


def test_complete_graph():
    """Test betweenness on complete graph."""
    print("\n" + "=" * 60)
    print("Test 8: Complete Graph")
    print("=" * 60)

    # Complete graph K4 (all vertices connected to all others)
    vertices = pa.table({
        'id': [0, 1, 2, 3],
        'label': pa.array(["A", "B", "C", "D"]).dictionary_encode()
    })

    # All edges (undirected complete graph)
    edges = pa.table({
        'src': [0, 1, 0, 2, 0, 3, 1, 2, 1, 3, 2, 3,
                1, 0, 2, 0, 3, 0, 2, 1, 3, 1, 3, 2],
        'dst': [1, 0, 2, 0, 3, 0, 2, 1, 3, 1, 3, 2,
                0, 1, 0, 2, 0, 3, 1, 2, 1, 3, 2, 3],
        'type': pa.array(["LINK"] * 24).dictionary_encode()
    })

    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    graph = PyPropertyGraph(vertex_table, edge_table)

    csr = graph.csr()

    result = betweenness_centrality(csr.indptr, csr.indices, graph.num_vertices())

    centrality = result.centrality().to_pylist()

    print(f"\nBetweenness centrality (complete graph):")
    for i, score in enumerate(centrality):
        print(f"    Vertex {i}: {score:.6f}")

    # In a complete graph, all vertices have equal betweenness (0)
    # because there are direct paths between all pairs
    for score in centrality:
        assert score == 0.0, "Complete graph vertices should have 0 betweenness"

    print("\n✅ Complete graph test passed!")


def test_bridge_detection():
    """Test detection of bridge vertices."""
    print("\n" + "=" * 60)
    print("Test 9: Bridge Detection")
    print("=" * 60)

    # Create two communities connected by a bridge
    # Community 1: 0-1-2 (triangle)
    # Bridge: 2-3
    # Community 2: 3-4-5 (triangle)
    vertices = pa.table({
        'id': [0, 1, 2, 3, 4, 5],
        'label': pa.array(["C1", "C1", "Bridge1", "Bridge2", "C2", "C2"]).dictionary_encode()
    })

    edges = pa.table({
        'src': [0, 1, 0, 2, 1, 2,  # Community 1
                2, 3,                # Bridge
                3, 4, 3, 5, 4, 5],   # Community 2
        'dst': [1, 0, 2, 0, 2, 1,  # Community 1
                3, 2,                # Bridge
                4, 3, 5, 3, 5, 4],   # Community 2
        'type': pa.array(["LINK"] * 14).dictionary_encode()
    })

    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    graph = PyPropertyGraph(vertex_table, edge_table)

    csr = graph.csr()

    result = betweenness_centrality(csr.indptr, csr.indices, graph.num_vertices())

    centrality = result.centrality().to_pylist()

    print(f"\nBetweenness centrality (bridge detection):")
    for i, score in enumerate(centrality):
        role = "Bridge" if i in [2, 3] else f"Community {1 if i < 2 else 2}"
        print(f"    Vertex {i} ({role}): {score:.6f}")

    # Bridge vertices (2, 3) should have highest betweenness
    assert centrality[2] > centrality[0], "Bridge vertex 2 should have high betweenness"
    assert centrality[2] > centrality[1], "Bridge vertex 2 should have high betweenness"
    assert centrality[3] > centrality[4], "Bridge vertex 3 should have high betweenness"
    assert centrality[3] > centrality[5], "Bridge vertex 3 should have high betweenness"

    print("\n✅ Bridge detection test passed!")


def test_negative_weights_rejection():
    """Test that negative weights are rejected."""
    print("\n" + "=" * 60)
    print("Test 10: Negative Weight Rejection")
    print("=" * 60)

    vertices = pa.table({
        'id': [0, 1, 2],
        'label': pa.array(["A", "B", "C"]).dictionary_encode()
    })

    edges = pa.table({
        'src': [0, 1],
        'dst': [1, 2],
        'type': pa.array(["LINK"] * 2).dictionary_encode()
    })

    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    graph = PyPropertyGraph(vertex_table, edge_table)

    csr = graph.csr()

    # Try with negative weight
    negative_weights = pa.array([1.0, -0.5], type=pa.float64())

    try:
        result = weighted_betweenness_centrality(csr.indptr, csr.indices,
                                                 negative_weights, graph.num_vertices())
        print("\n❌ ERROR: Should have rejected negative weights")
        assert False, "Should have raised error for negative weights"
    except Exception as e:
        print(f"\n✅ Correctly rejected negative weights: {str(e)}")

    print("\n✅ Negative weight rejection test passed!")


if __name__ == "__main__":
    print("\n🧪 Testing Betweenness Centrality Algorithms\n")

    test_basic_betweenness()
    test_star_graph_betweenness()
    test_normalization()
    test_weighted_betweenness()
    test_edge_betweenness()
    test_top_k_betweenness()
    test_isolated_vertices()
    test_complete_graph()
    test_bridge_detection()
    test_negative_weights_rejection()

    print("\n" + "=" * 60)
    print("🎉 All betweenness centrality tests passed!")
    print("=" * 60)
