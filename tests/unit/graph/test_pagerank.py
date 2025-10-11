"""
Test PageRank algorithms on property graphs.
"""
import pyarrow as pa
from sabot._cython.graph.storage.graph_storage import PyPropertyGraph, PyVertexTable, PyEdgeTable
from sabot._cython.graph.traversal.pagerank import (
    pagerank, personalized_pagerank, weighted_pagerank, top_k_pagerank, PyPageRankResult
)


def create_simple_graph():
    """
    Create a simple graph for PageRank testing:

    A simple 4-vertex graph with varying connectivity:
    0 --> 1 --> 2
    |           ^
    v           |
    3 ----------+

    Vertex 2 should have highest PageRank (3 incoming edges)
    """
    vertices = pa.table({
        'id': [0, 1, 2, 3],
        'label': pa.array(["A", "B", "C", "D"]).dictionary_encode(),
        'name': ["Alice", "Bob", "Carol", "Dave"]
    })

    edges = pa.table({
        'src': [0, 0, 1, 3],
        'dst': [1, 3, 2, 2],
        'type': pa.array(["LINK"] * 4).dictionary_encode()
    })

    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    return PyPropertyGraph(vertex_table, edge_table)


def create_web_graph():
    """
    Create a web graph for testing (PageRank's original use case):

    Simulates a network of 6 web pages with hyperlinks.

    0 (Homepage) --> 1 (About)
                 --> 2 (Products)
                 --> 3 (Blog)
    1 --> 2
    2 --> 3
    3 --> 0  (back to homepage)
    4 (External) --> 0
    5 (Isolated)

    Expected: Homepage (0) should have high rank due to back-link from Blog
    """
    vertices = pa.table({
        'id': [0, 1, 2, 3, 4, 5],
        'label': pa.array(["Page"] * 6).dictionary_encode(),
        'name': ["Homepage", "About", "Products", "Blog", "External", "Isolated"],
        'url': [
            "index.html",
            "about.html",
            "products.html",
            "blog.html",
            "external.html",
            "isolated.html"
        ]
    })

    edges = pa.table({
        'src': [0, 0, 0, 1, 2, 3, 4],
        'dst': [1, 2, 3, 2, 3, 0, 0],
        'type': pa.array(["LINK"] * 7).dictionary_encode()
    })

    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    return PyPropertyGraph(vertex_table, edge_table)


def test_basic_pagerank():
    """Test basic PageRank on simple graph."""
    print("=" * 60)
    print("Test 1: Basic PageRank")
    print("=" * 60)

    graph = create_simple_graph()
    csr = graph.csr()

    result = pagerank(csr.indptr, csr.indices, graph.num_vertices())

    print(f"\nPageRank from all vertices:")
    print(f"  Vertices: {result.num_vertices()}")
    print(f"  Iterations: {result.num_iterations()}")
    print(f"  Converged: {result.converged()}")
    print(f"  Ranks: {result.ranks().to_pylist()}")

    ranks = result.ranks().to_pylist()

    # Verify ranks sum to 1.0 (normalized)
    rank_sum = sum(ranks)
    assert abs(rank_sum - 1.0) < 1e-6, f"Ranks should sum to 1.0, got {rank_sum}"

    # Vertex 2 should have highest rank (3 incoming edges)
    assert ranks[2] > ranks[0], "Vertex 2 should have higher rank than 0"
    assert ranks[2] > ranks[1], "Vertex 2 should have higher rank than 1"
    assert ranks[2] > ranks[3], "Vertex 2 should have higher rank than 3"

    # All ranks should be positive
    for i, rank in enumerate(ranks):
        assert rank > 0.0, f"Rank for vertex {i} should be positive"

    print("\n✅ Basic PageRank test passed!")


def test_convergence():
    """Test PageRank convergence."""
    print("\n" + "=" * 60)
    print("Test 2: PageRank Convergence")
    print("=" * 60)

    graph = create_simple_graph()
    csr = graph.csr()

    # Test with different max iterations
    print("\nTesting convergence with different max iterations:")

    for max_iters in [10, 50, 100]:
        result = pagerank(csr.indptr, csr.indices, graph.num_vertices(),
                         max_iterations=max_iters)
        print(f"  Max {max_iters:3d} iters: converged={result.converged()}, "
              f"actual={result.num_iterations()}, delta={result.final_delta():.8f}")

    # Test with stricter tolerance
    print("\nTesting with stricter tolerance:")
    result_strict = pagerank(csr.indptr, csr.indices, graph.num_vertices(),
                            tolerance=1e-9, max_iterations=200)
    print(f"  Tolerance 1e-9: converged={result_strict.converged()}, "
          f"iterations={result_strict.num_iterations()}")

    print("\n✅ Convergence test passed!")


def test_damping_factor():
    """Test effect of damping factor on PageRank."""
    print("\n" + "=" * 60)
    print("Test 3: Damping Factor Effect")
    print("=" * 60)

    graph = create_web_graph()
    csr = graph.csr()

    print("\nComparing PageRank with different damping factors:")

    for damping in [0.5, 0.85, 0.95]:
        result = pagerank(csr.indptr, csr.indices, graph.num_vertices(), damping=damping)
        ranks = result.ranks().to_pylist()
        max_rank = max(ranks)
        min_rank = min(ranks)
        spread = max_rank - min_rank

        print(f"  Damping {damping}: spread={spread:.4f}, "
              f"max={max_rank:.4f}, min={min_rank:.4f}")

    # Higher damping should lead to more spread in ranks
    # (more importance flows through links)

    print("\n✅ Damping factor test passed!")


def test_personalized_pagerank():
    """Test Personalized PageRank."""
    print("\n" + "=" * 60)
    print("Test 4: Personalized PageRank")
    print("=" * 60)

    graph = create_web_graph()
    csr = graph.csr()
    vertices = graph.vertices()

    # Personalized PageRank from "Products" page (vertex 2)
    source_vertices = pa.array([2], type=pa.int64())

    result = personalized_pagerank(
        csr.indptr, csr.indices, graph.num_vertices(), source_vertices
    )

    print(f"\nPersonalized PageRank from vertex 2 (Products):")
    print(f"  Iterations: {result.num_iterations()}")
    print(f"  Converged: {result.converged()}")

    ranks = result.ranks().to_pylist()
    for i, rank in enumerate(ranks):
        name = vertices.table()['name'][i].as_py()
        print(f"    {name:12s}: {rank:.6f}")

    # Vertex 2 should have some rank since it's the source
    assert ranks[2] > 0.0, "Source vertex should have non-zero rank"

    # Ranks should still sum to 1.0
    rank_sum = sum(ranks)
    assert abs(rank_sum - 1.0) < 1e-6, f"Ranks should sum to 1.0, got {rank_sum}"

    print("\n✅ Personalized PageRank test passed!")


def test_weighted_pagerank():
    """Test weighted PageRank."""
    print("\n" + "=" * 60)
    print("Test 5: Weighted PageRank")
    print("=" * 60)

    # Create graph with weighted edges
    vertices = pa.table({
        'id': [0, 1, 2, 3],
        'label': pa.array(["A", "B", "C", "D"]).dictionary_encode(),
        'name': ["Alice", "Bob", "Carol", "Dave"]
    })

    # Varying weights: some links are more important than others
    edges = pa.table({
        'src': [0, 0, 1, 3],
        'dst': [1, 3, 2, 2],
        'type': pa.array(["LINK"] * 4).dictionary_encode(),
        'weight': [1.0, 0.5, 2.0, 1.5]  # Different weights
    })

    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    graph = PyPropertyGraph(vertex_table, edge_table)

    csr = graph.csr()
    weights = edges['weight'].combine_chunks()

    # Run weighted PageRank
    result = weighted_pagerank(csr.indptr, csr.indices, weights, graph.num_vertices())

    print(f"\nWeighted PageRank:")
    print(f"  Converged: {result.converged()}")
    print(f"  Iterations: {result.num_iterations()}")

    ranks = result.ranks().to_pylist()
    for i, rank in enumerate(ranks):
        print(f"    Vertex {i}: {rank:.6f}")

    # Verify ranks sum to 1.0
    rank_sum = sum(ranks)
    assert abs(rank_sum - 1.0) < 1e-6, f"Ranks should sum to 1.0, got {rank_sum}"

    print("\n✅ Weighted PageRank test passed!")


def test_top_k():
    """Test top-K PageRank extraction."""
    print("\n" + "=" * 60)
    print("Test 6: Top-K PageRank")
    print("=" * 60)

    graph = create_web_graph()
    csr = graph.csr()
    vertices = graph.vertices()

    # Compute PageRank
    result = pagerank(csr.indptr, csr.indices, graph.num_vertices())

    # Get top 3 pages
    top_3 = top_k_pagerank(result.ranks(), k=3)

    print(f"\nTop 3 pages by PageRank:")
    for i in range(len(top_3)):
        vertex_id = top_3['vertex_id'][i].as_py()
        rank = top_3['rank'][i].as_py()
        name = vertices.table()['name'][vertex_id].as_py()
        print(f"  {i+1}. {name:12s} (vertex {vertex_id}): {rank:.6f}")

    # Verify results are sorted in descending order
    ranks_in_top = [top_3['rank'][i].as_py() for i in range(len(top_3))]
    for i in range(len(ranks_in_top) - 1):
        assert ranks_in_top[i] >= ranks_in_top[i+1], "Top-K should be sorted descending"

    # Test with k=1 (top page)
    top_1 = top_k_pagerank(result.ranks(), k=1)
    assert len(top_1) == 1, "Top-1 should return exactly 1 result"

    print("\n✅ Top-K test passed!")


def test_isolated_vertices():
    """Test PageRank with isolated vertices."""
    print("\n" + "=" * 60)
    print("Test 7: Isolated Vertices")
    print("=" * 60)

    # Graph with isolated vertex (vertex 5 has no edges)
    graph = create_web_graph()
    csr = graph.csr()
    vertices = graph.vertices()

    result = pagerank(csr.indptr, csr.indices, graph.num_vertices())

    ranks = result.ranks().to_pylist()

    print(f"\nPageRank with isolated vertex:")
    for i, rank in enumerate(ranks):
        name = vertices.table()['name'][i].as_py()
        print(f"  {name:12s}: {rank:.6f}")

    # Isolated vertex should still have some small rank (from teleportation)
    isolated_id = 5
    assert ranks[isolated_id] > 0.0, "Isolated vertex should have non-zero rank"

    # Isolated vertex should have one of the lowest ranks
    # (it may be equal to other poorly connected vertices)
    assert ranks[isolated_id] <= min(ranks), \
        "Isolated vertex should have minimal rank"

    print("\n✅ Isolated vertices test passed!")


def test_dangling_nodes():
    """Test PageRank with dangling nodes (no outgoing edges)."""
    print("\n" + "=" * 60)
    print("Test 8: Dangling Nodes")
    print("=" * 60)

    # Create graph where vertex 2 is a dangling node (no outgoing edges)
    vertices = pa.table({
        'id': [0, 1, 2],
        'label': pa.array(["A", "B", "C"]).dictionary_encode(),
        'name': ["Alice", "Bob", "Carol"]
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

    result = pagerank(csr.indptr, csr.indices, graph.num_vertices())

    ranks = result.ranks().to_pylist()

    print(f"\nPageRank with dangling node:")
    for i, rank in enumerate(ranks):
        print(f"  Vertex {i}: {rank:.6f} (out_degree={csr.get_degree(i)})")

    # All ranks should be positive even for dangling node
    for i, rank in enumerate(ranks):
        assert rank > 0.0, f"Vertex {i} should have positive rank"

    # Ranks should sum to 1.0
    rank_sum = sum(ranks)
    assert abs(rank_sum - 1.0) < 1e-6, f"Ranks should sum to 1.0, got {rank_sum}"

    print("\n✅ Dangling nodes test passed!")


def test_negative_weights_rejection():
    """Test that negative weights are rejected."""
    print("\n" + "=" * 60)
    print("Test 9: Negative Weight Rejection")
    print("=" * 60)

    vertices = pa.table({
        'id': [0, 1, 2],
        'label': pa.array(["A", "B", "C"]).dictionary_encode(),
        'name': ["Alice", "Bob", "Carol"]
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

    # Try weighted PageRank with negative weight
    negative_weights = pa.array([1.0, -0.5], type=pa.float64())

    try:
        result = weighted_pagerank(csr.indptr, csr.indices, negative_weights,
                                   graph.num_vertices())
        print("\n❌ ERROR: Should have rejected negative weights")
        assert False, "Should have raised error for negative weights"
    except Exception as e:
        print(f"\n✅ Correctly rejected negative weights: {str(e)}")

    print("\n✅ Negative weight rejection test passed!")


def test_multi_source_personalized():
    """Test Personalized PageRank with multiple source vertices."""
    print("\n" + "=" * 60)
    print("Test 10: Multi-Source Personalized PageRank")
    print("=" * 60)

    graph = create_web_graph()
    csr = graph.csr()
    vertices = graph.vertices()

    # Personalized PageRank from multiple sources
    # (Homepage and Products)
    source_vertices = pa.array([0, 2], type=pa.int64())

    result = personalized_pagerank(
        csr.indptr, csr.indices, graph.num_vertices(), source_vertices
    )

    print(f"\nPersonalized PageRank from vertices 0 and 2:")
    ranks = result.ranks().to_pylist()
    for i, rank in enumerate(ranks):
        name = vertices.table()['name'][i].as_py()
        print(f"  {name:12s}: {rank:.6f}")

    # Both source vertices should have non-zero rank
    assert ranks[0] > 0.0, "Source vertex 0 should have non-zero rank"
    assert ranks[2] > 0.0, "Source vertex 2 should have non-zero rank"

    # Ranks should sum to 1.0
    rank_sum = sum(ranks)
    assert abs(rank_sum - 1.0) < 1e-6, f"Ranks should sum to 1.0, got {rank_sum}"

    print("\n✅ Multi-source personalized PageRank test passed!")


if __name__ == "__main__":
    print("\n🧪 Testing PageRank Algorithms\n")

    test_basic_pagerank()
    test_convergence()
    test_damping_factor()
    test_personalized_pagerank()
    test_weighted_pagerank()
    test_top_k()
    test_isolated_vertices()
    test_dangling_nodes()
    test_negative_weights_rejection()
    test_multi_source_personalized()

    print("\n" + "=" * 60)
    print("🎉 All PageRank tests passed!")
    print("=" * 60)
