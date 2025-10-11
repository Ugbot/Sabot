"""
Test shortest paths algorithms on property graphs.
"""
import pyarrow as pa
from sabot._cython.graph.storage.graph_storage import PyPropertyGraph, PyVertexTable, PyEdgeTable
from sabot._cython.graph.traversal.shortest_paths import (
    dijkstra, unweighted_shortest_paths, reconstruct_path,
    floyd_warshall, astar, PyShortestPathsResult
)


def create_weighted_graph():
    """
    Create a weighted graph for testing Dijkstra:

    0 --(5)--> 1 --(3)--> 3
    |          |          |
   (2)        (1)        (1)
    |          |          |
    v          v          v
    2 <--(2)-- 4 --(4)--> 5

    Shortest path 0->3: 0->1->3 (distance 8)
    Shortest path 0->5: 0->2->4->5 (distance 8)
    """
    vertices = pa.table({
        'id': [0, 1, 2, 3, 4, 5],
        'label': pa.array(["A", "B", "C", "D", "E", "F"]).dictionary_encode(),
        'name': ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"]
    })

    edges = pa.table({
        'src': [0, 0, 1, 1, 2, 4, 4],
        'dst': [1, 2, 3, 4, 4, 3, 5],
        'type': pa.array(["ROAD"] * 7).dictionary_encode(),
        'weight': [5.0, 2.0, 3.0, 1.0, 2.0, 1.0, 4.0]
    })

    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    graph = PyPropertyGraph(vertex_table, edge_table)

    # Extract weights
    weights = edges['weight'].combine_chunks()
    return graph, weights


def create_unweighted_graph():
    """
    Create an unweighted graph:

    0 --> 1 --> 3
    |     |
    v     v
    2     4

    5 (disconnected)
    """
    vertices = pa.table({
        'id': [0, 1, 2, 3, 4, 5],
        'label': pa.array(["A", "B", "C", "D", "E", "F"]).dictionary_encode(),
        'name': ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"]
    })

    edges = pa.table({
        'src': [0, 0, 1, 1],
        'dst': [1, 2, 3, 4],
        'type': pa.array(["KNOWS"] * 4).dictionary_encode()
    })

    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    return PyPropertyGraph(vertex_table, edge_table)


def test_dijkstra():
    """Test Dijkstra's algorithm on weighted graph."""
    print("=" * 60)
    print("Test 1: Dijkstra's Algorithm (Weighted Graph)")
    print("=" * 60)

    graph, weights = create_weighted_graph()
    csr = graph.csr()

    result = dijkstra(csr.indptr, csr.indices, weights, graph.num_vertices(), source=0)

    print(f"\nDijkstra from vertex 0:")
    print(f"  Source: {result.source()}")
    print(f"  Reached: {result.num_reached()} vertices")
    print(f"  Distances: {result.distances().to_pylist()}")
    print(f"  Predecessors: {result.predecessors().to_pylist()}")

    distances = result.distances().to_pylist()

    # Verify distances
    assert distances[0] == 0.0, "Distance to source should be 0"
    assert distances[1] == 5.0, "Distance to vertex 1 should be 5 (0->1)"
    assert distances[2] == 2.0, "Distance to vertex 2 should be 2 (0->2)"
    assert distances[3] == 5.0, "Distance to vertex 3 should be 5 (0->2->4->3)"
    assert distances[4] == 4.0, "Distance to vertex 4 should be 4 (0->2->4)"
    assert distances[5] == 8.0, "Distance to vertex 5 should be 8 (0->2->4->5)"

    print("\n✅ Dijkstra test passed!")


def test_unweighted():
    """Test unweighted shortest paths (BFS-based)."""
    print("\n" + "=" * 60)
    print("Test 2: Unweighted Shortest Paths (BFS-based)")
    print("=" * 60)

    graph = create_unweighted_graph()
    csr = graph.csr()

    result = unweighted_shortest_paths(csr.indptr, csr.indices, graph.num_vertices(), source=0)

    print(f"\nUnweighted shortest paths from vertex 0:")
    print(f"  Reached: {result.num_reached()} vertices")
    print(f"  Distances: {result.distances().to_pylist()}")

    distances = result.distances().to_pylist()

    # Verify distances (hop counts)
    assert distances[0] == 0.0, "Distance to source should be 0"
    assert distances[1] == 1.0, "Distance to vertex 1 should be 1 hop"
    assert distances[2] == 1.0, "Distance to vertex 2 should be 1 hop"
    assert distances[3] == 2.0, "Distance to vertex 3 should be 2 hops"
    assert distances[4] == 2.0, "Distance to vertex 4 should be 2 hops"
    assert float('inf') == distances[5], "Vertex 5 should be unreachable"

    print("\n✅ Unweighted shortest paths test passed!")


def test_path_reconstruction():
    """Test path reconstruction from predecessors."""
    print("\n" + "=" * 60)
    print("Test 3: Path Reconstruction")
    print("=" * 60)

    graph, weights = create_weighted_graph()
    csr = graph.csr()

    result = dijkstra(csr.indptr, csr.indices, weights, graph.num_vertices(), source=0)

    # Reconstruct path from 0 to 3
    path_0_to_3 = reconstruct_path(result.predecessors(), source=0, target=3)
    print(f"\nPath from 0 to 3: {path_0_to_3.to_pylist()}")
    assert path_0_to_3.to_pylist() == [0, 2, 4, 3], "Path should be 0->2->4->3 (shortest)"

    # Reconstruct path from 0 to 5
    path_0_to_5 = reconstruct_path(result.predecessors(), source=0, target=5)
    print(f"Path from 0 to 5: {path_0_to_5.to_pylist()}")
    assert path_0_to_5.to_pylist() == [0, 2, 4, 5], "Path should be 0->2->4->5"

    # Reconstruct path to source (should be just [0])
    path_0_to_0 = reconstruct_path(result.predecessors(), source=0, target=0)
    print(f"Path from 0 to 0: {path_0_to_0.to_pylist()}")
    assert path_0_to_0.to_pylist() == [0], "Path to source should be [0]"

    print("\n✅ Path reconstruction test passed!")


def test_unreachable():
    """Test handling of unreachable vertices."""
    print("\n" + "=" * 60)
    print("Test 4: Unreachable Vertices")
    print("=" * 60)

    graph = create_unweighted_graph()
    csr = graph.csr()

    result = unweighted_shortest_paths(csr.indptr, csr.indices, graph.num_vertices(), source=0)

    # Try to reconstruct path to unreachable vertex 5
    path_0_to_5 = reconstruct_path(result.predecessors(), source=0, target=5)
    print(f"\nPath from 0 to unreachable vertex 5: {path_0_to_5.to_pylist()}")
    assert len(path_0_to_5) == 0, "Path to unreachable vertex should be empty"

    distances = result.distances().to_pylist()
    assert float('inf') == distances[5], "Distance to unreachable should be infinity"

    print("\n✅ Unreachable vertices test passed!")


def test_floyd_warshall():
    """Test Floyd-Warshall all-pairs shortest paths."""
    print("\n" + "=" * 60)
    print("Test 5: Floyd-Warshall (All-Pairs Shortest Paths)")
    print("=" * 60)

    graph, weights = create_weighted_graph()
    csr = graph.csr()

    all_pairs = floyd_warshall(csr.indptr, csr.indices, weights, graph.num_vertices())

    print(f"\nAll-pairs shortest paths:")
    print(f"  Total pairs: {len(all_pairs)}")
    print(f"  Schema: {all_pairs.schema}")

    # Convert to dict for easier lookup
    pairs_dict = {}
    for i in range(len(all_pairs)):
        src = all_pairs['src'][i].as_py()
        dst = all_pairs['dst'][i].as_py()
        dist = all_pairs['distance'][i].as_py()
        pairs_dict[(src, dst)] = dist

    print(f"\nSample distances:")
    print(f"  0 -> 3: {pairs_dict.get((0, 3), 'N/A')}")
    print(f"  0 -> 5: {pairs_dict.get((0, 5), 'N/A')}")
    print(f"  1 -> 5: {pairs_dict.get((1, 5), 'N/A')}")

    # Verify key distances
    assert pairs_dict[(0, 0)] == 0.0, "Distance from vertex to itself should be 0"
    assert pairs_dict[(0, 3)] == 5.0, "Distance 0->3 should be 5"
    assert pairs_dict[(0, 5)] == 8.0, "Distance 0->5 should be 8"

    print("\n✅ Floyd-Warshall test passed!")


def test_astar():
    """Test A* with heuristic function."""
    print("\n" + "=" * 60)
    print("Test 6: A* Algorithm (Heuristic-based)")
    print("=" * 60)

    graph, weights = create_weighted_graph()
    csr = graph.csr()

    # Simple heuristic: estimated distance to target (vertex 3)
    # In a real scenario, this could be Euclidean distance on a grid
    heuristic = pa.array([
        3.0,  # vertex 0: estimate 3 to reach 3
        2.0,  # vertex 1: estimate 2 to reach 3
        5.0,  # vertex 2: estimate 5 to reach 3
        0.0,  # vertex 3: at target
        1.0,  # vertex 4: estimate 1 to reach 3
        2.0,  # vertex 5: estimate 2 to reach 3
    ], type=pa.float64())

    result = astar(csr.indptr, csr.indices, weights, graph.num_vertices(),
                   source=0, target=3, heuristic=heuristic)

    distances_list = result.distances().to_pylist()

    print(f"\nA* from vertex 0 to vertex 3:")
    print(f"  All distances: {distances_list}")
    print(f"  Distance to target 3: {distances_list[3]}")
    print(f"  Explored: {result.num_reached()} vertices")

    # Reconstruct path
    path = reconstruct_path(result.predecessors(), source=0, target=3)
    print(f"  Path: {path.to_pylist()}")

    # Verify A* found optimal path
    assert distances_list[3] == 5.0, f"A* should find optimal distance (got {distances_list[3]})"
    assert path.to_pylist() == [0, 2, 4, 3], "A* should find optimal path"

    print("\n✅ A* test passed!")


def test_comparison():
    """Compare Dijkstra vs unweighted shortest paths."""
    print("\n" + "=" * 60)
    print("Test 7: Dijkstra vs Unweighted Comparison")
    print("=" * 60)

    graph = create_unweighted_graph()
    csr = graph.csr()

    # Create uniform weights (all edges weight 1)
    num_edges = csr.num_edges()
    uniform_weights = pa.array([1.0] * num_edges, type=pa.float64())

    # Run both algorithms
    dijkstra_result = dijkstra(csr.indptr, csr.indices, uniform_weights, graph.num_vertices(), source=0)
    unweighted_result = unweighted_shortest_paths(csr.indptr, csr.indices, graph.num_vertices(), source=0)

    dijkstra_dists = dijkstra_result.distances().to_pylist()
    unweighted_dists = unweighted_result.distances().to_pylist()

    print(f"\nDijkstra distances: {dijkstra_dists}")
    print(f"Unweighted distances: {unweighted_dists}")

    # Results should be identical for unit weights
    for i in range(graph.num_vertices()):
        assert dijkstra_dists[i] == unweighted_dists[i], \
            f"Dijkstra and unweighted should match for unit weights at vertex {i}"

    print("\n✅ Comparison test passed!")


def test_negative_weights():
    """Test that negative weights are rejected."""
    print("\n" + "=" * 60)
    print("Test 8: Negative Weight Detection")
    print("=" * 60)

    graph, _ = create_weighted_graph()
    csr = graph.csr()

    # Create weights with a negative value
    negative_weights = pa.array([5.0, 2.0, -1.0, 1.0, 2.0, 1.0, 4.0], type=pa.float64())

    try:
        result = dijkstra(csr.indptr, csr.indices, negative_weights, graph.num_vertices(), source=0)
        print("\n❌ ERROR: Dijkstra should have rejected negative weights")
        assert False, "Should have raised error for negative weights"
    except Exception as e:
        print(f"\n✅ Correctly rejected negative weights: {str(e)}")

    print("\n✅ Negative weight detection test passed!")


if __name__ == "__main__":
    print("\n🧪 Testing Shortest Paths Algorithms\n")

    test_dijkstra()
    test_unweighted()
    test_path_reconstruction()
    test_unreachable()
    test_floyd_warshall()
    test_astar()
    test_comparison()
    test_negative_weights()

    print("\n" + "=" * 60)
    print("🎉 All shortest paths tests passed!")
    print("=" * 60)
