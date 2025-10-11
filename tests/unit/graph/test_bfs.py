"""
Test BFS traversal algorithms on a simple property graph.
"""
import pyarrow as pa
from sabot._cython.graph.storage.graph_storage import PyPropertyGraph, PyVertexTable, PyEdgeTable
from sabot._cython.graph.traversal.bfs import bfs, k_hop_neighbors, multi_bfs, component_bfs


def create_test_graph():
    """
    Create a simple graph for testing BFS:

    0 -> 1 -> 3
    |    |
    v    v
    2    4

    5 (disconnected)
    """
    # Vertices table
    vertices = pa.table({
        'id': [0, 1, 2, 3, 4, 5],
        'label': pa.array(["A", "B", "C", "D", "E", "F"]).dictionary_encode(),
        'name': ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"]
    })

    # Edges table (directed)
    edges = pa.table({
        'src': [0, 0, 1, 1],
        'dst': [1, 2, 3, 4],
        'type': pa.array(["KNOWS", "KNOWS", "WORKS_AT", "LIKES"]).dictionary_encode()
    })

    # Create graph
    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    graph = PyPropertyGraph(vertex_table, edge_table)
    return graph


def test_basic_bfs():
    """Test basic BFS from vertex 0."""
    print("=" * 60)
    print("Test 1: Basic BFS from vertex 0")
    print("=" * 60)

    graph = create_test_graph()
    csr = graph.csr()

    # BFS from vertex 0
    result = bfs(csr.indptr, csr.indices, graph.num_vertices(), source=0)

    print(f"\nBFS from vertex 0:")
    print(f"  Visited: {result.num_visited()} vertices")
    print(f"  Vertices: {result.vertices().to_pylist()}")
    print(f"  Distances: {result.distances().to_pylist()}")
    print(f"  Predecessors: {result.predecessors().to_pylist()}")

    assert result.num_visited() == 5, "Should visit 5 vertices (0,1,2,3,4)"
    assert result.vertices().to_pylist() == [0, 1, 2, 3, 4], "BFS order incorrect"
    print("\n✅ Basic BFS test passed!")


def test_k_hop_neighbors():
    """Test k-hop neighbors."""
    print("\n" + "=" * 60)
    print("Test 2: K-hop neighbors from vertex 0")
    print("=" * 60)

    graph = create_test_graph()
    csr = graph.csr()

    # 1-hop neighbors (direct neighbors)
    neighbors_1hop = k_hop_neighbors(csr.indptr, csr.indices, graph.num_vertices(), source=0, k=1)
    print(f"\n1-hop neighbors of vertex 0: {neighbors_1hop.to_pylist()}")
    assert set(neighbors_1hop.to_pylist()) == {1, 2}, "1-hop should be {1, 2}"

    # 2-hop neighbors
    neighbors_2hop = k_hop_neighbors(csr.indptr, csr.indices, graph.num_vertices(), source=0, k=2)
    print(f"2-hop neighbors of vertex 0: {neighbors_2hop.to_pylist()}")
    assert set(neighbors_2hop.to_pylist()) == {3, 4}, "2-hop should be {3, 4}"

    # 0-hop neighbors (just the source)
    neighbors_0hop = k_hop_neighbors(csr.indptr, csr.indices, graph.num_vertices(), source=0, k=0)
    print(f"0-hop neighbors of vertex 0: {neighbors_0hop.to_pylist()}")
    assert neighbors_0hop.to_pylist() == [0], "0-hop should be [0]"

    print("\n✅ K-hop neighbors test passed!")


def test_multi_source_bfs():
    """Test multi-source BFS."""
    print("\n" + "=" * 60)
    print("Test 3: Multi-source BFS from vertices [0, 5]")
    print("=" * 60)

    graph = create_test_graph()
    csr = graph.csr()

    # Multi-source BFS from vertices 0 and 5
    sources = pa.array([0, 5], type=pa.int64())
    result = multi_bfs(csr.indptr, csr.indices, graph.num_vertices(), sources=sources)

    print(f"\nMulti-source BFS from vertices [0, 5]:")
    print(f"  Visited: {result.num_visited()} vertices")
    print(f"  Vertices: {result.vertices().to_pylist()}")
    print(f"  Distances: {result.distances().to_pylist()}")
    print(f"  Predecessors: {result.predecessors().to_pylist()}")

    assert result.num_visited() == 6, "Should visit all 6 vertices"
    print("\n✅ Multi-source BFS test passed!")


def test_connected_component():
    """Test connected component detection."""
    print("\n" + "=" * 60)
    print("Test 4: Connected component from vertex 0")
    print("=" * 60)

    graph = create_test_graph()
    csr = graph.csr()

    # Component containing vertex 0
    component = component_bfs(csr.indptr, csr.indices, graph.num_vertices(), source=0)
    print(f"\nComponent containing vertex 0: {component.to_pylist()}")
    assert len(component) == 5, "Component should have 5 vertices"
    assert 5 not in component.to_pylist(), "Vertex 5 is disconnected"

    # Component containing vertex 5 (isolated)
    component5 = component_bfs(csr.indptr, csr.indices, graph.num_vertices(), source=5)
    print(f"Component containing vertex 5: {component5.to_pylist()}")
    assert component5.to_pylist() == [5], "Vertex 5 should be isolated"

    print("\n✅ Connected component test passed!")


def test_max_depth():
    """Test BFS with max depth limit."""
    print("\n" + "=" * 60)
    print("Test 5: BFS with max depth limit")
    print("=" * 60)

    graph = create_test_graph()
    csr = graph.csr()

    # BFS from vertex 0 with max_depth=1
    result = bfs(csr.indptr, csr.indices, graph.num_vertices(), source=0, max_depth=1)

    print(f"\nBFS from vertex 0 (max_depth=1):")
    print(f"  Visited: {result.num_visited()} vertices")
    print(f"  Vertices: {result.vertices().to_pylist()}")
    print(f"  Max distance: {max(result.distances().to_pylist())}")

    assert max(result.distances().to_pylist()) <= 1, "Max distance should be <= 1"
    assert result.num_visited() == 3, "Should visit vertices at distance 0 and 1"

    print("\n✅ Max depth test passed!")


if __name__ == "__main__":
    print("\n🧪 Testing BFS Traversal Algorithms\n")

    test_basic_bfs()
    test_k_hop_neighbors()
    test_multi_source_bfs()
    test_connected_component()
    test_max_depth()

    print("\n" + "=" * 60)
    print("🎉 All BFS tests passed!")
    print("=" * 60)
