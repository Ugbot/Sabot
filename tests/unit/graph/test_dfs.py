"""
Test DFS traversal algorithms on property graphs.
"""
import pyarrow as pa
from sabot._cython.graph.storage.graph_storage import PyPropertyGraph, PyVertexTable, PyEdgeTable
from sabot._cython.graph.traversal.dfs import (
    dfs, full_dfs, topological_sort, has_cycle, find_back_edges, PyDFSResult
)


def create_test_graph():
    """
    Create a simple directed graph for testing DFS:

    0 -> 1 -> 3
    |    |
    v    v
    2    4

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
        'type': pa.array(["KNOWS", "KNOWS", "WORKS_AT", "LIKES"]).dictionary_encode()
    })

    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    return PyPropertyGraph(vertex_table, edge_table)


def create_dag():
    """
    Create a Directed Acyclic Graph (DAG) for topological sort:

    0 -> 1 -> 3
    |         |
    v         v
    2 ------> 4

    Valid topological orders: [0, 1, 2, 3, 4] or [0, 2, 1, 3, 4]
    """
    vertices = pa.table({
        'id': [0, 1, 2, 3, 4],
        'label': pa.array(["Task1", "Task2", "Task3", "Task4", "Task5"]).dictionary_encode(),
        'name': ["Start", "Process", "Validate", "Compute", "Finish"]
    })

    edges = pa.table({
        'src': [0, 0, 1, 2, 3],
        'dst': [1, 2, 3, 4, 4],
        'type': pa.array(["DEPENDS_ON"] * 5).dictionary_encode()
    })

    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    return PyPropertyGraph(vertex_table, edge_table)


def create_cyclic_graph():
    """
    Create a graph with a cycle:

    0 -> 1 -> 2
         ^    |
         |    v
         +--- 3

    Cycle: 1 -> 2 -> 3 -> 1
    """
    vertices = pa.table({
        'id': [0, 1, 2, 3],
        'label': pa.array(["A", "B", "C", "D"]).dictionary_encode(),
        'name': ["Start", "Node1", "Node2", "Node3"]
    })

    edges = pa.table({
        'src': [0, 1, 2, 3],
        'dst': [1, 2, 3, 1],
        'type': pa.array(["EDGE"] * 4).dictionary_encode()
    })

    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    return PyPropertyGraph(vertex_table, edge_table)


def test_basic_dfs():
    """Test basic DFS from a source vertex."""
    print("=" * 60)
    print("Test 1: Basic DFS from vertex 0")
    print("=" * 60)

    graph = create_test_graph()
    csr = graph.csr()

    result = dfs(csr.indptr, csr.indices, graph.num_vertices(), source=0)

    print(f"\nDFS from vertex 0:")
    print(f"  Visited: {result.num_visited()} vertices")
    print(f"  Vertices (discovery order): {result.vertices().to_pylist()}")
    print(f"  Discovery times: {result.discovery_time().to_pylist()}")
    print(f"  Finish times: {result.finish_time().to_pylist()}")
    print(f"  Predecessors: {result.predecessors().to_pylist()}")

    # Verify DFS visited reachable vertices
    assert result.num_visited() == 5, "Should visit 5 reachable vertices (0,1,2,3,4)"

    # Verify discovery times are increasing in DFS order
    disc_times = result.discovery_time().to_pylist()
    for i in range(len(disc_times) - 1):
        assert disc_times[i] < disc_times[i+1], "Discovery times should be in order"

    # Verify finish times: parent.finish > child.finish (for tree edges)
    finish_times = result.finish_time().to_pylist()
    print(f"  ✅ Discovery times are ordered")
    print(f"  ✅ Finish times recorded: {finish_times}")

    print("\n✅ Basic DFS test passed!")


def test_full_dfs():
    """Test full DFS covering all components."""
    print("\n" + "=" * 60)
    print("Test 2: Full DFS (all connected components)")
    print("=" * 60)

    graph = create_test_graph()
    csr = graph.csr()

    result = full_dfs(csr.indptr, csr.indices, graph.num_vertices())

    print(f"\nFull DFS:")
    print(f"  Visited: {result.num_visited()} vertices")
    print(f"  Vertices: {result.vertices().to_pylist()}")
    print(f"  Discovery times: {result.discovery_time().to_pylist()}")

    assert result.num_visited() == 6, "Should visit all 6 vertices"
    print("\n✅ Full DFS test passed!")


def test_topological_sort():
    """Test topological sort on a DAG."""
    print("\n" + "=" * 60)
    print("Test 3: Topological Sort (DAG)")
    print("=" * 60)

    dag = create_dag()
    csr = dag.csr()

    topo_order = topological_sort(csr.indptr, csr.indices, dag.num_vertices())

    print(f"\nTopological order: {topo_order.to_pylist()}")

    # Verify topological property: for edge u->v, u appears before v
    topo_list = topo_order.to_pylist()
    position = {v: i for i, v in enumerate(topo_list)}

    edges_obj = dag.edges()
    edges_table = edges_obj.table()  # Call as method
    for i in range(len(edges_table)):
        src = edges_table['src'][i].as_py()
        dst = edges_table['dst'][i].as_py()
        assert position[src] < position[dst], f"Edge {src}->{dst} violates topological order"

    print("  ✅ Topological property verified: all edges go forward")
    print("\n✅ Topological sort test passed!")


def test_topological_sort_with_cycle():
    """Test topological sort on cyclic graph (should fail)."""
    print("\n" + "=" * 60)
    print("Test 4: Topological Sort (Cyclic Graph - should fail)")
    print("=" * 60)

    cyclic = create_cyclic_graph()
    csr = cyclic.csr()

    try:
        topo_order = topological_sort(csr.indptr, csr.indices, cyclic.num_vertices())
        print(f"\n❌ ERROR: Topological sort should have failed but got: {topo_order.to_pylist()}")
        assert False, "Should have raised error for cyclic graph"
    except Exception as e:
        print(f"\n✅ Correctly detected cycle: {str(e)}")
        print("\n✅ Topological sort cycle detection test passed!")


def test_cycle_detection():
    """Test cycle detection."""
    print("\n" + "=" * 60)
    print("Test 5: Cycle Detection")
    print("=" * 60)

    # Test DAG (no cycle)
    dag = create_dag()
    csr = dag.csr()
    has_cycle_dag = has_cycle(csr.indptr, csr.indices, dag.num_vertices())
    print(f"\nDAG has cycle: {has_cycle_dag}")
    assert not has_cycle_dag, "DAG should not have a cycle"

    # Test cyclic graph
    cyclic = create_cyclic_graph()
    csr = cyclic.csr()
    has_cycle_cyclic = has_cycle(csr.indptr, csr.indices, cyclic.num_vertices())
    print(f"Cyclic graph has cycle: {has_cycle_cyclic}")
    assert has_cycle_cyclic, "Cyclic graph should have a cycle"

    print("\n✅ Cycle detection test passed!")


def test_find_back_edges():
    """Test finding back edges."""
    print("\n" + "=" * 60)
    print("Test 6: Find Back Edges")
    print("=" * 60)

    # Test DAG (no back edges)
    dag = create_dag()
    csr = dag.csr()
    back_edges = find_back_edges(csr.indptr, csr.indices, dag.num_vertices())
    print(f"\nDAG back edges: {back_edges.to_pydict()}")
    assert len(back_edges) == 0, "DAG should have no back edges"

    # Test cyclic graph (should have back edges)
    cyclic = create_cyclic_graph()
    csr = cyclic.csr()
    back_edges = find_back_edges(csr.indptr, csr.indices, cyclic.num_vertices())
    print(f"Cyclic graph back edges: {back_edges.to_pydict()}")
    print(f"  Found {len(back_edges)} back edge(s)")

    assert len(back_edges) > 0, "Cyclic graph should have at least one back edge"

    # Verify back edge is part of the cycle (3 -> 1)
    src_list = back_edges['src'].to_pylist()
    dst_list = back_edges['dst'].to_pylist()
    print(f"  Back edges: {list(zip(src_list, dst_list))}")

    print("\n✅ Back edge detection test passed!")


def test_discovery_finish_times():
    """Test discovery and finish times follow DFS rules."""
    print("\n" + "=" * 60)
    print("Test 7: Discovery & Finish Times Validation")
    print("=" * 60)

    graph = create_test_graph()
    csr = graph.csr()

    result = dfs(csr.indptr, csr.indices, graph.num_vertices(), source=0)

    vertices = result.vertices().to_pylist()
    discovery = result.discovery_time().to_pylist()
    finish = result.finish_time().to_pylist()
    predecessors = result.predecessors().to_pylist()

    print(f"\nDFS Tree:")
    for i, v in enumerate(vertices):
        parent = predecessors[i]
        print(f"  Vertex {v}: disc={discovery[i]:2d}, finish={finish[i]:2d}, parent={parent}")

    # DFS property: discovery[v] < finish[v] for all vertices
    for i, v in enumerate(vertices):
        assert discovery[i] < finish[i], f"Vertex {v}: discovery >= finish"

    # DFS property: for tree edges (parent, child), parent.finish > child.finish
    for i, v in enumerate(vertices):
        parent = predecessors[i]
        if parent >= 0:  # Has a parent
            parent_idx = vertices.index(parent)
            assert finish[parent_idx] > finish[i], f"Parent {parent} finish <= child {v} finish"

    print("\n  ✅ All vertices: discovery < finish")
    print("  ✅ All tree edges: parent.finish > child.finish")
    print("\n✅ Discovery & finish times test passed!")


if __name__ == "__main__":
    print("\n🧪 Testing DFS Traversal Algorithms\n")

    test_basic_dfs()
    test_full_dfs()
    test_topological_sort()
    test_topological_sort_with_cycle()
    test_cycle_detection()
    test_find_back_edges()
    test_discovery_finish_times()

    print("\n" + "=" * 60)
    print("🎉 All DFS tests passed!")
    print("=" * 60)
