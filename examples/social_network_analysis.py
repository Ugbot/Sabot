"""
Social Network Analysis with Graph Traversal Algorithms

Demonstrates practical use of BFS, DFS, and path finding algorithms
on a social network graph.
"""
import pyarrow as pa
from sabot._cython.graph.storage.graph_storage import PyPropertyGraph, PyVertexTable, PyEdgeTable
from sabot._cython.graph.traversal.bfs import bfs, k_hop_neighbors, component_bfs
from sabot._cython.graph.traversal.dfs import dfs, has_cycle
from sabot._cython.graph.traversal.shortest_paths import unweighted_shortest_paths, reconstruct_path


def create_social_network():
    """
    Create a social network graph:

    People: Alice, Bob, Carol, Dave, Eve, Frank, Grace, Henry
    Relationships: FRIENDS_WITH

    Communities:
    - Group 1: Alice, Bob, Carol, Dave (highly connected)
    - Group 2: Eve, Frank (connected)
    - Isolated: Grace, Henry (no connections)
    """
    vertices = pa.table({
        'id': [0, 1, 2, 3, 4, 5, 6, 7],
        'label': pa.array(["Person"] * 8).dictionary_encode(),
        'name': ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Henry"],
        'age': [28, 32, 25, 30, 27, 35, 29, 31],
        'city': ["NYC", "NYC", "NYC", "Boston", "LA", "LA", "SF", "Seattle"]
    })

    # Friendships (undirected, so we add both directions)
    edges = pa.table({
        'src': [0, 1, 0, 2, 1, 3, 2, 3, 0, 3, 4, 5],
        'dst': [1, 0, 2, 0, 3, 1, 3, 2, 3, 0, 5, 4],
        'type': pa.array(["FRIENDS_WITH"] * 12).dictionary_encode(),
        'since_year': [2018, 2018, 2019, 2019, 2020, 2020, 2020, 2020, 2021, 2021, 2017, 2017]
    })

    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    return PyPropertyGraph(vertex_table, edge_table)


def analyze_network_structure(graph):
    """Analyze basic network structure."""
    print("=" * 70)
    print("SOCIAL NETWORK STRUCTURE ANALYSIS")
    print("=" * 70)

    csr = graph.csr()
    vertices = graph.vertices()

    print(f"\nNetwork Size:")
    print(f"  Total people: {graph.num_vertices()}")
    print(f"  Total friendships: {graph.num_edges() // 2} (undirected)")

    # Calculate degree distribution
    degrees = []
    print(f"\nDegree Distribution:")
    for v in range(graph.num_vertices()):
        degree = csr.get_degree(v)
        degrees.append(degree)
        name = vertices.table()['name'][v].as_py()
        print(f"  {name}: {degree} friends")

    avg_degree = sum(degrees) / len(degrees) if degrees else 0
    print(f"\n  Average degree: {avg_degree:.2f}")
    print(f"  Max degree: {max(degrees)} (most connected person)")
    print(f"  Min degree: {min(degrees)} (least connected)")


def find_communities(graph):
    """Detect communities using BFS."""
    print("\n" + "=" * 70)
    print("COMMUNITY DETECTION")
    print("=" * 70)

    csr = graph.csr()
    vertices = graph.vertices()
    visited = set()
    communities = []

    for v in range(graph.num_vertices()):
        if v in visited:
            continue

        # Find connected component starting from v
        component = component_bfs(csr.indptr, csr.indices, graph.num_vertices(), v)
        component_list = component.to_pylist()
        visited.update(component_list)
        communities.append(component_list)

    print(f"\nDetected {len(communities)} communities:")
    for i, community in enumerate(communities, 1):
        names = [vertices.table()['name'][v].as_py() for v in community]
        print(f"  Community {i} ({len(community)} people): {', '.join(names)}")


def find_friends_of_friends(graph, person_name):
    """Find friends-of-friends (2-hop neighbors)."""
    print("\n" + "=" * 70)
    print(f"FRIENDS-OF-FRIENDS ANALYSIS FOR {person_name.upper()}")
    print("=" * 70)

    csr = graph.csr()
    vertices = graph.vertices()

    # Find person ID
    person_id = None
    for v in range(graph.num_vertices()):
        if vertices.table()['name'][v].as_py() == person_name:
            person_id = v
            break

    if person_id is None:
        print(f"  Person '{person_name}' not found!")
        return

    # Find direct friends (1-hop)
    friends_1hop = k_hop_neighbors(csr.indptr, csr.indices, graph.num_vertices(), person_id, k=1)
    friends_1hop_set = set(friends_1hop.to_pylist())

    # Find friends-of-friends (2-hop)
    friends_2hop = k_hop_neighbors(csr.indptr, csr.indices, graph.num_vertices(), person_id, k=2)
    friends_2hop_set = set(friends_2hop.to_pylist())

    print(f"\nDirect Friends ({len(friends_1hop_set)}):")
    for friend_id in sorted(friends_1hop_set):
        name = vertices.table()['name'][friend_id].as_py()
        print(f"  - {name}")

    print(f"\nFriends-of-Friends ({len(friends_2hop_set)}):")
    for fof_id in sorted(friends_2hop_set):
        name = vertices.table()['name'][fof_id].as_py()
        print(f"  - {name}")

    # Potential new friends (2-hop but not 1-hop, excluding self)
    potential_friends = friends_2hop_set - friends_1hop_set - {person_id}
    print(f"\nPotential New Friends ({len(potential_friends)}):")
    for potential_id in sorted(potential_friends):
        name = vertices.table()['name'][potential_id].as_py()
        print(f"  - {name} (friend of a friend)")


def shortest_connection_path(graph, person1, person2):
    """Find shortest connection path between two people."""
    print("\n" + "=" * 70)
    print(f"SHORTEST CONNECTION: {person1.upper()} → {person2.upper()}")
    print("=" * 70)

    csr = graph.csr()
    vertices = graph.vertices()

    # Find person IDs
    person1_id = None
    person2_id = None
    for v in range(graph.num_vertices()):
        name = vertices.table()['name'][v].as_py()
        if name == person1:
            person1_id = v
        if name == person2:
            person2_id = v

    if person1_id is None or person2_id is None:
        print(f"  One or both people not found!")
        return

    # Find shortest path
    result = unweighted_shortest_paths(csr.indptr, csr.indices, graph.num_vertices(), person1_id)
    distance = result.distances()[person2_id]

    if float('inf') == distance:
        print(f"\n  ❌ No connection path exists between {person1} and {person2}")
        print(f"     (They are in different social circles)")
        return

    # Reconstruct path
    path = reconstruct_path(result.predecessors(), person1_id, person2_id)
    path_list = path.to_pylist()

    print(f"\n  ✅ Connection found!")
    print(f"     Degrees of separation: {int(distance)}")
    print(f"     Path: ", end="")

    path_names = []
    for i, v in enumerate(path_list):
        name = vertices.table()['name'][v].as_py()
        path_names.append(name)

    print(" → ".join(path_names))


def network_traversal_comparison(graph, start_person):
    """Compare BFS vs DFS traversal order."""
    print("\n" + "=" * 70)
    print(f"BFS VS DFS TRAVERSAL FROM {start_person.upper()}")
    print("=" * 70)

    csr = graph.csr()
    vertices = graph.vertices()

    # Find start person ID
    start_id = None
    for v in range(graph.num_vertices()):
        if vertices.table()['name'][v].as_py() == start_person:
            start_id = v
            break

    if start_id is None:
        print(f"  Person '{start_person}' not found!")
        return

    # BFS traversal
    bfs_result = bfs(csr.indptr, csr.indices, graph.num_vertices(), start_id)
    bfs_order = bfs_result.vertices().to_pylist()

    # DFS traversal
    dfs_result = dfs(csr.indptr, csr.indices, graph.num_vertices(), start_id)
    dfs_order = dfs_result.vertices().to_pylist()

    print(f"\nBFS Order (level-by-level exploration):")
    print(f"  ", end="")
    bfs_names = [vertices.table()['name'][v].as_py() for v in bfs_order]
    print(" → ".join(bfs_names))

    print(f"\nDFS Order (depth-first exploration):")
    print(f"  ", end="")
    dfs_names = [vertices.table()['name'][v].as_py() for v in dfs_order]
    print(" → ".join(dfs_names))

    print(f"\nInterpretation:")
    print(f"  - BFS: Explores by relationship distance (all friends first, then friends-of-friends)")
    print(f"  - DFS: Explores one branch deeply before backtracking")


def main():
    print("\n" + "=" * 70)
    print("SOCIAL NETWORK ANALYSIS DEMO")
    print("Powered by Sabot Graph Traversal Algorithms")
    print("=" * 70)

    # Create social network
    graph = create_social_network()

    # 1. Analyze network structure
    analyze_network_structure(graph)

    # 2. Find communities
    find_communities(graph)

    # 3. Friends-of-friends analysis
    find_friends_of_friends(graph, "Alice")

    # 4. Find shortest connection paths
    shortest_connection_path(graph, "Alice", "Dave")
    shortest_connection_path(graph, "Alice", "Eve")
    shortest_connection_path(graph, "Alice", "Grace")

    # 5. Compare BFS vs DFS
    network_traversal_comparison(graph, "Alice")

    print("\n" + "=" * 70)
    print("✅ Analysis Complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
