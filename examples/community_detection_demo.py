"""
Community Detection using Triangle Counting

Demonstrates how to use triangle counting and clustering coefficients to:
1. Identify tightly-knit communities
2. Detect network cohesion patterns
3. Find bridge vertices connecting communities
4. Analyze social network structure

Real-world use cases:
- Social network analysis (friend groups, communities)
- Fraud detection (identifying fraud rings)
- Recommendation systems (collaborative filtering clusters)
- Organizational analysis (team cohesion, silos)
"""

import pyarrow as pa
from sabot._cython.graph.storage.graph_storage import PyPropertyGraph, PyVertexTable, PyEdgeTable
from sabot._cython.graph.traversal.triangle_counting import (
    count_triangles, top_k_triangle_counts, compute_transitivity
)
from sabot._cython.graph.traversal.centrality import betweenness_centrality


def create_research_collaboration_network():
    """
    Create a research collaboration network with distinct communities.

    Communities:
    - ML Research Lab: Alice, Bob, Carol, David (4 people)
    - Systems Lab: Eve, Frank, Grace (3 people)
    - Theory Lab: Henry, Iris (2 people)
    - Bridge: Julia connects ML and Systems
    """
    # 10 researchers
    vertices = pa.table({
        'id': list(range(10)),
        'label': pa.array(["Researcher"] * 10).dictionary_encode(),
        'name': ["Alice", "Bob", "Carol", "David",  # ML Lab
                 "Eve", "Frank", "Grace",            # Systems Lab
                 "Henry", "Iris",                     # Theory Lab
                 "Julia"],                            # Bridge
        'lab': ["ML", "ML", "ML", "ML",
                "Systems", "Systems", "Systems",
                "Theory", "Theory",
                "Bridge"],
        'papers_published': [45, 38, 42, 35, 52, 48, 40, 28, 25, 60],
    })

    # Collaboration edges (undirected)
    # ML Lab: Highly collaborative (complete subgraph)
    # Systems Lab: Moderately collaborative
    # Theory Lab: Small, tight collaboration
    # Julia bridges ML and Systems
    edges = pa.table({
        'src': [
            # ML Lab (complete graph: 0,1,2,3)
            0, 1, 0, 2, 0, 3, 1, 2, 1, 3, 2, 3,
            # Systems Lab (triangle: 4,5,6)
            4, 5, 4, 6, 5, 6,
            # Theory Lab (pair: 7,8)
            7, 8,
            # Julia bridges (9 connects to ML: 0,1 and Systems: 4,5)
            9, 0, 9, 1, 9, 4, 9, 5,
        ],
        'dst': [
            # ML Lab reverse
            1, 0, 2, 0, 3, 0, 2, 1, 3, 1, 3, 2,
            # Systems Lab reverse
            5, 4, 6, 4, 6, 5,
            # Theory Lab reverse
            8, 7,
            # Julia reverse
            0, 9, 1, 9, 4, 9, 5, 9,
        ],
        'type': pa.array(["CoAuthor"] * 28).dictionary_encode(),
        'num_collaborations': [
            # ML Lab
            5, 5, 8, 8, 6, 6, 7, 7, 4, 4, 3, 3,
            # Systems Lab
            6, 6, 4, 4, 5, 5,
            # Theory Lab
            10, 10,
            # Julia bridges
            3, 3, 2, 2, 4, 4, 3, 3,
        ],
    })

    return PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges)), vertices


def analyze_community_structure(graph, vertices):
    """Analyze community structure using triangle counting."""
    print("=" * 70)
    print("COMMUNITY STRUCTURE ANALYSIS")
    print("=" * 70)

    csr = graph.csr()

    # Count triangles
    result = count_triangles(csr.indptr, csr.indices, graph.num_vertices())

    print(f"\n📊 Global Network Statistics:")
    print(f"  Total triangles: {result.total_triangles()}")
    print(f"  Global clustering coefficient: {result.global_clustering_coefficient():.4f}")
    print(f"  Transitivity: {compute_transitivity(csr.indptr, csr.indices, graph.num_vertices()):.4f}")

    triangles = result.per_vertex_triangles().to_pylist()
    clustering = result.clustering_coefficient().to_pylist()

    # Analyze per-person
    print(f"\n👥 Per-Researcher Analysis:")
    print(f"{'Name':<10} {'Lab':<10} {'Papers':<8} {'Triangles':<10} {'Clustering':<12} {'Community Role'}")
    print("-" * 70)

    for i in range(graph.num_vertices()):
        name = vertices['name'][i].as_py()
        lab = vertices['lab'][i].as_py()
        papers = vertices['papers_published'][i].as_py()
        tri = triangles[i]
        clust = clustering[i]

        # Classify role
        if clust > 0.9:
            role = "🔵 Core Member"
        elif clust > 0.5:
            role = "🟢 Active Member"
        elif clust > 0.0:
            role = "🟡 Peripheral"
        else:
            role = "🔴 Bridge/Isolate"

        print(f"{name:<10} {lab:<10} {papers:<8} {tri:<10} {clust:<12.4f} {role}")

    return result


def identify_communities(graph, vertices, triangle_result):
    """Identify communities based on clustering coefficients."""
    print("\n" + "=" * 70)
    print("COMMUNITY IDENTIFICATION")
    print("=" * 70)

    clustering = triangle_result.clustering_coefficient().to_pylist()
    triangles = triangle_result.per_vertex_triangles().to_pylist()

    # Group researchers by lab and compute community metrics
    communities = {}
    for i in range(graph.num_vertices()):
        lab = vertices['lab'][i].as_py()
        if lab not in communities:
            communities[lab] = {
                'members': [],
                'avg_clustering': 0.0,
                'total_triangles': 0,
            }
        communities[lab]['members'].append(vertices['name'][i].as_py())
        communities[lab]['avg_clustering'] += clustering[i]
        communities[lab]['total_triangles'] += triangles[i]

    # Compute averages
    for lab, data in communities.items():
        if len(data['members']) > 0:
            data['avg_clustering'] /= len(data['members'])

    # Sort by average clustering (cohesion)
    sorted_communities = sorted(
        communities.items(),
        key=lambda x: x[1]['avg_clustering'],
        reverse=True
    )

    print("\n🏢 Communities Ranked by Cohesion:")
    print(f"{'Lab':<15} {'Members':<5} {'Avg Clustering':<18} {'Total Triangles':<18} {'Cohesion'}")
    print("-" * 70)

    for lab, data in sorted_communities:
        cohesion = "🔥 Very High" if data['avg_clustering'] > 0.9 else \
                   "⭐ High" if data['avg_clustering'] > 0.5 else \
                   "✓ Moderate" if data['avg_clustering'] > 0.2 else \
                   "⚠️ Low"

        print(f"{lab:<15} {len(data['members']):<5} {data['avg_clustering']:<18.4f} "
              f"{data['total_triangles']:<18} {cohesion}")
        print(f"  Members: {', '.join(data['members'])}")

    return communities


def detect_bridges(graph, vertices):
    """Detect bridge vertices connecting communities using betweenness."""
    print("\n" + "=" * 70)
    print("BRIDGE DETECTION")
    print("=" * 70)

    csr = graph.csr()

    # Compute betweenness centrality
    centrality_result = betweenness_centrality(
        csr.indptr, csr.indices, graph.num_vertices(), normalized=True
    )

    betweenness = centrality_result.centrality().to_pylist()

    # Get triangle clustering
    triangle_result = count_triangles(csr.indptr, csr.indices, graph.num_vertices())
    clustering = triangle_result.clustering_coefficient().to_pylist()

    print("\n🌉 Bridge Analysis (High Betweenness + Low Clustering):")
    print(f"{'Name':<10} {'Lab':<10} {'Betweenness':<15} {'Clustering':<12} {'Role'}")
    print("-" * 70)

    bridges = []
    for i in range(graph.num_vertices()):
        name = vertices['name'][i].as_py()
        lab = vertices['lab'][i].as_py()
        bet = betweenness[i]
        clust = clustering[i]

        # Bridge: high betweenness (>0.1), low clustering (<0.5)
        if bet > 0.1 and clust < 0.5:
            role = "🌉 BRIDGE VERTEX"
            bridges.append((name, bet, clust))
            print(f"{name:<10} {lab:<10} {bet:<15.4f} {clust:<12.4f} {role}")
        elif bet > 0.05:
            role = "🔗 Connector"
            print(f"{name:<10} {lab:<10} {bet:<15.4f} {clust:<12.4f} {role}")

    if bridges:
        print(f"\n⚠️  Critical Bridges: {len(bridges)} vertices")
        print("   Removing these would disconnect communities!")
    else:
        print("\n✓ No critical bridges detected")

    return bridges


def analyze_cohesion_vs_influence(graph, vertices):
    """Compare triangle-based cohesion with degree-based influence."""
    print("\n" + "=" * 70)
    print("COHESION vs INFLUENCE ANALYSIS")
    print("=" * 70)

    csr = graph.csr()

    # Triangle counting
    triangle_result = count_triangles(csr.indptr, csr.indices, graph.num_vertices())
    triangles = triangle_result.per_vertex_triangles().to_pylist()
    clustering = triangle_result.clustering_coefficient().to_pylist()

    # Compute degrees
    degrees = []
    for i in range(graph.num_vertices()):
        degree = csr.indptr[i + 1].as_py() - csr.indptr[i].as_py()
        degrees.append(degree)

    print("\n📈 Researcher Metrics:")
    print(f"{'Name':<10} {'Degree':<8} {'Triangles':<10} {'Clustering':<12} {'Profile'}")
    print("-" * 70)

    for i in range(graph.num_vertices()):
        name = vertices['name'][i].as_py()
        deg = degrees[i]
        tri = triangles[i]
        clust = clustering[i]

        # Classify profile
        if deg >= 4 and clust > 0.8:
            profile = "🌟 Community Leader"
        elif deg >= 4 and clust < 0.5:
            profile = "🌐 Network Broker"
        elif deg < 4 and clust > 0.8:
            profile = "👥 Team Player"
        elif deg >= 4:
            profile = "🔗 Connector"
        else:
            profile = "🔵 Specialist"

        print(f"{name:<10} {deg:<8} {tri:<10} {clust:<12.4f} {profile}")

    print("\nProfile Definitions:")
    print("  🌟 Community Leader: High connections + high clustering (influential within community)")
    print("  🌐 Network Broker: High connections + low clustering (bridges communities)")
    print("  👥 Team Player: Few connections + high clustering (tight-knit group member)")
    print("  🔗 Connector: High connections, moderate clustering")
    print("  🔵 Specialist: Low connections (focused collaborations)")


def recommend_collaborations(graph, vertices):
    """Recommend new collaborations to strengthen network cohesion."""
    print("\n" + "=" * 70)
    print("COLLABORATION RECOMMENDATIONS")
    print("=" * 70)

    csr = graph.csr()

    # Find researchers with low clustering in same lab
    triangle_result = count_triangles(csr.indptr, csr.indices, graph.num_vertices())
    clustering = triangle_result.clustering_coefficient().to_pylist()

    # Build adjacency info
    neighbors = {}
    for i in range(graph.num_vertices()):
        start = csr.indptr[i].as_py()
        end = csr.indptr[i + 1].as_py()
        neighbors[i] = set(csr.indices[start:end].to_pylist())

    recommendations = []

    # Find pairs in same lab who aren't connected but share neighbors
    for i in range(graph.num_vertices()):
        lab_i = vertices['lab'][i].as_py()
        name_i = vertices['name'][i].as_py()

        for j in range(i + 1, graph.num_vertices()):
            lab_j = vertices['lab'][j].as_py()
            name_j = vertices['name'][j].as_py()

            # Same lab, not connected
            if lab_i == lab_j and lab_i != "Bridge" and j not in neighbors[i]:
                # Count common neighbors
                common = len(neighbors[i] & neighbors[j])
                if common >= 2:
                    recommendations.append((name_i, name_j, lab_i, common))

    if recommendations:
        print("\n💡 Recommended Collaborations (to form triangles):")
        print(f"{'Person 1':<10} {'Person 2':<10} {'Lab':<10} {'Common Collaborators'}")
        print("-" * 70)

        for name1, name2, lab, common in sorted(recommendations, key=lambda x: x[3], reverse=True):
            print(f"{name1:<10} {name2:<10} {lab:<10} {common}")

        print(f"\n✨ These {len(recommendations)} collaborations would create new triangles,")
        print("   strengthening community cohesion and research synergy!")
    else:
        print("\n✓ Network is well-connected! No obvious collaboration gaps.")


def main():
    print("\n" + "=" * 70)
    print("RESEARCH COLLABORATION NETWORK: Community Detection Demo")
    print("Using Triangle Counting and Clustering Coefficients")
    print("=" * 70)

    # Create network
    graph, vertices = create_research_collaboration_network()
    print(f"\nNetwork: {graph.num_vertices()} researchers, {graph.num_edges()} collaborations")

    # Analyze community structure
    triangle_result = analyze_community_structure(graph, vertices)

    # Identify communities
    communities = identify_communities(graph, vertices, triangle_result)

    # Detect bridges
    bridges = detect_bridges(graph, vertices)

    # Compare cohesion vs influence
    analyze_cohesion_vs_influence(graph, vertices)

    # Recommend collaborations
    recommend_collaborations(graph, vertices)

    print("\n" + "=" * 70)
    print("KEY INSIGHTS")
    print("=" * 70)
    print("""
Triangle counting reveals:
1. ML Lab has highest cohesion (complete graph → max triangles)
2. Systems Lab has moderate cohesion (triangle structure)
3. Theory Lab is small but tightly connected
4. Julia is a critical bridge (high betweenness, low clustering)
5. Removing Julia would fragment the network

Applications:
- Organizational design: Identify team cohesion and silos
- Fraud detection: Fraud rings show high clustering
- Recommendation systems: Triangle closure predicts new connections
- Social network analysis: Find communities and influencers
    """)

    print("=" * 70)


if __name__ == "__main__":
    main()
