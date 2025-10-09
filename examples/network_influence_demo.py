"""
Network Influence Analysis with Betweenness Centrality

Demonstrates using betweenness centrality to identify influential connectors
and bridge nodes in a social/organizational network.
"""
import pyarrow as pa
from sabot._cython.graph.storage.graph_storage import PyPropertyGraph, PyVertexTable, PyEdgeTable
from sabot._cython.graph.traversal.centrality import (
    betweenness_centrality, edge_betweenness_centrality, top_k_betweenness
)
from sabot._cython.graph.traversal.pagerank import pagerank, top_k_pagerank


def create_organizational_network():
    """
    Create an organizational network with departments and cross-functional teams.

    Structure:
    - Engineering (Alice, Bob, Carol, Dave)
    - Product (Eve, Frank, Grace)
    - Sales (Henry, Iris)
    - Executive (Jack - CEO)

    Key connectors:
    - Alice: Engineering lead, works with Product
    - Eve: Product lead, works with Engineering and Sales
    - Jack: CEO, connected to all department leads
    """
    vertices = pa.table({
        'id': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        'label': pa.array(["Person"] * 10).dictionary_encode(),
        'name': ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack"],
        'department': ["Engineering", "Engineering", "Engineering", "Engineering",
                      "Product", "Product", "Product",
                      "Sales", "Sales",
                      "Executive"],
        'title': ["Eng Lead", "Eng", "Eng", "Eng",
                 "Product Lead", "PM", "PM",
                 "Sales Lead", "Sales",
                 "CEO"],
        'seniority': [3, 2, 2, 1, 3, 2, 2, 3, 2, 5]
    })

    # Communication/collaboration edges (undirected)
    edges = pa.table({
        'src': [
            # Engineering internal
            0, 1, 0, 2, 0, 3, 1, 2,
            # Product internal
            4, 5, 4, 6,
            # Sales internal
            7, 8,
            # Cross-functional
            0, 4,  # Alice <-> Eve (Eng-Product)
            4, 7,  # Eve <-> Henry (Product-Sales)
            # CEO connections
            9, 0,  # Jack <-> Alice
            9, 4,  # Jack <-> Eve
            9, 7,  # Jack <-> Henry
            # Reverse edges (undirected)
            1, 0, 2, 0, 3, 0, 2, 1,
            5, 4, 6, 4,
            8, 7,
            4, 0,
            7, 4,
            0, 9,
            4, 9,
            7, 9
        ],
        'dst': [
            # Engineering internal
            1, 0, 2, 0, 3, 0, 2, 1,
            # Product internal
            5, 4, 6, 4,
            # Sales internal
            8, 7,
            # Cross-functional
            4, 0,
            7, 4,
            # CEO connections
            0, 9,
            4, 9,
            7, 9,
            # Reverse edges
            0, 1, 0, 2, 0, 3, 1, 2,
            4, 5, 4, 6,
            7, 8,
            0, 4,
            4, 7,
            9, 0,
            9, 4,
            9, 7
        ],
        'type': pa.array(["COLLABORATES"] * 48).dictionary_encode(),
        'interactions_per_week': [5, 5, 4, 4, 3, 3, 2, 2,  # Engineering (8)
                                  6, 6, 4, 4,              # Product (4)
                                  5, 5,                    # Sales (2)
                                  8, 8,                    # Alice-Eve (2)
                                  6, 6,                    # Eve-Henry (2)
                                  3, 3,                    # Jack-Alice (2)
                                  4, 4,                    # Jack-Eve (2)
                                  2, 2,                    # Jack-Henry (2)
                                  # Reverse (24)
                                  5, 5, 4, 4, 3, 3, 2, 2,  # Engineering
                                  6, 6, 4, 4,              # Product
                                  5, 5,                    # Sales
                                  8, 8,                    # Alice-Eve
                                  6, 6,                    # Eve-Henry
                                  3, 3,                    # Jack-Alice
                                  4, 4,                    # Jack-Eve
                                  2, 2]                    # Jack-Henry
    })

    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    return PyPropertyGraph(vertex_table, edge_table)


def analyze_influence_vs_connectivity():
    """Compare PageRank (influence) vs Betweenness (connectivity/bridging)."""
    print("=" * 70)
    print("INFLUENCE VS CONNECTIVITY ANALYSIS")
    print("=" * 70)

    graph = create_organizational_network()
    csr = graph.csr()
    vertices = graph.vertices()

    # Compute PageRank (influence/importance)
    pr_result = pagerank(csr.indptr, csr.indices, graph.num_vertices())
    pageranks = pr_result.ranks().to_pylist()

    # Compute Betweenness (bridging/connectivity)
    bc_result = betweenness_centrality(csr.indptr, csr.indices, graph.num_vertices())
    betweenness = bc_result.centrality().to_pylist()

    print("\n📊 PageRank vs Betweenness Centrality:\n")
    print(f"{'Name':<12} {'Dept':<12} {'PageRank':<12} {'Betweenness':<12} {'Role'}")
    print("-" * 70)

    for i in range(graph.num_vertices()):
        name = vertices.table()['name'][i].as_py()
        dept = vertices.table()['department'][i].as_py()
        pr = pageranks[i]
        bc = betweenness[i]

        # Determine role based on metrics
        if pr > 0.15 and bc > 0.4:
            role = "🌟 Key Leader"
        elif pr > 0.12:
            role = "👑 Influential"
        elif bc > 0.3:
            role = "🌉 Bridge"
        elif bc > 0.1:
            role = "🔗 Connector"
        else:
            role = "👤 Team Member"

        print(f"{name:<12} {dept:<12} {pr:<12.4f} {bc:<12.4f} {role}")

    print("\n💡 Insights:")
    print("  - PageRank: Measures overall influence (who's important)")
    print("  - Betweenness: Measures bridging power (who connects groups)")
    print("  - High both: Critical leaders (remove = network fragmentation)")
    print("  - High betweenness only: Key connectors between silos")


def identify_key_connectors():
    """Identify the most critical connectors in the organization."""
    print("\n" + "=" * 70)
    print("KEY CONNECTOR IDENTIFICATION")
    print("=" * 70)

    graph = create_organizational_network()
    csr = graph.csr()
    vertices = graph.vertices()

    # Compute betweenness
    bc_result = betweenness_centrality(csr.indptr, csr.indices, graph.num_vertices())

    # Get top 5 connectors
    top_5 = top_k_betweenness(bc_result.centrality(), k=5)

    print("\n🌉 Top 5 Connectors (by Betweenness Centrality):\n")

    for i in range(len(top_5)):
        vertex_id = top_5['vertex_id'][i].as_py()
        score = top_5['betweenness'][i].as_py()

        name = vertices.table()['name'][vertex_id].as_py()
        dept = vertices.table()['department'][vertex_id].as_py()
        title = vertices.table()['title'][vertex_id].as_py()

        print(f"  {i+1}. {name} - {title} ({dept})")
        print(f"     Betweenness: {score:.4f}")
        print(f"     Impact: Connects {int(score * 100)}% of shortest communication paths")
        print()

    print("⚠️  Organizational Risk:")
    print("  If these connectors leave, communication silos may form.")
    print("  Consider cross-training and building redundant connections.")


def detect_communication_silos():
    """Detect communication bottlenecks using edge betweenness."""
    print("\n" + "=" * 70)
    print("COMMUNICATION BOTTLENECK DETECTION")
    print("=" * 70)

    graph = create_organizational_network()
    csr = graph.csr()
    vertices = graph.vertices()

    # Compute edge betweenness
    edge_bc = edge_betweenness_centrality(csr.indptr, csr.indices, graph.num_vertices())

    # Sort by betweenness descending
    betweenness_scores = edge_bc['betweenness'].to_pylist()
    src_list = edge_bc['src'].to_pylist()
    dst_list = edge_bc['dst'].to_pylist()

    # Create sorted list of edges
    edges_with_scores = list(zip(src_list, dst_list, betweenness_scores))
    edges_with_scores.sort(key=lambda x: x[2], reverse=True)

    print("\n🚧 Critical Communication Links (Top 5):\n")
    print("These edges carry the most inter-group communication.\n")

    for i, (src, dst, score) in enumerate(edges_with_scores[:5]):
        if score == 0:
            break

        src_name = vertices.table()['name'][src].as_py()
        dst_name = vertices.table()['name'][dst].as_py()
        src_dept = vertices.table()['department'][src].as_py()
        dst_dept = vertices.table()['department'][dst].as_py()

        if src_dept != dst_dept:
            connection_type = f"🔗 Cross-Dept ({src_dept} ↔ {dst_dept})"
        else:
            connection_type = f"📍 Intra-Dept ({src_dept})"

        print(f"  {i+1}. {src_name} ↔ {dst_name}")
        print(f"     {connection_type}")
        print(f"     Edge Betweenness: {score:.2f}")
        print()

    print("💡 Recommendation:")
    print("  Strengthen these connections with:")
    print("  - Regular sync meetings")
    print("  - Shared collaboration spaces")
    print("  - Cross-functional project assignments")


def simulate_key_person_departure():
    """Simulate impact of losing a key connector."""
    print("\n" + "=" * 70)
    print("KEY PERSON DEPARTURE SIMULATION")
    print("=" * 70)

    graph = create_organizational_network()
    csr = graph.csr()
    vertices = graph.vertices()

    # Find person with highest betweenness
    bc_result = betweenness_centrality(csr.indptr, csr.indices, graph.num_vertices())
    betweenness = bc_result.centrality().to_pylist()

    key_person_id = betweenness.index(max(betweenness))
    key_person_name = vertices.table()['name'][key_person_id].as_py()
    key_person_dept = vertices.table()['department'][key_person_id].as_py()

    print(f"\n🎯 Simulating departure of: {key_person_name} ({key_person_dept})")
    print(f"   Original Betweenness: {betweenness[key_person_id]:.4f}")

    # Create modified graph without key person
    # (In real scenario, would remove vertex and recompute)
    # For demo, just show the impact

    print(f"\n⚠️  Impact Assessment:")
    print(f"   - Communication paths affected: {int(betweenness[key_person_id] * 100)}%")
    print(f"   - Departments affected: {key_person_dept} and cross-functional teams")
    print(f"   - Risk: High potential for communication silos")

    # Find backup connectors
    print(f"\n🔄 Potential Backup Connectors:")
    backup_candidates = [(i, betweenness[i]) for i in range(len(betweenness)) if i != key_person_id]
    backup_candidates.sort(key=lambda x: x[1], reverse=True)

    for i, (person_id, score) in enumerate(backup_candidates[:3]):
        name = vertices.table()['name'][person_id].as_py()
        dept = vertices.table()['department'][person_id].as_py()
        print(f"   {i+1}. {name} ({dept}) - Betweenness: {score:.4f}")

    print(f"\n💡 Succession Plan:")
    print(f"   - Train backup connectors in {key_person_name}'s role")
    print(f"   - Establish direct connections to reduce dependency")
    print(f"   - Create documentation for critical processes")


def recommend_new_connections():
    """Recommend new connections to improve network resilience."""
    print("\n" + "=" * 70)
    print("NETWORK RESILIENCE RECOMMENDATIONS")
    print("=" * 70)

    graph = create_organizational_network()
    csr = graph.csr()
    vertices = graph.vertices()

    print("\n🔧 Recommended New Connections:\n")

    # Analyze departments with few cross-connections
    departments = {}
    for i in range(graph.num_vertices()):
        dept = vertices.table()['department'][i].as_py()
        if dept not in departments:
            departments[dept] = []
        departments[dept].append(i)

    print("1. Engineering ↔ Sales: Direct connection needed")
    print("   Current: All communication goes through Product")
    print("   Recommendation: Bob (Engineering) ↔ Iris (Sales)")
    print("   Benefit: Faster feedback loop, reduced load on Product team")
    print()

    print("2. Executive ↔ Engineering: Strengthen connection")
    print("   Current: CEO only connected to Eng Lead")
    print("   Recommendation: Jack (CEO) ↔ Bob (Engineering)")
    print("   Benefit: Better visibility into technical progress")
    print()

    print("3. Product ↔ Sales: Add redundancy")
    print("   Current: Single point of contact (Eve ↔ Henry)")
    print("   Recommendation: Frank (PM) ↔ Iris (Sales)")
    print("   Benefit: Backup communication path")
    print()

    print("💡 Implementation Strategy:")
    print("  - Schedule regular 1-on-1s between recommended pairs")
    print("  - Create shared Slack channels")
    print("  - Assign collaborative projects")
    print("  - Measure: Re-run betweenness analysis in 3 months")


def main():
    print("\n" + "=" * 70)
    print("NETWORK INFLUENCE & CONNECTIVITY ANALYSIS")
    print("Understanding Organizational Communication Dynamics")
    print("=" * 70)

    # 1. Compare influence vs connectivity
    analyze_influence_vs_connectivity()

    # 2. Identify key connectors
    identify_key_connectors()

    # 3. Detect communication bottlenecks
    detect_communication_silos()

    # 4. Simulate key person departure
    simulate_key_person_departure()

    # 5. Recommend improvements
    recommend_new_connections()

    print("\n" + "=" * 70)
    print("✅ Analysis Complete!")
    print("=" * 70)
    print("\n📚 Key Takeaways:")
    print("  1. Betweenness identifies critical connectors, not just influential people")
    print("  2. High betweenness = single point of failure risk")
    print("  3. Edge betweenness reveals communication bottlenecks")
    print("  4. Building redundant connections improves resilience")
    print("  5. Regular network analysis helps prevent silos")
    print()


if __name__ == "__main__":
    main()
