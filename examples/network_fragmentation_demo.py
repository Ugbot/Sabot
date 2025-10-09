"""
Network Fragmentation Analysis using Connected Components

Demonstrates how to use connected components to:
1. Detect network fragmentation and isolation
2. Identify critical infrastructure failures
3. Analyze network resilience
4. Find disconnected subnetworks

Real-world use cases:
- Infrastructure monitoring (power grids, networks)
- Disaster response (road network connectivity)
- Social network analysis (echo chambers, isolated communities)
- Supply chain resilience (disruption analysis)
"""

import pyarrow as pa
from sabot._cython.graph.storage.graph_storage import PyPropertyGraph, PyVertexTable, PyEdgeTable
from sabot._cython.graph.traversal.connected_components import (
    connected_components, largest_component, component_statistics, count_isolated_vertices
)


def create_communication_network():
    """
    Create a communication network with potential fragmentation.

    Network structure:
    - Main Network: Cities 0-4 (5 cities)
    - Remote Region 1: Cities 5-7 (3 cities)
    - Remote Region 2: Cities 8-9 (2 cities)
    - Isolated: City 10
    """
    vertices = pa.table({
        'id': list(range(11)),
        'label': pa.array(["City"] * 11).dictionary_encode(),
        'name': ["NYC", "Boston", "Philly", "DC", "Baltimore",  # Main network
                 "Denver", "SLC", "Phoenix",                      # Remote 1
                 "Seattle", "Portland",                           # Remote 2
                 "Anchorage"],                                    # Isolated
        'population': [8_400_000, 692_000, 1_600_000, 700_000, 600_000,
                       716_000, 200_000, 1_700_000,
                       750_000, 660_000,
                       290_000],
        'region': ["East", "East", "East", "East", "East",
                   "West", "West", "West",
                   "Northwest", "Northwest",
                   "Alaska"],
    })

    # Communication links (undirected)
    edges = pa.table({
        'src': [
            # Main network (East Coast) - fully connected
            0, 1, 0, 2, 0, 3, 0, 4, 1, 2, 1, 3, 2, 3, 2, 4, 3, 4,
            # Remote Region 1 (Mountain West)
            5, 6, 5, 7, 6, 7,
            # Remote Region 2 (Pacific Northwest)
            8, 9,
            # No links to Anchorage (isolated)
        ],
        'dst': [
            # Main network reverse
            1, 0, 2, 0, 3, 0, 4, 0, 2, 1, 3, 1, 3, 2, 4, 2, 4, 3,
            # Remote 1 reverse
            6, 5, 7, 5, 7, 6,
            # Remote 2 reverse
            9, 8,
        ],
        'type': pa.array(["CommLink"] * 26).dictionary_encode(),
        'bandwidth_gbps': [10.0] * 26,  # All links 10 Gbps
    })

    return PyPropertyGraph(PyVertexTable(vertices), PyEdgeTable(edges)), vertices


def analyze_network_connectivity(graph, vertices):
    """Analyze overall network connectivity."""
    print("=" * 70)
    print("NETWORK CONNECTIVITY ANALYSIS")
    print("=" * 70)

    csr = graph.csr()

    result = connected_components(csr.indptr, csr.indices, graph.num_vertices())

    print(f"\n📊 Network Statistics:")
    print(f"  Total cities: {result.num_vertices()}")
    print(f"  Number of disconnected networks: {result.num_components()}")
    print(f"  Largest network size: {result.largest_component_size()}")

    isolated = count_isolated_vertices(result.component_sizes())
    print(f"  Isolated cities: {isolated}")

    # Get component stats
    stats = component_statistics(result.component_ids(), result.num_components())

    print(f"\n📡 Network Segments:")
    component_ids = result.component_ids().to_pylist()

    # Group cities by component
    components = {}
    for i in range(graph.num_vertices()):
        comp_id = component_ids[i]
        if comp_id not in components:
            components[comp_id] = []
        components[comp_id].append({
            'id': i,
            'name': vertices['name'][i].as_py(),
            'population': vertices['population'][i].as_py(),
            'region': vertices['region'][i].as_py(),
        })

    # Sort by size (largest first)
    sorted_components = sorted(components.items(), key=lambda x: len(x[1]), reverse=True)

    for comp_id, cities in sorted_components:
        size = len(cities)
        total_pop = sum(c['population'] for c in cities)
        regions = set(c['region'] for c in cities)

        if size == 1:
            status = "🔴 ISOLATED"
        elif size >= 5:
            status = "🟢 MAJOR NETWORK"
        else:
            status = "🟡 MINOR NETWORK"

        print(f"\n{status} - Network {comp_id} ({size} cities, {total_pop:,} people)")
        print(f"  Regions: {', '.join(sorted(regions))}")
        print(f"  Cities:")
        for city in sorted(cities, key=lambda c: c['population'], reverse=True):
            print(f"    - {city['name']} ({city['region']}) - Pop: {city['population']:,}")

    return result, components


def analyze_network_resilience(graph, vertices, components):
    """Analyze network resilience."""
    print("\n" + "=" * 70)
    print("NETWORK RESILIENCE ANALYSIS")
    print("=" * 70)

    # Analyze each component's resilience
    print(f"\n🔍 Resilience by Network:")

    for comp_id, cities in sorted(components.items(), key=lambda x: len(x[1]), reverse=True):
        size = len(cities)
        city_names = [c['name'] for c in cities]

        if size == 1:
            resilience = "🔴 NONE - Isolated"
            recommendation = "Add redundant links to other networks"
        elif size == 2:
            resilience = "🟡 LOW - Single link"
            recommendation = "Add backup link for redundancy"
        elif size >= 5:
            resilience = "🟢 HIGH - Multiple paths"
            recommendation = "Maintain redundant links"
        else:
            resilience = "🟡 MEDIUM - Limited redundancy"
            recommendation = "Add cross-region links"

        print(f"\n  Network {comp_id} ({', '.join(city_names[:3])}{'...' if len(city_names) > 3 else ''}):")
        print(f"    Size: {size} cities")
        print(f"    Resilience: {resilience}")
        print(f"    Recommendation: {recommendation}")


def recommend_redundancy(graph, vertices, components):
    """Recommend links to add for redundancy."""
    print("\n" + "=" * 70)
    print("REDUNDANCY RECOMMENDATIONS")
    print("=" * 70)

    # Find disconnected networks
    disconnected = [comp for comp_id, comp in components.items() if len(comp) > 1 and len(comp) < 5]

    print(f"\n💡 Recommended New Links:")

    if len(components) > 1:
        # Recommend connecting networks
        main_network = max(components.values(), key=len)
        main_cities = [c['name'] for c in main_network]

        for comp_id, cities in components.items():
            if cities == main_network:
                continue

            # Find closest cities to connect
            print(f"\n  Connect to Main Network:")
            for city in cities:
                print(f"    - Add link: {city['name']} <-> {main_cities[0]}")
                print(f"      Benefit: Connects {city['region']} region to main network")
    else:
        print(f"\n  ✓ All cities are connected!")

    print(f"\n📈 Priority Recommendations:")
    print(f"  1. Connect isolated cities (Anchorage)")
    print(f"  2. Add redundant links to prevent single points of failure")
    print(f"  3. Connect remote regions to main network")


def main():
    print("\n" + "=" * 70)
    print("COMMUNICATION NETWORK: Fragmentation Analysis Demo")
    print("Using Connected Components for Infrastructure Monitoring")
    print("=" * 70)

    # Create network
    graph, vertices = create_communication_network()

    # Analyze connectivity
    result, components = analyze_network_connectivity(graph, vertices)

    # Analyze resilience
    analyze_network_resilience(graph, vertices, components)

    # Recommend improvements
    recommend_redundancy(graph, vertices, components)

    print("\n" + "=" * 70)
    print("KEY INSIGHTS")
    print("=" * 70)
    print("""
Connected components reveal:
1. Network is fragmented into 4 disconnected segments
2. East Coast has the largest connected network (5 cities, 11.9M people)
3. Anchorage is completely isolated (290K people unreachable)
4. West and Northwest regions form separate networks

Recommended Actions:
1. Connect Anchorage to Seattle (enables Alaska communication)
2. Link Denver to DC (connects West to East Coast)
3. Link Seattle to NYC (connects Pacific Northwest)
4. Add redundant East Coast links (improve resilience)

Applications:
- Infrastructure monitoring: Detect connectivity issues
- Disaster response: Identify isolated regions
- Network planning: Find where to add redundant links
- Capacity planning: Balance traffic across regions
    """)

    print("=" * 70)


if __name__ == "__main__":
    main()
