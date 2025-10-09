"""
Supply Chain Routing Demo - Variable-Length Path Matching

Demonstrates using variable-length path matching to find shipping routes
in a supply chain network with warehouses and distribution centers.

Use Cases:
1. Find all possible shipping routes within N hops
2. Identify shortest/longest routes between locations
3. Discover alternative routes when primary routes are unavailable
4. Calculate route diversity and redundancy
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set library path for Arrow
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib'

import pyarrow as pa
from sabot._cython.graph.query import match_variable_length_path


def create_supply_chain_network():
    """
    Create a supply chain network with warehouses and distribution centers.

    Network topology:
    - 3 Manufacturing plants (M1, M2, M3)
    - 5 Regional warehouses (W1-W5)
    - 4 Distribution centers (D1-D4)
    - 3 Retail stores (R1-R3)

    Edges represent shipping routes with costs/times.
    """
    # Location IDs:
    # 0-2: Manufacturing plants (M1-M3)
    # 3-7: Regional warehouses (W1-W5)
    # 8-11: Distribution centers (D1-D4)
    # 12-14: Retail stores (R1-R3)

    edges = pa.table({
        'source': pa.array([
            # Manufacturing → Warehouses
            0, 0, 1, 1, 2, 2,
            # Warehouses → Distribution Centers
            3, 3, 4, 4, 5, 5, 6, 7,
            # Distribution Centers → Retail
            8, 8, 9, 9, 10, 11,
            # Cross-warehouse routes (redundancy)
            3, 4, 5,
        ], type=pa.int64()),
        'target': pa.array([
            # Manufacturing → Warehouses
            3, 4, 4, 5, 5, 6,
            # Warehouses → Distribution Centers
            8, 9, 9, 10, 10, 11, 11, 11,
            # Distribution Centers → Retail
            12, 13, 13, 14, 14, 14,
            # Cross-warehouse routes
            4, 5, 6,
        ], type=pa.int64())
    })

    location_names = {
        0: "Plant_Chicago", 1: "Plant_Dallas", 2: "Plant_Seattle",
        3: "Warehouse_Midwest", 4: "Warehouse_South", 5: "Warehouse_West",
        6: "Warehouse_Central", 7: "Warehouse_North",
        8: "DC_Atlanta", 9: "DC_Houston", 10: "DC_Phoenix", 11: "DC_Denver",
        12: "Store_Miami", 13: "Store_Austin", 14: "Store_LasVegas"
    }

    return edges, location_names


def find_all_routes(edges, location_names, source, target, min_hops, max_hops):
    """Find all routes from source to target within hop range"""
    result = match_variable_length_path(
        edges,
        source_vertex=source,
        target_vertex=target,
        min_hops=min_hops,
        max_hops=max_hops
    )

    source_name = location_names[source]
    target_name = location_names[target]

    print(f"\n{'=' * 60}")
    print(f"Routes from {source_name} to {target_name}")
    print(f"Hop range: {min_hops}-{max_hops}")
    print(f"{'=' * 60}")

    if result.num_matches() == 0:
        print("❌ No routes found")
        return []

    print(f"Found {result.num_matches()} route(s):\n")

    table = result.result_table()
    routes = []

    for i in range(result.num_matches()):
        path_id = table.column('path_id').to_pylist()[i]
        hop_count = table.column('hop_count').to_pylist()[i]
        vertices = table.column('vertices').to_pylist()[i]

        # Build route description
        route_desc = " → ".join([location_names[v] for v in vertices])

        print(f"Route {path_id + 1} ({hop_count} hops):")
        print(f"  {route_desc}")

        routes.append({
            'hops': hop_count,
            'vertices': vertices,
            'description': route_desc
        })

    return routes


def analyze_route_diversity(edges, location_names):
    """Analyze route diversity and redundancy"""
    print("\n" + "█" * 60)
    print("█" + " " * 58 + "█")
    print("█" + "  SUPPLY CHAIN ROUTE ANALYSIS".center(58) + "█")
    print("█" + " " * 58 + "█")
    print("█" * 60)

    # 1. Find routes from Chicago plant to Miami store
    print("\n1. PRIMARY SHIPPING ROUTES")
    print("   Chicago Manufacturing → Miami Retail Store")
    chicago_to_miami = find_all_routes(
        edges, location_names,
        source=0,  # Plant_Chicago
        target=12,  # Store_Miami
        min_hops=2,
        max_hops=4
    )

    if chicago_to_miami:
        # Analyze route characteristics
        print(f"\n   Route Analysis:")
        print(f"   • Shortest route: {min(r['hops'] for r in chicago_to_miami)} hops")
        print(f"   • Longest route: {max(r['hops'] for r in chicago_to_miami)} hops")
        print(f"   • Route redundancy: {len(chicago_to_miami)} alternative paths")

    # 2. Find routes from Dallas to Las Vegas
    print("\n2. ALTERNATE SHIPPING ROUTES")
    print("   Dallas Manufacturing → Las Vegas Retail Store")
    dallas_to_vegas = find_all_routes(
        edges, location_names,
        source=1,  # Plant_Dallas
        target=14,  # Store_LasVegas
        min_hops=2,
        max_hops=4
    )

    # 3. Find all destinations reachable from Seattle plant in 3 hops
    print("\n3. REACHABILITY ANALYSIS")
    print("   All locations reachable from Seattle in 3 hops")
    reachable = match_variable_length_path(
        edges,
        source_vertex=2,  # Plant_Seattle
        target_vertex=-1,  # All targets
        min_hops=3,
        max_hops=3
    )

    print(f"\n   Found {reachable.num_matches()} destinations reachable in exactly 3 hops:")
    table = reachable.result_table()
    destinations = set()
    for i in range(reachable.num_matches()):
        vertices = table.column('vertices').to_pylist()[i]
        destination = vertices[-1]
        destinations.add(destination)

    for dest in sorted(destinations):
        print(f"   • {location_names[dest]}")

    # 4. Identify critical warehouses (highest path count)
    print("\n4. CRITICAL WAREHOUSE ANALYSIS")
    print("   Warehouses with most path diversity")

    warehouse_path_counts = {}
    for warehouse_id in range(3, 8):  # W1-W5 (IDs 3-7)
        # Count paths passing through this warehouse
        # Find all paths from any manufacturing plant
        paths_through = 0
        for plant_id in range(0, 3):  # M1-M3
            result = match_variable_length_path(
                edges,
                source_vertex=plant_id,
                target_vertex=-1,
                min_hops=1,
                max_hops=4
            )

            # Count paths that include this warehouse
            for i in range(result.num_matches()):
                vertices = result.result_table().column('vertices').to_pylist()[i]
                if warehouse_id in vertices:
                    paths_through += 1

        warehouse_path_counts[warehouse_id] = paths_through

    # Sort by path count
    sorted_warehouses = sorted(
        warehouse_path_counts.items(),
        key=lambda x: x[1],
        reverse=True
    )

    print("\n   Critical warehouses (by path count):")
    for warehouse_id, count in sorted_warehouses:
        if count > 0:
            print(f"   • {location_names[warehouse_id]}: {count} paths")


def demonstrate_route_failures(edges, location_names):
    """Demonstrate finding alternative routes when primary routes fail"""
    print("\n" + "=" * 60)
    print("5. ROUTE FAILURE SIMULATION")
    print("=" * 60)

    # Original route from Chicago to Austin
    print("\nScenario: Find backup routes when primary route is unavailable")
    print("\nPrimary route (2 hops):")
    primary = find_all_routes(
        edges, location_names,
        source=0,  # Plant_Chicago
        target=13,  # Store_Austin
        min_hops=2,
        max_hops=2
    )

    print("\nBackup routes (3-4 hops):")
    backup = find_all_routes(
        edges, location_names,
        source=0,  # Plant_Chicago
        target=13,  # Store_Austin
        min_hops=3,
        max_hops=4
    )

    if backup:
        print(f"\n✓ Backup routing capability: {len(backup)} alternative route(s)")
    else:
        print("\n⚠️  Warning: No backup routes available!")


def calculate_network_statistics(edges, location_names):
    """Calculate overall network statistics"""
    print("\n" + "=" * 60)
    print("6. NETWORK STATISTICS")
    print("=" * 60)

    total_routes = 0
    max_hops_global = 0

    # Calculate total possible routes (within 5 hops) from all plants to all stores
    for plant_id in range(0, 3):
        for store_id in range(12, 15):
            result = match_variable_length_path(
                edges,
                source_vertex=plant_id,
                target_vertex=store_id,
                min_hops=1,
                max_hops=5
            )

            count = result.num_matches()
            total_routes += count

            if count > 0:
                table = result.result_table()
                max_hops = max(table.column('hop_count').to_pylist())
                max_hops_global = max(max_hops_global, max_hops)

    print(f"\nNetwork Metrics:")
    print(f"  • Total locations: {len(location_names)}")
    print(f"  • Total shipping routes (edges): {edges.num_rows}")
    print(f"  • Total end-to-end paths (plant → store): {total_routes}")
    print(f"  • Maximum path length: {max_hops_global} hops")
    print(f"  • Average routes per plant-store pair: {total_routes / (3 * 3):.1f}")


if __name__ == "__main__":
    edges, location_names = create_supply_chain_network()

    # Run all analyses
    analyze_route_diversity(edges, location_names)
    demonstrate_route_failures(edges, location_names)
    calculate_network_statistics(edges, location_names)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print("""
Variable-length path matching successfully demonstrated:
  ✓ Multi-hop route discovery
  ✓ Route redundancy analysis
  ✓ Reachability analysis
  ✓ Critical node identification
  ✓ Backup route planning

Applications:
  • Supply chain optimization
  • Logistics network design
  • Disaster recovery planning
  • Route diversity analysis
    """)
