"""
City Routing and Navigation with Weighted Shortest Paths

Demonstrates Dijkstra's algorithm and A* for practical route planning
in a city road network with traffic conditions.
"""
import pyarrow as pa
from sabot._cython.graph.storage.graph_storage import PyPropertyGraph, PyVertexTable, PyEdgeTable
from sabot._cython.graph.traversal.shortest_paths import (
    dijkstra, astar, reconstruct_path, floyd_warshall, unweighted_shortest_paths
)


def create_city_network():
    """
    Create a city road network with intersections and roads.

    Intersections (9 locations):
    0: Downtown       (0, 0)
    1: Mall           (3, 1)
    2: Hospital       (1, 3)
    3: University     (4, 4)
    4: Airport        (7, 2)
    5: Park           (2, 5)
    6: Stadium        (5, 6)
    7: Harbor         (8, 5)
    8: Train Station  (6, 3)

    Roads with travel times (minutes) and distances (km)
    """
    vertices = pa.table({
        'id': [0, 1, 2, 3, 4, 5, 6, 7, 8],
        'label': pa.array(["Intersection"] * 9).dictionary_encode(),
        'name': ["Downtown", "Mall", "Hospital", "University", "Airport",
                 "Park", "Stadium", "Harbor", "Train Station"],
        'x': [0, 3, 1, 4, 7, 2, 5, 8, 6],  # X coordinates for A* heuristic
        'y': [0, 1, 3, 4, 2, 5, 6, 5, 3],  # Y coordinates for A* heuristic
        'population': [50000, 30000, 15000, 40000, 10000, 5000, 20000, 8000, 25000]
    })

    # Roads (directed for one-way streets and traffic conditions)
    edges = pa.table({
        'src': [0, 1, 0, 2, 1, 3, 2, 5, 3, 6, 1, 4, 3, 8, 4, 7, 6, 7, 8, 4, 5, 6, 2, 3],
        'dst': [1, 0, 2, 0, 3, 1, 5, 2, 6, 3, 4, 1, 8, 3, 7, 4, 7, 6, 4, 8, 6, 5, 3, 2],
        'type': pa.array(["ROAD"] * 24).dictionary_encode(),
        'travel_time': [
            # Traffic-adjusted travel times (minutes)
            15.0, 12.0,  # Downtown <-> Mall
            20.0, 18.0,  # Downtown <-> Hospital
            25.0, 22.0,  # Mall <-> University
            30.0, 28.0,  # Hospital <-> Park
            35.0, 32.0,  # University <-> Stadium
            40.0, 38.0,  # Mall <-> Airport
            20.0, 18.0,  # University <-> Train Station
            45.0, 40.0,  # Airport <-> Harbor
            50.0, 48.0,  # Stadium <-> Harbor
            30.0, 28.0,  # Train Station <-> Airport
            25.0, 23.0,  # Park <-> Stadium
            35.0, 33.0   # Hospital <-> University
        ],
        'distance_km': [
            10.0, 10.0,  # Downtown <-> Mall
            12.0, 12.0,  # Downtown <-> Hospital
            15.0, 15.0,  # Mall <-> University
            18.0, 18.0,  # Hospital <-> Park
            20.0, 20.0,  # University <-> Stadium
            25.0, 25.0,  # Mall <-> Airport
            14.0, 14.0,  # University <-> Train Station
            30.0, 30.0,  # Airport <-> Harbor
            28.0, 28.0,  # Stadium <-> Harbor
            22.0, 22.0,  # Train Station <-> Airport
            16.0, 16.0,  # Park <-> Stadium
            20.0, 20.0   # Hospital <-> University
        ],
        'road_type': [
            "Highway", "Highway",
            "Main Street", "Main Street",
            "Avenue", "Avenue",
            "Local Road", "Local Road",
            "Boulevard", "Boulevard",
            "Highway", "Highway",
            "Avenue", "Avenue",
            "Highway", "Highway",
            "Boulevard", "Boulevard",
            "Highway", "Highway",
            "Local Road", "Local Road",
            "Avenue", "Avenue"
        ]
    })

    vertex_table = PyVertexTable(vertices)
    edge_table = PyEdgeTable(edges)
    return PyPropertyGraph(vertex_table, edge_table)


def find_fastest_route_dijkstra(graph, start_name, end_name):
    """Find fastest route using Dijkstra's algorithm."""
    print("=" * 70)
    print(f"FASTEST ROUTE: {start_name.upper()} → {end_name.upper()}")
    print("=" * 70)

    csr = graph.csr()
    vertices = graph.vertices()
    edges = graph.edges()

    # Find start and end vertex IDs
    start_id = None
    end_id = None
    for v in range(graph.num_vertices()):
        name = vertices.table()['name'][v].as_py()
        if name == start_name:
            start_id = v
        if name == end_name:
            end_id = v

    if start_id is None or end_id is None:
        print(f"  Error: Could not find {start_name} or {end_name}")
        return

    # Extract travel times as weights
    travel_times = edges.table()['travel_time'].combine_chunks()

    # Run Dijkstra
    result = dijkstra(csr.indptr, csr.indices, travel_times, graph.num_vertices(), start_id)

    # Get results
    distance = result.distances()[end_id]

    if distance == float('inf'):
        print(f"\n  ❌ No route found from {start_name} to {end_name}")
        return

    # Reconstruct path
    path = reconstruct_path(result.predecessors(), start_id, end_id)
    path_list = path.to_pylist()

    print(f"\n  ✅ Route Found!")
    print(f"     Total travel time: {distance:.1f} minutes")
    print(f"     Number of stops: {len(path_list) - 1}")
    print(f"\n  Route:")

    total_distance_km = 0.0
    for i in range(len(path_list)):
        v = path_list[i]
        location_name = vertices.table()['name'][v].as_py()

        if i == 0:
            print(f"     {i+1}. START: {location_name}")
        elif i == len(path_list) - 1:
            print(f"     {i+1}. END: {location_name}")
        else:
            print(f"     {i+1}. Via: {location_name}")

        # Calculate segment details
        if i < len(path_list) - 1:
            next_v = path_list[i + 1]
            # Find edge between current and next vertex
            for edge_idx in range(csr.indptr[v], csr.indptr[v + 1]):
                if csr.indices[edge_idx] == next_v:
                    segment_time = edges.table()['travel_time'][edge_idx].as_py()
                    segment_dist = edges.table()['distance_km'][edge_idx].as_py()
                    road_type = edges.table()['road_type'][edge_idx].as_py()
                    total_distance_km += segment_dist
                    print(f"        ↓ {segment_time:.1f} min, {segment_dist:.1f} km via {road_type}")
                    break

    print(f"\n  Total distance: {total_distance_km:.1f} km")
    print(f"  Average speed: {(total_distance_km / distance * 60):.1f} km/h")


def find_route_with_astar(graph, start_name, end_name):
    """Find route using A* with Euclidean distance heuristic."""
    print("\n" + "=" * 70)
    print(f"A* ROUTE (HEURISTIC-GUIDED): {start_name.upper()} → {end_name.upper()}")
    print("=" * 70)

    csr = graph.csr()
    vertices = graph.vertices()
    edges = graph.edges()

    # Find start and end vertex IDs
    start_id = None
    end_id = None
    for v in range(graph.num_vertices()):
        name = vertices.table()['name'][v].as_py()
        if name == start_name:
            start_id = v
        if name == end_name:
            end_id = v

    if start_id is None or end_id is None:
        print(f"  Error: Could not find {start_name} or {end_name}")
        return

    # Calculate Euclidean distance heuristic
    end_x = vertices.table()['x'][end_id].as_py()
    end_y = vertices.table()['y'][end_id].as_py()

    heuristic_values = []
    for v in range(graph.num_vertices()):
        x = vertices.table()['x'][v].as_py()
        y = vertices.table()['y'][v].as_py()
        # Euclidean distance * estimated travel time per unit (2 min/unit)
        euclidean = ((x - end_x)**2 + (y - end_y)**2)**0.5
        heuristic_values.append(euclidean * 2.0)  # Admissible heuristic

    heuristic = pa.array(heuristic_values, type=pa.float64())

    # Extract travel times as weights
    travel_times = edges.table()['travel_time'].combine_chunks()

    # Run A*
    result = astar(csr.indptr, csr.indices, travel_times, graph.num_vertices(),
                   start_id, end_id, heuristic)

    # Get results
    distance = result.distances()[end_id]

    if distance == float('inf'):
        print(f"\n  ❌ No route found from {start_name} to {end_name}")
        return

    # Reconstruct path
    path = reconstruct_path(result.predecessors(), start_id, end_id)
    path_list = path.to_pylist()

    print(f"\n  ✅ A* Route Found!")
    print(f"     Total travel time: {distance:.1f} minutes")
    print(f"     Nodes explored: {result.num_reached()} (vs {graph.num_vertices()} total)")
    print(f"     Efficiency: {(result.num_reached() / graph.num_vertices() * 100):.1f}% of graph explored")
    print(f"\n  Route:")

    for i, v in enumerate(path_list):
        location_name = vertices.table()['name'][v].as_py()
        if i == 0:
            print(f"     START: {location_name}")
        elif i == len(path_list) - 1:
            print(f"     END: {location_name}")
        else:
            print(f"     {i}. {location_name}")


def compare_all_routes(graph, start_name, end_name):
    """Compare different routing strategies."""
    print("\n" + "=" * 70)
    print(f"ROUTING COMPARISON: {start_name.upper()} → {end_name.upper()}")
    print("=" * 70)

    csr = graph.csr()
    vertices = graph.vertices()
    edges = graph.edges()

    # Find start and end vertex IDs
    start_id = None
    end_id = None
    for v in range(graph.num_vertices()):
        name = vertices.table()['name'][v].as_py()
        if name == start_name:
            start_id = v
        if name == end_name:
            end_id = v

    if start_id is None or end_id is None:
        print(f"  Error: Could not find locations")
        return

    # 1. Fastest time (Dijkstra with travel times)
    travel_times = edges.table()['travel_time'].combine_chunks()
    result_time = dijkstra(csr.indptr, csr.indices, travel_times, graph.num_vertices(), start_id)
    fastest_time = result_time.distances()[end_id]

    # 2. Shortest distance (Dijkstra with distances)
    distances = edges.table()['distance_km'].combine_chunks()
    result_dist = dijkstra(csr.indptr, csr.indices, distances, graph.num_vertices(), start_id)
    shortest_distance = result_dist.distances()[end_id]
    shortest_distance_time = 0.0

    # Calculate time for shortest distance route
    path_dist = reconstruct_path(result_dist.predecessors(), start_id, end_id)
    path_dist_list = path_dist.to_pylist()
    for i in range(len(path_dist_list) - 1):
        v = path_dist_list[i]
        next_v = path_dist_list[i + 1]
        for edge_idx in range(csr.indptr[v], csr.indptr[v + 1]):
            if csr.indices[edge_idx] == next_v:
                shortest_distance_time += edges.table()['travel_time'][edge_idx].as_py()
                break

    # 3. Fewest stops (unweighted)
    result_hops = unweighted_shortest_paths(csr.indptr, csr.indices, graph.num_vertices(), start_id)
    fewest_stops = int(result_hops.distances()[end_id])

    # Calculate time for fewest stops route
    path_hops = reconstruct_path(result_hops.predecessors(), start_id, end_id)
    path_hops_list = path_hops.to_pylist()
    fewest_stops_time = 0.0
    for i in range(len(path_hops_list) - 1):
        v = path_hops_list[i]
        next_v = path_hops_list[i + 1]
        for edge_idx in range(csr.indptr[v], csr.indptr[v + 1]):
            if csr.indices[edge_idx] == next_v:
                fewest_stops_time += edges.table()['travel_time'][edge_idx].as_py()
                break

    print(f"\n  Strategy Comparison:")
    print(f"  ┌{'─' * 66}┐")
    print(f"  │ {'Strategy':<20} │ {'Time (min)':<12} │ {'Distance (km)':<15} │ {'Stops':<6} │")
    print(f"  ├{'─' * 66}┤")
    print(f"  │ {'Fastest Route':<20} │ {fastest_time:<12.1f} │ {'N/A':<15} │ {'N/A':<6} │")
    print(f"  │ {'Shortest Distance':<20} │ {shortest_distance_time:<12.1f} │ {shortest_distance:<15.1f} │ {'N/A':<6} │")
    print(f"  │ {'Fewest Stops':<20} │ {fewest_stops_time:<12.1f} │ {'N/A':<15} │ {fewest_stops:<6} │")
    print(f"  └{'─' * 66}┘")

    print(f"\n  Recommendation:")
    if fastest_time == min(fastest_time, shortest_distance_time, fewest_stops_time):
        print(f"    🚗 Take the FASTEST ROUTE ({fastest_time:.1f} min)")
    elif shortest_distance_time == min(fastest_time, shortest_distance_time, fewest_stops_time):
        print(f"    🛣️  Take the SHORTEST DISTANCE route ({shortest_distance:.1f} km, {shortest_distance_time:.1f} min)")
    else:
        print(f"    🎯 Take the FEWEST STOPS route ({fewest_stops} stops, {fewest_stops_time:.1f} min)")


def analyze_city_connectivity(graph):
    """Analyze overall city network connectivity."""
    print("\n" + "=" * 70)
    print("CITY NETWORK CONNECTIVITY ANALYSIS")
    print("=" * 70)

    csr = graph.csr()
    vertices = graph.vertices()
    edges = graph.edges()

    print(f"\n  Network Statistics:")
    print(f"    Total intersections: {graph.num_vertices()}")
    print(f"    Total roads: {graph.num_edges() // 2} (bidirectional)")

    # Calculate average travel time
    all_times = edges.table()['travel_time'].to_pylist()
    avg_time = sum(all_times) / len(all_times)
    print(f"    Average travel time: {avg_time:.1f} minutes")

    # Find most connected intersection
    max_connections = 0
    most_connected = None
    for v in range(graph.num_vertices()):
        degree = csr.get_degree(v)
        if degree > max_connections:
            max_connections = degree
            most_connected = v

    if most_connected is not None:
        name = vertices.table()['name'][most_connected].as_py()
        print(f"    Most connected: {name} ({max_connections} roads)")

    # Find least connected
    min_connections = float('inf')
    least_connected = None
    for v in range(graph.num_vertices()):
        degree = csr.get_degree(v)
        if degree < min_connections:
            min_connections = degree
            least_connected = v

    if least_connected is not None:
        name = vertices.table()['name'][least_connected].as_py()
        print(f"    Least connected: {name} ({int(min_connections)} roads)")


def main():
    print("\n" + "=" * 70)
    print("CITY ROUTING AND NAVIGATION DEMO")
    print("Powered by Sabot Shortest Path Algorithms")
    print("=" * 70)

    # Create city network
    graph = create_city_network()

    # 1. Analyze city connectivity
    analyze_city_connectivity(graph)

    # 2. Find fastest route (Dijkstra)
    print("\n")
    find_fastest_route_dijkstra(graph, "Downtown", "Airport")

    # 3. Find route with A* (heuristic-guided)
    find_route_with_astar(graph, "Downtown", "Harbor")

    # 4. Compare different routing strategies
    compare_all_routes(graph, "Hospital", "Stadium")

    # 5. Multiple route queries
    print("\n" + "=" * 70)
    print("QUICK ROUTE QUERIES")
    print("=" * 70)

    queries = [
        ("Mall", "Train Station"),
        ("Park", "Airport"),
        ("University", "Harbor")
    ]

    for start, end in queries:
        csr = graph.csr()
        vertices = graph.vertices()
        edges = graph.edges()

        # Find IDs
        start_id = None
        end_id = None
        for v in range(graph.num_vertices()):
            name = vertices.table()['name'][v].as_py()
            if name == start:
                start_id = v
            if name == end:
                end_id = v

        if start_id is not None and end_id is not None:
            travel_times = edges.table()['travel_time'].combine_chunks()
            result = dijkstra(csr.indptr, csr.indices, travel_times, graph.num_vertices(), start_id)
            time = result.distances()[end_id]
            path = reconstruct_path(result.predecessors(), start_id, end_id)
            stops = len(path) - 1
            print(f"\n  {start} → {end}:")
            print(f"    Time: {time:.1f} min, Stops: {stops}")

    print("\n" + "=" * 70)
    print("✅ Demo Complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
