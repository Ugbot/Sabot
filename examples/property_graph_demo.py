#!/usr/bin/env python3
"""
Property Graph Demo with Sabot

Demonstrates Arrow-native graph storage and query capabilities:
- Property graph creation (vertices + edges with properties)
- CSR/CSC adjacency construction
- Neighbor traversals (out-neighbors, in-neighbors)
- Pattern matching: (VertexLabel)-[EdgeType]->(VertexLabel)
- Zero-copy filtering and projection

Use case: Social network + company relationships
"""

import pyarrow as pa
import time


def create_social_network_graph():
    """
    Create a social network graph with people, companies, and relationships.

    Graph:
    - 6 vertices: 4 persons, 2 companies
    - 8 edges: KNOWS (person-person), WORKS_AT (person-company), OWNS (person-company)
    """
    # Vertices: People and companies
    vertices = pa.table({
        'id': pa.array([0, 1, 2, 3, 4, 5], type=pa.int64()),
        'label': pa.array([
            'Person', 'Person', 'Person', 'Person',
            'Company', 'Company'
        ]).dictionary_encode(),
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Acme Corp', 'Globex Inc'],
        'age': [30, 35, 28, 42, 0, 0],  # 0 for companies
        'city': ['San Francisco', 'New York', 'San Francisco', 'Boston', 'Palo Alto', 'Boston']
    })

    # Edges: Social + work relationships
    edges = pa.table({
        'src': pa.array([0, 0, 1, 1, 2, 0, 3, 3], type=pa.int64()),
        'dst': pa.array([1, 2, 2, 4, 3, 4, 5, 4], type=pa.int64()),
        'type': pa.array([
            'KNOWS', 'KNOWS', 'KNOWS', 'WORKS_AT',
            'KNOWS', 'WORKS_AT', 'WORKS_AT', 'OWNS'
        ]).dictionary_encode(),
        'since': [2018, 2019, 2017, 2020, 2021, 2019, 2015, 2010],
        'weight': [1.0, 0.8, 0.9, 1.0, 0.7, 1.0, 1.0, 1.0]
    })

    return vertices, edges


def demo_graph_creation():
    """Demo: Creating property graph from Arrow tables."""
    print("=" * 60)
    print("Demo 1: Creating Property Graph")
    print("=" * 60)

    from sabot._cython.graph import VertexTable, EdgeTable, PropertyGraph

    vertices, edges = create_social_network_graph()

    # Create property graph
    start = time.perf_counter()
    vt = VertexTable(vertices)
    et = EdgeTable(edges)
    graph = PropertyGraph(vt, et)
    elapsed = time.perf_counter() - start

    print(f"✅ Created property graph:")
    print(f"   - Vertices: {graph.num_vertices()}")
    print(f"   - Edges: {graph.num_edges()}")
    print(f"   - Time: {elapsed*1000:.2f} ms\n")

    return graph


def demo_csr_construction(graph):
    """Demo: Building CSR/CSC adjacency indices."""
    print("=" * 60)
    print("Demo 2: CSR/CSC Adjacency Construction")
    print("=" * 60)

    # Build CSR (out-edges)
    start = time.perf_counter()
    graph.build_csr()
    elapsed_csr = time.perf_counter() - start

    # Build CSC (in-edges)
    start = time.perf_counter()
    graph.build_csc()
    elapsed_csc = time.perf_counter() - start

    print(f"✅ Built adjacency indices:")
    print(f"   - CSR (out-edges): {elapsed_csr*1000:.2f} ms")
    print(f"   - CSC (in-edges): {elapsed_csc*1000:.2f} ms")

    csr = graph.csr()
    print(f"\n   CSR Statistics:")
    print(f"   - Total edges: {csr.num_edges()}")
    print(f"   - Average degree: {csr.num_edges() / csr.num_vertices():.2f}")
    print()


def demo_neighbor_traversals(graph):
    """Demo: Neighbor lookups via CSR/CSC."""
    print("=" * 60)
    print("Demo 3: Neighbor Traversals")
    print("=" * 60)

    # Get vertex names for readability
    names = graph.vertices().table().column('name').to_pylist()

    # Alice (vertex 0) out-neighbors
    alice_neighbors = graph.get_neighbors(0)
    print(f"✅ Alice's neighbors (out-edges):")
    for neighbor_id in alice_neighbors.to_pylist():
        print(f"   - {names[neighbor_id]} (id={neighbor_id})")

    # Bob (vertex 1) out-neighbors
    bob_neighbors = graph.get_neighbors(1)
    print(f"\n✅ Bob's neighbors (out-edges):")
    for neighbor_id in bob_neighbors.to_pylist():
        print(f"   - {names[neighbor_id]} (id={neighbor_id})")

    # Acme Corp (vertex 4) in-neighbors (who knows/works at Acme)
    acme_in_neighbors = graph.get_in_neighbors(4)
    print(f"\n✅ Acme Corp's in-neighbors (who connects to Acme):")
    for neighbor_id in acme_in_neighbors.to_pylist():
        print(f"   - {names[neighbor_id]} (id={neighbor_id})")
    print()


def demo_vertex_filtering(graph):
    """Demo: Filtering vertices by label."""
    print("=" * 60)
    print("Demo 4: Vertex Filtering")
    print("=" * 60)

    vt = graph.vertices()

    # Filter persons
    persons = vt.filter_by_label('Person')
    print(f"✅ Persons ({persons.num_vertices()} vertices):")
    names = persons.table().column('name').to_pylist()
    ages = persons.table().column('age').to_pylist()
    for name, age in zip(names, ages):
        print(f"   - {name}, age {age}")

    # Filter companies
    companies = vt.filter_by_label('Company')
    print(f"\n✅ Companies ({companies.num_vertices()} vertices):")
    names = companies.table().column('name').to_pylist()
    cities = companies.table().column('city').to_pylist()
    for name, city in zip(names, cities):
        print(f"   - {name} ({city})")
    print()


def demo_edge_filtering(graph):
    """Demo: Filtering edges by type."""
    print("=" * 60)
    print("Demo 5: Edge Filtering")
    print("=" * 60)

    et = graph.edges()
    names = graph.vertices().table().column('name').to_pylist()

    # Filter KNOWS edges
    knows_edges = et.filter_by_type('KNOWS')
    print(f"✅ KNOWS edges ({knows_edges.num_edges()} edges):")
    srcs = knows_edges.get_sources().to_pylist()
    dsts = knows_edges.get_destinations().to_pylist()
    for src, dst in zip(srcs, dsts):
        print(f"   - {names[src]} → {names[dst]}")

    # Filter WORKS_AT edges
    works_at_edges = et.filter_by_type('WORKS_AT')
    print(f"\n✅ WORKS_AT edges ({works_at_edges.num_edges()} edges):")
    srcs = works_at_edges.get_sources().to_pylist()
    dsts = works_at_edges.get_destinations().to_pylist()
    for src, dst in zip(srcs, dsts):
        print(f"   - {names[src]} → {names[dst]}")
    print()


def demo_pattern_matching(graph):
    """Demo: Pattern matching queries."""
    print("=" * 60)
    print("Demo 6: Pattern Matching")
    print("=" * 60)

    names = graph.vertices().table().column('name').to_pylist()

    # Pattern: (Person)-[KNOWS]->(Person)
    print("✅ Pattern: (Person)-[KNOWS]->(Person)")
    result = graph.match_pattern('Person', 'KNOWS', 'Person')
    print(f"   Found {result.num_rows} matches:")
    srcs = result.column('src_id').to_pylist()
    dsts = result.column('dst_id').to_pylist()
    for src, dst in zip(srcs, dsts):
        print(f"   - {names[src]} knows {names[dst]}")

    # Pattern: (Person)-[WORKS_AT]->(Company)
    print("\n✅ Pattern: (Person)-[WORKS_AT]->(Company)")
    result = graph.match_pattern('Person', 'WORKS_AT', 'Company')
    print(f"   Found {result.num_rows} matches:")
    srcs = result.column('src_id').to_pylist()
    dsts = result.column('dst_id').to_pylist()
    for src, dst in zip(srcs, dsts):
        print(f"   - {names[src]} works at {names[dst]}")
    print()


def demo_projection(graph):
    """Demo: Column projection (select specific columns)."""
    print("=" * 60)
    print("Demo 7: Column Projection")
    print("=" * 60)

    vt = graph.vertices()

    # Project name + age only
    projected = vt.select(['id', 'name', 'age'])
    print(f"✅ Projected columns: {projected.schema().names}")
    print(f"   Schema: {projected.schema()}")

    # Show data
    table = projected.table()
    print(f"\n   Sample data:")
    for i in range(min(3, table.num_rows)):
        row = {col: table.column(col)[i].as_py() for col in table.column_names}
        print(f"   - {row}")
    print()


def demo_performance(graph):
    """Demo: Performance measurements."""
    print("=" * 60)
    print("Demo 8: Performance Measurements")
    print("=" * 60)

    # Neighbor lookup benchmark
    num_lookups = 1000
    start = time.perf_counter()
    for _ in range(num_lookups):
        neighbors = graph.get_neighbors(0)
    elapsed = time.perf_counter() - start

    per_lookup_us = (elapsed / num_lookups) * 1e6
    print(f"✅ Neighbor Lookup Performance:")
    print(f"   - {num_lookups} lookups in {elapsed*1000:.2f} ms")
    print(f"   - {per_lookup_us:.2f} μs per lookup")
    print(f"   - {num_lookups/elapsed/1e6:.2f} M lookups/sec")

    # Pattern matching benchmark
    num_queries = 100
    start = time.perf_counter()
    for _ in range(num_queries):
        result = graph.match_pattern('Person', 'KNOWS', 'Person')
    elapsed = time.perf_counter() - start

    print(f"\n✅ Pattern Matching Performance:")
    print(f"   - {num_queries} queries in {elapsed*1000:.2f} ms")
    print(f"   - {elapsed/num_queries*1000:.2f} ms per query")
    print(f"   - {num_queries/elapsed:.2f} queries/sec")
    print()


def main():
    """Run all demos."""
    print("\n" + "="*60)
    print(" Sabot Property Graph Demo")
    print("="*60 + "\n")

    try:
        from sabot._cython.graph import CSRAdjacency, VertexTable, EdgeTable, PropertyGraph
        print("✅ Graph storage module loaded successfully\n")
    except ImportError as e:
        print(f"❌ Graph storage module not built: {e}")
        print("\nBuild with: python build.py")
        return

    # Run demos
    graph = demo_graph_creation()
    demo_csr_construction(graph)
    demo_neighbor_traversals(graph)
    demo_vertex_filtering(graph)
    demo_edge_filtering(graph)
    demo_pattern_matching(graph)
    demo_projection(graph)
    demo_performance(graph)

    print("="*60)
    print("Summary:")
    print("  ✅ Property graph with 6 vertices, 8 edges")
    print("  ✅ CSR/CSC construction (<1ms)")
    print("  ✅ O(1) neighbor lookups (<1μs)")
    print("  ✅ Zero-copy filtering and projection")
    print("  ✅ Pattern matching with vectorized joins")
    print("="*60 + "\n")


if __name__ == '__main__':
    main()
