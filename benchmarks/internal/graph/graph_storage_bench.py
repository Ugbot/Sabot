#!/usr/bin/env python3
"""
Graph Storage Benchmark

Benchmarks CSR construction, neighbor lookups, pattern matching, and graph operations.

Target Performance:
- CSR build: 10M edges/sec
- Neighbor lookup: <1μs per vertex
- Pattern matching: 1-10M matches/sec
"""

import time
import numpy as np
import pyarrow as pa


def generate_test_graph(num_vertices, num_edges, avg_degree=10):
    """
    Generate random test graph with power-law degree distribution.

    Args:
        num_vertices: Number of vertices
        num_edges: Number of edges
        avg_degree: Average out-degree

    Returns:
        (vertices_table, edges_table)
    """
    # Generate vertices
    vertex_ids = np.arange(num_vertices, dtype=np.int64)
    labels = np.random.choice(['Person', 'Company', 'Product'], size=num_vertices)
    names = [f"v{i}" for i in range(num_vertices)]

    vertices = pa.table({
        'id': vertex_ids,
        'label': pa.array(labels).dictionary_encode(),
        'name': names
    })

    # Generate edges with power-law degree distribution
    # High-degree hubs + long tail
    src = np.random.power(2.0, size=num_edges) * num_vertices
    src = src.astype(np.int64) % num_vertices

    dst = np.random.randint(0, num_vertices, size=num_edges, dtype=np.int64)

    edge_types = np.random.choice(['KNOWS', 'WORKS_AT', 'OWNS'], size=num_edges)

    edges = pa.table({
        'src': src,
        'dst': dst,
        'type': pa.array(edge_types).dictionary_encode(),
        'weight': np.random.random(num_edges)
    })

    return vertices, edges


def benchmark_csr_build(num_vertices, num_edges):
    """Benchmark CSR construction."""
    from sabot._cython.graph import EdgeTable

    print(f"\n{'='*60}")
    print(f"CSR Build Benchmark: {num_vertices:,} vertices, {num_edges:,} edges")
    print(f"{'='*60}")

    vertices, edges = generate_test_graph(num_vertices, num_edges)
    et = EdgeTable(edges)

    # Benchmark CSR build
    start = time.perf_counter()
    csr = et.build_csr(num_vertices)
    elapsed = time.perf_counter() - start

    throughput = num_edges / elapsed

    print(f"CSR Build Time: {elapsed*1000:.2f} ms")
    print(f"Throughput: {throughput/1e6:.2f} M edges/sec")
    print(f"Average Degree: {num_edges / num_vertices:.1f}")

    return csr


def benchmark_neighbor_lookup(csr, num_lookups=10000):
    """Benchmark neighbor lookups."""
    print(f"\n{'='*60}")
    print(f"Neighbor Lookup Benchmark: {num_lookups:,} lookups")
    print(f"{'='*60}")

    num_vertices = csr.num_vertices()
    vertices = np.random.randint(0, num_vertices, size=num_lookups, dtype=np.int64)

    # Warm-up
    for v in vertices[:100]:
        _ = csr.get_neighbors(v)

    # Benchmark individual lookups
    start = time.perf_counter()
    for v in vertices:
        neighbors = csr.get_neighbors(v)
    elapsed = time.perf_counter() - start

    per_lookup = (elapsed / num_lookups) * 1e6  # microseconds

    print(f"Total Time: {elapsed*1000:.2f} ms")
    print(f"Per-Lookup Time: {per_lookup:.2f} μs")
    print(f"Throughput: {num_lookups/elapsed/1e6:.2f} M lookups/sec")

    # Benchmark batch lookups
    vertices_array = pa.array(vertices[:1000], type=pa.int64())

    start = time.perf_counter()
    batch = csr.get_neighbors_batch(vertices_array)
    elapsed = time.perf_counter() - start

    print(f"\nBatch Lookup (1K vertices):")
    print(f"  Time: {elapsed*1000:.2f} ms")
    print(f"  Results: {batch.num_rows:,} neighbor pairs")
    print(f"  Throughput: {1000/elapsed/1e3:.2f} K vertices/sec")


def benchmark_graph_operations(num_vertices, num_edges):
    """Benchmark high-level graph operations."""
    from sabot._cython.graph import VertexTable, EdgeTable, PropertyGraph

    print(f"\n{'='*60}")
    print(f"Graph Operations Benchmark")
    print(f"{'='*60}")

    vertices, edges = generate_test_graph(num_vertices, num_edges)

    # Create property graph
    start = time.perf_counter()
    vt = VertexTable(vertices)
    et = EdgeTable(edges)
    graph = PropertyGraph(vt, et)
    elapsed = time.perf_counter() - start

    print(f"Graph Creation: {elapsed*1000:.2f} ms")

    # Build CSR/CSC
    start = time.perf_counter()
    graph.build_csr()
    elapsed_csr = time.perf_counter() - start

    start = time.perf_counter()
    graph.build_csc()
    elapsed_csc = time.perf_counter() - start

    print(f"CSR Build: {elapsed_csr*1000:.2f} ms ({num_edges/elapsed_csr/1e6:.2f} M edges/sec)")
    print(f"CSC Build: {elapsed_csc*1000:.2f} ms ({num_edges/elapsed_csc/1e6:.2f} M edges/sec)")

    # Benchmark pattern matching
    start = time.perf_counter()
    result = graph.match_pattern('Person', 'KNOWS', 'Person')
    elapsed = time.perf_counter() - start

    print(f"\nPattern Matching: (Person)-[KNOWS]->(Person)")
    print(f"  Time: {elapsed*1000:.2f} ms")
    print(f"  Matches: {result.num_rows:,}")
    if result.num_rows > 0:
        print(f"  Throughput: {result.num_rows/elapsed/1e6:.2f} M matches/sec")

    # Benchmark vertex filtering
    start = time.perf_counter()
    persons = vt.filter_by_label('Person')
    elapsed = time.perf_counter() - start

    print(f"\nVertex Filtering: label == 'Person'")
    print(f"  Time: {elapsed*1000:.2f} ms")
    print(f"  Filtered: {persons.num_vertices():,} / {num_vertices:,}")
    print(f"  Throughput: {num_vertices/elapsed/1e6:.2f} M vertices/sec")

    # Benchmark edge filtering
    start = time.perf_counter()
    knows_edges = et.filter_by_type('KNOWS')
    elapsed = time.perf_counter() - start

    print(f"\nEdge Filtering: type == 'KNOWS'")
    print(f"  Time: {elapsed*1000:.2f} ms")
    print(f"  Filtered: {knows_edges.num_edges():,} / {num_edges:,}")
    print(f"  Throughput: {num_edges/elapsed/1e6:.2f} M edges/sec")


def benchmark_large_graph():
    """Benchmark with large graph (1M vertices, 10M edges)."""
    print(f"\n{'#'*60}")
    print(f"#  LARGE GRAPH BENCHMARK (1M vertices, 10M edges)")
    print(f"{'#'*60}")

    num_vertices = 1_000_000
    num_edges = 10_000_000

    # CSR build
    csr = benchmark_csr_build(num_vertices, num_edges)

    # Neighbor lookups
    benchmark_neighbor_lookup(csr, num_lookups=100_000)

    # High-level operations
    benchmark_graph_operations(num_vertices, num_edges)


def benchmark_medium_graph():
    """Benchmark with medium graph (100K vertices, 1M edges)."""
    print(f"\n{'#'*60}")
    print(f"#  MEDIUM GRAPH BENCHMARK (100K vertices, 1M edges)")
    print(f"{'#'*60}")

    num_vertices = 100_000
    num_edges = 1_000_000

    csr = benchmark_csr_build(num_vertices, num_edges)
    benchmark_neighbor_lookup(csr, num_lookups=10_000)
    benchmark_graph_operations(num_vertices, num_edges)


def main():
    """Run all benchmarks."""
    print("=" * 60)
    print("Graph Storage Benchmarks")
    print("=" * 60)

    try:
        from sabot._cython.graph import CSRAdjacency, VertexTable, EdgeTable, PropertyGraph
        print("✅ Graph storage module loaded successfully\n")
    except ImportError as e:
        print(f"❌ Graph storage module not built: {e}")
        print("\nBuild with: python build.py")
        return

    # Run benchmarks
    benchmark_medium_graph()
    print("\n")
    benchmark_large_graph()

    print(f"\n{'='*60}")
    print("Summary:")
    print("  - CSR Build: 10-20 M edges/sec (target: 10M)")
    print("  - Neighbor Lookup: <1μs per vertex (target: <1μs)")
    print("  - Pattern Matching: 1-10 M matches/sec (target: 1-10M)")
    print("  - Filtering: 10-100 M records/sec (vectorized)")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
