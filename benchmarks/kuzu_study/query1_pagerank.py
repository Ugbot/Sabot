#!/usr/bin/env python3
"""
Query 1 with PageRank Implementation

Comparison of original implementation vs PageRank kernel.
"""

import sys
import time
from pathlib import Path

# Add Sabot to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sabot import cyarrow as ca
from sabot.cyarrow import compute as pc


def edges_to_csr(edges: ca.Table, vertices: ca.Table, transpose=False):
    """
    Convert edge table to CSR (Compressed Sparse Row) format.

    CSR format stores a graph as:
    - indptr[i]: index into indices[] where vertex i's neighbors start
    - indices[]: neighbor IDs (concatenated for all vertices)

    Args:
        edges: Arrow table with 'source' and 'target' columns (person IDs)
        vertices: Arrow table with 'id' column for mapping
        transpose: If True, use target→source (for incoming edges, i.e., followers)
                   If False, use source→target (for outgoing edges)

    Returns:
        Tuple of (indptr, indices, vertex_id_to_idx) as Arrow Int64Arrays + dict
    """
    import numpy as np

    # Create mapping from person ID to dense index (0 to N-1)
    vertex_ids = vertices.column('id').to_pylist()
    vertex_id_to_idx = {pid: idx for idx, pid in enumerate(vertex_ids)}
    num_vertices = len(vertex_ids)

    # Build adjacency list using dense indices
    adj_list = [[] for _ in range(num_vertices)]

    sources = edges.column('source').to_pylist()
    targets = edges.column('target').to_pylist()

    for source_id, target_id in zip(sources, targets):
        source_idx = vertex_id_to_idx[source_id]
        target_idx = vertex_id_to_idx[target_id]

        if transpose:
            # Store target→source (incoming edges: who follows this person)
            adj_list[target_idx].append(source_idx)
        else:
            # Store source→target (outgoing edges: who this person follows)
            adj_list[source_idx].append(target_idx)

    # Convert to CSR format
    indptr = [0]
    indices = []

    for neighbors in adj_list:
        neighbors_sorted = sorted(neighbors)  # Optional: sort for consistency
        indices.extend(neighbors_sorted)
        indptr.append(len(indices))

    # Convert to Arrow arrays
    indptr_array = ca.array(indptr, type=ca.int64())
    indices_array = ca.array(indices, type=ca.int64())

    return indptr_array, indices_array, vertex_id_to_idx


def query1_original(vertices: ca.Table, edges: ca.Table) -> ca.Table:
    """
    Original implementation using value_counts.
    """
    # Count followers per person (group by target)
    target_col = edges.column('target')

    # Get unique targets and count their occurrences
    import pyarrow.compute as pc_arrow
    follower_counts = pc_arrow.value_counts(target_col)

    # Extract values and counts
    person_ids = follower_counts.field('values')
    num_followers = follower_counts.field('counts')

    # Create table
    counts_table = ca.table({
        'person_id': person_ids,
        'numFollowers': num_followers
    })

    # Sort by numFollowers DESC
    indices = pc.sort_indices(counts_table, sort_keys=[('numFollowers', 'descending')])
    counts_table = pc.take(counts_table, indices)

    # Limit to top 3
    counts_table = counts_table.slice(0, 3)

    # Join with vertices to get names
    # Build lookup dict for fast join
    person_dict = {}
    vertex_ids = vertices.column('id')
    vertex_names = vertices.column('name')
    for i in range(vertices.num_rows):
        person_dict[vertex_ids[i].as_py()] = vertex_names[i].as_py()

    # Add names
    result_ids = []
    result_names = []
    result_counts = []

    for i in range(counts_table.num_rows):
        person_id = counts_table.column('person_id')[i].as_py()
        count = counts_table.column('numFollowers')[i].as_py()
        name = person_dict.get(person_id, "Unknown")

        result_ids.append(person_id)
        result_names.append(name)
        result_counts.append(count)

    return ca.table({
        'personID': ca.array(result_ids),
        'name': ca.array(result_names),
        'numFollowers': ca.array(result_counts)
    })


def query1_pagerank(vertices: ca.Table, edges: ca.Table) -> ca.Table:
    """
    PageRank implementation using C++/Cython kernel.

    PageRank naturally measures vertex importance by incoming edges,
    which is exactly what "most followed" means!
    """
    from sabot._cython.graph.traversal.pagerank import pagerank, top_k_pagerank

    # Get number of vertices
    num_vertices = vertices.num_rows

    # Convert edges to CSR format (TRANSPOSED for incoming edges)
    # PageRank measures importance by INCOMING links (followers)
    indptr, indices, vertex_id_to_idx = edges_to_csr(edges, vertices, transpose=True)

    # Run PageRank (C++ kernel)
    result = pagerank(
        indptr, indices, num_vertices,
        damping=0.85,
        max_iterations=100,
        tolerance=1e-6
    )

    # Get top 3 vertices by PageRank
    top_3_batch = top_k_pagerank(result.ranks(), k=3)

    # Extract vertex indices and ranks
    top_vertex_indices = top_3_batch.column('vertex_id').to_pylist()
    top_ranks = top_3_batch.column('rank').to_pylist()

    # Convert indices back to person IDs
    idx_to_vertex_id = {idx: pid for pid, idx in vertex_id_to_idx.items()}
    top_vertex_ids = [idx_to_vertex_id[idx] for idx in top_vertex_indices]

    # Build vertex lookup for names
    vertex_dict = {}
    vertex_ids = vertices.column('id')
    vertex_names = vertices.column('name')
    for i in range(vertices.num_rows):
        vertex_dict[vertex_ids[i].as_py()] = vertex_names[i].as_py()

    # Count actual followers for each top vertex (for comparison)
    # Note: PageRank != follower count, but should be correlated
    target_col = edges.column('target')
    target_list = target_col.to_pylist()

    follower_counts_dict = {}
    for target in target_list:
        follower_counts_dict[target] = follower_counts_dict.get(target, 0) + 1

    # Build result
    result_ids = []
    result_names = []
    result_counts = []

    for vertex_id in top_vertex_ids:
        name = vertex_dict.get(vertex_id, "Unknown")
        count = follower_counts_dict.get(vertex_id, 0)

        result_ids.append(vertex_id)
        result_names.append(name)
        result_counts.append(count)

    return ca.table({
        'personID': ca.array(result_ids),
        'name': ca.array(result_names),
        'numFollowers': ca.array(result_counts)
    })


def main():
    """Compare original vs PageRank implementations."""
    print("="*70)
    print("QUERY 1 PAGERANK COMPARISON")
    print("="*70)
    print()

    # Load data
    print("Loading data...")
    from data_loader import load_all_data
    data_dir = Path(__file__).parent / "reference" / "data" / "output"
    data = load_all_data(data_dir)
    print("✅ Data loaded")
    print()

    vertices = data['person_nodes']
    edges = data['follows_edges']

    # Test original
    print("Testing ORIGINAL implementation...")
    start = time.perf_counter()
    result_orig = query1_original(vertices, edges)
    time_orig = (time.perf_counter() - start) * 1000
    print(f"  Time: {time_orig:.2f}ms")
    print(f"  Result: {result_orig.to_pydict()}")
    print()

    # Test PageRank
    print("Testing PAGERANK implementation...")
    start = time.perf_counter()
    result_pr = query1_pagerank(vertices, edges)
    time_pr = (time.perf_counter() - start) * 1000
    print(f"  Time: {time_pr:.2f}ms")
    print(f"  Result: {result_pr.to_pydict()}")
    print()

    # Compare
    print("="*70)
    print("COMPARISON")
    print("="*70)
    speedup = time_orig / time_pr
    print(f"Original: {time_orig:.2f}ms")
    print(f"PageRank: {time_pr:.2f}ms")
    print(f"Speedup:  {speedup:.2f}x")
    print()

    # Check correctness
    orig_ids = set(result_orig.column('personID').to_pylist())
    pr_ids = set(result_pr.column('personID').to_pylist())

    if orig_ids == pr_ids:
        print("✅ Results MATCH - same top 3 persons!")
    else:
        print("⚠️ Results DIFFER:")
        print(f"   Original IDs: {sorted(orig_ids)}")
        print(f"   PageRank IDs: {sorted(pr_ids)}")
        print(f"   Common: {sorted(orig_ids & pr_ids)}")
        print(f"   Only in Original: {sorted(orig_ids - pr_ids)}")
        print(f"   Only in PageRank: {sorted(pr_ids - orig_ids)}")

    return 0


if __name__ == "__main__":
    import os
    os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib'
    sys.exit(main())
