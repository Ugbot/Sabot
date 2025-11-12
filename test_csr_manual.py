#!/usr/bin/env python3
"""
Manual test to verify CSR graph implementation approach.

This tests the concept even if the Cython bindings aren't built yet.
"""

import pyarrow as pa
import numpy as np


def build_csr_python(edges_batch, source_col='subject', target_col='object'):
    """
    Pure Python implementation of CSR build for verification.

    This matches the C++ algorithm logic to verify correctness before
    building the full C++ version.
    """
    sources = edges_batch[source_col].to_numpy()
    targets = edges_batch[target_col].to_numpy()

    num_edges = len(sources)

    if num_edges == 0:
        return {
            'offsets': pa.array([0], type=pa.int64()),
            'targets': pa.array([], type=pa.int64()),
            'edge_ids': pa.array([], type=pa.int64()),
            'num_nodes': 0,
            'num_edges': 0
        }

    # Find max node ID
    max_node = max(sources.max(), targets.max())
    num_nodes = max_node + 1

    # Count outgoing edges per node
    degree = np.zeros(num_nodes, dtype=np.int64)
    for src in sources:
        degree[src] += 1

    # Build offsets (cumulative sum)
    offsets = np.zeros(num_nodes + 1, dtype=np.int64)
    offsets[1:] = np.cumsum(degree)

    # Allocate targets and edge_ids
    target_vec = np.zeros(num_edges, dtype=np.int64)
    edge_id_vec = np.zeros(num_edges, dtype=np.int64)

    # Track current write position
    current_offset = degree.copy()

    # Fill arrays
    for i, (src, tgt) in enumerate(zip(sources, targets)):
        pos = offsets[src] + (degree[src] - current_offset[src])
        current_offset[src] -= 1
        target_vec[pos] = tgt
        edge_id_vec[pos] = i

    return {
        'offsets': pa.array(offsets),
        'targets': pa.array(target_vec),
        'edge_ids': pa.array(edge_id_vec),
        'num_nodes': num_nodes,
        'num_edges': num_edges
    }


def get_successors_python(csr, node):
    """Get successors of a node (Python version)."""
    offsets = csr['offsets'].to_numpy()
    targets = csr['targets'].to_numpy()

    if node < 0 or node >= csr['num_nodes']:
        return pa.array([], type=pa.int64())

    start = offsets[node]
    end = offsets[node + 1]

    return pa.array(targets[start:end])


def test_csr_simple():
    """Test CSR build with simple graph."""
    print("=" * 60)
    print("Test: Simple CSR Graph Build")
    print("=" * 60)

    # Graph: 0→1, 0→2, 1→2, 2→3, 2→0
    edges = pa.RecordBatch.from_pydict({
        'subject': pa.array([0, 0, 1, 2, 2], type=pa.int64()),
        'predicate': pa.array([10, 10, 10, 10, 10], type=pa.int64()),
        'object': pa.array([1, 2, 2, 3, 0], type=pa.int64())
    })

    csr = build_csr_python(edges, 'subject', 'object')

    print(f"\nGraph structure:")
    print(f"  Nodes: {csr['num_nodes']}")
    print(f"  Edges: {csr['num_edges']}")
    print(f"\nCSR format:")
    print(f"  offsets:  {csr['offsets'].to_pylist()}")
    print(f"  targets:  {csr['targets'].to_pylist()}")
    print(f"  edge_ids: {csr['edge_ids'].to_pylist()}")

    # Verify structure
    expected_offsets = [0, 2, 3, 5, 5]
    expected_targets = [1, 2, 2, 3, 0]
    expected_edge_ids = [0, 1, 2, 3, 4]

    actual_offsets = csr['offsets'].to_pylist()
    actual_targets = csr['targets'].to_pylist()
    actual_edge_ids = csr['edge_ids'].to_pylist()

    assert actual_offsets == expected_offsets, f"Offsets mismatch: {actual_offsets} != {expected_offsets}"
    assert actual_targets == expected_targets, f"Targets mismatch: {actual_targets} != {expected_targets}"
    assert actual_edge_ids == expected_edge_ids, f"Edge IDs mismatch: {actual_edge_ids} != {expected_edge_ids}"

    print("\n✅ CSR structure correct!")

    # Test successor lookup
    print("\nSuccessor lookups:")
    for node in range(csr['num_nodes']):
        successors = get_successors_python(csr, node)
        print(f"  Node {node} → {successors.to_pylist()}")

    # Verify specific successors
    assert get_successors_python(csr, 0).to_pylist() == [1, 2]
    assert get_successors_python(csr, 1).to_pylist() == [2]
    assert get_successors_python(csr, 2).to_pylist() == [3, 0]
    assert get_successors_python(csr, 3).to_pylist() == []

    print("\n✅ All successor lookups correct!")


def test_csr_empty():
    """Test empty graph."""
    print("\n" + "=" * 60)
    print("Test: Empty Graph")
    print("=" * 60)

    empty = pa.RecordBatch.from_pydict({
        'subject': pa.array([], type=pa.int64()),
        'predicate': pa.array([], type=pa.int64()),
        'object': pa.array([], type=pa.int64())
    })

    csr = build_csr_python(empty, 'subject', 'object')

    assert csr['num_nodes'] == 0
    assert csr['num_edges'] == 0
    assert len(csr['offsets']) == 1
    assert csr['offsets'].to_pylist() == [0]

    print("✅ Empty graph handled correctly!")


def test_csr_self_loop():
    """Test graph with self-loop."""
    print("\n" + "=" * 60)
    print("Test: Self-Loop")
    print("=" * 60)

    edges = pa.RecordBatch.from_pydict({
        'subject': pa.array([0, 0], type=pa.int64()),
        'predicate': pa.array([10, 10], type=pa.int64()),
        'object': pa.array([0, 1], type=pa.int64())
    })

    csr = build_csr_python(edges, 'subject', 'object')

    successors = get_successors_python(csr, 0)
    assert 0 in successors.to_pylist(), "Self-loop should be preserved"
    assert 1 in successors.to_pylist()

    print(f"Node 0 successors: {successors.to_pylist()}")
    print("✅ Self-loop preserved!")


if __name__ == "__main__":
    test_csr_simple()
    test_csr_empty()
    test_csr_self_loop()

    print("\n" + "=" * 60)
    print("All CSR graph tests passed!")
    print("=" * 60)
    print("\nNext: Build C++ implementation and run full test suite")
