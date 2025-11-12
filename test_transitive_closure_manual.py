#!/usr/bin/env python3
"""
Manual test for transitive closure algorithm.

Verifies the DFS algorithm logic before building C++ version.
"""

import pyarrow as pa
import numpy as np
from test_csr_manual import build_csr_python, get_successors_python


def transitive_closure_python(csr, start_nodes, min_dist=1, max_dist=100):
    """
    Python implementation of transitive closure (DFS from QLever).

    Matches the C++ algorithm in transitive_closure.cpp.
    """
    if isinstance(start_nodes, (int, np.integer)):
        start_nodes = [start_nodes]
    elif hasattr(start_nodes, 'to_pylist'):
        start_nodes = start_nodes.to_pylist()

    start_results = []
    end_results = []
    dist_results = []

    for start_node in start_nodes:
        # DFS stack: (node, distance)
        stack = [(start_node, 0)]

        # Visited bitmap
        visited = set()

        while stack:
            node, steps = stack.pop()

            # Skip if already visited
            if node in visited:
                continue

            # Check distance bounds
            if steps > max_dist:
                continue

            # Mark as visited
            visited.add(node)

            # Emit result if within min distance
            if steps >= min_dist:
                start_results.append(start_node)
                end_results.append(node)
                dist_results.append(steps)

            # Expand successors
            successors = get_successors_python(csr, node)
            for succ in successors.to_pylist():
                stack.append((succ, steps + 1))

    return pa.RecordBatch.from_pydict({
        'start': pa.array(start_results, type=pa.int64()),
        'end': pa.array(end_results, type=pa.int64()),
        'dist': pa.array(dist_results, type=pa.int64())
    })


def test_linear_chain():
    """Test: 0 → 1 → 2 → 3 → 4"""
    print("=" * 60)
    print("Test: Linear Chain (p+)")
    print("=" * 60)

    edges = pa.RecordBatch.from_pydict({
        'subject': pa.array([0, 1, 2, 3], type=pa.int64()),
        'object': pa.array([1, 2, 3, 4], type=pa.int64())
    })

    csr = build_csr_python(edges, 'subject', 'object')
    result = transitive_closure_python(csr, [0], min_dist=1, max_dist=100)

    print(f"\nResult ({result.num_rows} rows):")
    for i in range(result.num_rows):
        print(f"  {result['start'][i].as_py()} → {result['end'][i].as_py()} (dist={result['dist'][i].as_py()})")

    # Verify
    reachable = set(result['end'].to_pylist())
    expected = {1, 2, 3, 4}
    assert reachable == expected, f"Expected {expected}, got {reachable}"
    print("\n✅ Linear chain test passed!")


def test_cycle():
    """Test: 0 → 1 → 2 → 0 (cycle)"""
    print("\n" + "=" * 60)
    print("Test: Cycle Detection")
    print("=" * 60)

    edges = pa.RecordBatch.from_pydict({
        'subject': pa.array([0, 1, 2], type=pa.int64()),
        'object': pa.array([1, 2, 0], type=pa.int64())
    })

    csr = build_csr_python(edges, 'subject', 'object')
    result = transitive_closure_python(csr, [0], min_dist=1, max_dist=100)

    print(f"\nResult ({result.num_rows} rows):")
    for i in range(result.num_rows):
        print(f"  {result['start'][i].as_py()} → {result['end'][i].as_py()} (dist={result['dist'][i].as_py()})")

    # Should visit 1, 2 (not 0, since min_dist=1)
    reachable = set(result['end'].to_pylist())
    expected = {1, 2}
    assert reachable == expected, f"Expected {expected}, got {reachable}"
    print("\n✅ Cycle test passed (no infinite loop)!")


def test_zero_plus():
    """Test p* (min_dist=0, includes start node)"""
    print("\n" + "=" * 60)
    print("Test: Zero-or-More (p*)")
    print("=" * 60)

    edges = pa.RecordBatch.from_pydict({
        'subject': pa.array([0, 1], type=pa.int64()),
        'object': pa.array([1, 2], type=pa.int64())
    })

    csr = build_csr_python(edges, 'subject', 'object')
    result = transitive_closure_python(csr, [0], min_dist=0, max_dist=100)

    print(f"\nResult ({result.num_rows} rows):")
    for i in range(result.num_rows):
        print(f"  {result['start'][i].as_py()} → {result['end'][i].as_py()} (dist={result['dist'][i].as_py()})")

    # Should include 0 (reflexive)
    reachable = set(result['end'].to_pylist())
    expected = {0, 1, 2}
    assert reachable == expected, f"Expected {expected}, got {reachable}"
    print("\n✅ Zero-or-more test passed (includes start node)!")


def test_bounded_paths():
    """Test p{2,2} (exactly 2 hops)"""
    print("\n" + "=" * 60)
    print("Test: Bounded Paths p{2,2}")
    print("=" * 60)

    edges = pa.RecordBatch.from_pydict({
        'subject': pa.array([0, 1, 2, 3], type=pa.int64()),
        'object': pa.array([1, 2, 3, 4], type=pa.int64())
    })

    csr = build_csr_python(edges, 'subject', 'object')
    result = transitive_closure_python(csr, [0], min_dist=2, max_dist=2)

    print(f"\nResult ({result.num_rows} rows):")
    for i in range(result.num_rows):
        print(f"  {result['start'][i].as_py()} → {result['end'][i].as_py()} (dist={result['dist'][i].as_py()})")

    # Should reach only node 2 (exactly 2 hops)
    reachable = set(result['end'].to_pylist())
    expected = {2}
    assert reachable == expected, f"Expected {expected}, got {reachable}"
    print("\n✅ Bounded paths test passed!")


def test_branching():
    """
    Test branching graph:
         1 → 3
       ↗      ↘
      0        5
       ↘      ↗
         2 → 4
    """
    print("\n" + "=" * 60)
    print("Test: Branching Graph")
    print("=" * 60)

    edges = pa.RecordBatch.from_pydict({
        'subject': pa.array([0, 0, 1, 1, 2, 4], type=pa.int64()),
        'object': pa.array([1, 2, 3, 5, 4, 5], type=pa.int64())
    })

    csr = build_csr_python(edges, 'subject', 'object')
    result = transitive_closure_python(csr, [0], min_dist=1, max_dist=100)

    print(f"\nResult ({result.num_rows} rows):")
    for i in range(result.num_rows):
        print(f"  {result['start'][i].as_py()} → {result['end'][i].as_py()} (dist={result['dist'][i].as_py()})")

    # Should reach all nodes except 0
    reachable = set(result['end'].to_pylist())
    expected = {1, 2, 3, 4, 5}
    assert reachable == expected, f"Expected {expected}, got {reachable}"
    print("\n✅ Branching graph test passed!")


def test_multiple_start_nodes():
    """Test multiple start nodes"""
    print("\n" + "=" * 60)
    print("Test: Multiple Start Nodes")
    print("=" * 60)

    edges = pa.RecordBatch.from_pydict({
        'subject': pa.array([0, 1, 3, 4], type=pa.int64()),
        'object': pa.array([1, 2, 4, 5], type=pa.int64())
    })

    csr = build_csr_python(edges, 'subject', 'object')
    result = transitive_closure_python(csr, [0, 3], min_dist=1, max_dist=100)

    print(f"\nResult ({result.num_rows} rows):")
    # Group by start node
    start_to_ends = {}
    for i in range(result.num_rows):
        start = result['start'][i].as_py()
        end = result['end'][i].as_py()
        if start not in start_to_ends:
            start_to_ends[start] = []
        start_to_ends[start].append(end)

    for start, ends in sorted(start_to_ends.items()):
        print(f"  Start {start}: reaches {sorted(ends)}")

    # Verify
    assert set(start_to_ends[0]) == {1, 2}
    assert set(start_to_ends[3]) == {4, 5}
    print("\n✅ Multiple start nodes test passed!")


if __name__ == "__main__":
    test_linear_chain()
    test_cycle()
    test_zero_plus()
    test_bounded_paths()
    test_branching()
    test_multiple_start_nodes()

    print("\n" + "=" * 60)
    print("All transitive closure tests passed!")
    print("=" * 60)
    print("\nAlgorithm verified! Ready for C++ implementation.")
