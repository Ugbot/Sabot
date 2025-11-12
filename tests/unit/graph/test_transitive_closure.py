"""
Unit tests for transitive closure algorithms (p+, p*).

Tests the DFS-based transitive closure borrowed from QLever,
implemented as Arrow compute kernels.

TDD Phase: RED - Tests written before implementation.
"""

import pytest
import pyarrow as pa
import numpy as np
from pathlib import Path
import sys

# Add sabot_ql to path
sabot_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(sabot_root))


class TestTransitiveClosureOnePlus:
    """Test p+ operator (one or more hops)."""

    def test_linear_chain(self):
        """
        Test transitive closure on linear chain.

        Graph: 0 → 1 → 2 → 3 → 4

        Query: ?x p+ ?y WHERE ?x = 0
        Expected: 0 reaches [1, 2, 3, 4]
        """
        edges = pa.RecordBatch.from_pydict({
            'start': pa.array([0, 1, 2, 3], type=pa.int64()),
            'end': pa.array([1, 2, 3, 4], type=pa.int64())
        })

        from sabot_ql.graph import build_csr_graph, transitive_closure

        csr = build_csr_graph(edges, 'start', 'end')

        # Query: Find all nodes reachable from node 0 (min_dist=1 for p+)
        result = transitive_closure(
            csr,
            start_nodes=pa.array([0], type=pa.int64()),
            min_dist=1,
            max_dist=100  # Unbounded
        )

        # Result should be RecordBatch with (start, end) pairs
        assert result.num_rows == 4, f"Expected 4 reachable nodes, got {result.num_rows}"

        # Verify reachable nodes
        reachable = set(result['end'].to_pylist())
        expected = {1, 2, 3, 4}
        assert reachable == expected, f"Reachable nodes {reachable} != expected {expected}"

    def test_branching_graph(self):
        """
        Test transitive closure with branching.

        Graph:     1 → 3
                 ↗      ↘
                0        5
                 ↘      ↗
                   2 → 4

        Query: 0 p+ ?y
        Expected: 0 reaches [1, 2, 3, 4, 5]
        """
        edges = pa.RecordBatch.from_pydict({
            'start': pa.array([0, 0, 1, 1, 2, 4], type=pa.int64()),
            'end': pa.array([1, 2, 3, 5, 4, 5], type=pa.int64())
        })

        from sabot_ql.graph import build_csr_graph, transitive_closure

        csr = build_csr_graph(edges, 'start', 'end')
        result = transitive_closure(
            csr,
            start_nodes=pa.array([0], type=pa.int64()),
            min_dist=1,
            max_dist=100
        )

        reachable = set(result['end'].to_pylist())
        expected = {1, 2, 3, 4, 5}
        assert reachable == expected

    def test_cycle_detection(self):
        """
        Test that cycles are handled correctly.

        Graph: 0 → 1 → 2 → 0 (cycle)

        Query: 0 p+ ?y
        Expected: 0 reaches [1, 2] (not 0 itself, since min_dist=1)

        Critical: Should NOT infinite loop!
        """
        edges = pa.RecordBatch.from_pydict({
            'start': pa.array([0, 1, 2], type=pa.int64()),
            'end': pa.array([1, 2, 0], type=pa.int64())
        })

        from sabot_ql.graph import build_csr_graph, transitive_closure

        csr = build_csr_graph(edges, 'start', 'end')
        result = transitive_closure(
            csr,
            start_nodes=pa.array([0], type=pa.int64()),
            min_dist=1,
            max_dist=100
        )

        # Should visit each node once (visited set prevents infinite loop)
        reachable = set(result['end'].to_pylist())
        expected = {1, 2}  # Not 0 (min_dist=1 excludes start)
        assert reachable == expected

    def test_multiple_start_nodes(self):
        """
        Test transitive closure from multiple start nodes.

        Graph: 0 → 1 → 2
               3 → 4 → 5

        Query: [0, 3] p+ ?y
        Expected: 0 reaches [1, 2], 3 reaches [4, 5]
        """
        edges = pa.RecordBatch.from_pydict({
            'start': pa.array([0, 1, 3, 4], type=pa.int64()),
            'end': pa.array([1, 2, 4, 5], type=pa.int64())
        })

        from sabot_ql.graph import build_csr_graph, transitive_closure

        csr = build_csr_graph(edges, 'start', 'end')
        result = transitive_closure(
            csr,
            start_nodes=pa.array([0, 3], type=pa.int64()),
            min_dist=1,
            max_dist=100
        )

        # Group results by start node
        start_to_ends = {}
        for i in range(result.num_rows):
            start = result['start'][i].as_py()
            end = result['end'][i].as_py()
            if start not in start_to_ends:
                start_to_ends[start] = set()
            start_to_ends[start].add(end)

        assert start_to_ends[0] == {1, 2}
        assert start_to_ends[3] == {4, 5}

    def test_disconnected_components(self):
        """
        Test with disconnected graph components.

        Graph: 0 → 1    3 → 4 (disconnected)

        Query: 0 p+ ?y
        Expected: 0 reaches [1] only (not 3, 4)
        """
        edges = pa.RecordBatch.from_pydict({
            'start': pa.array([0, 3], type=pa.int64()),
            'end': pa.array([1, 4], type=pa.int64())
        })

        from sabot_ql.graph import build_csr_graph, transitive_closure

        csr = build_csr_graph(edges, 'start', 'end')
        result = transitive_closure(
            csr,
            start_nodes=pa.array([0], type=pa.int64()),
            min_dist=1,
            max_dist=100
        )

        reachable = set(result['end'].to_pylist())
        assert reachable == {1}, "Should not reach disconnected component"

    def test_empty_result(self):
        """Test node with no outgoing edges."""
        edges = pa.RecordBatch.from_pydict({
            'start': pa.array([0], type=pa.int64()),
            'end': pa.array([1], type=pa.int64())
        })

        from sabot_ql.graph import build_csr_graph, transitive_closure

        csr = build_csr_graph(edges, 'start', 'end')
        result = transitive_closure(
            csr,
            start_nodes=pa.array([1], type=pa.int64()),  # Node 1 has no edges
            min_dist=1,
            max_dist=100
        )

        assert result.num_rows == 0, "Should return empty result"


class TestTransitiveClosureZeroPlus:
    """Test p* operator (zero or more hops)."""

    def test_includes_start_node(self):
        """
        Test that p* includes the start node (reflexive).

        Graph: 0 → 1 → 2

        Query: 0 p* ?y
        Expected: 0 reaches [0, 1, 2] (includes 0 itself)
        """
        edges = pa.RecordBatch.from_pydict({
            'start': pa.array([0, 1], type=pa.int64()),
            'end': pa.array([1, 2], type=pa.int64())
        })

        from sabot_ql.graph import build_csr_graph, transitive_closure

        csr = build_csr_graph(edges, 'start', 'end')
        result = transitive_closure(
            csr,
            start_nodes=pa.array([0], type=pa.int64()),
            min_dist=0,  # p* uses min_dist=0
            max_dist=100
        )

        reachable = set(result['end'].to_pylist())
        expected = {0, 1, 2}  # Includes 0 (reflexive)
        assert reachable == expected

    def test_zero_hops_only(self):
        """
        Test node with no edges returns itself for p*.

        Graph: 0 (isolated node)

        Query: 0 p* ?y
        Expected: [0] (reflexive only)
        """
        # Empty graph with node 0
        edges = pa.RecordBatch.from_pydict({
            'start': pa.array([], type=pa.int64()),
            'end': pa.array([], type=pa.int64())
        })

        from sabot_ql.graph import build_csr_graph, transitive_closure

        csr = build_csr_graph(edges, 'start', 'end')
        result = transitive_closure(
            csr,
            start_nodes=pa.array([0], type=pa.int64()),
            min_dist=0,
            max_dist=100
        )

        # Should return reflexive edge only
        reachable = set(result['end'].to_pylist())
        assert reachable == {0}


class TestBoundedPaths:
    """Test bounded path lengths p{m,n}."""

    def test_exact_distance(self):
        """
        Test exact distance constraint.

        Graph: 0 → 1 → 2 → 3 → 4

        Query: 0 p{2,2} ?y (exactly 2 hops)
        Expected: [2] only
        """
        edges = pa.RecordBatch.from_pydict({
            'start': pa.array([0, 1, 2, 3], type=pa.int64()),
            'end': pa.array([1, 2, 3, 4], type=pa.int64())
        })

        from sabot_ql.graph import build_csr_graph, transitive_closure

        csr = build_csr_graph(edges, 'start', 'end')
        result = transitive_closure(
            csr,
            start_nodes=pa.array([0], type=pa.int64()),
            min_dist=2,
            max_dist=2
        )

        reachable = set(result['end'].to_pylist())
        assert reachable == {2}, f"Expected exactly 2 hops to reach node 2, got {reachable}"

    def test_distance_range(self):
        """
        Test distance range.

        Graph: 0 → 1 → 2 → 3 → 4

        Query: 0 p{1,3} ?y (1 to 3 hops)
        Expected: [1, 2, 3]
        """
        edges = pa.RecordBatch.from_pydict({
            'start': pa.array([0, 1, 2, 3], type=pa.int64()),
            'end': pa.array([1, 2, 3, 4], type=pa.int64())
        })

        from sabot_ql.graph import build_csr_graph, transitive_closure

        csr = build_csr_graph(edges, 'start', 'end')
        result = transitive_closure(
            csr,
            start_nodes=pa.array([0], type=pa.int64()),
            min_dist=1,
            max_dist=3
        )

        reachable = set(result['end'].to_pylist())
        expected = {1, 2, 3}  # Not 4 (requires 4 hops)
        assert reachable == expected

    def test_max_distance_prevents_infinite_traversal(self):
        """
        Test that max_dist prevents exploring entire graph.

        Graph: 0 → 1 → 2 → 3 → ... → 100

        Query: 0 p{1,5} ?y
        Expected: [1, 2, 3, 4, 5] only
        """
        # Build chain 0 → 1 → ... → 100
        n = 100
        edges = pa.RecordBatch.from_pydict({
            'start': pa.array(list(range(n)), type=pa.int64()),
            'end': pa.array(list(range(1, n + 1)), type=pa.int64())
        })

        from sabot_ql.graph import build_csr_graph, transitive_closure

        csr = build_csr_graph(edges, 'start', 'end')
        result = transitive_closure(
            csr,
            start_nodes=pa.array([0], type=pa.int64()),
            min_dist=1,
            max_dist=5
        )

        reachable = set(result['end'].to_pylist())
        expected = {1, 2, 3, 4, 5}
        assert reachable == expected
        assert 100 not in reachable, "Should not reach node 100 (beyond max_dist)"


class TestPerformanceCharacteristics:
    """Test performance properties of transitive closure."""

    def test_complexity_is_linear(self):
        """
        Verify O(V + E) time complexity.

        Test with increasing graph sizes.
        """
        import time
        from sabot_ql.graph import build_csr_graph, transitive_closure

        sizes = [100, 1000, 10000]
        times = []

        for n in sizes:
            # Build chain graph: 0 → 1 → ... → n
            edges = pa.RecordBatch.from_pydict({
                'start': pa.array(list(range(n)), type=pa.int64()),
                'end': pa.array(list(range(1, n + 1)), type=pa.int64())
            })

            csr = build_csr_graph(edges, 'start', 'end')

            start = time.perf_counter()
            result = transitive_closure(
                csr,
                start_nodes=pa.array([0], type=pa.int64()),
                min_dist=1,
                max_dist=n
            )
            elapsed = time.perf_counter() - start

            times.append(elapsed)
            print(f"n={n}: {elapsed*1000:.2f}ms ({result.num_rows} nodes reached)")

        # Verify roughly linear scaling
        # 100x more nodes should take <200x time (not 10000x for O(n²))
        ratio = times[2] / times[0]
        assert ratio < 200, f"Time scaling too high: {ratio:.1f}x for 100x nodes"

    def test_visited_set_prevents_redundant_work(self):
        """
        Test that visited set prevents revisiting nodes.

        Graph with many paths to same node should not explore exponentially.
        """
        # Diamond graph: multiple paths 0 → 3
        #     1
        #   ↗   ↘
        #  0     3
        #   ↘   ↗
        #     2
        edges = pa.RecordBatch.from_pydict({
            'start': pa.array([0, 0, 1, 2], type=pa.int64()),
            'end': pa.array([1, 2, 3, 3], type=pa.int64())
        })

        from sabot_ql.graph import build_csr_graph, transitive_closure
        import time

        csr = build_csr_graph(edges, 'start', 'end')

        start = time.perf_counter()
        result = transitive_closure(
            csr,
            start_nodes=pa.array([0], type=pa.int64()),
            min_dist=1,
            max_dist=10
        )
        elapsed = time.perf_counter() - start

        # Should be very fast (<1ms) - visited set prevents redundant paths
        assert elapsed < 0.001, f"Too slow: {elapsed*1000:.2f}ms (visited set not working?)"

        # Each node should appear at most once in result
        reached_nodes = result['end'].to_pylist()
        assert len(reached_nodes) == len(set(reached_nodes)), "Duplicate nodes in result"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
