"""
Unit tests for Arrow-based graph representations.

Tests the CSR (Compressed Sparse Row) format for efficient graph storage
and traversal, following TDD principles.
"""

import pytest
import pyarrow as pa
import pyarrow.compute as pc
from pathlib import Path
import sys

# Add sabot_ql bindings to path (will be built in GREEN phase)
sabot_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(sabot_root))


class TestCSRGraphFormat:
    """Test CSR (Compressed Sparse Row) graph representation."""

    def test_build_csr_from_simple_triples(self):
        """
        Test converting RDF triples to CSR format.

        Graph structure:
          0 → 1
          0 → 2
          1 → 2
          2 → 3
          2 → 0 (cycle back)

        Expected CSR:
          offsets: [0, 2, 3, 5, 5]  (node boundaries)
          targets: [1, 2, 2, 3, 0]  (edge targets)
          edge_ids: [0, 1, 2, 3, 4] (original indices)
        """
        triples = pa.RecordBatch.from_pydict({
            'subject': pa.array([0, 0, 1, 2, 2], type=pa.int64()),
            'predicate': pa.array([10, 10, 10, 10, 10], type=pa.int64()),
            'object': pa.array([1, 2, 2, 3, 0], type=pa.int64())
        })

        # This will fail until we implement the kernel
        from sabot_ql.graph import build_csr_graph

        csr = build_csr_graph(triples, source_col='subject', target_col='object')

        # Verify CSR structure
        assert csr.num_nodes == 4, f"Expected 4 nodes, got {csr.num_nodes}"
        assert csr.num_edges == 5, f"Expected 5 edges, got {csr.num_edges}"

        # Verify offsets array
        expected_offsets = [0, 2, 3, 5, 5]
        actual_offsets = csr.offsets.to_pylist()
        assert actual_offsets == expected_offsets, \
            f"Offsets mismatch: expected {expected_offsets}, got {actual_offsets}"

        # Verify targets array
        expected_targets = [1, 2, 2, 3, 0]
        actual_targets = csr.targets.to_pylist()
        assert actual_targets == expected_targets, \
            f"Targets mismatch: expected {expected_targets}, got {actual_targets}"

        # Verify edge IDs (should preserve original order)
        expected_edge_ids = [0, 1, 2, 3, 4]
        actual_edge_ids = csr.edge_ids.to_pylist()
        assert actual_edge_ids == expected_edge_ids, \
            f"Edge IDs mismatch: expected {expected_edge_ids}, got {actual_edge_ids}"

    def test_build_csr_empty_graph(self):
        """Test CSR construction with empty graph."""
        empty_triples = pa.RecordBatch.from_pydict({
            'subject': pa.array([], type=pa.int64()),
            'predicate': pa.array([], type=pa.int64()),
            'object': pa.array([], type=pa.int64())
        })

        from sabot_ql.graph import build_csr_graph

        csr = build_csr_graph(empty_triples, source_col='subject', target_col='object')

        assert csr.num_nodes == 0
        assert csr.num_edges == 0
        assert len(csr.offsets) == 1  # Just [0]
        assert len(csr.targets) == 0

    def test_build_csr_disconnected_nodes(self):
        """Test CSR with disconnected components."""
        # Graph: 0 → 1, 3 → 4 (node 2 isolated)
        triples = pa.RecordBatch.from_pydict({
            'subject': pa.array([0, 3], type=pa.int64()),
            'predicate': pa.array([10, 10], type=pa.int64()),
            'object': pa.array([1, 4], type=pa.int64())
        })

        from sabot_ql.graph import build_csr_graph

        csr = build_csr_graph(triples, source_col='subject', target_col='object')

        # Should include all nodes 0-4
        assert csr.num_nodes == 5
        assert csr.num_edges == 2

        # Node 2 should have no outgoing edges
        offsets = csr.offsets.to_pylist()
        # offsets[2] == offsets[3] means node 2 has no edges
        assert offsets[2] == offsets[3]

    def test_build_csr_self_loops(self):
        """Test CSR with self-loops."""
        # Graph: 0 → 0, 0 → 1
        triples = pa.RecordBatch.from_pydict({
            'subject': pa.array([0, 0], type=pa.int64()),
            'predicate': pa.array([10, 10], type=pa.int64()),
            'object': pa.array([0, 1], type=pa.int64())
        })

        from sabot_ql.graph import build_csr_graph

        csr = build_csr_graph(triples, source_col='subject', target_col='object')

        assert csr.num_nodes == 2
        assert csr.num_edges == 2
        assert 0 in csr.targets.to_pylist()  # Self-loop preserved


class TestCSRSuccessors:
    """Test CSR successor lookups."""

    @pytest.fixture
    def sample_csr(self):
        """Create a sample CSR graph for testing."""
        triples = pa.RecordBatch.from_pydict({
            'subject': pa.array([0, 0, 1, 2, 2], type=pa.int64()),
            'predicate': pa.array([10, 10, 10, 10, 10], type=pa.int64()),
            'object': pa.array([1, 2, 2, 3, 0], type=pa.int64())
        })

        from sabot_ql.graph import build_csr_graph
        return build_csr_graph(triples, source_col='subject', target_col='object')

    def test_get_successors_node_with_edges(self, sample_csr):
        """Test getting successors of node with outgoing edges."""
        # Node 0 has successors [1, 2]
        successors = sample_csr.get_successors(0)

        assert isinstance(successors, pa.Int64Array)
        assert successors.to_pylist() == [1, 2]

    def test_get_successors_node_one_edge(self, sample_csr):
        """Test getting successors of node with one outgoing edge."""
        # Node 1 has successor [2]
        successors = sample_csr.get_successors(1)

        assert successors.to_pylist() == [2]

    def test_get_successors_node_no_edges(self, sample_csr):
        """Test getting successors of node with no outgoing edges."""
        # Node 3 has no outgoing edges
        successors = sample_csr.get_successors(3)

        assert len(successors) == 0

    def test_get_successors_invalid_node_negative(self, sample_csr):
        """Test getting successors with negative node ID."""
        successors = sample_csr.get_successors(-1)

        # Should return empty array, not crash
        assert len(successors) == 0

    def test_get_successors_invalid_node_out_of_range(self, sample_csr):
        """Test getting successors with node ID beyond graph."""
        successors = sample_csr.get_successors(1000)

        # Should return empty array, not crash
        assert len(successors) == 0

    def test_get_successors_zero_copy(self, sample_csr):
        """Test that get_successors returns zero-copy slice."""
        successors = sample_csr.get_successors(0)

        # Verify it's a slice of the original targets array
        # (This is a performance property - should not copy data)
        # In Arrow, slices share the same buffer
        assert successors.buffers()[1].address == sample_csr.targets.buffers()[1].address


class TestCSRPerformance:
    """Performance characteristics of CSR format."""

    def test_csr_build_time_complexity(self):
        """Verify CSR build is O(E) - linear in edge count."""
        import time
        from sabot_ql.graph import build_csr_graph

        # Test with increasing edge counts
        edge_counts = [100, 1000, 10000]
        build_times = []

        for num_edges in edge_counts:
            # Generate random graph
            import numpy as np
            subjects = np.random.randint(0, num_edges // 10, num_edges)
            objects = np.random.randint(0, num_edges // 10, num_edges)

            triples = pa.RecordBatch.from_pydict({
                'subject': pa.array(subjects, type=pa.int64()),
                'predicate': pa.array([10] * num_edges, type=pa.int64()),
                'object': pa.array(objects, type=pa.int64())
            })

            start = time.perf_counter()
            csr = build_csr_graph(triples, source_col='subject', target_col='object')
            elapsed = time.perf_counter() - start

            build_times.append(elapsed)
            print(f"Built CSR for {num_edges} edges in {elapsed*1000:.2f}ms")

        # Verify roughly linear scaling
        # 10x more edges should take <20x time (not 100x for O(E²))
        ratio = build_times[2] / build_times[0]
        assert ratio < 20, f"Build time scaling too high: {ratio:.1f}x for 100x edges"

    def test_successor_lookup_constant_time(self):
        """Verify successor lookup is O(1)."""
        from sabot_ql.graph import build_csr_graph
        import time

        # Build large graph
        num_edges = 100000
        import numpy as np
        subjects = np.random.randint(0, 10000, num_edges)
        objects = np.random.randint(0, 10000, num_edges)

        triples = pa.RecordBatch.from_pydict({
            'subject': pa.array(subjects, type=pa.int64()),
            'predicate': pa.array([10] * num_edges, type=pa.int64()),
            'object': pa.array(objects, type=pa.int64())
        })

        csr = build_csr_graph(triples, source_col='subject', target_col='object')

        # Time 1000 random lookups
        nodes_to_lookup = np.random.randint(0, csr.num_nodes, 1000)

        start = time.perf_counter()
        for node in nodes_to_lookup:
            successors = csr.get_successors(int(node))
        elapsed = time.perf_counter() - start

        avg_lookup_time = elapsed / 1000 * 1e6  # microseconds
        print(f"Average successor lookup: {avg_lookup_time:.2f}µs")

        # Should be very fast (< 10µs per lookup)
        assert avg_lookup_time < 10, f"Lookup too slow: {avg_lookup_time:.2f}µs"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
