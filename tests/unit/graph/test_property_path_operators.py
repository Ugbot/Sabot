"""
Unit tests for property path operators.

Tests:
- Inverse path (^p)
- Sequence path (p/q)
- Alternative path (p|q)
- Negation (!p)

TDD Phase: RED - Tests written before implementation.
"""

import pytest
import pyarrow as pa
from pathlib import Path
import sys

sabot_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(sabot_root))


class TestInversePath:
    """Test ^p operator (inverse/reverse path)."""

    def test_simple_inverse(self):
        """
        Test inverse path on simple graph.

        Graph: 0 → 1 → 2 (forward)
        Query: ?x ^p ?y (who points TO y?)

        Expected:
        - For y=2: x=1 (1 points to 2)
        - For y=1: x=0 (0 points to 1)
        """
        edges = pa.RecordBatch.from_pydict({
            'start': pa.array([0, 1], type=pa.int64()),
            'end': pa.array([1, 2], type=pa.int64())
        })

        from sabot_ql.graph import build_csr_graph, build_reverse_csr_graph, transitive_closure

        # Build reverse CSR (swap start/end)
        reverse_csr = build_reverse_csr_graph(edges, 'start', 'end')

        # Query: Who points TO node 2?
        result = transitive_closure(
            reverse_csr,
            start_nodes=pa.array([2], type=pa.int64()),
            min_dist=1,
            max_dist=1  # Direct inverse only
        )

        # Expected: Node 1 points to 2
        assert result.num_rows == 1
        assert result['end'][0].as_py() == 1

    def test_inverse_transitive(self):
        """
        Test inverse transitive path (^p+).

        Graph: 0 → 1 → 2 → 3
        Query: ?x ^p+ 3 (who can reach node 3?)

        Expected: [2, 1, 0] (all predecessors)
        """
        edges = pa.RecordBatch.from_pydict({
            'start': pa.array([0, 1, 2], type=pa.int64()),
            'end': pa.array([1, 2, 3], type=pa.int64())
        })

        from sabot_ql.graph import build_reverse_csr_graph, transitive_closure

        reverse_csr = build_reverse_csr_graph(edges, 'start', 'end')

        # Query: Transitive predecessors of node 3
        result = transitive_closure(
            reverse_csr,
            start_nodes=pa.array([3], type=pa.int64()),
            min_dist=1,
            max_dist=100
        )

        predecessors = set(result['end'].to_pylist())
        expected = {0, 1, 2}
        assert predecessors == expected

    def test_inverse_with_cycle(self):
        """
        Test inverse on cyclic graph.

        Graph: 0 ⇄ 1 (bidirectional)
        Query: ?x ^p 1

        Expected: x=0 (0 points to 1)
        """
        edges = pa.RecordBatch.from_pydict({
            'start': pa.array([0, 1], type=pa.int64()),
            'end': pa.array([1, 0], type=pa.int64())
        })

        from sabot_ql.graph import build_reverse_csr_graph, transitive_closure

        reverse_csr = build_reverse_csr_graph(edges, 'start', 'end')

        result = transitive_closure(
            reverse_csr,
            start_nodes=pa.array([1], type=pa.int64()),
            min_dist=1,
            max_dist=1
        )

        assert result.num_rows == 1
        assert result['end'][0].as_py() == 0


class TestSequencePath:
    """Test p/q operator (path sequence)."""

    def test_simple_sequence(self):
        """
        Test two-step path sequence.

        Graph:
          0 --p--> 1 --q--> 2

        Query: ?x (p/q) ?y
        Expected: (0, 2) - following p then q
        """
        # First path: 0 --p--> 1
        path_p = pa.RecordBatch.from_pydict({
            'start': pa.array([0], type=pa.int64()),
            'end': pa.array([1], type=pa.int64())
        })

        # Second path: 1 --q--> 2
        path_q = pa.RecordBatch.from_pydict({
            'start': pa.array([1], type=pa.int64()),
            'end': pa.array([2], type=pa.int64())
        })

        from sabot_ql.graph import sequence_path

        # Sequence: follow p, then q
        result = sequence_path(path_p, path_q)

        # Expected: 0 → 2 (via 1)
        assert result.num_rows == 1
        assert result['start'][0].as_py() == 0
        assert result['end'][0].as_py() == 2

    def test_sequence_with_branching(self):
        """
        Test sequence with multiple intermediate nodes.

        Graph:
          0 --p--> 1 --q--> 3
          0 --p--> 2 --q--> 4

        Query: ?x (p/q) ?y
        Expected: (0, 3) and (0, 4)
        """
        path_p = pa.RecordBatch.from_pydict({
            'start': pa.array([0, 0], type=pa.int64()),
            'end': pa.array([1, 2], type=pa.int64())
        })

        path_q = pa.RecordBatch.from_pydict({
            'start': pa.array([1, 2], type=pa.int64()),
            'end': pa.array([3, 4], type=pa.int64())
        })

        from sabot_ql.graph import sequence_path

        result = sequence_path(path_p, path_q)

        # Expected: (0, 3) and (0, 4)
        assert result.num_rows == 2
        pairs = set(zip(result['start'].to_pylist(), result['end'].to_pylist()))
        expected = {(0, 3), (0, 4)}
        assert pairs == expected

    def test_sequence_no_match(self):
        """
        Test sequence where paths don't connect.

        Graph:
          0 --p--> 1
          2 --q--> 3  (disconnected)

        Query: ?x (p/q) ?y
        Expected: empty (no connection)
        """
        path_p = pa.RecordBatch.from_pydict({
            'start': pa.array([0], type=pa.int64()),
            'end': pa.array([1], type=pa.int64())
        })

        path_q = pa.RecordBatch.from_pydict({
            'start': pa.array([2], type=pa.int64()),
            'end': pa.array([3], type=pa.int64())
        })

        from sabot_ql.graph import sequence_path

        result = sequence_path(path_p, path_q)

        # No connection between paths
        assert result.num_rows == 0


class TestAlternativePath:
    """Test p|q operator (path alternative)."""

    def test_simple_alternative(self):
        """
        Test path alternative (union).

        Graph:
          0 --p--> 1
          0 --q--> 2

        Query: ?x (p|q) ?y
        Expected: (0, 1) and (0, 2)
        """
        path_p = pa.RecordBatch.from_pydict({
            'start': pa.array([0], type=pa.int64()),
            'end': pa.array([1], type=pa.int64())
        })

        path_q = pa.RecordBatch.from_pydict({
            'start': pa.array([0], type=pa.int64()),
            'end': pa.array([2], type=pa.int64())
        })

        from sabot_ql.graph import alternative_path

        result = alternative_path(path_p, path_q)

        # Both paths included
        assert result.num_rows == 2
        pairs = set(zip(result['start'].to_pylist(), result['end'].to_pylist()))
        expected = {(0, 1), (0, 2)}
        assert pairs == expected

    def test_alternative_with_duplicates(self):
        """
        Test alternative with duplicate paths (should deduplicate).

        Graph:
          0 --p--> 1
          0 --q--> 1  (same destination)

        Query: ?x (p|q) ?y
        Expected: (0, 1) once (deduplicated)
        """
        path_p = pa.RecordBatch.from_pydict({
            'start': pa.array([0], type=pa.int64()),
            'end': pa.array([1], type=pa.int64())
        })

        path_q = pa.RecordBatch.from_pydict({
            'start': pa.array([0], type=pa.int64()),
            'end': pa.array([1], type=pa.int64())
        })

        from sabot_ql.graph import alternative_path

        result = alternative_path(path_p, path_q)

        # Duplicate should be removed
        assert result.num_rows == 1
        assert result['start'][0].as_py() == 0
        assert result['end'][0].as_py() == 1

    def test_alternative_empty_path(self):
        """Test alternative where one path is empty."""
        path_p = pa.RecordBatch.from_pydict({
            'start': pa.array([0], type=pa.int64()),
            'end': pa.array([1], type=pa.int64())
        })

        path_q = pa.RecordBatch.from_pydict({
            'start': pa.array([], type=pa.int64()),
            'end': pa.array([], type=pa.int64())
        })

        from sabot_ql.graph import alternative_path

        result = alternative_path(path_p, path_q)

        # Should return non-empty path
        assert result.num_rows == 1
        assert result['start'][0].as_py() == 0
        assert result['end'][0].as_py() == 1


class TestNegatedPropertySet:
    """Test !p operator (negated property set)."""

    def test_simple_negation(self):
        """
        Test negated property.

        Graph (with predicates):
          0 --p1--> 1
          0 --p2--> 2
          0 --p3--> 3

        Query: ?x !p2 ?y (any predicate EXCEPT p2)
        Expected: (0, 1) and (0, 3) - not (0, 2)
        """
        edges = pa.RecordBatch.from_pydict({
            'start': pa.array([0, 0, 0], type=pa.int64()),
            'predicate': pa.array([10, 20, 30], type=pa.int64()),
            'end': pa.array([1, 2, 3], type=pa.int64())
        })

        from sabot_ql.graph import filter_by_predicate

        # Filter: exclude predicate 20
        result = filter_by_predicate(edges, excluded_predicates=[20])

        # Should have 2 rows (not the one with predicate=20)
        assert result.num_rows == 2
        pairs = set(zip(result['start'].to_pylist(), result['end'].to_pylist()))
        expected = {(0, 1), (0, 3)}
        assert pairs == expected

    def test_negation_multiple_predicates(self):
        """Test negating multiple predicates."""
        edges = pa.RecordBatch.from_pydict({
            'start': pa.array([0, 0, 0, 0], type=pa.int64()),
            'predicate': pa.array([10, 20, 30, 40], type=pa.int64()),
            'end': pa.array([1, 2, 3, 4], type=pa.int64())
        })

        from sabot_ql.graph import filter_by_predicate

        # Exclude predicates 20 and 30
        result = filter_by_predicate(edges, excluded_predicates=[20, 30])

        assert result.num_rows == 2
        remaining_preds = set(result['predicate'].to_pylist())
        expected = {10, 40}
        assert remaining_preds == expected

    def test_negation_all_excluded(self):
        """Test when all predicates are excluded."""
        edges = pa.RecordBatch.from_pydict({
            'start': pa.array([0], type=pa.int64()),
            'predicate': pa.array([10], type=pa.int64()),
            'end': pa.array([1], type=pa.int64())
        })

        from sabot_ql.graph import filter_by_predicate

        # Exclude all predicates
        result = filter_by_predicate(edges, excluded_predicates=[10])

        # Should be empty
        assert result.num_rows == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
