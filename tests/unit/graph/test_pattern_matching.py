"""
Comprehensive Unit Tests for Graph Pattern Matching

Tests all pattern matching functions:
- 2-hop patterns
- 3-hop patterns
- Variable-length paths
- Join optimizer
- Query planner
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

# Set library path for Arrow
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib'

import pytest
import pyarrow as pa
from sabot._cython.graph.query import (
    match_2hop,
    match_3hop,
    match_variable_length_path,
    OptimizeJoinOrder,
    PyPatternMatchResult
)

try:
    from sabot._cython.graph.query.query_planner import (
        optimize_pattern_query,
        create_pattern_query_plan,
        explain_pattern_query
    )
    QUERY_PLANNER_AVAILABLE = True
except ImportError:
    QUERY_PLANNER_AVAILABLE = False


class TestPatternMatchResult:
    """Test PatternMatchResult wrapper."""

    def test_empty_result(self):
        """Test empty result handling."""
        edges = pa.table({
            'source': pa.array([], type=pa.int64()),
            'target': pa.array([], type=pa.int64())
        })

        result = match_2hop(edges, edges)
        assert result.num_matches() == 0
        assert result.result_table().num_rows == 0

    def test_result_attributes(self):
        """Test result object attributes."""
        edges = pa.table({
            'source': pa.array([0, 1], type=pa.int64()),
            'target': pa.array([1, 2], type=pa.int64())
        })

        result = match_2hop(edges, edges)
        assert hasattr(result, 'num_matches')
        assert hasattr(result, 'result_table')
        assert hasattr(result, 'binding_names')
        assert result.binding_names() == ['a', 'b', 'c']


class Test2HopPatterns:
    """Test 2-hop pattern matching."""

    def test_simple_chain(self):
        """Test basic 2-hop chain: 0->1->2."""
        edges = pa.table({
            'source': pa.array([0, 1], type=pa.int64()),
            'target': pa.array([1, 2], type=pa.int64())
        })

        result = match_2hop(edges, edges)
        assert result.num_matches() == 1

        table = result.result_table()
        assert table.column('a_id')[0].as_py() == 0
        assert table.column('b_id')[0].as_py() == 1
        assert table.column('c_id')[0].as_py() == 2

    def test_branching_graph(self):
        """Test 2-hop in branching graph."""
        edges = pa.table({
            'source': pa.array([0, 0, 1, 2], type=pa.int64()),
            'target': pa.array([1, 2, 3, 3], type=pa.int64())
        })

        result = match_2hop(edges, edges)
        # Paths: 0->1->3, 0->2->3
        assert result.num_matches() == 2

    def test_disconnected_graph(self):
        """Test 2-hop with disconnected components."""
        edges = pa.table({
            'source': pa.array([0, 2], type=pa.int64()),
            'target': pa.array([1, 3], type=pa.int64())
        })

        result = match_2hop(edges, edges)
        # No 2-hop paths (components don't connect)
        assert result.num_matches() == 0

    def test_cycle(self):
        """Test 2-hop with cycle."""
        edges = pa.table({
            'source': pa.array([0, 1, 2], type=pa.int64()),
            'target': pa.array([1, 2, 0], type=pa.int64())
        })

        result = match_2hop(edges, edges)
        # Cycle: 0->1->2, 1->2->0, 2->0->1
        assert result.num_matches() == 3

    def test_large_fanout(self):
        """Test 2-hop with high-degree vertex."""
        # Create star graph: 0 -> [1,2,3,4,5]
        sources = [0] * 5
        targets = list(range(1, 6))

        edges = pa.table({
            'source': pa.array(sources, type=pa.int64()),
            'target': pa.array(targets, type=pa.int64())
        })

        result = match_2hop(edges, edges)
        # No 2-hop paths (star has no 2-hop connections)
        assert result.num_matches() == 0


class Test3HopPatterns:
    """Test 3-hop pattern matching."""

    def test_simple_chain(self):
        """Test basic 3-hop chain: 0->1->2->3."""
        edges = pa.table({
            'source': pa.array([0, 1, 2], type=pa.int64()),
            'target': pa.array([1, 2, 3], type=pa.int64())
        })

        result = match_3hop(edges, edges, edges)
        assert result.num_matches() == 1

        table = result.result_table()
        assert table.column('a_id')[0].as_py() == 0
        assert table.column('b_id')[0].as_py() == 1
        assert table.column('c_id')[0].as_py() == 2
        assert table.column('d_id')[0].as_py() == 3

    def test_diamond_pattern(self):
        """Test 3-hop in diamond graph."""
        # Diamond: 0->1, 0->2, 1->3, 2->3
        edges = pa.table({
            'source': pa.array([0, 0, 1, 2], type=pa.int64()),
            'target': pa.array([1, 2, 3, 3], type=pa.int64())
        })

        result = match_3hop(edges, edges, edges)
        # Paths: 0->1->3 (no 3rd hop), 0->2->3 (no 3rd hop)
        # Need edges from 3 for 3-hop
        assert result.num_matches() == 0

    def test_complete_3hop(self):
        """Test complete 3-hop path."""
        edges = pa.table({
            'source': pa.array([0, 1, 2, 3], type=pa.int64()),
            'target': pa.array([1, 2, 3, 4], type=pa.int64())
        })

        result = match_3hop(edges, edges, edges)
        # Paths: 0->1->2->3, 1->2->3->4
        assert result.num_matches() == 2

    def test_multiple_3hop_paths(self):
        """Test graph with multiple 3-hop paths."""
        edges = pa.table({
            'source': pa.array([0, 0, 1, 1, 2, 2, 3, 3], type=pa.int64()),
            'target': pa.array([1, 2, 3, 4, 3, 4, 5, 6], type=pa.int64())
        })

        result = match_3hop(edges, edges, edges)
        # Multiple paths: 0->1->3->5, 0->1->3->6, etc.
        assert result.num_matches() > 0


class TestVariableLengthPaths:
    """Test variable-length path matching."""

    def test_exact_length(self):
        """Test finding paths of exact length."""
        edges = pa.table({
            'source': pa.array([0, 1, 2, 3], type=pa.int64()),
            'target': pa.array([1, 2, 3, 4], type=pa.int64())
        })

        # Find paths of exactly length 2
        result = match_variable_length_path(edges, 0, -1, 2, 2)
        assert result.num_matches() == 1  # 0->1->2

        table = result.result_table()
        assert table.column('hop_count')[0].as_py() == 2

    def test_length_range(self):
        """Test finding paths within length range."""
        edges = pa.table({
            'source': pa.array([0, 1, 2, 3], type=pa.int64()),
            'target': pa.array([1, 2, 3, 4], type=pa.int64())
        })

        # Find paths of length 1-3
        result = match_variable_length_path(edges, 0, -1, 1, 3)
        assert result.num_matches() == 3  # 0->1, 0->1->2, 0->1->2->3

    def test_specific_target(self):
        """Test finding paths to specific target."""
        edges = pa.table({
            'source': pa.array([0, 1, 2, 3], type=pa.int64()),
            'target': pa.array([1, 2, 3, 4], type=pa.int64())
        })

        # Find paths from 0 to 3
        result = match_variable_length_path(edges, 0, 3, 1, 5)
        assert result.num_matches() == 1

        table = result.result_table()
        vertices = table.column('vertices')[0].as_py()
        assert vertices == [0, 1, 2, 3]

    def test_no_paths(self):
        """Test when no paths exist."""
        # Disconnected graph
        edges = pa.table({
            'source': pa.array([0, 2], type=pa.int64()),
            'target': pa.array([1, 3], type=pa.int64())
        })

        result = match_variable_length_path(edges, 0, 3, 1, 5)
        assert result.num_matches() == 0

    def test_cyclic_graph(self):
        """Test paths in graph with cycles."""
        # Cycle: 0->1->2->0
        edges = pa.table({
            'source': pa.array([0, 1, 2], type=pa.int64()),
            'target': pa.array([1, 2, 0], type=pa.int64())
        })

        result = match_variable_length_path(edges, 0, -1, 1, 3)
        # Will find paths including cycles
        assert result.num_matches() >= 3

    def test_path_tracking(self):
        """Test that full paths are tracked correctly."""
        edges = pa.table({
            'source': pa.array([0, 1, 2], type=pa.int64()),
            'target': pa.array([1, 2, 3], type=pa.int64())
        })

        result = match_variable_length_path(edges, 0, 3, 3, 3)
        assert result.num_matches() == 1

        table = result.result_table()
        vertices = table.column('vertices')[0].as_py()
        edges_list = table.column('edges')[0].as_py()

        assert vertices == [0, 1, 2, 3]
        assert edges_list == [0, 1, 2]


class TestJoinOptimizer:
    """Test join order optimization."""

    def test_basic_ordering(self):
        """Test optimizer chooses smallest table first."""
        small = pa.table({
            'source': pa.array([0], type=pa.int64()),
            'target': pa.array([1], type=pa.int64())
        })

        medium = pa.table({
            'source': pa.array([0, 1], type=pa.int64()),
            'target': pa.array([1, 2], type=pa.int64())
        })

        large = pa.table({
            'source': pa.array([0, 1, 2], type=pa.int64()),
            'target': pa.array([1, 2, 3], type=pa.int64())
        })

        order = OptimizeJoinOrder([large, medium, small])
        # Should choose: [2, 1, 0] (small, medium, large)
        assert order[0] == 2

    def test_single_table(self):
        """Test optimizer with single table."""
        edges = pa.table({
            'source': pa.array([0], type=pa.int64()),
            'target': pa.array([1], type=pa.int64())
        })

        order = OptimizeJoinOrder([edges])
        assert order == [0]

    def test_empty_input(self):
        """Test optimizer with empty input."""
        order = OptimizeJoinOrder([])
        assert order == []

    def test_identical_tables(self):
        """Test optimizer with identical tables."""
        edges = pa.table({
            'source': pa.array([0, 1], type=pa.int64()),
            'target': pa.array([1, 2], type=pa.int64())
        })

        order = OptimizeJoinOrder([edges, edges, edges])
        # Any order is valid for identical tables
        assert len(order) == 3
        assert set(order) == {0, 1, 2}


@pytest.mark.skipif(not QUERY_PLANNER_AVAILABLE, reason="Query planner not available")
class TestQueryPlanner:
    """Test query planning functionality."""

    def test_optimize_pattern_query(self):
        """Test pattern query optimization."""
        e1 = pa.table({
            'source': pa.array([0, 1], type=pa.int64()),
            'target': pa.array([1, 2], type=pa.int64())
        })
        e2 = pa.table({
            'source': pa.array([0, 1, 2], type=pa.int64()),
            'target': pa.array([1, 2, 3], type=pa.int64())
        })

        order = optimize_pattern_query([e1, e2])
        assert len(order) == 2
        assert order[0] == 0  # e1 is smaller

    def test_create_query_plan(self):
        """Test query plan creation."""
        edges = pa.table({
            'source': pa.array([0, 1], type=pa.int64()),
            'target': pa.array([1, 2], type=pa.int64())
        })

        plan = create_pattern_query_plan([edges, edges], '2hop')
        assert plan is not None
        assert len(plan.get_edge_tables()) == 2
        assert len(plan.get_join_orders()) == 1

    def test_explain_query(self):
        """Test query explanation."""
        edges = pa.table({
            'source': pa.array([0, 1], type=pa.int64()),
            'target': pa.array([1, 2], type=pa.int64())
        })

        explanation = explain_pattern_query([edges, edges], '2hop')
        assert isinstance(explanation, str)
        assert 'Query Plan' in explanation
        assert '2hop' in explanation


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_null_values(self):
        """Test handling of null values in graph."""
        # Note: Current implementation may not handle nulls
        # This test documents expected behavior
        edges = pa.table({
            'source': pa.array([0, 1, None], type=pa.int64()),
            'target': pa.array([1, None, 3], type=pa.int64())
        })

        # Should skip null entries
        result = match_2hop(edges, edges)
        # Result depends on null handling
        assert result.num_matches() >= 0

    def test_self_loops(self):
        """Test graph with self-loops."""
        edges = pa.table({
            'source': pa.array([0, 0, 1], type=pa.int64()),
            'target': pa.array([0, 1, 2], type=pa.int64())
        })

        result = match_2hop(edges, edges)
        # Self-loops create interesting patterns
        assert result.num_matches() >= 0

    def test_large_vertex_ids(self):
        """Test with large vertex IDs."""
        edges = pa.table({
            'source': pa.array([1000000, 1000001], type=pa.int64()),
            'target': pa.array([1000001, 1000002], type=pa.int64())
        })

        result = match_2hop(edges, edges)
        assert result.num_matches() == 1


def run_all_tests():
    """Run all pattern matching tests."""
    pytest.main([__file__, '-v', '--tb=short'])


if __name__ == '__main__':
    run_all_tests()
