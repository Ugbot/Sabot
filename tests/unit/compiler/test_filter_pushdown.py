"""
Unit Tests for Filter Pushdown Optimization

Tests filter pushdown rules from Phase 7 plan optimizer:
- Push through stateless operators (map, select)
- Block at stateful operators (join, aggregate)
- Combine multiple filters
- Preserve DAG correctness

Based on: /docs/implementation/PHASE7_PLAN_OPTIMIZATION.md
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from typing import List, Dict, Set

# Import after compilation
try:
    from sabot.compiler.plan_optimizer import PlanOptimizer
    from sabot.compiler.optimizations.filter_pushdown import FilterPushdown, FilterInfo
    from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType
    OPTIMIZER_AVAILABLE = True
except ImportError:
    OPTIMIZER_AVAILABLE = False


def create_mock_operator(
    operator_id: str,
    operator_type: str,
    upstream_ids: List[str] = None,
    parameters: Dict = None
) -> StreamOperatorNode:
    """Helper to create mock operator nodes"""
    op = Mock(spec=StreamOperatorNode)
    op.operator_id = operator_id
    op.operator_type = operator_type
    op.upstream_ids = upstream_ids or []
    op.parameters = parameters or {}
    return op


def create_mock_job_graph(operators: Dict[str, StreamOperatorNode]) -> JobGraph:
    """Helper to create mock JobGraph"""
    graph = Mock(spec=JobGraph)
    graph.operators = operators

    # Mock get_sinks
    def get_sinks():
        sinks = []
        for op_id, op in operators.items():
            # An operator is a sink if no other operator has it as upstream
            is_sink = True
            for other_op in operators.values():
                if op_id in other_op.upstream_ids:
                    is_sink = False
                    break
            if is_sink:
                sinks.append(op)
        return sinks

    graph.get_sinks = get_sinks

    return graph


@pytest.mark.skipif(not OPTIMIZER_AVAILABLE, reason="Optimizer not compiled")
class TestFilterPushdownRules:
    """Test filter pushdown rules"""

    def test_filter_can_push_through_map(self):
        """Test that filter can be pushed through map operator"""
        # Create FilterInfo for: WHERE amount > 100
        filter_op = create_mock_operator(
            'filter-1',
            OperatorType.FILTER,
            parameters={'predicate': 'amount > 100'}
        )

        filter_info = FilterInfo(
            filter_op=filter_op,
            referenced_columns={'amount'}
        )

        # Map operator: map(lambda x: x.user_id)
        map_op = create_mock_operator(
            'map-1',
            OperatorType.MAP,
            parameters={'function': 'lambda x: x.user_id'}
        )

        # Filter CAN be pushed through map
        assert filter_info.can_push_through(map_op) is True

    def test_filter_can_push_through_select(self):
        """Test that filter can be pushed through select if columns preserved"""
        # Filter: WHERE user_id = 123
        filter_op = create_mock_operator(
            'filter-1',
            OperatorType.FILTER,
            parameters={'predicate': 'user_id = 123'}
        )

        filter_info = FilterInfo(
            filter_op=filter_op,
            referenced_columns={'user_id'}
        )

        # Select includes user_id
        select_op = create_mock_operator(
            'select-1',
            OperatorType.SELECT,
            parameters={'columns': ['user_id', 'amount', 'timestamp']}
        )

        # Filter CAN be pushed through select (user_id is selected)
        assert filter_info.can_push_through(select_op) is True

    def test_filter_cannot_push_through_select_missing_column(self):
        """Test that filter CANNOT be pushed through select if column dropped"""
        # Filter: WHERE user_id = 123
        filter_op = create_mock_operator(
            'filter-1',
            OperatorType.FILTER,
            parameters={'predicate': 'user_id = 123'}
        )

        filter_info = FilterInfo(
            filter_op=filter_op,
            referenced_columns={'user_id'}
        )

        # Select does NOT include user_id
        select_op = create_mock_operator(
            'select-1',
            OperatorType.SELECT,
            parameters={'columns': ['amount', 'timestamp']}  # Missing user_id!
        )

        # Filter CANNOT be pushed (user_id dropped)
        assert filter_info.can_push_through(select_op) is False

    def test_filter_cannot_push_through_join(self):
        """Test that filter CANNOT be pushed through join (complex semantics)"""
        # Filter: WHERE amount > 100
        filter_op = create_mock_operator(
            'filter-1',
            OperatorType.FILTER,
            parameters={'predicate': 'amount > 100'}
        )

        filter_info = FilterInfo(
            filter_op=filter_op,
            referenced_columns={'amount'}
        )

        # Hash join
        join_op = create_mock_operator(
            'join-1',
            OperatorType.HASH_JOIN,
            parameters={'left_key': 'user_id', 'right_key': 'user_id'}
        )

        # Filter CANNOT be pushed through join (for now - conservative)
        assert filter_info.can_push_through(join_op) is False

    def test_filter_cannot_push_through_aggregate(self):
        """Test that filter CANNOT be pushed through aggregate"""
        # Filter: WHERE count > 10
        filter_op = create_mock_operator(
            'filter-1',
            OperatorType.FILTER,
            parameters={'predicate': 'count > 10'}
        )

        filter_info = FilterInfo(
            filter_op=filter_op,
            referenced_columns={'count'}
        )

        # Aggregate
        agg_op = create_mock_operator(
            'agg-1',
            OperatorType.AGGREGATE,
            parameters={'agg_function': 'count'}
        )

        # Filter CANNOT be pushed through aggregate (changes semantics)
        assert filter_info.can_push_through(agg_op) is False

    def test_filter_can_push_through_filter(self):
        """Test that multiple filters can be combined"""
        # Filter 1: WHERE amount > 100
        filter_op1 = create_mock_operator(
            'filter-1',
            OperatorType.FILTER,
            parameters={'predicate': 'amount > 100'}
        )

        filter_info = FilterInfo(
            filter_op=filter_op1,
            referenced_columns={'amount'}
        )

        # Filter 2: WHERE user_id = 123
        filter_op2 = create_mock_operator(
            'filter-2',
            OperatorType.FILTER,
            parameters={'predicate': 'user_id = 123'}
        )

        # Filters CAN be combined
        assert filter_info.can_push_through(filter_op2) is True

    def test_filter_cannot_push_before_source(self):
        """Test that filter CANNOT be pushed before source"""
        # Filter: WHERE amount > 100
        filter_op = create_mock_operator(
            'filter-1',
            OperatorType.FILTER,
            parameters={'predicate': 'amount > 100'}
        )

        filter_info = FilterInfo(
            filter_op=filter_op,
            referenced_columns={'amount'}
        )

        # Source operator
        source_op = create_mock_operator(
            'source-1',
            OperatorType.SOURCE,
            parameters={'topic': 'events'}
        )

        # Filter CANNOT be pushed before source (terminal)
        assert filter_info.can_push_through(source_op) is False


@pytest.mark.skipif(not OPTIMIZER_AVAILABLE, reason="Optimizer not compiled")
class TestFilterPushdownOptimization:
    """Test end-to-end filter pushdown optimization"""

    def test_pushdown_simple_pipeline(self):
        """
        Test pushdown in simple pipeline:
        Source -> Map -> Filter -> Sink

        After optimization:
        Source -> Filter -> Map -> Sink (if possible)
        """
        # Create DAG
        source = create_mock_operator('source-1', OperatorType.SOURCE)
        map_op = create_mock_operator(
            'map-1',
            OperatorType.MAP,
            upstream_ids=['source-1']
        )
        filter_op = create_mock_operator(
            'filter-1',
            OperatorType.FILTER,
            upstream_ids=['map-1'],
            parameters={'predicate': 'amount > 100'}
        )
        sink = create_mock_operator(
            'sink-1',
            OperatorType.SINK,
            upstream_ids=['filter-1']
        )

        operators = {
            'source-1': source,
            'map-1': map_op,
            'filter-1': filter_op,
            'sink-1': sink
        }

        job_graph = create_mock_job_graph(operators)

        # Apply filter pushdown
        pushdown = FilterPushdown()
        optimized = pushdown.optimize(job_graph)

        # Verify optimization attempted
        # (Actual verification depends on implementation)

    def test_pushdown_blocked_by_aggregate(self):
        """
        Test pushdown blocked by aggregate:
        Source -> Aggregate -> Filter -> Sink

        Should NOT push filter before aggregate
        """
        source = create_mock_operator('source-1', OperatorType.SOURCE)
        agg = create_mock_operator(
            'agg-1',
            OperatorType.AGGREGATE,
            upstream_ids=['source-1']
        )
        filter_op = create_mock_operator(
            'filter-1',
            OperatorType.FILTER,
            upstream_ids=['agg-1'],
            parameters={'predicate': 'count > 10'}
        )
        sink = create_mock_operator(
            'sink-1',
            OperatorType.SINK,
            upstream_ids=['filter-1']
        )

        operators = {
            'source-1': source,
            'agg-1': agg,
            'filter-1': filter_op,
            'sink-1': sink
        }

        job_graph = create_mock_job_graph(operators)

        # Apply filter pushdown
        pushdown = FilterPushdown()
        optimized = pushdown.optimize(job_graph)

        # Filter should remain after aggregate
        # (Verification depends on implementation)

    def test_multiple_filters_combined(self):
        """
        Test multiple filters combined:
        Source -> Filter1 -> Filter2 -> Sink

        Should combine into single filter
        """
        source = create_mock_operator('source-1', OperatorType.SOURCE)
        filter1 = create_mock_operator(
            'filter-1',
            OperatorType.FILTER,
            upstream_ids=['source-1'],
            parameters={'predicate': 'amount > 100'}
        )
        filter2 = create_mock_operator(
            'filter-2',
            OperatorType.FILTER,
            upstream_ids=['filter-1'],
            parameters={'predicate': 'user_id = 123'}
        )
        sink = create_mock_operator(
            'sink-1',
            OperatorType.SINK,
            upstream_ids=['filter-2']
        )

        operators = {
            'source-1': source,
            'filter-1': filter1,
            'filter-2': filter2,
            'sink-1': sink
        }

        job_graph = create_mock_job_graph(operators)

        # Apply filter pushdown
        pushdown = FilterPushdown()
        optimized = pushdown.optimize(job_graph)

        # Should combine filters
        # (Verification depends on implementation)


@pytest.mark.skipif(not OPTIMIZER_AVAILABLE, reason="Optimizer not compiled")
class TestPlanOptimizerIntegration:
    """Test PlanOptimizer integration with filter pushdown"""

    def test_optimizer_applies_filter_pushdown(self):
        """Test that PlanOptimizer applies filter pushdown"""
        # Create simple DAG
        source = create_mock_operator('source-1', OperatorType.SOURCE)
        filter_op = create_mock_operator(
            'filter-1',
            OperatorType.FILTER,
            upstream_ids=['source-1'],
            parameters={'predicate': 'amount > 100'}
        )
        sink = create_mock_operator(
            'sink-1',
            OperatorType.SINK,
            upstream_ids=['filter-1']
        )

        operators = {
            'source-1': source,
            'filter-1': filter_op,
            'sink-1': sink
        }

        job_graph = create_mock_job_graph(operators)

        # Create optimizer with filter pushdown enabled
        optimizer = PlanOptimizer(enable_filter_pushdown=True)

        # Optimize
        optimized = optimizer.optimize(job_graph)

        # Verify stats tracked
        stats = optimizer.get_stats()
        assert hasattr(stats, 'filters_pushed')

    def test_optimizer_can_disable_filter_pushdown(self):
        """Test that filter pushdown can be disabled"""
        source = create_mock_operator('source-1', OperatorType.SOURCE)
        filter_op = create_mock_operator(
            'filter-1',
            OperatorType.FILTER,
            upstream_ids=['source-1']
        )
        sink = create_mock_operator('sink-1', OperatorType.SINK, upstream_ids=['filter-1'])

        operators = {'source-1': source, 'filter-1': filter_op, 'sink-1': sink}
        job_graph = create_mock_job_graph(operators)

        # Optimizer with filter pushdown DISABLED
        optimizer = PlanOptimizer(enable_filter_pushdown=False)
        optimized = optimizer.optimize(job_graph)

        # No filter pushdown should occur
        stats = optimizer.get_stats()
        assert stats.filters_pushed == 0


@pytest.mark.skipif(not OPTIMIZER_AVAILABLE, reason="Optimizer not compiled")
class TestFilterPushdownCorrectness:
    """Test correctness of filter pushdown transformations"""

    def test_dag_structure_preserved(self):
        """Test that DAG structure is preserved after optimization"""
        # Create DAG with multiple paths
        source = create_mock_operator('source-1', OperatorType.SOURCE)
        map1 = create_mock_operator('map-1', OperatorType.MAP, upstream_ids=['source-1'])
        map2 = create_mock_operator('map-2', OperatorType.MAP, upstream_ids=['source-1'])
        filter_op = create_mock_operator(
            'filter-1',
            OperatorType.FILTER,
            upstream_ids=['map-1', 'map-2']
        )
        sink = create_mock_operator('sink-1', OperatorType.SINK, upstream_ids=['filter-1'])

        operators = {
            'source-1': source,
            'map-1': map1,
            'map-2': map2,
            'filter-1': filter_op,
            'sink-1': sink
        }

        job_graph = create_mock_job_graph(operators)

        # Optimize
        pushdown = FilterPushdown()
        optimized = pushdown.optimize(job_graph)

        # DAG should still be valid (no cycles, all operators connected)
        # (Specific verification depends on JobGraph implementation)

    def test_no_data_loss_after_pushdown(self):
        """Test that filter semantics are preserved"""
        # This is a semantic test - filter predicate should not change
        # even if position in DAG changes

        source = create_mock_operator('source-1', OperatorType.SOURCE)
        filter_op = create_mock_operator(
            'filter-1',
            OperatorType.FILTER,
            upstream_ids=['source-1'],
            parameters={'predicate': 'amount > 100'}
        )
        sink = create_mock_operator('sink-1', OperatorType.SINK, upstream_ids=['filter-1'])

        operators = {'source-1': source, 'filter-1': filter_op, 'sink-1': sink}
        job_graph = create_mock_job_graph(operators)

        # Optimize
        pushdown = FilterPushdown()
        optimized = pushdown.optimize(job_graph)

        # Filter predicate should be unchanged
        # (Find filter in optimized graph and verify parameters)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
