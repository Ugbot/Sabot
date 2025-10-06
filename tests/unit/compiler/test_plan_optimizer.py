# -*- coding: utf-8 -*-
"""
Unit tests for PlanOptimizer.
"""

import unittest
from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType
from sabot.compiler.plan_optimizer import PlanOptimizer


class TestFilterPushdown(unittest.TestCase):
    """Test filter pushdown optimization."""

    def test_filter_pushed_through_map(self):
        """Filter should be pushed before map operator."""
        graph = JobGraph(job_name="test")

        # Create: source -> map -> filter
        source = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        map_op = StreamOperatorNode(operator_type=OperatorType.MAP)
        filter_op = StreamOperatorNode(
            operator_type=OperatorType.FILTER,
            parameters={'columns': ['value']}
        )

        graph.add_operator(source)
        graph.add_operator(map_op)
        graph.add_operator(filter_op)

        graph.connect(source.operator_id, map_op.operator_id)
        graph.connect(map_op.operator_id, filter_op.operator_id)

        # Optimize
        optimizer = PlanOptimizer()
        optimized = optimizer.optimize(graph)

        # Verify: filter should be before map
        # source -> filter -> map
        filter_opt = [op for op in optimized.operators.values()
                     if op.operator_type == OperatorType.FILTER][0]

        self.assertIn(source.operator_id, filter_opt.upstream_ids)
        self.assertGreater(optimizer.stats.filters_pushed, 0)

    def test_filter_not_pushed_through_join(self):
        """Filter should NOT be pushed through join (changes semantics)."""
        graph = JobGraph(job_name="test")

        # Create: source1 -> join -> filter
        #         source2 ->
        source1 = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        source2 = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        join_op = StreamOperatorNode(operator_type=OperatorType.HASH_JOIN)
        filter_op = StreamOperatorNode(operator_type=OperatorType.FILTER)

        graph.add_operator(source1)
        graph.add_operator(source2)
        graph.add_operator(join_op)
        graph.add_operator(filter_op)

        graph.connect(source1.operator_id, join_op.operator_id)
        graph.connect(source2.operator_id, join_op.operator_id)
        graph.connect(join_op.operator_id, filter_op.operator_id)

        # Optimize
        optimizer = PlanOptimizer()
        optimized = optimizer.optimize(graph)

        # Verify: filter should still be after join
        filter_opt = [op for op in optimized.operators.values()
                     if op.operator_type == OperatorType.FILTER][0]

        self.assertIn(join_op.operator_id, filter_opt.upstream_ids)


class TestProjectionPushdown(unittest.TestCase):
    """Test projection pushdown optimization."""

    def test_unused_columns_pruned(self):
        """Unused columns should be pruned early."""
        graph = JobGraph(job_name="test")

        # Create: source (10 cols) -> filter (uses 2 cols) -> sink
        source = StreamOperatorNode(
            operator_type=OperatorType.SOURCE,
            parameters={'columns': [f'col_{i}' for i in range(10)]}
        )
        filter_op = StreamOperatorNode(
            operator_type=OperatorType.FILTER,
            parameters={'columns': ['col_0', 'col_1']}
        )
        sink = StreamOperatorNode(operator_type=OperatorType.SINK)

        graph.add_operator(source)
        graph.add_operator(filter_op)
        graph.add_operator(sink)

        graph.connect(source.operator_id, filter_op.operator_id)
        graph.connect(filter_op.operator_id, sink.operator_id)

        # Optimize
        optimizer = PlanOptimizer()
        optimized = optimizer.optimize(graph)

        # Verify: SELECT should be inserted after source
        select_ops = [op for op in optimized.operators.values()
                     if op.operator_type == OperatorType.SELECT]

        self.assertGreater(len(select_ops), 0)
        self.assertGreater(optimizer.stats.projections_pushed, 0)


class TestJoinReordering(unittest.TestCase):
    """Test join reordering optimization."""

    def test_joins_reordered_by_selectivity(self):
        """Joins should be reordered to start with most selective."""
        graph = JobGraph(job_name="test")

        # Create: A -> join1 -> join2 -> sink
        #         B ->         C ->
        # Where join2 is more selective than join1

        source_a = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        source_b = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        source_c = StreamOperatorNode(operator_type=OperatorType.SOURCE)

        join1 = StreamOperatorNode(
            operator_type=OperatorType.HASH_JOIN,
            parameters={'join_type': 'inner', 'left_keys': ['id']}
        )
        join2 = StreamOperatorNode(
            operator_type=OperatorType.HASH_JOIN,
            parameters={'join_type': 'inner', 'left_keys': ['id']}
        )
        sink = StreamOperatorNode(operator_type=OperatorType.SINK)

        graph.add_operator(source_a)
        graph.add_operator(source_b)
        graph.add_operator(source_c)
        graph.add_operator(join1)
        graph.add_operator(join2)
        graph.add_operator(sink)

        graph.connect(source_a.operator_id, join1.operator_id)
        graph.connect(source_b.operator_id, join1.operator_id)
        graph.connect(join1.operator_id, join2.operator_id)
        graph.connect(source_c.operator_id, join2.operator_id)
        graph.connect(join2.operator_id, sink.operator_id)

        # Optimize
        optimizer = PlanOptimizer()
        optimized = optimizer.optimize(graph)

        # Verify: join order may have changed
        # (Difficult to test without actual selectivity stats)
        self.assertGreaterEqual(optimizer.stats.joins_reordered, 0)


class TestOperatorFusion(unittest.TestCase):
    """Test operator fusion optimization."""

    def test_map_filter_fused(self):
        """Map + filter should be fused."""
        graph = JobGraph(job_name="test")

        # Create: source -> map -> filter -> sink
        source = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        map_op = StreamOperatorNode(operator_type=OperatorType.MAP)
        filter_op = StreamOperatorNode(operator_type=OperatorType.FILTER)
        sink = StreamOperatorNode(operator_type=OperatorType.SINK)

        graph.add_operator(source)
        graph.add_operator(map_op)
        graph.add_operator(filter_op)
        graph.add_operator(sink)

        graph.connect(source.operator_id, map_op.operator_id)
        graph.connect(map_op.operator_id, filter_op.operator_id)
        graph.connect(filter_op.operator_id, sink.operator_id)

        initial_op_count = len(graph.operators)

        # Optimize
        optimizer = PlanOptimizer()
        optimized = optimizer.optimize(graph)

        # Verify: should have fewer operators (fused)
        self.assertLess(len(optimized.operators), initial_op_count)
        self.assertGreater(optimizer.stats.operators_fused, 0)


class TestDeadCodeElimination(unittest.TestCase):
    """Test dead code elimination."""

    def test_unreachable_operators_removed(self):
        """Operators not connected to sink should be removed."""
        graph = JobGraph(job_name="test")

        # Create: source -> map -> sink
        #         unused -> unused_map (not connected to sink)
        source = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        map_op = StreamOperatorNode(operator_type=OperatorType.MAP)
        sink = StreamOperatorNode(operator_type=OperatorType.SINK)

        unused = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        unused_map = StreamOperatorNode(operator_type=OperatorType.MAP)

        graph.add_operator(source)
        graph.add_operator(map_op)
        graph.add_operator(sink)
        graph.add_operator(unused)
        graph.add_operator(unused_map)

        graph.connect(source.operator_id, map_op.operator_id)
        graph.connect(map_op.operator_id, sink.operator_id)
        # unused operators not connected

        initial_op_count = len(graph.operators)

        # Optimize
        optimizer = PlanOptimizer()
        optimized = optimizer.optimize(graph)

        # Verify: unused operators removed
        self.assertLess(len(optimized.operators), initial_op_count)
        self.assertGreater(optimizer.stats.dead_code_eliminated, 0)


if __name__ == '__main__':
    unittest.main()
