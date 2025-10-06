# -*- coding: utf-8 -*-
"""
Projection Pushdown Optimization

Analyzes which columns are actually used and inserts projections
to prune unused columns early, reducing memory and network transfer.

Inspired by DuckDB's RemoveUnusedColumns.
"""

import logging
from typing import Dict, Set, List
from dataclasses import dataclass

from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType

logger = logging.getLogger(__name__)


class ProjectionPushdown:
    """
    Projection pushdown optimization.

    Analyzes column usage and inserts SELECT operators to prune
    unused columns early, reducing memory and network transfer.
    """

    def __init__(self):
        self.projections_pushed_count = 0

    def optimize(self, job_graph: JobGraph) -> JobGraph:
        """Apply projection pushdown to job graph."""

        # Analyze column usage (backward pass)
        column_usage = self._analyze_column_usage(job_graph)

        # Insert projections where beneficial (forward pass)
        self._insert_projections(job_graph, column_usage)

        logger.info(f"Inserted {self.projections_pushed_count} projection operators")

        return job_graph

    def _analyze_column_usage(self,
                             job_graph: JobGraph) -> Dict[str, Set[str]]:
        """
        Analyze which columns each operator requires.

        Returns:
            Dict mapping operator_id -> required column names
        """
        column_usage: Dict[str, Set[str]] = {}

        # Topological sort (reverse order)
        sorted_ops = list(job_graph.operators.values())
        # Simple reverse sort by operator ID (not perfect but works for now)
        sorted_ops.reverse()

        for op in sorted_ops:
            required = set()

            # If sink, assume all columns needed
            if self._is_sink(op, job_graph):
                # Get all columns from upstream
                # Simplified: would need schema info
                required = set(op.parameters.get('output_columns', ['*']))

            # Collect requirements from downstream operators
            for downstream_id in op.downstream_ids:
                downstream = job_graph.operators[downstream_id]
                downstream_required = column_usage.get(downstream_id, set())

                # Add columns needed by downstream
                required.update(downstream_required)

            # Add columns needed by this operator itself
            op_columns = self._get_operator_column_usage(op)
            required.update(op_columns)

            column_usage[op.operator_id] = required

        return column_usage

    def _is_sink(self, op: StreamOperatorNode, job_graph: JobGraph) -> bool:
        """Check if operator is a sink (no downstream connections)."""
        return not op.downstream_ids

    def _get_operator_column_usage(self,
                                   op: StreamOperatorNode) -> Set[str]:
        """
        Determine which columns this operator uses.

        Simplified implementation - would need expression analysis.
        """
        if op.operator_type == OperatorType.FILTER:
            # Filters use specific columns
            return set(op.parameters.get('columns', []))

        if op.operator_type == OperatorType.SELECT:
            # Projections define columns
            return set(op.parameters.get('columns', []))

        if op.operator_type == OperatorType.HASH_JOIN:
            # Joins use key columns
            left_keys = op.parameters.get('left_keys', [])
            right_keys = op.parameters.get('right_keys', [])
            return set(left_keys + right_keys)

        if op.operator_type == OperatorType.AGGREGATE:
            # Aggregations use group-by and agg columns
            group_by = op.parameters.get('group_by', [])
            agg_cols = op.parameters.get('aggregations', {}).keys()
            return set(group_by) | set(agg_cols)

        # Conservative: assume all columns needed
        return set(['*'])

    def _insert_projections(self,
                           job_graph: JobGraph,
                           column_usage: Dict[str, Set[str]]):
        """
        Insert SELECT operators to prune unused columns.

        Strategy:
        - After each operator, check if it produces more columns than needed
        - If yes, insert SELECT to prune
        """
        # Work on copy of operator list (we'll be adding operators)
        operators_to_check = list(job_graph.operators.values())

        for op in operators_to_check:
            # Skip if already a projection
            if op.operator_type == OperatorType.SELECT:
                continue

            # Get columns this operator produces
            produced_cols = self._get_produced_columns(op)
            if '*' in produced_cols:
                # Don't optimize wildcard (need schema info)
                continue

            # Get columns downstream actually needs
            required_cols = set()
            for downstream_id in op.downstream_ids:
                required_cols.update(column_usage.get(downstream_id, set()))

            # If producing more than needed, insert projection
            unused_cols = produced_cols - required_cols
            if unused_cols:
                self._insert_select_operator(job_graph, op, required_cols)
                self.projections_pushed_count += 1

                logger.debug(
                    f"Inserted projection after {op.name}, "
                    f"pruning {len(unused_cols)} columns"
                )

    def _get_produced_columns(self, op: StreamOperatorNode) -> Set[str]:
        """Get columns produced by this operator."""
        # Simplified: would need schema tracking
        if op.operator_type == OperatorType.SELECT:
            return set(op.parameters.get('columns', []))

        # Most operators preserve columns
        return set(['*'])  # Wildcard means "all columns"

    def _insert_select_operator(self,
                               job_graph: JobGraph,
                               after_op: StreamOperatorNode,
                               columns: Set[str]):
        """Insert a SELECT operator after the given operator."""
        import uuid

        # Create new SELECT operator
        select_op = StreamOperatorNode(
            operator_id=str(uuid.uuid4()),
            operator_type=OperatorType.SELECT,
            name=f"select_{after_op.name}",
            parameters={'columns': list(columns)},
            upstream_ids=[after_op.operator_id],
            downstream_ids=after_op.downstream_ids.copy()
        )

        # Rewire DAG
        # Before: op -> downstream
        # After:  op -> select -> downstream

        for downstream_id in after_op.downstream_ids:
            downstream = job_graph.operators[downstream_id]

            # Replace op with select in downstream's upstreams
            if after_op.operator_id in downstream.upstream_ids:
                idx = downstream.upstream_ids.index(after_op.operator_id)
                downstream.upstream_ids[idx] = select_op.operator_id

        # Update op's downstreams
        after_op.downstream_ids = [select_op.operator_id]

        # Add select to graph
        job_graph.operators[select_op.operator_id] = select_op
