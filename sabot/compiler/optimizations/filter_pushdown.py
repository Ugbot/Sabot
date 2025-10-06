# -*- coding: utf-8 -*-
"""
Filter Pushdown Optimization

Moves filter operators closer to data sources to reduce data volume early.
Inspired by DuckDB's filter_pushdown.cpp but adapted for streaming.
"""

import logging
from typing import Dict, List, Set, Optional
from dataclasses import dataclass

from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType

logger = logging.getLogger(__name__)


@dataclass
class FilterInfo:
    """Information about a filter predicate."""

    filter_op: StreamOperatorNode
    referenced_columns: Set[str]  # Columns used in filter expression

    def can_push_through(self, operator: StreamOperatorNode) -> bool:
        """
        Determine if filter can be pushed through this operator.

        Rules:
        - Can push through stateless ops if columns preserved
        - Cannot push through stateful ops (changes semantics)
        - Cannot push through schema-changing ops without referenced columns
        """
        if operator.operator_type == OperatorType.SOURCE:
            # Can't push before source
            return False

        if operator.operator_type in [OperatorType.MAP, OperatorType.FLAT_MAP]:
            # Can push through map if it doesn't modify filter columns
            # Conservative: assume we can push (would need UDF analysis)
            return True

        if operator.operator_type == OperatorType.SELECT:
            # Can push through projection if filter columns are selected
            selected_cols = set(operator.parameters.get('columns', []))
            return self.referenced_columns.issubset(selected_cols)

        if operator.operator_type == OperatorType.FILTER:
            # Can always combine filters
            return True

        if operator.operator_type == OperatorType.HASH_JOIN:
            # Can push to appropriate join side (complex - simplified here)
            # For now, don't push through joins
            return False

        if operator.operator_type in [OperatorType.AGGREGATE, OperatorType.GROUP_BY,
                                       OperatorType.TUMBLING_WINDOW, OperatorType.SLIDING_WINDOW]:
            # Cannot push through stateful operators (changes semantics)
            return False

        if operator.operator_type == OperatorType.UNION:
            # Can push into both branches
            return True

        # Conservative default
        return False


class FilterPushdown:
    """
    Filter pushdown optimization.

    Identifies filter operators and pushes them closer to sources
    to reduce data volume early in the pipeline.
    """

    def __init__(self):
        self.filters_pushed_count = 0

    def optimize(self, job_graph: JobGraph) -> JobGraph:
        """
        Apply filter pushdown to job graph.

        Args:
            job_graph: Input JobGraph

        Returns:
            Optimized JobGraph with filters pushed down
        """
        # Collect all filter operators
        filters = self._collect_filters(job_graph)

        if not filters:
            logger.debug("No filters to push down")
            return job_graph

        logger.info(f"Found {len(filters)} filter operators to optimize")

        # For each filter, try to push it down
        for filter_info in filters:
            self._push_filter_down(job_graph, filter_info)

        return job_graph

    def _collect_filters(self, job_graph: JobGraph) -> List[FilterInfo]:
        """Collect all filter operators from job graph."""
        filters = []

        for op in job_graph.operators.values():
            if op.operator_type == OperatorType.FILTER:
                # Extract referenced columns from filter expression
                # Simplified: would need actual expression analysis
                referenced_cols = self._extract_columns_from_filter(op)

                filters.append(FilterInfo(
                    filter_op=op,
                    referenced_columns=referenced_cols
                ))

        return filters

    def _extract_columns_from_filter(self, filter_op: StreamOperatorNode) -> Set[str]:
        """
        Extract column names referenced in filter expression.

        Simplified implementation - would need full expression analysis.
        """
        # Placeholder: extract from function or parameters
        # In real implementation, parse filter expression AST
        columns = filter_op.parameters.get('columns', [])
        return set(columns) if columns else set()

    def _push_filter_down(self, job_graph: JobGraph, filter_info: FilterInfo):
        """
        Push a single filter down through the DAG.

        Strategy:
        1. Start at filter operator
        2. Walk upstream
        3. If can push through, continue
        4. Otherwise, stop and reposition filter
        """
        current_op = filter_info.filter_op

        # Find upstream operators
        if not current_op.upstream_ids:
            # Already at source
            return

        # Simple case: single upstream (linear pipeline)
        if len(current_op.upstream_ids) == 1:
            upstream_id = current_op.upstream_ids[0]
            upstream_op = job_graph.operators[upstream_id]

            # Can we push through this operator?
            if filter_info.can_push_through(upstream_op):
                # Reposition filter before upstream operator
                self._reposition_filter(job_graph, current_op, upstream_op)
                self.filters_pushed_count += 1

                logger.debug(
                    f"Pushed filter {current_op.name} through {upstream_op.name}"
                )

                # Recursively try to push further
                filter_info.filter_op = current_op  # Update reference
                self._push_filter_down(job_graph, filter_info)

    def _reposition_filter(self,
                          job_graph: JobGraph,
                          filter_op: StreamOperatorNode,
                          target_op: StreamOperatorNode):
        """
        Reposition filter operator before target operator.

        Rewires DAG edges:
        Before: ... -> target -> filter -> downstream
        After:  ... -> filter -> target -> downstream
        """
        # Get target's upstream operators
        target_upstreams = target_op.upstream_ids.copy()

        # Remove filter from target's downstream
        if filter_op.operator_id in target_op.downstream_ids:
            target_op.downstream_ids.remove(filter_op.operator_id)

        # Remove target from filter's upstream
        if target_op.operator_id in filter_op.upstream_ids:
            filter_op.upstream_ids.remove(target_op.operator_id)

        # Rewire: filter -> target
        filter_op.downstream_ids = [target_op.operator_id]
        target_op.upstream_ids = [filter_op.operator_id]

        # Connect filter to target's original upstreams
        filter_op.upstream_ids = target_upstreams
        for upstream_id in target_upstreams:
            upstream_op = job_graph.operators[upstream_id]

            # Replace target with filter in upstream's downstreams
            if target_op.operator_id in upstream_op.downstream_ids:
                idx = upstream_op.downstream_ids.index(target_op.operator_id)
                upstream_op.downstream_ids[idx] = filter_op.operator_id
