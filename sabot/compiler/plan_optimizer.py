# -*- coding: utf-8 -*-
"""
Plan Optimizer - Query Optimization for Streaming Dataflow DAGs

Inspired by DuckDB's optimizer architecture but adapted for streaming semantics.
Applies rule-based optimizations to JobGraph before physical deployment.

Key Optimizations:
1. Filter Pushdown - Move filters closer to sources
2. Projection Pushdown - Select only required columns early
3. Join Reordering - Reorder joins based on selectivity estimates
4. Operator Fusion - Combine compatible operators to reduce overhead

Design Principles:
- Rule-based (not cost-based) for simplicity
- Streaming-aware (preserve ordering, watermarks)
- Conservative (correctness over aggressive optimization)
- Measurable (each optimization tracked and benchmarked)
"""

import logging
from typing import List, Dict, Set, Optional
from dataclasses import dataclass
from copy import deepcopy

logger = logging.getLogger(__name__)


@dataclass
class OptimizationStats:
    """Track optimization statistics for debugging and benchmarking."""

    filters_pushed: int = 0
    projections_pushed: int = 0
    joins_reordered: int = 0
    operators_fused: int = 0
    dead_code_eliminated: int = 0

    def total_optimizations(self) -> int:
        return (self.filters_pushed + self.projections_pushed +
                self.joins_reordered + self.operators_fused +
                self.dead_code_eliminated)


class PlanOptimizer:
    """
    Rule-based optimizer for streaming dataflow DAGs.

    Applies optimizations to JobGraph before physical execution:
    1. Filter pushdown (move filters before joins/aggregations)
    2. Projection pushdown (select columns early)
    3. Join reordering (based on selectivity)
    4. Operator fusion (chain compatible ops)

    Usage:
        optimizer = PlanOptimizer()
        optimized_graph = optimizer.optimize(job_graph)
    """

    def __init__(self,
                 enable_filter_pushdown: bool = True,
                 enable_projection_pushdown: bool = True,
                 enable_join_reordering: bool = True,
                 enable_operator_fusion: bool = True,
                 enable_dead_code_elimination: bool = True):
        """
        Initialize optimizer with configuration.

        Args:
            enable_filter_pushdown: Enable filter pushdown optimization
            enable_projection_pushdown: Enable projection pushdown
            enable_join_reordering: Enable join reordering
            enable_operator_fusion: Enable operator fusion
            enable_dead_code_elimination: Enable dead code elimination
        """
        self.enable_filter_pushdown = enable_filter_pushdown
        self.enable_projection_pushdown = enable_projection_pushdown
        self.enable_join_reordering = enable_join_reordering
        self.enable_operator_fusion = enable_operator_fusion
        self.enable_dead_code_elimination = enable_dead_code_elimination

        self.stats = OptimizationStats()

    def optimize(self, job_graph) -> 'JobGraph':
        """
        Apply all enabled optimizations to job graph.

        Args:
            job_graph: Input JobGraph (logical DAG)

        Returns:
            Optimized JobGraph
        """
        # Work on a copy to preserve original
        optimized = deepcopy(job_graph)

        logger.info(f"Starting optimization of JobGraph: {job_graph.job_name}")
        initial_op_count = len(optimized.operators)

        # Apply optimizations in order
        # Order matters: pushdown before reordering before fusion

        if self.enable_filter_pushdown:
            optimized = self._apply_filter_pushdown(optimized)

        if self.enable_projection_pushdown:
            optimized = self._apply_projection_pushdown(optimized)

        if self.enable_join_reordering:
            optimized = self._apply_join_reordering(optimized)

        if self.enable_operator_fusion:
            optimized = self._apply_operator_fusion(optimized)

        if self.enable_dead_code_elimination:
            optimized = self._apply_dead_code_elimination(optimized)

        final_op_count = len(optimized.operators)

        logger.info(
            f"Optimization complete: {initial_op_count} -> {final_op_count} operators, "
            f"{self.stats.total_optimizations()} optimizations applied"
        )
        logger.debug(f"Optimization stats: {self.stats}")

        return optimized

    def _apply_filter_pushdown(self, job_graph) -> 'JobGraph':
        """
        Push filter operators closer to sources.

        Strategy:
        - Filters can be pushed through stateless operators (map, select)
        - Filters should be pushed before stateful operators (join, aggregate)
        - Filters cannot be pushed through operators that change schema

        Inspired by DuckDB's FilterPushdown class.
        """
        logger.debug("Applying filter pushdown optimization")

        from .optimizations.filter_pushdown import FilterPushdown
        pushdown = FilterPushdown()
        optimized = pushdown.optimize(job_graph)
        self.stats.filters_pushed = pushdown.filters_pushed_count

        return optimized

    def _apply_projection_pushdown(self, job_graph) -> 'JobGraph':
        """
        Push column selection (projection) closer to sources.

        Strategy:
        - Analyze which columns are actually used downstream
        - Insert SELECT operators early to prune unused columns
        - Reduce data transfer and memory footprint

        Inspired by DuckDB's RemoveUnusedColumns.
        """
        logger.debug("Applying projection pushdown optimization")

        from .optimizations.projection_pushdown import ProjectionPushdown
        pushdown = ProjectionPushdown()
        optimized = pushdown.optimize(job_graph)
        self.stats.projections_pushed = pushdown.projections_pushed_count

        return optimized

    def _apply_join_reordering(self, job_graph) -> 'JobGraph':
        """
        Reorder joins based on selectivity estimates.

        Strategy:
        - Build join graph from JobGraph
        - Estimate selectivity of each join (cardinality estimation)
        - Reorder to start with most selective joins
        - Preserve semantic correctness (only inner joins)

        Inspired by DuckDB's JoinOrderOptimizer.
        """
        logger.debug("Applying join reordering optimization")

        from .optimizations.join_reordering import JoinReordering
        reorderer = JoinReordering()
        optimized = reorderer.optimize(job_graph)
        self.stats.joins_reordered = reorderer.joins_reordered_count

        return optimized

    def _apply_operator_fusion(self, job_graph) -> 'JobGraph':
        """
        Fuse compatible operators to reduce overhead.

        Strategy:
        - Chain stateless operators (map -> map -> filter -> select)
        - Eliminate intermediate materializations
        - Generate fused Cython operator for better codegen

        Example: map().filter().map() -> fused_transform()

        Inspired by operator pipelining in modern query engines.
        """
        logger.debug("Applying operator fusion optimization")

        from .optimizations.operator_fusion import OperatorFusion
        fusion = OperatorFusion()
        optimized = fusion.optimize(job_graph)
        self.stats.operators_fused = fusion.operators_fused_count

        return optimized

    def _apply_dead_code_elimination(self, job_graph) -> 'JobGraph':
        """
        Remove operators that don't contribute to output.

        Strategy:
        - Start from sinks, walk backwards
        - Mark reachable operators
        - Remove unreachable operators
        """
        logger.debug("Applying dead code elimination")

        # Basic dead code elimination implementation
        reachable = set()

        # Find sinks using JobGraph method
        sinks = job_graph.get_sinks()

        logger.debug(f"Found {len(sinks)} sink operators")

        # BFS from sinks
        queue = sinks[:]
        while queue:
            current_op = queue.pop(0)
            if current_op.operator_id in reachable:
                continue

            reachable.add(current_op.operator_id)
            logger.debug(f"Marked reachable: {current_op.name}")

            # Add upstream operators to queue
            for upstream_id in current_op.upstream_ids:
                if upstream_id in job_graph.operators:
                    upstream_op = job_graph.operators[upstream_id]
                    if upstream_op not in queue:
                        queue.append(upstream_op)

        # Remove unreachable operators
        all_ops = set(job_graph.operators.keys())
        unreachable = all_ops - reachable

        logger.debug(f"Total operators: {len(all_ops)}, reachable: {len(reachable)}, unreachable: {len(unreachable)}")

        for op_id in unreachable:
            del job_graph.operators[op_id]
            self.stats.dead_code_eliminated += 1

        if unreachable:
            logger.info(f"Removed {len(unreachable)} unreachable operators")

        return job_graph

    def get_stats(self) -> OptimizationStats:
        """Get optimization statistics."""
        return self.stats