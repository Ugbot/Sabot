# -*- coding: utf-8 -*-
"""
Join Reordering Optimization

Reorders join operators based on selectivity estimates to minimize
intermediate result sizes.

Inspired by DuckDB's JoinOrderOptimizer.
"""

import logging
from typing import List, Dict, Set, Tuple, Optional
from dataclasses import dataclass

from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType

logger = logging.getLogger(__name__)


@dataclass
class JoinNode:
    """Represents a join in the join graph."""

    operator: StreamOperatorNode
    left_id: str
    right_id: str
    selectivity: float

    @property
    def cost(self) -> float:
        """Join cost estimate (lower is better)."""
        return 1.0 - self.selectivity  # More selective = lower cost


class JoinReordering:
    """
    Join reordering optimization.

    Reorders joins to start with most selective joins first,
    reducing intermediate result sizes.

    Limitations:
    - Only inner joins (outer joins have fixed order)
    - No cross products
    - Conservative estimates (no statistics yet)
    """

    def __init__(self):
        self.joins_reordered_count = 0

    def optimize(self, job_graph: JobGraph) -> JobGraph:
        """Apply join reordering to job graph."""

        # Extract join subgraphs
        join_chains = self._find_join_chains(job_graph)

        if not join_chains:
            logger.debug("No join chains to optimize")
            return job_graph

        logger.info(f"Found {len(join_chains)} join chains to optimize")

        # Optimize each join chain independently
        for chain in join_chains:
            self._optimize_join_chain(job_graph, chain)

        return job_graph

    def _find_join_chains(self, job_graph: JobGraph) -> List[List[JoinNode]]:
        """
        Find chains of joins in the DAG.

        A join chain is a sequence of join operators where each join
        consumes the output of the previous join.
        """
        join_chains = []
        visited = set()

        # Find all join operators
        join_ops = [op for op in job_graph.operators.values()
                   if op.operator_type == OperatorType.HASH_JOIN]

        for join_op in join_ops:
            if join_op.operator_id in visited:
                continue

            # Try to build a chain starting from this join
            chain = self._build_join_chain(job_graph, join_op, visited)

            if len(chain) > 1:  # Only optimize chains of 2+ joins
                join_chains.append(chain)

        return join_chains

    def _build_join_chain(self,
                         job_graph: JobGraph,
                         start_join: StreamOperatorNode,
                         visited: Set[str]) -> List[JoinNode]:
        """
        Build a join chain starting from the given join operator.

        Walk upstream and downstream to find connected joins.
        """
        chain = []

        # Create JoinNode for current join
        join_node = self._create_join_node(start_join)
        chain.append(join_node)
        visited.add(start_join.operator_id)

        # Walk downstream to find chained joins
        current = start_join
        while current.downstream_ids:
            # Check if downstream is a join
            downstream_joins = [
                job_graph.operators[down_id]
                for down_id in current.downstream_ids
                if job_graph.operators[down_id].operator_type == OperatorType.HASH_JOIN
            ]

            if not downstream_joins:
                break

            # Take first join (simplified - could have multiple branches)
            next_join = downstream_joins[0]
            if next_join.operator_id in visited:
                break

            join_node = self._create_join_node(next_join)
            chain.append(join_node)
            visited.add(next_join.operator_id)
            current = next_join

        return chain

    def _create_join_node(self, join_op: StreamOperatorNode) -> JoinNode:
        """Create JoinNode with selectivity estimate."""

        # Get left and right inputs
        left_id = join_op.upstream_ids[0] if join_op.upstream_ids else None
        right_id = join_op.upstream_ids[1] if len(join_op.upstream_ids) > 1 else None

        # Estimate selectivity
        selectivity = self._estimate_selectivity(join_op)

        return JoinNode(
            operator=join_op,
            left_id=left_id,
            right_id=right_id,
            selectivity=selectivity
        )

    def _estimate_selectivity(self, join_op: StreamOperatorNode) -> float:
        """
        Estimate join selectivity (fraction of rows that pass).

        Simple heuristic-based estimation:
        - Equality on key columns: high selectivity (0.1)
        - Equality on non-key: medium selectivity (0.5)
        - Range predicates: low selectivity (0.8)

        Future: use statistics from data sources.
        """
        join_type = join_op.parameters.get('join_type', 'inner')

        # Simplified: use fixed estimates
        if join_type == 'inner':
            # Check if joining on keys
            left_keys = join_op.parameters.get('left_keys', [])
            right_keys = join_op.parameters.get('right_keys', [])

            # Heuristic: single column join on 'id' is likely a key
            if (len(left_keys) == 1 and 'id' in left_keys[0].lower()):
                return 0.1  # High selectivity

            return 0.5  # Medium selectivity

        # Outer joins are less selective
        return 0.8

    def _optimize_join_chain(self,
                            job_graph: JobGraph,
                            chain: List[JoinNode]):
        """
        Optimize a chain of joins by reordering.

        Strategy (greedy):
        1. Sort joins by selectivity (most selective first)
        2. Rewrite DAG to match new order

        Constraint: preserve correctness (only reorder independent joins)
        """
        if len(chain) < 2:
            return

        logger.debug(f"Optimizing join chain of length {len(chain)}")

        # Sort by cost (most selective first)
        original_order = [j.operator.operator_id for j in chain]
        sorted_chain = sorted(chain, key=lambda j: j.cost)
        new_order = [j.operator.operator_id for j in sorted_chain]

        if original_order == new_order:
            logger.debug("Join chain already optimal")
            return

        # Check if reordering is valid (simplified - would need dependency analysis)
        if not self._can_reorder(chain, sorted_chain):
            logger.debug("Cannot reorder join chain (dependencies prevent it)")
            return

        # Rewrite DAG to match new order
        self._rewrite_join_chain(job_graph, chain, sorted_chain)
        self.joins_reordered_count += len(chain)

        logger.info(f"Reordered join chain: {original_order} -> {new_order}")

    def _can_reorder(self,
                    original: List[JoinNode],
                    reordered: List[JoinNode]) -> bool:
        """
        Check if join reordering preserves correctness.

        Simplified: only allow reordering if all joins are inner joins
        and there are no cross-dependencies.

        Real implementation would check:
        - Join type (inner vs outer)
        - Column dependencies
        - Foreign key constraints
        """
        # Only inner joins can be freely reordered
        for join_node in original:
            join_type = join_node.operator.parameters.get('join_type', 'inner')
            if join_type != 'inner':
                return False

        return True

    def _rewrite_join_chain(self,
                           job_graph: JobGraph,
                           original_chain: List[JoinNode],
                           new_chain: List[JoinNode]):
        """
        Rewrite DAG to reflect new join order.

        This is complex - simplified implementation.
        Real version would need full DAG rewriting logic.
        """
        # Placeholder: in real implementation, would rewire upstream/downstream
        # connections to reflect new join order

        logger.warning("Join chain rewriting not fully implemented yet")

        # TODO: Implement full DAG rewriting
        # 1. Identify inputs to join chain
        # 2. Reorder internal join operators
        # 3. Rewire connections
        # 4. Update operator_ids in graph
