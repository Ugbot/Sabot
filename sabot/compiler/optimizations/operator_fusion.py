# -*- coding: utf-8 -*-
"""
Operator Fusion Optimization

Combines chains of compatible operators to reduce overhead.
Generates fused operators for better performance.
"""

import logging
from typing import List, Optional, Callable, Set
from dataclasses import dataclass

from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType

logger = logging.getLogger(__name__)


@dataclass
class FusionChain:
    """Represents a chain of operators that can be fused."""

    operators: List[StreamOperatorNode]

    def can_fuse(self) -> bool:
        """Check if this chain can be fused."""
        if len(self.operators) < 2:
            return False

        # All operators must be stateless
        for op in self.operators:
            if op.stateful:
                return False

        # All operators must be compatible types
        fusable_types = {
            OperatorType.MAP,
            OperatorType.FILTER,
            OperatorType.SELECT,
            OperatorType.FLAT_MAP
        }

        for op in self.operators:
            if op.operator_type not in fusable_types:
                return False

        return True

    def create_fused_operator(self) -> StreamOperatorNode:
        """Create a single fused operator from the chain."""
        import uuid

        # Create fused operator
        fused_op = StreamOperatorNode(
            operator_id=str(uuid.uuid4()),
            operator_type=OperatorType.MAP,  # Fused operator is a map
            name=f"fused_{'_'.join(op.name for op in self.operators)}",
            parameters={
                'fused_functions': [op.function for op in self.operators],
                'fused_types': [op.operator_type.value for op in self.operators],
            },
            upstream_ids=self.operators[0].upstream_ids.copy(),
            downstream_ids=self.operators[-1].downstream_ids.copy()
        )

        return fused_op


class OperatorFusion:
    """
    Operator fusion optimization.

    Identifies chains of compatible operators and fuses them
    into single operators to reduce overhead.
    """

    def __init__(self):
        self.operators_fused_count = 0

    def optimize(self, job_graph: JobGraph) -> JobGraph:
        """Apply operator fusion to job graph."""

        # Find fusion chains
        chains = self._find_fusion_chains(job_graph)

        if not chains:
            logger.debug("No fusion chains found")
            return job_graph

        logger.info(f"Found {len(chains)} fusion chains")

        # Fuse each chain
        for chain in chains:
            if chain.can_fuse():
                self._fuse_chain(job_graph, chain)

        logger.info(f"Fused {self.operators_fused_count} operators")

        return job_graph

    def _find_fusion_chains(self, job_graph: JobGraph) -> List[FusionChain]:
        """
        Find chains of operators that can be fused.

        Walk DAG and identify linear sequences of fusable operators.
        """
        chains = []
        visited = set()

        # Topological sort to process in order
        sorted_ops = list(job_graph.operators.values())
        # Simple sort by operator ID (not perfect but works for now)

        for op in sorted_ops:
            if op.operator_id in visited:
                continue

            # Try to build a fusion chain starting from this operator
            chain = self._build_fusion_chain(job_graph, op, visited)

            if len(chain) >= 2:
                chains.append(FusionChain(operators=chain))

        return chains

    def _build_fusion_chain(self,
                           job_graph: JobGraph,
                           start_op: StreamOperatorNode,
                           visited: Set[str]) -> List[StreamOperatorNode]:
        """
        Build a fusion chain starting from the given operator.

        Walk downstream as long as operators are fusable.
        """
        chain = []
        current = start_op

        # Fusable operator types
        fusable_types = {
            OperatorType.MAP,
            OperatorType.FILTER,
            OperatorType.SELECT,
            OperatorType.FLAT_MAP
        }

        while current:
            # Check if current operator is fusable
            if current.operator_type not in fusable_types or current.stateful:
                break

            # Check if current has single downstream (linear chain)
            if len(current.downstream_ids) != 1:
                # Add current and stop
                if current.operator_id not in visited:
                    chain.append(current)
                    visited.add(current.operator_id)
                break

            # Add to chain
            if current.operator_id not in visited:
                chain.append(current)
                visited.add(current.operator_id)

            # Move to next operator
            next_id = current.downstream_ids[0]
            current = job_graph.operators.get(next_id)

        return chain

    def _fuse_chain(self, job_graph: JobGraph, chain: FusionChain):
        """
        Fuse a chain of operators into a single operator.

        Removes individual operators and replaces with fused operator.
        """
        # Create fused operator
        fused_op = chain.create_fused_operator()

        # Remove individual operators from graph
        for op in chain.operators:
            del job_graph.operators[op.operator_id]
            self.operators_fused_count += 1

        # Add fused operator
        job_graph.operators[fused_op.operator_id] = fused_op

        # Rewire connections
        # Update upstream operators to point to fused op
        for upstream_id in fused_op.upstream_ids:
            upstream = job_graph.operators[upstream_id]

            # Replace first operator in chain with fused op
            first_op_id = chain.operators[0].operator_id
            if first_op_id in upstream.downstream_ids:
                idx = upstream.downstream_ids.index(first_op_id)
                upstream.downstream_ids[idx] = fused_op.operator_id

        # Update downstream operators to point to fused op
        for downstream_id in fused_op.downstream_ids:
            downstream = job_graph.operators[downstream_id]

            # Replace last operator in chain with fused op
            last_op_id = chain.operators[-1].operator_id
            if last_op_id in downstream.upstream_ids:
                idx = downstream.upstream_ids.index(last_op_id)
                downstream.upstream_ids[idx] = fused_op.operator_id

        logger.debug(
            f"Fused {len(chain.operators)} operators into {fused_op.name}"
        )
