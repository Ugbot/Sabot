#!/usr/bin/env python3
"""
Shuffle Execution Coordinator

Integrates shuffle operations with DBOS-controlled parallelism and DAG execution.
Manages shuffle lifecycle across distributed agents.
"""

import logging
import asyncio
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
import time

from .execution_graph import ExecutionGraph, ShuffleEdge, ShuffleEdgeType, TaskVertex

logger = logging.getLogger(__name__)


@dataclass
class ShuffleExecutionPlan:
    """
    Execution plan for a shuffle operation.

    Maps shuffle edge to concrete task assignments and agent locations.
    """

    shuffle_id: str
    edge_type: ShuffleEdgeType
    partition_keys: Optional[List[str]]

    # Upstream (write side)
    upstream_task_ids: List[str] = field(default_factory=list)
    upstream_agent_map: Dict[str, str] = field(default_factory=dict)  # task_id -> agent_address

    # Downstream (read side)
    downstream_task_ids: List[str] = field(default_factory=list)
    downstream_agent_map: Dict[str, str] = field(default_factory=dict)  # task_id -> agent_address

    # Partitioning
    num_partitions: int = 1

    # State
    initiated_at: Optional[float] = None
    completed_at: Optional[float] = None
    rows_shuffled: int = 0
    bytes_shuffled: int = 0


class ShuffleCoordinator:
    """
    Coordinates shuffle execution across distributed agents.

    Integrates with:
    - DBOS parallel controller for work distribution
    - Agent runtime for task deployment
    - Shuffle manager (Cython) for actual data transfer
    """

    def __init__(self, execution_graph: ExecutionGraph):
        """
        Initialize shuffle coordinator.

        Args:
            execution_graph: Physical execution graph with tasks and shuffles
        """
        self.execution_graph = execution_graph
        self.shuffle_plans: Dict[str, ShuffleExecutionPlan] = {}
        self._initialized = False

    async def initialize(self, agent_assignments: Dict[str, str]) -> None:
        """
        Initialize shuffle coordinator with agent assignments.

        Args:
            agent_assignments: task_id -> agent_address mapping

        This method must be called after tasks are assigned to agents.
        """
        logger.info("Initializing shuffle coordinator...")

        # Create execution plans for each shuffle edge
        for shuffle_id, shuffle_edge in self.execution_graph.shuffle_edges.items():
            plan = self._create_shuffle_plan(shuffle_edge, agent_assignments)
            self.shuffle_plans[shuffle_id] = plan

            logger.info(
                f"Created shuffle plan {shuffle_id}: "
                f"{len(plan.upstream_task_ids)} upstream -> {len(plan.downstream_task_ids)} downstream "
                f"(type: {plan.edge_type.value})"
            )

        self._initialized = True
        logger.info(f"Shuffle coordinator initialized ({len(self.shuffle_plans)} shuffles)")

    def _create_shuffle_plan(
        self,
        shuffle_edge: ShuffleEdge,
        agent_assignments: Dict[str, str]
    ) -> ShuffleExecutionPlan:
        """
        Create execution plan for a shuffle edge.

        Args:
            shuffle_edge: Shuffle edge metadata
            agent_assignments: task_id -> agent_address mapping

        Returns:
            ShuffleExecutionPlan
        """
        plan = ShuffleExecutionPlan(
            shuffle_id=shuffle_edge.shuffle_id,
            edge_type=shuffle_edge.edge_type,
            partition_keys=shuffle_edge.partition_keys,
            num_partitions=shuffle_edge.num_partitions,
        )

        # Map upstream tasks to agents
        for task_id in shuffle_edge.upstream_task_ids:
            plan.upstream_task_ids.append(task_id)
            if task_id in agent_assignments:
                plan.upstream_agent_map[task_id] = agent_assignments[task_id]

        # Map downstream tasks to agents
        for task_id in shuffle_edge.downstream_task_ids:
            plan.downstream_task_ids.append(task_id)
            if task_id in agent_assignments:
                plan.downstream_agent_map[task_id] = agent_assignments[task_id]

        return plan

    async def register_shuffles_with_agents(self, shuffle_managers: Dict[str, Any]) -> None:
        """
        Register shuffle operations with agent shuffle managers.

        Args:
            shuffle_managers: agent_id -> ShuffleManager (Cython) mapping

        This sets up shuffle metadata on each agent for coordination.
        """
        if not self._initialized:
            raise RuntimeError("Shuffle coordinator not initialized")

        logger.info("Registering shuffles with agents...")

        for shuffle_id, plan in self.shuffle_plans.items():
            # Get unique agents involved
            all_agents = set(plan.upstream_agent_map.values()) | set(plan.downstream_agent_map.values())

            for agent_address in all_agents:
                agent_id = agent_address.split(":")[0]  # Extract agent ID

                if agent_id in shuffle_managers:
                    manager = shuffle_managers[agent_id]

                    # Register shuffle with agent's shuffle manager
                    # (This calls into Cython ShuffleManager)
                    try:
                        # Import here to avoid circular dependencies
                        from ..._cython.shuffle.types import ShuffleEdgeType as CythonShuffleEdgeType

                        # Map Python enum to Cython enum
                        cython_edge_type = self._map_shuffle_type_to_cython(plan.edge_type)

                        # Get schema from first task
                        schema = self._get_shuffle_schema(plan)

                        # Register with shuffle manager
                        manager_shuffle_id = manager.register_shuffle(
                            operator_id=plan.shuffle_id.encode('utf-8'),
                            num_partitions=plan.num_partitions,
                            partition_keys=[k.encode('utf-8') for k in (plan.partition_keys or [])],
                            edge_type=cython_edge_type,
                            schema=schema
                        )

                        logger.info(f"Registered shuffle {shuffle_id} on agent {agent_id}")

                    except Exception as e:
                        logger.error(f"Failed to register shuffle on agent {agent_id}: {e}")

        logger.info("Shuffle registration complete")

    def _map_shuffle_type_to_cython(self, edge_type: ShuffleEdgeType):
        """Map Python ShuffleEdgeType to Cython enum."""
        try:
            from ..._cython.shuffle.types import ShuffleEdgeType as CythonType

            mapping = {
                ShuffleEdgeType.FORWARD: CythonType.FORWARD,
                ShuffleEdgeType.HASH: CythonType.HASH,
                ShuffleEdgeType.BROADCAST: CythonType.BROADCAST,
                ShuffleEdgeType.REBALANCE: CythonType.REBALANCE,
                ShuffleEdgeType.RANGE: CythonType.RANGE,
            }

            return mapping.get(edge_type, CythonType.HASH)
        except ImportError:
            # Fallback if Cython module not available
            return 1  # HASH

    def _get_shuffle_schema(self, plan: ShuffleExecutionPlan):
        """Get Arrow schema for shuffle (from upstream task)."""
        # Get first upstream task
        if plan.upstream_task_ids:
            task_id = plan.upstream_task_ids[0]
            task = self.execution_graph.tasks.get(task_id)

            # Get schema from task parameters
            if task and 'schema' in task.parameters:
                return task.parameters['schema']

        # Default: return None (will be inferred at runtime)
        return None

    async def set_task_locations(self, shuffle_managers: Dict[str, Any]) -> None:
        """
        Set task location metadata for shuffle coordination.

        Tells each agent where to fetch data from during shuffle reads.

        Args:
            shuffle_managers: agent_id -> ShuffleManager mapping
        """
        if not self._initialized:
            raise RuntimeError("Shuffle coordinator not initialized")

        logger.info("Setting task locations for shuffle coordination...")

        for shuffle_id, plan in self.shuffle_plans.items():
            # For each shuffle, register upstream task locations with downstream agents
            for downstream_task_id in plan.downstream_task_ids:
                if downstream_task_id in plan.downstream_agent_map:
                    agent_address = plan.downstream_agent_map[downstream_task_id]
                    agent_id = agent_address.split(":")[0]

                    if agent_id in shuffle_managers:
                        manager = shuffle_managers[agent_id]

                        # Register each upstream task location
                        for i, upstream_task_id in enumerate(plan.upstream_task_ids):
                            if upstream_task_id in plan.upstream_agent_map:
                                upstream_address = plan.upstream_agent_map[upstream_task_id]

                                # Set task location
                                manager.set_task_location(
                                    shuffle_id.encode('utf-8'),
                                    i,  # task index
                                    upstream_address.encode('utf-8')
                                )

        logger.info("Task locations configured")

    def get_shuffle_plan(self, shuffle_id: str) -> Optional[ShuffleExecutionPlan]:
        """Get execution plan for a shuffle."""
        return self.shuffle_plans.get(shuffle_id)

    def get_shuffles_for_task(self, task_id: str) -> List[ShuffleExecutionPlan]:
        """Get all shuffles involving a task."""
        shuffles = []
        for plan in self.shuffle_plans.values():
            if task_id in plan.upstream_task_ids or task_id in plan.downstream_task_ids:
                shuffles.append(plan)
        return shuffles

    def record_shuffle_metrics(self, shuffle_id: str, rows: int, bytes_count: int) -> None:
        """Record shuffle metrics."""
        if shuffle_id in self.shuffle_plans:
            plan = self.shuffle_plans[shuffle_id]
            plan.rows_shuffled += rows
            plan.bytes_shuffled += bytes_count

            # Update execution graph edge
            if shuffle_id in self.execution_graph.shuffle_edges:
                edge = self.execution_graph.shuffle_edges[shuffle_id]
                edge.rows_shuffled += rows
                edge.bytes_shuffled += bytes_count

    def get_shuffle_metrics(self) -> Dict[str, Any]:
        """Get aggregate shuffle metrics."""
        total_rows = sum(plan.rows_shuffled for plan in self.shuffle_plans.values())
        total_bytes = sum(plan.bytes_shuffled for plan in self.shuffle_plans.values())

        return {
            'total_shuffles': len(self.shuffle_plans),
            'total_rows_shuffled': total_rows,
            'total_bytes_shuffled': total_bytes,
            'shuffle_details': {
                shuffle_id: {
                    'edge_type': plan.edge_type.value,
                    'rows_shuffled': plan.rows_shuffled,
                    'bytes_shuffled': plan.bytes_shuffled,
                    'num_upstream': len(plan.upstream_task_ids),
                    'num_downstream': len(plan.downstream_task_ids),
                }
                for shuffle_id, plan in self.shuffle_plans.items()
            }
        }

    async def cleanup(self) -> None:
        """Cleanup shuffle resources."""
        logger.info("Cleaning up shuffle coordinator...")
        self.shuffle_plans.clear()
        self._initialized = False
