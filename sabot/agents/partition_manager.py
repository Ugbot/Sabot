#!/usr/bin/env python3
"""
Partition Manager for Sabot Agents

Implements intelligent work distribution and load balancing across agent instances:
- Partition assignment strategies
- Load balancing algorithms
- Dynamic scaling decisions
- Work routing and distribution

This ensures optimal resource utilization and fault tolerance.
"""

import asyncio
import logging
import hashlib
import random
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import time

from ..sabot_types import AgentSpec
from ..observability import get_observability

logger = logging.getLogger(__name__)


class PartitionStrategy(Enum):
    """Strategies for assigning partitions to agents."""
    ROUND_ROBIN = "round_robin"  # Simple round-robin distribution
    HASH_BASED = "hash_based"  # Hash-based for consistency
    LOAD_BALANCED = "load_balanced"  # Balance based on current load
    RANGE_BASED = "range_based"  # Divide key ranges
    CUSTOM = "custom"  # User-defined strategy


@dataclass
class PartitionInfo:
    """Information about a partition."""
    partition_id: str
    agent_id: str
    key_range_start: Optional[str] = None
    key_range_end: Optional[str] = None
    message_count: int = 0
    last_message_time: Optional[float] = None
    load_factor: float = 1.0


@dataclass
class AgentLoad:
    """Load information for an agent."""
    agent_id: str
    active_partitions: int = 0
    message_rate: float = 0.0  # messages per second
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    last_heartbeat: float = 0.0
    is_healthy: bool = True


@dataclass
class PartitionAssignment:
    """Result of partition assignment operation."""
    assignments: Dict[str, List[str]]  # agent_id -> [partition_ids]
    unassigned_partitions: List[str]
    timestamp: float
    strategy_used: PartitionStrategy
    rebalance_reason: Optional[str] = None


class PartitionManager:
    """
    Manages partition distribution across agent instances.

    Implements various strategies for distributing work partitions among
    running agents, with support for dynamic rebalancing and scaling.
    """

    def __init__(self, strategy: PartitionStrategy = PartitionStrategy.LOAD_BALANCED):
        """
        Initialize the partition manager.

        Args:
            strategy: Default partition assignment strategy
        """
        self.strategy = strategy
        self.observability = get_observability()

        # Partition state
        self._partitions: Dict[str, PartitionInfo] = {}
        self._agent_loads: Dict[str, AgentLoad] = {}
        self._partition_assignments: Dict[str, str] = {}  # partition_id -> agent_id

        # Assignment history
        self._assignment_history: List[PartitionAssignment] = []

        # Configuration
        self._rebalance_threshold = 0.2  # Rebalance if load imbalance > 20%
        self._max_partitions_per_agent = 10

        logger.info(f"PartitionManager initialized with strategy: {strategy.value}")

    async def register_agent(self, agent_spec: AgentSpec) -> None:
        """
        Register a new agent for partition assignment.

        Args:
            agent_spec: Agent specification
        """
        with self.observability.trace_operation(
            "partition_register_agent",
            {"agent_id": agent_spec.name, "concurrency": agent_spec.concurrency}
        ):
            agent_load = AgentLoad(
                agent_id=agent_spec.name,
                last_heartbeat=time.time(),
                is_healthy=True
            )

            self._agent_loads[agent_spec.name] = agent_load

            # Trigger rebalancing if we have partitions
            if self._partitions:
                await self.rebalance_partitions("agent_registered")

            logger.info(f"Registered agent {agent_spec.name} for partition management")

    async def unregister_agent(self, agent_id: str) -> List[str]:
        """
        Unregister an agent and return its partitions for reassignment.

        Args:
            agent_id: Agent to unregister

        Returns:
            List of partition IDs that were assigned to this agent
        """
        with self.observability.trace_operation(
            "partition_unregister_agent",
            {"agent_id": agent_id}
        ):
            if agent_id in self._agent_loads:
                del self._agent_loads[agent_id]

            # Find partitions assigned to this agent
            agent_partitions = [
                partition_id for partition_id, assigned_agent in self._partition_assignments.items()
                if assigned_agent == agent_id
            ]

            # Remove assignments
            for partition_id in agent_partitions:
                if partition_id in self._partition_assignments:
                    del self._partition_assignments[partition_id]

            # Trigger rebalancing for remaining partitions
            if agent_partitions and self._agent_loads:
                await self.rebalance_partitions("agent_unregistered")

            logger.info(f"Unregistered agent {agent_id}, released {len(agent_partitions)} partitions")
            return agent_partitions

    async def add_partition(self, partition_id: str, key_range_start: Optional[str] = None,
                          key_range_end: Optional[str] = None) -> None:
        """
        Add a new partition to be managed.

        Args:
            partition_id: Unique partition identifier
            key_range_start: Start of key range (for range-based partitioning)
            key_range_end: End of key range (for range-based partitioning)
        """
        with self.observability.trace_operation(
            "partition_add",
            {"partition_id": partition_id}
        ):
            if partition_id in self._partitions:
                logger.warning(f"Partition {partition_id} already exists")
                return

            partition_info = PartitionInfo(
                partition_id=partition_id,
                agent_id="",  # Will be assigned
                key_range_start=key_range_start,
                key_range_end=key_range_end
            )

            self._partitions[partition_id] = partition_info

            # Assign to an agent
            await self._assign_partition(partition_id)

            logger.info(f"Added partition {partition_id}")

    async def remove_partition(self, partition_id: str) -> Optional[str]:
        """
        Remove a partition from management.

        Args:
            partition_id: Partition to remove

        Returns:
            Agent ID that was assigned to this partition
        """
        with self.observability.trace_operation(
            "partition_remove",
            {"partition_id": partition_id}
        ):
            if partition_id not in self._partitions:
                return None

            assigned_agent = self._partition_assignments.get(partition_id)
            del self._partitions[partition_id]

            if partition_id in self._partition_assignments:
                del self._partition_assignments[partition_id]

            logger.info(f"Removed partition {partition_id} from agent {assigned_agent}")
            return assigned_agent

    async def get_partition_assignment(self, partition_id: str) -> Optional[str]:
        """
        Get the agent assigned to a partition.

        Args:
            partition_id: Partition to check

        Returns:
            Agent ID assigned to the partition, or None
        """
        return self._partition_assignments.get(partition_id)

    async def get_agent_partitions(self, agent_id: str) -> List[str]:
        """
        Get all partitions assigned to an agent.

        Args:
            agent_id: Agent to check

        Returns:
            List of partition IDs assigned to the agent
        """
        return [
            partition_id for partition_id, assigned_agent in self._partition_assignments.items()
            if assigned_agent == agent_id
        ]

    async def rebalance_partitions(self, reason: str = "manual") -> PartitionAssignment:
        """
        Rebalance partitions across available agents.

        Args:
            reason: Reason for rebalancing

        Returns:
            PartitionAssignment with the new assignments
        """
        with self.observability.trace_operation(
            "partition_rebalance",
            {"reason": reason, "agent_count": len(self._agent_loads)}
        ) as span:

            if not self._agent_loads:
                # No agents available
                assignment = PartitionAssignment(
                    assignments={},
                    unassigned_partitions=list(self._partitions.keys()),
                    timestamp=time.time(),
                    strategy_used=self.strategy,
                    rebalance_reason=reason
                )
                self._assignment_history.append(assignment)
                return assignment

            # Calculate new assignments based on strategy
            new_assignments = await self._calculate_assignments()

            # Apply new assignments
            old_assignments = self._partition_assignments.copy()
            self._partition_assignments = new_assignments

            # Create assignment result
            assignments_by_agent = {}
            for partition_id, agent_id in new_assignments.items():
                if agent_id not in assignments_by_agent:
                    assignments_by_agent[agent_id] = []
                assignments_by_agent[agent_id].append(partition_id)

            unassigned = [
                pid for pid in self._partitions.keys()
                if pid not in new_assignments
            ]

            assignment = PartitionAssignment(
                assignments=assignments_by_agent,
                unassigned_partitions=unassigned,
                timestamp=time.time(),
                strategy_used=self.strategy,
                rebalance_reason=reason
            )

            self._assignment_history.append(assignment)

            # Record metrics
            self.observability.record_metric("partition_rebalance", 1, {
                "reason": reason,
                "agents_affected": len(assignments_by_agent),
                "partitions_assigned": len(new_assignments),
                "unassigned_partitions": len(unassigned)
            })

            span.set_attribute("assignments_made", len(new_assignments))
            span.set_attribute("unassigned_count", len(unassigned))
            span.set_attribute("reason", reason)

            logger.info(f"Rebalanced partitions: {len(new_assignments)} assigned, {len(unassigned)} unassigned")
            return assignment

    async def _calculate_assignments(self) -> Dict[str, str]:
        """Calculate partition assignments based on current strategy."""
        if self.strategy == PartitionStrategy.ROUND_ROBIN:
            return await self._assign_round_robin()
        elif self.strategy == PartitionStrategy.HASH_BASED:
            return await self._assign_hash_based()
        elif self.strategy == PartitionStrategy.LOAD_BALANCED:
            return await self._assign_load_balanced()
        elif self.strategy == PartitionStrategy.RANGE_BASED:
            return await self._assign_range_based()
        else:
            return await self._assign_round_robin()  # Default fallback

    async def _assign_round_robin(self) -> Dict[str, str]:
        """Assign partitions using round-robin strategy."""
        assignments = {}
        healthy_agents = [
            agent_id for agent_id, load in self._agent_loads.items()
            if load.is_healthy
        ]

        if not healthy_agents:
            return assignments

        agent_index = 0
        for partition_id in self._partitions.keys():
            agent_id = healthy_agents[agent_index % len(healthy_agents)]
            assignments[partition_id] = agent_id

            # Update agent load
            if agent_id in self._agent_loads:
                self._agent_loads[agent_id].active_partitions += 1

            agent_index += 1

        return assignments

    async def _assign_hash_based(self) -> Dict[str, str]:
        """Assign partitions using hash-based strategy for consistency."""
        assignments = {}
        healthy_agents = [
            agent_id for agent_id, load in self._agent_loads.items()
            if load.is_healthy
        ]

        if not healthy_agents:
            return assignments

        for partition_id in self._partitions.keys():
            # Hash the partition ID to get consistent agent assignment
            hash_value = int(hashlib.md5(partition_id.encode()).hexdigest(), 16)
            agent_index = hash_value % len(healthy_agents)
            agent_id = healthy_agents[agent_index]

            assignments[partition_id] = agent_id

            # Update agent load
            if agent_id in self._agent_loads:
                self._agent_loads[agent_id].active_partitions += 1

        return assignments

    async def _assign_load_balanced(self) -> Dict[str, str]:
        """Assign partitions using load-balanced strategy."""
        assignments = {}
        healthy_agents = [
            (agent_id, load) for agent_id, load in self._agent_loads.items()
            if load.is_healthy
        ]

        if not healthy_agents:
            return assignments

        # Sort agents by current load (fewest partitions first)
        healthy_agents.sort(key=lambda x: x[1].active_partitions)

        for partition_id in self._partitions.keys():
            # Assign to agent with lowest current load
            agent_id, load = healthy_agents[0]
            assignments[partition_id] = agent_id

            # Update load (simulate assignment)
            load.active_partitions += 1

            # Re-sort after assignment
            healthy_agents.sort(key=lambda x: x[1].active_partitions)

        return assignments

    async def _assign_range_based(self) -> Dict[str, str]:
        """Assign partitions using range-based strategy."""
        assignments = {}
        healthy_agents = [
            agent_id for agent_id, load in self._agent_loads.items()
            if load.is_healthy
        ]

        if not healthy_agents:
            return assignments

        # Sort partitions by key range
        sorted_partitions = sorted(
            self._partitions.items(),
            key=lambda x: x[1].key_range_start or ""
        )

        # Divide partitions evenly among agents
        partitions_per_agent = len(sorted_partitions) // len(healthy_agents)
        remainder = len(sorted_partitions) % len(healthy_agents)

        agent_index = 0
        partitions_assigned = 0

        for partition_id, partition_info in sorted_partitions:
            # Assign to current agent
            agent_id = healthy_agents[agent_index]
            assignments[partition_id] = agent_id

            # Update agent load
            if agent_id in self._agent_loads:
                self._agent_loads[agent_id].active_partitions += 1

            partitions_assigned += 1

            # Check if we should move to next agent
            target_partitions = partitions_per_agent
            if agent_index < remainder:
                target_partitions += 1

            if partitions_assigned >= target_partitions:
                agent_index = (agent_index + 1) % len(healthy_agents)
                partitions_assigned = 0

        return assignments

    async def _assign_partition(self, partition_id: str) -> None:
        """Assign a single partition to an agent."""
        if not self._agent_loads:
            return  # No agents available

        # Use current strategy to assign
        assignments = await self._calculate_assignments()
        if partition_id in assignments:
            self._partition_assignments[partition_id] = assignments[partition_id]

            # Update partition info
            if partition_id in self._partitions:
                self._partitions[partition_id].agent_id = assignments[partition_id]

    def update_agent_load(self, agent_id: str, message_rate: float,
                         cpu_usage: float, memory_usage: float) -> None:
        """
        Update load information for an agent.

        Args:
            agent_id: Agent to update
            message_rate: Current message processing rate
            cpu_usage: Current CPU usage (0-1)
            memory_usage: Current memory usage (0-1)
        """
        if agent_id in self._agent_loads:
            load = self._agent_loads[agent_id]
            load.message_rate = message_rate
            load.cpu_usage = cpu_usage
            load.memory_usage = memory_usage
            load.last_heartbeat = time.time()

    def mark_agent_unhealthy(self, agent_id: str) -> None:
        """Mark an agent as unhealthy."""
        if agent_id in self._agent_loads:
            self._agent_loads[agent_id].is_healthy = False

            # Trigger rebalancing
            asyncio.create_task(self.rebalance_partitions("agent_unhealthy"))

    def mark_agent_healthy(self, agent_id: str) -> None:
        """Mark an agent as healthy."""
        if agent_id in self._agent_loads:
            self._agent_loads[agent_id].is_healthy = True

            # Trigger rebalancing
            asyncio.create_task(self.rebalance_partitions("agent_recovered"))

    def get_partition_stats(self) -> Dict[str, Any]:
        """Get statistics about current partition distribution."""
        agent_partition_counts = {}
        for agent_id in self._agent_loads.keys():
            agent_partition_counts[agent_id] = 0

        for assigned_agent in self._partition_assignments.values():
            if assigned_agent in agent_partition_counts:
                agent_partition_counts[assigned_agent] += 1

        return {
            "total_partitions": len(self._partitions),
            "assigned_partitions": len(self._partition_assignments),
            "unassigned_partitions": len(self._partitions) - len(self._partition_assignments),
            "agent_distribution": agent_partition_counts,
            "strategy": self.strategy.value,
            "last_rebalance": self._assignment_history[-1].timestamp if self._assignment_history else None
        }

    def set_strategy(self, strategy: PartitionStrategy) -> None:
        """
        Change the partition assignment strategy.

        Args:
            strategy: New strategy to use
        """
        old_strategy = self.strategy
        self.strategy = strategy

        logger.info(f"Changed partition strategy from {old_strategy.value} to {strategy.value}")

        # Trigger rebalancing with new strategy
        asyncio.create_task(self.rebalance_partitions("strategy_changed"))
