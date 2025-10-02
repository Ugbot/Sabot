#!/usr/bin/env python3
"""
Agent Supervisor for Sabot

Implements supervision strategies for managing agent lifecycles:
- One-for-one: Restart only failed agents
- One-for-all: Restart all agents when any fails
- Rest-for-one: Restart failed agent and all started after it

Provides fault-tolerant agent management with configurable restart policies.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from enum import Enum
import time

from .runtime import AgentRuntime, SupervisionStrategy, AgentState, RestartPolicy
from ..core.metrics import MetricsCollector

logger = logging.getLogger(__name__)


@dataclass
class SupervisionGroup:
    """A group of agents under supervision."""
    name: str
    strategy: SupervisionStrategy
    agents: List[str] = field(default_factory=list)
    restart_intensity: int = 3  # Max restarts within window
    restart_period: float = 60.0  # Time window in seconds
    restart_history: List[float] = field(default_factory=list)

    def should_supervise_restart(self, failed_agent: str, agent_states: Dict[str, AgentState]) -> List[str]:
        """
        Determine which agents should be restarted based on supervision strategy.

        Args:
            failed_agent: The agent that failed
            agent_states: Current states of all agents

        Returns:
            List of agent IDs to restart
        """
        if failed_agent not in self.agents:
            return []

        to_restart = []

        if self.strategy == SupervisionStrategy.ONE_FOR_ONE:
            # Only restart the failed agent
            to_restart = [failed_agent]

        elif self.strategy == SupervisionStrategy.ONE_FOR_ALL:
            # Restart all agents in the group
            to_restart = self.agents.copy()

        elif self.strategy == SupervisionStrategy.REST_FOR_ONE:
            # Restart failed agent and all agents started after it
            failed_index = self.agents.index(failed_agent)
            to_restart = self.agents[failed_index:]

        elif self.strategy == SupervisionStrategy.SIMPLE_ONE_FOR_ONE:
            # For dynamic agents, only restart the failed one
            to_restart = [failed_agent]

        return to_restart

    def record_restart(self) -> None:
        """Record a restart event for intensity limiting."""
        current_time = time.time()
        self.restart_history.append(current_time)

        # Remove old restarts outside the window
        cutoff_time = current_time - self.restart_period
        self.restart_history = [t for t in self.restart_history if t > cutoff_time]

    def can_restart(self) -> bool:
        """Check if restart is allowed based on intensity limits."""
        return len(self.restart_history) < self.restart_intensity


@dataclass
class SupervisorConfig:
    """Configuration for the agent supervisor."""
    default_strategy: SupervisionStrategy = SupervisionStrategy.ONE_FOR_ONE
    default_restart_intensity: int = 3
    default_restart_period: float = 60.0
    enable_metrics: bool = True


class AgentSupervisor:
    """
    Agent Supervisor System

    Provides hierarchical supervision of agent groups with different strategies:
    - Fault detection and isolation
    - Configurable restart policies
    - Intensity limiting to prevent restart storms
    - Metrics and monitoring integration
    """

    def __init__(self, runtime: AgentRuntime, config: Optional[SupervisorConfig] = None):
        """
        Initialize the agent supervisor.

        Args:
            runtime: Agent runtime to supervise
            config: Supervisor configuration
        """
        self.runtime = runtime
        self.config = config or SupervisorConfig()

        # Supervision groups
        self._groups: Dict[str, SupervisionGroup] = {}
        self._agent_to_group: Dict[str, str] = {}

        # Metrics
        self.metrics: Optional[MetricsCollector] = None
        if self.config.enable_metrics:
            self.metrics = MetricsCollector()

        logger.info(f"AgentSupervisor initialized with strategy: {self.config.default_strategy}")

    def create_supervision_group(self, name: str,
                                strategy: Optional[SupervisionStrategy] = None,
                                restart_intensity: Optional[int] = None,
                                restart_period: Optional[float] = None) -> str:
        """
        Create a new supervision group.

        Args:
            name: Group name
            strategy: Supervision strategy
            restart_intensity: Max restarts in period
            restart_period: Restart window in seconds

        Returns:
            Group name
        """
        if name in self._groups:
            raise ValueError(f"Supervision group {name} already exists")

        group = SupervisionGroup(
            name=name,
            strategy=strategy or self.config.default_strategy,
            restart_intensity=restart_intensity or self.config.default_restart_intensity,
            restart_period=restart_period or self.config.default_restart_period
        )

        self._groups[name] = group
        logger.info(f"Created supervision group: {name} ({strategy.value})")

        return name

    def add_agent_to_group(self, agent_id: str, group_name: str) -> None:
        """
        Add an agent to a supervision group.

        Args:
            agent_id: Agent identifier
            group_name: Supervision group name

        Raises:
            ValueError: If group doesn't exist
        """
        if group_name not in self._groups:
            raise ValueError(f"Supervision group {group_name} not found")

        group = self._groups[group_name]

        if agent_id not in group.agents:
            group.agents.append(agent_id)
            self._agent_to_group[agent_id] = group_name

            logger.info(f"Added agent {agent_id} to supervision group {group_name}")

    def remove_agent_from_group(self, agent_id: str, group_name: str) -> None:
        """
        Remove an agent from a supervision group.

        Args:
            agent_id: Agent identifier
            group_name: Supervision group name
        """
        if group_name in self._groups:
            group = self._groups[group_name]
            if agent_id in group.agents:
                group.agents.remove(agent_id)
                if agent_id in self._agent_to_group:
                    del self._agent_to_group[agent_id]

                logger.info(f"Removed agent {agent_id} from supervision group {group_name}")

    async def handle_agent_failure(self, failed_agent_id: str) -> List[str]:
        """
        Handle an agent failure according to supervision strategy.

        Args:
            failed_agent_id: The agent that failed

        Returns:
            List of agents that were restarted
        """
        group_name = self._agent_to_group.get(failed_agent_id)
        if not group_name:
            logger.warning(f"Agent {failed_agent_id} not in any supervision group")
            return []

        group = self._groups[group_name]

        # Check if group can restart (intensity limiting)
        if not group.can_restart():
            logger.warning(f"Supervision group {group_name} exceeded restart intensity limit")
            return []

        # Get all agent states
        all_status = self.runtime.get_all_agent_status()
        agent_states = {
            agent_id: AgentState(status.get("state", "stopped"))
            for agent_id, status in all_status.items()
        }

        # Determine which agents to restart
        to_restart = group.should_supervise_restart(failed_agent_id, agent_states)

        if not to_restart:
            logger.info(f"No agents to restart for failed agent {failed_agent_id}")
            return []

        # Record the restart event
        group.record_restart()

        # Perform the restarts
        restarted = []
        for agent_id in to_restart:
            if agent_id == failed_agent_id:
                # Failed agent is already stopped, just restart it
                success = await self.runtime.restart_agent(agent_id)
                if success:
                    restarted.append(agent_id)
                    logger.info(f"Supervised restart of failed agent {agent_id}")
            else:
                # Stop and restart other agents
                await self.runtime.stop_agent(agent_id, force=True)
                success = await self.runtime.start_agent(agent_id)
                if success:
                    restarted.append(agent_id)
                    logger.info(f"Supervised restart of agent {agent_id} (due to {failed_agent_id} failure)")

        # Record metrics
        if self.metrics:
            await self.metrics.record_error(failed_agent_id, f"supervised_restart: {len(restarted)} agents")
            for agent_id in restarted:
                await self.metrics.record_agent_status(agent_id, "restarted")

        logger.info(f"Supervision completed for {failed_agent_id}: restarted {len(restarted)} agents")
        return restarted

    def get_supervision_status(self) -> Dict[str, Any]:
        """Get status of all supervision groups."""
        return {
            group_name: {
                "strategy": group.strategy.value,
                "agents": group.agents.copy(),
                "restart_intensity": group.restart_intensity,
                "restart_period": group.restart_period,
                "recent_restarts": len(group.restart_history),
                "can_restart": group.can_restart()
            }
            for group_name, group in self._groups.items()
        }

    def get_agent_supervision_info(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """Get supervision information for a specific agent."""
        group_name = self._agent_to_group.get(agent_id)
        if not group_name:
            return None

        group = self._groups[group_name]
        return {
            "group": group_name,
            "strategy": group.strategy.value,
            "position_in_group": group.agents.index(agent_id) if agent_id in group.agents else -1,
            "group_size": len(group.agents),
            "restart_intensity": group.restart_intensity,
            "restart_period": group.restart_period
        }

    async def emergency_stop_group(self, group_name: str) -> int:
        """
        Emergency stop all agents in a supervision group.

        Args:
            group_name: Supervision group name

        Returns:
            Number of agents stopped
        """
        if group_name not in self._groups:
            logger.error(f"Supervision group {group_name} not found")
            return 0

        group = self._groups[group_name]
        stopped = 0

        for agent_id in group.agents:
            success = await self.runtime.stop_agent(agent_id, force=True)
            if success:
                stopped += 1

        logger.info(f"Emergency stopped {stopped} agents in group {group_name}")
        return stopped

    async def emergency_restart_group(self, group_name: str) -> int:
        """
        Emergency restart all agents in a supervision group.

        Args:
            group_name: Supervision group name

        Returns:
            Number of agents restarted
        """
        if group_name not in self._groups:
            logger.error(f"Supervision group {group_name} not found")
            return 0

        group = self._groups[group_name]

        # Reset restart history to allow emergency restart
        group.restart_history.clear()

        restarted = 0
        for agent_id in group.agents:
            success = await self.runtime.start_agent(agent_id)
            if success:
                restarted += 1

        logger.info(f"Emergency restarted {restarted} agents in group {group_name}")
        return restarted

    def delete_supervision_group(self, group_name: str) -> bool:
        """
        Delete a supervision group.

        Args:
            group_name: Supervision group name

        Returns:
            True if deleted successfully
        """
        if group_name not in self._groups:
            return False

        group = self._groups[group_name]

        # Remove all agents from the group
        for agent_id in group.agents:
            if agent_id in self._agent_to_group:
                del self._agent_to_group[agent_id]

        # Delete the group
        del self._groups[group_name]

        logger.info(f"Deleted supervision group: {group_name}")
        return True
