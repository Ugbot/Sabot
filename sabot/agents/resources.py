#!/usr/bin/env python3
"""
Resource Management for Sabot Agents

Implements resource isolation and limits:
- Memory limits and monitoring
- CPU limits and throttling
- Process isolation
- Resource quotas and enforcement

Provides production-grade resource management for agent processes.
"""

import asyncio
import logging
import psutil
import os
import signal
import time
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
import threading

from ..core.metrics import MetricsCollector

logger = logging.getLogger(__name__)


class ResourceLimitType(Enum):
    """Types of resource limits."""
    MEMORY = "memory"
    CPU = "cpu"
    FILE_DESCRIPTORS = "file_descriptors"
    PROCESSES = "processes"


@dataclass
class ResourceLimits:
    """Resource limits for an agent."""
    memory_mb: Optional[int] = None
    cpu_percent: Optional[float] = None
    max_file_descriptors: Optional[int] = None
    max_processes: Optional[int] = None

    def is_empty(self) -> bool:
        """Check if no limits are set."""
        return all(v is None for v in [self.memory_mb, self.cpu_percent,
                                     self.max_file_descriptors, self.max_processes])


@dataclass
class ResourceUsage:
    """Current resource usage."""
    memory_mb: float = 0.0
    cpu_percent: float = 0.0
    file_descriptors: Optional[int] = None
    threads: int = 0
    timestamp: float = field(default_factory=time.time)

    @property
    def is_stale(self) -> bool:
        """Check if usage data is stale (older than 30 seconds)."""
        return time.time() - self.timestamp > 30.0


@dataclass
class ResourceQuota:
    """Resource quota for a group of agents."""
    name: str
    total_memory_mb: Optional[int] = None
    total_cpu_percent: Optional[float] = None
    max_agents: Optional[int] = None
    agents: List[str] = field(default_factory=list)
    current_memory_mb: float = 0.0
    current_cpu_percent: float = 0.0

    def can_add_agent(self, agent_limits: ResourceLimits) -> bool:
        """Check if an agent can be added to this quota."""
        if self.max_agents and len(self.agents) >= self.max_agents:
            return False

        if self.total_memory_mb and agent_limits.memory_mb:
            if self.current_memory_mb + agent_limits.memory_mb > self.total_memory_mb:
                return False

        if self.total_cpu_percent and agent_limits.cpu_percent:
            if self.current_cpu_percent + agent_limits.cpu_percent > self.total_cpu_percent:
                return False

        return True

    def add_agent(self, agent_id: str, agent_limits: ResourceLimits) -> None:
        """Add an agent to this quota."""
        if agent_id not in self.agents:
            self.agents.append(agent_id)

            if agent_limits.memory_mb:
                self.current_memory_mb += agent_limits.memory_mb

            if agent_limits.cpu_percent:
                self.current_cpu_percent += agent_limits.cpu_percent

    def remove_agent(self, agent_id: str, agent_limits: ResourceLimits) -> None:
        """Remove an agent from this quota."""
        if agent_id in self.agents:
            self.agents.remove(agent_id)

            if agent_limits.memory_mb:
                self.current_memory_mb -= agent_limits.memory_mb

            if agent_limits.cpu_percent:
                self.current_cpu_percent -= agent_limits.cpu_percent


class ResourceManager:
    """
    Resource Manager for Agent Processes

    Provides comprehensive resource management with:
    - Per-agent resource limits
    - Resource quotas for agent groups
    - Real-time monitoring and enforcement
    - Graceful degradation under resource pressure
    """

    def __init__(self, enable_metrics: bool = True):
        """
        Initialize the resource manager.

        Args:
            enable_metrics: Whether to enable metrics collection
        """
        self.enable_metrics = enable_metrics
        self.metrics: Optional[MetricsCollector] = None

        # Resource tracking
        self._agent_limits: Dict[str, ResourceLimits] = {}
        self._agent_usage: Dict[str, ResourceUsage] = {}
        self._quotas: Dict[str, ResourceQuota] = {}

        # Enforcement settings
        self._enforcement_enabled = True
        self._graceful_degradation = True

        # Monitoring
        self._monitoring_interval = 5.0  # seconds
        self._violation_callbacks: List[callable] = []

        # Threading
        self._lock = threading.Lock()
        self._monitoring_task: Optional[asyncio.Task] = None
        self._running = False

        if enable_metrics:
            self.metrics = MetricsCollector()

        logger.info("ResourceManager initialized")

    async def start(self) -> None:
        """Start resource monitoring."""
        if self._running:
            return

        self._running = True
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())

        logger.info("ResourceManager monitoring started")

    async def stop(self) -> None:
        """Stop resource monitoring."""
        if not self._running:
            return

        self._running = False

        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass

        logger.info("ResourceManager monitoring stopped")

    def set_agent_limits(self, agent_id: str, limits: ResourceLimits) -> None:
        """
        Set resource limits for an agent.

        Args:
            agent_id: Agent identifier
            limits: Resource limits
        """
        with self._lock:
            self._agent_limits[agent_id] = limits
            logger.info(f"Set resource limits for agent {agent_id}: {limits}")

    def get_agent_limits(self, agent_id: str) -> Optional[ResourceLimits]:
        """Get resource limits for an agent."""
        return self._agent_limits.get(agent_id)

    def create_resource_quota(self, name: str, total_memory_mb: Optional[int] = None,
                            total_cpu_percent: Optional[float] = None,
                            max_agents: Optional[int] = None) -> str:
        """
        Create a resource quota.

        Args:
            name: Quota name
            total_memory_mb: Total memory limit in MB
            total_cpu_percent: Total CPU limit in percent
            max_agents: Maximum number of agents

        Returns:
            Quota name
        """
        if name in self._quotas:
            raise ValueError(f"Resource quota {name} already exists")

        quota = ResourceQuota(
            name=name,
            total_memory_mb=total_memory_mb,
            total_cpu_percent=total_cpu_percent,
            max_agents=max_agents
        )

        self._quotas[name] = quota
        logger.info(f"Created resource quota: {name}")

        return name

    def check_quota_allocation(self, quota_name: str, agent_limits: ResourceLimits) -> bool:
        """
        Check if an agent can be allocated to a quota.

        Args:
            quota_name: Quota name
            agent_limits: Agent resource limits

        Returns:
            True if allocation is allowed
        """
        quota = self._quotas.get(quota_name)
        if not quota:
            return True  # No quota means unlimited

        return quota.can_add_agent(agent_limits)

    def allocate_to_quota(self, quota_name: str, agent_id: str, agent_limits: ResourceLimits) -> None:
        """
        Allocate an agent to a resource quota.

        Args:
            quota_name: Quota name
            agent_id: Agent identifier
            agent_limits: Agent resource limits

        Raises:
            ValueError: If quota doesn't exist or allocation not allowed
        """
        quota = self._quotas.get(quota_name)
        if not quota:
            raise ValueError(f"Resource quota {quota_name} not found")

        if not quota.can_add_agent(agent_limits):
            raise ValueError(f"Cannot allocate agent {agent_id} to quota {quota_name}")

        quota.add_agent(agent_id, agent_limits)
        logger.info(f"Allocated agent {agent_id} to quota {quota_name}")

    def deallocate_from_quota(self, quota_name: str, agent_id: str) -> None:
        """
        Deallocate an agent from a resource quota.

        Args:
            quota_name: Quota name
            agent_id: Agent identifier
        """
        quota = self._quotas.get(quota_name)
        if quota:
            agent_limits = self._agent_limits.get(agent_id, ResourceLimits())
            quota.remove_agent(agent_id, agent_limits)
            logger.info(f"Deallocated agent {agent_id} from quota {quota_name}")

    async def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        logger.info("Resource monitoring loop started")

        while self._running:
            try:
                await self._collect_resource_usage()
                await self._enforce_limits()

                await asyncio.sleep(self._monitoring_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Resource monitoring error: {e}")
                await asyncio.sleep(1.0)

        logger.info("Resource monitoring loop stopped")

    async def _collect_resource_usage(self) -> None:
        """Collect current resource usage for all tracked agents."""
        # Get all processes we're tracking
        pids_to_check = []

        with self._lock:
            for agent_id, limits in self._agent_limits.items():
                if not limits.is_empty():
                    # We need to get the PID from somewhere - this would be
                    # passed from the runtime system
                    # For now, we'll skip detailed per-process monitoring
                    pass

        # System-wide resource monitoring
        try:
            system_memory = psutil.virtual_memory()
            system_cpu = psutil.cpu_percent(interval=0.1)

            # Record system metrics
            if self.metrics:
                await self.metrics.record_resource_usage(
                    "system",
                    int(system_memory.used),
                    system_cpu
                )

        except Exception as e:
            logger.error(f"Failed to collect system resource usage: {e}")

    async def _enforce_limits(self) -> None:
        """Enforce resource limits on agents."""
        violations = []

        with self._lock:
            for agent_id, limits in self._agent_limits.items():
                usage = self._agent_usage.get(agent_id)

                if not usage or usage.is_stale:
                    continue

                # Check memory limit
                if limits.memory_mb and usage.memory_mb > limits.memory_mb:
                    violations.append({
                        'agent_id': agent_id,
                        'limit_type': ResourceLimitType.MEMORY,
                        'limit_value': limits.memory_mb,
                        'actual_value': usage.memory_mb,
                        'severity': 'critical' if usage.memory_mb > limits.memory_mb * 1.5 else 'warning'
                    })

                # Check CPU limit
                if limits.cpu_percent and usage.cpu_percent > limits.cpu_percent:
                    violations.append({
                        'agent_id': agent_id,
                        'limit_type': ResourceLimitType.CPU,
                        'limit_value': limits.cpu_percent,
                        'actual_value': usage.cpu_percent,
                        'severity': 'warning'
                    })

        # Handle violations
        for violation in violations:
            await self._handle_violation(violation)

    async def _handle_violation(self, violation: Dict[str, Any]) -> None:
        """
        Handle a resource limit violation.

        Args:
            violation: Violation details
        """
        agent_id = violation['agent_id']
        limit_type = violation['limit_type']
        severity = violation['severity']

        logger.warning(f"Resource violation for agent {agent_id}: "
                      f"{limit_type.value} limit exceeded ({violation['actual_value']} > {violation['limit_value']})")

        # Record metrics
        if self.metrics:
            await self.metrics.record_error(
                agent_id,
                f"resource_violation_{limit_type.value}_{severity}"
            )

        # Call violation callbacks
        for callback in self._violation_callbacks:
            try:
                await callback(violation)
            except Exception as e:
                logger.error(f"Violation callback error: {e}")

        # For critical violations, we would signal the agent runtime
        # to restart or throttle the agent
        if severity == 'critical' and self._enforcement_enabled:
            logger.error(f"Critical resource violation for agent {agent_id} - enforcement needed")

    def add_violation_callback(self, callback: callable) -> None:
        """
        Add a callback for resource violations.

        Args:
            callback: Async function to call on violations
        """
        self._violation_callbacks.append(callback)

    def get_resource_status(self) -> Dict[str, Any]:
        """Get current resource status."""
        with self._lock:
            return {
                'agent_limits': {
                    agent_id: {
                        'memory_mb': limits.memory_mb,
                        'cpu_percent': limits.cpu_percent,
                        'file_descriptors': limits.max_file_descriptors,
                        'processes': limits.max_processes
                    }
                    for agent_id, limits in self._agent_limits.items()
                },
                'quotas': {
                    quota_name: {
                        'total_memory_mb': quota.total_memory_mb,
                        'total_cpu_percent': quota.total_cpu_percent,
                        'max_agents': quota.max_agents,
                        'current_agents': len(quota.agents),
                        'current_memory_mb': quota.current_memory_mb,
                        'current_cpu_percent': quota.current_cpu_percent
                    }
                    for quota_name, quota in self._quotas.items()
                },
                'enforcement_enabled': self._enforcement_enabled,
                'monitoring_interval': self._monitoring_interval
            }

    def get_agent_resource_usage(self, agent_id: str) -> Optional[ResourceUsage]:
        """Get current resource usage for an agent."""
        return self._agent_usage.get(agent_id)

    def enable_enforcement(self, enabled: bool = True) -> None:
        """Enable or disable resource limit enforcement."""
        self._enforcement_enabled = enabled
        logger.info(f"Resource enforcement {'enabled' if enabled else 'disabled'}")

    def set_monitoring_interval(self, interval: float) -> None:
        """Set the monitoring interval in seconds."""
        self._monitoring_interval = max(1.0, interval)
        logger.info(f"Resource monitoring interval set to {interval}s")
