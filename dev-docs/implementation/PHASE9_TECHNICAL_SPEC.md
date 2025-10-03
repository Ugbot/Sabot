# Phase 9: Additional Features - Technical Specification

**Phase:** 9 of 9
**Focus:** Add production-ready operational features
**Dependencies:** Phases 1-6 (Core functionality working)
**Goal:** Enable production monitoring, supervision, and operational excellence

---

## Overview

Phase 9 adds "nice to have" features that make Sabot production-ready but are not strictly required for core streaming functionality. These features enhance observability, fault tolerance, and operational control.

**Current State:**
- Basic supervision structure exists (`sabot/agents/supervisor.py`)
- Resource monitoring stubs exist (`sabot/agents/resources.py`)
- Metrics collection partially implemented (`sabot/core/metrics.py`)
- No savepoint functionality

**Goal:**
- Complete supervision strategies (ONE_FOR_ONE, ONE_FOR_ALL, REST_FOR_ONE)
- Add resource monitoring (CPU, memory, backpressure)
- Complete metrics collection and export (Prometheus, StatsD)
- Implement savepoints (manual checkpoints)

**Note:** This phase should be implemented AFTER Phases 1-6 are complete and core functionality is working.

---

## Step 9.1: Implement Process Supervision

### Overview

Implement Erlang/OTP-style supervision strategies to automatically restart failed agents.

**Supervision Strategies:**
- **ONE_FOR_ONE**: Restart only the failed agent
- **ONE_FOR_ALL**: Restart all agents when any fails
- **REST_FOR_ONE**: Restart failed agent and all started after it
- **SIMPLE_ONE_FOR_ONE**: For dynamically created agents

### Implementation

#### Complete `sabot/agents/supervisor.py`

```python
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

from .runtime import AgentProcess, AgentState
from ..core.metrics import MetricsCollector

logger = logging.getLogger(__name__)


class SupervisionStrategy(Enum):
    """Supervision strategies for agent restarts."""
    ONE_FOR_ONE = "one_for_one"
    ONE_FOR_ALL = "one_for_all"
    REST_FOR_ONE = "rest_for_one"
    SIMPLE_ONE_FOR_ONE = "simple_one_for_one"


class RestartPolicy(Enum):
    """When to restart agents."""
    PERMANENT = "permanent"    # Always restart
    TRANSIENT = "transient"    # Restart only on abnormal exit
    TEMPORARY = "temporary"    # Never restart


@dataclass
class SupervisionGroup:
    """A group of agents under supervision."""
    name: str
    strategy: SupervisionStrategy
    agents: List[str] = field(default_factory=list)
    restart_intensity: int = 3  # Max restarts within window
    restart_period: float = 60.0  # Time window in seconds
    restart_history: List[float] = field(default_factory=list)

    def should_supervise_restart(
        self,
        failed_agent: str,
        agent_states: Dict[str, AgentState]
    ) -> List[str]:
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
    default_restart_policy: RestartPolicy = RestartPolicy.PERMANENT
    default_restart_intensity: int = 3
    default_restart_period: float = 60.0
    enable_metrics: bool = True


class AgentSupervisor:
    """
    Agent Supervisor System

    Provides hierarchical supervision of agent groups with different strategies:
    - Fault detection and isolation
    - Configurable restart policies
    - Restart intensity limits
    - Health monitoring
    """

    def __init__(self, config: Optional[SupervisorConfig] = None):
        self.config = config or SupervisorConfig()

        self.groups: Dict[str, SupervisionGroup] = {}
        self.agent_to_group: Dict[str, str] = {}
        self.agent_processes: Dict[str, AgentProcess] = {}
        self.agent_restart_policies: Dict[str, RestartPolicy] = {}

        self.metrics: Optional[MetricsCollector] = None
        if self.config.enable_metrics:
            self.metrics = MetricsCollector()

        self._shutdown = False
        self._monitor_task: Optional[asyncio.Task] = None

    def create_supervision_group(
        self,
        name: str,
        strategy: SupervisionStrategy,
        restart_intensity: Optional[int] = None,
        restart_period: Optional[float] = None
    ) -> SupervisionGroup:
        """
        Create a supervision group.

        Args:
            name: Group name
            strategy: Supervision strategy
            restart_intensity: Max restarts in time window
            restart_period: Time window for restart limiting (seconds)

        Returns:
            SupervisionGroup instance
        """
        group = SupervisionGroup(
            name=name,
            strategy=strategy,
            restart_intensity=restart_intensity or self.config.default_restart_intensity,
            restart_period=restart_period or self.config.default_restart_period
        )

        self.groups[name] = group
        logger.info(f"Created supervision group '{name}' with strategy {strategy.value}")

        return group

    def add_agent(
        self,
        agent_id: str,
        agent_process: AgentProcess,
        group_name: str = "default",
        restart_policy: Optional[RestartPolicy] = None
    ) -> None:
        """
        Add an agent to a supervision group.

        Args:
            agent_id: Agent identifier
            agent_process: AgentProcess instance
            group_name: Supervision group name
            restart_policy: Restart policy for this agent
        """
        # Create default group if needed
        if group_name not in self.groups:
            self.create_supervision_group(
                name=group_name,
                strategy=self.config.default_strategy
            )

        # Add to group
        group = self.groups[group_name]
        if agent_id not in group.agents:
            group.agents.append(agent_id)

        self.agent_to_group[agent_id] = group_name
        self.agent_processes[agent_id] = agent_process
        self.agent_restart_policies[agent_id] = (
            restart_policy or self.config.default_restart_policy
        )

        logger.info(
            f"Added agent '{agent_id}' to supervision group '{group_name}' "
            f"with policy {self.agent_restart_policies[agent_id].value}"
        )

    async def start(self) -> None:
        """Start supervisor and monitoring loop."""
        logger.info("Starting agent supervisor")
        self._shutdown = False
        self._monitor_task = asyncio.create_task(self._monitor_agents())

    async def stop(self) -> None:
        """Stop supervisor and all monitored agents."""
        logger.info("Stopping agent supervisor")
        self._shutdown = True

        # Stop monitoring
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

        # Stop all agents
        for agent_id, agent_process in self.agent_processes.items():
            try:
                await agent_process.stop()
            except Exception as e:
                logger.error(f"Error stopping agent '{agent_id}': {e}")

    async def _monitor_agents(self) -> None:
        """Monitor agent health and restart failed agents."""
        while not self._shutdown:
            try:
                await asyncio.sleep(5.0)  # Check every 5 seconds

                # Check each agent
                for agent_id, agent_process in list(self.agent_processes.items()):
                    if await self._is_agent_failed(agent_process):
                        logger.warning(f"Agent '{agent_id}' has failed")
                        await self._handle_agent_failure(agent_id)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in agent monitor: {e}", exc_info=True)

    async def _is_agent_failed(self, agent_process: AgentProcess) -> bool:
        """
        Check if agent has failed.

        Args:
            agent_process: AgentProcess to check

        Returns:
            True if agent is failed
        """
        if not hasattr(agent_process, 'process') or agent_process.process is None:
            return False

        # Check if task is done and raised an exception
        if agent_process.process.done():
            try:
                agent_process.process.result()
                # Completed normally
                return False
            except Exception:
                # Failed with exception
                return True

        return False

    async def _handle_agent_failure(self, failed_agent_id: str) -> None:
        """
        Handle agent failure according to supervision strategy.

        Args:
            failed_agent_id: ID of failed agent
        """
        group_name = self.agent_to_group.get(failed_agent_id)
        if not group_name:
            logger.error(f"Failed agent '{failed_agent_id}' not in any group")
            return

        group = self.groups[group_name]

        # Check restart policy
        restart_policy = self.agent_restart_policies.get(failed_agent_id)
        if restart_policy == RestartPolicy.TEMPORARY:
            logger.info(
                f"Not restarting agent '{failed_agent_id}' (TEMPORARY policy)"
            )
            return

        # Check if transient and exited normally
        # (For now, we assume all failures are abnormal)

        # Check restart intensity
        if not group.can_restart():
            logger.error(
                f"Restart intensity limit exceeded for group '{group_name}' "
                f"({group.restart_intensity} restarts in {group.restart_period}s)"
            )
            # TODO: Escalate to parent supervisor
            return

        # Record restart
        group.record_restart()

        # Determine which agents to restart
        agent_states = {}  # TODO: Get actual agent states
        to_restart = group.should_supervise_restart(failed_agent_id, agent_states)

        logger.info(
            f"Restarting agents {to_restart} due to failure of '{failed_agent_id}' "
            f"(strategy: {group.strategy.value})"
        )

        # Restart agents
        for agent_id in to_restart:
            await self._restart_agent(agent_id)

        # Record metrics
        if self.metrics:
            self.metrics.increment(
                "agent_restarts_total",
                labels={
                    "agent_id": failed_agent_id,
                    "group": group_name,
                    "strategy": group.strategy.value
                }
            )

    async def _restart_agent(self, agent_id: str) -> None:
        """
        Restart a specific agent.

        Args:
            agent_id: Agent to restart
        """
        agent_process = self.agent_processes.get(agent_id)
        if not agent_process:
            logger.error(f"Cannot restart agent '{agent_id}': not found")
            return

        try:
            # Stop agent
            logger.info(f"Stopping agent '{agent_id}'")
            await agent_process.stop()

            # Wait a bit before restarting
            await asyncio.sleep(1.0)

            # Restart agent
            logger.info(f"Restarting agent '{agent_id}'")
            await agent_process.start(recover=True)

            logger.info(f"Agent '{agent_id}' restarted successfully")

        except Exception as e:
            logger.error(f"Failed to restart agent '{agent_id}': {e}", exc_info=True)

            # Record failure metric
            if self.metrics:
                self.metrics.increment(
                    "agent_restart_failures_total",
                    labels={"agent_id": agent_id}
                )
```

### Verification

#### Create `tests/unit/test_supervision.py`

```python
"""Unit tests for agent supervision."""

import pytest
import asyncio
from dataclasses import dataclass
from unittest.mock import Mock, AsyncMock

from sabot.agents.supervisor import (
    AgentSupervisor,
    SupervisorConfig,
    SupervisionStrategy,
    RestartPolicy,
    SupervisionGroup
)


@dataclass
class MockAgentProcess:
    """Mock agent process for testing."""
    agent_id: str
    process: Optional[asyncio.Task] = None
    _failed: bool = False

    async def start(self, recover: bool = True):
        """Mock start."""
        pass

    async def stop(self):
        """Mock stop."""
        pass


class TestSupervisionGroup:
    """Test supervision group logic."""

    def test_one_for_one_strategy(self):
        """Test ONE_FOR_ONE only restarts failed agent."""
        group = SupervisionGroup(
            name="test-group",
            strategy=SupervisionStrategy.ONE_FOR_ONE,
            agents=["agent1", "agent2", "agent3"]
        )

        to_restart = group.should_supervise_restart("agent2", {})

        assert to_restart == ["agent2"]

    def test_one_for_all_strategy(self):
        """Test ONE_FOR_ALL restarts all agents."""
        group = SupervisionGroup(
            name="test-group",
            strategy=SupervisionStrategy.ONE_FOR_ALL,
            agents=["agent1", "agent2", "agent3"]
        )

        to_restart = group.should_supervise_restart("agent2", {})

        assert to_restart == ["agent1", "agent2", "agent3"]

    def test_rest_for_one_strategy(self):
        """Test REST_FOR_ONE restarts failed agent and those after it."""
        group = SupervisionGroup(
            name="test-group",
            strategy=SupervisionStrategy.REST_FOR_ONE,
            agents=["agent1", "agent2", "agent3", "agent4"]
        )

        to_restart = group.should_supervise_restart("agent2", {})

        assert to_restart == ["agent2", "agent3", "agent4"]

    def test_restart_intensity_limit(self):
        """Test restart intensity limiting."""
        group = SupervisionGroup(
            name="test-group",
            strategy=SupervisionStrategy.ONE_FOR_ONE,
            restart_intensity=3,
            restart_period=60.0
        )

        # Record 3 restarts
        for _ in range(3):
            group.record_restart()

        # Should not allow more restarts
        assert not group.can_restart()

        # After time passes, restarts should be allowed again
        # (This would require mocking time)


class TestAgentSupervisor:
    """Test agent supervisor."""

    @pytest.mark.asyncio
    async def test_create_supervision_group(self):
        """Test creating supervision groups."""
        supervisor = AgentSupervisor()

        group = supervisor.create_supervision_group(
            name="test-group",
            strategy=SupervisionStrategy.ONE_FOR_ALL
        )

        assert group.name == "test-group"
        assert group.strategy == SupervisionStrategy.ONE_FOR_ALL
        assert "test-group" in supervisor.groups

    @pytest.mark.asyncio
    async def test_add_agent_to_group(self):
        """Test adding agents to supervision groups."""
        supervisor = AgentSupervisor()

        agent_process = MockAgentProcess(agent_id="test-agent")

        supervisor.add_agent(
            agent_id="test-agent",
            agent_process=agent_process,
            group_name="test-group",
            restart_policy=RestartPolicy.PERMANENT
        )

        assert "test-agent" in supervisor.agent_processes
        assert "test-agent" in supervisor.agent_to_group
        assert supervisor.agent_to_group["test-agent"] == "test-group"

    @pytest.mark.asyncio
    async def test_temporary_agents_not_restarted(self):
        """Test TEMPORARY agents are not restarted."""
        supervisor = AgentSupervisor()

        agent_process = MockAgentProcess(agent_id="temp-agent")
        agent_process._failed = True

        supervisor.add_agent(
            agent_id="temp-agent",
            agent_process=agent_process,
            restart_policy=RestartPolicy.TEMPORARY
        )

        # Simulate failure
        # Agent should not be restarted
        # (This would require mocking the restart logic)
```

### CLI Integration

#### Update `sabot/cli.py` - Add Supervision Options

```python
@app.command()
def worker(
    app_spec: str = typer.Option(..., "-A", "--app", help="App module"),
    workers: int = typer.Option(1, "-c", "--concurrency"),
    supervision_strategy: str = typer.Option(
        "one_for_one",
        "--supervision",
        help="Supervision strategy (one_for_one, one_for_all, rest_for_one)"
    ),
    restart_intensity: int = typer.Option(3, "--restart-intensity"),
    restart_period: float = typer.Option(60.0, "--restart-period"),
    log_level: str = typer.Option("INFO", "-l", "--loglevel"),
):
    """Start Sabot worker with supervision."""
    import logging
    from sabot.agents.supervisor import (
        AgentSupervisor,
        SupervisorConfig,
        SupervisionStrategy
    )

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    try:
        # Load app
        app_instance = load_app_from_spec(app_spec)

        # Create supervisor
        strategy = SupervisionStrategy[supervision_strategy.upper()]

        supervisor_config = SupervisorConfig(
            default_strategy=strategy,
            default_restart_intensity=restart_intensity,
            default_restart_period=restart_period
        )

        supervisor = AgentSupervisor(config=supervisor_config)

        # Add agents to supervisor
        for agent_id, agent_spec in app_instance._agents.items():
            # Create agent process
            agent_process = create_agent_process(agent_spec)

            supervisor.add_agent(
                agent_id=agent_id,
                agent_process=agent_process
            )

        # Start supervisor
        asyncio.run(supervisor.start())

        console.print("[green]Worker started with supervision[/green]")

    except KeyboardInterrupt:
        console.print("\n[dim]Worker stopped by user[/dim]")
        raise typer.Exit(code=0)
```

---

## Step 9.2: Implement Resource Monitoring

### Overview

Monitor resource usage (CPU, memory) and enforce limits to prevent runaway agents from consuming all system resources.

### Implementation

#### Complete `sabot/agents/resources.py`

```python
#!/usr/bin/env python3
"""
Resource Management for Sabot Agents

Implements resource isolation and limits:
- Memory limits and monitoring
- CPU limits and throttling
- Backpressure detection
- Resource quotas and enforcement
"""

import asyncio
import logging
import psutil
import os
from typing import Dict, Optional
from dataclasses import dataclass, field
from enum import Enum
import time

from ..core.metrics import MetricsCollector

logger = logging.getLogger(__name__)


class BackpressureSignal(Enum):
    """Backpressure signals."""
    NONE = "none"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class ResourceLimits:
    """Resource limits for an agent."""
    memory_mb: Optional[int] = None
    cpu_percent: Optional[float] = None

    def is_empty(self) -> bool:
        """Check if no limits are set."""
        return self.memory_mb is None and self.cpu_percent is None


@dataclass
class ResourceUsage:
    """Current resource usage."""
    memory_mb: float = 0.0
    memory_percent: float = 0.0
    cpu_percent: float = 0.0
    threads: int = 0
    timestamp: float = field(default_factory=time.time)

    @property
    def is_stale(self) -> bool:
        """Check if usage data is stale (older than 30 seconds)."""
        return time.time() - self.timestamp > 30.0


class ResourceMonitor:
    """
    Monitor resource usage for agents.

    Tracks:
    - Memory usage (MB and %)
    - CPU usage (%)
    - Thread count
    - Backpressure signals
    """

    def __init__(self, poll_interval: float = 5.0):
        self.poll_interval = poll_interval
        self.process = psutil.Process(os.getpid())

        self.agent_limits: Dict[str, ResourceLimits] = {}
        self.agent_usage: Dict[str, ResourceUsage] = {}

        self.metrics: Optional[MetricsCollector] = None
        self._shutdown = False
        self._monitor_task: Optional[asyncio.Task] = None

    def set_agent_limits(self, agent_id: str, limits: ResourceLimits) -> None:
        """
        Set resource limits for an agent.

        Args:
            agent_id: Agent identifier
            limits: Resource limits
        """
        self.agent_limits[agent_id] = limits
        logger.info(
            f"Set resource limits for agent '{agent_id}': "
            f"memory={limits.memory_mb}MB, cpu={limits.cpu_percent}%"
        )

    async def start(self, metrics: Optional[MetricsCollector] = None) -> None:
        """Start resource monitoring."""
        self.metrics = metrics
        self._shutdown = False
        self._monitor_task = asyncio.create_task(self._monitoring_loop())
        logger.info("Resource monitor started")

    async def stop(self) -> None:
        """Stop resource monitoring."""
        self._shutdown = True
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("Resource monitor stopped")

    async def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        while not self._shutdown:
            try:
                await asyncio.sleep(self.poll_interval)
                await self._collect_metrics()
                await self._check_limits()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in resource monitoring: {e}", exc_info=True)

    async def _collect_metrics(self) -> None:
        """Collect resource usage metrics."""
        try:
            # Get process-wide metrics
            memory_info = self.process.memory_info()
            cpu_percent = self.process.cpu_percent(interval=0.1)
            num_threads = self.process.num_threads()

            memory_mb = memory_info.rss / (1024 * 1024)
            memory_percent = self.process.memory_percent()

            # Store usage (simplified - in reality, per-agent tracking needs more work)
            usage = ResourceUsage(
                memory_mb=memory_mb,
                memory_percent=memory_percent,
                cpu_percent=cpu_percent,
                threads=num_threads,
                timestamp=time.time()
            )

            # For now, use process-wide metrics for all agents
            # TODO: Implement per-agent resource tracking
            for agent_id in self.agent_limits:
                self.agent_usage[agent_id] = usage

            # Export to metrics
            if self.metrics:
                self.metrics.set_gauge(
                    "agent_memory_mb",
                    memory_mb,
                    labels={"agent_id": "system"}
                )
                self.metrics.set_gauge(
                    "agent_cpu_percent",
                    cpu_percent,
                    labels={"agent_id": "system"}
                )
                self.metrics.set_gauge(
                    "agent_threads",
                    num_threads,
                    labels={"agent_id": "system"}
                )

        except Exception as e:
            logger.error(f"Error collecting resource metrics: {e}")

    async def _check_limits(self) -> None:
        """Check if resource limits are exceeded."""
        for agent_id, limits in self.agent_limits.items():
            usage = self.agent_usage.get(agent_id)
            if not usage or usage.is_stale:
                continue

            # Check memory limit
            if limits.memory_mb and usage.memory_mb > limits.memory_mb:
                logger.warning(
                    f"Agent '{agent_id}' exceeded memory limit: "
                    f"{usage.memory_mb:.1f}MB > {limits.memory_mb}MB"
                )

                if self.metrics:
                    self.metrics.increment(
                        "agent_limit_violations_total",
                        labels={"agent_id": agent_id, "limit_type": "memory"}
                    )

            # Check CPU limit
            if limits.cpu_percent and usage.cpu_percent > limits.cpu_percent:
                logger.warning(
                    f"Agent '{agent_id}' exceeded CPU limit: "
                    f"{usage.cpu_percent:.1f}% > {limits.cpu_percent}%"
                )

                if self.metrics:
                    self.metrics.increment(
                        "agent_limit_violations_total",
                        labels={"agent_id": agent_id, "limit_type": "cpu"}
                    )

    def get_usage(self, agent_id: str) -> Optional[ResourceUsage]:
        """
        Get current resource usage for an agent.

        Args:
            agent_id: Agent identifier

        Returns:
            ResourceUsage or None if not available
        """
        return self.agent_usage.get(agent_id)

    def get_backpressure_signal(self, agent_id: str) -> BackpressureSignal:
        """
        Get backpressure signal based on resource usage.

        Args:
            agent_id: Agent identifier

        Returns:
            BackpressureSignal level
        """
        usage = self.get_usage(agent_id)
        if not usage:
            return BackpressureSignal.NONE

        limits = self.agent_limits.get(agent_id)
        if not limits or limits.is_empty():
            return BackpressureSignal.NONE

        # Calculate pressure based on memory usage
        if limits.memory_mb:
            memory_ratio = usage.memory_mb / limits.memory_mb

            if memory_ratio > 0.95:
                return BackpressureSignal.CRITICAL
            elif memory_ratio > 0.85:
                return BackpressureSignal.HIGH
            elif memory_ratio > 0.70:
                return BackpressureSignal.MEDIUM
            elif memory_ratio > 0.50:
                return BackpressureSignal.LOW

        # Calculate pressure based on CPU usage
        if limits.cpu_percent:
            cpu_ratio = usage.cpu_percent / limits.cpu_percent

            if cpu_ratio > 0.95:
                return BackpressureSignal.CRITICAL
            elif cpu_ratio > 0.85:
                return BackpressureSignal.HIGH
            elif cpu_ratio > 0.70:
                return BackpressureSignal.MEDIUM
            elif cpu_ratio > 0.50:
                return BackpressureSignal.LOW

        return BackpressureSignal.NONE
```

### Verification

```python
# Test resource monitoring
import asyncio
from sabot.agents.resources import ResourceMonitor, ResourceLimits

async def test_monitoring():
    monitor = ResourceMonitor(poll_interval=2.0)

    # Set limits
    monitor.set_agent_limits(
        agent_id="test-agent",
        limits=ResourceLimits(memory_mb=512, cpu_percent=50.0)
    )

    # Start monitoring
    await monitor.start()

    # Let it run for 10 seconds
    await asyncio.sleep(10)

    # Get usage
    usage = monitor.get_usage("test-agent")
    print(f"Memory: {usage.memory_mb:.1f}MB")
    print(f"CPU: {usage.cpu_percent:.1f}%")

    # Get backpressure
    pressure = monitor.get_backpressure_signal("test-agent")
    print(f"Backpressure: {pressure.value}")

    # Stop
    await monitor.stop()

asyncio.run(test_monitoring())
```

---

## Step 9.3: Add Metrics Collection

### Overview

Complete the metrics collection system for observability and monitoring.

### Implementation

#### Complete `sabot/core/metrics.py`

```python
#!/usr/bin/env python3
"""
Metrics Collection and Monitoring for Sabot

Provides comprehensive metrics collection for:
- Throughput monitoring
- Latency tracking
- Error rates
- Resource usage
- Checkpoint metrics
- Prometheus integration
"""

import time
import logging
import asyncio
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict, deque
import statistics

try:
    from prometheus_client import (
        Counter,
        Gauge,
        Histogram,
        Summary,
        CollectorRegistry,
        generate_latest,
        push_to_gateway
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("prometheus_client not available, metrics export disabled")

logger = logging.getLogger(__name__)


@dataclass
class MetricPoint:
    """Individual metric measurement point."""
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class MetricSeries:
    """Time series data for a metric."""
    name: str
    points: deque = field(default_factory=lambda: deque(maxlen=1000))

    def add_point(self, value: float, labels: Dict[str, str] = None) -> None:
        """Add a measurement point."""
        point = MetricPoint(
            timestamp=time.time(),
            value=value,
            labels=labels or {}
        )
        self.points.append(point)

    def get_latest(self) -> Optional[float]:
        """Get the latest value."""
        return self.points[-1].value if self.points else None

    def get_average(self, window_seconds: float = 60.0) -> Optional[float]:
        """Get average over time window."""
        if not self.points:
            return None

        cutoff = time.time() - window_seconds
        recent_points = [p.value for p in self.points if p.timestamp >= cutoff]

        return statistics.mean(recent_points) if recent_points else None


class MetricsCollector:
    """
    Comprehensive metrics collection system for Sabot.

    Collects and exports:
    - Agent throughput (events/sec)
    - Processing latency (p50, p95, p99)
    - Error rates
    - Checkpoint metrics
    - Resource usage
    """

    def __init__(self, enable_prometheus: bool = True):
        self.enable_prometheus = enable_prometheus and PROMETHEUS_AVAILABLE

        # In-memory metric series
        self.series: Dict[str, MetricSeries] = {}

        # Prometheus metrics
        if self.enable_prometheus:
            self.registry = CollectorRegistry()
            self._init_prometheus_metrics()
        else:
            self.registry = None

    def _init_prometheus_metrics(self) -> None:
        """Initialize Prometheus metrics."""
        if not self.enable_prometheus:
            return

        # Counters
        self.prom_events_processed = Counter(
            'sabot_events_processed_total',
            'Total events processed',
            ['agent_id', 'topic'],
            registry=self.registry
        )

        self.prom_errors = Counter(
            'sabot_errors_total',
            'Total errors',
            ['agent_id', 'error_type'],
            registry=self.registry
        )

        self.prom_checkpoints = Counter(
            'sabot_checkpoints_total',
            'Total checkpoints created',
            ['agent_id'],
            registry=self.registry
        )

        # Gauges
        self.prom_memory_mb = Gauge(
            'sabot_memory_mb',
            'Memory usage in MB',
            ['agent_id'],
            registry=self.registry
        )

        self.prom_cpu_percent = Gauge(
            'sabot_cpu_percent',
            'CPU usage percentage',
            ['agent_id'],
            registry=self.registry
        )

        self.prom_watermark = Gauge(
            'sabot_watermark_timestamp',
            'Current watermark timestamp',
            ['agent_id', 'partition'],
            registry=self.registry
        )

        # Histograms
        self.prom_latency = Histogram(
            'sabot_processing_latency_seconds',
            'Event processing latency',
            ['agent_id'],
            registry=self.registry,
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
        )

        self.prom_checkpoint_duration = Histogram(
            'sabot_checkpoint_duration_seconds',
            'Checkpoint creation duration',
            ['agent_id'],
            registry=self.registry,
            buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]
        )

    def increment(self, metric_name: str, value: float = 1.0, labels: Dict[str, str] = None) -> None:
        """
        Increment a counter metric.

        Args:
            metric_name: Metric name
            value: Value to increment by
            labels: Optional labels
        """
        # Add to in-memory series
        if metric_name not in self.series:
            self.series[metric_name] = MetricSeries(name=metric_name)

        current = self.series[metric_name].get_latest() or 0.0
        self.series[metric_name].add_point(current + value, labels)

        # Update Prometheus
        if self.enable_prometheus:
            prom_metric = self._get_prometheus_metric(metric_name)
            if prom_metric and hasattr(prom_metric, 'inc'):
                if labels:
                    prom_metric.labels(**labels).inc(value)
                else:
                    prom_metric.inc(value)

    def set_gauge(self, metric_name: str, value: float, labels: Dict[str, str] = None) -> None:
        """
        Set a gauge metric.

        Args:
            metric_name: Metric name
            value: Value to set
            labels: Optional labels
        """
        # Add to in-memory series
        if metric_name not in self.series:
            self.series[metric_name] = MetricSeries(name=metric_name)

        self.series[metric_name].add_point(value, labels)

        # Update Prometheus
        if self.enable_prometheus:
            prom_metric = self._get_prometheus_metric(metric_name)
            if prom_metric and hasattr(prom_metric, 'set'):
                if labels:
                    prom_metric.labels(**labels).set(value)
                else:
                    prom_metric.set(value)

    def observe(self, metric_name: str, value: float, labels: Dict[str, str] = None) -> None:
        """
        Observe a histogram metric.

        Args:
            metric_name: Metric name
            value: Value to observe
            labels: Optional labels
        """
        # Add to in-memory series
        if metric_name not in self.series:
            self.series[metric_name] = MetricSeries(name=metric_name)

        self.series[metric_name].add_point(value, labels)

        # Update Prometheus
        if self.enable_prometheus:
            prom_metric = self._get_prometheus_metric(metric_name)
            if prom_metric and hasattr(prom_metric, 'observe'):
                if labels:
                    prom_metric.labels(**labels).observe(value)
                else:
                    prom_metric.observe(value)

    def _get_prometheus_metric(self, metric_name: str):
        """Get Prometheus metric by name."""
        if not self.enable_prometheus:
            return None

        # Map metric names to Prometheus metrics
        metric_map = {
            'events_processed_total': self.prom_events_processed,
            'errors_total': self.prom_errors,
            'checkpoints_total': self.prom_checkpoints,
            'agent_memory_mb': self.prom_memory_mb,
            'agent_cpu_percent': self.prom_cpu_percent,
            'agent_watermark': self.prom_watermark,
            'processing_latency_seconds': self.prom_latency,
            'checkpoint_duration_seconds': self.prom_checkpoint_duration,
        }

        return metric_map.get(metric_name)

    def get_metrics_text(self) -> str:
        """
        Get Prometheus metrics in text format.

        Returns:
            Metrics in Prometheus exposition format
        """
        if not self.enable_prometheus:
            return ""

        return generate_latest(self.registry).decode('utf-8')

    def push_to_gateway(self, gateway_url: str, job_name: str) -> None:
        """
        Push metrics to Prometheus Pushgateway.

        Args:
            gateway_url: Pushgateway URL
            job_name: Job name for grouping
        """
        if not self.enable_prometheus:
            logger.warning("Prometheus not available, cannot push metrics")
            return

        try:
            push_to_gateway(gateway_url, job=job_name, registry=self.registry)
            logger.debug(f"Pushed metrics to {gateway_url}")
        except Exception as e:
            logger.error(f"Failed to push metrics to gateway: {e}")

    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary of all metrics.

        Returns:
            Dict of metric name -> latest value
        """
        summary = {}
        for name, series in self.series.items():
            summary[name] = {
                'latest': series.get_latest(),
                'avg_60s': series.get_average(60.0)
            }
        return summary
```

### HTTP Endpoint for Metrics

#### Add to `sabot/app.py`

```python
def metrics_endpoint(self):
    """
    Get Prometheus metrics endpoint handler.

    Usage:
        from flask import Flask
        flask_app = Flask(__name__)

        @flask_app.route('/metrics')
        def metrics():
            return app.metrics_endpoint()
    """
    if not hasattr(self, 'metrics') or self.metrics is None:
        return "Metrics not enabled", 404

    return self.metrics.get_metrics_text(), 200, {'Content-Type': 'text/plain'}
```

### Verification

```python
# Test metrics collection
from sabot.core.metrics import MetricsCollector

metrics = MetricsCollector(enable_prometheus=True)

# Record some metrics
metrics.increment('events_processed_total', labels={'agent_id': 'test', 'topic': 'events'})
metrics.set_gauge('agent_memory_mb', 256.5, labels={'agent_id': 'test'})
metrics.observe('processing_latency_seconds', 0.025, labels={'agent_id': 'test'})

# Get Prometheus text
print(metrics.get_metrics_text())

# Get summary
print(metrics.get_summary())
```

---

## Step 9.4: Add Savepoints

### Overview

Implement savepoints - manually triggered checkpoints with retention policies.

### Implementation

#### Create `sabot/checkpoint/savepoints.py`

```python
"""
Savepoint Management for Sabot

Savepoints are manually-triggered checkpoints with retention policies:
- Create savepoint on demand
- Resume from specific savepoint
- List available savepoints
- Delete old savepoints
"""

import logging
from typing import Dict, List, Optional
from dataclasses import dataclass
from pathlib import Path
import json
import shutil

from sabot.checkpoint import CheckpointStorage, CheckpointRecovery

logger = logging.getLogger(__name__)


@dataclass
class SavepointMetadata:
    """Metadata for a savepoint."""
    savepoint_id: int
    timestamp: float
    description: str
    agent_ids: List[str]
    size_bytes: int
    path: str


class SavepointManager:
    """
    Manage savepoints for Sabot applications.

    Savepoints differ from checkpoints:
    - Manually triggered (not automatic)
    - Retained indefinitely (or until manually deleted)
    - Can have descriptions/tags
    - Used for upgrades, testing, rollback
    """

    def __init__(self, savepoint_dir: str):
        self.savepoint_dir = Path(savepoint_dir)
        self.savepoint_dir.mkdir(parents=True, exist_ok=True)

        self.metadata_file = self.savepoint_dir / "savepoints.json"
        self.savepoints: Dict[int, SavepointMetadata] = {}

        self._load_metadata()

    def _load_metadata(self) -> None:
        """Load savepoint metadata from disk."""
        if not self.metadata_file.exists():
            return

        try:
            with open(self.metadata_file, 'r') as f:
                data = json.load(f)

            for sp_data in data.get('savepoints', []):
                metadata = SavepointMetadata(**sp_data)
                self.savepoints[metadata.savepoint_id] = metadata

            logger.info(f"Loaded {len(self.savepoints)} savepoint(s)")
        except Exception as e:
            logger.error(f"Failed to load savepoint metadata: {e}")

    def _save_metadata(self) -> None:
        """Save savepoint metadata to disk."""
        try:
            data = {
                'savepoints': [
                    {
                        'savepoint_id': sp.savepoint_id,
                        'timestamp': sp.timestamp,
                        'description': sp.description,
                        'agent_ids': sp.agent_ids,
                        'size_bytes': sp.size_bytes,
                        'path': sp.path
                    }
                    for sp in self.savepoints.values()
                ]
            }

            with open(self.metadata_file, 'w') as f:
                json.dump(data, f, indent=2)

        except Exception as e:
            logger.error(f"Failed to save savepoint metadata: {e}")

    def create_savepoint(
        self,
        checkpoint_id: int,
        checkpoint_dir: str,
        description: str = "",
        agent_ids: Optional[List[str]] = None
    ) -> SavepointMetadata:
        """
        Create a savepoint from a checkpoint.

        Args:
            checkpoint_id: Checkpoint ID to create savepoint from
            checkpoint_dir: Directory containing checkpoints
            description: Optional description
            agent_ids: List of agent IDs included

        Returns:
            SavepointMetadata
        """
        import time

        # Generate savepoint ID
        savepoint_id = int(time.time() * 1000)

        # Copy checkpoint to savepoint directory
        checkpoint_path = Path(checkpoint_dir) / f"checkpoint_{checkpoint_id}.bin"
        if not checkpoint_path.exists():
            raise FileNotFoundError(f"Checkpoint {checkpoint_id} not found")

        savepoint_path = self.savepoint_dir / f"savepoint_{savepoint_id}.bin"
        shutil.copy2(checkpoint_path, savepoint_path)

        # Get size
        size_bytes = savepoint_path.stat().st_size

        # Create metadata
        metadata = SavepointMetadata(
            savepoint_id=savepoint_id,
            timestamp=time.time(),
            description=description,
            agent_ids=agent_ids or [],
            size_bytes=size_bytes,
            path=str(savepoint_path)
        )

        self.savepoints[savepoint_id] = metadata
        self._save_metadata()

        logger.info(f"Created savepoint {savepoint_id}: {description}")

        return metadata

    def list_savepoints(self) -> List[SavepointMetadata]:
        """
        List all available savepoints.

        Returns:
            List of SavepointMetadata, sorted by timestamp (newest first)
        """
        return sorted(
            self.savepoints.values(),
            key=lambda sp: sp.timestamp,
            reverse=True
        )

    def get_savepoint(self, savepoint_id: int) -> Optional[SavepointMetadata]:
        """
        Get metadata for a specific savepoint.

        Args:
            savepoint_id: Savepoint ID

        Returns:
            SavepointMetadata or None
        """
        return self.savepoints.get(savepoint_id)

    def delete_savepoint(self, savepoint_id: int) -> None:
        """
        Delete a savepoint.

        Args:
            savepoint_id: Savepoint ID to delete
        """
        if savepoint_id not in self.savepoints:
            raise ValueError(f"Savepoint {savepoint_id} not found")

        metadata = self.savepoints[savepoint_id]

        # Delete file
        savepoint_path = Path(metadata.path)
        if savepoint_path.exists():
            savepoint_path.unlink()

        # Remove from metadata
        del self.savepoints[savepoint_id]
        self._save_metadata()

        logger.info(f"Deleted savepoint {savepoint_id}")

    def restore_from_savepoint(
        self,
        savepoint_id: int,
        target_checkpoint_dir: str
    ) -> int:
        """
        Restore a savepoint as a checkpoint for recovery.

        Args:
            savepoint_id: Savepoint ID to restore
            target_checkpoint_dir: Directory to restore checkpoint to

        Returns:
            Checkpoint ID created
        """
        import time

        if savepoint_id not in self.savepoints:
            raise ValueError(f"Savepoint {savepoint_id} not found")

        metadata = self.savepoints[savepoint_id]

        # Generate new checkpoint ID
        checkpoint_id = int(time.time() * 1000)

        # Copy savepoint to checkpoint directory
        savepoint_path = Path(metadata.path)
        checkpoint_path = Path(target_checkpoint_dir) / f"checkpoint_{checkpoint_id}.bin"

        Path(target_checkpoint_dir).mkdir(parents=True, exist_ok=True)
        shutil.copy2(savepoint_path, checkpoint_path)

        logger.info(
            f"Restored savepoint {savepoint_id} as checkpoint {checkpoint_id}"
        )

        return checkpoint_id
```

### CLI Commands for Savepoints

#### Add to `sabot/cli.py`

```python
@app.command()
def savepoint_create(
    app_spec: str = typer.Option(..., "-A", "--app"),
    description: str = typer.Option("", "--description", "-d"),
    savepoint_dir: str = typer.Option("/tmp/sabot/savepoints", "--savepoint-dir")
):
    """Create a savepoint from latest checkpoint."""
    from sabot.checkpoint.savepoints import SavepointManager

    try:
        # Load app
        app_instance = load_app_from_spec(app_spec)

        # Get latest checkpoint
        checkpoint_dir = f"/tmp/sabot/checkpoints/{app_instance.id}"
        storage = CheckpointStorage(checkpoint_dir=checkpoint_dir)
        checkpoints = storage.list_checkpoints()

        if not checkpoints:
            console.print("[red]No checkpoints found[/red]")
            raise typer.Exit(code=1)

        latest_checkpoint = checkpoints[-1]

        # Create savepoint
        manager = SavepointManager(savepoint_dir=savepoint_dir)
        metadata = manager.create_savepoint(
            checkpoint_id=latest_checkpoint,
            checkpoint_dir=checkpoint_dir,
            description=description
        )

        console.print(f"[green]Savepoint created:[/green] {metadata.savepoint_id}")
        console.print(f"Description: {metadata.description}")
        console.print(f"Size: {metadata.size_bytes / 1024:.1f} KB")

    except Exception as e:
        console.print(f"[red]Error creating savepoint:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def savepoint_list(
    savepoint_dir: str = typer.Option("/tmp/sabot/savepoints", "--savepoint-dir")
):
    """List all available savepoints."""
    from sabot.checkpoint.savepoints import SavepointManager
    from rich.table import Table

    manager = SavepointManager(savepoint_dir=savepoint_dir)
    savepoints = manager.list_savepoints()

    if not savepoints:
        console.print("[yellow]No savepoints found[/yellow]")
        return

    table = Table(title="Savepoints")
    table.add_column("ID", style="cyan")
    table.add_column("Timestamp")
    table.add_column("Description")
    table.add_column("Size")

    for sp in savepoints:
        from datetime import datetime
        timestamp = datetime.fromtimestamp(sp.timestamp).strftime("%Y-%m-%d %H:%M:%S")
        size_kb = sp.size_bytes / 1024

        table.add_row(
            str(sp.savepoint_id),
            timestamp,
            sp.description or "(no description)",
            f"{size_kb:.1f} KB"
        )

    console.print(table)


@app.command()
def savepoint_restore(
    savepoint_id: int = typer.Argument(..., help="Savepoint ID to restore"),
    app_spec: str = typer.Option(..., "-A", "--app"),
    savepoint_dir: str = typer.Option("/tmp/sabot/savepoints", "--savepoint-dir")
):
    """Restore from a savepoint."""
    from sabot.checkpoint.savepoints import SavepointManager

    try:
        # Load app
        app_instance = load_app_from_spec(app_spec)

        # Restore savepoint
        manager = SavepointManager(savepoint_dir=savepoint_dir)
        checkpoint_dir = f"/tmp/sabot/checkpoints/{app_instance.id}"

        checkpoint_id = manager.restore_from_savepoint(
            savepoint_id=savepoint_id,
            target_checkpoint_dir=checkpoint_dir
        )

        console.print(f"[green]Savepoint {savepoint_id} restored as checkpoint {checkpoint_id}[/green]")
        console.print("Start worker with --recover to use this savepoint")

    except Exception as e:
        console.print(f"[red]Error restoring savepoint:[/red] {e}")
        raise typer.Exit(code=1)
```

### Verification

```bash
# Create a savepoint
sabot -A examples.fraud_app:app savepoint-create --description "Before upgrade"

# List savepoints
sabot savepoint-list

# Restore from savepoint
sabot -A examples.fraud_app:app savepoint-restore 1696348800000

# Start worker from savepoint
sabot -A examples.fraud_app:app worker --recover
```

---

## Phase 9 Completion Criteria

### Implementation Complete

- [ ] Supervision strategies implemented (ONE_FOR_ONE, ONE_FOR_ALL, REST_FOR_ONE)
- [ ] Supervisor integrated with agent runtime
- [ ] Resource monitoring working (CPU, memory, backpressure)
- [ ] Metrics collection complete with Prometheus export
- [ ] Savepoint creation and restoration working

### Tests Created

- [ ] `tests/unit/test_supervision.py` - Supervision strategy tests
- [ ] `tests/unit/test_resource_monitoring.py` - Resource monitoring tests
- [ ] `tests/unit/test_metrics.py` - Metrics collection tests
- [ ] `tests/unit/test_savepoints.py` - Savepoint management tests

### CLI Commands Added

- [ ] `sabot worker --supervision <strategy>` - Start with supervision
- [ ] `sabot savepoint-create` - Create savepoint
- [ ] `sabot savepoint-list` - List savepoints
- [ ] `sabot savepoint-restore` - Restore from savepoint

### Documentation

- [ ] Supervision strategies documented
- [ ] Resource limits configuration guide
- [ ] Metrics export setup guide
- [ ] Savepoint usage guide

### Coverage Goals

- Supervision: 60%+
- Resource monitoring: 60%+
- Metrics: 70%+
- Savepoints: 70%+

---

## Dependencies

**Required from Previous Phases:**
- Phase 2: Checkpoint coordination working
- Phase 6: Checkpoint recovery working
- Agent runtime functional

**External Dependencies:**
- `psutil` for resource monitoring
- `prometheus_client` for metrics export (optional)

---

## Next Steps

After Phase 9 is complete:

1. **Performance Optimization** (if needed)
   - Profile hot paths
   - Optimize Cython modules
   - Tune batch sizes

2. **Production Hardening**
   - Add more error handling
   - Improve logging
   - Add health checks

3. **Additional Features** (optional)
   - Web UI for monitoring
   - SQL query interface
   - Additional connectors
   - CEP (Complex Event Processing)

---

**Last Updated:** October 3, 2025
**Status:** Technical specification for Phase 9
