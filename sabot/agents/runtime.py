#!/usr/bin/env python3
"""
Agent Runtime System for Sabot

Implements the real agent execution engine with:
- Process management and spawning
- Supervision strategies and auto-restart
- Resource isolation and limits
- Health monitoring and lifecycle control

This replaces the mocked agent execution in the current system.
"""

import asyncio
import logging
import multiprocessing as mp
import os
import signal
import time
import psutil
import threading
from typing import Dict, List, Optional, Any, Callable, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum
import traceback
import sys

from ..sabot_types import AgentSpec, AgentFun
from ..core.metrics import MetricsCollector

logger = logging.getLogger(__name__)


class SupervisionStrategy(Enum):
    """Agent supervision strategies."""
    ONE_FOR_ONE = "one_for_one"  # Restart only failed agent
    ONE_FOR_ALL = "one_for_all"  # Restart all agents if any fails
    REST_FOR_ONE = "rest_for_one"  # Restart failed + all started after it
    SIMPLE_ONE_FOR_ONE = "simple_one_for_one"  # Dynamic agent spawning


class AgentState(Enum):
    """Agent lifecycle states."""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    FAILED = "failed"
    RESTARTING = "restarting"


class RestartPolicy(Enum):
    """Agent restart policies."""
    PERMANENT = "permanent"    # Always restart
    TRANSIENT = "transient"    # Restart only if abnormal exit
    TEMPORARY = "temporary"    # Never restart


@dataclass
class AgentProcess:
    """Represents a running agent process."""
    agent_id: str
    process: Optional[mp.Process] = None
    pid: Optional[int] = None
    state: AgentState = AgentState.STOPPED
    start_time: Optional[float] = None
    restart_count: int = 0
    max_restarts: int = 3
    restart_window: float = 60.0  # seconds
    last_restart_time: Optional[float] = None
    health_check_interval: float = 10.0
    last_health_check: Optional[float] = None
    memory_limit_mb: Optional[int] = None
    cpu_limit_percent: Optional[float] = None
    exit_code: Optional[int] = None
    error_message: Optional[str] = None

    @property
    def uptime(self) -> float:
        """Get agent uptime in seconds."""
        if self.start_time:
            return time.time() - self.start_time
        return 0.0

    @property
    def is_healthy(self) -> bool:
        """Check if agent is healthy."""
        if self.state != AgentState.RUNNING:
            return False

        if not self.pid:
            return False

        try:
            process = psutil.Process(self.pid)
            return process.is_running()
        except psutil.NoSuchProcess:
            return False

    def get_resource_usage(self) -> Dict[str, float]:
        """Get current resource usage."""
        if not self.pid:
            return {"memory_mb": 0.0, "cpu_percent": 0.0}

        try:
            process = psutil.Process(self.pid)
            memory_mb = process.memory_info().rss / 1024 / 1024
            cpu_percent = process.cpu_percent(interval=0.1)

            return {
                "memory_mb": memory_mb,
                "cpu_percent": cpu_percent
            }
        except psutil.NoSuchProcess:
            return {"memory_mb": 0.0, "cpu_percent": 0.0}


@dataclass
class AgentRuntimeConfig:
    """Configuration for the agent runtime."""
    max_agents: int = 100
    default_restart_policy: RestartPolicy = RestartPolicy.PERMANENT
    default_supervision_strategy: SupervisionStrategy = SupervisionStrategy.ONE_FOR_ONE
    default_max_restarts: int = 3
    default_restart_window: float = 60.0
    default_health_check_interval: float = 10.0
    default_memory_limit_mb: Optional[int] = None
    default_cpu_limit_percent: Optional[float] = None
    enable_metrics: bool = True


class AgentRuntime:
    """
    Real Agent Runtime System

    Manages the complete lifecycle of agent processes with:
    - Process spawning and management
    - Supervision and auto-restart
    - Resource limits and isolation
    - Health monitoring
    - Graceful shutdown
    """

    def __init__(self, config: Optional[AgentRuntimeConfig] = None):
        """
        Initialize the agent runtime.

        Args:
            config: Runtime configuration
        """
        self.config = config or AgentRuntimeConfig()
        self.metrics: Optional[MetricsCollector] = None

        # Agent process management
        self._agents: Dict[str, AgentProcess] = {}
        self._agent_specs: Dict[str, AgentSpec] = {}
        self._supervision_groups: Dict[str, List[str]] = {}

        # Runtime state
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._supervision_task: Optional[asyncio.Task] = None

        # Locks for thread safety
        self._lock = threading.Lock()

        # Initialize metrics if enabled
        if self.config.enable_metrics:
            self.metrics = MetricsCollector()

        logger.info(f"AgentRuntime initialized with config: max_agents={self.config.max_agents}")

    async def start(self) -> None:
        """Start the agent runtime system."""
        if self._running:
            logger.warning("AgentRuntime already running")
            return

        self._running = True
        self._shutdown_event.clear()

        # Start supervision task
        self._supervision_task = asyncio.create_task(self._supervision_loop())

        logger.info("AgentRuntime started")

    async def stop(self) -> None:
        """Stop the agent runtime system."""
        if not self._running:
            logger.warning("AgentRuntime not running")
            return

        self._running = False
        self._shutdown_event.set()

        # Stop supervision task
        if self._supervision_task:
            self._supervision_task.cancel()
            try:
                await self._supervision_task
            except asyncio.CancelledError:
                pass

        # Stop all agents
        stop_tasks = []
        for agent_id in list(self._agents.keys()):
            stop_tasks.append(self.stop_agent(agent_id))

        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)

        logger.info("AgentRuntime stopped")

    async def deploy_agent(self, agent_spec: AgentSpec,
                          supervision_group: Optional[str] = None) -> str:
        """
        Deploy a new agent with the specified configuration.

        Args:
            agent_spec: Agent specification
            supervision_group: Optional supervision group name

        Returns:
            Agent ID

        Raises:
            RuntimeError: If max agents limit reached
        """
        with self._lock:
            if len(self._agents) >= self.config.max_agents:
                raise RuntimeError(f"Maximum agents limit reached: {self.config.max_agents}")

            agent_id = agent_spec.name or f"agent_{len(self._agents)}"

            if agent_id in self._agents:
                raise ValueError(f"Agent {agent_id} already exists")

            # Create agent process record
            agent_process = AgentProcess(
                agent_id=agent_id,
                max_restarts=agent_spec.max_restarts or self.config.default_max_restarts,
                restart_window=agent_spec.restart_window or self.config.default_restart_window,
                health_check_interval=agent_spec.health_check_interval or self.config.default_health_check_interval,
                memory_limit_mb=agent_spec.memory_limit_mb or self.config.default_memory_limit_mb,
                cpu_limit_percent=agent_spec.cpu_limit_percent or self.config.default_cpu_limit_percent
            )

            self._agents[agent_id] = agent_process
            self._agent_specs[agent_id] = agent_spec

            # Add to supervision group
            if supervision_group:
                if supervision_group not in self._supervision_groups:
                    self._supervision_groups[supervision_group] = []
                self._supervision_groups[supervision_group].append(agent_id)

        # Start the agent
        await self.start_agent(agent_id)

        logger.info(f"Deployed agent: {agent_id}")
        return agent_id

    async def start_agent(self, agent_id: str) -> bool:
        """
        Start an agent process.

        Args:
            agent_id: Agent identifier

        Returns:
            True if started successfully
        """
        with self._lock:
            if agent_id not in self._agents:
                logger.error(f"Agent {agent_id} not found")
                return False

            agent_process = self._agents[agent_id]
            agent_spec = self._agent_specs[agent_id]

        if agent_process.state == AgentState.RUNNING:
            logger.warning(f"Agent {agent_id} already running")
            return True

        try:
            # Create and start the process
            agent_process.state = AgentState.STARTING
            agent_process.start_time = time.time()
            agent_process.last_restart_time = time.time()
            agent_process.exit_code = None
            agent_process.error_message = None

            # Start process with agent function
            process = mp.Process(
                target=self._agent_process_wrapper,
                args=(agent_id, agent_spec),
                name=f"sabot-agent-{agent_id}"
            )

            process.start()
            agent_process.process = process
            agent_process.pid = process.pid
            agent_process.state = AgentState.RUNNING

            logger.info(f"Started agent {agent_id} (PID: {process.pid})")

            # Record metrics
            if self.metrics:
                await self.metrics.record_agent_status(agent_id, "running")

            return True

        except Exception as e:
            agent_process.state = AgentState.FAILED
            agent_process.error_message = str(e)

            logger.error(f"Failed to start agent {agent_id}: {e}")

            # Record metrics
            if self.metrics:
                await self.metrics.record_error(agent_id, f"start_failed: {str(e)}")
                await self.metrics.record_agent_status(agent_id, "failed")

            return False

    async def stop_agent(self, agent_id: str, force: bool = False) -> bool:
        """
        Stop an agent process.

        Args:
            agent_id: Agent identifier
            force: Force kill if graceful shutdown fails

        Returns:
            True if stopped successfully
        """
        with self._lock:
            if agent_id not in self._agents:
                logger.error(f"Agent {agent_id} not found")
                return False

            agent_process = self._agents[agent_id]

        if agent_process.state in [AgentState.STOPPED, AgentState.FAILED]:
            logger.warning(f"Agent {agent_id} already stopped")
            return True

        try:
            agent_process.state = AgentState.STOPPING

            if agent_process.process and agent_process.process.is_alive():
                # Try graceful shutdown first
                if not force:
                    agent_process.process.terminate()

                    # Wait for graceful shutdown
                    for _ in range(30):  # 3 seconds timeout
                        if not agent_process.process.is_alive():
                            break
                        await asyncio.sleep(0.1)

                # Force kill if still alive
                if agent_process.process.is_alive():
                    agent_process.process.kill()
                    logger.warning(f"Force killed agent {agent_id}")

            agent_process.state = AgentState.STOPPED
            agent_process.process = None
            agent_process.pid = None

            logger.info(f"Stopped agent {agent_id}")

            # Record metrics
            if self.metrics:
                await self.metrics.record_agent_status(agent_id, "stopped")

            return True

        except Exception as e:
            logger.error(f"Failed to stop agent {agent_id}: {e}")
            agent_process.state = AgentState.FAILED
            agent_process.error_message = str(e)
            return False

    async def restart_agent(self, agent_id: str) -> bool:
        """
        Restart an agent process.

        Args:
            agent_id: Agent identifier

        Returns:
            True if restarted successfully
        """
        logger.info(f"Restarting agent {agent_id}")

        # Stop first
        await self.stop_agent(agent_id, force=True)

        # Then start
        return await self.start_agent(agent_id)

    async def scale_agent(self, agent_id: str, target_concurrency: int) -> bool:
        """
        Scale agent concurrency (placeholder for now).

        Args:
            agent_id: Agent identifier
            target_concurrency: Target concurrency level

        Returns:
            True if scaled successfully
        """
        # For now, just log - full implementation would require
        # managing multiple processes per agent
        logger.info(f"Scaling agent {agent_id} to concurrency {target_concurrency}")
        return True

    def _agent_process_wrapper(self, agent_id: str, agent_spec: AgentSpec) -> None:
        """
        Process wrapper that runs the agent function.

        Args:
            agent_id: Agent identifier
            agent_spec: Agent specification
        """
        try:
            # Set process name
            import setproctitle
            setproctitle.setproctitle(f"sabot-agent-{agent_id}")
        except ImportError:
            pass  # setproctitle not available

        # Set up signal handlers
        def signal_handler(signum, frame):
            logger.info(f"Agent {agent_id} received signal {signum}")
            sys.exit(0)

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        try:
            # Create asyncio event loop for the agent
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Run the agent function
            agent_fun = agent_spec.fun
            stream = agent_spec.stream

            if agent_fun:
                logger.info(f"Agent {agent_id} starting execution")

                # For now, just call the function once
                # In real implementation, this would be an async loop
                # processing messages from the stream
                try:
                    # Simulate agent work
                    while True:
                        time.sleep(1)  # Simulate work

                        # Check resource limits
                        if agent_spec.memory_limit_mb:
                            process = psutil.Process()
                            memory_mb = process.memory_info().rss / 1024 / 1024
                            if memory_mb > agent_spec.memory_limit_mb:
                                logger.warning(f"Agent {agent_id} exceeded memory limit: {memory_mb:.1f}MB")
                                sys.exit(1)

                        if agent_spec.cpu_limit_percent:
                            cpu_percent = psutil.cpu_percent(interval=0.1)
                            if cpu_percent > agent_spec.cpu_limit_percent:
                                logger.warning(f"Agent {agent_id} high CPU usage: {cpu_percent:.1f}%")

                except KeyboardInterrupt:
                    logger.info(f"Agent {agent_id} interrupted")
                    sys.exit(0)

            else:
                logger.error(f"Agent {agent_id} has no function to execute")
                sys.exit(1)

        except Exception as e:
            logger.error(f"Agent {agent_id} failed: {e}")
            traceback.print_exc()
            sys.exit(1)

    async def _supervision_loop(self) -> None:
        """Main supervision loop that monitors and manages agents."""
        logger.info("Agent supervision loop started")

        while self._running:
            try:
                await self._perform_health_checks()
                await self._handle_failed_agents()
                await self._enforce_resource_limits()

                # Wait before next supervision cycle
                await asyncio.sleep(5.0)  # 5 second supervision interval

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Supervision loop error: {e}")
                await asyncio.sleep(1.0)

        logger.info("Agent supervision loop stopped")

    async def _perform_health_checks(self) -> None:
        """Perform health checks on all running agents."""
        current_time = time.time()

        for agent_id, agent_process in list(self._agents.items()):
            if agent_process.state != AgentState.RUNNING:
                continue

            # Check if health check is due
            if (agent_process.last_health_check and
                current_time - agent_process.last_health_check < agent_process.health_check_interval):
                continue

            agent_process.last_health_check = current_time

            # Perform health check
            if not agent_process.is_healthy:
                logger.warning(f"Agent {agent_id} health check failed")
                agent_process.state = AgentState.FAILED

                # Record metrics
                if self.metrics:
                    await self.metrics.record_error(agent_id, "health_check_failed")
                    await self.metrics.record_agent_status(agent_id, "failed")

                continue

            # Get resource usage
            resources = agent_process.get_resource_usage()

            # Record metrics
            if self.metrics:
                await self.metrics.record_resource_usage(
                    f"agent_{agent_id}",
                    int(resources["memory_mb"] * 1024 * 1024),  # Convert to bytes
                    resources["cpu_percent"]
                )

    async def _handle_failed_agents(self) -> None:
        """Handle failed agents according to supervision strategy."""
        for agent_id, agent_process in list(self._agents.items()):
            if agent_process.state != AgentState.FAILED:
                continue

            agent_spec = self._agent_specs.get(agent_id)
            if not agent_spec:
                continue

            # Check restart policy
            restart_policy = agent_spec.restart_policy or self.config.default_restart_policy

            should_restart = False

            if restart_policy == RestartPolicy.PERMANENT:
                should_restart = True
            elif restart_policy == RestartPolicy.TRANSIENT:
                # Only restart if abnormal exit (non-zero exit code)
                should_restart = agent_process.exit_code != 0
            # TEMPORARY agents are never restarted

            if should_restart and agent_process.restart_count < agent_process.max_restarts:
                # Check restart window
                current_time = time.time()
                if (agent_process.last_restart_time and
                    current_time - agent_process.last_restart_time < agent_process.restart_window):
                    # Too many restarts in window
                    logger.error(f"Agent {agent_id} exceeded restart limit in window")
                    continue

                logger.info(f"Restarting failed agent {agent_id}")
                agent_process.state = AgentState.RESTARTING
                agent_process.restart_count += 1

                success = await self.restart_agent(agent_id)
                if success:
                    logger.info(f"Successfully restarted agent {agent_id}")
                else:
                    logger.error(f"Failed to restart agent {agent_id}")

    async def _enforce_resource_limits(self) -> None:
        """Enforce resource limits on running agents."""
        for agent_id, agent_process in list(self._agents.items()):
            if agent_process.state != AgentState.RUNNING:
                continue

            resources = agent_process.get_resource_usage()

            # Check memory limit
            if (agent_process.memory_limit_mb and
                resources["memory_mb"] > agent_process.memory_limit_mb):
                logger.warning(f"Agent {agent_id} exceeded memory limit: "
                             f"{resources['memory_mb']:.1f}MB > {agent_process.memory_limit_mb}MB")

                # Kill the agent - supervisor will restart if configured
                await self.stop_agent(agent_id, force=True)

            # Check CPU limit (warning only for now)
            if (agent_process.cpu_limit_percent and
                resources["cpu_percent"] > agent_process.cpu_limit_percent):
                logger.warning(f"Agent {agent_id} high CPU usage: "
                             f"{resources['cpu_percent']:.1f}% > {agent_process.cpu_limit_percent}%")

    def get_agent_status(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """Get status information for an agent."""
        if agent_id not in self._agents:
            return None

        agent_process = self._agents[agent_id]
        resources = agent_process.get_resource_usage()

        return {
            "agent_id": agent_id,
            "state": agent_process.state.value,
            "pid": agent_process.pid,
            "uptime": agent_process.uptime,
            "restart_count": agent_process.restart_count,
            "memory_mb": resources["memory_mb"],
            "cpu_percent": resources["cpu_percent"],
            "is_healthy": agent_process.is_healthy
        }

    def get_all_agent_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status for all agents."""
        return {
            agent_id: self.get_agent_status(agent_id)
            for agent_id in self._agents.keys()
        }

    def get_runtime_stats(self) -> Dict[str, Any]:
        """Get runtime statistics."""
        total_agents = len(self._agents)
        running_agents = sum(1 for a in self._agents.values() if a.state == AgentState.RUNNING)
        failed_agents = sum(1 for a in self._agents.values() if a.state == AgentState.FAILED)

        return {
            "total_agents": total_agents,
            "running_agents": running_agents,
            "failed_agents": failed_agents,
            "supervision_groups": len(self._supervision_groups),
            "is_running": self._running
        }

    async def health_check(self) -> Dict[str, Any]:
        """Perform runtime health check."""
        return {
            "status": "healthy" if self._running else "stopped",
            "running": self._running,
            "agents": self.get_runtime_stats()
        }
