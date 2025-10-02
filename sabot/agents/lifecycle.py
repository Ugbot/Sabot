#!/usr/bin/env python3
"""
Agent Lifecycle Management for Sabot

Implements comprehensive agent lifecycle controls:
- Start/stop/restart operations
- Graceful shutdown handling
- Lifecycle state transitions
- Resource cleanup and recovery

This provides the operational interface for managing agent processes.
"""

import asyncio
import logging
import signal
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
from enum import Enum

from .runtime import AgentRuntime, AgentState, AgentProcess
from ..observability import get_observability

logger = logging.getLogger(__name__)


class LifecycleOperation(Enum):
    """Types of lifecycle operations."""
    START = "start"
    STOP = "stop"
    RESTART = "restart"
    PAUSE = "pause"
    RESUME = "resume"


@dataclass
class LifecycleResult:
    """Result of a lifecycle operation."""
    operation: LifecycleOperation
    agent_id: str
    success: bool
    previous_state: AgentState
    new_state: AgentState
    duration_seconds: float
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = None


class AgentLifecycleManager:
    """
    Manages the complete lifecycle of agents.

    Provides high-level operations for starting, stopping, restarting,
    and monitoring agent processes with proper state management.
    """

    def __init__(self, runtime: AgentRuntime):
        """
        Initialize the lifecycle manager.

        Args:
            runtime: The agent runtime to manage
        """
        self.runtime = runtime
        self.observability = get_observability()

        # Operation tracking
        self._operations: Dict[str, LifecycleResult] = {}
        self._operation_lock = asyncio.Lock()

        logger.info("AgentLifecycleManager initialized")

    async def start_agent(self, agent_id: str, timeout_seconds: float = 30.0) -> LifecycleResult:
        """
        Start an agent with timeout and monitoring.

        Args:
            agent_id: Agent to start
            timeout_seconds: Maximum time to wait for startup

        Returns:
            LifecycleResult with operation details
        """
        start_time = time.time()

        with self.observability.trace_operation(
            "lifecycle_start_agent",
            {"agent_id": agent_id, "timeout_seconds": timeout_seconds}
        ) as span:

            # Get current state
            previous_state = await self._get_agent_state(agent_id)

            try:
                # Perform the start operation
                success = await self.runtime.start_agent(agent_id)

                # Wait for agent to reach running state
                if success:
                    success = await self._wait_for_state(
                        agent_id, AgentState.RUNNING, timeout_seconds
                    )

                new_state = await self._get_agent_state(agent_id)
                duration = time.time() - start_time

                result = LifecycleResult(
                    operation=LifecycleOperation.START,
                    agent_id=agent_id,
                    success=success,
                    previous_state=previous_state,
                    new_state=new_state,
                    duration_seconds=duration,
                    metadata={"timeout_seconds": timeout_seconds}
                )

                # Record metrics
                self.observability.record_operation("agent_start", duration, {
                    "agent_id": agent_id,
                    "success": success
                })

                span.set_attribute("success", success)
                span.set_attribute("duration_seconds", duration)

                await self._store_operation_result(result)
                return result

            except Exception as e:
                duration = time.time() - start_time
                result = LifecycleResult(
                    operation=LifecycleOperation.START,
                    agent_id=agent_id,
                    success=False,
                    previous_state=previous_state,
                    new_state=previous_state,  # State didn't change
                    duration_seconds=duration,
                    error_message=str(e)
                )

                span.set_attribute("error", str(e))
                await self._store_operation_result(result)
                return result

    async def stop_agent(self, agent_id: str, timeout_seconds: float = 30.0,
                        graceful: bool = True) -> LifecycleResult:
        """
        Stop an agent with graceful shutdown support.

        Args:
            agent_id: Agent to stop
            timeout_seconds: Maximum time to wait for shutdown
            graceful: Whether to attempt graceful shutdown

        Returns:
            LifecycleResult with operation details
        """
        start_time = time.time()

        with self.observability.trace_operation(
            "lifecycle_stop_agent",
            {"agent_id": agent_id, "timeout_seconds": timeout_seconds, "graceful": graceful}
        ) as span:

            previous_state = await self._get_agent_state(agent_id)

            try:
                if graceful:
                    # Send SIGTERM first for graceful shutdown
                    success = await self._graceful_stop(agent_id, timeout_seconds)
                else:
                    # Force stop with SIGKILL
                    success = await self.runtime.stop_agent(agent_id)

                new_state = await self._get_agent_state(agent_id)
                duration = time.time() - start_time

                result = LifecycleResult(
                    operation=LifecycleOperation.STOP,
                    agent_id=agent_id,
                    success=success,
                    previous_state=previous_state,
                    new_state=new_state,
                    duration_seconds=duration,
                    metadata={"graceful": graceful, "timeout_seconds": timeout_seconds}
                )

                # Record metrics
                self.observability.record_operation("agent_stop", duration, {
                    "agent_id": agent_id,
                    "success": success,
                    "graceful": graceful
                })

                span.set_attribute("success", success)
                span.set_attribute("graceful", graceful)
                span.set_attribute("duration_seconds", duration)

                await self._store_operation_result(result)
                return result

            except Exception as e:
                duration = time.time() - start_time
                result = LifecycleResult(
                    operation=LifecycleOperation.STOP,
                    agent_id=agent_id,
                    success=False,
                    previous_state=previous_state,
                    new_state=previous_state,
                    duration_seconds=duration,
                    error_message=str(e)
                )

                span.set_attribute("error", str(e))
                await self._store_operation_result(result)
                return result

    async def restart_agent(self, agent_id: str, timeout_seconds: float = 60.0) -> LifecycleResult:
        """
        Restart an agent with proper state transition.

        Args:
            agent_id: Agent to restart
            timeout_seconds: Maximum time for restart operation

        Returns:
            LifecycleResult with operation details
        """
        start_time = time.time()

        with self.observability.trace_operation(
            "lifecycle_restart_agent",
            {"agent_id": agent_id, "timeout_seconds": timeout_seconds}
        ) as span:

            previous_state = await self._get_agent_state(agent_id)

            try:
                # Stop the agent first
                stop_result = await self.stop_agent(agent_id, timeout_seconds / 2, graceful=True)
                if not stop_result.success:
                    raise RuntimeError(f"Failed to stop agent for restart: {stop_result.error_message}")

                # Start the agent
                start_result = await self.start_agent(agent_id, timeout_seconds / 2)
                if not start_result.success:
                    raise RuntimeError(f"Failed to start agent after stop: {start_result.error_message}")

                duration = time.time() - start_time

                result = LifecycleResult(
                    operation=LifecycleOperation.RESTART,
                    agent_id=agent_id,
                    success=True,
                    previous_state=previous_state,
                    new_state=start_result.new_state,
                    duration_seconds=duration,
                    metadata={
                        "stop_duration": stop_result.duration_seconds,
                        "start_duration": start_result.duration_seconds,
                        "timeout_seconds": timeout_seconds
                    }
                )

                # Record metrics
                self.observability.record_operation("agent_restart", duration, {
                    "agent_id": agent_id,
                    "success": True
                })

                span.set_attribute("success", True)
                span.set_attribute("total_duration_seconds", duration)
                span.set_attribute("stop_duration_seconds", stop_result.duration_seconds)
                span.set_attribute("start_duration_seconds", start_result.duration_seconds)

                await self._store_operation_result(result)
                return result

            except Exception as e:
                duration = time.time() - start_time
                result = LifecycleResult(
                    operation=LifecycleOperation.RESTART,
                    agent_id=agent_id,
                    success=False,
                    previous_state=previous_state,
                    new_state=await self._get_agent_state(agent_id),
                    duration_seconds=duration,
                    error_message=str(e)
                )

                span.set_attribute("error", str(e))
                await self._store_operation_result(result)
                return result

    async def bulk_operation(self, agent_ids: List[str], operation: LifecycleOperation,
                           **kwargs) -> List[LifecycleResult]:
        """
        Perform a lifecycle operation on multiple agents.

        Args:
            agent_ids: List of agent IDs to operate on
            operation: Type of operation to perform
            **kwargs: Additional arguments for the operation

        Returns:
            List of LifecycleResult objects
        """
        with self.observability.trace_operation(
            "lifecycle_bulk_operation",
            {
                "operation": operation.value,
                "agent_count": len(agent_ids),
                "agent_ids": agent_ids
            }
        ) as span:

            # Execute operations concurrently
            tasks = []
            for agent_id in agent_ids:
                if operation == LifecycleOperation.START:
                    task = self.start_agent(agent_id, **kwargs)
                elif operation == LifecycleOperation.STOP:
                    task = self.stop_agent(agent_id, **kwargs)
                elif operation == LifecycleOperation.RESTART:
                    task = self.restart_agent(agent_id, **kwargs)
                else:
                    continue

                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results
            processed_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    # Operation failed with exception
                    processed_results.append(LifecycleResult(
                        operation=operation,
                        agent_id=agent_ids[i],
                        success=False,
                        previous_state=AgentState.UNKNOWN,
                        new_state=AgentState.UNKNOWN,
                        duration_seconds=0.0,
                        error_message=str(result)
                    ))
                else:
                    processed_results.append(result)

            span.set_attribute("total_operations", len(processed_results))
            span.set_attribute("successful_operations",
                             sum(1 for r in processed_results if r.success))

            return processed_results

    async def get_agent_status(self, agent_id: str) -> Dict[str, Any]:
        """
        Get detailed status of an agent.

        Args:
            agent_id: Agent to check

        Returns:
            Dictionary with agent status information
        """
        with self.observability.trace_operation("lifecycle_get_status", {"agent_id": agent_id}):
            try:
                agent_process = await self._get_agent_process(agent_id)

                status = {
                    "agent_id": agent_id,
                    "state": agent_process.state.value if agent_process else "unknown",
                    "pid": agent_process.pid if agent_process else None,
                    "start_time": agent_process.start_time if agent_process else None,
                    "uptime_seconds": None,
                    "restart_count": agent_process.restart_count if agent_process else 0,
                    "last_restart_time": agent_process.last_restart_time if agent_process else None,
                    "health_status": await self._check_agent_health(agent_id)
                }

                if agent_process and agent_process.start_time:
                    status["uptime_seconds"] = time.time() - agent_process.start_time

                return status

            except Exception as e:
                logger.error(f"Failed to get status for agent {agent_id}: {e}")
                return {
                    "agent_id": agent_id,
                    "state": "error",
                    "error": str(e)
                }

    async def _graceful_stop(self, agent_id: str, timeout_seconds: float) -> bool:
        """Attempt graceful shutdown of an agent."""
        try:
            # Send SIGTERM for graceful shutdown
            agent_process = await self._get_agent_process(agent_id)
            if agent_process and agent_process.process:
                agent_process.process.terminate()

                # Wait for process to exit
                start_wait = time.time()
                while time.time() - start_wait < timeout_seconds:
                    if not agent_process.process.is_alive():
                        return True
                    await asyncio.sleep(0.1)

                # If still alive after timeout, force kill
                if agent_process.process.is_alive():
                    agent_process.process.kill()
                    await asyncio.sleep(0.1)  # Give it time to die

                return not agent_process.process.is_alive()

        except Exception as e:
            logger.error(f"Graceful stop failed for {agent_id}: {e}")
            # Fall back to forceful stop
            return await self.runtime.stop_agent(agent_id)

        return False

    async def _wait_for_state(self, agent_id: str, target_state: AgentState,
                            timeout_seconds: float) -> bool:
        """Wait for an agent to reach a specific state."""
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            current_state = await self._get_agent_state(agent_id)
            if current_state == target_state:
                return True
            await asyncio.sleep(0.1)

        return False

    async def _get_agent_state(self, agent_id: str) -> AgentState:
        """Get the current state of an agent."""
        try:
            agent_process = await self._get_agent_process(agent_id)
            return agent_process.state if agent_process else AgentState.STOPPED
        except Exception:
            return AgentState.UNKNOWN

    async def _get_agent_process(self, agent_id: str) -> Optional[AgentProcess]:
        """Get the agent process object."""
        # This would need to be implemented to access runtime's internal state
        # For now, return None
        return None

    async def _check_agent_health(self, agent_id: str) -> str:
        """Check the health status of an agent."""
        try:
            state = await self._get_agent_state(agent_id)
            if state == AgentState.RUNNING:
                return "healthy"
            elif state == AgentState.STARTING:
                return "starting"
            elif state == AgentState.STOPPING:
                return "stopping"
            else:
                return "unhealthy"
        except Exception:
            return "unknown"

    async def _store_operation_result(self, result: LifecycleResult) -> None:
        """Store the result of a lifecycle operation."""
        async with self._operation_lock:
            key = f"{result.operation.value}_{result.agent_id}_{int(time.time())}"
            self._operations[key] = result

            # Keep only recent operations (last 1000)
            if len(self._operations) > 1000:
                oldest_key = min(self._operations.keys())
                del self._operations[oldest_key]

    async def get_operation_history(self, agent_id: Optional[str] = None,
                                  limit: int = 50) -> List[LifecycleResult]:
        """
        Get the history of lifecycle operations.

        Args:
            agent_id: Optional agent ID to filter by
            limit: Maximum number of operations to return

        Returns:
            List of LifecycleResult objects
        """
        async with self._operation_lock:
            operations = list(self._operations.values())

            if agent_id:
                operations = [op for op in operations if op.agent_id == agent_id]

            # Sort by timestamp (newest first)
            operations.sort(key=lambda op: op.duration_seconds, reverse=True)

            return operations[:limit]
