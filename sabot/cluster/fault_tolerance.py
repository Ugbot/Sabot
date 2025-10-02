#!/usr/bin/env python3
"""
Fault Tolerance for Sabot distributed clusters.

Provides automatic failure detection, recovery mechanisms, and
resilient operation across cluster node failures.
"""

import asyncio
import time
import logging
from typing import Dict, List, Any, Optional, Callable, Set
from dataclasses import dataclass, field
from enum import Enum

from .coordinator import ClusterNode, NodeStatus

logger = logging.getLogger(__name__)


class FailureType(Enum):
    """Types of node failures."""
    NETWORK_PARTITION = "network_partition"
    NODE_CRASH = "node_crash"
    PROCESS_FAILURE = "process_failure"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    UNKNOWN = "unknown"


@dataclass
class FailureEvent:
    """Represents a node failure event."""
    node_id: str
    failure_type: FailureType
    detected_at: float
    last_seen: float
    consecutive_failures: int
    recovery_attempts: int = 0
    resolved: bool = False
    resolved_at: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecoveryAction:
    """Represents a recovery action for a failed node."""
    action_id: str
    node_id: str
    action_type: str  # 'reassign_work', 'restart_node', 'scale_up', 'failover'
    priority: int  # 1=low, 5=critical
    created_at: float = field(default_factory=time.time)
    executed_at: Optional[float] = None
    completed_at: Optional[float] = None
    success: bool = False
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class FailureDetector:
    """
    Failure detector for cluster nodes.

    Uses multiple detection mechanisms:
    - Heartbeat monitoring
    - TCP connection checks
    - Application-level health checks
    - Suspicion-based failure detection
    """

    def __init__(self, timeout_seconds: float = 30.0, check_interval: float = 10.0):
        self.timeout_seconds = timeout_seconds
        self.check_interval = check_interval
        self.last_check_times: Dict[str, float] = {}
        self.failure_history: Dict[str, List[FailureEvent]] = {}
        self.suspected_nodes: Set[str] = set()
        self._running = False
        self._check_task: Optional[asyncio.Task] = None

        # Callbacks
        self.failure_callbacks: List[Callable[[FailureEvent], None]] = []
        self.recovery_callbacks: List[Callable[[str], None]] = []

    async def start(self) -> None:
        """Start failure detection."""
        if self._running:
            return

        self._running = True
        self._check_task = asyncio.create_task(self._detection_loop())
        logger.info("Failure detector started")

    async def stop(self) -> None:
        """Stop failure detection."""
        self._running = False
        if self._check_task:
            self._check_task.cancel()
            try:
                await self._check_task
            except asyncio.CancelledError:
                pass
        logger.info("Failure detector stopped")

    async def _detection_loop(self) -> None:
        """Main failure detection loop."""
        while self._running:
            try:
                await self._perform_failure_checks()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in failure detection loop: {e}")
                await asyncio.sleep(5)

    async def _perform_failure_checks(self) -> None:
        """Perform failure detection checks."""
        current_time = time.time()

        # Check nodes that haven't been checked recently
        for node_id, last_check in list(self.last_check_times.items()):
            if current_time - last_check >= self.check_interval:
                await self._check_node_failure(node_id)
                self.last_check_times[node_id] = current_time

    async def _check_node_failure(self, node_id: str) -> None:
        """Check if a specific node has failed."""
        # This would be called with actual node information
        # For now, we'll rely on heartbeat-based detection
        # In a real implementation, this would perform additional checks

        if node_id in self.suspected_nodes:
            # Node is already suspected, check if it should be marked as failed
            failure_events = self.failure_history.get(node_id, [])
            if failure_events:
                last_failure = failure_events[-1]
                if not last_failure.resolved and time.time() - last_failure.detected_at > self.timeout_seconds:
                    # Mark as confirmed failure
                    await self._confirm_node_failure(node_id, FailureType.NODE_CRASH)

    def report_heartbeat(self, node_id: str, timestamp: float) -> None:
        """Report a heartbeat from a node."""
        current_time = time.time()

        # Clear any suspicion for this node
        if node_id in self.suspected_nodes:
            self.suspected_nodes.discard(node_id)

            # Mark failure as resolved
            failure_events = self.failure_history.get(node_id, [])
            if failure_events and not failure_events[-1].resolved:
                failure_events[-1].resolved = True
                failure_events[-1].resolved_at = current_time

                # Notify recovery
                for callback in self.recovery_callbacks:
                    try:
                        callback(node_id)
                    except Exception as e:
                        logger.error(f"Recovery callback failed: {e}")

    def report_node_down(self, node_id: str, failure_type: FailureType = FailureType.UNKNOWN) -> None:
        """Report that a node is down."""
        if node_id not in self.suspected_nodes:
            self.suspected_nodes.add(node_id)

            # Create failure event
            failure_event = FailureEvent(
                node_id=node_id,
                failure_type=failure_type,
                detected_at=time.time(),
                last_seen=self.last_check_times.get(node_id, 0),
                consecutive_failures=self._get_consecutive_failures(node_id) + 1
            )

            # Store failure history
            if node_id not in self.failure_history:
                self.failure_history[node_id] = []
            self.failure_history[node_id].append(failure_event)

            # Notify listeners
            for callback in self.failure_callbacks:
                try:
                    callback(failure_event)
                except Exception as e:
                    logger.error(f"Failure callback failed: {e}")

    async def _confirm_node_failure(self, node_id: str, failure_type: FailureType) -> None:
        """Confirm that a node has actually failed."""
        logger.warning(f"Confirmed failure of node {node_id}: {failure_type.value}")

        # Additional confirmation logic could go here
        # For example, trying to contact the node via alternative paths

    def _get_consecutive_failures(self, node_id: str) -> int:
        """Get the number of consecutive failures for a node."""
        failure_events = self.failure_history.get(node_id, [])
        if not failure_events:
            return 0

        count = 0
        for event in reversed(failure_events):
            if not event.resolved:
                count += 1
            else:
                break

        return count

    def add_failure_callback(self, callback: Callable[[FailureEvent], None]) -> None:
        """Add a callback for failure events."""
        self.failure_callbacks.append(callback)

    def add_recovery_callback(self, callback: Callable[[str], None]) -> None:
        """Add a callback for recovery events."""
        self.recovery_callbacks.append(callback)

    def get_failure_stats(self, node_id: Optional[str] = None) -> Dict[str, Any]:
        """Get failure statistics."""
        if node_id:
            # Stats for specific node
            failure_events = self.failure_history.get(node_id, [])
            return {
                'node_id': node_id,
                'total_failures': len(failure_events),
                'unresolved_failures': len([e for e in failure_events if not e.resolved]),
                'avg_time_to_resolve': self._calculate_avg_resolution_time(failure_events),
                'last_failure': failure_events[-1] if failure_events else None
            }
        else:
            # Global stats
            total_failures = sum(len(events) for events in self.failure_history.values())
            unresolved_failures = sum(
                len([e for e in events if not e.resolved])
                for events in self.failure_history.values()
            )

            return {
                'total_nodes_with_failures': len(self.failure_history),
                'total_failures': total_failures,
                'unresolved_failures': unresolved_failures,
                'currently_suspected': len(self.suspected_nodes)
            }

    def _calculate_avg_resolution_time(self, events: List[FailureEvent]) -> Optional[float]:
        """Calculate average time to resolve failures."""
        resolved_events = [e for e in events if e.resolved and e.resolved_at]
        if not resolved_events:
            return None

        resolution_times = [
            e.resolved_at - e.detected_at
            for e in resolved_events
            if e.resolved_at
        ]

        return sum(resolution_times) / len(resolution_times) if resolution_times else None


class RecoveryManager:
    """
    Recovery manager for handling node failures and cluster healing.

    Coordinates recovery actions across the cluster:
    - Work reassignment
    - Node restart coordination
    - Cluster reconfiguration
    - Data consistency recovery
    """

    def __init__(self):
        self.recovery_actions: Dict[str, RecoveryAction] = {}
        self.active_recoveries: Set[str] = set()

        # Recovery strategies
        self.recovery_strategies = {
            'reassign_work': self._reassign_work,
            'restart_node': self._restart_node,
            'scale_up': self._scale_up_cluster,
            'failover': self._perform_failover
        }

        # Callbacks
        self.action_callbacks: List[Callable[[RecoveryAction], None]] = []

    def add_recovery_action(self, node_id: str, action_type: str,
                           priority: int = 3, metadata: Optional[Dict[str, Any]] = None) -> str:
        """Add a recovery action to be executed."""
        action_id = f"{node_id}_{action_type}_{int(time.time())}"

        action = RecoveryAction(
            action_id=action_id,
            node_id=node_id,
            action_type=action_type,
            priority=priority,
            metadata=metadata or {}
        )

        self.recovery_actions[action_id] = action
        logger.info(f"Added recovery action: {action_type} for node {node_id}")

        return action_id

    async def execute_recovery_actions(self) -> None:
        """Execute pending recovery actions in priority order."""
        # Sort actions by priority (highest first)
        pending_actions = [
            action for action in self.recovery_actions.values()
            if action.executed_at is None
        ]

        pending_actions.sort(key=lambda a: (-a.priority, a.created_at))

        for action in pending_actions:
            if action.action_id in self.active_recoveries:
                continue  # Already being executed

            self.active_recoveries.add(action.action_id)
            action.executed_at = time.time()

            try:
                # Execute the action
                success = await self._execute_action(action)

                action.completed_at = time.time()
                action.success = success

                if success:
                    logger.info(f"Recovery action {action.action_id} completed successfully")
                else:
                    logger.warning(f"Recovery action {action.action_id} failed")

            except Exception as e:
                action.completed_at = time.time()
                action.success = False
                action.error_message = str(e)
                logger.error(f"Recovery action {action.action_id} failed with error: {e}")

            finally:
                self.active_recoveries.discard(action.action_id)

                # Notify callbacks
                for callback in self.action_callbacks:
                    try:
                        callback(action)
                    except Exception as e:
                        logger.error(f"Recovery action callback failed: {e}")

    async def _execute_action(self, action: RecoveryAction) -> bool:
        """Execute a specific recovery action."""
        strategy = self.recovery_strategies.get(action.action_type)
        if not strategy:
            logger.error(f"Unknown recovery action type: {action.action_type}")
            return False

        try:
            return await strategy(action)
        except Exception as e:
            logger.error(f"Recovery action {action.action_type} failed: {e}")
            return False

    async def _reassign_work(self, action: RecoveryAction) -> bool:
        """Reassign work from failed node to healthy nodes."""
        # This would interact with the cluster coordinator
        # For now, just log the action
        logger.info(f"Reassigning work from failed node {action.node_id}")
        return True

    async def _restart_node(self, action: RecoveryAction) -> bool:
        """Attempt to restart a failed node."""
        # This would attempt to restart the node process
        # Implementation depends on deployment mechanism
        logger.info(f"Attempting to restart node {action.node_id}")
        return False  # Placeholder

    async def _scale_up_cluster(self, action: RecoveryAction) -> bool:
        """Scale up the cluster to compensate for failed nodes."""
        # This would provision new nodes
        logger.info(f"Scaling up cluster due to failure of node {action.node_id}")
        return False  # Placeholder

    async def _perform_failover(self, action: RecoveryAction) -> bool:
        """Perform failover operations for critical components."""
        # This would handle failover for coordinator, gateways, etc.
        logger.info(f"Performing failover for node {action.node_id}")
        return True  # Placeholder

    def add_action_callback(self, callback: Callable[[RecoveryAction], None]) -> None:
        """Add a callback for recovery action completion."""
        self.action_callbacks.append(callback)

    def get_recovery_stats(self) -> Dict[str, Any]:
        """Get recovery statistics."""
        total_actions = len(self.recovery_actions)
        completed_actions = len([
            a for a in self.recovery_actions.values()
            if a.completed_at is not None
        ])
        successful_actions = len([
            a for a in self.recovery_actions.values()
            if a.success
        ])

        return {
            'total_actions': total_actions,
            'completed_actions': completed_actions,
            'successful_actions': successful_actions,
            'active_recoveries': len(self.active_recoveries),
            'success_rate': successful_actions / max(1, completed_actions)
        }

    def cleanup_old_actions(self, older_than_hours: float = 24.0) -> int:
        """Clean up old recovery actions."""
        cutoff_time = time.time() - (older_than_hours * 3600)
        to_remove = []

        for action_id, action in self.recovery_actions.items():
            if action.completed_at and action.completed_at < cutoff_time:
                to_remove.append(action_id)

        for action_id in to_remove:
            del self.recovery_actions[action_id]

        logger.info(f"Cleaned up {len(to_remove)} old recovery actions")
        return len(to_remove)


class CircuitBreaker:
    """
    Circuit breaker pattern for fault tolerance.

    Prevents cascading failures by temporarily stopping calls
    to services that are failing.
    """

    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0,
                 expected_exception: Exception = Exception):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception

        self.failure_count = 0
        self.last_failure_time = 0
        self.state = 'closed'  # closed, open, half-open

    def call(self, func: Callable, *args, **kwargs):
        """Execute function through circuit breaker."""
        if self.state == 'open':
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = 'half-open'
            else:
                raise CircuitBreakerOpenException("Circuit breaker is open")

        try:
            result = func(*args, **kwargs)

            if self.state == 'half-open':
                self.state = 'closed'
                self.failure_count = 0

            return result

        except self.expected_exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.state = 'open'

            raise e

    async def async_call(self, func: Callable, *args, **kwargs):
        """Execute async function through circuit breaker."""
        if self.state == 'open':
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = 'half-open'
            else:
                raise CircuitBreakerOpenException("Circuit breaker is open")

        try:
            result = await func(*args, **kwargs)

            if self.state == 'half-open':
                self.state = 'closed'
                self.failure_count = 0

            return result

        except self.expected_exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.state = 'open'

            raise e

    def get_state(self) -> Dict[str, Any]:
        """Get circuit breaker state."""
        return {
            'state': self.state,
            'failure_count': self.failure_count,
            'last_failure_time': self.last_failure_time,
            'time_since_last_failure': time.time() - self.last_failure_time
        }


class CircuitBreakerOpenException(Exception):
    """Exception raised when circuit breaker is open."""
    pass
