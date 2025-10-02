#!/usr/bin/env python3
"""
Slot Pool - Resource Management for Task Deployment

Flink-style slot-based resource management:
- Slots are resource containers on worker nodes
- Tasks are deployed to available slots
- Supports slot sharing (multiple tasks per slot)
- Dynamic slot allocation and release

Key Concepts:
- Slot: Resource container (CPU + memory) on a worker node
- Task Slot: Physical location where a task runs
- Slot Sharing: Multiple tasks from different operators share same slot
- Slot Request: Request for slot allocation from scheduler
"""

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Set
from enum import Enum
import time
import uuid

logger = logging.getLogger(__name__)


class SlotState(Enum):
    """Slot allocation states."""

    FREE = "free"  # Available for allocation
    ALLOCATED = "allocated"  # Allocated to task but not yet running
    ACTIVE = "active"  # Task running in slot
    RELEASING = "releasing"  # Being released
    FAILED = "failed"  # Slot failed (node failure, etc.)


@dataclass
class Slot:
    """
    Resource container on a worker node.

    Represents a fixed amount of resources (CPU + memory) that can run
    one or more tasks (via slot sharing).
    """

    # Identity
    slot_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    node_id: str = ""  # Worker node ID
    slot_index: int = 0  # Index on node (0 to num_slots-1)

    # State
    state: SlotState = SlotState.FREE

    # Resources
    cpu_cores: float = 1.0  # CPU cores
    memory_mb: int = 1024  # Memory in MB

    # Allocation
    allocated_to: Optional[str] = None  # Task ID using this slot
    allocated_at: Optional[float] = None

    # Slot sharing (multiple tasks per slot)
    tasks: Set[str] = field(default_factory=set)  # Task IDs in this slot
    sharing_group: Optional[str] = None  # Tasks from same group can share

    # Metrics
    last_heartbeat: Optional[float] = None

    def is_free(self) -> bool:
        """Check if slot is available for allocation."""
        return self.state == SlotState.FREE

    def is_active(self) -> bool:
        """Check if slot has running tasks."""
        return self.state == SlotState.ACTIVE

    def can_share_with(self, task_operator_id: str, sharing_group: Optional[str]) -> bool:
        """
        Check if this slot can be shared with a new task.

        Args:
            task_operator_id: Operator ID of task requesting slot
            sharing_group: Slot sharing group

        Returns:
            True if slot can be shared
        """
        # Slot must be active and in same sharing group
        if not self.is_active():
            return False

        if self.sharing_group != sharing_group:
            return False

        # Don't allow tasks from same operator to share slot (avoid resource contention)
        for task_id in self.tasks:
            if task_operator_id in task_id:  # Task ID contains operator ID
                return False

        return True

    def allocate(self, task_id: str) -> None:
        """
        Allocate slot to a task.

        Args:
            task_id: Task ID to allocate
        """
        if self.state == SlotState.FREE:
            self.allocated_to = task_id
            self.allocated_at = time.time()
            self.state = SlotState.ALLOCATED
            self.tasks.add(task_id)
            logger.debug(f"Slot {self.slot_id} allocated to task {task_id}")
        elif self.state == SlotState.ACTIVE and task_id not in self.tasks:
            # Slot sharing
            self.tasks.add(task_id)
            logger.debug(f"Task {task_id} sharing slot {self.slot_id}")
        else:
            raise ValueError(f"Cannot allocate slot {self.slot_id} in state {self.state}")

    def activate(self) -> None:
        """Mark slot as active (task started)."""
        if self.state == SlotState.ALLOCATED:
            self.state = SlotState.ACTIVE
            logger.debug(f"Slot {self.slot_id} activated")

    def release(self, task_id: str) -> None:
        """
        Release a task from this slot.

        Args:
            task_id: Task ID to release
        """
        if task_id in self.tasks:
            self.tasks.remove(task_id)
            logger.debug(f"Task {task_id} released from slot {self.slot_id}")

        # If no more tasks, free the slot
        if not self.tasks:
            self.state = SlotState.FREE
            self.allocated_to = None
            self.allocated_at = None
            logger.debug(f"Slot {self.slot_id} freed")

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            'slot_id': self.slot_id,
            'node_id': self.node_id,
            'slot_index': self.slot_index,
            'state': self.state.value,
            'cpu_cores': self.cpu_cores,
            'memory_mb': self.memory_mb,
            'allocated_to': self.allocated_to,
            'allocated_at': self.allocated_at,
            'tasks': list(self.tasks),
            'sharing_group': self.sharing_group,
            'last_heartbeat': self.last_heartbeat,
        }


@dataclass
class SlotRequest:
    """Request for slot allocation from scheduler."""

    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_id: str = ""
    operator_id: str = ""

    # Resource requirements
    cpu_cores: float = 0.5
    memory_mb: int = 256

    # Slot sharing
    sharing_group: Optional[str] = None  # Tasks in same group can share slots

    # Constraints
    preferred_node_id: Optional[str] = None  # Prefer specific node (locality)

    # Status
    created_at: float = field(default_factory=time.time)
    fulfilled_at: Optional[float] = None
    allocated_slot_id: Optional[str] = None


class SlotPool:
    """
    Manages slot allocation across cluster nodes.

    Provides Flink-style slot management with:
    - Dynamic slot allocation/release
    - Slot sharing across operators
    - Node locality hints
    - Resource tracking
    """

    def __init__(self):
        """Initialize slot pool."""
        self.slots: Dict[str, Slot] = {}  # slot_id -> Slot
        self.node_slots: Dict[str, List[str]] = {}  # node_id -> [slot_ids]
        self.task_slots: Dict[str, str] = {}  # task_id -> slot_id

        # Pending requests
        self.pending_requests: Dict[str, SlotRequest] = {}

        logger.info("SlotPool initialized")

    def register_node(self, node_id: str, num_slots: int,
                      cpu_per_slot: float = 1.0, memory_per_slot_mb: int = 1024) -> int:
        """
        Register a worker node with its slots.

        Args:
            node_id: Worker node ID
            num_slots: Number of task slots on this node
            cpu_per_slot: CPU cores per slot
            memory_per_slot_mb: Memory per slot (MB)

        Returns:
            Number of slots created
        """
        if node_id in self.node_slots:
            logger.warning(f"Node {node_id} already registered, skipping")
            return 0

        self.node_slots[node_id] = []

        for slot_index in range(num_slots):
            slot = Slot(
                node_id=node_id,
                slot_index=slot_index,
                cpu_cores=cpu_per_slot,
                memory_mb=memory_per_slot_mb,
            )

            self.slots[slot.slot_id] = slot
            self.node_slots[node_id].append(slot.slot_id)

        logger.info(
            f"Registered node {node_id} with {num_slots} slots "
            f"({cpu_per_slot} cores, {memory_per_slot_mb}MB each)"
        )

        return num_slots

    def unregister_node(self, node_id: str) -> int:
        """
        Unregister a worker node and release all its slots.

        Args:
            node_id: Worker node ID

        Returns:
            Number of slots released
        """
        if node_id not in self.node_slots:
            logger.warning(f"Node {node_id} not registered")
            return 0

        slot_ids = self.node_slots[node_id]

        # Release all slots
        for slot_id in slot_ids:
            if slot_id in self.slots:
                slot = self.slots[slot_id]

                # Mark tasks as failed
                for task_id in slot.tasks:
                    if task_id in self.task_slots:
                        del self.task_slots[task_id]

                del self.slots[slot_id]

        del self.node_slots[node_id]

        logger.info(f"Unregistered node {node_id}, released {len(slot_ids)} slots")
        return len(slot_ids)

    def request_slot(self, request: SlotRequest) -> Optional[Slot]:
        """
        Request a slot for a task.

        Args:
            request: SlotRequest with task requirements

        Returns:
            Allocated Slot if available, None otherwise
        """
        # Try to find shareable slot first (slot sharing)
        if request.sharing_group:
            for slot in self.slots.values():
                if slot.can_share_with(request.operator_id, request.sharing_group):
                    slot.allocate(request.task_id)
                    self.task_slots[request.task_id] = slot.slot_id
                    request.fulfilled_at = time.time()
                    request.allocated_slot_id = slot.slot_id
                    logger.info(
                        f"Task {request.task_id} sharing slot {slot.slot_id} "
                        f"(group {request.sharing_group})"
                    )
                    return slot

        # Find free slot (prefer locality)
        candidate_slots = [s for s in self.slots.values() if s.is_free()]

        if not candidate_slots:
            # No free slots, queue request
            self.pending_requests[request.request_id] = request
            logger.debug(f"No free slots for task {request.task_id}, queued")
            return None

        # Prefer node locality if specified
        if request.preferred_node_id:
            preferred = [s for s in candidate_slots if s.node_id == request.preferred_node_id]
            if preferred:
                candidate_slots = preferred

        # Allocate first available slot
        slot = candidate_slots[0]
        slot.allocate(request.task_id)
        self.task_slots[request.task_id] = slot.slot_id

        request.fulfilled_at = time.time()
        request.allocated_slot_id = slot.slot_id

        logger.info(f"Allocated slot {slot.slot_id} to task {request.task_id}")
        return slot

    def release_slot(self, task_id: str) -> bool:
        """
        Release slot allocated to a task.

        Args:
            task_id: Task ID

        Returns:
            True if slot was released
        """
        if task_id not in self.task_slots:
            logger.warning(f"Task {task_id} has no allocated slot")
            return False

        slot_id = self.task_slots[task_id]
        slot = self.slots.get(slot_id)

        if not slot:
            logger.error(f"Slot {slot_id} not found")
            return False

        slot.release(task_id)
        del self.task_slots[task_id]

        # Try to fulfill pending requests with freed slot
        self._process_pending_requests()

        return True

    def activate_slot(self, task_id: str) -> bool:
        """
        Mark slot as active (task started running).

        Args:
            task_id: Task ID

        Returns:
            True if slot was activated
        """
        if task_id not in self.task_slots:
            logger.warning(f"Task {task_id} has no allocated slot")
            return False

        slot_id = self.task_slots[task_id]
        slot = self.slots.get(slot_id)

        if not slot:
            logger.error(f"Slot {slot_id} not found")
            return False

        slot.activate()
        return True

    def get_slot_for_task(self, task_id: str) -> Optional[Slot]:
        """Get slot allocated to a task."""
        slot_id = self.task_slots.get(task_id)
        if not slot_id:
            return None
        return self.slots.get(slot_id)

    def get_available_slots(self) -> List[Slot]:
        """Get all free slots."""
        return [slot for slot in self.slots.values() if slot.is_free()]

    def get_node_slots(self, node_id: str) -> List[Slot]:
        """Get all slots on a specific node."""
        if node_id not in self.node_slots:
            return []
        slot_ids = self.node_slots[node_id]
        return [self.slots[sid] for sid in slot_ids if sid in self.slots]

    def get_total_slots(self) -> int:
        """Get total number of slots in pool."""
        return len(self.slots)

    def get_free_slots(self) -> int:
        """Get number of free slots."""
        return len(self.get_available_slots())

    def get_utilization(self) -> float:
        """
        Get slot utilization percentage.

        Returns:
            Utilization (0.0 to 1.0)
        """
        total = self.get_total_slots()
        if total == 0:
            return 0.0

        used = total - self.get_free_slots()
        return used / total

    def _process_pending_requests(self) -> int:
        """
        Process pending slot requests with newly freed slots.

        Returns:
            Number of requests fulfilled
        """
        if not self.pending_requests:
            return 0

        fulfilled = 0
        completed_requests = []

        for request_id, request in self.pending_requests.items():
            slot = self.request_slot(request)
            if slot:
                completed_requests.append(request_id)
                fulfilled += 1

        # Remove fulfilled requests
        for request_id in completed_requests:
            del self.pending_requests[request_id]

        if fulfilled > 0:
            logger.info(f"Fulfilled {fulfilled} pending slot requests")

        return fulfilled

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get slot pool metrics.

        Returns:
            Dictionary with metrics
        """
        total_slots = self.get_total_slots()
        free_slots = self.get_free_slots()
        active_slots = len([s for s in self.slots.values() if s.is_active()])

        return {
            'total_slots': total_slots,
            'free_slots': free_slots,
            'active_slots': active_slots,
            'allocated_slots': total_slots - free_slots,
            'utilization': self.get_utilization(),
            'pending_requests': len(self.pending_requests),
            'total_nodes': len(self.node_slots),
        }

    def to_dict(self) -> Dict[str, Any]:
        """Serialize slot pool to dictionary."""
        return {
            'slots': {sid: slot.to_dict() for sid, slot in self.slots.items()},
            'node_slots': self.node_slots,
            'task_slots': self.task_slots,
            'pending_requests': {
                rid: {
                    'request_id': req.request_id,
                    'task_id': req.task_id,
                    'operator_id': req.operator_id,
                    'cpu_cores': req.cpu_cores,
                    'memory_mb': req.memory_mb,
                    'created_at': req.created_at,
                }
                for rid, req in self.pending_requests.items()
            },
            'metrics': self.get_metrics(),
        }
