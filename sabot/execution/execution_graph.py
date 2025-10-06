#!/usr/bin/env python3
"""
Execution Graph - Physical Execution Plan

Represents the physical deployment of a streaming job with:
- Task vertices (parallel instances of logical operators)
- Task edges (data flow between parallel tasks)
- Shuffle edges (data redistribution for operator parallelism)
- Slot assignments (where tasks run on cluster nodes)
- Task state tracking (CREATED -> RUNNING -> COMPLETED)

Similar to Flink's ExecutionGraph and Spark's PhysicalPlan.
"""

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Callable, Set
from enum import Enum
import time

from .job_graph import OperatorType

logger = logging.getLogger(__name__)


class ShuffleEdgeType(Enum):
    """
    Types of data redistribution between operator tasks.

    Determines how data flows from N upstream tasks to M downstream tasks.
    """
    FORWARD = "forward"          # 1:1 mapping, no shuffle (operator chaining)
    HASH = "hash"                # Hash partition by key (joins, groupBy)
    BROADCAST = "broadcast"      # Replicate to all downstream (dimension tables)
    REBALANCE = "rebalance"      # Round-robin redistribution (load balance)
    RANGE = "range"              # Range partition (sorted data)
    CUSTOM = "custom"            # User-defined partitioning


class TaskState(Enum):
    """Task execution states (Flink-style state machine)."""

    CREATED = "created"  # Task created, not yet scheduled
    SCHEDULED = "scheduled"  # Scheduled to slot, not yet deployed
    DEPLOYING = "deploying"  # Being deployed to worker
    RUNNING = "running"  # Running on worker
    FINISHED = "finished"  # Successfully completed (batch jobs)
    CANCELING = "canceling"  # Cancellation requested
    CANCELED = "canceled"  # Successfully canceled
    FAILED = "failed"  # Failed with error
    RECONCILING = "reconciling"  # Recovering from failure


@dataclass
class TaskVertex:
    """
    Physical task instance (parallel instance of a logical operator).

    Represents a single execution unit that runs on a worker node.
    Multiple tasks per operator = operator parallelism.
    """

    # Identity
    task_id: str  # Unique task ID (e.g., "map_0_task_2")
    operator_id: str  # Parent logical operator
    operator_type: OperatorType
    operator_name: str
    task_index: int  # Index within operator parallelism (0 to parallelism-1)
    parallelism: int  # Total parallelism of parent operator

    # State
    state: TaskState = TaskState.CREATED
    state_updated_at: float = field(default_factory=time.time)

    # Topology
    upstream_task_ids: List[str] = field(default_factory=list)
    downstream_task_ids: List[str] = field(default_factory=list)

    # Deployment
    slot_id: Optional[str] = None  # Assigned slot (node + slot index)
    node_id: Optional[str] = None  # Worker node ID

    # Configuration (from logical operator)
    function: Optional[Callable] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    stateful: bool = False
    key_by: Optional[List[str]] = None

    # Resources
    memory_mb: int = 256
    cpu_cores: float = 0.5

    # Metrics
    rows_processed: int = 0
    bytes_processed: int = 0
    failure_count: int = 0
    last_heartbeat: Optional[float] = None

    def update_state(self, new_state: TaskState) -> None:
        """Update task state with timestamp."""
        old_state = self.state
        self.state = new_state
        self.state_updated_at = time.time()
        logger.debug(f"Task {self.task_id} state: {old_state.value} -> {new_state.value}")

    def is_running(self) -> bool:
        """Check if task is currently running."""
        return self.state == TaskState.RUNNING

    def is_terminal(self) -> bool:
        """Check if task is in terminal state."""
        return self.state in [TaskState.FINISHED, TaskState.CANCELED, TaskState.FAILED]

    def can_reschedule(self) -> bool:
        """Check if task can be rescheduled (not currently running)."""
        return self.state in [TaskState.CREATED, TaskState.SCHEDULED, TaskState.FAILED]

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            'task_id': self.task_id,
            'operator_id': self.operator_id,
            'operator_type': self.operator_type.value,
            'operator_name': self.operator_name,
            'task_index': self.task_index,
            'parallelism': self.parallelism,
            'state': self.state.value,
            'state_updated_at': self.state_updated_at,
            'upstream_task_ids': self.upstream_task_ids,
            'downstream_task_ids': self.downstream_task_ids,
            'slot_id': self.slot_id,
            'node_id': self.node_id,
            'parameters': self.parameters,
            'stateful': self.stateful,
            'key_by': self.key_by,
            'memory_mb': self.memory_mb,
            'cpu_cores': self.cpu_cores,
            'rows_processed': self.rows_processed,
            'bytes_processed': self.bytes_processed,
            'failure_count': self.failure_count,
            'last_heartbeat': self.last_heartbeat,
        }


@dataclass
class ShuffleEdge:
    """
    Shuffle edge between operators requiring data redistribution.

    Represents data flow from N upstream tasks to M downstream tasks
    with repartitioning based on shuffle type.
    """

    # Identity
    shuffle_id: str
    upstream_operator_id: str
    downstream_operator_id: str

    # Shuffle configuration
    edge_type: ShuffleEdgeType
    partition_keys: Optional[List[str]] = None  # For hash/range partitioning
    num_partitions: int = 1  # Target number of partitions

    # Task mappings (N upstream × M downstream)
    upstream_task_ids: List[str] = field(default_factory=list)
    downstream_task_ids: List[str] = field(default_factory=list)

    # Metadata
    created_at: float = field(default_factory=time.time)
    rows_shuffled: int = 0
    bytes_shuffled: int = 0

    def requires_network_shuffle(self) -> bool:
        """Check if this shuffle requires network transfer."""
        return self.edge_type != ShuffleEdgeType.FORWARD

    def requires_partitioning(self) -> bool:
        """Check if this shuffle requires data partitioning."""
        return self.edge_type in [
            ShuffleEdgeType.HASH,
            ShuffleEdgeType.RANGE,
            ShuffleEdgeType.REBALANCE
        ]

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            'shuffle_id': self.shuffle_id,
            'upstream_operator_id': self.upstream_operator_id,
            'downstream_operator_id': self.downstream_operator_id,
            'edge_type': self.edge_type.value,
            'partition_keys': self.partition_keys,
            'num_partitions': self.num_partitions,
            'upstream_task_ids': self.upstream_task_ids,
            'downstream_task_ids': self.downstream_task_ids,
            'created_at': self.created_at,
            'rows_shuffled': self.rows_shuffled,
            'bytes_shuffled': self.bytes_shuffled,
        }


@dataclass
class ExecutionGraph:
    """
    Physical execution plan for a streaming job.

    Represents the deployed job with parallel task instances, slot assignments,
    and runtime state tracking. Converted from JobGraph with parallelism expansion.
    """

    job_id: str
    job_name: str

    # Tasks (physical execution units)
    tasks: Dict[str, TaskVertex] = field(default_factory=dict)

    # Operator grouping (operator_id -> list of task IDs)
    operator_tasks: Dict[str, List[str]] = field(default_factory=dict)

    # Shuffle edges (data redistribution between operators)
    shuffle_edges: Dict[str, ShuffleEdge] = field(default_factory=dict)

    # Configuration
    checkpoint_interval_ms: int = 60000
    state_backend: str = "tonbo"

    # Execution state
    start_time: Optional[float] = None
    end_time: Optional[float] = None

    def add_task(self, task: TaskVertex) -> None:
        """
        Add task to execution graph.

        Args:
            task: TaskVertex to add
        """
        self.tasks[task.task_id] = task

        # Group by operator
        if task.operator_id not in self.operator_tasks:
            self.operator_tasks[task.operator_id] = []
        self.operator_tasks[task.operator_id].append(task.task_id)

        logger.debug(f"Added task {task.task_id} (operator {task.operator_id})")

    def connect_tasks(self, upstream_task_id: str, downstream_task_id: str) -> None:
        """
        Connect two tasks (create data flow edge).

        Args:
            upstream_task_id: Source task ID
            downstream_task_id: Destination task ID
        """
        if upstream_task_id not in self.tasks:
            raise ValueError(f"Upstream task {upstream_task_id} not found")
        if downstream_task_id not in self.tasks:
            raise ValueError(f"Downstream task {downstream_task_id} not found")

        upstream = self.tasks[upstream_task_id]
        downstream = self.tasks[downstream_task_id]

        if downstream_task_id not in upstream.downstream_task_ids:
            upstream.downstream_task_ids.append(downstream_task_id)

        if upstream_task_id not in downstream.upstream_task_ids:
            downstream.upstream_task_ids.append(upstream_task_id)

        logger.debug(f"Connected tasks {upstream_task_id} -> {downstream_task_id}")

    def add_shuffle_edge(self, shuffle_edge: ShuffleEdge) -> None:
        """
        Add shuffle edge between operators.

        Args:
            shuffle_edge: ShuffleEdge to add
        """
        self.shuffle_edges[shuffle_edge.shuffle_id] = shuffle_edge
        logger.debug(
            f"Added shuffle edge {shuffle_edge.shuffle_id}: "
            f"{shuffle_edge.upstream_operator_id} -> {shuffle_edge.downstream_operator_id} "
            f"(type: {shuffle_edge.edge_type.value})"
        )

    def get_shuffle_edges_for_operator(self, operator_id: str) -> List[ShuffleEdge]:
        """Get all shuffle edges involving an operator."""
        edges = []
        for edge in self.shuffle_edges.values():
            if edge.upstream_operator_id == operator_id or edge.downstream_operator_id == operator_id:
                edges.append(edge)
        return edges

    def detect_shuffle_edge_type(
        self,
        upstream_op_id: str,
        downstream_op_id: str,
        downstream_stateful: bool,
        downstream_key_by: Optional[List[str]]
    ) -> ShuffleEdgeType:
        """
        Automatically detect shuffle type based on operator characteristics.

        Args:
            upstream_op_id: Upstream operator ID
            downstream_op_id: Downstream operator ID
            downstream_stateful: Is downstream operator stateful?
            downstream_key_by: Key columns for downstream operator

        Returns:
            Appropriate ShuffleEdgeType
        """
        upstream_tasks = self.operator_tasks.get(upstream_op_id, [])
        downstream_tasks = self.operator_tasks.get(downstream_op_id, [])

        # Same parallelism + same physical location → FORWARD (operator chaining)
        if len(upstream_tasks) == len(downstream_tasks):
            # TODO: Check if tasks are co-located
            return ShuffleEdgeType.FORWARD

        # Different parallelism + stateful with keys → HASH
        if downstream_stateful and downstream_key_by:
            return ShuffleEdgeType.HASH

        # Different parallelism + stateless → REBALANCE
        return ShuffleEdgeType.REBALANCE

    def get_task_count(self) -> int:
        """Get total number of tasks."""
        return len(self.tasks)

    def get_operator_parallelism(self, operator_id: str) -> int:
        """Get parallelism of an operator."""
        if operator_id not in self.operator_tasks:
            return 0
        return len(self.operator_tasks[operator_id])

    def get_tasks_by_operator(self, operator_id: str) -> List[TaskVertex]:
        """Get all tasks for a specific operator."""
        if operator_id not in self.operator_tasks:
            return []
        task_ids = self.operator_tasks[operator_id]
        return [self.tasks[tid] for tid in task_ids]

    def get_tasks_by_state(self, state: TaskState) -> List[TaskVertex]:
        """Get all tasks in a specific state."""
        return [task for task in self.tasks.values() if task.state == state]

    def get_source_tasks(self) -> List[TaskVertex]:
        """Get all source tasks (no upstream dependencies)."""
        return [task for task in self.tasks.values() if not task.upstream_task_ids]

    def get_sink_tasks(self) -> List[TaskVertex]:
        """Get all sink tasks (no downstream dependencies)."""
        return [task for task in self.tasks.values() if not task.downstream_task_ids]

    def rescale_operator(self, operator_id: str, new_parallelism: int) -> Dict[str, Any]:
        """
        Rescale an operator to new parallelism (live rescaling).

        This is the core implementation of zero-downtime rescaling:
        1. Create new task instances at new parallelism
        2. Redistribute state across new tasks (if stateful)
        3. Reconnect upstream/downstream tasks
        4. Cancel old tasks after new tasks are running

        Args:
            operator_id: Operator to rescale
            new_parallelism: New parallelism level

        Returns:
            Dictionary with rescaling plan
        """
        if operator_id not in self.operator_tasks:
            raise ValueError(f"Operator {operator_id} not found")

        old_tasks = self.get_tasks_by_operator(operator_id)
        old_parallelism = len(old_tasks)

        if new_parallelism == old_parallelism:
            logger.info(f"Operator {operator_id} already at parallelism {new_parallelism}")
            return {'status': 'no_change', 'parallelism': new_parallelism}

        logger.info(
            f"Rescaling operator {operator_id}: "
            f"{old_parallelism} -> {new_parallelism} tasks"
        )

        # Get operator metadata from first task
        template = old_tasks[0]

        # Create new tasks
        new_task_ids = []
        for task_index in range(new_parallelism):
            new_task = TaskVertex(
                task_id=f"{operator_id}_task_{task_index}_v2",
                operator_id=operator_id,
                operator_type=template.operator_type,
                operator_name=template.operator_name,
                task_index=task_index,
                parallelism=new_parallelism,
                state=TaskState.CREATED,
                function=template.function,
                parameters=template.parameters.copy(),
                stateful=template.stateful,
                key_by=template.key_by,
                memory_mb=template.memory_mb,
                cpu_cores=template.cpu_cores,
            )
            self.add_task(new_task)
            new_task_ids.append(new_task.task_id)

        # Reconnect upstream tasks to new tasks
        upstream_tasks = set()
        for old_task in old_tasks:
            upstream_tasks.update(old_task.upstream_task_ids)

        for upstream_task_id in upstream_tasks:
            # Remove old connections
            upstream = self.tasks[upstream_task_id]
            for old_task in old_tasks:
                if old_task.task_id in upstream.downstream_task_ids:
                    upstream.downstream_task_ids.remove(old_task.task_id)

            # Add new connections (round-robin or hash partition)
            if template.stateful and template.key_by:
                # Hash partition: all upstream connect to all downstream
                for new_task_id in new_task_ids:
                    self.connect_tasks(upstream_task_id, new_task_id)
            else:
                # Round-robin
                upstream_idx = list(upstream_tasks).index(upstream_task_id)
                new_task_id = new_task_ids[upstream_idx % new_parallelism]
                self.connect_tasks(upstream_task_id, new_task_id)

        # Reconnect downstream tasks to new tasks
        downstream_tasks = set()
        for old_task in old_tasks:
            downstream_tasks.update(old_task.downstream_task_ids)

        for downstream_task_id in downstream_tasks:
            # Remove old connections
            downstream = self.tasks[downstream_task_id]
            for old_task in old_tasks:
                if old_task.task_id in downstream.upstream_task_ids:
                    downstream.upstream_task_ids.remove(old_task.task_id)

            # Add new connections
            downstream_op = self.tasks[downstream_task_id]
            if downstream_op.stateful and downstream_op.key_by:
                # All new tasks connect to downstream
                for new_task_id in new_task_ids:
                    self.connect_tasks(new_task_id, downstream_task_id)
            else:
                # Round-robin
                downstream_idx = list(downstream_tasks).index(downstream_task_id)
                new_task_id = new_task_ids[downstream_idx % new_parallelism]
                self.connect_tasks(new_task_id, downstream_task_id)

        # Mark old tasks for cancellation (will be done after new tasks running)
        old_task_ids = [task.task_id for task in old_tasks]

        return {
            'status': 'rescaling',
            'operator_id': operator_id,
            'old_parallelism': old_parallelism,
            'new_parallelism': new_parallelism,
            'old_task_ids': old_task_ids,
            'new_task_ids': new_task_ids,
            'stateful': template.stateful,
            'requires_state_redistribution': template.stateful,
        }

    def get_rescaling_plan(self) -> Dict[str, Any]:
        """
        Get current rescaling status for all operators.

        Returns:
            Dictionary with rescaling information
        """
        rescaling = {}

        for operator_id in self.operator_tasks:
            tasks = self.get_tasks_by_operator(operator_id)

            # Check if any tasks are in RECONCILING state (rescaling in progress)
            reconciling = [t for t in tasks if t.state == TaskState.RECONCILING]

            if reconciling:
                rescaling[operator_id] = {
                    'status': 'in_progress',
                    'parallelism': len(tasks),
                    'reconciling_tasks': len(reconciling),
                }

        return rescaling

    def to_dict(self) -> Dict[str, Any]:
        """Serialize execution graph to dictionary."""
        return {
            'job_id': self.job_id,
            'job_name': self.job_name,
            'tasks': {tid: task.to_dict() for tid, task in self.tasks.items()},
            'operator_tasks': self.operator_tasks,
            'checkpoint_interval_ms': self.checkpoint_interval_ms,
            'state_backend': self.state_backend,
            'start_time': self.start_time,
            'end_time': self.end_time,
        }

    def get_metrics_summary(self) -> Dict[str, Any]:
        """
        Get execution metrics summary.

        Returns:
            Dictionary with aggregate metrics
        """
        running_tasks = len([t for t in self.tasks.values() if t.is_running()])
        failed_tasks = len(self.get_tasks_by_state(TaskState.FAILED))
        total_rows = sum(t.rows_processed for t in self.tasks.values())
        total_bytes = sum(t.bytes_processed for t in self.tasks.values())

        return {
            'total_tasks': len(self.tasks),
            'running_tasks': running_tasks,
            'failed_tasks': failed_tasks,
            'total_rows_processed': total_rows,
            'total_bytes_processed': total_bytes,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'uptime_seconds': time.time() - self.start_time if self.start_time else 0,
        }
