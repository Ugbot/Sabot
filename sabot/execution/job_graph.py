#!/usr/bin/env python3
"""
Job Graph - Logical DAG Representation

Defines the logical structure of streaming jobs before physical deployment.
Similar to Flink's StreamGraph and Spark's LogicalPlan.

Key Concepts:
- StreamOperatorNode: Logical operator (map, filter, join, etc.)
- JobGraph: DAG of operators with edge dependencies
- Conversion to ExecutionGraph for physical deployment
"""

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Callable, Set
from enum import Enum
import uuid

logger = logging.getLogger(__name__)


class OperatorType(Enum):
    """Streaming operator types matching Cython operator library."""

    # Transform operators
    SOURCE = "source"
    FILTER = "filter"
    MAP = "map"
    FLAT_MAP = "flat_map"
    SELECT = "select"
    UNION = "union"

    # Aggregation operators
    AGGREGATE = "aggregate"
    REDUCE = "reduce"
    DISTINCT = "distinct"
    GROUP_BY = "group_by"

    # Join operators
    HASH_JOIN = "hash_join"
    INTERVAL_JOIN = "interval_join"
    ASOF_JOIN = "asof_join"

    # Window operators
    TUMBLING_WINDOW = "tumbling_window"
    SLIDING_WINDOW = "sliding_window"
    SESSION_WINDOW = "session_window"

    # Sink operators
    SINK = "sink"


@dataclass
class StreamOperatorNode:
    """
    Logical operator node in the job graph.

    Represents a single transformation in the streaming pipeline before
    physical deployment (no parallelism, no task assignment yet).
    """

    # Identity
    operator_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    operator_type: OperatorType = OperatorType.MAP
    name: Optional[str] = None

    # Topology
    upstream_ids: List[str] = field(default_factory=list)
    downstream_ids: List[str] = field(default_factory=list)

    # Configuration
    function: Optional[Callable] = None  # User function (lambda, UDF, etc.)
    parameters: Dict[str, Any] = field(default_factory=dict)

    # Parallelism hints (used for execution graph generation)
    parallelism: int = 1  # Default parallelism
    max_parallelism: int = 128  # Maximum rescaling parallelism

    # State management
    stateful: bool = False  # Does this operator have keyed state?
    key_by: Optional[List[str]] = None  # Key columns for state partitioning

    # Resource hints
    memory_mb: int = 256  # Memory per task instance
    cpu_cores: float = 0.5  # CPU cores per task instance

    def __post_init__(self):
        """Initialize derived fields."""
        if self.name is None:
            self.name = f"{self.operator_type.value}_{self.operator_id[:8]}"

        # Auto-detect statefulness
        if self.operator_type in [OperatorType.AGGREGATE, OperatorType.GROUP_BY,
                                   OperatorType.DISTINCT, OperatorType.HASH_JOIN]:
            self.stateful = True

    def add_upstream(self, operator_id: str) -> None:
        """Add upstream dependency."""
        if operator_id not in self.upstream_ids:
            self.upstream_ids.append(operator_id)

    def add_downstream(self, operator_id: str) -> None:
        """Add downstream dependency."""
        if operator_id not in self.downstream_ids:
            self.downstream_ids.append(operator_id)

    def is_source(self) -> bool:
        """Check if this is a source operator."""
        return self.operator_type == OperatorType.SOURCE

    def is_sink(self) -> bool:
        """Check if this is a sink operator."""
        return self.operator_type == OperatorType.SINK

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for persistence."""
        return {
            'operator_id': self.operator_id,
            'operator_type': self.operator_type.value,
            'name': self.name,
            'upstream_ids': self.upstream_ids,
            'downstream_ids': self.downstream_ids,
            'parameters': self.parameters,
            'parallelism': self.parallelism,
            'max_parallelism': self.max_parallelism,
            'stateful': self.stateful,
            'key_by': self.key_by,
            'memory_mb': self.memory_mb,
            'cpu_cores': self.cpu_cores,
        }


@dataclass
class JobGraph:
    """
    Logical DAG representation of a streaming job.

    Similar to Flink's StreamGraph - represents the logical structure
    before physical deployment with task parallelism and slot allocation.
    """

    job_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    job_name: str = "unnamed_job"

    # Operators
    operators: Dict[str, StreamOperatorNode] = field(default_factory=dict)

    # Job-level configuration
    default_parallelism: int = 4
    checkpoint_interval_ms: int = 60000  # 1 minute
    state_backend: str = "tonbo"  # 'tonbo', 'rocksdb', 'memory'

    # Metadata
    created_at: Optional[float] = None

    def add_operator(self, operator: StreamOperatorNode) -> str:
        """
        Add operator to job graph.

        Args:
            operator: StreamOperatorNode to add

        Returns:
            Operator ID
        """
        self.operators[operator.operator_id] = operator
        logger.debug(f"Added operator {operator.name} ({operator.operator_id})")
        return operator.operator_id

    def connect(self, upstream_id: str, downstream_id: str) -> None:
        """
        Connect two operators (create edge).

        Args:
            upstream_id: Source operator ID
            downstream_id: Destination operator ID
        """
        if upstream_id not in self.operators:
            raise ValueError(f"Upstream operator {upstream_id} not found")
        if downstream_id not in self.operators:
            raise ValueError(f"Downstream operator {downstream_id} not found")

        self.operators[upstream_id].add_downstream(downstream_id)
        self.operators[downstream_id].add_upstream(upstream_id)

        logger.debug(f"Connected {upstream_id} -> {downstream_id}")

    def get_sources(self) -> List[StreamOperatorNode]:
        """Get all source operators (no upstream dependencies)."""
        return [op for op in self.operators.values() if op.is_source()]

    def get_sinks(self) -> List[StreamOperatorNode]:
        """Get all sink operators (no downstream dependencies)."""
        return [op for op in self.operators.values() if op.is_sink()]

    def topological_sort(self) -> List[StreamOperatorNode]:
        """
        Topological sort of operators (sources first, sinks last).

        Returns:
            List of operators in topological order

        Raises:
            ValueError: If graph has cycles
        """
        # Kahn's algorithm
        in_degree = {op_id: len(op.upstream_ids)
                     for op_id, op in self.operators.items()}

        queue = [op_id for op_id, degree in in_degree.items() if degree == 0]
        sorted_ops = []

        while queue:
            op_id = queue.pop(0)
            sorted_ops.append(self.operators[op_id])

            # Reduce in-degree for downstream operators
            for downstream_id in self.operators[op_id].downstream_ids:
                in_degree[downstream_id] -= 1
                if in_degree[downstream_id] == 0:
                    queue.append(downstream_id)

        if len(sorted_ops) != len(self.operators):
            raise ValueError("Job graph has cycles - cannot topologically sort")

        return sorted_ops

    def validate(self) -> bool:
        """
        Validate job graph structure.

        Returns:
            True if valid

        Raises:
            ValueError: If graph is invalid
        """
        # Check for cycles
        try:
            self.topological_sort()
        except ValueError as e:
            raise ValueError(f"Invalid job graph: {e}")

        # Check that all upstream/downstream references exist
        for op in self.operators.values():
            for upstream_id in op.upstream_ids:
                if upstream_id not in self.operators:
                    raise ValueError(
                        f"Operator {op.operator_id} references non-existent "
                        f"upstream {upstream_id}"
                    )
            for downstream_id in op.downstream_ids:
                if downstream_id not in self.operators:
                    raise ValueError(
                        f"Operator {op.operator_id} references non-existent "
                        f"downstream {downstream_id}"
                    )

        # Check that sources have no upstream
        for op in self.get_sources():
            if op.upstream_ids:
                raise ValueError(f"Source operator {op.operator_id} has upstream dependencies")

        logger.info(f"Job graph validated: {len(self.operators)} operators")
        return True

    def to_execution_graph(self):
        """
        Convert logical job graph to physical execution graph.

        This expands each operator into parallel task instances based on
        parallelism settings and creates task vertices for deployment.

        Returns:
            ExecutionGraph ready for deployment
        """
        from .execution_graph import ExecutionGraph, TaskVertex, TaskState

        self.validate()

        exec_graph = ExecutionGraph(
            job_id=self.job_id,
            job_name=self.job_name,
            checkpoint_interval_ms=self.checkpoint_interval_ms,
            state_backend=self.state_backend,
        )

        # Map operator_id -> list of task vertex IDs (one per parallel instance)
        operator_to_tasks: Dict[str, List[str]] = {}

        # Create task vertices for each operator (expand parallelism)
        for operator in self.topological_sort():
            parallelism = operator.parallelism or self.default_parallelism
            task_ids = []

            for task_index in range(parallelism):
                task = TaskVertex(
                    task_id=f"{operator.operator_id}_task_{task_index}",
                    operator_id=operator.operator_id,
                    operator_type=operator.operator_type,
                    operator_name=operator.name,
                    task_index=task_index,
                    parallelism=parallelism,
                    state=TaskState.CREATED,
                    function=operator.function,
                    parameters=operator.parameters,
                    stateful=operator.stateful,
                    key_by=operator.key_by,
                    memory_mb=operator.memory_mb,
                    cpu_cores=operator.cpu_cores,
                )

                task_ids.append(task.task_id)
                exec_graph.add_task(task)

            operator_to_tasks[operator.operator_id] = task_ids

        # Create task edges and shuffle edges (connect parallel instances)
        from .execution_graph import ShuffleEdge, ShuffleEdgeType
        import uuid

        for operator in self.operators.values():
            upstream_tasks = operator_to_tasks[operator.operator_id]

            for downstream_op_id in operator.downstream_ids:
                downstream_op = self.operators[downstream_op_id]
                downstream_tasks = operator_to_tasks[downstream_op_id]

                # Detect shuffle type based on operator characteristics
                shuffle_type = exec_graph.detect_shuffle_edge_type(
                    operator.operator_id,
                    downstream_op_id,
                    downstream_op.stateful,
                    downstream_op.key_by
                )

                # Create shuffle edge metadata
                shuffle_edge = ShuffleEdge(
                    shuffle_id=f"shuffle_{operator.operator_id}_{downstream_op_id}_{str(uuid.uuid4())[:8]}",
                    upstream_operator_id=operator.operator_id,
                    downstream_operator_id=downstream_op_id,
                    edge_type=shuffle_type,
                    partition_keys=downstream_op.key_by if downstream_op.stateful else None,
                    num_partitions=len(downstream_tasks),
                    upstream_task_ids=upstream_tasks.copy(),
                    downstream_task_ids=downstream_tasks.copy(),
                )
                exec_graph.add_shuffle_edge(shuffle_edge)

                # Connect tasks based on shuffle type
                if shuffle_type == ShuffleEdgeType.FORWARD:
                    # 1:1 mapping (operator chaining, no shuffle)
                    for i in range(min(len(upstream_tasks), len(downstream_tasks))):
                        exec_graph.connect_tasks(upstream_tasks[i], downstream_tasks[i])

                elif shuffle_type == ShuffleEdgeType.HASH:
                    # Hash partition: All upstream connect to all downstream (N×M)
                    # Data will be partitioned by key at runtime
                    for up_task in upstream_tasks:
                        for down_task in downstream_tasks:
                            exec_graph.connect_tasks(up_task, down_task)

                elif shuffle_type == ShuffleEdgeType.REBALANCE:
                    # Round-robin: Each upstream connects to subset of downstream
                    for i, up_task in enumerate(upstream_tasks):
                        down_task = downstream_tasks[i % len(downstream_tasks)]
                        exec_graph.connect_tasks(up_task, down_task)

                elif shuffle_type == ShuffleEdgeType.BROADCAST:
                    # Broadcast: All upstream connect to all downstream
                    for up_task in upstream_tasks:
                        for down_task in downstream_tasks:
                            exec_graph.connect_tasks(up_task, down_task)

                else:
                    # Default: round-robin
                    for i, up_task in enumerate(upstream_tasks):
                        down_task = downstream_tasks[i % len(downstream_tasks)]
                        exec_graph.connect_tasks(up_task, down_task)

        logger.info(
            f"Created execution graph: {exec_graph.get_task_count()} tasks "
            f"from {len(self.operators)} operators"
        )

        return exec_graph

    def to_dict(self) -> Dict[str, Any]:
        """Serialize job graph to dictionary."""
        return {
            'job_id': self.job_id,
            'job_name': self.job_name,
            'operators': {op_id: op.to_dict() for op_id, op in self.operators.items()},
            'default_parallelism': self.default_parallelism,
            'checkpoint_interval_ms': self.checkpoint_interval_ms,
            'state_backend': self.state_backend,
            'created_at': self.created_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'JobGraph':
        """Deserialize job graph from dictionary."""
        job_graph = cls(
            job_id=data['job_id'],
            job_name=data['job_name'],
            default_parallelism=data.get('default_parallelism', 4),
            checkpoint_interval_ms=data.get('checkpoint_interval_ms', 60000),
            state_backend=data.get('state_backend', 'tonbo'),
            created_at=data.get('created_at'),
        )

        # Reconstruct operators
        for op_id, op_data in data['operators'].items():
            operator = StreamOperatorNode(
                operator_id=op_data['operator_id'],
                operator_type=OperatorType(op_data['operator_type']),
                name=op_data['name'],
                upstream_ids=op_data['upstream_ids'],
                downstream_ids=op_data['downstream_ids'],
                parameters=op_data['parameters'],
                parallelism=op_data['parallelism'],
                max_parallelism=op_data['max_parallelism'],
                stateful=op_data['stateful'],
                key_by=op_data.get('key_by'),
                memory_mb=op_data['memory_mb'],
                cpu_cores=op_data['cpu_cores'],
            )
            job_graph.operators[op_id] = operator

        return job_graph
