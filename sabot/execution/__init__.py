#!/usr/bin/env python3
"""
Sabot Execution Layer

Provides Flink/Spark-style job execution with:
- Job Graph: Logical DAG representation of streaming jobs
- Execution Graph: Physical execution plan with task parallelism
- Slot Pool: Resource management for task deployment
- Live Rescaling: Zero-downtime operator parallelism changes
- Dynamic Rebalancing: Automatic load distribution

Integrates with morsel-based parallelism for fine-grained work distribution.
"""

from .job_graph import (
    StreamOperatorNode,
    JobGraph,
    OperatorType,
)

from .execution_graph import (
    TaskVertex,
    ExecutionGraph,
    TaskState,
)

__all__ = [
    'StreamOperatorNode',
    'JobGraph',
    'OperatorType',
    'TaskVertex',
    'ExecutionGraph',
    'TaskState',
]
