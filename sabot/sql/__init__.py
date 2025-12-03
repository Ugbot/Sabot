"""
Sabot SQL Module

SQL query execution with agent-based distributed processing.
Uses DuckDB for parsing/optimization and Sabot morsel operators for execution.

Architecture:
- DuckDB C++ parses SQL and generates logical plans
- StagePartitioner (C++) partitions plans into distributed stages
- StageScheduler (Python) orchestrates wave-based execution
- Cython operators execute each stage with morsel parallelism
"""

from .controller import SQLController
from .agents import SQLScanAgent, SQLJoinAgent, SQLAggregateAgent
from .distributed_plan import (
    DistributedQueryPlan,
    ExecutionStage,
    OperatorSpec,
    ShuffleSpec,
    ShuffleType,
    OperatorType,
)
from .stage_scheduler import StageScheduler, ShuffleCoordinator

__all__ = [
    # Controller
    'SQLController',
    # Agents
    'SQLScanAgent',
    'SQLJoinAgent',
    'SQLAggregateAgent',
    # Distributed Plan
    'DistributedQueryPlan',
    'ExecutionStage',
    'OperatorSpec',
    'ShuffleSpec',
    'ShuffleType',
    'OperatorType',
    # Scheduler
    'StageScheduler',
    'ShuffleCoordinator',
]

