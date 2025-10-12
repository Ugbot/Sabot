"""
Sabot SQL Module

SQL query execution with agent-based distributed processing.
Uses DuckDB for parsing/optimization and Sabot morsel operators for execution.
"""

from .controller import SQLController
from .agents import SQLScanAgent, SQLJoinAgent, SQLAggregateAgent

__all__ = [
    'SQLController',
    'SQLScanAgent',
    'SQLJoinAgent',
    'SQLAggregateAgent',
]

