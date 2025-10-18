"""
Sabot Query Layer

Unified query representation and optimization.
Enables composition across Stream, SQL, and Graph APIs.
"""

from .logical_plan import (
    LogicalPlan,
    LogicalOperatorType,
    ScanPlan,
    FilterPlan,
    ProjectPlan,
    JoinPlan,
    GroupByPlan,
    WindowPlan,
    LimitPlan,
    LogicalPlanBuilder
)

__all__ = [
    'LogicalPlan',
    'LogicalOperatorType',
    'ScanPlan',
    'FilterPlan',
    'ProjectPlan',
    'JoinPlan',
    'GroupByPlan',
    'WindowPlan',
    'LimitPlan',
    'LogicalPlanBuilder',
]
