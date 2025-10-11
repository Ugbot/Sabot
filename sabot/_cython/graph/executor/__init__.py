"""
Graph Query Execution Engine

Morsel-based, shuffle-aware execution for SPARQL and Cypher queries.
"""

try:
    from .query_operators import (
        PatternScanOperator,
        GraphFilterOperator,
        GraphProjectOperator,
        GraphLimitOperator,
        GraphJoinOperator,
        GraphAggregateOperator,
    )
    from .query_executor import QueryExecutor
    from .physical_plan_builder import PhysicalPlanBuilder

    __all__ = [
        'PatternScanOperator',
        'GraphFilterOperator',
        'GraphProjectOperator',
        'GraphLimitOperator',
        'GraphJoinOperator',
        'GraphAggregateOperator',
        'QueryExecutor',
        'PhysicalPlanBuilder',
    ]
except ImportError as e:
    print(f"Warning: Could not import executor components: {e}")
    __all__ = []
