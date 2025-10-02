"""
Cython-accelerated streaming operators for Flink/Spark-level performance.

All operators use Arrow C++ compute kernels for SIMD-accelerated processing
with zero-copy semantics throughout the pipeline.
"""

# Transform operators (stateless)
try:
    from .transform import (
        CythonMapOperator,
        CythonFilterOperator,
        CythonSelectOperator,
        CythonFlatMapOperator,
        CythonUnionOperator,
    )
except ImportError:
    # Fallback if Cython not compiled
    CythonMapOperator = None
    CythonFilterOperator = None
    CythonSelectOperator = None
    CythonFlatMapOperator = None
    CythonUnionOperator = None

# Aggregation operators (stateful)
try:
    from .aggregations import (
        CythonGroupByOperator,
        CythonReduceOperator,
        CythonAggregateOperator,
        CythonDistinctOperator,
    )
except ImportError:
    # Fallback if Cython not compiled
    CythonGroupByOperator = None
    CythonReduceOperator = None
    CythonAggregateOperator = None
    CythonDistinctOperator = None

# Join operators (stateful)
try:
    from .joins import (
        CythonHashJoinOperator,
        CythonIntervalJoinOperator,
        CythonAsofJoinOperator,
    )
except ImportError:
    # Fallback if Cython not compiled
    CythonHashJoinOperator = None
    CythonIntervalJoinOperator = None
    CythonAsofJoinOperator = None

__all__ = [
    # Transform operators
    'CythonMapOperator',
    'CythonFilterOperator',
    'CythonSelectOperator',
    'CythonFlatMapOperator',
    'CythonUnionOperator',
    # Aggregation operators
    'CythonGroupByOperator',
    'CythonReduceOperator',
    'CythonAggregateOperator',
    'CythonDistinctOperator',
    # Join operators
    'CythonHashJoinOperator',
    'CythonIntervalJoinOperator',
    'CythonAsofJoinOperator',
]
