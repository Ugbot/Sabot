# cython: language_level=3
"""
Cython header for join operators.

Defines streaming join operators with smart state backend selection:
- Small build side (<10M rows): In-memory hash map
- Large build side: Tonbo columnar storage
- Time-based joins: RocksDB timers + state
"""

from sabot._cython.operators.transform cimport BaseOperator


cdef class CythonHashJoinOperator(BaseOperator):
    """Vectorized hash join operator using Arrow C++ compute kernels."""
    cdef object _right_source
    cdef object _hash_builder
    cdef list _left_keys
    cdef list _right_keys
    cdef str _join_type



cdef class CythonIntervalJoinOperator(BaseOperator):
    """Interval join operator for time-based joins."""
    cdef object _right_source
    cdef str _time_column
    cdef long _lower_bound
    cdef long _upper_bound
    cdef dict _time_indexed_state
    cdef object _rocksdb_timers



cdef class CythonAsofJoinOperator(BaseOperator):
    """As-of join operator (sorted merge join)."""
    cdef object _right_source
    cdef str _time_column
    cdef str _direction
    cdef list _sorted_state
