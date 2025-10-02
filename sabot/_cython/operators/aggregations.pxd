# cython: language_level=3
"""
Cython header for aggregation operators.

Defines aggregation operators using Tonbo state backend for columnar state.
"""

from sabot._cython.operators.transform cimport BaseOperator


cdef class CythonGroupByOperator(BaseOperator):
    """GroupBy operator using Tonbo columnar state."""
    cdef list _keys
    cdef dict _aggregations
    cdef object _tonbo_state

    cpdef object process_batch(self, object batch)
    cpdef object get_result(self)


cdef class CythonReduceOperator(BaseOperator):
    """Reduce operator with user-defined aggregation function."""
    cdef object _reduce_func
    cdef object _initial_value
    cdef object _accumulator

    cpdef object process_batch(self, object batch)
    cpdef object get_result(self)


cdef class CythonAggregateOperator(BaseOperator):
    """Aggregate operator with multiple aggregation functions."""
    cdef dict _aggregations
    cdef object _tonbo_state

    cpdef object process_batch(self, object batch)
    cpdef object get_result(self)


cdef class CythonDistinctOperator(BaseOperator):
    """Distinct operator using Tonbo ValueState."""
    cdef list _columns
    cdef object _tonbo_state
    cdef set _seen_keys

    cpdef object process_batch(self, object batch)
