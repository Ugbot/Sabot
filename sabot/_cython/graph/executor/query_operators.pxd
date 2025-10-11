# cython: language_level=3

"""
Query Operators Header File

Cython declarations for graph query operators.
"""

from sabot._cython.operators.base_operator cimport BaseOperator


cdef class PatternScanOperator(BaseOperator):
    cdef public object pattern
    cdef public object graph
    cdef public object match_2hop
    cdef public object match_3hop
    cdef public object match_variable_length_path

    cpdef object process_batch(self, object batch)


cdef class GraphFilterOperator(BaseOperator):
    cdef public object filter_expr

    cpdef object process_batch(self, object batch)
    cdef object _evaluate_expression(self, object batch, object expr)


cdef class GraphProjectOperator(BaseOperator):
    cdef public list columns
    cdef public dict aliases

    cpdef object process_batch(self, object batch)


cdef class GraphLimitOperator(BaseOperator):
    cdef public object limit
    cdef public long long offset
    cdef public long long rows_seen
    cdef public long long rows_returned

    cpdef object process_batch(self, object batch)


cdef class GraphJoinOperator(BaseOperator):
    cdef public list left_keys
    cdef public list right_keys
    cdef public str join_type

    cpdef object process_batch(self, object batch)


cdef class GraphAggregateOperator(BaseOperator):
    cdef public list group_keys
    cdef public dict agg_funcs

    cpdef object process_batch(self, object batch)
