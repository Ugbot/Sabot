# cython: language_level=3
"""
Query Planner header file for Cython declarations.
"""

cdef class PatternQueryPlan:
    cdef list edge_tables
    cdef list join_orders
    cdef object output_schema
