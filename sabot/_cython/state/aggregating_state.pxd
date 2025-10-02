# -*- coding: utf-8 -*-
"""
AggregatingState Cython Header

Header declarations for AggregatingState primitive.
"""

from libc.stdint cimport int64_t, uint64_t

from .state_backend cimport StateBackend


cdef class AggregatingState:
    cdef:
        StateBackend backend
        bytes state_name
        object add_function
        object aggregate_function
        bint ttl_enabled
        int64_t ttl_ms
        uint64_t last_access_time

    # Core operations
    cpdef void add(self, object input_value)
    cpdef object get(self)
    cpdef void clear(self)
    cpdef bint is_empty(self)
    cpdef object get_accumulator(self)
    cpdef void merge(self, object other_accumulator)

    # TTL operations
    cpdef bint is_expired(self)
    cpdef void refresh_ttl(self)

    # Metadata
    cpdef str get_state_name(self)
    cpdef object get_add_function(self)
    cpdef object get_aggregate_function(self)
