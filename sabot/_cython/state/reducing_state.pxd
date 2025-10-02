# -*- coding: utf-8 -*-
"""
ReducingState Cython Header

Header declarations for ReducingState primitive.
"""

from libc.stdint cimport int64_t, uint64_t

from .state_backend cimport StateBackend


cdef class ReducingState:
    cdef:
        StateBackend backend
        bytes state_name
        object reduce_function
        bint ttl_enabled
        int64_t ttl_ms
        uint64_t last_access_time

    # Core operations
    cpdef void add(self, object value)
    cpdef object get(self)
    cpdef void clear(self)
    cpdef bint is_empty(self)

    # TTL operations
    cpdef bint is_expired(self)
    cpdef void refresh_ttl(self)

    # Metadata
    cpdef str get_state_name(self)
    cpdef object get_reduce_function(self)
