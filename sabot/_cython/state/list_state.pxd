# -*- coding: utf-8 -*-
"""
ListState Cython Header

Header declarations for ListState primitive.
"""

from libc.stdint cimport int64_t, uint64_t

from .state_backend cimport StateBackend


cdef class ListState:
    cdef:
        StateBackend backend
        bytes state_name
        bint ttl_enabled
        int64_t ttl_ms
        uint64_t last_access_time

    # Core operations
    cpdef void add(self, object value)
    cpdef void add_all(self, object values)
    cpdef object get(self)
    cpdef void update(self, object values)
    cpdef void clear(self)
    cpdef bint is_empty(self)
    cpdef int size(self)
    cpdef bint contains(self, object value)
    cpdef void remove(self, object value)

    # TTL operations
    cpdef bint is_expired(self)
    cpdef void refresh_ttl(self)

    # Metadata
    cpdef str get_state_name(self)
