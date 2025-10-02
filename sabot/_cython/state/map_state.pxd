# -*- coding: utf-8 -*-
"""
MapState Cython Header

Header declarations for MapState primitive.
"""

from libc.stdint cimport int64_t, uint64_t

from .state_backend cimport StateBackend


cdef class MapState:
    cdef:
        StateBackend backend
        bytes state_name
        bint ttl_enabled
        int64_t ttl_ms
        uint64_t last_access_time

    # Core operations
    cpdef void put(self, object key, object value)
    cpdef void put_all(self, object kv_pairs)
    cpdef object get(self, object key, object default_value=?)
    cpdef object get_all(self)
    cpdef void remove(self, object key)
    cpdef bint contains(self, object key)
    cpdef object keys(self)
    cpdef object values(self)
    cpdef object items(self)
    cpdef void clear(self)
    cpdef bint is_empty(self)
    cpdef int size(self)

    # TTL operations
    cpdef bint is_expired(self)
    cpdef void refresh_ttl(self)

    # Metadata
    cpdef str get_state_name(self)
