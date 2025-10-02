# -*- coding: utf-8 -*-
"""
ValueState Header

Flink-compatible ValueState[T] primitive in Cython.
Provides O(1) get/set operations for single values.
"""

from libc.stdint cimport int64_t, uint64_t
from libcpp.string cimport string
from cpython.ref cimport PyObject

from .state_backend cimport StateBackend


cdef class ValueState:
    """
    Flink-compatible ValueState[T] primitive.

    Stores a single value per key with O(1) access time.
    Supports TTL and automatic cleanup.
    """

    cdef:
        StateBackend backend
        string state_name
        object default_value
        bint ttl_enabled
        int64_t ttl_ms
        uint64_t last_access_time

    # Core operations
    cpdef void update(self, object value)
    cpdef object value(self)
    cpdef void clear(self)

    # TTL support
    cpdef bint is_expired(self)
    cpdef void refresh_ttl(self)

    # Metadata
    cpdef str get_state_name(self)
    cpdef bint has_value(self)
