# -*- coding: utf-8 -*-
"""
Barrier Cython Header

Header declarations for Barrier data structure.
"""

from libc.stdint cimport int64_t, int32_t


cdef class Barrier:
    cdef:
        int64_t checkpoint_id
        int64_t timestamp
        int32_t source_id
        bint is_cancellation_barrier
        object checkpoint_metadata

    # Core properties
    cpdef int64_t get_checkpoint_id(self)
    cpdef int64_t get_timestamp(self)
    cpdef int32_t get_source_id(self)
    cpdef bint is_cancellation(self)

    # Metadata
    cpdef void set_metadata(self, str key, object value)
    cpdef object get_metadata(self, str key, object default_value=?)
    cpdef object get_all_metadata(self)

    # Barrier behavior
    cpdef bint should_trigger_checkpoint(self)
    cpdef bint should_cancel_checkpoint(self)

    # Comparison
    cpdef int compare_to(self, Barrier other)
    cpdef bint is_same_checkpoint(self, Barrier other)

    # Internal helper
    cdef int64_t _get_timestamp_ns(self)

    # Note: Factory methods cannot be @staticmethod cpdef in Cython
    # They are defined as regular Python @staticmethod in the .pyx file
