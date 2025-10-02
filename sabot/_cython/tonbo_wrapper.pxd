# -*- coding: utf-8 -*-
"""
Header file for High-Performance Tonbo Cython Wrapper
"""

from libc.stdint cimport int64_t, int32_t

cdef class FastTonboBackend:
    cdef:
        object _db_path
        object _tonbo_db
        object _event_loop
        bint _initialized
        object _logger

    cdef int64_t _get_current_timestamp_ns(self)

cdef class TonboCythonWrapper:
    cdef:
        FastTonboBackend _backend
        object _serializer
