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

    cpdef initialize(self)
    cpdef close(self)
    cpdef fast_get(self, str key)
    cpdef fast_insert(self, str key, bytes value)
    cpdef fast_delete(self, str key) except -1
    cpdef fast_scan_range(self, str start_key = *, str end_key = *, int32_t limit = *)
    cpdef fast_batch_insert(self, list key_value_pairs)
    cpdef fast_exists(self, str key) except -1
    cpdef get_stats(self)

cdef class TonboCythonWrapper:
    cdef:
        FastTonboBackend _backend
        object _serializer

    cpdef initialize(self, serializer = *)
    cpdef close(self)
    cpdef get(self, object key)
    cpdef put(self, object key, object value)
    cpdef delete(self, object key) except -1
    cpdef scan(self, object start_key = *, object end_key = *, int limit = *)"
