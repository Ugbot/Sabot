# -*- coding: utf-8 -*-
"""
Header file for High-Performance Tonbo Cython Wrapper (FFI version)
"""

from libc.stdint cimport int64_t, int32_t, uint8_t, uintptr_t
from sabot._cython.tonbo_ffi cimport TonboDb

cdef class FastTonboBackend:
    cdef:
        TonboDb* _db
        bytes _db_path_bytes
        bint _initialized
        object _logger

cdef class TonboCythonWrapper:
    cdef:
        FastTonboBackend _backend
        object _serializer
