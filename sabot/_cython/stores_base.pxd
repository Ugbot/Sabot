# cython: language_level=3
"""Cython header for stores_base module."""

from libc.stdint cimport uint64_t, int64_t
from libcpp.string cimport string as cpp_string


cdef class BackendConfig:
    cdef:
        cpp_string backend_type
        cpp_string path
        int64_t max_size
        double ttl_seconds
        cpp_string compression
        dict options

    cpdef str get_backend_type(self)
    cpdef str get_path(self)
    cpdef int64_t get_max_size(self)
    cpdef double get_ttl_seconds(self)
    cpdef str get_compression(self)
    cpdef dict get_options(self)


cdef class StoreBackend:
    cdef:
        BackendConfig _config
        object _lock

    cdef BackendConfig get_config(self)


cdef class StoreTransaction:
    cdef:
        StoreBackend _backend
        list _operations

    cpdef list get_operations(self)

# Backwards compatibility
FastStoreConfig = BackendConfig
FastStoreBackend = StoreBackend
FastStoreTransaction = StoreTransaction