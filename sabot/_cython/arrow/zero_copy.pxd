# cython: language_level=3
"""
Zero-copy Arrow buffer access

Provides direct buffer views without Python object allocation.
Performance target: <5ns per access (vs ~50-100ns for .to_numpy())
"""

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t
from libcpp cimport bool as cbool

cimport pyarrow.lib as pa

# Typed memoryview accessors (zero-copy!)
cpdef const int8_t[:] get_int8_buffer(object array)
cpdef const int16_t[:] get_int16_buffer(object array)
cpdef const int32_t[:] get_int32_buffer(object array)
cpdef const int64_t[:] get_int64_buffer(object array)
cpdef const uint8_t[:] get_uint8_buffer(object array)
cpdef const uint16_t[:] get_uint16_buffer(object array)
cpdef const uint32_t[:] get_uint32_buffer(object array)
cpdef const uint64_t[:] get_uint64_buffer(object array)
cpdef const float[:] get_float32_buffer(object array)
cpdef const double[:] get_float64_buffer(object array)

# Null bitmap access
cpdef const uint8_t[:] get_null_bitmap(object array)
cpdef cbool has_nulls(object array)
cpdef size_t count_nulls(object array)

# Direct pointer access (for C++ integration)
cdef const void* get_data_ptr(object array)
cdef const uint8_t* get_null_ptr(object array)

