# cython: language_level=3
"""
Zero-copy Arrow buffer access

Provides direct buffer views without Python object allocation.
Performance target: <5ns per access (vs ~50-100ns for .to_numpy())
"""

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t

# Typed memoryview accessors (zero-copy!)
cpdef const int32_t[:] get_int32_buffer(object array)
cpdef const int64_t[:] get_int64_buffer(object array)
cpdef const float[:] get_float32_buffer(object array)
cpdef const double[:] get_float64_buffer(object array)

# Null bitmap access
cpdef const uint8_t[:] get_null_bitmap(object array)
cpdef bint has_nulls(object array)
cpdef size_t count_nulls(object array)

# Convenience functions
cpdef const int64_t[:] get_int64_column(object batch, str column_name)
cpdef const double[:] get_float64_column(object batch, str column_name)
cpdef bint can_zero_copy(object array)

