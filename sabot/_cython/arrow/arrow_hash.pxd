# cython: language_level=3
"""
Arrow C++ hash function declarations.

Wraps Arrow's high-performance XXH3-based hash functions.
"""

from libc.stdint cimport int64_t, uint64_t, uint32_t, uint8_t

cdef extern from "arrow/util/hashing.h" namespace "arrow::internal" nogil:
    # Main string hash function (uses XXH3 for >16 bytes, optimized for <=16 bytes)
    # Template instantiation for AlgNum=0
    uint64_t ComputeStringHash_0 "arrow::internal::ComputeStringHash<0>"(const void* data, int64_t length)
