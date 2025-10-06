# cython: language_level=3
"""
Arrow Batch Processor Cython Header

Header declarations for zero-copy ArrowBatchProcessor using pyarrow.lib.
"""

from libc.stdint cimport int64_t
from libcpp cimport bool as cbool

cimport pyarrow.lib as ca


cdef class ArrowBatchProcessor:
    cdef:
        ca.RecordBatch _batch  # Cython-level RecordBatch
        int64_t _num_rows
        int64_t _num_columns
        dict _column_indices
        cbool _initialized

    # Core operations
    cpdef void initialize_batch(self, ca.RecordBatch batch) except *
    cdef inline int64_t _get_column_index(self, str column_name) except -1
    cdef int64_t _get_int64_value_nogil(self, int64_t col_idx, int64_t row_idx) nogil
    cdef double _get_float64_value_nogil(self, int64_t col_idx, int64_t row_idx) nogil
    cdef int64_t _sum_column_nogil(self, int64_t col_idx) nogil

    cpdef int64_t get_int64(self, str column_name, int64_t row_idx) except? -1
    cpdef double get_float64(self, str column_name, int64_t row_idx) except? -1.0
    cpdef int64_t sum_column(self, str column_name)
    cpdef void process_batch_timestamps(self, str timestamp_column,
                                       object watermark_tracker,
                                       object window_assigner) except *
    cpdef dict get_batch_info(self)
    cpdef ca.RecordBatch get_batch(self)


cdef class ArrowComputeEngine:
    # Static methods - no declaration needed
    pass
