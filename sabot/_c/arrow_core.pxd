# cython: language_level=3
"""
Direct Arrow C++ Bindings Header

Header declarations for zero-copy Arrow operations using PyArrow's C++ API.
"""

from libc.stdint cimport int64_t, int32_t, uint64_t, uint32_t, uint8_t
from libcpp cimport bool as cbool
from libcpp.memory cimport shared_ptr

# Import PyArrow's C++ types directly
cimport pyarrow.lib as ca
from pyarrow.includes.libarrow cimport (
    CArray as PCArray,
    CArrayData as PCArrayData,
    CBuffer as PCBuffer,
    CRecordBatch as PCRecordBatch,
    CSchema as PCSchema,
)


# ============================================================================
# Wrapper Classes
# ============================================================================

cdef class ArrowArray:
    cdef shared_ptr[PCArray] _array

    cdef void set_array(self, shared_ptr[PCArray] arr) nogil
    cpdef int64_t length(self)
    cpdef int64_t null_count(self)
    cpdef bint has_nulls(self)
    cpdef int64_t get_int64(self, int64_t i)
    cpdef int64_t sum_int64(self)


cdef class ArrowRecordBatch:
    cdef shared_ptr[PCRecordBatch] _batch

    cdef void set_batch(self, shared_ptr[PCRecordBatch] batch) nogil
    cpdef int64_t num_rows(self)
    cpdef int64_t num_columns(self)
    cpdef ArrowArray column(self, int32_t i)
    cpdef int64_t sum_column_int64(self, int32_t col_idx)


cdef class ArrowComputeEngine:
    cpdef object filter_batch(self, ca.RecordBatch batch, str condition)
    cpdef object hash_join(self, ca.RecordBatch left, ca.RecordBatch right,
                          str left_key, str right_key, str join_type=*)


cdef class ArrowMemoryPool:
    cdef ca.CMemoryPool* _pool


# ============================================================================
# Compatibility Functions (for existing batch_processor.pyx)
# ============================================================================

cdef const int64_t* get_int64_data_ptr(ca.Array arr) nogil
cdef const double* get_float64_data_ptr(ca.Array arr) nogil
cdef const uint8_t* get_validity_bitmap(ca.Array arr) nogil
cdef int64_t get_array_length(ca.Array arr) nogil
cdef int64_t get_batch_num_rows(ca.RecordBatch batch) nogil
cdef int64_t get_batch_num_columns(ca.RecordBatch batch) nogil
cdef ca.Array get_batch_column(ca.RecordBatch batch, int64_t i)
cdef shared_ptr[PCArray] get_batch_column_cpp(ca.RecordBatch batch, int64_t i) nogil
cdef const int64_t* get_int64_data_ptr_cpp(shared_ptr[PCArray] arr_ptr) nogil
cdef const double* get_float64_data_ptr_cpp(shared_ptr[PCArray] arr_ptr) nogil
cdef int64_t get_array_length_cpp(shared_ptr[PCArray] arr_ptr) nogil
cdef int64_t find_column_index(ca.RecordBatch batch, str column_name)
cdef cbool is_null(ca.Array arr, int64_t i) nogil


# ============================================================================
# Compute Functions (Zero-Copy Operations)
# ============================================================================

cpdef object compute_window_ids(object batch, str timestamp_column, int64_t window_size_ms)
cpdef object sort_and_take(object batch, list sort_keys, int64_t limit=*)
cpdef object hash_join_batches(object left_batch, object right_batch,
                               str left_key, str right_key, str join_type=*)


# ============================================================================
# Factory Functions
# ============================================================================

cpdef ArrowArray create_arrow_array()
cpdef ArrowRecordBatch create_arrow_record_batch()
cpdef ArrowComputeEngine create_compute_engine()
cpdef ArrowMemoryPool create_memory_pool()