# cython: language_level=3
"""
Arrow Core Shims - Zero-Copy Buffer Access

Provides Cython-level access to Arrow types via pyarrow.lib cimport.
All hot-path operations use direct buffer pointers with no Python overhead.

Key principle: Use Buffer.address() to get raw const uint8_t* pointers,
then cast to appropriate types for direct memory access.
"""

from libc.stdint cimport int64_t, int32_t, int16_t, int8_t, uint64_t, uint32_t, uint16_t, uint8_t
from libcpp cimport bool as cbool
from libcpp.memory cimport shared_ptr

# Import PyArrow's Cython API
cimport pyarrow.lib as pa
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport (
    CArray, CArrayData, CBuffer, CRecordBatch, CSchema
)


# ============================================================================
# Buffer Access Helpers
# ============================================================================

cdef inline const uint8_t* get_buffer_address(pa.Buffer buf) nogil:
    """
    Get raw pointer to Arrow buffer data.

    Zero-copy access: returns address of underlying memory.
    Performance: <1ns

    Args:
        buf: Arrow Buffer object

    Returns:
        const uint8_t*: Pointer to buffer memory
    """
    # Access C++ Buffer directly through shared_ptr
    return buf.buffer.get().data()


cdef inline int64_t get_buffer_size(pa.Buffer buf) nogil:
    """
    Get buffer size in bytes.

    Performance: <1ns
    """
    return buf.buffer.get().size()


# ============================================================================
# Array Access Helpers
# ============================================================================

cdef inline int64_t get_array_length(pa.Array arr) nogil:
    """
    Get array length.

    Performance: <1ns (direct struct access)
    """
    return arr.ap.length()


cdef inline int64_t get_array_null_count(pa.Array arr) nogil:
    """Get number of null values in array."""
    return arr.ap.null_count()


cdef inline const int64_t* get_int64_data_ptr(pa.Array arr) nogil:
    """
    Get direct pointer to int64 array data buffer.

    Buffer layout for primitive types:
        buffers[0] = validity bitmap (null mask)
        buffers[1] = data buffer

    Returns pointer to data buffer (buffers[1]).

    Performance: ~5ns
    Usage: Direct indexing in nogil loops

    Example:
        cdef const int64_t* data = get_int64_data_ptr(arr)
        cdef int64_t value = data[i]  # <10ns access
    """
    # Access C++ API directly: arr.ap (CArray*) -> data() -> buffers[1] -> data()
    cdef shared_ptr[CArrayData] array_data = arr.ap.data()
    cdef const uint8_t* addr = array_data.get().buffers[1].get().data()
    return <const int64_t*>addr


cdef inline const int32_t* get_int32_data_ptr(pa.Array arr) nogil:
    """Get direct pointer to int32 array data buffer."""
    cdef shared_ptr[CArrayData] array_data = arr.ap.data()
    cdef const uint8_t* addr = array_data.get().buffers[1].get().data()
    return <const int32_t*>addr


cdef inline const int16_t* get_int16_data_ptr(pa.Array arr) nogil:
    """Get direct pointer to int16 array data buffer."""
    cdef shared_ptr[CArrayData] array_data = arr.ap.data()
    cdef const uint8_t* addr = array_data.get().buffers[1].get().data()
    return <const int16_t*>addr


cdef inline const double* get_float64_data_ptr(pa.Array arr) nogil:
    """Get direct pointer to float64 array data buffer."""
    cdef shared_ptr[CArrayData] array_data = arr.ap.data()
    cdef const uint8_t* addr = array_data.get().buffers[1].get().data()
    return <const double*>addr


cdef inline const float* get_float32_data_ptr(pa.Array arr) nogil:
    """Get direct pointer to float32 array data buffer."""
    cdef shared_ptr[CArrayData] array_data = arr.ap.data()
    cdef const uint8_t* addr = array_data.get().buffers[1].get().data()
    return <const float*>addr


cdef inline const uint8_t* get_validity_bitmap(pa.Array arr) nogil:
    """
    Get pointer to validity bitmap (null mask).

    Bitmap layout: bit 1 = valid, bit 0 = null
    Returns: pointer to buffers[0] (validity bitmap)

    Performance: ~5ns

    Note: If NULL, the array has no nulls.
    """
    cdef shared_ptr[CArrayData] array_data = arr.ap.data()
    if array_data.get().buffers.size() > 0 and array_data.get().buffers[0].get() != NULL:
        return array_data.get().buffers[0].get().data()
    return NULL


cdef inline cbool is_null(pa.Array arr, int64_t i) nogil:
    """
    Check if value at index i is null.

    Uses validity bitmap - bit 1 = valid, bit 0 = null
    Performance: ~10ns

    Args:
        arr: Arrow array
        i: Element index

    Returns:
        True if null, False if valid
    """
    cdef const uint8_t* bitmap = get_validity_bitmap(arr)
    if bitmap == NULL:
        return False  # No validity buffer = no nulls

    cdef int64_t byte_idx = i >> 3  # i / 8
    cdef int64_t bit_idx = i & 7     # i % 8
    return (bitmap[byte_idx] & (1 << bit_idx)) == 0


# ============================================================================
# RecordBatch Access Helpers
# ============================================================================

cdef inline int64_t get_batch_num_rows(pa.RecordBatch batch) nogil:
    """Get number of rows in RecordBatch."""
    return batch.batch.num_rows()


cdef inline int64_t get_batch_num_columns(pa.RecordBatch batch) nogil:
    """Get number of columns in RecordBatch."""
    return batch.batch.num_columns()


cdef inline shared_ptr[CArray] get_batch_column_cpp(pa.RecordBatch batch, int64_t i) nogil:
    """
    Get C++ shared_ptr to column array from RecordBatch by index.

    Performance: <5ns (direct access)

    Args:
        batch: RecordBatch
        i: Column index

    Returns:
        shared_ptr[CArray]: C++ array pointer (can be used in nogil)
    """
    return batch.batch.column(i)


cdef inline pa.Array get_batch_column(pa.RecordBatch batch, int64_t i):
    """
    Get column array from RecordBatch by index (requires GIL).

    This wraps the C++ array in a Python pa.Array object.

    Args:
        batch: RecordBatch
        i: Column index

    Returns:
        pa.Array: Column array (Python-wrapped)
    """
    # Get C++ array and wrap it in pa.Array
    cdef shared_ptr[CArray] cpp_array = batch.batch.column(i)
    cdef pa.Array result = pa.Array.__new__(pa.Array)
    result.init(cpp_array)
    return result


# ============================================================================
# C++ Array Data Access (nogil-safe)
# ============================================================================

cdef inline const int64_t* get_int64_data_ptr_cpp(shared_ptr[CArray] arr_ptr) nogil:
    """
    Get int64 data pointer from C++ Array shared_ptr (nogil-safe).

    This is the preferred method for hot loops.
    """
    cdef shared_ptr[CArrayData] array_data = arr_ptr.get().data()
    cdef const uint8_t* addr = array_data.get().buffers[1].get().data()
    return <const int64_t*>addr


cdef inline const double* get_float64_data_ptr_cpp(shared_ptr[CArray] arr_ptr) nogil:
    """
    Get float64 data pointer from C++ Array shared_ptr (nogil-safe).
    """
    cdef shared_ptr[CArrayData] array_data = arr_ptr.get().data()
    cdef const uint8_t* addr = array_data.get().buffers[1].get().data()
    return <const double*>addr


cdef inline int64_t get_array_length_cpp(shared_ptr[CArray] arr_ptr) nogil:
    """Get array length from C++ shared_ptr (nogil-safe)."""
    return arr_ptr.get().length()


# ============================================================================
# Fast Column Extraction (by name)
# ============================================================================

cdef inline int64_t find_column_index(pa.RecordBatch batch, str column_name):
    """
    Find column index by name.

    Note: Requires GIL for string comparison.
    Cache the result in hot paths to avoid repeated lookups.

    Args:
        batch: RecordBatch
        column_name: Column name to find

    Returns:
        Column index, or -1 if not found
    """
    cdef pa.Schema schema = batch.schema()
    return schema.get_field_index(column_name)


# ============================================================================
# Type-Specific Fast Path Examples
# ============================================================================

cdef inline int64_t sum_int64_array(pa.Array arr) nogil:
    """
    Sum all values in int64 array - zero-copy hot loop.

    Example of nogil tight loop over Arrow buffer.
    Performance: ~5ns per element

    This is a reference implementation showing the pattern for
    high-performance array operations.
    """
    cdef const int64_t* data = get_int64_data_ptr(arr)
    cdef int64_t n = get_array_length(arr)
    cdef int64_t s = 0
    cdef int64_t i

    for i in range(n):
        s += data[i]

    return s


cdef inline int64_t sum_int64_array_with_nulls(pa.Array arr) nogil:
    """
    Sum int64 array handling nulls via validity bitmap.

    Performance: ~10ns per element (includes null check)

    Shows how to handle nullable arrays efficiently.
    """
    cdef const int64_t* data = get_int64_data_ptr(arr)
    cdef const uint8_t* bitmap = get_validity_bitmap(arr)
    cdef int64_t n = get_array_length(arr)
    cdef int64_t s = 0
    cdef int64_t i
    cdef int64_t byte_idx, bit_idx

    if bitmap == NULL:
        # No nulls - fast path
        for i in range(n):
            s += data[i]
    else:
        # Check validity bitmap
        for i in range(n):
            byte_idx = i >> 3
            bit_idx = i & 7
            if (bitmap[byte_idx] & (1 << bit_idx)) != 0:  # Valid
                s += data[i]

    return s


cdef inline double mean_float64_array(pa.Array arr) nogil:
    """
    Calculate mean of float64 array.

    Example showing float operations.
    Performance: ~5ns per element
    """
    cdef const double* data = get_float64_data_ptr(arr)
    cdef int64_t n = get_array_length(arr)
    cdef double s = 0.0
    cdef int64_t i

    for i in range(n):
        s += data[i]

    return s / <double>n if n > 0 else 0.0
