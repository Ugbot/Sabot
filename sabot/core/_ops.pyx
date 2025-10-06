# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
Core Operators - Zero-Copy Stream Processing

All operators work directly on Arrow buffers via pyarrow.lib cimport.
Hot paths use nogil for maximum performance.

Performance targets (per roadmap):
- Single value access: <10ns
- Batch processing: ~5ns per row
- Aggregate operations: ~1-5μs for 1000 rows
"""

from libc.stdint cimport int64_t, int32_t, uint8_t
from libcpp cimport bool as cbool

cimport cython
cimport pyarrow.lib as ca

# Import our Arrow shims for zero-copy access
from sabot._c.arrow_core cimport (
    get_int64_data_ptr,
    get_float64_data_ptr,
    get_int64_data_ptr_cpp,
    get_float64_data_ptr_cpp,
    get_array_length_cpp,
    get_validity_bitmap,
    get_array_length,
    get_batch_num_rows,
    get_batch_column,
    get_batch_column_cpp,
    is_null,
)
from pyarrow.includes.libarrow cimport CArray
from libcpp.memory cimport shared_ptr


# ============================================================================
# Aggregation Operators
# ============================================================================

@cython.cfunc
cdef int64_t _sum_int64_column_nogil(ca.Array arr) nogil:
    """
    Sum int64 column - pure C with no Python overhead.

    Performance: ~5ns per element
    For 1M elements: ~5ms total

    Args:
        arr: Int64 Arrow array

    Returns:
        Sum of all elements
    """
    cdef const int64_t* data = get_int64_data_ptr(arr)
    cdef int64_t n = get_array_length(arr)
    cdef int64_t total = 0
    cdef int64_t i

    for i in range(n):
        total += data[i]

    return total


@cython.cfunc
cdef int64_t _sum_int64_column_with_nulls_nogil(ca.Array arr) nogil:
    """
    Sum int64 column handling nulls.

    Performance: ~10ns per element (includes null check)

    Args:
        arr: Nullable Int64 Arrow array

    Returns:
        Sum of all non-null elements
    """
    cdef const int64_t* data = get_int64_data_ptr(arr)
    cdef const uint8_t* bitmap = get_validity_bitmap(arr)
    cdef int64_t n = get_array_length(arr)
    cdef int64_t total = 0
    cdef int64_t i
    cdef int64_t byte_idx, bit_idx

    if bitmap == NULL:
        # No nulls - fast path
        for i in range(n):
            total += data[i]
    else:
        # Check validity bitmap
        for i in range(n):
            byte_idx = i >> 3
            bit_idx = i & 7
            if (bitmap[byte_idx] & (1 << bit_idx)) != 0:  # Valid
                total += data[i]

    return total


@cython.cfunc
cdef double _mean_float64_column_nogil(ca.Array arr) nogil:
    """
    Calculate mean of float64 column.

    Performance: ~5ns per element
    """
    cdef const double* data = get_float64_data_ptr(arr)
    cdef int64_t n = get_array_length(arr)
    cdef double total = 0.0
    cdef int64_t i

    for i in range(n):
        total += data[i]

    return total / <double>n if n > 0 else 0.0


@cython.cfunc
cdef int64_t _count_where_int64_nogil(ca.Array arr, int64_t threshold) nogil:
    """
    Count elements greater than threshold.

    Performance: ~5ns per element
    Shows filter-like operation pattern.
    """
    cdef const int64_t* data = get_int64_data_ptr(arr)
    cdef int64_t n = get_array_length(arr)
    cdef int64_t count = 0
    cdef int64_t i

    for i in range(n):
        if data[i] > threshold:
            count += 1

    return count


# ============================================================================
# RecordBatch Operations
# ============================================================================

@cython.cfunc
cdef int64_t _sum_batch_column_nogil(ca.RecordBatch batch, int64_t col_idx) nogil:
    """
    Sum a column in a RecordBatch.

    Performance: ~5ns per row
    This is the HOT PATH for stream processing.

    Args:
        batch: RecordBatch (Cython cdef type)
        col_idx: Column index

    Returns:
        Sum of column
    """
    cdef shared_ptr[CArray] arr_ptr = get_batch_column_cpp(batch, col_idx)
    cdef const int64_t* data = get_int64_data_ptr_cpp(arr_ptr)
    cdef int64_t n = get_array_length_cpp(arr_ptr)
    cdef int64_t total = 0
    cdef int64_t i

    for i in range(n):
        total += data[i]

    return total


# ============================================================================
# Public Python API
# ============================================================================

def sum_int64_column(ca.Array arr, bint handle_nulls=False):
    """
    Sum int64 Arrow array.

    Python-callable wrapper that releases GIL for hot computation.

    Args:
        arr: PyArrow Int64Array (or Cython ca.Array)
        handle_nulls: Whether to check validity bitmap

    Returns:
        int: Sum of all (non-null) elements

    Performance:
        - Without nulls: ~5ns per element
        - With nulls: ~10ns per element
    """
    cdef int64_t result

    with nogil:
        if handle_nulls:
            result = _sum_int64_column_with_nulls_nogil(arr)
        else:
            result = _sum_int64_column_nogil(arr)

    return result


def mean_float64_column(ca.Array arr):
    """
    Calculate mean of float64 Arrow array.

    Releases GIL for computation.
    """
    cdef double result

    with nogil:
        result = _mean_float64_column_nogil(arr)

    return result


def count_where_int64(ca.Array arr, int64_t threshold):
    """
    Count elements greater than threshold.

    Example of filter-like operation.
    """
    cdef int64_t result

    with nogil:
        result = _count_where_int64_nogil(arr, threshold)

    return result


def sum_batch_column(batch, col_name: str):
    """
    Sum a column in a RecordBatch by name.

    This demonstrates the full zero-copy pipeline:
    1. Find column index (requires GIL for string)
    2. Sum column (releases GIL for computation)

    Args:
        batch: PyArrow RecordBatch
        col_name: Column name to sum

    Returns:
        int: Sum of column

    Performance:
        - Column lookup: ~100ns (cached in practice)
        - Sum computation: ~5ns per row (nogil)
        - For 1M rows: ~5ms total
    """
    # Access schema from Python batch (property access)
    schema_obj = batch.schema

    # Find column index using Python API
    col_idx_py = schema_obj.get_field_index(col_name)
    if col_idx_py < 0:
        raise ValueError(f"Column '{col_name}' not found in batch")

    # Convert to Cython types for nogil operation
    cdef ca.RecordBatch cython_batch = batch
    cdef int64_t col_idx = col_idx_py

    # Sum column (release GIL)
    cdef int64_t result
    with nogil:
        result = _sum_batch_column_nogil(cython_batch, col_idx)

    return result


# ============================================================================
# Operator Class (for stateful operations)
# ============================================================================

cdef class StreamOperator:
    """
    Base class for stream operators that work on RecordBatches.

    Subclass this to implement custom operators with zero-copy semantics.
    """

    cpdef ca.RecordBatch process(self, batch):
        """
        Process a RecordBatch and return result.

        Override this in subclasses for custom logic.
        """
        raise NotImplementedError("Subclass must implement process()")


cdef class SumOperator(StreamOperator):
    """
    Example operator that sums a column across batches.

    Demonstrates stateful operator pattern.
    """

    cdef:
        str _column_name
        int64_t _col_idx
        int64_t _running_sum
        cbool _initialized

    def __cinit__(self, str column_name):
        self._column_name = column_name
        self._col_idx = -1
        self._running_sum = 0
        self._initialized = False

    cpdef ca.RecordBatch process(self, batch):
        """
        Process batch and update running sum.

        Returns the same batch (passthrough) with sum tracked internally.
        """
        # Declare all cdef variables at function start
        cdef int64_t batch_sum
        cdef ca.RecordBatch cython_batch

        # Initialize column index on first batch
        if not self._initialized:
            schema_py = batch.schema
            self._col_idx = schema_py.get_field_index(self._column_name)
            if self._col_idx < 0:
                raise ValueError(f"Column '{self._column_name}' not found")
            self._initialized = True

        # Convert to Cython batch for nogil operation
        cython_batch = batch

        # Update running sum (nogil)
        with nogil:
            batch_sum = _sum_batch_column_nogil(cython_batch, self._col_idx)

        self._running_sum += batch_sum

        return batch

    cpdef int64_t get_sum(self):
        """Get the current running sum."""
        return self._running_sum

    cpdef void reset(self):
        """Reset the running sum."""
        self._running_sum = 0
