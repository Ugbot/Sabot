# cython: language_level=3
"""
Shuffle Buffer type definitions - Memory management with spill-to-disk.

Zero-copy implementation using Sabot's direct Arrow C++ bindings.
"""

from libc.stdint cimport int32_t, int64_t, uint8_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.queue cimport queue

# Import Cython Arrow types
cimport pyarrow.lib as pa
from pyarrow.includes.libarrow cimport (
    CRecordBatch as PCRecordBatch,
    CSchema as PCSchema,
    CMemoryPool,
)


cdef class ShuffleBuffer:
    """
    Buffer for accumulating partitioned data before flush.

    Manages memory for a single partition, with automatic flushing
    when size thresholds exceeded and spill-to-disk for backpressure.

    Design inspired by Flink's NetworkBufferPool.
    """
    cdef:
        int32_t partition_id
        int64_t max_rows
        int64_t max_bytes
        shared_ptr[PCSchema] schema_cpp

        # Buffered batches (stored as C++ shared_ptr for efficiency)
        vector[shared_ptr[PCRecordBatch]] batches_cpp
        int64_t total_rows
        int64_t total_bytes

        # Spill state
        cbool spilled
        string spill_path
        object spill_store  # Tonbo store handle

    cdef void add_batch(self, pa.RecordBatch batch)
    cdef pa.RecordBatch get_merged_batch(self)
    cdef cbool should_flush(self) nogil
    cpdef void clear(self) except *
    cdef int64_t num_rows(self) nogil
    cdef int64_t num_bytes(self) nogil


cdef class NetworkBufferPool:
    """
    Global buffer pool for managing shuffle memory.

    Implements Flink-style exclusive + floating buffer allocation:
    - Exclusive buffers: Permanently assigned to each partition (2 per partition)
    - Floating buffers: Shared pool for handling data skew (8 per shuffle)

    Provides natural backpressure when buffers exhausted.
    """
    cdef:
        int32_t buffer_size  # Size per buffer (default 32KB)
        int32_t total_buffers
        int32_t exclusive_per_partition
        int32_t floating_per_shuffle

        # Buffer pools
        queue[uint8_t*] free_exclusive
        queue[uint8_t*] free_floating
        int32_t allocated_exclusive
        int32_t allocated_floating

        # Memory tracking
        int64_t total_allocated_bytes
        int64_t peak_allocated_bytes

        # Locks for thread safety
        object _lock

    cdef uint8_t* allocate_exclusive(self) except NULL
    cdef uint8_t* allocate_floating(self) except NULL
    cdef void release_buffer(self, uint8_t* buffer, cbool is_exclusive)
    cdef int32_t available_buffers(self) nogil
    cdef cbool has_available_buffers(self) nogil


cdef class SpillManager:
    """
    Manages spilling of shuffle data to disk when memory pressure high.

    Uses Arrow IPC for persistent storage with zero-copy integration.
    """
    cdef:
        string base_path
        object tonbo_store  # Tonbo store instance
        int64_t spill_threshold_bytes
        int64_t total_spilled_bytes

    cdef string spill_batch(self, bytes shuffle_id, int32_t partition_id,
                            pa.RecordBatch batch)
    cdef vector[shared_ptr[PCRecordBatch]] read_spilled(self, bytes spill_path)
    cpdef void cleanup_spill(self, bytes spill_path) except *
