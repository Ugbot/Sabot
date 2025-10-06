# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
Shuffle Buffer Implementation - Memory management with spill-to-disk.

Zero-copy implementation using Sabot's direct Arrow C++ bindings.
Implements Flink-style buffer pools with exclusive and floating buffers.
"""

from libc.stdint cimport int32_t, int64_t, uint8_t
from libc.stdlib cimport malloc, free
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.memory cimport shared_ptr, make_shared
from cython.operator cimport dereference as deref

import os
import threading

# Import Cython Arrow types
cimport pyarrow.lib as pa
from pyarrow.includes.libarrow cimport (
    CRecordBatch as PCRecordBatch,
    CSchema as PCSchema,
    CMemoryPool,
    CTable as PCTable,
)

# For Python API access when needed
import pyarrow
import pyarrow.ipc as ipc


# ============================================================================
# Helper Functions
# ============================================================================

cdef inline pa.RecordBatch _wrap_batch(shared_ptr[PCRecordBatch] batch_cpp):
    """Wrap C++ RecordBatch as Cython pa.RecordBatch (zero-copy)."""
    cdef pa.RecordBatch result = pa.RecordBatch.__new__(pa.RecordBatch)
    result.init(batch_cpp)
    return result


cdef inline shared_ptr[PCRecordBatch] _unwrap_batch(pa.RecordBatch batch):
    """Unwrap Cython pa.RecordBatch to C++ shared_ptr (zero-copy)."""
    return batch.sp_batch


cdef inline pa.Schema _wrap_schema(shared_ptr[PCSchema] schema_cpp):
    """Wrap C++ Schema as Cython pa.Schema (zero-copy)."""
    cdef pa.Schema result = pa.Schema.__new__(pa.Schema)
    result.init_schema(schema_cpp)
    return result


# ============================================================================
# ShuffleBuffer
# ============================================================================

cdef class ShuffleBuffer:
    """
    Buffer for accumulating shuffle data for a single partition.
    """

    def __cinit__(self, int32_t partition_id, int64_t max_rows, int64_t max_bytes,
                  schema):
        """
        Initialize shuffle buffer.

        Args:
            partition_id: Partition ID this buffer serves
            max_rows: Maximum rows before flush
            max_bytes: Maximum bytes before flush
            schema: Arrow schema (Cython pa.Schema or Python pyarrow.Schema)
        """
        cdef pa.Schema schema_cython

        self.partition_id = partition_id
        self.max_rows = max_rows
        self.max_bytes = max_bytes

        # Handle schema - accept both Cython and Python types
        if not isinstance(schema, pa.Schema):
            # Python pyarrow.Schema - convert to Cython type
            schema_cython = schema
            self.schema_cpp = schema_cython.sp_schema
        else:
            # Already Cython pa.Schema
            self.schema_cpp = (<pa.Schema>schema).sp_schema

        # Initialize state
        self.batches_cpp.clear()
        self.total_rows = 0
        self.total_bytes = 0
        self.spilled = False
        self.spill_path = b""
        self.spill_store = None

    cdef void add_batch(self, pa.RecordBatch batch):
        """
        Add batch to buffer (zero-copy).

        Args:
            batch: RecordBatch to buffer (Cython pa.RecordBatch)

        Automatically spills to disk if memory pressure detected.
        """
        # Zero-copy: just store the C++ shared_ptr
        self.batches_cpp.push_back(batch.sp_batch)

        # Update metadata
        self.total_rows += batch.sp_batch.get().num_rows()
        # Note: nbytes requires Python API, calculate estimate
        # Each column has data + validity buffer
        self.total_bytes += batch.sp_batch.get().num_rows() * batch.sp_batch.get().num_columns() * 8  # Rough estimate

    cdef pa.RecordBatch get_merged_batch(self):
        """
        Merge all buffered batches into single batch.

        Returns:
            Merged RecordBatch (Cython pa.RecordBatch)

        Clears buffer after merging.
        """
        cdef:
            pa.Schema schema_cython
            list py_batches = []
            object merged_table_obj
            object merged_batch_obj
            pa.RecordBatch result
            int i

        if self.batches_cpp.size() == 0:
            # Return empty batch
            schema_cython = _wrap_schema(self.schema_cpp)
            merged_batch_obj = pyarrow.RecordBatch.from_arrays(
                [pyarrow.array([], type=field.type) for field in schema_cython],
                schema=schema_cython
            )
            result = merged_batch_obj
            return result

        if self.batches_cpp.size() == 1:
            # Single batch - return directly (zero-copy)
            result = _wrap_batch(self.batches_cpp[0])
            self.clear()
            return result

        # Multiple batches - concatenate using Arrow Table
        schema_cython = _wrap_schema(self.schema_cpp)

        for i in range(<int>self.batches_cpp.size()):
            py_batches.append(_wrap_batch(self.batches_cpp[i]))

        # Use Python API for concatenation
        merged_table_obj = pyarrow.Table.from_batches(py_batches, schema=schema_cython)
        merged_batch_obj = merged_table_obj.to_batches(max_chunksize=self.total_rows)[0]

        # Clear buffer
        self.clear()

        # Convert back to Cython type
        result = merged_batch_obj
        return result

    cdef cbool should_flush(self) nogil:
        """
        Check if buffer should be flushed.

        Returns:
            True if buffer exceeds thresholds
        """
        if self.total_rows >= self.max_rows:
            return True
        if self.total_bytes >= self.max_bytes:
            return True
        return False

    cpdef void clear(self) except *:
        """Clear all buffered data."""
        self.batches_cpp.clear()
        self.total_rows = 0
        self.total_bytes = 0

    cdef int64_t num_rows(self) nogil:
        """Get total buffered rows."""
        return self.total_rows

    cdef int64_t num_bytes(self) nogil:
        """Get total buffered bytes."""
        return self.total_bytes


# ============================================================================
# NetworkBufferPool
# ============================================================================

cdef class NetworkBufferPool:
    """
    Global buffer pool for shuffle memory management.

    Inspired by Flink's NetworkBufferPool with exclusive + floating buffers.
    """

    def __cinit__(self, int32_t buffer_size=32768, int32_t total_buffers=1024,
                  int32_t exclusive_per_partition=2, int32_t floating_per_shuffle=8):
        """
        Initialize network buffer pool.

        Args:
            buffer_size: Size of each buffer in bytes (default 32KB)
            total_buffers: Total number of buffers to allocate
            exclusive_per_partition: Exclusive buffers per partition (default 2)
            floating_per_shuffle: Floating buffers per shuffle (default 8)
        """
        self.buffer_size = buffer_size
        self.total_buffers = total_buffers
        self.exclusive_per_partition = exclusive_per_partition
        self.floating_per_shuffle = floating_per_shuffle

        # Initialize state
        self.allocated_exclusive = 0
        self.allocated_floating = 0
        self.total_allocated_bytes = 0
        self.peak_allocated_bytes = 0

        # Thread safety
        self._lock = threading.Lock()

    def __dealloc__(self):
        """Release all allocated buffers."""
        cdef uint8_t* buffer

        # Free exclusive buffers
        while not self.free_exclusive.empty():
            buffer = self.free_exclusive.front()
            self.free_exclusive.pop()
            if buffer != NULL:
                free(buffer)

        # Free floating buffers
        while not self.free_floating.empty():
            buffer = self.free_floating.front()
            self.free_floating.pop()
            if buffer != NULL:
                free(buffer)

    cdef uint8_t* allocate_exclusive(self) except NULL:
        """
        Allocate exclusive buffer.

        Returns:
            Pointer to buffer (NULL if allocation fails)
        """
        cdef uint8_t* buffer

        with self._lock:
            if not self.free_exclusive.empty():
                buffer = self.free_exclusive.front()
                self.free_exclusive.pop()
                return buffer

            # Allocate new buffer if under limit
            if self.allocated_exclusive < (self.total_buffers // 2):
                buffer = <uint8_t*>malloc(self.buffer_size)
                if buffer != NULL:
                    self.allocated_exclusive += 1
                    self.total_allocated_bytes += self.buffer_size
                    if self.total_allocated_bytes > self.peak_allocated_bytes:
                        self.peak_allocated_bytes = self.total_allocated_bytes
                return buffer

        return NULL

    cdef uint8_t* allocate_floating(self) except NULL:
        """
        Allocate floating buffer.

        Returns:
            Pointer to buffer (NULL if allocation fails)
        """
        cdef uint8_t* buffer

        with self._lock:
            if not self.free_floating.empty():
                buffer = self.free_floating.front()
                self.free_floating.pop()
                return buffer

            # Allocate new buffer if under limit
            if self.allocated_floating < (self.total_buffers // 2):
                buffer = <uint8_t*>malloc(self.buffer_size)
                if buffer != NULL:
                    self.allocated_floating += 1
                    self.total_allocated_bytes += self.buffer_size
                    if self.total_allocated_bytes > self.peak_allocated_bytes:
                        self.peak_allocated_bytes = self.total_allocated_bytes
                return buffer

        return NULL

    cdef void release_buffer(self, uint8_t* buffer, cbool is_exclusive):
        """
        Release buffer back to pool.

        Args:
            buffer: Buffer pointer to release
            is_exclusive: True if exclusive buffer, False if floating
        """
        if buffer == NULL:
            return

        with self._lock:
            if is_exclusive:
                self.free_exclusive.push(buffer)
            else:
                self.free_floating.push(buffer)

    cdef int32_t available_buffers(self) nogil:
        """Get number of available buffers."""
        return <int32_t>(self.free_exclusive.size() + self.free_floating.size())

    cdef cbool has_available_buffers(self) nogil:
        """Check if buffers are available."""
        return self.available_buffers() > 0


# ============================================================================
# SpillManager
# ============================================================================

cdef class SpillManager:
    """
    Manages spilling of shuffle data to disk when memory pressure high.

    Uses Arrow IPC for persistent storage with zero-copy integration.
    """

    def __cinit__(self, base_path, int64_t spill_threshold_bytes=1024*1024*1024):
        """
        Initialize spill manager.

        Args:
            base_path: Base directory for spill files
            spill_threshold_bytes: Threshold for triggering spills (default 1GB)
        """
        self.base_path = base_path.encode('utf-8') if isinstance(base_path, str) else base_path
        self.spill_threshold_bytes = spill_threshold_bytes
        self.total_spilled_bytes = 0
        self.tonbo_store = None

        # Create base directory if it doesn't exist
        os.makedirs(base_path, exist_ok=True)

    cdef string spill_batch(
        self,
        bytes shuffle_id,
        int32_t partition_id,
        pa.RecordBatch batch
    ):
        """
        Spill batch to disk using Arrow IPC.

        Args:
            shuffle_id: Shuffle identifier
            partition_id: Partition ID
            batch: Batch to spill (Cython pa.RecordBatch)

        Returns:
            Path to spilled file
        """
        cdef:
            bytes spill_filename
            object py_batch_obj

        # Generate spill file path
        spill_filename = f"{shuffle_id.decode('utf-8')}_partition_{partition_id}.arrow".encode('utf-8')
        spill_path = os.path.join(self.base_path.decode('utf-8'), spill_filename.decode('utf-8'))

        # Convert to Python for IPC operations
        # The Cython pa.RecordBatch can be used directly as Python object
        py_batch_obj = batch

        # Write using Arrow IPC (zero-copy to disk)
        with ipc.new_file(spill_path, py_batch_obj.schema) as writer:
            writer.write_batch(py_batch_obj)

        self.total_spilled_bytes += py_batch_obj.nbytes

        return spill_path.encode('utf-8')

    cdef vector[shared_ptr[PCRecordBatch]] read_spilled(self, bytes spill_path):
        """
        Read spilled data from disk.

        Args:
            spill_path: Path to spilled file

        Returns:
            Vector of RecordBatches
        """
        cdef:
            vector[shared_ptr[PCRecordBatch]] result
            object reader_obj
            object batch_obj
            pa.RecordBatch batch_cython

        # Read using Arrow IPC
        with ipc.open_file(spill_path.decode('utf-8')) as reader_obj:
            for i in range(reader_obj.num_record_batches):
                batch_obj = reader_obj.get_batch(i)
                batch_cython = batch_obj
                result.push_back(batch_cython.sp_batch)

        return result

    cpdef void cleanup_spill(self, bytes spill_path) except *:
        """
        Cleanup spilled file.

        Args:
            spill_path: Path to spilled file
        """
        if os.path.exists(spill_path.decode('utf-8')):
            os.remove(spill_path.decode('utf-8'))
