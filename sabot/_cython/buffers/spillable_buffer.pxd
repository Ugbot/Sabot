# -*- coding: utf-8 -*-
# cython: language_level=3
"""SpillableBuffer declaration file."""

from libc.stdint cimport int64_t

cdef class SpillableBuffer:
    """Memory-managed buffer that spills Arrow batches to MarbleDB."""

    cdef:
        # Memory tracking
        int64_t _memory_used_bytes
        int64_t _memory_threshold_bytes

        # In-memory storage
        list _in_memory_batches
        int64_t _in_memory_count

        # Spill state
        bint _has_spilled
        str _spill_table_name
        object _spill_store  # MarbleDBStoreBackend
        int64_t _spilled_batch_count

        # Schema (inferred from first batch)
        object _schema

        # Configuration
        str _buffer_id
        str _db_path
        object _lock  # threading.Lock
        bint _closed

    # Public methods
    cpdef void append(self, object batch)
    cpdef void clear(self)
    cpdef int64_t memory_used(self)
    cpdef int64_t total_batches(self)
    cpdef bint is_spilled(self)
    cpdef void close(self)
    cpdef void flush(self)

    # Internal methods
    cdef void _trigger_spill(self)
    cdef void _spill_batch(self, object batch)
    cdef void _init_spill_store(self)
