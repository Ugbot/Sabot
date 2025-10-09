# cython: language_level=3
"""
pgoutput Checkpointer - Cython Declarations

LSN-based checkpointing for pgoutput CDC with RocksDB/Tonbo integration.
"""

from libc.stdint cimport uint64_t, int64_t
from libcpp.string cimport string as cpp_string
from libcpp.memory cimport shared_ptr
from pyarrow.includes.libarrow cimport CRecordBatch


cdef class PgoutputCheckpointer:
    """
    LSN-based checkpoint manager for pgoutput CDC.

    Uses RocksDB for metadata (LSN, checkpoint state) and Tonbo for
    buffering uncommitted Arrow batches.
    """

    # Slot identification
    cdef str slot_name
    cdef str checkpoint_dir

    # State backends
    cdef object rocksdb_backend  # RocksDB for metadata
    cdef object tonbo_backend    # Tonbo for batch buffering

    # Current checkpoint state
    cdef uint64_t last_checkpoint_lsn
    cdef int64_t last_checkpoint_timestamp
    cdef uint64_t buffered_batches_count

    # Configuration
    cdef uint64_t checkpoint_interval_lsn  # LSN distance between checkpoints
    cdef double checkpoint_interval_seconds  # Time between checkpoints
    cdef int64_t last_checkpoint_time

    # Statistics
    cdef uint64_t checkpoints_created
    cdef uint64_t checkpoint_failures
    cdef uint64_t batches_recovered

    # Public methods
    cpdef void initialize(self, str slot_name, str checkpoint_dir)
    cpdef uint64_t get_last_checkpoint_lsn(self)
    cpdef void save_checkpoint(self, uint64_t commit_lsn, int64_t commit_timestamp)
    cpdef void buffer_batch(self, uint64_t lsn, object batch)
    cpdef list recover_buffered_batches(self, uint64_t from_lsn)
    cpdef void clear_buffer(self, uint64_t up_to_lsn)
    cpdef object get_checkpoint_stats(self)
    cpdef void close(self)

    # Internal methods
    cdef void _save_lsn_to_rocksdb(self, uint64_t lsn, int64_t timestamp)
    cdef uint64_t _load_lsn_from_rocksdb(self)
    cdef void _buffer_batch_to_tonbo(self, uint64_t lsn, object batch)
    cdef list _load_batches_from_tonbo(self, uint64_t from_lsn)
    cdef bint _should_checkpoint(self, uint64_t current_lsn, int64_t current_time)
