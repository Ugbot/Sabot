# cython: language_level=3
"""
pgoutput CDC Reader - Cython Declarations

Declarations for pgoutput-based PostgreSQL CDC reader.
"""

from libc.stdint cimport uint32_t, uint64_t
from libcpp.memory cimport shared_ptr
from pyarrow.includes.libarrow cimport CRecordBatch, CSchema

# Forward declarations (don't import cdef classes here)
# Types will be resolved at compile time


cdef class PgoutputCDCReader:
    """
    PostgreSQL CDC reader using built-in pgoutput plugin.

    Zero-copy Arrow RecordBatch streaming without custom plugin requirements.
    """

    # Connection and parser (use object type to avoid circular imports)
    cdef object conn  # PostgreSQLConnection
    cdef object parser  # PgoutputParser
    cdef str slot_name
    cdef list publication_names

    # Reader state
    cdef bint schema_read
    cdef uint64_t batch_count
    cdef bint reader_initialized

    # Current transaction state
    cdef uint32_t current_xid
    cdef uint32_t current_relation_oid
    cdef list pending_batches

    # Statistics
    cdef uint64_t messages_received
    cdef uint64_t transactions_processed
    cdef uint64_t last_commit_lsn

    # Public methods
    cpdef bint is_initialized(self)
    cpdef object get_schema(self, uint32_t relation_oid)
    cpdef uint64_t get_batch_count(self)
    cpdef object get_stats(self)

    # Internal methods
    cdef object _process_message(self, object replication_msg)
    cdef object _finalize_transaction(self)
