# cython: language_level=3
"""
pgoutput Binary Protocol Parser - Cython Declarations

Declarations for parsing PostgreSQL pgoutput logical replication messages.
"""

from libc.stdint cimport uint8_t, uint16_t, uint32_t, uint64_t, int16_t, int32_t, int64_t
from libcpp.string cimport string as cpp_string
from libcpp.vector cimport vector
from libcpp.memory cimport shared_ptr

# Import vendored Arrow C++
cimport pyarrow.lib as ca
from pyarrow.includes.libarrow cimport CSchema, CRecordBatch


# ============================================================================
# Message Type Constants
# ============================================================================

cdef enum PgoutputMessageType:
    MSG_BEGIN = 66          # 'B' - Transaction begin
    MSG_COMMIT = 67         # 'C' - Transaction commit
    MSG_ORIGIN = 79         # 'O' - Replication origin
    MSG_RELATION = 82       # 'R' - Relation metadata
    MSG_TYPE = 89           # 'Y' - Type metadata
    MSG_INSERT = 73         # 'I' - Insert operation
    MSG_UPDATE = 85         # 'U' - Update operation
    MSG_DELETE = 68         # 'D' - Delete operation
    MSG_TRUNCATE = 84       # 'T' - Truncate operation
    MSG_MESSAGE = 77        # 'M' - Generic message
    MSG_STREAM_START = 83   # 'S' - Stream start (protocol v2+)
    MSG_STREAM_STOP = 69    # 'E' - Stream end (protocol v2+)
    MSG_STREAM_COMMIT = 99  # 'c' - Stream commit (protocol v2+)
    MSG_STREAM_ABORT = 65   # 'A' - Stream abort (protocol v2+)


# ============================================================================
# Tuple Data Constants
# ============================================================================

cdef enum TupleDataType:
    TUPLE_NULL = 110       # 'n' - NULL value
    TUPLE_UNCHANGED = 117  # 'u' - Unchanged TOASTed value
    TUPLE_TEXT = 116       # 't' - Text format value
    TUPLE_BINARY = 98      # 'b' - Binary format value (unused)


# ============================================================================
# Replica Identity Constants
# ============================================================================

cdef enum ReplicaIdentity:
    REPLICA_IDENTITY_DEFAULT = 100  # 'd' - Default (primary key)
    REPLICA_IDENTITY_NOTHING = 110  # 'n' - No replica identity
    REPLICA_IDENTITY_FULL = 102     # 'f' - All columns
    REPLICA_IDENTITY_INDEX = 105    # 'i' - Specific index


# ============================================================================
# Parsed Message Structures
# ============================================================================

cdef struct PgoutputBeginMessage:
    uint64_t final_lsn
    int64_t commit_timestamp  # Microseconds since 2000-01-01
    uint32_t xid


cdef struct PgoutputCommitMessage:
    uint8_t flags
    uint64_t commit_lsn
    uint64_t end_lsn
    int64_t commit_timestamp


cdef struct PgoutputRelationColumn:
    uint8_t flags
    char* name
    uint32_t type_oid
    int32_t type_modifier


cdef struct PgoutputRelationMessage:
    uint32_t relation_oid
    char* namespace_name
    char* relation_name
    uint8_t replica_identity
    uint16_t num_columns
    PgoutputRelationColumn* columns


cdef struct PgoutputTupleData:
    uint8_t kind  # TupleDataType
    uint32_t length
    char* data


cdef struct PgoutputInsertMessage:
    uint32_t relation_oid
    uint8_t new_tuple_flag  # Always 'N'
    uint16_t num_columns
    PgoutputTupleData* columns


cdef struct PgoutputUpdateMessage:
    uint32_t relation_oid
    uint8_t has_key_tuple
    uint8_t has_old_tuple
    uint8_t has_new_tuple
    uint16_t num_columns
    PgoutputTupleData* old_columns  # If has_old_tuple
    PgoutputTupleData* new_columns


cdef struct PgoutputDeleteMessage:
    uint32_t relation_oid
    uint8_t has_key_tuple
    uint8_t has_old_tuple
    uint16_t num_columns
    PgoutputTupleData* columns


# ============================================================================
# Schema Cache Entry
# ============================================================================

cdef struct SchemaCacheEntry:
    uint32_t relation_oid
    char* schema_name
    char* table_name
    uint8_t replica_identity
    uint16_t num_columns
    char** column_names
    uint32_t* column_type_oids
    int32_t* column_type_modifiers
    shared_ptr[CSchema] arrow_schema


# ============================================================================
# Parser Class
# ============================================================================

cdef class PgoutputParser:
    """
    Binary parser for PostgreSQL pgoutput logical replication protocol.

    Parses replication messages and builds Arrow RecordBatches.
    """

    # Schema cache (relation OID -> schema)
    cdef dict _schema_cache

    # Current transaction state
    cdef uint32_t _current_xid
    cdef uint64_t _current_lsn
    cdef int64_t _current_timestamp

    # Batch building state
    cdef list _pending_rows
    cdef object _current_relation_oid
    cdef object _current_batch_builder

    # Statistics
    cdef uint64_t _messages_parsed
    cdef uint64_t _batches_built
    cdef uint64_t _parse_errors

    # Public methods
    cpdef object parse_message(self, const unsigned char* data, int length)
    cpdef object build_arrow_batch(self, uint32_t relation_oid)
    cpdef object get_cached_schema(self, uint32_t relation_oid)
    cpdef void clear_batch_state(self)

    # Internal methods
    cdef list _extract_tuple_data(self, PgoutputTupleData* columns, uint16_t num_columns)

    # Internal parsing methods
    cdef PgoutputBeginMessage _parse_begin(self, const unsigned char* data, int length)
    cdef PgoutputCommitMessage _parse_commit(self, const unsigned char* data, int length)
    cdef PgoutputRelationMessage _parse_relation(self, const unsigned char* data, int length)
    cdef PgoutputInsertMessage _parse_insert(self, const unsigned char* data, int length)
    cdef PgoutputUpdateMessage _parse_update(self, const unsigned char* data, int length)
    cdef PgoutputDeleteMessage _parse_delete(self, const unsigned char* data, int length)

    # Schema cache methods
    cdef void _cache_relation_schema(self, PgoutputRelationMessage* relation_msg)
    cdef shared_ptr[CSchema] _build_arrow_schema(self, PgoutputRelationMessage* relation_msg)

    # Utility methods
    cdef uint16_t _read_uint16(self, const unsigned char* data, int* offset) nogil
    cdef uint32_t _read_uint32(self, const unsigned char* data, int* offset) nogil
    cdef uint64_t _read_uint64(self, const unsigned char* data, int* offset) nogil
    cdef int64_t _read_int64(self, const unsigned char* data, int* offset) nogil
    cdef char* _read_string(self, const unsigned char* data, int* offset)
    cdef PgoutputTupleData _read_tuple_column(self, const unsigned char* data, int* offset)
