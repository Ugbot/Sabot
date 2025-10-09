# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
"""
pgoutput Binary Protocol Parser

Parses PostgreSQL pgoutput logical replication binary messages and converts
them to Arrow RecordBatches for zero-copy CDC streaming.

Based on PostgreSQL's pgoutput plugin protocol:
https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
"""

import logging
from typing import Optional, Dict, List
from libc.stdint cimport uint8_t, uint16_t, uint32_t, uint64_t, int16_t, int32_t, int64_t
from libc.string cimport memcpy, strlen
from libc.stdlib cimport malloc, free
from libcpp.memory cimport make_shared, shared_ptr
from libcpp.string cimport string as cpp_string
from libcpp.vector cimport vector

# Import vendored Arrow C++
cimport pyarrow.lib as ca
from pyarrow.includes.libarrow cimport (
    CSchema, CField, CRecordBatch, CDataType,
    CStatus, CResult, GetResultValue
)

logger = logging.getLogger(__name__)


# ============================================================================
# Error Handling
# ============================================================================

class PgoutputParseError(Exception):
    """Pgoutput message parsing error."""
    pass


cdef inline void check_arrow_status(const CStatus& status, str context) except *:
    """Check Arrow C++ status and raise on error."""
    if not status.ok():
        error_msg = status.message().decode('utf-8')
        raise PgoutputParseError(f"{context}: {error_msg}")


# ============================================================================
# pgoutput Parser Implementation
# ============================================================================

cdef class PgoutputParser:
    """
    Binary parser for PostgreSQL pgoutput logical replication protocol.

    Parses binary messages from pgoutput and builds Arrow RecordBatches.

    Message Types Supported:
    - Begin ('B'): Transaction start
    - Commit ('C'): Transaction commit
    - Relation ('R'): Table schema metadata
    - Insert ('I'): Row insert
    - Update ('U'): Row update
    - Delete ('D'): Row delete

    Performance:
    - Zero-copy parsing where possible
    - Schema caching by relation OID
    - Batch building at transaction boundaries

    Example:
        >>> parser = PgoutputParser()
        >>> msg = parser.parse_message(data, len(data))
        >>> if msg['type'] == 'commit':
        ...     batch = parser.build_arrow_batch(relation_oid)
    """

    def __cinit__(self):
        """Initialize parser state."""
        self._schema_cache = {}
        self._current_xid = 0
        self._current_lsn = 0
        self._current_timestamp = 0
        self._pending_rows = []
        self._current_relation_oid = None
        self._current_batch_builder = None
        self._messages_parsed = 0
        self._batches_built = 0
        self._parse_errors = 0

    # ========================================================================
    # Public API
    # ========================================================================

    cpdef object parse_message(self, const unsigned char* data, int length):
        """
        Parse a single pgoutput message.

        Args:
            data: Raw binary message data
            length: Message length in bytes

        Returns:
            Dict with parsed message data:
                - type: Message type ('begin', 'commit', 'relation', 'insert', etc.)
                - Additional fields depend on message type

        Raises:
            PgoutputParseError: If message cannot be parsed
        """
        # All cdef declarations must be at function start (Cython requirement)
        cdef uint8_t msg_type
        cdef object result
        cdef PgoutputBeginMessage begin_msg
        cdef PgoutputCommitMessage commit_msg
        cdef PgoutputRelationMessage rel_msg
        cdef PgoutputInsertMessage ins_msg
        cdef PgoutputUpdateMessage upd_msg
        cdef PgoutputDeleteMessage del_msg

        if length == 0:
            raise PgoutputParseError("Empty message")

        msg_type = data[0]
        result = {}

        try:
            if msg_type == MSG_BEGIN:
                # Transaction begin
                begin_msg = self._parse_begin(data, length)
                self._current_xid = begin_msg.xid
                self._current_lsn = begin_msg.final_lsn
                self._current_timestamp = begin_msg.commit_timestamp
                result = {
                    'type': 'begin',
                    'xid': begin_msg.xid,
                    'final_lsn': begin_msg.final_lsn,
                    'commit_timestamp': begin_msg.commit_timestamp
                }

            elif msg_type == MSG_COMMIT:
                # Transaction commit
                commit_msg = self._parse_commit(data, length)
                result = {
                    'type': 'commit',
                    'flags': commit_msg.flags,
                    'commit_lsn': commit_msg.commit_lsn,
                    'end_lsn': commit_msg.end_lsn,
                    'commit_timestamp': commit_msg.commit_timestamp
                }

            elif msg_type == MSG_RELATION:
                # Relation metadata (schema)
                rel_msg = self._parse_relation(data, length)
                self._cache_relation_schema(&rel_msg)
                result = {
                    'type': 'relation',
                    'relation_oid': rel_msg.relation_oid,
                    'namespace': rel_msg.namespace_name.decode('utf-8') if rel_msg.namespace_name else '',
                    'relation_name': rel_msg.relation_name.decode('utf-8') if rel_msg.relation_name else '',
                    'num_columns': rel_msg.num_columns
                }

            elif msg_type == MSG_INSERT:
                # Insert operation
                ins_msg = self._parse_insert(data, length)
                result = {
                    'type': 'insert',
                    'relation_oid': ins_msg.relation_oid,
                    'num_columns': ins_msg.num_columns,
                    'columns': self._extract_tuple_data(ins_msg.columns, ins_msg.num_columns)
                }
                # Add to pending rows for batch building
                self._pending_rows.append(result)
                self._current_relation_oid = ins_msg.relation_oid

            elif msg_type == MSG_UPDATE:
                # Update operation
                upd_msg = self._parse_update(data, length)
                result = {
                    'type': 'update',
                    'relation_oid': upd_msg.relation_oid,
                    'has_old_tuple': upd_msg.has_old_tuple,
                    'num_columns': upd_msg.num_columns,
                    'new_columns': self._extract_tuple_data(upd_msg.new_columns, upd_msg.num_columns)
                }
                if upd_msg.has_old_tuple:
                    result['old_columns'] = self._extract_tuple_data(upd_msg.old_columns, upd_msg.num_columns)
                self._pending_rows.append(result)
                self._current_relation_oid = upd_msg.relation_oid

            elif msg_type == MSG_DELETE:
                # Delete operation
                del_msg = self._parse_delete(data, length)
                result = {
                    'type': 'delete',
                    'relation_oid': del_msg.relation_oid,
                    'num_columns': del_msg.num_columns,
                    'columns': self._extract_tuple_data(del_msg.columns, del_msg.num_columns)
                }
                self._pending_rows.append(result)
                self._current_relation_oid = del_msg.relation_oid

            elif msg_type == MSG_ORIGIN:
                # Replication origin (skip for now)
                result = {'type': 'origin'}

            elif msg_type == MSG_TYPE:
                # Type metadata (skip for now)
                result = {'type': 'type'}

            elif msg_type == MSG_TRUNCATE:
                # Truncate operation
                result = {'type': 'truncate'}

            else:
                # Unknown message type
                logger.warning(f"Unknown pgoutput message type: {chr(msg_type)} (0x{msg_type:02x})")
                result = {'type': 'unknown', 'msg_type': msg_type}

            self._messages_parsed += 1
            return result

        except Exception as e:
            self._parse_errors += 1
            raise PgoutputParseError(f"Failed to parse message type {chr(msg_type)}: {e}") from e

    cpdef object build_arrow_batch(self, uint32_t relation_oid):
        """
        Build Arrow RecordBatch from pending rows for a relation.

        Args:
            relation_oid: PostgreSQL relation OID

        Returns:
            Arrow RecordBatch (pyarrow.RecordBatch) or None if no pending rows

        Raises:
            PgoutputParseError: If batch building fails
        """
        if not self._pending_rows:
            return None

        # Get cached schema for this relation
        if relation_oid not in self._schema_cache:
            raise PgoutputParseError(f"No cached schema for relation OID {relation_oid}")

        # Note: Schema cache stores Python dicts (can't store C++ structs in Python dict)
        entry = self._schema_cache[relation_oid]

        # Build batch using Arrow builders
        # TODO: Implement Arrow RecordBatchBuilder integration
        # For now, return None as placeholder

        self._batches_built += 1
        self.clear_batch_state()

        return None

    cpdef object get_cached_schema(self, uint32_t relation_oid):
        """
        Get cached Arrow schema for relation.

        Args:
            relation_oid: PostgreSQL relation OID

        Returns:
            pyarrow.Schema or None if not cached
        """
        if relation_oid in self._schema_cache:
            entry = self._schema_cache[relation_oid]
            # Already wrapped as Python object when stored
            return entry['arrow_schema']
        return None

    cpdef void clear_batch_state(self):
        """Clear pending batch state after commit."""
        self._pending_rows = []
        self._current_relation_oid = None

    # ========================================================================
    # Internal Parsing Methods
    # ========================================================================

    cdef PgoutputBeginMessage _parse_begin(self, const unsigned char* data, int length):
        """Parse BEGIN message."""
        cdef int offset = 1  # Skip message type
        cdef PgoutputBeginMessage msg

        if length < 21:  # 1 (type) + 8 (lsn) + 8 (timestamp) + 4 (xid)
            raise PgoutputParseError(f"BEGIN message too short: {length} bytes")

        msg.final_lsn = self._read_uint64(data, &offset)
        msg.commit_timestamp = self._read_int64(data, &offset)
        msg.xid = self._read_uint32(data, &offset)

        logger.debug(f"BEGIN: xid={msg.xid}, lsn={msg.final_lsn}, ts={msg.commit_timestamp}")
        return msg

    cdef PgoutputCommitMessage _parse_commit(self, const unsigned char* data, int length):
        """Parse COMMIT message."""
        cdef int offset = 1  # Skip message type
        cdef PgoutputCommitMessage msg

        if length < 26:  # 1 (type) + 1 (flags) + 8 + 8 + 8
            raise PgoutputParseError(f"COMMIT message too short: {length} bytes")

        msg.flags = data[offset]
        offset += 1
        msg.commit_lsn = self._read_uint64(data, &offset)
        msg.end_lsn = self._read_uint64(data, &offset)
        msg.commit_timestamp = self._read_int64(data, &offset)

        logger.debug(f"COMMIT: lsn={msg.commit_lsn}, end_lsn={msg.end_lsn}")
        return msg

    cdef PgoutputRelationMessage _parse_relation(self, const unsigned char* data, int length):
        """Parse RELATION message."""
        cdef int offset = 1  # Skip message type
        cdef PgoutputRelationMessage msg
        cdef uint16_t i

        msg.relation_oid = self._read_uint32(data, &offset)
        msg.namespace_name = self._read_string(data, &offset)
        msg.relation_name = self._read_string(data, &offset)
        msg.replica_identity = data[offset]
        offset += 1
        msg.num_columns = self._read_uint16(data, &offset)

        # Allocate column array
        msg.columns = <PgoutputRelationColumn*>malloc(msg.num_columns * sizeof(PgoutputRelationColumn))
        if not msg.columns:
            raise MemoryError("Failed to allocate column array")

        # Parse each column
        for i in range(msg.num_columns):
            msg.columns[i].flags = data[offset]
            offset += 1
            msg.columns[i].name = self._read_string(data, &offset)
            msg.columns[i].type_oid = self._read_uint32(data, &offset)
            msg.columns[i].type_modifier = <int32_t>self._read_uint32(data, &offset)

        logger.debug(
            f"RELATION: oid={msg.relation_oid}, "
            f"name={msg.namespace_name.decode('utf-8') if msg.namespace_name else ''}.{msg.relation_name.decode('utf-8') if msg.relation_name else ''}, "
            f"columns={msg.num_columns}"
        )
        return msg

    cdef PgoutputInsertMessage _parse_insert(self, const unsigned char* data, int length):
        """Parse INSERT message."""
        cdef int offset = 1  # Skip message type
        cdef PgoutputInsertMessage msg
        cdef uint16_t i

        msg.relation_oid = self._read_uint32(data, &offset)
        msg.new_tuple_flag = data[offset]  # Should be 'N'
        offset += 1

        if msg.new_tuple_flag != ord('N'):
            raise PgoutputParseError(f"Expected 'N' tuple flag, got '{chr(msg.new_tuple_flag)}'")

        msg.num_columns = self._read_uint16(data, &offset)

        # Allocate columns array
        msg.columns = <PgoutputTupleData*>malloc(msg.num_columns * sizeof(PgoutputTupleData))
        if not msg.columns:
            raise MemoryError("Failed to allocate columns array")

        # Parse each column
        for i in range(msg.num_columns):
            msg.columns[i] = self._read_tuple_column(data, &offset)

        logger.debug(f"INSERT: oid={msg.relation_oid}, columns={msg.num_columns}")
        return msg

    cdef PgoutputUpdateMessage _parse_update(self, const unsigned char* data, int length):
        """Parse UPDATE message."""
        cdef int offset = 1  # Skip message type
        cdef PgoutputUpdateMessage msg
        cdef uint8_t tuple_type
        cdef uint16_t i

        msg.relation_oid = self._read_uint32(data, &offset)
        tuple_type = data[offset]
        offset += 1

        msg.has_key_tuple = 0
        msg.has_old_tuple = 0
        msg.has_new_tuple = 0

        # Check for optional old/key tuple
        if tuple_type == ord('K'):
            # Key tuple (for replica identity INDEX)
            msg.has_key_tuple = 1
            msg.num_columns = self._read_uint16(data, &offset)
            msg.old_columns = <PgoutputTupleData*>malloc(msg.num_columns * sizeof(PgoutputTupleData))
            for i in range(msg.num_columns):
                msg.old_columns[i] = self._read_tuple_column(data, &offset)
            tuple_type = data[offset]
            offset += 1

        elif tuple_type == ord('O'):
            # Old tuple (for replica identity FULL)
            msg.has_old_tuple = 1
            msg.num_columns = self._read_uint16(data, &offset)
            msg.old_columns = <PgoutputTupleData*>malloc(msg.num_columns * sizeof(PgoutputTupleData))
            for i in range(msg.num_columns):
                msg.old_columns[i] = self._read_tuple_column(data, &offset)
            tuple_type = data[offset]
            offset += 1

        # New tuple (always present)
        if tuple_type == ord('N'):
            msg.has_new_tuple = 1
            msg.num_columns = self._read_uint16(data, &offset)
            msg.new_columns = <PgoutputTupleData*>malloc(msg.num_columns * sizeof(PgoutputTupleData))
            for i in range(msg.num_columns):
                msg.new_columns[i] = self._read_tuple_column(data, &offset)
        else:
            raise PgoutputParseError(f"Expected 'N' tuple flag in UPDATE, got '{chr(tuple_type)}'")

        logger.debug(
            f"UPDATE: oid={msg.relation_oid}, "
            f"has_old={msg.has_old_tuple}, has_key={msg.has_key_tuple}"
        )
        return msg

    cdef PgoutputDeleteMessage _parse_delete(self, const unsigned char* data, int length):
        """Parse DELETE message."""
        cdef int offset = 1  # Skip message type
        cdef PgoutputDeleteMessage msg
        cdef uint8_t tuple_type
        cdef uint16_t i

        msg.relation_oid = self._read_uint32(data, &offset)
        tuple_type = data[offset]
        offset += 1

        msg.has_key_tuple = 0
        msg.has_old_tuple = 0

        if tuple_type == ord('K'):
            # Key tuple
            msg.has_key_tuple = 1
        elif tuple_type == ord('O'):
            # Old tuple (replica identity FULL)
            msg.has_old_tuple = 1
        else:
            raise PgoutputParseError(f"Expected 'K' or 'O' tuple flag in DELETE, got '{chr(tuple_type)}'")

        msg.num_columns = self._read_uint16(data, &offset)
        msg.columns = <PgoutputTupleData*>malloc(msg.num_columns * sizeof(PgoutputTupleData))
        for i in range(msg.num_columns):
            msg.columns[i] = self._read_tuple_column(data, &offset)

        logger.debug(f"DELETE: oid={msg.relation_oid}, columns={msg.num_columns}")
        return msg

    # ========================================================================
    # Schema Caching
    # ========================================================================

    cdef void _cache_relation_schema(self, PgoutputRelationMessage* relation_msg):
        """Cache relation schema for later use."""
        cdef uint16_t i
        cdef SchemaCacheEntry entry
        cdef shared_ptr[CSchema] arrow_schema

        # Build Arrow schema
        arrow_schema = self._build_arrow_schema(relation_msg)

        # Store in cache (wrap C++ schema as Python object)
        self._schema_cache[relation_msg.relation_oid] = {
            'relation_oid': relation_msg.relation_oid,
            'schema_name': relation_msg.namespace_name.decode('utf-8') if relation_msg.namespace_name else '',
            'table_name': relation_msg.relation_name.decode('utf-8') if relation_msg.relation_name else '',
            'num_columns': relation_msg.num_columns,
            'column_names': [
                relation_msg.columns[i].name.decode('utf-8') if relation_msg.columns[i].name else f'col_{i}'
                for i in range(relation_msg.num_columns)
            ],
            'column_type_oids': [
                relation_msg.columns[i].type_oid
                for i in range(relation_msg.num_columns)
            ],
            'arrow_schema': ca.pyarrow_wrap_schema(arrow_schema)
        }

        logger.info(
            f"Cached schema for {relation_msg.namespace_name.decode('utf-8') if relation_msg.namespace_name else ''}.{relation_msg.relation_name.decode('utf-8') if relation_msg.relation_name else ''} "
            f"(OID {relation_msg.relation_oid}, {relation_msg.num_columns} columns)"
        )

    cdef shared_ptr[CSchema] _build_arrow_schema(self, PgoutputRelationMessage* relation_msg):
        """
        Build Arrow schema from PostgreSQL relation metadata.

        Maps PostgreSQL types to Arrow types.
        """
        # TODO: Implement PostgreSQL → Arrow type mapping
        # For now, return empty schema as placeholder
        return shared_ptr[CSchema]()

    # ========================================================================
    # Utility Methods
    # ========================================================================

    cdef uint16_t _read_uint16(self, const unsigned char* data, int* offset) nogil:
        """Read big-endian uint16 from data."""
        cdef uint16_t value = (
            (<uint16_t>data[offset[0]] << 8) |
            (<uint16_t>data[offset[0] + 1])
        )
        offset[0] += 2
        return value

    cdef uint32_t _read_uint32(self, const unsigned char* data, int* offset) nogil:
        """Read big-endian uint32 from data."""
        cdef uint32_t value = (
            (<uint32_t>data[offset[0]] << 24) |
            (<uint32_t>data[offset[0] + 1] << 16) |
            (<uint32_t>data[offset[0] + 2] << 8) |
            (<uint32_t>data[offset[0] + 3])
        )
        offset[0] += 4
        return value

    cdef uint64_t _read_uint64(self, const unsigned char* data, int* offset) nogil:
        """Read big-endian uint64 from data."""
        cdef uint64_t value = (
            (<uint64_t>data[offset[0]] << 56) |
            (<uint64_t>data[offset[0] + 1] << 48) |
            (<uint64_t>data[offset[0] + 2] << 40) |
            (<uint64_t>data[offset[0] + 3] << 32) |
            (<uint64_t>data[offset[0] + 4] << 24) |
            (<uint64_t>data[offset[0] + 5] << 16) |
            (<uint64_t>data[offset[0] + 6] << 8) |
            (<uint64_t>data[offset[0] + 7])
        )
        offset[0] += 8
        return value

    cdef int64_t _read_int64(self, const unsigned char* data, int* offset) nogil:
        """Read big-endian int64 from data."""
        return <int64_t>self._read_uint64(data, offset)

    cdef char* _read_string(self, const unsigned char* data, int* offset):
        """Read null-terminated string from data."""
        cdef int start = offset[0]
        cdef int length = 0

        # Find null terminator
        while data[offset[0] + length] != 0:
            length += 1

        # Allocate and copy string
        cdef char* result = <char*>malloc(length + 1)
        if not result:
            raise MemoryError("Failed to allocate string")

        memcpy(result, &data[offset[0]], length)
        result[length] = 0

        offset[0] += length + 1  # Skip string + null terminator
        return result

    cdef PgoutputTupleData _read_tuple_column(self, const unsigned char* data, int* offset):
        """Read a single tuple column value."""
        cdef PgoutputTupleData column
        column.kind = data[offset[0]]
        offset[0] += 1

        if column.kind == TUPLE_NULL:
            # NULL value
            column.length = 0
            column.data = NULL

        elif column.kind == TUPLE_UNCHANGED:
            # Unchanged TOASTed value
            column.length = 0
            column.data = NULL

        elif column.kind == TUPLE_TEXT:
            # Text format value
            column.length = self._read_uint32(data, offset)
            column.data = <char*>malloc(column.length)
            if not column.data:
                raise MemoryError("Failed to allocate column data")
            memcpy(column.data, &data[offset[0]], column.length)
            offset[0] += column.length

        else:
            raise PgoutputParseError(f"Unknown tuple data type: {chr(column.kind)}")

        return column

    cdef list _extract_tuple_data(self, PgoutputTupleData* columns, uint16_t num_columns):
        """Extract tuple data to Python list."""
        cdef uint16_t i
        cdef list result = []

        for i in range(num_columns):
            if columns[i].kind == TUPLE_NULL:
                result.append(None)
            elif columns[i].kind == TUPLE_UNCHANGED:
                result.append('<UNCHANGED>')
            elif columns[i].kind == TUPLE_TEXT:
                # Decode as UTF-8 string
                result.append(columns[i].data[:columns[i].length].decode('utf-8'))
            else:
                result.append(f'<UNKNOWN:{chr(columns[i].kind)}>')

        return result

    # ========================================================================
    # Statistics
    # ========================================================================

    def get_stats(self):
        """Get parser statistics."""
        return {
            'messages_parsed': self._messages_parsed,
            'batches_built': self._batches_built,
            'parse_errors': self._parse_errors,
            'cached_schemas': len(self._schema_cache),
            'pending_rows': len(self._pending_rows)
        }

    def __repr__(self):
        stats = self.get_stats()
        return (
            f"PgoutputParser(messages={stats['messages_parsed']}, "
            f"batches={stats['batches_built']}, "
            f"cached_schemas={stats['cached_schemas']})"
        )
