# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
"""
High-performance libpq wrapper for PostgreSQL logical replication.

Provides zero-copy access to PostgreSQL CDC streams via logical replication protocol.
"""

import asyncio
import logging
from typing import Optional, Dict, Any, List, AsyncIterator, Callable
from dataclasses import dataclass
from libc.stdint cimport uint32_t, uint64_t, int64_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy
import time

# Import libpq declarations
from sabot._cython.connectors.postgresql.libpq_decl cimport *

# Platform-specific byte order functions
# macOS uses libkern/OSByteOrder.h, Linux uses endian.h
cdef extern from *:
    """
    #if defined(__APPLE__)
    #include <libkern/OSByteOrder.h>
    #define be64toh(x) OSSwapBigToHostInt64(x)
    #define htobe64(x) OSSwapHostToBigInt64(x)
    #elif defined(__linux__)
    #include <endian.h>
    #else
    #error "Unsupported platform"
    #endif
    """
    uint64_t be64toh(uint64_t x) nogil
    uint64_t htobe64(uint64_t x) nogil

logger = logging.getLogger(__name__)


# ============================================================================
# Note: parse_xlogdata_message() and build_standby_status_update() are
# implemented as static inline C functions in libpq_decl.pxd and can be
# called directly from Cython code.
# ============================================================================

# ============================================================================
# Error Handling
# ============================================================================

class PostgreSQLError(Exception):
    """PostgreSQL operation error."""

    def __init__(self, message: str, connection: str = None, query: str = None):
        self.message = message
        self.connection = connection
        self.query = query
        super().__init__(message)

    def __str__(self):
        parts = [self.message]
        if self.connection:
            parts.append(f"connection: {self.connection}")
        if self.query:
            parts.append(f"query: {self.query[:100]}...")
        return " | ".join(parts)


cdef inline void check_result(PGresult* res, str context, str query=None) except *:
    """Check PostgreSQL result status and raise on error."""
    cdef ExecStatusType status
    cdef char* error_msg

    if not res:
        raise PostgreSQLError(f"No result returned: {context}", query=query)

    status = PQresultStatus(res)
    if status not in (PGRES_COMMAND_OK, PGRES_TUPLES_OK, PGRES_COPY_OUT, PGRES_COPY_BOTH):
        error_msg = PQresultErrorMessage(res)
        if error_msg:
            raise PostgreSQLError(f"{context}: {error_msg.decode('utf-8')}", query=query)
        else:
            raise PostgreSQLError(f"{context}: Unknown error", query=query)


cdef inline void check_conn(PGconn* conn, str context) except *:
    """Check PostgreSQL connection status and raise on error."""
    cdef ConnStatusType status
    cdef char* error_msg

    if not conn:
        raise PostgreSQLError(f"No connection: {context}")

    status = PQstatus(conn)
    if status != CONNECTION_OK:
        error_msg = PQerrorMessage(conn)
        if error_msg:
            raise PostgreSQLError(f"Connection error: {error_msg.decode('utf-8')}", context)
        else:
            raise PostgreSQLError(f"Connection error (status={status})", context)


# ============================================================================
# Replication Message
# ============================================================================

cdef class ReplicationMessage:
    """Logical replication message from PostgreSQL."""
    # Note: cdef public declarations are in libpq_conn.pxd

    def __init__(self, bytes data, wal_start, wal_end, send_time, str message_type='w'):
        self.data = data
        self.wal_start = wal_start
        self.wal_end = wal_end
        self.send_time = send_time
        self.message_type = message_type

    def __repr__(self):
        return (f"ReplicationMessage(type={self.message_type}, "
                f"wal={self.wal_start}..{self.wal_end}, "
                f"len={len(self.data)})")


# ============================================================================
# PostgreSQL Connection
# ============================================================================

cdef class PostgreSQLConnection:
    """
    High-performance PostgreSQL connection with logical replication support.

    Wraps libpq C library for zero-overhead access to PostgreSQL.
    """

    def __cinit__(self, str conninfo, bint replication=False):
        """
        Create PostgreSQL connection.

        Args:
            conninfo: PostgreSQL connection string (e.g., "host=localhost dbname=mydb")
            replication: Whether to create replication connection (for CDC)
        """
        self.conn = NULL
        self.conninfo = conninfo
        self.is_replication_conn = replication
        self.last_lsn = 0
        self._loop = None

    def __init__(self, str conninfo, bint replication=False):
        """Initialize connection."""
        # Build connection string
        cdef str full_conninfo = conninfo
        if replication:
            # Add replication parameter
            if 'replication=' not in conninfo:
                full_conninfo = f"{conninfo} replication=database"

        # Connect
        cdef bytes conninfo_bytes = full_conninfo.encode('utf-8')
        self.conn = PQconnectdb(conninfo_bytes)

        # Check connection
        check_conn(self.conn, "connect")

        logger.info(f"Connected to PostgreSQL (replication={replication})")

    def __dealloc__(self):
        """Cleanup connection."""
        self.close()

    def close(self):
        """Close connection."""
        if self.conn != NULL:
            PQfinish(self.conn)
            self.conn = NULL
            logger.debug("PostgreSQL connection closed")

    @property
    def closed(self) -> bool:
        """Check if connection is closed."""
        return self.conn == NULL or PQstatus(<const PGconn*>self.conn) != CONNECTION_OK

    # ========================================================================
    # Query Execution
    # ========================================================================

    def execute(self, str query) -> List[tuple]:
        """
        Execute SQL query and return results.

        Args:
            query: SQL query string

        Returns:
            List of tuples (one per row)
        """
        check_conn(self.conn, "execute")

        cdef bytes query_bytes = query.encode('utf-8')
        cdef PGresult* res
        cdef int nrows, ncols, row, col
        cdef list results
        cdef char* value

        res = PQexec(self.conn, query_bytes)

        try:
            check_result(res, "execute", query)

            # Get result data
            nrows = PQntuples(res)
            ncols = PQnfields(res)
            results = []

            for row in range(nrows):
                row_data = []
                for col in range(ncols):
                    if PQgetisnull(res, row, col):
                        row_data.append(None)
                    else:
                        value = PQgetvalue(res, row, col)
                        row_data.append(value.decode('utf-8'))
                results.append(tuple(row_data))

            return results

        finally:
            PQclear(res)

    def execute_scalar(self, str query):
        """
        Execute query and return first column of first row.

        Args:
            query: SQL query

        Returns:
            Single value or None
        """
        results = self.execute(query)
        if results and results[0]:
            return results[0][0]
        return None

    # ========================================================================
    # Replication Slot Management
    # ========================================================================

    def create_replication_slot(self, str slot_name, str output_plugin='wal2arrow', temporary=False):
        """
        Create logical replication slot.

        Args:
            slot_name: Slot name
            output_plugin: Output plugin name (default: 'wal2arrow')
            temporary: Whether slot is temporary (dropped on disconnect)

        Returns:
            Dict with slot info (name, consistent_point, snapshot_name, output_plugin)
        """
        check_conn(self.conn, "create_replication_slot")

        # Check if slot exists
        cdef str check_query = f"""
        SELECT slot_name FROM pg_replication_slots WHERE slot_name = '{slot_name}'
        """
        existing = self.execute(check_query)
        if existing:
            logger.info(f"Replication slot '{slot_name}' already exists")
            return {'name': slot_name, 'exists': True}

        # Create slot
        cdef str temp_str = "TEMPORARY" if temporary else ""
        cdef str create_query = f"CREATE_REPLICATION_SLOT {slot_name} {temp_str} LOGICAL {output_plugin}"
        cdef bytes query_bytes = create_query.encode('utf-8')

        cdef PGresult* res = PQexec(self.conn, query_bytes)
        try:
            check_result(res, "create_replication_slot", create_query)

            # Parse result
            if PQntuples(res) > 0:
                return {
                    'name': PQgetvalue(res, 0, 0).decode('utf-8'),
                    'consistent_point': PQgetvalue(res, 0, 1).decode('utf-8'),
                    'snapshot_name': PQgetvalue(res, 0, 2).decode('utf-8') if PQnfields(res) > 2 else None,
                    'output_plugin': PQgetvalue(res, 0, 3).decode('utf-8') if PQnfields(res) > 3 else output_plugin,
                }
            return {'name': slot_name}

        finally:
            PQclear(res)

    def drop_replication_slot(self, str slot_name):
        """
        Drop logical replication slot.

        Args:
            slot_name: Slot name
        """
        check_conn(self.conn, "drop_replication_slot")

        cdef str drop_query = f"DROP_REPLICATION_SLOT {slot_name}"
        cdef bytes query_bytes = drop_query.encode('utf-8')

        cdef PGresult* res = PQexec(self.conn, query_bytes)
        try:
            check_result(res, "drop_replication_slot", drop_query)
            logger.info(f"Dropped replication slot: {slot_name}")
        finally:
            PQclear(res)

    # ========================================================================
    # Logical Replication Streaming
    # ========================================================================

    async def start_replication(
        self,
        str slot_name,
        start_lsn: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None
    ):
        """
        Start logical replication from slot.

        Args:
            slot_name: Replication slot name
            start_lsn: Starting LSN (e.g., '0/0') or None for slot's restart_lsn
            options: Plugin options (e.g., {'include-xids': '1', 'include-timestamp': '1'})

        Returns:
            AsyncIterator of ReplicationMessage
        """
        check_conn(self.conn, "start_replication")

        # Build START_REPLICATION command
        cdef str lsn_str = start_lsn if start_lsn else '0/0'
        cdef str options_str = ''

        if options:
            option_parts = [f"{k} '{v}'" for k, v in options.items()]
            options_str = f" ({', '.join(option_parts)})"

        cdef str start_query = f"START_REPLICATION SLOT {slot_name} LOGICAL {lsn_str}{options_str}"
        cdef bytes query_bytes = start_query.encode('utf-8')

        logger.info(f"Starting replication: {start_query}")

        # Send START_REPLICATION command
        cdef int status = PQsendQuery(self.conn, query_bytes)
        if status == 0:
            raise PostgreSQLError("Failed to send START_REPLICATION command")

        # Get result (should be PGRES_COPY_BOTH)
        cdef PGresult* res
        cdef ExecStatusType res_status

        res = PQgetResult(self.conn)
        try:
            check_result(res, "start_replication", start_query)

            res_status = PQresultStatus(res)
            if res_status != PGRES_COPY_BOTH:
                raise PostgreSQLError(f"Unexpected status after START_REPLICATION: {res_status}")

            logger.info("Replication started successfully")

        finally:
            PQclear(res)

        # Stream replication messages
        async for msg in self._stream_replication_messages():
            yield msg

    async def _stream_replication_messages(self) -> AsyncIterator[ReplicationMessage]:
        """
        Stream replication messages from PostgreSQL.

        Yields:
            ReplicationMessage instances
        """
        cdef char* buffer = NULL
        cdef char* error_msg
        cdef int nbytes
        cdef uint64_t wal_start, wal_end, send_time_us
        cdef const char* msg_data
        cdef int msg_len
        cdef bytes data_bytes
        cdef double last_keepalive, keepalive_interval, now

        # Get event loop
        if self._loop is None:
            self._loop = asyncio.get_event_loop()

        last_keepalive = time.time()
        keepalive_interval = 10.0  # seconds

        try:
            while True:
                # Read data from replication stream
                nbytes = PQgetCopyData(self.conn, &buffer, 0)  # Non-blocking

                if nbytes > 0:
                    try:
                        # Parse XLogData message
                        if parse_xlogdata_message(
                            buffer, nbytes,
                            &wal_start, &wal_end, &send_time_us,
                            &msg_data, &msg_len
                        ) == 0:
                            # Extract message data (zero-copy via memoryview would be ideal)
                            data_bytes = msg_data[:msg_len]

                            # Update last LSN
                            self.last_lsn = wal_end

                            # Yield message
                            yield ReplicationMessage(
                                data=data_bytes,
                                wal_start=wal_start,
                                wal_end=wal_end,
                                send_time=send_time_us,
                                message_type='w'
                            )

                        else:
                            # Not an XLogData message (could be keepalive)
                            if buffer[0] == b'k':
                                logger.debug("Received keepalive message")
                            else:
                                logger.warning(f"Unknown message type: {buffer[0]}")

                    finally:
                        # Free buffer allocated by PQgetCopyData
                        if buffer != NULL:
                            free(buffer)
                            buffer = NULL

                elif nbytes == -1:
                    # No more data
                    logger.info("Replication stream ended")
                    break

                elif nbytes == -2:
                    # Error
                    error_msg = PQerrorMessage(<const PGconn*>self.conn)
                    raise PostgreSQLError(f"Replication stream error: {error_msg.decode('utf-8')}")

                else:
                    # No data available, sleep briefly (1ms for low latency)
                    await asyncio.sleep(0.001)

                # Send keepalive (standby status update)
                now = time.time()
                if now - last_keepalive > keepalive_interval:
                    await self._send_standby_status_update()
                    last_keepalive = now

        finally:
            # Cleanup
            if buffer != NULL:
                free(buffer)

    async def _send_standby_status_update(self):
        """Send standby status update (keepalive) to PostgreSQL."""
        cdef char buffer[1 + 8 + 8 + 8 + 8 + 1]
        cdef int64_t send_time_us
        cdef int msg_len, status

        send_time_us = <int64_t>(time.time() * 1000000)

        msg_len = build_standby_status_update(
            buffer, sizeof(buffer),
            self.last_lsn,  # write_lsn
            self.last_lsn,  # flush_lsn
            self.last_lsn,  # apply_lsn
            send_time_us,
            0  # replyRequested
        )

        if msg_len > 0:
            status = PQputCopyData(self.conn, buffer, msg_len)
            if status != 1:
                logger.warning(f"Failed to send standby status update: {status}")
            else:
                # Flush to ensure message is sent immediately
                PQflush(self.conn)
                logger.info(f"✅ Sent standby status update (LSN={self.last_lsn})")
        else:
            logger.error(f"❌ build_standby_status_update failed: msg_len={msg_len}")

    # ========================================================================
    # Context Manager
    # ========================================================================

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    # ========================================================================
    # Table Discovery & Configuration (for auto-configure)
    # ========================================================================

    def discover_tables(self, schemas=None):
        """
        Discover all tables in database.

        Args:
            schemas: List of schemas to search (None = all user schemas)

        Returns:
            List of (schema_name, table_name) tuples
        """
        schema_filter = ""
        if schemas:
            schema_list = "','".join(schemas)
            schema_filter = f"AND table_schema IN ('{schema_list}')"

        query = f"""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            {schema_filter}
            AND table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name
        """

        return self.execute(query)

    def get_table_columns(self, schema, table):
        """
        Get column metadata for table.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            List of dicts with: column_name, data_type, is_nullable
        """
        query = f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = '{table}'
            ORDER BY ordinal_position
        """

        results = self.execute(query)
        return [
            {'name': row[0], 'type': row[1], 'nullable': row[2] == 'YES'}
            for row in results
        ]

    def check_wal_level(self):
        """
        Check current wal_level setting.

        Returns:
            String: 'replica', 'logical', etc.
        """
        return self.execute_scalar("SHOW wal_level")

    def check_replication_config(self):
        """
        Check replication-related configuration.

        Returns:
            Dict with: wal_level, max_replication_slots, max_wal_senders
        """
        config = {}
        config['wal_level'] = self.execute_scalar("SHOW wal_level")
        config['max_replication_slots'] = int(self.execute_scalar("SHOW max_replication_slots"))
        config['max_wal_senders'] = int(self.execute_scalar("SHOW max_wal_senders"))
        return config

    def ensure_logical_replication_enabled(self):
        """
        Verify logical replication is enabled.

        Raises:
            PostgreSQLError: If not properly configured
        """
        config = self.check_replication_config()

        errors = []

        if config['wal_level'] != 'logical':
            errors.append(
                f"❌ wal_level is '{config['wal_level']}' but must be 'logical'"
            )

        if config['max_replication_slots'] == 0:
            errors.append("❌ max_replication_slots is 0 (should be >= 1)")

        if config['max_wal_senders'] == 0:
            errors.append("❌ max_wal_senders is 0 (should be >= 1)")

        if errors:
            error_msg = "\n".join(errors)
            raise PostgreSQLError(
                f"PostgreSQL not configured for logical replication:\n\n"
                f"{error_msg}\n\n"
                f"Required postgresql.conf settings:\n"
                f"  wal_level = logical\n"
                f"  max_replication_slots = 10\n"
                f"  max_wal_senders = 10\n\n"
                f"Restart PostgreSQL after changing settings."
            )

        logger.info("✅ Logical replication enabled")

    def create_publication(self, pub_name, tables):
        """
        Create publication for logical replication (PostgreSQL 10+).

        Args:
            pub_name: Publication name
            tables: List of (schema, table) tuples

        Publications specify which tables to replicate.
        """
        # Check if exists
        check = self.execute(
            f"SELECT pubname FROM pg_publication WHERE pubname = '{pub_name}'"
        )
        if check:
            logger.info(f"Publication '{pub_name}' already exists")
            return

        # Format table list
        table_list = ', '.join([f'"{schema}"."{table}"' for schema, table in tables])

        # Create publication
        query = f"CREATE PUBLICATION {pub_name} FOR TABLE {table_list}"
        self.execute(query)

        logger.info(f"Created publication '{pub_name}' for {len(tables)} tables")

    def grant_table_access(self, username, tables):
        """
        Grant SELECT on specific tables for CDC user.

        Args:
            username: Username to grant access to
            tables: List of (schema, table) tuples
        """
        for schema, table in tables:
            grant_query = f'GRANT SELECT ON TABLE "{schema}"."{table}" TO {username}'
            try:
                self.execute(grant_query)
                logger.debug(f"Granted SELECT on {schema}.{table} to {username}")
            except Exception as e:
                logger.warning(f"Failed to grant on {schema}.{table}: {e}")

        logger.info(f"Granted SELECT on {len(tables)} tables to {username}")

