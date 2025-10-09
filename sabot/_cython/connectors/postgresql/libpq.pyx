# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
"""
Cython wrapper for PostgreSQL libpq C API.

Provides high-performance connection and logical replication support for CDC.
"""

import asyncio
import logging
from typing import Optional, Dict, Any, List, AsyncGenerator, Tuple
from dataclasses import dataclass
from libc.stdint cimport uint32_t, uint64_t
from libcpp.string cimport string

logger = logging.getLogger(__name__)

# Import libpq declarations
cimport sabot._cython.connectors.postgresql.libpq as libpq


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


cdef inline void check_pg_result(PGresult* res, str context, str query=None) except *:
    """Check PostgreSQL result status and raise on error."""
    if not res:
        raise PostgreSQLError(f"No result returned: {context}", query=query)

    status = libpq.PQresultStatus(res)
    if status not in (libpq.PGRES_COMMAND_OK, libpq.PGRES_TUPLES_OK, libpq.PGRES_COPY_OUT):
        error_msg = libpq.PQresultErrorMessage(res)
        if error_msg:
            raise PostgreSQLError(f"{context}: {error_msg.decode('utf-8')}", query=query)
        else:
            raise PostgreSQLError(f"{context}: Unknown error", query=query)


cdef inline void check_pg_conn(PGconn* conn, str context) except *:
    """Check PostgreSQL connection status and raise on error."""
    if not conn:
        raise PostgreSQLError(f"No connection: {context}")

    status = libpq.PQstatus(conn)
    if status != libpq.CONNECTION_OK:
        error_msg = libpq.PQerrorMessage(conn)
        if error_msg:
            raise PostgreSQLError(f"Connection error ({status}): {error_msg.decode('utf-8')}", context)
        else:
            raise PostgreSQLError(f"Connection error ({status})", context)


# ============================================================================
# Replication Message
# ============================================================================

@dataclass
class ReplicationMessage:
    """Logical replication message from PostgreSQL."""

    data: bytes
    wal_end: int
    server_wal_end: int
    message_type: Optional[str] = None
    message_lsn: Optional[int] = None

    @classmethod
    def from_c_struct(cls, libpq.PGReplicationMessage* msg):
        """Create from C structure."""
        if not msg:
            return None

        return cls(
            data=msg.data[:msg.len] if msg.data else b"",
            wal_end=msg.walEnd,
            server_wal_end=msg.serverWalEnd,
            message_type=msg.srvmsg_type.decode('utf-8') if msg.srvmsg_type else None,
            message_lsn=msg.srvmsg_lsn if msg.srvmsg_lsn else None
        )


# ============================================================================
# PostgreSQL Connection Wrapper
# ============================================================================

cdef class PostgreSQLConnection:
    """
    High-performance PostgreSQL connection wrapper.

    Manages connection lifecycle and provides logical replication support.
    """

    cdef libpq.PGconn* _conn
    cdef readonly str connection_string
    cdef readonly bint is_connected
    cdef readonly bint is_replication

    def __init__(self, str connection_string):
        """
        Initialize PostgreSQL connection.

        Args:
            connection_string: PostgreSQL connection string
        """
        self.connection_string = connection_string
        self._conn = NULL
        self.is_connected = False
        self.is_replication = False

    def __dealloc__(self):
        """Cleanup connection on destruction."""
        self.close()

    def connect(self):
        """Establish connection to PostgreSQL."""
        if self.is_connected:
            return

        cdef bytes conn_str_bytes = self.connection_string.encode('utf-8')
        cdef const char* conn_str_c = conn_str_bytes

        self._conn = libpq.PQconnectdb(conn_str_c)
        check_pg_conn(self._conn, "connect")

        self.is_connected = True
        logger.info(f"Connected to PostgreSQL: {self.connection_string}")

    def close(self):
        """Close connection to PostgreSQL."""
        if self._conn:
            libpq.PQfinish(self._conn)
            self._conn = NULL
            self.is_connected = False
            self.is_replication = False
            logger.info("Disconnected from PostgreSQL")

    def execute(self, str query, parameters: List[Any] = None) -> List[Dict[str, Any]]:
        """
        Execute SQL query and return results.

        Args:
            query: SQL query string
            parameters: Query parameters (optional)

        Returns:
            List of result rows as dictionaries
        """
        check_pg_conn(self._conn, "execute")

        cdef bytes query_bytes = query.encode('utf-8')
        cdef const char* query_c = query_bytes
        cdef PGresult* res
        cdef int n_params
        cdef const char** param_values
        cdef int ntuples
        cdef int nfields

        if parameters:
            # Use parameterized query
            n_params = len(parameters)
            param_values = <const char**>malloc(n_params * sizeof(const char*))
            if not param_values:
                raise MemoryError("Failed to allocate parameter array")

            try:
                # Convert parameters to strings
                param_strings = []
                for i, param in enumerate(parameters):
                    if param is None:
                        param_values[i] = NULL
                    else:
                        param_str = str(param)
                        param_strings.append(param_str.encode('utf-8'))
                        param_values[i] = param_strings[-1]

                res = libpq.PQexecParams(self._conn, query_c, n_params, NULL,
                                       param_values, NULL, NULL, 0)
            finally:
                free(param_values)
        else:
            res = libpq.PQexec(self._conn, query_c)

        try:
            check_pg_result(res, "execute", query)

            # Convert result to Python objects
            ntuples = libpq.PQntuples(res)
            nfields = libpq.PQnfields(res)

            results = []
            for row_idx in range(ntuples):
                row = {}
                for col_idx in range(nfields):
                    col_name = libpq.PQfname(res, col_idx).decode('utf-8')
                    if libpq.PQgetisnull(res, row_idx, col_idx):
                        col_value = None
                    else:
                        col_value_bytes = libpq.PQgetvalue(res, row_idx, col_idx)
                        col_value_len = libpq.PQgetlength(res, row_idx, col_idx)
                        col_value = col_value_bytes[:col_value_len].decode('utf-8')
                    row[col_name] = col_value
                results.append(row)

            return results

        finally:
            if res:
                libpq.PQclear(res)

    def create_replication_slot(self, str slot_name, str output_plugin = "wal2json"):
        """
        Create a logical replication slot.

        Args:
            slot_name: Name of the replication slot
            output_plugin: Output plugin (default: wal2json)
        """
        check_pg_conn(self._conn, "create_replication_slot")

        query = f"CREATE_REPLICATION_SLOT {slot_name} LOGICAL {output_plugin}"
        cdef bytes query_bytes = query.encode('utf-8')
        cdef const char* query_c = query_bytes

        cdef PGresult* res = libpq.PQexec(self._conn, query_c)
        try:
            check_pg_result(res, "create_replication_slot", query)
            logger.info(f"Created replication slot: {slot_name} with plugin {output_plugin}")
        finally:
            if res:
                libpq.PQclear(res)

    def drop_replication_slot(self, str slot_name):
        """
        Drop a logical replication slot.

        Args:
            slot_name: Name of the replication slot
        """
        check_pg_conn(self._conn, "drop_replication_slot")

        query = f"DROP_REPLICATION_SLOT {slot_name}"
        cdef bytes query_bytes = query.encode('utf-8')
        cdef const char* query_c = query_bytes

        cdef PGresult* res = libpq.PQexec(self._conn, query_c)
        try:
            check_pg_result(res, "drop_replication_slot", query)
            logger.info(f"Dropped replication slot: {slot_name}")
        finally:
            if res:
                libpq.PQclear(res)

    def start_replication(self, str slot_name, uint64_t start_lsn = 0,
                         plugin_args: List[str] = None):
        """
        Start logical replication from a slot.

        Args:
            slot_name: Name of the replication slot
            start_lsn: Starting LSN (0 for latest)
            plugin_args: Additional plugin arguments
        """
        check_pg_conn(self._conn, "start_replication")

        if self.is_replication:
            raise PostgreSQLError("Replication already started")

        # Convert plugin args to C array
        cdef int n_args = len(plugin_args) if plugin_args else 0
        cdef const char** args_c = NULL

        if n_args > 0:
            args_c = <const char**>malloc((n_args + 1) * sizeof(const char*))
            if not args_c:
                raise MemoryError("Failed to allocate plugin args array")

            try:
                for i in range(n_args):
                    arg_bytes = plugin_args[i].encode('utf-8')
                    args_c[i] = arg_bytes
                args_c[n_args] = NULL
            except:
                free(args_c)
                raise

        # Start replication
        cdef PGresult* res = libpq.PQexecStartReplication(self._conn, slot_name.encode('utf-8'),
                                                        "0/0", start_lsn, args_c)

        if args_c:
            free(args_c)

        try:
            check_pg_result(res, "start_replication", f"START_REPLICATION SLOT {slot_name}")
            self.is_replication = True
            logger.info(f"Started replication from slot: {slot_name}")
        finally:
            if res:
                libpq.PQclear(res)

    async def stream_replication(self) -> AsyncGenerator[ReplicationMessage, None]:
        """
        Stream replication messages.

        Yields:
            ReplicationMessage objects containing WAL data
        """
        if not self.is_replication:
            raise PostgreSQLError("Replication not started")

        cdef libpq.PGReplicationMessage* msg = NULL
        cdef int msg_result

        try:
            while True:
                # Get next replication message
                msg_result = libpq.PQgetReplicationMessage(self._conn, &msg)

                if msg_result < 0:
                    # Error or no message
                    error_msg = libpq.PQerrorMessage(self._conn)
                    if error_msg:
                        raise PostgreSQLError(f"Replication error: {error_msg.decode('utf-8')}")
                    break

                if msg_result == 0:
                    # No message available, wait a bit
                    await asyncio.sleep(0.01)
                    continue

                if msg:
                    # Convert to Python object
                    py_msg = ReplicationMessage.from_c_struct(msg)
                    if py_msg:
                        yield py_msg

                    # Free the message
                    # Note: libpq manages memory internally, so we don't free msg.data

        except asyncio.CancelledError:
            logger.info("Replication streaming cancelled")
            raise
        except Exception as e:
            logger.error(f"Replication streaming error: {e}")
            raise

    def send_standby_status(self, uint64_t write_lsn, uint64_t flush_lsn,
                           uint64_t apply_lsn):
        """
        Send standby status update to primary.

        Args:
            write_lsn: Last written LSN
            flush_lsn: Last flushed LSN
            apply_lsn: Last applied LSN
        """
        if not self.is_replication:
            raise PostgreSQLError("Replication not started")

        cdef int result = libpq.PQsendStandbyStatusUpdate(
            self._conn, write_lsn, flush_lsn, apply_lsn, 0, NULL
        )

        if result != 1:
            error_msg = libpq.PQerrorMessage(self._conn)
            if error_msg:
                raise PostgreSQLError(f"Failed to send standby status: {error_msg.decode('utf-8')}")
            else:
                raise PostgreSQLError("Failed to send standby status")

    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information."""
        if not self._conn:
            return {"connected": False}

        return {
            "connected": self.is_connected,
            "replication": self.is_replication,
            "backend_pid": libpq.PQbackendPID(self._conn),
            "server_version": libpq.PQparameterStatus(self._conn, "server_version").decode('utf-8') if libpq.PQparameterStatus(self._conn, "server_version") else None,
            "server_encoding": libpq.PQparameterStatus(self._conn, "server_encoding").decode('utf-8') if libpq.PQparameterStatus(self._conn, "server_encoding") else None,
            "client_encoding": libpq.PQparameterStatus(self._conn, "client_encoding").decode('utf-8') if libpq.PQparameterStatus(self._conn, "client_encoding") else None,
        }


# ============================================================================
# Convenience Functions
# ============================================================================

def connect(str connection_string) -> PostgreSQLConnection:
    """
    Create and connect to PostgreSQL.

    Args:
        connection_string: PostgreSQL connection string

    Returns:
        Connected PostgreSQLConnection instance
    """
    conn = PostgreSQLConnection(connection_string)
    conn.connect()
    return conn






