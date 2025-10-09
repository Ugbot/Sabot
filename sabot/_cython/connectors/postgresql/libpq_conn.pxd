# cython: language_level=3
"""
libpq Connection - Cython Declarations

Declarations for PostgreSQL libpq connection wrapper.
"""

from libc.stdint cimport uint64_t, int64_t


cdef extern from "libpq-fe.h":
    ctypedef struct PGconn:
        pass


cdef class PostgreSQLConnection:
    """
    High-performance PostgreSQL connection with logical replication support.
    """
    cdef:
        PGconn* conn
        str conninfo
        bint is_replication_conn
        uint64_t last_lsn
        object _loop  # asyncio event loop


cdef class ReplicationMessage:
    """Replication message from PostgreSQL."""
    cdef public:
        bytes data
        uint64_t wal_start
        uint64_t wal_end
        int64_t send_time
        str message_type
