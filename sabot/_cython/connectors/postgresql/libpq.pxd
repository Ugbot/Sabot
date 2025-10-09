# cython: language_level=3
"""
Cython declarations for PostgreSQL libpq C API.

Provides access to PostgreSQL client library for logical replication and CDC.
"""

from libc.stdint cimport uint32_t, uint64_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy

# PostgreSQL Oid type
ctypedef unsigned int Oid

# Forward declarations
cdef extern from "libpq-fe.h":
    # Connection states
    cdef enum ConnStatusType:
        CONNECTION_OK
        CONNECTION_BAD
        CONNECTION_STARTED
        CONNECTION_MADE
        CONNECTION_AWAITING_RESPONSE
        CONNECTION_AUTH_OK
        CONNECTION_SETENV
        CONNECTION_SSL_STARTUP
        CONNECTION_NEEDED
        CONNECTION_CHECK_WRITABLE
        CONNECTION_CONSUME
        CONNECTION_GSS_STARTUP
        CONNECTION_CHECK_TARGET
        CONNECTION_CHECK_STANDBY

    cdef enum PostgresPollingStatusType:
        PGRES_POLLING_FAILED
        PGRES_POLLING_READING
        PGRES_POLLING_WRITING
        PGRES_POLLING_OK

    cdef enum ExecStatusType:
        PGRES_EMPTY_QUERY
        PGRES_COMMAND_OK
        PGRES_TUPLES_OK
        PGRES_COPY_OUT
        PGRES_COPY_IN
        PGRES_BAD_RESPONSE
        PGRES_NONFATAL_ERROR
        PGRES_FATAL_ERROR
        PGRES_COPY_BOTH
        PGRES_SINGLE_TUPLE
        PGRES_PIPELINE_SYNC
        PGRES_PIPELINE_ABORTED

    # Connection structure (opaque)
    ctypedef struct PGconn

    # Result structure (opaque)
    ctypedef struct PGresult

    # Connection functions
    PGconn* PQconnectdb(const char* conninfo)
    PGconn* PQconnectdbParams(const char* const* keywords, const char* const* values, int expand_dbname)
    void PQfinish(PGconn* conn)
    ConnStatusType PQstatus(const PGconn* conn)
    char* PQerrorMessage(const PGconn* conn)
    int PQsocket(const PGconn* conn)
    int PQbackendPID(const PGconn* conn)
    char* PQparameterStatus(const PGconn* conn, const char* paramName)

    # Query execution
    PGresult* PQexec(PGconn* conn, const char* query)
    PGresult* PQexecParams(PGconn* conn, const char* command, int nParams,
                          const Oid* paramTypes, const char* const* paramValues,
                          const int* paramLengths, const int* paramFormats, int resultFormat)
    void PQclear(PGresult* res)
    ExecStatusType PQresultStatus(const PGresult* res)
    char* PQresultErrorMessage(const PGresult* res)
    int PQntuples(const PGresult* res)
    int PQnfields(const PGresult* res)
    char* PQfname(const PGresult* res, int column_number)
    char* PQgetvalue(const PGresult* res, int row_number, int column_number)
    int PQgetisnull(const PGresult* res, int row_number, int column_number)
    int PQgetlength(const PGresult* res, int row_number, int column_number)
    Oid PQftype(const PGresult* res, int field_num)
    char* PQcmdStatus(PGresult* res)
    char* PQcmdTuples(PGresult* res)

    # Asynchronous operations
    int PQsendQuery(PGconn* conn, const char* query)
    int PQsendQueryParams(PGconn* conn, const char* command, int nParams,
                         const Oid* paramTypes, const char* const* paramValues,
                         const int* paramLengths, const int* paramFormats, int resultFormat)
    PGresult* PQgetResult(PGconn* conn)
    int PQconsumeInput(PGconn* conn)
    int PQisBusy(PGconn* conn)
    int PQflush(PGconn* conn)
    int PQsetnonblocking(PGconn* conn, int arg)

    # COPY operations
    int PQputCopyData(PGconn* conn, const char* buffer, int nbytes)
    int PQputCopyEnd(PGconn* conn, const char* errormsg)
    int PQgetCopyData(PGconn* conn, char** buffer, int async)

    # Large objects (for future use)
    int lo_open(PGconn* conn, Oid lobjId, int mode)
    int lo_close(PGconn* conn, int fd)
    int lo_read(PGconn* conn, int fd, char* buf, size_t len)
    int lo_write(PGconn* conn, int fd, const char* buf, size_t len)
    int lo_lseek(PGconn* conn, int fd, int offset, int whence)
    Oid lo_creat(PGconn* conn, int mode)
    Oid lo_create(PGconn* conn, Oid lobjId)
    int lo_tell(PGconn* conn, int fd)
    int lo_truncate(PGconn* conn, int fd, size_t len)
    int lo_unlink(PGconn* conn, Oid lobjId)

    # Replication functions (logical decoding)
    PGresult* PQexecStartReplication(PGconn* conn, const char* replication_slot_name,
                                    const char* timeline, uint64_t start_lsn,
                                    const char* plugin_args[])
    PGresult* PQexecCreateReplicationSlot(PGconn* conn, const char* slot_name,
                                         const char* output_plugin)
    PGresult* PQexecDropReplicationSlot(PGconn* conn, const char* slot_name)
    int PQsendStartReplication(PGconn* conn, const char* replication_slot_name,
                              const char* timeline, uint64_t start_lsn,
                              const char* plugin_args[])
    int PQsendCreateReplicationSlot(PGconn* conn, const char* slot_name,
                                   const char* output_plugin)
    int PQsendDropReplicationSlot(PGconn* conn, const char* slot_name)

    # Replication message reading
    ctypedef struct PGReplicationMessage:
        char* data
        size_t len
        uint64_t walEnd
        uint64_t serverWalEnd
        char* srvmsg_type
        uint64_t srvmsg_lsn

    int PQgetReplicationMessage(PGconn* conn, PGReplicationMessage** msg)

    # Logical replication feedback
    int PQsendStandbyStatusUpdate(PGconn* conn, uint64_t write_lsn, uint64_t flush_lsn,
                                 uint64_t apply_lsn, uint32_t reply_requested, const char* reply)
    int PQsendStandbyStatusUpdate_10plus(PGconn* conn, uint64_t write_lsn, uint64_t flush_lsn,
                                       uint64_t apply_lsn, uint32_t reply_requested)

# Replication message structure
cdef extern from "libpq-fe.h":
    pass  # Already declared above

# Helper functions for replication
cdef extern from "libpq-fe.h":
    # These are internal but we need them for replication
    int PQsetSingleRowMode(PGconn* conn)
    int PQenterPipelineMode(PGconn* conn)
    int PQexitPipelineMode(PGconn* conn)
    int PQpipelineSync(PGconn* conn)
    int PQsendFlushRequest(PGconn* conn)

# Additional PostgreSQL types
ctypedef unsigned long long XLogRecPtr
ctypedef unsigned long long TimestampTz

# Constants
cdef extern from "libpq-fe.h":
    cdef int PQ_COPYDATA
    cdef int PQ_COPYDONE
    cdef int PQ_COPYBOTH
    cdef int PQ_COPYIN
    cdef int PQ_COPYOUT

# Error codes
cdef extern from "libpq-fe.h":
    cdef char* PQresStatus(ExecStatusType status)






