# cython: language_level=3
"""
Cython declarations for PostgreSQL libpq C API.

Covers connection management, query execution, and logical replication protocol.
Based on PostgreSQL 14+ libpq interface.
"""

from libc.stdint cimport uint32_t, uint64_t, int64_t
from libc.stdio cimport FILE

cdef extern from "libpq-fe.h" nogil:
    # ========================================================================
    # Connection Management
    # ========================================================================

    ctypedef struct pg_conn:
        pass
    ctypedef pg_conn* PGconn

    ctypedef struct pg_result:
        pass
    ctypedef pg_result* PGresult

    # Connection status
    ctypedef enum ConnStatusType:
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

    # Result status
    ctypedef enum ExecStatusType:
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

    # Connection functions
    PGconn* PQconnectdb(const char* conninfo)
    PGconn* PQsetdbLogin(const char* pghost, const char* pgport,
                         const char* pgoptions, const char* pgtty,
                         const char* dbName, const char* login, const char* pwd)
    void PQfinish(PGconn* conn)
    ConnStatusType PQstatus(const PGconn* conn)
    char* PQerrorMessage(const PGconn* conn)
    int PQsocket(const PGconn* conn)

    # Query execution
    PGresult* PQexec(PGconn* conn, const char* command)
    PGresult* PQexecParams(PGconn* conn, const char* command,
                           int nParams, const unsigned int* paramTypes,
                           const char* const* paramValues,
                           const int* paramLengths,
                           const int* paramFormats,
                           int resultFormat)

    # Result handling
    ExecStatusType PQresultStatus(const PGresult* res)
    char* PQresultErrorMessage(const PGresult* res)
    void PQclear(PGresult* res)
    int PQntuples(const PGresult* res)
    int PQnfields(const PGresult* res)
    char* PQfname(const PGresult* res, int column_number)
    char* PQgetvalue(const PGresult* res, int row_number, int column_number)
    int PQgetlength(const PGresult* res, int row_number, int column_number)
    int PQgetisnull(const PGresult* res, int row_number, int column_number)

    # ========================================================================
    # Logical Replication
    # ========================================================================

    # Replication slot functions
    PGresult* PQcreateReplicationSlot(PGconn* conn, const char* slot_name,
                                       const char* output_plugin)
    PGresult* PQdropReplicationSlot(PGconn* conn, const char* slot_name)

    # Start replication
    int PQsendQuery(PGconn* conn, const char* command)
    PGresult* PQgetResult(PGconn* conn)

    # COPY protocol (used for logical replication streaming)
    int PQgetCopyData(PGconn* conn, char** buffer, int async_)
    int PQputCopyData(PGconn* conn, const char* buffer, int nbytes)
    int PQflush(PGconn* conn)
    int PQputCopyEnd(PGconn* conn, const char* errormsg)

    # ========================================================================
    # Type OIDs (for type mapping)
    # ========================================================================

    # Built-in type OIDs
    cdef enum:
        BOOLOID = 16
        BYTEAOID = 17
        CHAROID = 18
        NAMEOID = 19
        INT8OID = 20
        INT2OID = 21
        INT4OID = 23
        TEXTOID = 25
        OIDOID = 26
        FLOAT4OID = 700
        FLOAT8OID = 701
        TIMESTAMPOID = 1114
        TIMESTAMPTZOID = 1184
        DATEOID = 1082
        TIMEOID = 1083
        TIMETZOID = 1266
        NUMERICOID = 1700
        UUIDOID = 2950
        JSONOID = 114
        JSONBOID = 3802
        VARCHAROID = 1043
        BPCHAROID = 1042


# ========================================================================
# PostgreSQL Replication Protocol Structures
# ========================================================================

cdef extern from "postgres_ext.h" nogil:
    # Oid type
    ctypedef unsigned int Oid

    # LSN (Log Sequence Number)
    ctypedef uint64_t XLogRecPtr

    # Transaction ID
    ctypedef uint32_t TransactionId


# ========================================================================
# Custom Replication Structures (from pg_recvlogical / pg_basebackup)
# ========================================================================

cdef extern from *:
    """
    // Replication message structure (simplified)
    typedef struct PGReplicationMessage {
        char    *data;          // Message data
        int      len;           // Message length
        uint64_t walEnd;        // WAL end position
        uint64_t serverWalEnd;  // Server WAL end position
        int64_t  sendTime;      // Send timestamp
        char     srvmsg_type;   // Message type ('w' = XLogData, 'k' = keepalive)
        uint64_t srvmsg_lsn;    // Message LSN
    } PGReplicationMessage;

    // Standby status update (for keepalive)
    typedef struct PGStandbyStatusUpdate {
        uint64_t write_lsn;     // Last WAL position written
        uint64_t flush_lsn;     // Last WAL position flushed
        uint64_t apply_lsn;     // Last WAL position applied
        int64_t  sendTime;      // Send timestamp
        char     replyRequested; // Reply requested flag
    } PGStandbyStatusUpdate;
    """

    ctypedef struct PGReplicationMessage:
        char*    data
        int      len
        uint64_t walEnd
        uint64_t serverWalEnd
        int64_t  sendTime
        char     srvmsg_type
        uint64_t srvmsg_lsn

    ctypedef struct PGStandbyStatusUpdate:
        uint64_t write_lsn
        uint64_t flush_lsn
        uint64_t apply_lsn
        int64_t  sendTime
        char     replyRequested


# ========================================================================
# Replication Helper Functions (Custom)
# ========================================================================

cdef extern from *:
    """
    #include <stdint.h>

    // Platform-specific byte order functions
    #if defined(__APPLE__)
    #include <libkern/OSByteOrder.h>
    #define be64toh(x) OSSwapBigToHostInt64(x)
    #define htobe64(x) OSSwapHostToBigInt64(x)
    #elif defined(__linux__)
    #include <endian.h>
    #else
    #error "Unsupported platform for byte order functions"
    #endif

    // Parse XLogData message from replication stream
    static inline int parse_xlogdata_message(
        const char *buf,
        int len,
        uint64_t *walStart,
        uint64_t *walEnd,
        uint64_t *sendTime,
        const char **data,
        int *dataLen
    ) {
        if (len < 1 + 8 + 8 + 8) {
            return -1;  // Too short
        }

        // Message type ('w' for XLogData)
        if (buf[0] != 'w') {
            return -1;
        }

        // Parse header (network byte order)
        const char *p = buf + 1;
        *walStart = be64toh(*(uint64_t*)p); p += 8;
        *walEnd = be64toh(*(uint64_t*)p); p += 8;
        *sendTime = be64toh(*(uint64_t*)p); p += 8;

        // Data follows header
        *data = p;
        *dataLen = len - (1 + 8 + 8 + 8);

        return 0;
    }

    // Build standby status update message
    static inline int build_standby_status_update(
        char *buf,
        int buflen,
        uint64_t write_lsn,
        uint64_t flush_lsn,
        uint64_t apply_lsn,
        int64_t sendTime,
        char replyRequested
    ) {
        if (buflen < 1 + 8 + 8 + 8 + 8 + 1) {
            return -1;  // Buffer too small
        }

        char *p = buf;
        *p++ = 'r';  // Standby status update message type

        // LSNs (network byte order)
        *(uint64_t*)p = htobe64(write_lsn); p += 8;
        *(uint64_t*)p = htobe64(flush_lsn); p += 8;
        *(uint64_t*)p = htobe64(apply_lsn); p += 8;
        *(int64_t*)p = htobe64(sendTime); p += 8;
        *p++ = replyRequested;

        return (int)(p - buf);
    }
    """

    int parse_xlogdata_message(
        const char *buf,
        int len,
        uint64_t *walStart,
        uint64_t *walEnd,
        uint64_t *sendTime,
        const char **data,
        int *dataLen
    ) nogil

    int build_standby_status_update(
        char *buf,
        int buflen,
        uint64_t write_lsn,
        uint64_t flush_lsn,
        uint64_t apply_lsn,
        int64_t sendTime,
        char replyRequested
    ) nogil
