# cython: language_level=3
"""
Cython declarations for DuckDB C API.

Wraps the DuckDB C API from vendor/duckdb for zero-copy Arrow integration.
Based on vendor/duckdb/src/include/duckdb.h
"""

from libc.stdint cimport uint8_t, int8_t, int16_t, int32_t, int64_t, uint64_t, uintptr_t
from libc.stddef cimport size_t

# Forward declare Arrow C Data Interface structs
cdef extern from "arrow/c/abi.h":
    ctypedef struct ArrowSchema:
        pass

    ctypedef struct ArrowArray:
        pass

# DuckDB C API declarations
cdef extern from "duckdb.h":
    # ========================================================================
    # Type Definitions
    # ========================================================================

    ctypedef uint64_t idx_t

    # Opaque pointer types (all use void* internally)
    ctypedef struct _duckdb_database:
        void *internal_ptr
    ctypedef _duckdb_database* duckdb_database

    ctypedef struct _duckdb_connection:
        void *internal_ptr
    ctypedef _duckdb_connection* duckdb_connection

    ctypedef struct _duckdb_config:
        void *internal_ptr
    ctypedef _duckdb_config* duckdb_config

    ctypedef struct _duckdb_arrow:
        void *internal_ptr
    ctypedef _duckdb_arrow* duckdb_arrow

    ctypedef struct _duckdb_arrow_schema:
        void *internal_ptr
    ctypedef _duckdb_arrow_schema* duckdb_arrow_schema

    ctypedef struct _duckdb_arrow_array:
        void *internal_ptr
    ctypedef _duckdb_arrow_array* duckdb_arrow_array

    ctypedef struct _duckdb_arrow_stream:
        void *internal_ptr
    ctypedef _duckdb_arrow_stream* duckdb_arrow_stream

    ctypedef struct _duckdb_arrow_options:
        void *internal_ptr
    ctypedef _duckdb_arrow_options* duckdb_arrow_options

    # Error data structure
    ctypedef struct _duckdb_error_data:
        void *internal_ptr
    ctypedef _duckdb_error_data* duckdb_error_data

    # ========================================================================
    # Enums
    # ========================================================================

    ctypedef enum duckdb_state:
        DuckDBSuccess = 0
        DuckDBError = 1

    ctypedef enum duckdb_pending_state:
        DUCKDB_PENDING_RESULT_READY = 0
        DUCKDB_PENDING_RESULT_NOT_READY = 1
        DUCKDB_PENDING_ERROR = 2
        DUCKDB_PENDING_NO_TASKS_AVAILABLE = 3

    # ========================================================================
    # Core Database Functions
    # ========================================================================

    # Database lifecycle
    duckdb_state duckdb_open(const char *path, duckdb_database *out_database) nogil
    duckdb_state duckdb_open_ext(const char *path, duckdb_database *out_database,
                                  duckdb_config config, char **out_error) nogil
    void duckdb_close(duckdb_database *database) nogil

    # Connection lifecycle
    duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out_connection) nogil
    void duckdb_disconnect(duckdb_connection *connection) nogil
    void duckdb_interrupt(duckdb_connection connection) nogil

    # Get Arrow options from connection
    void duckdb_connection_get_arrow_options(duckdb_connection connection,
                                             duckdb_arrow_options *out_arrow_options) nogil

    # Library version
    const char* duckdb_library_version() nogil

    # ========================================================================
    # Configuration
    # ========================================================================

    duckdb_state duckdb_create_config(duckdb_config *out_config) nogil
    void duckdb_destroy_config(duckdb_config *config) nogil
    duckdb_state duckdb_set_config(duckdb_config config, const char *name, const char *option) nogil

    # ========================================================================
    # Query Execution
    # ========================================================================

    # Simple query execution (returns regular result, not Arrow)
    ctypedef struct duckdb_result:
        pass

    duckdb_state duckdb_query(duckdb_connection connection, const char *query,
                             duckdb_result *out_result) nogil
    void duckdb_destroy_result(duckdb_result *result) nogil

    # ========================================================================
    # Arrow Integration - Reading (DuckDB → Arrow)
    # ========================================================================

    # Execute query and get Arrow result
    duckdb_state duckdb_query_arrow(duckdb_connection connection, const char *query,
                                   duckdb_arrow *out_result) nogil

    # Get Arrow schema from result
    duckdb_state duckdb_query_arrow_schema(duckdb_arrow result,
                                          duckdb_arrow_schema *out_schema) nogil

    # Get Arrow array (batch) from result
    duckdb_state duckdb_query_arrow_array(duckdb_arrow result,
                                         duckdb_arrow_array *out_array) nogil

    # Get metadata
    idx_t duckdb_arrow_column_count(duckdb_arrow result) nogil
    idx_t duckdb_arrow_row_count(duckdb_arrow result) nogil
    idx_t duckdb_arrow_rows_changed(duckdb_arrow result) nogil

    # Get error message
    const char* duckdb_query_arrow_error(duckdb_arrow result) nogil

    # Cleanup
    void duckdb_destroy_arrow(duckdb_arrow *result) nogil
    void duckdb_destroy_arrow_stream(duckdb_arrow_stream *stream_p) nogil
    void duckdb_destroy_arrow_options(duckdb_arrow_options *arrow_options) nogil

    # ========================================================================
    # Arrow Integration - Writing (Arrow → DuckDB)
    # ========================================================================

    # Register Arrow stream as a virtual table
    duckdb_state duckdb_arrow_scan(duckdb_connection connection, const char *table_name,
                                  duckdb_arrow_stream arrow) nogil

    # Register Arrow array as a virtual table (deprecated but still usable)
    duckdb_state duckdb_arrow_array_scan(duckdb_connection connection, const char *table_name,
                                        duckdb_arrow_schema arrow_schema,
                                        duckdb_arrow_array arrow_array,
                                        duckdb_arrow_stream *out_stream) nogil

    # ========================================================================
    # Memory Management
    # ========================================================================

    void duckdb_free(void *ptr) nogil


# ============================================================================
# Helper functions for error handling (to be implemented in .pyx)
# ============================================================================

cdef class DuckDBError(Exception):
    """DuckDB error with context."""
    cdef readonly str message
    cdef readonly str query


cdef inline void check_duckdb_error(duckdb_state state, str context) except *
