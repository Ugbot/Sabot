# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
"""
DuckDB C API Cython Wrapper for Sabot.

Zero-copy Arrow integration for reading from and writing to DuckDB-supported data sources.
Uses vendored Arrow C++ API directly via pyarrow.lib cimport for true zero-copy.

Performance:
- Query execution: <1ms overhead
- Arrow streaming: Zero-copy (direct C++ buffer access)
- Filter/projection pushdown: Automatic via DuckDB optimizer

Usage:
    >>> conn = DuckDBConnection(':memory:')
    >>> result = conn.execute_arrow("SELECT * FROM read_parquet('data.parquet')")
    >>> for batch in result:
    >>>     process(batch)
"""

from libc.stdlib cimport malloc, free
from libc.string cimport strcpy, strlen
from libcpp.memory cimport shared_ptr
from cpython.ref cimport PyObject, Py_INCREF, Py_DECREF

# Import DuckDB C API declarations
from sabot._cython.connectors.duckdb_core cimport *

# Import vendored Arrow C++ API directly (zero-copy)
cimport pyarrow.lib as ca
from pyarrow.includes.libarrow cimport (
    CRecordBatch as PCRecordBatch,
    CSchema as PCSchema,
    CArray as PCArray,
)
from pyarrow.includes.common cimport *

import logging
logger = logging.getLogger(__name__)


# ============================================================================
# Error Handling
# ============================================================================

class DuckDBError(Exception):
    """DuckDB operation error."""

    def __init__(self, message: str, query: str = None):
        self.message = message
        self.query = query
        super().__init__(message)

    def __str__(self):
        if self.query:
            return f"{self.message} (query: {self.query[:100]}...)"
        return self.message


cdef inline void check_duckdb_error(duckdb_state state, str context) except *:
    """Check DuckDB operation status and raise on error."""
    if state == DuckDBError:
        raise DuckDBError(f"DuckDB error: {context}")


# ============================================================================
# DuckDB Connection Wrapper
# ============================================================================

cdef class DuckDBConnection:
    """
    High-performance DuckDB connection for Arrow streaming.

    Manages database lifecycle and provides zero-copy query execution.
    All queries return Arrow results for seamless Sabot integration.

    Examples:
        >>> # In-memory database
        >>> conn = DuckDBConnection()

        >>> # Persistent database
        >>> conn = DuckDBConnection('/path/to/db.duckdb')

        >>> # Execute query with Arrow results
        >>> result = conn.execute_arrow("SELECT * FROM read_parquet('data.parquet')")
        >>> for batch in result:
        >>>     print(batch.num_rows)
    """

    cdef duckdb_database db
    cdef duckdb_connection conn
    cdef readonly str path
    cdef bint is_open

    def __init__(self, str path = ':memory:'):
        """
        Initialize DuckDB connection.

        Args:
            path: Database file path, or ':memory:' for in-memory database
        """
        self.path = path
        self.is_open = False
        self.db = NULL
        self.conn = NULL

        # Open database
        cdef bytes path_bytes = path.encode('utf-8')
        cdef const char* path_cstr = path_bytes
        cdef duckdb_state state

        with nogil:
            state = duckdb_open(path_cstr, &self.db)

        check_duckdb_error(state, f"Failed to open database: {path}")

        # Create connection
        with nogil:
            state = duckdb_connect(self.db, &self.conn)

        if state == DuckDBError:
            with nogil:
                duckdb_close(&self.db)
            raise DuckDBError(f"Failed to create connection to {path}")

        self.is_open = True
        logger.info(f"DuckDB connection opened: {path}")

    def __dealloc__(self):
        """Cleanup resources on garbage collection."""
        self.close()

    def close(self):
        """Close connection and database."""
        if self.is_open:
            with nogil:
                if self.conn != NULL:
                    duckdb_disconnect(&self.conn)
                if self.db != NULL:
                    duckdb_close(&self.db)

            self.conn = NULL
            self.db = NULL
            self.is_open = False
            logger.debug(f"DuckDB connection closed: {self.path}")

    def execute_arrow(self, str query):
        """
        Execute SQL query and return Arrow result for streaming.

        Args:
            query: SQL query string

        Returns:
            DuckDBArrowResult: Arrow streaming result

        Raises:
            DuckDBError: If query execution fails
        """
        if not self.is_open:
            raise DuckDBError("Connection is closed")

        return DuckDBArrowResult(self, query)

    def execute(self, str query):
        """
        Execute SQL query without returning results (DDL, DML).

        Args:
            query: SQL query string

        Raises:
            DuckDBError: If query execution fails
        """
        cdef bytes query_bytes = query.encode('utf-8')
        cdef const char* query_cstr = query_bytes
        cdef duckdb_result result
        cdef duckdb_state state

        with nogil:
            state = duckdb_query(self.conn, query_cstr, &result)

        # Always destroy result
        with nogil:
            duckdb_destroy_result(&result)

        check_duckdb_error(state, f"Query failed: {query[:100]}")

    def install_extension(self, str extension_name):
        """
        Install DuckDB extension.

        Args:
            extension_name: Extension name (e.g., 'httpfs', 'postgres_scanner')
        """
        query = f"INSTALL {extension_name}"
        self.execute(query)
        logger.info(f"Installed extension: {extension_name}")

    def load_extension(self, str extension_name):
        """
        Load installed DuckDB extension.

        Args:
            extension_name: Extension name
        """
        query = f"LOAD {extension_name}"
        self.execute(query)
        logger.info(f"Loaded extension: {extension_name}")

    @property
    def version(self) -> str:
        """Get DuckDB library version."""
        cdef const char* version_cstr
        with nogil:
            version_cstr = duckdb_library_version()
        return version_cstr.decode('utf-8')


# ============================================================================
# Arrow Result Streaming
# ============================================================================

cdef class DuckDBArrowResult:
    """
    Streaming Arrow result from DuckDB query.

    Provides zero-copy iteration over Arrow RecordBatches.
    Automatically handles cleanup of DuckDB resources.

    Examples:
        >>> result = conn.execute_arrow("SELECT * FROM table")
        >>> for batch in result:
        >>>     print(f"Batch: {batch.num_rows} rows")

        >>> # Access schema
        >>> print(result.schema)

        >>> # Get metadata
        >>> print(f"Total rows: {result.row_count}")
    """

    cdef duckdb_arrow arrow_result
    cdef duckdb_connection conn_ref
    cdef readonly str query
    cdef object _schema  # Arrow schema (cached)
    cdef bint is_consumed

    def __init__(self, DuckDBConnection connection, str query):
        """
        Initialize Arrow result (internal use only).

        Args:
            connection: DuckDBConnection instance
            query: SQL query string
        """
        self.conn_ref = connection.conn
        self.query = query
        self._schema = None
        self.is_consumed = False
        self.arrow_result = NULL

        # Execute query
        cdef bytes query_bytes = query.encode('utf-8')
        cdef const char* query_cstr = query_bytes
        cdef duckdb_state state

        with nogil:
            state = duckdb_query_arrow(self.conn_ref, query_cstr, &self.arrow_result)

        if state == DuckDBError:
            cdef const char* error_msg
            if self.arrow_result != NULL:
                with nogil:
                    error_msg = duckdb_query_arrow_error(self.arrow_result)
                error_str = error_msg.decode('utf-8')
                with nogil:
                    duckdb_destroy_arrow(&self.arrow_result)
            else:
                error_str = "Unknown error"

            raise DuckDBError(error_str, query)

    def __dealloc__(self):
        """Cleanup Arrow result."""
        if self.arrow_result != NULL:
            with nogil:
                duckdb_destroy_arrow(&self.arrow_result)
            self.arrow_result = NULL

    def __iter__(self):
        """Iterate over Arrow RecordBatches."""
        self.is_consumed = False
        return self

    def __next__(self):
        """Get next Arrow RecordBatch (zero-copy)."""
        if self.is_consumed:
            raise StopIteration

        if self.arrow_result == NULL:
            raise StopIteration

        # Get Arrow schema on first call
        if self._schema is None:
            self._get_schema()

        # Get next Arrow array (batch)
        cdef duckdb_arrow_array arrow_array_wrapper
        cdef duckdb_arrow_schema arrow_schema_wrapper
        cdef duckdb_state state

        # Get schema wrapper for this batch
        with nogil:
            state = duckdb_query_arrow_schema(self.arrow_result, &arrow_schema_wrapper)

        if state == DuckDBError:
            self.is_consumed = True
            raise StopIteration

        # Get array wrapper
        with nogil:
            state = duckdb_query_arrow_array(self.arrow_result, &arrow_array_wrapper)

        if state == DuckDBError:
            self.is_consumed = True
            raise StopIteration

        # Convert to vendored Arrow RecordBatch using Arrow C Data Interface
        # DuckDB returns ArrowArray and ArrowSchema via the C Data Interface standard
        # We import directly into vendored Arrow C++ (zero-copy)

        # Cast DuckDB opaque pointers to ArrowArray/ArrowSchema pointers
        cdef ArrowArray* c_array = <ArrowArray*>(<void*>arrow_array_wrapper)
        cdef ArrowSchema* c_schema = <ArrowSchema*>(<void*>arrow_schema_wrapper)

        try:
            # Import using vendored Arrow's C Data Interface
            # This creates a ca.RecordBatch which wraps shared_ptr[PCRecordBatch]
            batch = ca.RecordBatch._import_from_c(<uintptr_t>c_array, <uintptr_t>c_schema)
            return batch

        except Exception as e:
            self.is_consumed = True
            raise RuntimeError(f"Failed to convert DuckDB Arrow batch (zero-copy): {e}") from e

    cdef void _get_schema(self) except *:
        """Extract Arrow schema from result (zero-copy internal)."""
        cdef duckdb_arrow_schema arrow_schema_wrapper
        cdef duckdb_state state

        with nogil:
            state = duckdb_query_arrow_schema(self.arrow_result, &arrow_schema_wrapper)

        check_duckdb_error(state, "Failed to get Arrow schema")

        # Convert to vendored Arrow Schema using C Data Interface (zero-copy)
        cdef ArrowSchema* c_schema = <ArrowSchema*>(<void*>arrow_schema_wrapper)

        try:
            # Import using vendored Arrow's C Data Interface
            # Creates ca.Schema wrapping shared_ptr[PCSchema]
            self._schema = ca.Schema._import_from_c(<uintptr_t>c_schema)

        except Exception as e:
            raise RuntimeError(f"Failed to convert DuckDB Arrow schema (zero-copy): {e}") from e

    @property
    def schema(self):
        """Get Arrow schema."""
        if self._schema is None:
            self._get_schema()
        return self._schema

    @property
    def column_count(self) -> int:
        """Get number of columns."""
        cdef idx_t count
        with nogil:
            count = duckdb_arrow_column_count(self.arrow_result)
        return count

    @property
    def row_count(self) -> int:
        """Get total number of rows."""
        cdef idx_t count
        with nogil:
            count = duckdb_arrow_row_count(self.arrow_result)
        return count


# ============================================================================
# Public API Functions
# ============================================================================

def get_duckdb_version() -> str:
    """Get DuckDB library version."""
    cdef const char* version
    with nogil:
        version = duckdb_library_version()
    return version.decode('utf-8')
