# cython: language_level=3
"""
Arrow Flight Transport C++ Wrapper - Type Definitions

Zero-copy network transport for shuffle partitions using Arrow Flight C++ API.
"""

from libc.stdint cimport int32_t, int64_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.memory cimport unique_ptr, shared_ptr
from libcpp.unordered_map cimport unordered_map

# Cython Arrow types
cimport pyarrow.lib as pa

# Arrow Flight C++ API
from pyarrow.includes.libarrow_flight cimport (
    CFlightClient,
    CFlightStreamReader,
    CLocation,
    CTicket,
    CFlightCallOptions,
)


cdef class ShuffleFlightClient:
    """
    Arrow Flight client for fetching shuffle partitions.

    Uses C++ Flight API for zero-copy network transport.
    Maintains connection pool to remote agents.
    """
    cdef:
        dict _connections  # (host, port) -> unique_ptr[CFlightClient]
        object _lock  # threading.Lock

    cdef public int32_t max_connections
    cdef public int32_t max_retries
    cdef public double timeout_seconds

    cdef unique_ptr[CFlightClient]* _get_connection(self, bytes host, int32_t port) except NULL
    cpdef pa.RecordBatch fetch_partition(
        self,
        bytes host,
        int32_t port,
        bytes shuffle_id,
        int32_t partition_id
    )
    cpdef void close_all(self) except *


cdef class ShuffleFlightServer:
    """
    Arrow Flight server for serving shuffle partitions.

    Uses C++ Flight API for zero-copy network transport.
    Stores partitions in memory and serves via do_get().
    """
    cdef:
        string host
        int32_t port
        dict _partition_store  # (shuffle_id, partition_id) -> pa.RecordBatch
        object _server_thread  # threading.Thread
        object _server  # PyArrow FlightServerBase (for now)
        object _lock  # threading.Lock

    cdef public cbool running

    cpdef void start(self) except *
    cpdef void stop(self) except *
    cpdef void register_partition(
        self,
        bytes shuffle_id,
        int32_t partition_id,
        pa.RecordBatch batch
    ) except *
    cpdef pa.RecordBatch get_partition(
        self,
        bytes shuffle_id,
        int32_t partition_id
    )
