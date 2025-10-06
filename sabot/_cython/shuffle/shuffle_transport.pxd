# cython: language_level=3
"""
Shuffle Transport type definitions - Arrow Flight-based network layer.

Zero-copy implementation using Sabot's direct Arrow C++ bindings.
"""

from libc.stdint cimport int32_t, int64_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.memory cimport shared_ptr

# Import Cython Arrow types
cimport pyarrow.lib as pa
from pyarrow.includes.libarrow cimport (
    CRecordBatch as PCRecordBatch,
)

# Import Flight transport
from .flight_transport cimport ShuffleFlightServer, ShuffleFlightClient


cdef class ShuffleServer:
    """
    Shuffle server using Arrow Flight for zero-copy IPC.
    """
    cdef:
        string host
        int32_t port
        cbool running
        ShuffleFlightServer flight_server
        dict _partition_store  # (shuffle_id, partition_id) -> pa.RecordBatch

    cpdef void start(self) except *
    cpdef void stop(self) except *
    cdef void register_partition(self, bytes shuffle_id, int32_t partition_id,
                                 pa.RecordBatch batch)


cdef class ShuffleClient:
    """
    Shuffle client using Arrow Flight for zero-copy IPC.
    """
    cdef:
        ShuffleFlightClient flight_client
        dict _connection_pool
        int32_t max_connections
        int32_t max_retries
        double timeout_seconds

    cdef pa.RecordBatch fetch_partition(self, bytes host, int32_t port,
                                        bytes shuffle_id, int32_t partition_id)


cdef class ShuffleTransport:
    """
    Unified shuffle transport managing both server and client.
    """
    cdef:
        ShuffleServer server
        ShuffleClient client
        string agent_host
        int32_t agent_port
        cbool initialized

    cpdef void start(self) except *
    cpdef void stop(self) except *
    cdef void publish_partition(self, bytes shuffle_id, int32_t partition_id,
                                pa.RecordBatch batch)
    cdef pa.RecordBatch fetch_partition_from_agent(self, bytes agent_address,
                                                   bytes shuffle_id, int32_t partition_id)
