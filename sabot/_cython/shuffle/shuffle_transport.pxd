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
cimport pyarrow.lib as ca
from pyarrow.includes.libarrow cimport (
    CRecordBatch as PCRecordBatch,
)

# Import lock-free Flight transport
from .flight_transport_lockfree cimport LockFreeFlightServer, LockFreeFlightClient


cdef class ShuffleServer:
    """
    Shuffle server using Arrow Flight for zero-copy IPC (lock-free).
    """
    cdef:
        string host
        int32_t port
        cbool running
        LockFreeFlightServer flight_server  # Lock-free Flight server

    cpdef void start(self) except *
    cpdef void stop(self) except *
    # register_partition now exposed as Python def method


cdef class ShuffleClient:
    """
    Shuffle client using Arrow Flight for zero-copy IPC (lock-free).
    """
    cdef:
        LockFreeFlightClient flight_client  # Lock-free Flight client
        int32_t max_connections
        int32_t max_retries
        double timeout_seconds

    # fetch_partition now exposed as Python def method


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
                                ca.RecordBatch batch)
    cdef ca.RecordBatch fetch_partition_from_agent(self, bytes agent_address,
                                                   bytes shuffle_id, int32_t partition_id)

    # Shuffle coordination methods (Phase 4)
    cpdef void start_shuffle(self, bytes shuffle_id, int32_t num_partitions, list downstream_agents)
    cpdef void send_partition(self, bytes shuffle_id, int32_t partition_id, ca.RecordBatch batch, bytes target_agent)
    cpdef list receive_partitions(self, bytes shuffle_id, int32_t partition_id)
    cpdef void end_shuffle(self, bytes shuffle_id)
