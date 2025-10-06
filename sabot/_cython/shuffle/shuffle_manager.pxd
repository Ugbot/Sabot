# cython: language_level=3
"""
Shuffle Manager type definitions - Coordination of shuffle operations.

Zero-copy implementation using Sabot's direct Arrow C++ bindings.
"""

from libc.stdint cimport int32_t, int64_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.memory cimport shared_ptr
from libcpp.unordered_map cimport unordered_map

# Import Cython Arrow types
cimport pyarrow.lib as ca
from pyarrow.includes.libarrow cimport (
    CRecordBatch as PCRecordBatch,
    CSchema as PCSchema,
)

from .types cimport ShuffleMetadata, PartitionInfo, ShuffleEdgeType
from .partitioner cimport Partitioner
from .shuffle_buffer cimport ShuffleBuffer, NetworkBufferPool
from .shuffle_transport cimport ShuffleTransport


cdef class ShuffleManager:
    """
    Manages all shuffle operations for an agent.

    Coordinates partitioning, buffering, and transport for all shuffles.
    Each agent has one ShuffleManager instance.
    """
    cdef:
        string agent_id
        string agent_host
        int32_t agent_port

        # Shuffle metadata
        unordered_map[string, ShuffleMetadata] shuffles

        # Components (stored as dict for now, can be optimized)
        dict _partitioners  # shuffle_id -> Partitioner
        dict _partition_buffers  # shuffle_id -> {partition_id -> ShuffleBuffer}
        ShuffleTransport transport
        NetworkBufferPool buffer_pool
        dict _task_locations

    # Shuffle lifecycle
    cpdef bytes register_shuffle(
        self, bytes operator_id, int32_t num_partitions,
        vector[string] partition_keys, ShuffleEdgeType edge_type,
        ca.Schema schema
    ) except *

    # Write side (upstream tasks)
    cdef void write_partition(
        self, bytes shuffle_id, int32_t partition_id,
        ca.RecordBatch batch
    )

    # Read side (downstream tasks)
    cdef ca.RecordBatch read_partition(
        self, bytes shuffle_id, int32_t partition_id,
        list upstream_agents
    )

    # Metadata
    cpdef void set_task_location(self, bytes shuffle_id, int32_t task_id,
                                 bytes agent_address) except *

    # Lifecycle
    cpdef void start(self) except *
    cpdef void stop(self) except *
