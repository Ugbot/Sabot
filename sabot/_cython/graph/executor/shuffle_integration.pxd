# cython: language_level=3
"""
Shuffle Integration Header

Cython declarations for zero-copy shuffle orchestration.
"""

from libcpp.vector cimport vector
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string
from libc.stdint cimport int32_t, int64_t

cimport pyarrow.lib as ca
from pyarrow.lib cimport CRecordBatch as PCRecordBatch


cdef class ShuffleOrchestrator:
    """Zero-copy shuffle orchestrator."""

    # Attributes
    cdef object shuffle_transport
    cdef list agent_addresses
    cdef int32_t local_agent_id

    # Public methods
    cpdef object execute_with_shuffle(
        self,
        object operator,
        object batch,
        bytes shuffle_id,
        list partition_keys
    )

    # Private methods (C++)
    cdef void _partition_and_send_zero_copy(
        self,
        object partitioner,
        object record_batch,
        bytes shuffle_id,
        int32_t num_agents
    ) except *

    cdef void _send_partitions_cpp(
        self,
        object partitioner,
        object record_batch,
        bytes shuffle_id,
        int32_t num_agents
    ) except *

    cdef void _send_partitions_python(
        self,
        list partitions,
        bytes shuffle_id
    ) except *
