# cython: language_level=3
"""
Common C++ type definitions for shuffle module.
"""

from libc.stdint cimport int32_t, int64_t, uint8_t, uint32_t, uint64_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.unordered_map cimport unordered_map
from libcpp.pair cimport pair

# Arrow C++ types
from pyarrow.includes.libarrow cimport (
    CRecordBatch,
    CSchema,
    CArray,
    CField,
    CDataType,
    CStatus,
    CResult,
    CMemoryPool,
)

# Shuffle edge types (matches execution graph)
cpdef enum ShuffleEdgeType:
    FORWARD = 0      # 1:1 mapping, no shuffle (operator chaining)
    HASH = 1         # Hash-based repartitioning by key
    BROADCAST = 2    # Replicate to all downstream tasks
    REBALANCE = 3    # Round-robin redistribution
    RANGE = 4        # Range-based partitioning for sorted data

# Shuffle metadata structure
cdef struct ShuffleMetadata:
    string shuffle_id
    int32_t num_partitions
    int32_t num_upstream_tasks
    int32_t num_downstream_tasks
    ShuffleEdgeType edge_type
    vector[string] partition_keys

# Partition information
cdef struct PartitionInfo:
    int32_t partition_id
    int64_t num_rows
    int64_t num_bytes
    string location  # Agent address serving this partition
    cbool is_local   # True if partition is on same agent
