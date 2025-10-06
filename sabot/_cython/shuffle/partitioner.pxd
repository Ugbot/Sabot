# cython: language_level=3
"""
Partitioner type definitions - Hash/Range/RoundRobin partitioning for shuffle.

Zero-copy implementation using Sabot's direct Arrow C++ bindings.
"""

from libc.stdint cimport int32_t, int64_t, uint32_t, uint64_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.memory cimport shared_ptr

# Import Cython Arrow types (not Python!)
cimport pyarrow.lib as pa
from pyarrow.includes.libarrow cimport (
    CRecordBatch as PCRecordBatch,
    CSchema as PCSchema,
    CArray as PCArray,
)

from .types cimport ShuffleEdgeType


cdef class Partitioner:
    """
    Base partitioner interface.

    Partitions Arrow RecordBatches based on key columns, producing N output
    batches (one per partition) for redistribution to downstream tasks.
    """
    cdef:
        int32_t num_partitions
        vector[string] key_columns
        shared_ptr[PCSchema] schema_cpp

    cdef vector[shared_ptr[PCRecordBatch]] partition_batch(
        self,
        pa.RecordBatch batch  # Cython type, not Python!
    )

    cdef int32_t get_partition_for_row(self, int64_t row_index)


cdef class HashPartitioner(Partitioner):
    """
    Hash-based partitioner.

    Uses MurmurHash3 to deterministically assign rows to partitions based on
    key column values. Ensures same keys go to same partition.

    Algorithm:
        partition_id = hash(key_columns) % num_partitions

    Performance: >1M rows/sec on M1 Pro
    """
    cdef:
        shared_ptr[PCArray] _hash_cache  # Cached hash values for current batch
        cbool _use_murmur3  # Use MurmurHash3 (default true)

    cdef void _compute_hashes(self, pa.RecordBatch batch)


cdef class RangePartitioner(Partitioner):
    """
    Range-based partitioner.

    Partitions data based on key ranges, useful for sorted data or when
    data distribution is known upfront.

    Example:
        partition 0: keys in [0, 100)
        partition 1: keys in [100, 200)
        partition 2: keys in [200, 300)
    """
    cdef:
        vector[int64_t] _range_boundaries  # Partition boundaries
        cbool _ranges_initialized

    cpdef void set_ranges(self, vector[int64_t] boundaries) except *
    cdef int32_t _get_partition_for_value(self, int64_t value) nogil


cdef class RoundRobinPartitioner(Partitioner):
    """
    Round-robin partitioner.

    Distributes rows evenly across partitions in round-robin fashion.
    No key columns needed - stateless distribution for load balancing.

    Useful for: Rebalancing after data skew, stateless operators
    """
    cdef:
        int64_t _current_index  # Current position in round-robin


# Factory function
cpdef Partitioner create_partitioner(
    ShuffleEdgeType edge_type,
    int32_t num_partitions,
    vector[string] key_columns,
    pa.Schema schema  # Cython type
) except *
