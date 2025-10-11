# cython: language_level=3

from sabot._cython.operators.base_operator cimport BaseOperator
from libc.stdint cimport int32_t, int64_t
from libcpp.memory cimport shared_ptr
from libcpp.vector cimport vector
from pyarrow.includes.libarrow cimport CRecordBatch as PCRecordBatch
from sabot._cython.shuffle.partitioner cimport Partitioner
cimport pyarrow.lib as ca

cdef class ShuffledOperator(BaseOperator):
    """Base class for operators requiring network shuffle."""

    cdef:
        # Public attributes (for tests and metadata)
        public str operator_id           # Unique operator identifier
        public int32_t parallelism       # Parallelism hint

        # Internal attributes
        list _partition_keys             # Columns to partition by
        int32_t _num_partitions          # Number of downstream tasks
        bint _stateful                   # Always True for shuffled ops

        # Shuffle transport (set by agent runtime)
        object _shuffle_transport        # ShuffleTransport instance
        bytes _shuffle_id                # Unique shuffle ID
        int32_t _task_id                 # This task's partition ID

        # Morsel configuration
        int64_t _morsel_size_kb          # Morsel size

        # Partitioner instance
        Partitioner _partitioner        # HashPartitioner

    cpdef bint requires_shuffle(self)
    cpdef list get_partition_keys(self)
    cpdef int32_t get_num_partitions(self)
    cpdef void set_shuffle_config(self, object transport, bytes shuffle_id,
                                  int32_t task_id, int32_t num_partitions)

    cpdef list _partition_batch(self, ca.RecordBatch batch)
    cdef void _send_partitions(self, list partitions, list agent_addresses)
    cdef object _receive_shuffled_batches(self)
    cdef object _send_shuffled_batches(self)
