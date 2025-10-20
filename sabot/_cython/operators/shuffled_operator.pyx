# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
ShuffledOperator Base Class

Abstract base for stateful operators requiring network shuffle.
Handles partitioning, shuffle transport, and co-location guarantees.
"""

from libc.stdint cimport int32_t, int64_t
from libcpp cimport bool as cbool
from libcpp.vector cimport vector

cimport pyarrow.lib as ca
from sabot._cython.operators.base_operator cimport BaseOperator
from sabot._cython.shuffle.partitioner cimport HashPartitioner
from libcpp.vector cimport vector
from libcpp.string cimport string
from libcpp.memory cimport shared_ptr
from pyarrow.includes.libarrow cimport CRecordBatch as PCRecordBatch

# Constants
DEF DEFAULT_MORSEL_SIZE_KB = 64


cdef class ShuffledOperator(BaseOperator):
    """
    Base class for operators requiring network shuffle.

    Responsibilities:
    1. Declare shuffle requirement via requires_shuffle()
    2. Specify partition keys via get_partition_keys()
    3. Provide hash partitioning logic
    4. Integrate with ShuffleTransport

    Subclasses:
    - HashJoinOperator (shuffle both sides by join key)
    - GroupByOperator (shuffle by group keys)
    - DistinctOperator (shuffle by all columns)
    - WindowOperator (shuffle by window key + partition by)
    """

    def __cinit__(self, *args, **kwargs):
        """Initialize shuffle operator."""
        # NOTE: Cython automatically calls parent __cinit__ - don't call super().__init__()

        # Store operator_id and parallelism (expected by tests and metadata)
        self.operator_id = kwargs.get('operator_id', 'shuffled-op')
        self.parallelism = kwargs.get('parallelism', 1)

        self._partition_keys = kwargs.get('partition_keys', [])
        self._num_partitions = kwargs.get('num_partitions', 4)
        self._stateful = True

        self._shuffle_transport = None
        self._shuffle_id = None
        self._task_id = -1
        self._morsel_size_kb = DEFAULT_MORSEL_SIZE_KB

        # Initialize partitioner (will be updated when shuffle config is set)
        self._partitioner = None

    # ========================================================================
    # Shuffle Protocol Interface
    # ========================================================================

    cpdef bint requires_shuffle(self):
        """
        Declare that this operator requires shuffle.

        JobManager uses this to insert shuffle edges in execution graph.
        """
        return True

    cpdef list get_partition_keys(self):
        """
        Get columns to partition by for shuffle.

        Returns:
            List of column names for hash partitioning.
        """
        return self._partition_keys

    cpdef int32_t get_num_partitions(self):
        """Get number of downstream partitions."""
        return self._num_partitions

    cpdef void set_shuffle_config(
        self,
        object transport,
        bytes shuffle_id,
        int32_t task_id,
        int32_t num_partitions
    ):
        """
        Configure shuffle transport (called by agent runtime).

        Args:
            transport: ShuffleTransport instance
            shuffle_id: Unique shuffle identifier
            task_id: This task's partition ID
            num_partitions: Total number of partitions
        """
        self._shuffle_transport = transport
        self._shuffle_id = shuffle_id
        self._task_id = task_id
        self._num_partitions = num_partitions

        # Initialize partitioner with proper parameters
        cdef vector[string] key_columns
        for key_col in self._partition_keys:
            key_columns.push_back(key_col.encode('utf-8'))

        # Use the existing schema (assuming it's set)
        self._partitioner = HashPartitioner(
            num_partitions,
            key_columns,
            self._schema  # This needs to be the ca.Schema type
        )

    # ========================================================================
    # Hash Partitioning
    # ========================================================================


    cpdef list _partition_batch(
        self,
        ca.RecordBatch batch
    ):
        """
        Partition batch into N partitions by hash(key).

        Args:
            batch: Input RecordBatch

        Returns:
            List of RecordBatches (one per partition)
        """
        # Use the existing HashPartitioner (returns C++ vector directly)
        cdef vector[shared_ptr[PCRecordBatch]] cpp_partitions
        cpp_partitions = self._partitioner.partition_batch(batch)

        # Convert C++ vector to Python list of Cython RecordBatches
        result = []
        cdef shared_ptr[PCRecordBatch] cpp_batch
        cdef ca.RecordBatch cython_batch

        for i in range(cpp_partitions.size()):
            cpp_batch = cpp_partitions[i]
            if cpp_batch.get() != NULL:
                cython_batch = ca.RecordBatch.__new__(ca.RecordBatch)
                cython_batch.init(cpp_batch)
                result.append(cython_batch)
            else:
                result.append(None)

        return result

    # ========================================================================
    # Shuffle Send (Upstream Task)
    # ========================================================================

    cdef void _send_partitions(
        self,
        list partitions,
        list agent_addresses
    ):
        """
        Send partitions to downstream agents via Arrow Flight.

        Args:
            partitions: List of partitioned batches
            agent_addresses: List of "host:port" for downstream agents
        """
        cdef int32_t partition_id
        cdef ca.RecordBatch partition_batch

        for partition_id in range(len(partitions)):
            partition_batch = partitions[partition_id]

            if partition_batch is None or partition_batch.num_rows == 0:
                continue

            # Get target agent address
            agent_address = agent_addresses[partition_id]

            # Send via shuffle transport
            if self._shuffle_transport is not None:
                self._shuffle_transport.publish_partition(
                    self._shuffle_id,
                    partition_id,
                    partition_batch
                )

    # ========================================================================
    # Shuffle Receive (Downstream Task)
    # ========================================================================

    def __iter__(self):
        """
        Receive shuffled data and process.

        For downstream tasks, this receives data from shuffle transport
        instead of directly from upstream operator.
        """
        # Check if we're a downstream task (shuffle receiver)
        if self._shuffle_transport is not None and self._task_id >= 0:
            # Receive mode: pull data from shuffle transport
            yield from self._receive_shuffled_batches()
        else:
            # Send mode: partition and send upstream data
            yield from self._send_shuffled_batches()

    cdef _receive_shuffled_batches(self):
        """
        Receive shuffled batches for this partition.

        Pulls batches from ShuffleTransport that have been sent to this task.
        """
        # Use ShuffleTransport to receive partitions
        if self._shuffle_transport is not None:
            return self._shuffle_transport.receive_partitions(
                self._shuffle_id, self._task_id
            )
        return []

    cdef _send_shuffled_batches(self):
        """
        Partition upstream batches and send via shuffle.

        Processes upstream operator, partitions output, sends to downstream.
        """
        # Get shuffle metadata
        if self._shuffle_transport is None:
            return

        # TODO: Get downstream agent addresses from shuffle metadata
        # For now, placeholder
        downstream_agents = [b"localhost:8817"] * self._num_partitions

        # Process upstream batches
        for batch in self._source:
            if batch is None or batch.num_rows == 0:
                continue

            # Partition the batch
            partitions = self._partition_batch(batch)

            # Send each partition to its downstream agent
            for i in range(len(partitions)):
                if partitions[i] is not None and partitions[i].num_rows > 0:
                    self._shuffle_transport.send_partition(
                        self._shuffle_id,
                        i,  # partition_id
                        partitions[i],
                        downstream_agents[i]
                    )
