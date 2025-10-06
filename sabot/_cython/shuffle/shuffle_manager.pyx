# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
Shuffle Manager Implementation - Coordinates all shuffle operations.

Zero-copy implementation using Sabot's direct Arrow C++ bindings.
Integrates partitioning, buffering, and transport for complete shuffle system.

NOTE: Can be extended to use Tonbo for shuffle materialization/spill.
"""

from libc.stdint cimport int32_t, int64_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.memory cimport shared_ptr
from libcpp.unordered_map cimport unordered_map

import uuid

# Import Cython Arrow types
cimport pyarrow.lib as pa
from pyarrow.includes.libarrow cimport (
    CRecordBatch as PCRecordBatch,
    CSchema as PCSchema,
)

# For Python API access when needed
import pyarrow

from .types cimport ShuffleMetadata, PartitionInfo, ShuffleEdgeType
from .partitioner cimport Partitioner, create_partitioner
from .shuffle_buffer cimport ShuffleBuffer, NetworkBufferPool
from .shuffle_transport cimport ShuffleTransport


# ============================================================================
# Helper Functions
# ============================================================================

cdef inline pa.RecordBatch _wrap_batch(shared_ptr[PCRecordBatch] batch_cpp):
    """Wrap C++ RecordBatch as Cython pa.RecordBatch (zero-copy)."""
    cdef pa.RecordBatch result = pa.RecordBatch.__new__(pa.RecordBatch)
    result.init(batch_cpp)
    return result


cdef inline shared_ptr[PCRecordBatch] _unwrap_batch(pa.RecordBatch batch):
    """Unwrap Cython pa.RecordBatch to C++ shared_ptr (zero-copy)."""
    return batch.sp_batch


# ============================================================================
# ShuffleManager
# ============================================================================

cdef class ShuffleManager:
    """
    Coordinates all shuffle operations for an agent.
    """

    def __cinit__(self, agent_id, agent_host=b"0.0.0.0", int32_t agent_port=8816):
        """
        Initialize shuffle manager.

        Args:
            agent_id: Unique agent identifier
            agent_host: Agent host for shuffle server
            agent_port: Agent port for shuffle server
        """
        self.agent_id = agent_id if isinstance(agent_id, bytes) else agent_id.encode('utf-8')
        self.agent_host = agent_host if isinstance(agent_host, bytes) else agent_host.encode('utf-8')
        self.agent_port = agent_port

        # Initialize components
        self._partitioners = {}
        self._partition_buffers = {}
        self._task_locations = {}

        # Create transport layer
        self.transport = ShuffleTransport(agent_host, agent_port)

        # Create buffer pool (1024 buffers, 32KB each = 32MB total)
        self.buffer_pool = NetworkBufferPool(
            buffer_size=32768,
            total_buffers=1024,
            exclusive_per_partition=2,
            floating_per_shuffle=8
        )

    cpdef bytes register_shuffle(
        self,
        bytes operator_id,
        int32_t num_partitions,
        vector[string] partition_keys,
        ShuffleEdgeType edge_type,
        pa.Schema schema
    ) except *:
        """
        Register a new shuffle operation.

        Args:
            operator_id: Logical operator ID
            num_partitions: Number of output partitions
            partition_keys: Column names to partition by
            edge_type: Type of shuffle (HASH, RANGE, REBALANCE)
            schema: Arrow schema (Cython pa.Schema)

        Returns:
            Shuffle ID
        """
        # Generate shuffle ID
        shuffle_id = f"{operator_id.decode('utf-8')}_{str(uuid.uuid4())[:8]}".encode('utf-8')

        # Create partitioner
        partitioner = create_partitioner(edge_type, num_partitions, partition_keys, schema)
        self._partitioners[shuffle_id] = partitioner

        # Create buffers for each partition
        buffers = {}
        for partition_id in range(num_partitions):
            buffer = ShuffleBuffer(
                partition_id,
                max_rows=10000,  # 10K rows per buffer
                max_bytes=1024*1024*32,  # 32MB per buffer
                schema=schema
            )
            buffers[partition_id] = buffer
        self._partition_buffers[shuffle_id] = buffers

        # Initialize task location tracking
        self._task_locations[shuffle_id] = {}

        return shuffle_id

    cdef void write_partition(
        self,
        bytes shuffle_id,
        int32_t partition_id,
        pa.RecordBatch batch
    ):
        """
        Write batch to partition (with partitioning and buffering).

        Args:
            shuffle_id: Shuffle identifier
            partition_id: Target partition ID
            batch: Batch to write (Cython pa.RecordBatch)
        """
        # Get partitioner
        cdef Partitioner partitioner = self._partitioners.get(shuffle_id)
        if partitioner is None:
            raise ValueError(f"Shuffle {shuffle_id} not registered")

        # Partition batch
        cdef vector[shared_ptr[PCRecordBatch]] partitions = partitioner.partition_batch(batch)

        # Buffer each partition
        buffers = self._partition_buffers[shuffle_id]
        cdef int i
        for i in range(<int>partitions.size()):
            if i < len(buffers):
                buffer = buffers[i]
                partition_batch = _wrap_batch(partitions[i])
                buffer.add_batch(partition_batch)

                # Flush if needed
                if buffer.should_flush():
                    merged = buffer.get_merged_batch()
                    self.transport.publish_partition(shuffle_id, i, merged)
                    buffer.clear()

    cdef pa.RecordBatch read_partition(
        self,
        bytes shuffle_id,
        int32_t partition_id,
        list upstream_agents
    ):
        """
        Read partition data from upstream agents.

        Args:
            shuffle_id: Shuffle identifier
            partition_id: Partition ID to read
            upstream_agents: List of upstream agent addresses

        Returns:
            Merged batch from all upstream agents (Cython pa.RecordBatch)
        """
        cdef:
            list batches = []
            object batch_obj
            pa.RecordBatch result

        # Fetch from each upstream agent
        for agent_address in upstream_agents:
            agent_bytes = agent_address if isinstance(agent_address, bytes) else agent_address.encode('utf-8')
            batch = self.transport.fetch_partition_from_agent(
                agent_bytes,
                shuffle_id,
                partition_id
            )
            batches.append(batch)

        # Merge batches (using Python API)
        if len(batches) == 0:
            # Return empty batch
            result_obj = pyarrow.RecordBatch.from_arrays([], names=[])
            result = result_obj
            return result

        if len(batches) == 1:
            return batches[0]

        # Concatenate multiple batches
        table_obj = pyarrow.Table.from_batches(batches)
        result_obj = table_obj.to_batches()[0] if table_obj.num_rows > 0 else batches[0]
        result = result_obj
        return result

    cpdef void set_task_location(
        self,
        bytes shuffle_id,
        int32_t task_id,
        bytes agent_address
    ) except *:
        """
        Set location of upstream task for data fetching.

        Args:
            shuffle_id: Shuffle identifier
            task_id: Task ID
            agent_address: Agent address (host:port)
        """
        if shuffle_id not in self._task_locations:
            self._task_locations[shuffle_id] = {}
        self._task_locations[shuffle_id][task_id] = agent_address

    cpdef void start(self) except *:
        """Start shuffle manager (start transport)."""
        self.transport.start()

    cpdef void stop(self) except *:
        """Stop shuffle manager (stop transport)."""
        self.transport.stop()
