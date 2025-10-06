# cython: language_level=3
"""
Lock-Free SPSC Ring Buffer - Type Definitions

Single Producer Single Consumer ring buffer using C++ atomics.
Zero mutex usage, wait-free operations for high-throughput shuffle transport.

Performance targets:
- Push/Pop: <100ns per operation
- Throughput: 10M+ ops/sec per core
- Zero contention between producer and consumer
"""

from libc.stdint cimport int32_t, int64_t, uint64_t
from libcpp cimport bool as cbool
from libcpp.atomic cimport atomic, memory_order
from libcpp.memory cimport shared_ptr
from pyarrow.includes.libarrow cimport CRecordBatch as PCRecordBatch


# ============================================================================
# Partition Slot Structure
# ============================================================================

cdef struct PartitionSlot:
    """
    Single slot in ring buffer holding shuffle partition metadata + batch.

    Uses versioned slots to avoid ABA problem in lock-free operations.
    Cache-line aligned to prevent false sharing.
    """
    int64_t shuffle_id_hash      # Hash of shuffle ID string
    int32_t partition_id          # Partition ID
    int32_t _padding              # Align to 8-byte boundary
    shared_ptr[PCRecordBatch] batch  # Zero-copy batch reference
    uint64_t version              # Slot version (incremented on reuse)


# ============================================================================
# SPSC Lock-Free Ring Buffer
# ============================================================================

cdef class SPSCRingBuffer:
    """
    Single Producer Single Consumer lock-free ring buffer.

    Uses atomic head/tail indices with acquire/release memory ordering.
    Producer writes to tail, consumer reads from head.

    Memory layout:
    - head/tail on separate cache lines (64 bytes apart)
    - Slots array aligned to cache line boundary

    Performance:
    - Push: O(1), wait-free
    - Pop: O(1), wait-free
    - No locks, no CAS, pure load/store atomics
    """
    cdef:
        PartitionSlot* slots          # Ring buffer slots
        int64_t capacity              # Capacity (power of 2)
        int64_t mask                  # Capacity - 1 (for fast modulo)

        # Producer-side (aligned to cache line)
        atomic[int64_t] tail          # Write index (producer)
        char _padding1[56]            # Cache line padding (64 - 8 bytes)

        # Consumer-side (separate cache line)
        atomic[int64_t] head          # Read index (consumer)
        char _padding2[56]            # Cache line padding

        cbool _initialized

    cdef cbool init(self, int64_t capacity) except False
    cdef void destroy(self) nogil

    # Producer API (nogil)
    cdef cbool push(
        self,
        int64_t shuffle_id_hash,
        int32_t partition_id,
        shared_ptr[PCRecordBatch] batch
    ) nogil

    # Consumer API (nogil)
    cdef cbool pop(
        self,
        int64_t* shuffle_id_hash,
        int32_t* partition_id,
        shared_ptr[PCRecordBatch]* batch
    ) nogil

    # Query API
    cdef int64_t size(self) nogil
    cdef cbool is_empty(self) nogil
    cdef cbool is_full(self) nogil


# ============================================================================
# MPSC Lock-Free Queue (Multi-Producer Single Consumer)
# ============================================================================

cdef class MPSCQueue:
    """
    Multi-Producer Single Consumer lock-free queue.

    Uses CAS (Compare-And-Swap) for producers, lock-free pop for consumer.
    Useful for aggregating partitions from multiple shuffle operators.

    Performance:
    - Push: O(1) with CAS retry
    - Pop: O(1), wait-free
    - Producer contention handled via exponential backoff
    """
    cdef:
        PartitionSlot* slots
        int64_t capacity
        int64_t mask

        # Shared tail for all producers
        atomic[int64_t] tail
        char _padding1[56]

        # Consumer-only head
        atomic[int64_t] head
        char _padding2[56]

        cbool _initialized

    cdef cbool init(self, int64_t capacity) except False
    cdef void destroy(self) nogil

    # Multi-producer push (uses CAS)
    cdef cbool push_mpsc(
        self,
        int64_t shuffle_id_hash,
        int32_t partition_id,
        shared_ptr[PCRecordBatch] batch
    ) nogil

    # Single consumer pop
    cdef cbool pop_mpsc(
        self,
        int64_t* shuffle_id_hash,
        int32_t* partition_id,
        shared_ptr[PCRecordBatch]* batch
    ) nogil

    cdef int64_t size(self) nogil
    cdef cbool is_empty(self) nogil
