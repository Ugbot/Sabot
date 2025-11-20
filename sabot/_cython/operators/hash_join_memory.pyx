# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
"""
Pre-allocated memory pools for streaming hash join.

Zero-allocation hot path: All buffers pre-allocated in __init__, reused forever.
"""

from libc.stdint cimport uint8_t, uint16_t, uint32_t, uint64_t, int64_t
from libcpp cimport bool as cbool
from libcpp.memory cimport shared_ptr, make_shared

# Arrow C++ imports
cdef extern from "arrow/api.h" namespace "arrow" nogil:
    cdef cppclass CBuffer "arrow::Buffer":
        uint8_t* data()
        int64_t size()
        cbool is_mutable()

    cdef cppclass CBufferBuilder "arrow::BufferBuilder":
        CBufferBuilder()
        CBufferBuilder(int64_t initial_capacity)
        int64_t capacity()
        void Reset()

    cdef cppclass CMemoryPool "arrow::MemoryPool":
        pass

    CMemoryPool* default_memory_pool()

    cdef cppclass CStatus "arrow::Status":
        cbool ok()
        const char* message()

    cdef cppclass CResult "arrow::Result"[T]:
        T ValueOrDie() except +
        cbool ok()

    CResult[shared_ptr[CBuffer]] AllocateBuffer(int64_t size, CMemoryPool* pool)


# Cache-line size for alignment (prevents false sharing)
DEF CACHE_LINE_SIZE = 64

# Maximum batch size (pre-allocate for this many rows)
DEF MAX_BATCH_SIZE = 65536  # 64K rows (typical morsel size)


# Pre-allocated thread-local buffers for hash join.
# NOTE: Should be cache-line aligned (64 bytes) to prevent false sharing.
# Arrow's AllocateBuffer already provides 64-byte alignment by default.
# All buffers allocated once in __init__, reused forever.
cdef struct PreAllocatedBuffers:
    # Hash values (uint32_t[MAX_BATCH_SIZE])
    shared_ptr[CBuffer] hash_buffer

    # Left table row indices for matches (uint32_t[MAX_BATCH_SIZE])
    shared_ptr[CBuffer] left_indices

    # Right table row indices for matches (uint32_t[MAX_BATCH_SIZE])
    shared_ptr[CBuffer] right_indices

    # Bloom filter bit mask (uint8_t[MAX_BATCH_SIZE/8])
    # Used to quickly filter non-matching rows
    shared_ptr[CBuffer] bloom_mask

    # Match bitvector for SIMD probes (uint8_t[MAX_BATCH_SIZE])
    # 1 = row matches, 0 = no match
    shared_ptr[CBuffer] match_bitvector

    # Number of matches found (returned by probe)
    int num_matches


cdef class HashJoinMemoryPool:
    """
    Thread-local memory pool for hash join operations.

    Pre-allocates all buffers once, reuses them forever.
    Zero allocations in hot path (probe loop).
    """
    cdef:
        PreAllocatedBuffers _buffers
        CMemoryPool* _pool
        int _thread_id
        cbool _initialized

    def __cinit__(self, int thread_id=0):
        """Initialize memory pool with pre-allocated buffers."""
        self._pool = default_memory_pool()
        self._thread_id = thread_id
        self._initialized = False

    def __init__(self, int thread_id=0):
        """Allocate all buffers (called once per thread)."""
        # Allocate hash buffer (uint32_t * MAX_BATCH_SIZE)
        self._buffers.hash_buffer = AllocateBuffer(
            MAX_BATCH_SIZE * sizeof(uint32_t),
            self._pool
        ).ValueOrDie()

        # Allocate left indices buffer
        self._buffers.left_indices = AllocateBuffer(
            MAX_BATCH_SIZE * sizeof(uint32_t),
            self._pool
        ).ValueOrDie()

        # Allocate right indices buffer
        self._buffers.right_indices = AllocateBuffer(
            MAX_BATCH_SIZE * sizeof(uint32_t),
            self._pool
        ).ValueOrDie()

        # Allocate bloom filter mask (1 bit per row, rounded up to bytes)
        self._buffers.bloom_mask = AllocateBuffer(
            (MAX_BATCH_SIZE + 7) // 8,
            self._pool
        ).ValueOrDie()

        # Allocate match bitvector (1 byte per row for SIMD convenience)
        self._buffers.match_bitvector = AllocateBuffer(
            MAX_BATCH_SIZE * sizeof(uint8_t),
            self._pool
        ).ValueOrDie()

        self._buffers.num_matches = 0
        self._initialized = True

    cdef uint32_t* get_hash_buffer(self) nogil:
        """Get mutable pointer to hash buffer."""
        return <uint32_t*>self._buffers.hash_buffer.get().data()

    cdef uint32_t* get_left_indices(self) nogil:
        """Get mutable pointer to left indices buffer."""
        return <uint32_t*>self._buffers.left_indices.get().data()

    cdef uint32_t* get_right_indices(self) nogil:
        """Get mutable pointer to right indices buffer."""
        return <uint32_t*>self._buffers.right_indices.get().data()

    cdef uint8_t* get_bloom_mask(self) nogil:
        """Get mutable pointer to bloom filter mask."""
        return <uint8_t*>self._buffers.bloom_mask.get().data()

    cdef uint8_t* get_match_bitvector(self) nogil:
        """Get mutable pointer to match bitvector."""
        return <uint8_t*>self._buffers.match_bitvector.get().data()

    cdef void reset_match_count(self) nogil:
        """Reset match count (called before each probe)."""
        self._buffers.num_matches = 0

    cdef void set_match_count(self, int count) nogil:
        """Set match count (called after probe completes)."""
        self._buffers.num_matches = count

    cdef int get_match_count(self) nogil:
        """Get number of matches found."""
        return self._buffers.num_matches

    def get_stats(self):
        """Get memory pool statistics (for debugging)."""
        if not self._initialized:
            return {
                'initialized': False,
                'thread_id': self._thread_id,
            }

        return {
            'initialized': True,
            'thread_id': self._thread_id,
            'hash_buffer_size': self._buffers.hash_buffer.get().size(),
            'left_indices_size': self._buffers.left_indices.get().size(),
            'right_indices_size': self._buffers.right_indices.get().size(),
            'bloom_mask_size': self._buffers.bloom_mask.get().size(),
            'match_bitvector_size': self._buffers.match_bitvector.get().size(),
            'total_bytes': (
                self._buffers.hash_buffer.get().size() +
                self._buffers.left_indices.get().size() +
                self._buffers.right_indices.get().size() +
                self._buffers.bloom_mask.get().size() +
                self._buffers.match_bitvector.get().size()
            ),
        }

    def __repr__(self):
        stats = self.get_stats()
        if stats['initialized']:
            return (
                f"<HashJoinMemoryPool thread={stats['thread_id']} "
                f"total_bytes={stats['total_bytes']:,}>"
            )
        else:
            return f"<HashJoinMemoryPool thread={stats['thread_id']} uninitialized>"


# Thread-local storage for memory pools
# Each worker thread gets its own pool to avoid contention
_thread_local_pools = {}


cpdef HashJoinMemoryPool get_thread_local_pool(int thread_id=0):
    """
    Get or create thread-local memory pool.

    Args:
        thread_id: Thread identifier (0 for main thread)

    Returns:
        HashJoinMemoryPool instance for this thread
    """
    global _thread_local_pools

    if thread_id not in _thread_local_pools:
        _thread_local_pools[thread_id] = HashJoinMemoryPool(thread_id)

    return _thread_local_pools[thread_id]


def clear_thread_local_pools():
    """Clear all thread-local pools (for cleanup)."""
    global _thread_local_pools
    _thread_local_pools.clear()
