# cython: language_level=3
"""
Atomic Partition Store - Lock-Free Hash Table

LMAX Disruptor-inspired lock-free partition storage.
Uses atomic sequences and memory barriers for coordination.

Performance targets:
- Insert: <200ns
- Lookup: <50ns
- No locks, pure atomic operations
"""

from libc.stdint cimport int32_t, int64_t, uint64_t
from libcpp cimport bool as cbool
from libcpp.atomic cimport atomic, memory_order
from libcpp.memory cimport shared_ptr
from pyarrow.includes.libarrow cimport CRecordBatch as PCRecordBatch


# ============================================================================
# Hash Table Entry
# ============================================================================

cdef struct HashEntry:
    """
    Single hash table entry with atomic versioning.

    Uses sequence numbers (LMAX style) instead of traditional locks.
    """
    atomic[int64_t] sequence        # Entry sequence (odd=writing, even=ready)
    int64_t shuffle_id_hash          # Shuffle ID hash
    int32_t partition_id             # Partition ID
    int32_t _padding
    shared_ptr[PCRecordBatch] batch  # Zero-copy batch reference


# ============================================================================
# Atomic Partition Store (LMAX-style)
# ============================================================================

cdef class AtomicPartitionStore:
    """
    Lock-free hash table using LMAX Disruptor sequence barriers.

    Write protocol:
    1. Claim sequence (atomic increment)
    2. Write data
    3. Publish sequence (release barrier)

    Read protocol:
    1. Load sequence (acquire barrier)
    2. Verify even (committed)
    3. Read data
    4. Verify sequence unchanged

    Performance:
    - Single-writer insert: O(1), wait-free
    - Multi-writer insert: O(1) with CAS retry
    - Lookup: O(1) expected, lock-free
    """
    cdef:
        HashEntry* table              # Hash table entries
        int64_t table_size            # Size (power of 2)
        int64_t mask                  # Size - 1 (for fast modulo)

        # LMAX-style sequence tracking
        atomic[int64_t] cursor        # Current write cursor
        atomic[int64_t] published     # Last published sequence

        # Cache line padding
        char _padding1[64]

        atomic[int64_t] num_entries   # Entry count (approximate)
        char _padding2[64]

        cbool _initialized

    cdef cbool init(self, int64_t size) except False
    cdef void destroy(self) nogil

    # Insert with LMAX claim/publish protocol
    cdef cbool insert(
        self,
        int64_t shuffle_id_hash,
        int32_t partition_id,
        shared_ptr[PCRecordBatch] batch
    ) nogil

    # Lock-free lookup with sequence validation
    cdef shared_ptr[PCRecordBatch] get(
        self,
        int64_t shuffle_id_hash,
        int32_t partition_id
    ) nogil

    # Atomic removal (marks as deleted)
    cdef cbool remove(
        self,
        int64_t shuffle_id_hash,
        int32_t partition_id
    ) nogil

    # Query methods
    cdef int64_t size(self) nogil
    cdef double load_factor(self) nogil


# ============================================================================
# Hash Functions
# ============================================================================

cdef inline int64_t hash_combine(int64_t h1, int32_t h2) nogil:
    """Combine two hash values (similar to boost::hash_combine)."""
    # Use golden ratio constant for good mixing
    return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2))


cdef inline int64_t hash_string(const char* s, int64_t len) nogil:
    """FNV-1a hash for strings (fast, good distribution)."""
    cdef int64_t hash_val = 14695981039346656037  # FNV offset basis
    cdef int64_t i

    for i in range(len):
        hash_val ^= <int64_t>s[i]
        hash_val *= 1099511628211  # FNV prime

    return hash_val
