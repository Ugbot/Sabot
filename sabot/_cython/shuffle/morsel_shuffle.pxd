# cython: language_level=3

from libc.stdint cimport int32_t, int64_t
from libcpp cimport bool as cbool
from libcpp.vector cimport vector

cdef class MorselShuffleManager:
    """Morsel-driven shuffle manager with work-stealing."""

    cdef:
        int32_t _num_partitions
        list _partition_queues  # MPSCQueue instances (use Python list, not C++ vector)
        object _client                    # ShuffleClient
        int32_t _num_workers
        bint _running
        int64_t _morsel_size_kb

    cpdef void start(self)
    cpdef void stop(self)
    cpdef void enqueue_morsel(self, int32_t partition_id, object morsel)
    cpdef object get_stats(self)
