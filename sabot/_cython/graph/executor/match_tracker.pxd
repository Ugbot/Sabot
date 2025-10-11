# cython: language_level=3
"""
Match Tracker Header

Efficient tracking of emitted pattern matches for deduplication in continuous queries.
"""

from libc.stdint cimport int64_t, uint64_t, int32_t
from libcpp.unordered_set cimport unordered_set
from libcpp.vector cimport vector
from libcpp cimport bool as cbool

cimport pyarrow.lib as ca


cdef class MatchTracker:
    """
    Track emitted pattern matches for incremental continuous queries.

    Uses bloom filter + exact match set for space-efficient deduplication.
    """

    # Bloom filter (bit array)
    cdef vector[cbool] bloom_filter
    cdef int32_t bloom_size
    cdef int32_t num_hash_functions

    # Exact match tracking (LRU-like eviction)
    cdef unordered_set[uint64_t] exact_matches
    cdef int64_t max_exact_matches

    # Statistics
    cdef int64_t total_checked
    cdef int64_t bloom_hits
    cdef int64_t exact_hits
    cdef int64_t new_matches

    # Methods
    cpdef cbool is_new_match(self, list vertex_ids) except *
    cpdef void add_match(self, list vertex_ids) except *
    cpdef uint64_t compute_match_signature(self, list vertex_ids)
    cpdef void reset(self) except *
    cpdef dict get_stats(self)

    # Private methods
    cdef cbool _bloom_check(self, uint64_t signature) nogil
    cdef void _bloom_add(self, uint64_t signature) nogil
    cdef uint64_t _hash(self, uint64_t signature, int32_t seed) nogil
