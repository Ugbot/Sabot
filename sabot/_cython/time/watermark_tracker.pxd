# -*- coding: utf-8 -*-
"""
Watermark Tracker Cython Header

Header declarations for WatermarkTracker.
"""

from libc.stdint cimport int64_t, int32_t


cdef class WatermarkTracker:
    cdef:
        int32_t num_partitions
        int64_t* partition_watermarks
        int64_t current_watermark
        bint has_updated
        object state_backend
        bytes watermark_key

    # Internal methods
    cdef void _recompute_global_watermark(self)

    # Core operations
    cpdef int64_t update_watermark(self, int32_t partition, int64_t watermark)
    cpdef int64_t get_current_watermark(self)
    cpdef bint has_watermark_updated(self)
    cpdef bint is_late_event(self, int64_t event_timestamp)
    cpdef object get_partition_watermarks(self)
    cpdef void reset_partition(self, int32_t partition)
    cpdef void persist_state(self)
    cpdef void restore_state(self)
