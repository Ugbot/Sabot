# -*- coding: utf-8 -*-
"""
Time Service Cython Header

Header declarations for TimeService.
"""

from libc.stdint cimport int64_t, int32_t

from .watermark_tracker cimport WatermarkTracker
from .timers cimport TimerService


cdef class TimeService:
    cdef:
        WatermarkTracker watermark_tracker
        TimerService timer_service
        int32_t num_partitions
        int64_t time_characteristic
        object state_backend

    # Time characteristic
    cpdef void set_time_characteristic(self, str characteristic)
    cpdef str get_time_characteristic(self)

    # Watermark operations
    cpdef int64_t update_watermark(self, int32_t partition, int64_t watermark)
    cpdef int64_t get_current_watermark(self)
    cpdef bint has_watermark_updated(self)
    cpdef bint is_late_event(self, int64_t event_timestamp)
    cpdef object get_partition_watermarks(self)

    # Timer operations
    cpdef void register_event_timer(self, int64_t timestamp, object callback)
    cpdef void register_processing_timer(self, int64_t timestamp, object callback)
    cpdef void register_timer(self, int64_t timestamp, object callback)
    cpdef void update_processing_time(self, int64_t current_time)
    cpdef object process_timers(self)
    cpdef object get_active_timers(self)
    cpdef void clear_all_timers(self)

    # Time operations
    cpdef int64_t get_current_time(self)

    # State management
    cpdef void persist_state(self)
    cpdef void restore_state(self)

    # Statistics
    cpdef object get_stats(self)

    # Event processing
    cpdef void process_event(self, int32_t partition, int64_t event_timestamp)
