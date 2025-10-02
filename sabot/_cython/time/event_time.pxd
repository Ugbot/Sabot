# -*- coding: utf-8 -*-
"""
Event Time Service Cython Header

Header declarations for EventTimeService.
"""

from libc.stdint cimport int64_t, int32_t


cdef class EventTimeService:
    cdef:
        object time_service
        int64_t watermark_interval_ms
        int64_t max_lateness_ms
        int64_t last_watermark_emit
        object late_event_handler
        bint enable_idleness_detection
        int64_t* last_event_times
        int32_t idle_timeout_ms

    # Internal methods
    cdef void _handle_late_event(self, int32_t partition, int64_t event_timestamp, object event_data)

    # Configuration
    cpdef void set_late_event_handler(self, object handler)

    # Watermark generation
    cpdef int64_t generate_watermark(self, int32_t partition, int64_t event_timestamp)

    # Event processing
    cpdef void process_event(self, int32_t partition, int64_t event_timestamp, object event_data=?)

    # Idleness detection
    cpdef bint is_partition_idle(self, int32_t partition)
    cpdef object get_idle_partitions(self)
    cpdef void advance_watermark_for_idle_partitions(self)

    # Time operations
    cpdef int64_t get_event_time(self)

    # Timer operations
    cpdef void register_event_timer(self, int64_t timestamp, object callback)
    cpdef void register_processing_timer(self, int64_t timestamp, object callback)

    # Delegate methods
    cpdef int64_t get_current_watermark(self)
    cpdef bint has_watermark_updated(self)
    cpdef object get_partition_watermarks(self)
    cpdef object get_active_timers(self)
    cpdef void clear_all_timers(self)
    cpdef object get_stats(self)

    # State management
    cpdef void persist_state(self)
    cpdef void restore_state(self)
