# -*- coding: utf-8 -*-
"""
Timer Service Cython Header

Header declarations for TimerService.
"""

from libc.stdint cimport int64_t, int32_t


cdef class TimerService:
    cdef:
        object state_backend
        object watermark_tracker
        bytes timer_prefix
        int64_t current_time
        object event_loop

    # Internal methods
    cdef void _register_timer(self, int64_t timestamp, int32_t timer_type, object callback_data)
    cdef void _remove_timer(self, int64_t timestamp, int32_t timer_type, object callback_data)

    # Core timer operations
    cpdef void set_watermark_tracker(self, object tracker)
    cpdef void update_processing_time(self, int64_t current_time)
    cpdef void register_event_timer(self, int64_t timestamp, object callback_data)
    cpdef void register_processing_timer(self, int64_t timestamp, object callback_data)
    cpdef object get_expired_timers(self, int64_t watermark=?)
    cpdef void fire_expired_timers(self, int64_t watermark=?)
    cpdef object get_active_timers(self)
    cpdef void clear_all_timers(self)
    cpdef int64_t get_next_timer_timestamp(self)
    cpdef object get_timer_stats(self)
