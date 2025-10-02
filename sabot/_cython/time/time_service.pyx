# -*- coding: utf-8 -*-
"""
Time Service Implementation

Unified time service interface for Flink-compatible event-time processing.
Combines watermark tracking and timer services into a coherent API.
"""

from libc.stdint cimport int64_t, int32_t
from libc.limits cimport LLONG_MAX

cimport cython

from .watermark_tracker cimport WatermarkTracker
from .timers cimport TimerService


cdef class TimeService:
    """
    Unified time service for Flink-compatible event-time processing.

    Provides a high-level interface that combines:
    - Watermark tracking across partitions
    - Timer registration and firing
    - Time characteristic management

    This is the main API that applications use for time-related operations.
    """
    # Attributes declared in .pxd file

    def __cinit__(self, int32_t num_partitions, object state_backend, time_characteristic="event-time"):
        """Initialize time service."""
        self.num_partitions = num_partitions
        self.state_backend = state_backend

        # Set time characteristic
        if time_characteristic == "event-time":
            self.time_characteristic = 1
        elif time_characteristic == "processing-time":
            self.time_characteristic = 0
        else:
            raise ValueError(f"Invalid time characteristic: {time_characteristic}")

        # Initialize components
        self.watermark_tracker = WatermarkTracker(num_partitions, state_backend)
        self.timer_service = TimerService(state_backend, self.watermark_tracker)

        # Restore state if available
        self.watermark_tracker.restore_state()

    cpdef void set_time_characteristic(self, str characteristic):
        """Set the time characteristic (processing-time or event-time)."""
        if characteristic == "event-time":
            self.time_characteristic = 1
        elif characteristic == "processing-time":
            self.time_characteristic = 0
        else:
            raise ValueError(f"Invalid time characteristic: {characteristic}")

    cpdef str get_time_characteristic(self):
        """Get the current time characteristic."""
        if self.time_characteristic == 1:
            return "event-time"
        else:
            return "processing-time"

    # Watermark operations

    cpdef int64_t update_watermark(self, int32_t partition, int64_t watermark):
        """
        Update watermark for a partition.

        Returns the new global watermark (minimum across all partitions).
        """
        return self.watermark_tracker.update_watermark(partition, watermark)

    cpdef int64_t get_current_watermark(self):
        """Get the current global watermark."""
        return self.watermark_tracker.get_current_watermark()

    cpdef bint has_watermark_updated(self):
        """Check if watermark was updated since last check."""
        return self.watermark_tracker.has_watermark_updated()

    cpdef bint is_late_event(self, int64_t event_timestamp):
        """Check if an event is late based on current watermark."""
        return self.watermark_tracker.is_late_event(event_timestamp)

    cpdef object get_partition_watermarks(self):
        """Get watermarks for all partitions."""
        return self.watermark_tracker.get_partition_watermarks()

    # Timer operations

    cpdef void register_event_timer(self, int64_t timestamp, object callback):
        """
        Register an event-time timer.

        The timer will fire when the watermark advances past the timestamp.
        """
        self.timer_service.register_event_timer(timestamp, callback)

    cpdef void register_processing_timer(self, int64_t timestamp, object callback):
        """
        Register a processing-time timer.

        The timer will fire when wall-clock time reaches the timestamp.
        """
        self.timer_service.register_processing_timer(timestamp, callback)

    cpdef void register_timer(self, int64_t timestamp, object callback):
        """
        Register a timer based on current time characteristic.

        Uses event-time or processing-time based on configuration.
        """
        if self.time_characteristic == 1:  # event-time
            self.register_event_timer(timestamp, callback)
        else:  # processing-time
            self.register_processing_timer(timestamp, callback)

    cpdef void update_processing_time(self, int64_t current_time):
        """Update current processing time for processing-time timers."""
        self.timer_service.update_processing_time(current_time)

    cpdef object process_timers(self):
        """
        Process and fire all expired timers.

        Returns list of fired timers for monitoring/debugging.
        """
        cdef int64_t watermark = self.get_current_watermark()
        cdef object expired_timers = self.timer_service.get_expired_timers(watermark)

        # Fire the timers
        self.timer_service.fire_expired_timers(watermark)

        return expired_timers

    cpdef object get_active_timers(self):
        """Get all currently active timers."""
        return self.timer_service.get_active_timers()

    cpdef void clear_all_timers(self):
        """Clear all timers."""
        self.timer_service.clear_all_timers()

    # Time-based operations

    cpdef int64_t get_current_time(self):
        """
        Get current time based on time characteristic.

        For event-time: returns current watermark
        For processing-time: returns current processing time
        """
        if self.time_characteristic == 1:  # event-time
            return self.get_current_watermark()
        else:  # processing-time
            # This would need to be updated externally
            # For now, return a reasonable default
            import time
            return int(time.time() * 1000)

    # State management

    cpdef void persist_state(self):
        """Persist time service state."""
        self.watermark_tracker.persist_state()

    cpdef void restore_state(self):
        """Restore time service state."""
        self.watermark_tracker.restore_state()

    # Statistics and monitoring

    cpdef object get_stats(self):
        """Get comprehensive time service statistics."""
        timer_stats = self.timer_service.get_timer_stats()

        return {
            'time_characteristic': self.get_time_characteristic(),
            'num_partitions': self.num_partitions,
            'current_watermark': self.get_current_watermark(),
            'partition_watermarks': self.get_partition_watermarks(),
            'timers': timer_stats,
            'current_time': self.get_current_time(),
        }

    # Event processing integration

    cpdef void process_event(self, int32_t partition, int64_t event_timestamp):
        """
        Process an event and update time tracking.

        This is typically called for each event in a stream.
        Updates watermarks and triggers timer processing.
        """
        # Update watermark based on event timestamp
        # In a real implementation, watermark would be computed from event timestamps
        # For now, use event timestamp as watermark (simplified)
        self.update_watermark(partition, event_timestamp)

        # Process any expired timers
        self.process_timers()

    # Python special methods

    def __str__(self):
        """String representation."""
        watermark = self.get_current_watermark()
        time_char = self.get_time_characteristic()
        return f"TimeService({time_char}, watermark={watermark})"

    def __repr__(self):
        """Detailed representation."""
        stats = self.get_stats()
        return (f"TimeService(time_characteristic={stats['time_characteristic']}, "
                f"partitions={stats['num_partitions']}, "
                f"watermark={stats['current_watermark']}, "
                f"active_timers={stats['timers']['active_timers']})")

    # Context manager support

    def __enter__(self):
        """Enter context - restore state."""
        self.restore_state()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context - persist state."""
        self.persist_state()
