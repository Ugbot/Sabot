# -*- coding: utf-8 -*-
"""
Event Time Service Implementation

Specialized service for Flink-compatible event-time processing.
Handles watermark generation, event-time timers, and late event management.
"""

from libc.stdint cimport int64_t, int32_t, uint64_t
from libc.limits cimport LLONG_MAX, LLONG_MIN
from cpython.mem cimport PyMem_Malloc, PyMem_Free

cimport cython

from .time_service cimport TimeService


cdef class EventTimeService:
    """
    Specialized event-time service for Flink-compatible processing.

    Provides advanced event-time features:
    - Watermark generation strategies (periodic, punctuated)
    - Late event handling and side outputs
    - Event-time timer management
    - Idleness detection

    This extends TimeService with event-time specific functionality.
    """
    # Attributes declared in .pxd file

    def __cinit__(self, int32_t num_partitions, object state_backend,
                  int64_t watermark_interval_ms=1000, int64_t max_lateness_ms=0,
                  bint enable_idleness_detection=True, int64_t idle_timeout_ms=60000):
        """Initialize event-time service."""
        # Initialize base time service with event-time characteristic
        self.time_service = TimeService(num_partitions, state_backend, "event-time")

        self.watermark_interval_ms = watermark_interval_ms
        self.max_lateness_ms = max_lateness_ms
        self.last_watermark_emit = 0
        self.late_event_handler = None
        self.enable_idleness_detection = enable_idleness_detection
        self.idle_timeout_ms = idle_timeout_ms

        # Allocate array for tracking last event times per partition
        self.last_event_times = <int64_t*>PyMem_Malloc(num_partitions * sizeof(int64_t))
        if not self.last_event_times:
            raise MemoryError("Failed to allocate last event times array")

        # Initialize to minimum (no events seen)
        cdef int32_t i
        for i in range(num_partitions):
            self.last_event_times[i] = LLONG_MIN

    def __dealloc__(self):
        """Clean up allocated memory."""
        if self.last_event_times:
            PyMem_Free(self.last_event_times)

    cpdef void set_late_event_handler(self, object handler):
        """
        Set handler for late events.

        Handler should be a callable that accepts (partition, event_timestamp, event_data).
        """
        self.late_event_handler = handler

    cpdef int64_t generate_watermark(self, int32_t partition, int64_t event_timestamp):
        """
        Generate watermark for a partition based on event timestamp.

        Uses periodic watermark generation strategy.
        Returns the new global watermark.
        """
        cdef int64_t current_time
        cdef int64_t watermark_to_emit

        # Update last event time for this partition
        if 0 <= partition < self.time_service.num_partitions:
            self.last_event_times[partition] = event_timestamp

        # Compute watermark for this partition
        # For periodic watermarks, use: max_event_time - out_of_orderness_bound
        # Simplified: use event_timestamp as watermark (no out-of-orderness bound)
        watermark_to_emit = event_timestamp

        # Update watermark in time service
        cdef int64_t global_watermark = self.time_service.update_watermark(partition, watermark_to_emit)

        # Check if we should emit a watermark update
        import time
        current_time = int(time.time() * 1000)

        if current_time - self.last_watermark_emit >= self.watermark_interval_ms:
            self.last_watermark_emit = current_time
            # Watermark is automatically tracked in time_service

        return global_watermark

    cpdef void process_event(self, int32_t partition, int64_t event_timestamp, object event_data=None):
        """
        Process an event with event-time semantics.

        - Updates watermarks
        - Handles late events
        - Triggers timer processing
        - Updates idleness tracking
        """
        cdef int64_t current_watermark = self.time_service.get_current_watermark()
        cdef bint is_late = self.time_service.is_late_event(event_timestamp)

        if is_late:
            # Handle late event
            self._handle_late_event(partition, event_timestamp, event_data)
        else:
            # Process normal event
            self.generate_watermark(partition, event_timestamp)

            # Process any timers that may have been triggered
            self.time_service.process_timers()

    cdef void _handle_late_event(self, int32_t partition, int64_t event_timestamp, object event_data):
        """Handle a late event."""
        if self.late_event_handler and callable(self.late_event_handler):
            try:
                self.late_event_handler(partition, event_timestamp, event_data)
            except Exception as e:
                # Log but don't crash on handler errors
                print(f"Late event handler failed: {e}")

    cpdef bint is_partition_idle(self, int32_t partition):
        """
        Check if a partition is idle (no events for too long).

        Used for watermark advancement when partitions become idle.
        """
        if not self.enable_idleness_detection:
            return False

        if not (0 <= partition < self.time_service.num_partitions):
            return False

        cdef int64_t last_event_time = self.last_event_times[partition]
        if last_event_time == LLONG_MIN:
            return True  # Never seen events

        import time
        cdef int64_t current_time = int(time.time() * 1000)
        cdef int64_t time_since_last_event = current_time - last_event_time

        return time_since_last_event > self.idle_timeout_ms

    cpdef object get_idle_partitions(self):
        """Get list of currently idle partitions."""
        cdef list idle_partitions = []
        cdef int32_t i

        for i in range(self.time_service.num_partitions):
            if self.is_partition_idle(i):
                idle_partitions.append(i)

        return idle_partitions

    cpdef void advance_watermark_for_idle_partitions(self):
        """
        Advance watermarks for idle partitions.

        When partitions are idle, we can advance their watermarks to allow
        progress in event-time processing.
        """
        cdef object idle_partitions = self.get_idle_partitions()
        cdef int64_t current_watermark = self.time_service.get_current_watermark()

        # Advance idle partitions to current global watermark
        for partition in idle_partitions:
            self.time_service.update_watermark(partition, current_watermark)

    cpdef int64_t get_event_time(self):
        """Get current event time (same as watermark)."""
        return self.time_service.get_current_watermark()

    cpdef void register_event_timer(self, int64_t timestamp, object callback):
        """Register an event-time timer."""
        self.time_service.register_event_timer(timestamp, callback)

    cpdef void register_processing_timer(self, int64_t timestamp, object callback):
        """Register a processing-time timer (not recommended for event-time processing)."""
        self.time_service.register_processing_timer(timestamp, callback)

    # Delegate other methods to time_service

    cpdef int64_t get_current_watermark(self):
        """Get current watermark."""
        return self.time_service.get_current_watermark()

    cpdef bint has_watermark_updated(self):
        """Check if watermark was updated."""
        return self.time_service.has_watermark_updated()

    cpdef object get_partition_watermarks(self):
        """Get partition watermarks."""
        return self.time_service.get_partition_watermarks()

    cpdef object get_active_timers(self):
        """Get active timers."""
        return self.time_service.get_active_timers()

    cpdef void clear_all_timers(self):
        """Clear all timers."""
        self.time_service.clear_all_timers()

    cpdef object get_stats(self):
        """Get comprehensive statistics."""
        base_stats = self.time_service.get_stats()

        # Add event-time specific stats
        event_stats = {
            'watermark_interval_ms': self.watermark_interval_ms,
            'max_lateness_ms': self.max_lateness_ms,
            'idle_partitions': self.get_idle_partitions(),
            'enable_idleness_detection': self.enable_idleness_detection,
            'idle_timeout_ms': self.idle_timeout_ms,
            'has_late_event_handler': self.late_event_handler is not None,
        }

        base_stats.update(event_stats)
        return base_stats

    # State management

    cpdef void persist_state(self):
        """Persist event-time service state."""
        self.time_service.persist_state()

    cpdef void restore_state(self):
        """Restore event-time service state."""
        self.time_service.restore_state()

    # Python special methods

    def __str__(self):
        """String representation."""
        watermark = self.get_current_watermark()
        idle_count = len(self.get_idle_partitions())
        return f"EventTimeService(watermark={watermark}, idle_partitions={idle_count})"

    def __repr__(self):
        """Detailed representation."""
        stats = self.get_stats()
        return (f"EventTimeService(watermark={stats['current_watermark']}, "
                f"partitions={stats['num_partitions']}, "
                f"idle={len(stats['idle_partitions'])}, "
                f"timers={stats['timers']['active_timers']})")

    # Context manager support

    def __enter__(self):
        """Enter context."""
        self.restore_state()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context."""
        self.persist_state()
