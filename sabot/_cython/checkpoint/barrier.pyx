# -*- coding: utf-8 -*-
"""
Barrier Data Structure

Represents checkpoint barriers in Flink-style exactly-once processing.
Barriers flow through the streaming topology to coordinate snapshots.
"""

from libc.stdint cimport int64_t, int32_t

cimport cython


@cython.final
cdef class Barrier:
    """
    Checkpoint barrier data structure.

    Barriers flow through the streaming topology to coordinate distributed snapshots.
    Each barrier carries checkpoint metadata and ensures exactly-once semantics.

    This is the core data structure for Chandy-Lamport algorithm implementation.

    Note: All cdef attributes are declared in barrier.pxd
    """

    def __cinit__(self, int64_t checkpoint_id, int32_t source_id,
                  bint is_cancellation_barrier=False):
        """Initialize barrier."""
        self.checkpoint_id = checkpoint_id
        self.source_id = source_id
        self.timestamp = self._get_timestamp_ns()
        self.is_cancellation_barrier = is_cancellation_barrier
        self.checkpoint_metadata = {}

    cdef int64_t _get_timestamp_ns(self):
        """Get current timestamp in nanoseconds."""
        cdef timespec ts
        clock_gettime(CLOCK_REALTIME, &ts)
        return ts.tv_sec * 1000000000 + ts.tv_nsec

    cpdef int64_t get_checkpoint_id(self):
        """Get checkpoint ID."""
        return self.checkpoint_id

    cpdef int64_t get_timestamp(self):
        """Get barrier timestamp."""
        return self.timestamp

    cpdef int32_t get_source_id(self):
        """Get source ID that emitted this barrier."""
        return self.source_id

    cpdef bint is_cancellation(self):
        """Check if this is a cancellation barrier."""
        return self.is_cancellation_barrier

    cpdef void set_metadata(self, str key, object value):
        """Set checkpoint metadata."""
        self.checkpoint_metadata[key] = value

    cpdef object get_metadata(self, str key, object default_value=None):
        """Get checkpoint metadata."""
        return self.checkpoint_metadata.get(key, default_value)

    cpdef object get_all_metadata(self):
        """Get all checkpoint metadata."""
        return self.checkpoint_metadata.copy()

    cpdef bint should_trigger_checkpoint(self):
        """
        Check if this barrier should trigger a checkpoint.

        Regular barriers trigger checkpoints, cancellation barriers cancel them.
        """
        return not self.is_cancellation_barrier

    cpdef bint should_cancel_checkpoint(self):
        """
        Check if this barrier should cancel a checkpoint.

        Cancellation barriers stop ongoing checkpoints.
        """
        return self.is_cancellation_barrier

    # Factory methods for creating barriers
    # Note: These must be regular Python methods, not cpdef, due to @staticmethod

    @staticmethod
    def create_checkpoint_barrier(int64_t checkpoint_id, int32_t source_id):
        """Create a regular checkpoint barrier."""
        return Barrier(checkpoint_id, source_id, False)

    @staticmethod
    def create_cancellation_barrier(int64_t checkpoint_id, int32_t source_id):
        """Create a cancellation barrier."""
        return Barrier(checkpoint_id, source_id, True)

    # Comparison operations for barrier ordering

    cpdef int compare_to(self, Barrier other):
        """
        Compare barriers for ordering.

        Barriers are ordered by checkpoint ID, then by timestamp.
        Returns: -1 (self < other), 0 (equal), 1 (self > other)
        """
        if self.checkpoint_id < other.checkpoint_id:
            return -1
        elif self.checkpoint_id > other.checkpoint_id:
            return 1
        else:
            # Same checkpoint ID - compare timestamps
            if self.timestamp < other.timestamp:
                return -1
            elif self.timestamp > other.timestamp:
                return 1
            else:
                return 0

    cpdef bint is_same_checkpoint(self, Barrier other):
        """Check if barriers belong to same checkpoint."""
        return self.checkpoint_id == other.checkpoint_id

    # Python special methods

    def __str__(self):
        """String representation."""
        barrier_type = "CANCEL" if self.is_cancellation_barrier else "CHECKPOINT"
        return f"Barrier({barrier_type}, id={self.checkpoint_id}, source={self.source_id})"

    def __repr__(self):
        """Detailed representation."""
        barrier_type = "CANCEL" if self.is_cancellation_barrier else "CHECKPOINT"
        return (f"Barrier({barrier_type}, id={self.checkpoint_id}, "
                f"source={self.source_id}, timestamp={self.timestamp}, "
                f"metadata={self.checkpoint_metadata})")

    def __eq__(self, other):
        """Equality comparison."""
        if not isinstance(other, Barrier):
            return False
        return (self.checkpoint_id == other.checkpoint_id and
                self.source_id == other.source_id and
                self.is_cancellation_barrier == other.is_cancellation_barrier)

    def __hash__(self):
        """Hash function for barrier."""
        return hash((self.checkpoint_id, self.source_id, self.is_cancellation_barrier))

    def __lt__(self, other):
        """Less than comparison."""
        if not isinstance(other, Barrier):
            return NotImplemented
        return self.compare_to(other) < 0

    def __le__(self, other):
        """Less than or equal comparison."""
        if not isinstance(other, Barrier):
            return NotImplemented
        return self.compare_to(other) <= 0

    def __gt__(self, other):
        """Greater than comparison."""
        if not isinstance(other, Barrier):
            return NotImplemented
        return self.compare_to(other) > 0

    def __ge__(self, other):
        """Greater than or equal comparison."""
        if not isinstance(other, Barrier):
            return NotImplemented
        return self.compare_to(other) >= 0


# External C declarations
cdef extern from "<time.h>" nogil:
    cdef struct timespec:
        long tv_sec
        long tv_nsec
    int clock_gettime(int clk_id, timespec *tp) nogil

cdef extern from "<sys/time.h>" nogil:
    int CLOCK_REALTIME
