# -*- coding: utf-8 -*-
"""
AggregatingState Implementation

Flink-compatible AggregatingState[IN,OUT] primitive with C-level performance.
Maintains an accumulated value using add/aggregate functions.
"""

from libc.stdint cimport int64_t, uint64_t
from libcpp.string cimport string
from cpython.ref cimport PyObject

import time
import logging

from .state_backend cimport StateBackend

logger = logging.getLogger(__name__)


cdef class AggregatingState:
    """
    Flink-compatible AggregatingState[IN,OUT] primitive.

    Maintains an accumulated value using separate add and aggregate functions.
    Each add() accumulates input values, get() applies final aggregation.
    Performance: <500ns add (in-memory), <2ms get (persistent).
    """

    def __cinit__(self, StateBackend backend, str state_name,
                  object add_function, object aggregate_function,
                  bint ttl_enabled=False, int64_t ttl_ms=0):
        """C-level initialization."""
        self.backend = backend
        self.state_name = state_name.encode('utf-8')
        self.add_function = add_function
        self.aggregate_function = aggregate_function
        self.ttl_enabled = ttl_enabled
        self.ttl_ms = ttl_ms
        self.last_access_time = 0

    cpdef void add(self, object input_value):
        """
        Add an input value to accumulate.

        Uses the configured add function to accumulate input values.
        Performance: <500ns (in-memory), <2ms (persistent)
        """
        cdef double start_time = time.perf_counter() * 1000

        # Check TTL before add
        if self.ttl_enabled and self.is_expired():
            # Clear expired accumulator first
            self.backend.clear_aggregating(self.state_name.decode('utf-8'))

        # Get current accumulator
        cdef object accumulator = self.backend.get_aggregating(
            self.state_name.decode('utf-8'), None
        )

        # Apply add function
        cdef object new_accumulator
        if accumulator is None:
            # First input - initialize accumulator
            new_accumulator = self.add_function(input_value)
        else:
            # Add to existing accumulator
            new_accumulator = self.add_function(input_value, accumulator)

        # Store new accumulator
        self.backend.add_to_aggregating(self.state_name.decode('utf-8'), new_accumulator)
        self.last_access_time = <uint64_t>(time.time() * 1000)

        # Record metrics
        duration = time.perf_counter() * 1000 - start_time
        self.backend._record_operation("aggregating_add", duration)

    cpdef object get(self):
        """
        Get the final aggregated result.

        Applies the aggregate function to the accumulated values.
        Returns None if no values accumulated or TTL expired.
        Performance: <500ns (in-memory), <2ms (persistent)
        """
        cdef double start_time = time.perf_counter() * 1000
        cdef double duration
        cdef object accumulator
        cdef object result

        # Check TTL
        if self.ttl_enabled and self.is_expired():
            # Clear expired accumulator
            self.backend.clear_aggregating(self.state_name.decode('utf-8'))
            self.last_access_time = 0

            duration = time.perf_counter() * 1000 - start_time
            self.backend._record_operation("aggregating_get_expired", duration)
            return None

        # Get current accumulator
        accumulator = self.backend.get_aggregating(
            self.state_name.decode('utf-8'), None
        )

        # Apply aggregate function
        if accumulator is None:
            result = None
        else:
            result = self.aggregate_function(accumulator)

        self.last_access_time = <uint64_t>(time.time() * 1000)

        duration = time.perf_counter() * 1000 - start_time
        self.backend._record_operation("aggregating_get", duration)

        return result

    cpdef void clear(self):
        """
        Clear the accumulated values.

        Performance: <50ns (in-memory), <500μs (persistent)
        """
        cdef double start_time = time.perf_counter() * 1000

        self.backend.clear_aggregating(self.state_name.decode('utf-8'))
        self.last_access_time = 0

        duration = time.perf_counter() * 1000 - start_time
        self.backend._record_operation("aggregating_clear", duration)

    cpdef bint is_empty(self):
        """
        Check if the state is empty.

        Returns True if no accumulator exists or TTL expired.
        Performance: <200ns (in-memory), <1ms (persistent)
        """
        cdef object accumulator = self.backend.get_aggregating(
            self.state_name.decode('utf-8'), None
        )
        return accumulator is None or (self.ttl_enabled and self.is_expired())

    cpdef object get_accumulator(self):
        """
        Get the raw accumulator value (before final aggregation).

        Useful for debugging or custom processing.
        Returns None if no accumulator exists or TTL expired.
        """
        cdef double start_time = time.perf_counter() * 1000
        cdef double duration
        cdef object accumulator

        # Check TTL
        if self.ttl_enabled and self.is_expired():
            # Clear expired accumulator
            self.backend.clear_aggregating(self.state_name.decode('utf-8'))
            self.last_access_time = 0

            duration = time.perf_counter() * 1000 - start_time
            self.backend._record_operation("aggregating_get_accumulator_expired", duration)
            return None

        # Get current accumulator
        accumulator = self.backend.get_aggregating(
            self.state_name.decode('utf-8'), None
        )

        duration = time.perf_counter() * 1000 - start_time
        self.backend._record_operation("aggregating_get_accumulator", duration)

        return accumulator

    cpdef void merge(self, object other_accumulator):
        """
        Merge another accumulator into this state.

        Useful for state migration or combining results.
        Performance: <500ns (in-memory), <2ms (persistent)
        """
        cdef double start_time = time.perf_counter() * 1000

        if other_accumulator is None:
            return

        # Get current accumulator
        cdef object current = self.backend.get_aggregating(
            self.state_name.decode('utf-8'), None
        )

        # Merge accumulators
        cdef object merged
        if current is None:
            merged = other_accumulator
        else:
            # Use add function to merge
            merged = self.add_function(other_accumulator, current)

        # Store merged accumulator
        self.backend.clear_aggregating(self.state_name.decode('utf-8'))
        self.backend.add_to_aggregating(self.state_name.decode('utf-8'), merged)
        self.last_access_time = <uint64_t>(time.time() * 1000)

        duration = time.perf_counter() * 1000 - start_time
        self.backend._record_operation("aggregating_merge", duration)

    cpdef bint is_expired(self):
        """
        Check if the current accumulator has expired based on TTL.

        Returns True if TTL is enabled and accumulator has expired.
        """
        if not self.ttl_enabled or self.last_access_time == 0:
            return False

        cdef uint64_t current_time = <uint64_t>(time.time() * 1000)
        cdef uint64_t age_ms = current_time - self.last_access_time

        return age_ms > self.ttl_ms

    cpdef void refresh_ttl(self):
        """
        Refresh the TTL timestamp for the current accumulator.

        Call this after accessing the accumulator to extend its lifetime.
        """
        self.last_access_time = <uint64_t>(time.time() * 1000)

    cpdef str get_state_name(self):
        """Get the state name."""
        return self.state_name.decode('utf-8')

    cpdef object get_add_function(self):
        """Get the add function."""
        return self.add_function

    cpdef object get_aggregate_function(self):
        """Get the aggregate function."""
        return self.aggregate_function

    # Python special methods for convenience

    def __str__(self):
        """String representation."""
        return f"AggregatingState(name='{self.get_state_name()}', ttl={self.ttl_enabled})"

    def __repr__(self):
        """Detailed representation."""
        accumulator = self.get_accumulator()
        result = self.get()
        return (f"AggregatingState(name='{self.get_state_name()}', "
                f"accumulator={accumulator!r}, result={result!r}, "
                f"ttl={self.ttl_enabled}, expired={self.is_expired()})")

    def __bool__(self):
        """Check if state has accumulated values."""
        return not self.is_empty()

    # Context manager support for scoped operations

    def __enter__(self):
        """Enter context - ensure key is set."""
        if not self.backend.has_current_key():
            raise RuntimeError("No current key set for AggregatingState operation")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context."""
        pass

    # Property-style access for Python convenience

    @property
    def name(self):
        """Get state name."""
        return self.get_state_name()

    @property
    def value(self):
        """Get aggregated value."""
        return self.get()

    @property
    def accumulator(self):
        """Get raw accumulator."""
        return self.get_accumulator()

    @property
    def expired(self):
        """Check if expired."""
        return self.is_expired()

    @property
    def empty(self):
        """Check if empty."""
        return self.is_empty()

    @property
    def adder(self):
        """Get add function."""
        return self.get_add_function()

    @property
    def aggregator(self):
        """Get aggregate function."""
        return self.get_aggregate_function()
