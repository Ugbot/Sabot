# -*- coding: utf-8 -*-
"""
ReducingState Implementation

Flink-compatible ReducingState[T] primitive with C-level performance.
Maintains a single accumulated value using a reduce function.
"""

from libc.stdint cimport int64_t, uint64_t
from libcpp.string cimport string
from cpython.ref cimport PyObject

import time
import logging

from .state_backend cimport StateBackend

logger = logging.getLogger(__name__)


cdef class ReducingState:
    """
    Flink-compatible ReducingState[T] primitive.

    Maintains a single accumulated value using a reduce function.
    Each add() merges the new value with the current accumulated value.
    Performance: <500ns add (in-memory), <2ms (persistent).
    """

    def __cinit__(self, StateBackend backend, str state_name, object reduce_function,
                  bint ttl_enabled=False, int64_t ttl_ms=0):
        """C-level initialization."""
        self.backend = backend
        self.state_name = state_name.encode('utf-8')
        self.reduce_function = reduce_function
        self.ttl_enabled = ttl_enabled
        self.ttl_ms = ttl_ms
        self.last_access_time = 0

    cpdef void add(self, object value):
        """
        Add a value and reduce it with the current accumulated value.

        Uses the configured reduce function to merge values.
        Performance: <500ns (in-memory), <2ms (persistent)
        """
        cdef double start_time = time.perf_counter() * 1000

        # Check TTL before add
        if self.ttl_enabled and self.is_expired():
            # Clear expired value first
            self.backend.clear_reducing(self.state_name.decode('utf-8'))

        # Get current value
        cdef object current_value = self.backend.get_reducing(
            self.state_name.decode('utf-8'), None
        )

        # Apply reduce function
        cdef object new_value
        if current_value is None:
            # First value
            new_value = value
        else:
            # Reduce with current value
            new_value = self.reduce_function(current_value, value)

        # Store new accumulated value
        self.backend.add_to_reducing(self.state_name.decode('utf-8'), new_value)
        self.last_access_time = <uint64_t>(time.time() * 1000)

        # Record metrics
        duration = time.perf_counter() * 1000 - start_time
        self.backend._record_operation("reducing_add", duration)

    cpdef object get(self):
        """
        Get the current accumulated value.

        Returns None if no value exists or TTL expired.
        Performance: <200ns (in-memory), <1ms (persistent)
        """
        cdef double start_time = time.perf_counter() * 1000

        # Check TTL
        if self.ttl_enabled and self.is_expired():
            # Clear expired value
            self.backend.clear_reducing(self.state_name.decode('utf-8'))
            self.last_access_time = 0

            duration = time.perf_counter() * 1000 - start_time
            self.backend._record_operation("reducing_get_expired", duration)
            return None

        # Get current value
        cdef object result = self.backend.get_reducing(
            self.state_name.decode('utf-8'), None
        )

        self.last_access_time = <uint64_t>(time.time() * 1000)

        duration = time.perf_counter() * 1000 - start_time
        self.backend._record_operation("reducing_get", duration)

        return result

    cpdef void clear(self):
        """
        Clear the accumulated value.

        Performance: <50ns (in-memory), <500μs (persistent)
        """
        cdef double start_time = time.perf_counter() * 1000

        self.backend.clear_reducing(self.state_name.decode('utf-8'))
        self.last_access_time = 0

        duration = time.perf_counter() * 1000 - start_time
        self.backend._record_operation("reducing_clear", duration)

    cpdef bint is_empty(self):
        """
        Check if the state is empty.

        Returns True if no value exists or TTL expired.
        Performance: <200ns (in-memory), <1ms (persistent)
        """
        cdef object value = self.get()
        return value is None

    cpdef bint is_expired(self):
        """
        Check if the current value has expired based on TTL.

        Returns True if TTL is enabled and value has expired.
        """
        if not self.ttl_enabled or self.last_access_time == 0:
            return False

        cdef uint64_t current_time = <uint64_t>(time.time() * 1000)
        cdef uint64_t age_ms = current_time - self.last_access_time

        return age_ms > self.ttl_ms

    cpdef void refresh_ttl(self):
        """
        Refresh the TTL timestamp for the current value.

        Call this after accessing the value to extend its lifetime.
        """
        self.last_access_time = <uint64_t>(time.time() * 1000)

    cpdef str get_state_name(self):
        """Get the state name."""
        return self.state_name.decode('utf-8')

    cpdef object get_reduce_function(self):
        """Get the reduce function."""
        return self.reduce_function

    # Python special methods for convenience

    def __str__(self):
        """String representation."""
        return f"ReducingState(name='{self.get_state_name()}', ttl={self.ttl_enabled})"

    def __repr__(self):
        """Detailed representation."""
        current_val = self.get()
        return (f"ReducingState(name='{self.get_state_name()}', "
                f"value={current_val!r}, ttl={self.ttl_enabled}, "
                f"expired={self.is_expired()})")

    def __bool__(self):
        """Check if state has a value."""
        return not self.is_empty()

    # Context manager support for scoped operations

    def __enter__(self):
        """Enter context - ensure key is set."""
        if not self.backend.has_current_key():
            raise RuntimeError("No current key set for ReducingState operation")
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
        """Get current value."""
        return self.get()

    @value.setter
    def value(self, new_value):
        """Set value directly (clears and sets)."""
        self.backend.clear_reducing(self.state_name.decode('utf-8'))
        if new_value is not None:
            self.backend.add_to_reducing(self.state_name.decode('utf-8'), new_value)
        self.last_access_time = <uint64_t>(time.time() * 1000)

    @property
    def expired(self):
        """Check if expired."""
        return self.is_expired()

    @property
    def empty(self):
        """Check if empty."""
        return self.is_empty()

    @property
    def reducer(self):
        """Get reduce function."""
        return self.get_reduce_function()
