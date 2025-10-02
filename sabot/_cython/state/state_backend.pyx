# -*- coding: utf-8 -*-
"""
State Backend Base Implementation

Provides the core state backend interface and common functionality
for Flink-compatible state management in Sabot.
"""

from libc.stdint cimport int64_t, uint64_t, int32_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from cython.operator cimport dereference as deref, preincrement as inc
from cpython.ref cimport PyObject

import time
import logging

logger = logging.getLogger(__name__)

# Structs are declared in .pxd file


cdef class StateBackend:
    """
    Base implementation of state backend interface.

    Provides common functionality for all state backends:
    - Key scoping and registration
    - Operation counting and metrics
    - TTL handling
    - Error handling and logging
    """

    def __cinit__(self):
        """C-level initialization."""
        self.current_namespace = string()
        self.current_key = string()
        self.key_scope = KeyScope()
        self.registered_states = unordered_map[string, StateDescriptor]()

    cpdef void set_current_key(self, str namespace, str key):
        """
        Set the current key context for state operations.

        All subsequent state operations will be scoped to this key.
        Performance: <1ns (string assignment)
        """
        self.current_namespace = namespace.encode('utf-8')
        self.current_key = key.encode('utf-8')

    cpdef tuple get_current_key(self):
        """Get the current key context as Python tuple."""
        return (
            self.current_namespace.decode('utf-8'),
            self.current_key.decode('utf-8')
        )

    cpdef bint has_current_key(self):
        """Check if a current key is set."""
        return not self.current_namespace.empty() and not self.current_key.empty()

    cpdef void register_state(self, StateDescriptor descriptor):
        """Register a state descriptor."""
        self.registered_states[descriptor.state_name] = descriptor

    cpdef StateDescriptor get_state_descriptor(self, str state_name):
        """Get state descriptor by name."""
        cdef string name = state_name.encode('utf-8')
        if self.registered_states.count(name):
            return self.registered_states[name]
        else:
            raise KeyError(f"State '{state_name}' not registered")

    cpdef vector[string] list_registered_states(self):
        """List all registered state names."""
        cdef vector[string] names
        cdef unordered_map[string, StateDescriptor].iterator it

        it = self.registered_states.begin()
        while it != self.registered_states.end():
            names.push_back(deref(it).first)
            inc(it)

        return names

    # Abstract methods that must be implemented by subclasses

    cpdef void put_value(self, str state_name, object value):
        """Put value to ValueState - must be implemented by subclass."""
        raise NotImplementedError("put_value must be implemented by subclass")

    cpdef object get_value(self, str state_name, object default_value=None):
        """Get value from ValueState - must be implemented by subclass."""
        raise NotImplementedError("get_value must be implemented by subclass")

    cpdef void clear_value(self, str state_name):
        """Clear ValueState - must be implemented by subclass."""
        raise NotImplementedError("clear_value must be implemented by subclass")

    cpdef void add_to_list(self, str state_name, object value):
        """Add to ListState - must be implemented by subclass."""
        raise NotImplementedError("add_to_list must be implemented by subclass")

    cpdef object get_list(self, str state_name):
        """Get ListState - must be implemented by subclass."""
        raise NotImplementedError("get_list must be implemented by subclass")

    cpdef void clear_list(self, str state_name):
        """Clear ListState - must be implemented by subclass."""
        raise NotImplementedError("clear_list must be implemented by subclass")

    cpdef void remove_from_list(self, str state_name, object value):
        """Remove from ListState - must be implemented by subclass."""
        raise NotImplementedError("remove_from_list must be implemented by subclass")

    cpdef bint list_contains(self, str state_name, object value):
        """Check if ListState contains value - must be implemented by subclass."""
        raise NotImplementedError("list_contains must be implemented by subclass")

    cpdef void put_to_map(self, str state_name, object key, object value):
        """Put to MapState - must be implemented by subclass."""
        raise NotImplementedError("put_to_map must be implemented by subclass")

    cpdef object get_from_map(self, str state_name, object key, object default_value=None):
        """Get from MapState - must be implemented by subclass."""
        raise NotImplementedError("get_from_map must be implemented by subclass")

    cpdef void remove_from_map(self, str state_name, object key):
        """Remove from MapState - must be implemented by subclass."""
        raise NotImplementedError("remove_from_map must be implemented by subclass")

    cpdef bint map_contains(self, str state_name, object key):
        """Check if MapState contains key - must be implemented by subclass."""
        raise NotImplementedError("map_contains must be implemented by subclass")

    cpdef object get_map_keys(self, str state_name):
        """Get MapState keys - must be implemented by subclass."""
        raise NotImplementedError("get_map_keys must be implemented by subclass")

    cpdef object get_map_values(self, str state_name):
        """Get MapState values - must be implemented by subclass."""
        raise NotImplementedError("get_map_values must be implemented by subclass")

    cpdef object get_map_entries(self, str state_name):
        """Get MapState entries - must be implemented by subclass."""
        raise NotImplementedError("get_map_entries must be implemented by subclass")

    cpdef void clear_map(self, str state_name):
        """Clear MapState - must be implemented by subclass."""
        raise NotImplementedError("clear_map must be implemented by subclass")

    cpdef void add_to_reducing(self, str state_name, object value):
        """Add to ReducingState - must be implemented by subclass."""
        raise NotImplementedError("add_to_reducing must be implemented by subclass")

    cpdef object get_reducing(self, str state_name, object default_value=None):
        """Get ReducingState - must be implemented by subclass."""
        raise NotImplementedError("get_reducing must be implemented by subclass")

    cpdef void clear_reducing(self, str state_name):
        """Clear ReducingState - must be implemented by subclass."""
        raise NotImplementedError("clear_reducing must be implemented by subclass")

    cpdef void add_to_aggregating(self, str state_name, object input_value):
        """Add to AggregatingState - must be implemented by subclass."""
        raise NotImplementedError("add_to_aggregating must be implemented by subclass")

    cpdef object get_aggregating(self, str state_name, object default_value=None):
        """Get AggregatingState - must be implemented by subclass."""
        raise NotImplementedError("get_aggregating must be implemented by subclass")

    cpdef void clear_aggregating(self, str state_name):
        """Clear AggregatingState - must be implemented by subclass."""
        raise NotImplementedError("clear_aggregating must be implemented by subclass")

    cpdef void clear_all_states(self):
        """Clear all states - must be implemented by subclass."""
        raise NotImplementedError("clear_all_states must be implemented by subclass")

    cpdef object snapshot_all_states(self):
        """Snapshot all states - must be implemented by subclass."""
        raise NotImplementedError("snapshot_all_states must be implemented by subclass")

    cpdef void restore_all_states(self, object snapshot):
        """Restore all states - must be implemented by subclass."""
        raise NotImplementedError("restore_all_states must be implemented by subclass")

    # Monitoring and metrics (base implementation - attributes declared in .pxd)

    def __init__(self):
        """Python-level initialization."""
        self.operation_counts = unordered_map[string, uint64_t]()
        self.total_operation_time_ms = 0.0
        self.operation_count = 0

    cdef inline void _record_operation(self, str operation, double duration_ms):
        """Record operation metrics."""
        cdef string op = operation.encode('utf-8')
        cdef uint64_t count = 1

        # Update counts
        if self.operation_counts.count(op):
            self.operation_counts[op] += 1
        else:
            self.operation_counts[op] = 1

        self.total_operation_time_ms += duration_ms
        self.operation_count += 1

    cpdef unordered_map[string, uint64_t] get_operation_counts(self):
        """Get operation count metrics."""
        return self.operation_counts

    cpdef double get_average_operation_latency_ms(self):
        """Get average operation latency."""
        if self.operation_count == 0:
            return 0.0
        return self.total_operation_time_ms / self.operation_count

    cpdef uint64_t get_memory_usage_bytes(self):
        """Get memory usage - base implementation."""
        # Subclasses should override with actual memory tracking
        return 0

    # Lifecycle methods (base implementation)

    cpdef void open(self):
        """Open the state backend."""
        logger.info("Opening state backend")

    cpdef void close(self):
        """Close the state backend."""
        logger.info("Closing state backend")

    cpdef void flush(self):
        """Flush pending operations."""
        pass
