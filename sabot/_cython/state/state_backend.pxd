# -*- coding: utf-8 -*-
"""
State Backend Interface Header

Defines the Cython interface for state backends in Sabot.
Provides Flink-compatible state management with C-level performance.
"""

from libc.stdint cimport int64_t, uint64_t, int32_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map

# Forward declarations - make them proper Cython structs, not extern
cdef struct StateDescriptor:
    string state_name
    int state_type  # 0=Value, 1=List, 2=Map, 3=Reducing, 4=Aggregating
    string key_serializer
    string value_serializer
    bint ttl_enabled
    int64_t ttl_ms

cdef struct KeyScope:
    string namespace
    string key_group
    int32_t parallelism
    int32_t max_parallelism


cdef class StateBackend:
    """
    Base interface for state backends in Sabot.

    Provides Flink-compatible state management with C-level performance.
    All state operations are scoped to a current key context.
    """

    cdef:
        string current_namespace
        string current_key
        KeyScope key_scope
        unordered_map[string, StateDescriptor] registered_states
        unordered_map[string, uint64_t] operation_counts
        double total_operation_time_ms
        uint64_t operation_count

    # Internal methods
    cdef inline void _record_operation(self, str operation, double duration_ms)

    # Core lifecycle methods
    cpdef void set_current_key(self, str namespace, str key)
    cpdef tuple get_current_key(self)
    cpdef bint has_current_key(self)

    # State registration
    cpdef void register_state(self, StateDescriptor descriptor)
    cpdef StateDescriptor get_state_descriptor(self, str state_name)
    cpdef vector[string] list_registered_states(self)

    # ValueState operations
    cpdef void put_value(self, str state_name, object value)
    cpdef object get_value(self, str state_name, object default_value=*)
    cpdef void clear_value(self, str state_name)

    # ListState operations
    cpdef void add_to_list(self, str state_name, object value)
    cpdef object get_list(self, str state_name)
    cpdef void clear_list(self, str state_name)
    cpdef void remove_from_list(self, str state_name, object value)
    cpdef bint list_contains(self, str state_name, object value)

    # MapState operations
    cpdef void put_to_map(self, str state_name, object key, object value)
    cpdef object get_from_map(self, str state_name, object key, object default_value=*)
    cpdef void remove_from_map(self, str state_name, object key)
    cpdef bint map_contains(self, str state_name, object key)
    cpdef object get_map_keys(self, str state_name)
    cpdef object get_map_values(self, str state_name)
    cpdef object get_map_entries(self, str state_name)
    cpdef void clear_map(self, str state_name)

    # ReducingState operations
    cpdef void add_to_reducing(self, str state_name, object value)
    cpdef object get_reducing(self, str state_name, object default_value=*)
    cpdef void clear_reducing(self, str state_name)

    # AggregatingState operations
    cpdef void add_to_aggregating(self, str state_name, object input_value)
    cpdef object get_aggregating(self, str state_name, object default_value=*)
    cpdef void clear_aggregating(self, str state_name)

    # Bulk operations
    cpdef void clear_all_states(self)
    cpdef object snapshot_all_states(self)
    cpdef void restore_all_states(self, object snapshot)

    # Performance and monitoring
    cpdef unordered_map[string, uint64_t] get_operation_counts(self)
    cpdef double get_average_operation_latency_ms(self)
    cpdef uint64_t get_memory_usage_bytes(self)

    # Lifecycle
    cpdef void open(self)
    cpdef void close(self)
    cpdef void flush(self)
