# -*- coding: utf-8 -*-
"""
Cython Tonbo State Backend

High-performance columnar state backend using Tonbo + Arrow integration.
Provides Flink-compatible state primitives with zero-copy columnar operations.
"""

from libc.stdint cimport int64_t, int32_t
from cpython.ref cimport PyObject

cimport cython

# Note: stores_base is in the parent _cython directory, not in a stores subdirectory
# Commenting out for now - tonbo_state doesn't actually need StoreBackend
# from ..stores_base cimport StoreBackend


@cython.final
cdef class TonboStateBackend:
    """
    Tonbo-based state backend with Arrow columnar integration.

    Provides high-performance state management for Sabot's streaming operations
    with native support for columnar data structures and analytical queries.
    """
    # Note: cdef attributes are declared in tonbo_state.pxd

    def __cinit__(self):
        """Initialize the Tonbo state backend."""
        self._underlying_store = None
        self._arrow_store = None
        self._current_namespace = None
        self._initialized = False

    cpdef void initialize(self, object store_backend):
        """
        Initialize with underlying store backend.

        Args:
            store_backend: The underlying Tonbo store backend (object type for flexibility)
        """
        self._underlying_store = store_backend

        # Try to get Arrow integration if available
        if hasattr(store_backend, '_arrow_store') and store_backend._arrow_store:
            self._arrow_store = store_backend._arrow_store

        self._initialized = True

    cpdef void set_current_namespace(self, str namespace):
        """
        Set the current namespace for state operations.

        This allows isolation between different operators/workflows.
        """
        self._current_namespace = namespace

    cpdef str get_current_namespace(self):
        """Get the current namespace."""
        return self._current_namespace or "default"

    # State primitive implementations

    cpdef object get_value_state(self, str state_name):
        """
        Get a ValueState instance for the given name.

        Returns a state object that can store single values with TTL support.
        """
        if not self._initialized:
            raise RuntimeError("TonboStateBackend not initialized")

        return TonboValueState(self, state_name)

    cpdef object get_list_state(self, str state_name):
        """
        Get a ListState instance for the given name.

        Returns a state object for storing lists of values.
        """
        if not self._initialized:
            raise RuntimeError("TonboStateBackend not initialized")

        return TonboListState(self, state_name)

    cpdef object get_map_state(self, str state_name):
        """
        Get a MapState instance for the given name.

        Returns a state object for storing key-value maps.
        """
        if not self._initialized:
            raise RuntimeError("TonboStateBackend not initialized")

        return TonboMapState(self, state_name)

    cpdef object get_reducing_state(self, str state_name, object reduce_function):
        """
        Get a ReducingState instance for the given name.

        Args:
            state_name: Name of the state
            reduce_function: Function to reduce multiple values to single value

        Returns:
            ReducingState instance
        """
        if not self._initialized:
            raise RuntimeError("TonboStateBackend not initialized")

        return TonboReducingState(self, state_name, reduce_function)

    cpdef object get_aggregating_state(self, str state_name, object add_function,
                                      object get_result_function):
        """
        Get an AggregatingState instance for the given name.

        Args:
            state_name: Name of the state
            add_function: Function to add input to accumulator
            get_result_function: Function to extract result from accumulator

        Returns:
            AggregatingState instance
        """
        if not self._initialized:
            raise RuntimeError("TonboStateBackend not initialized")

        return TonboAggregatingState(self, state_name, add_function, get_result_function)

    # Arrow-native operations for analytical workloads

    cpdef object arrow_scan_state(self, str state_name, str start_key = None,
                                str end_key = None, int32_t limit = 10000):
        """
        Scan state data as Arrow table for columnar processing.

        This enables analytical operations on state data using Arrow compute.
        """
        if not self._arrow_store:
            return None

        # Construct state key prefix
        state_prefix = f"{self.get_current_namespace()}:{state_name}:"

        # Use Arrow store to scan data
        return self._arrow_store.arrow_scan_to_table(
            start_key=f"{state_prefix}{start_key}" if start_key else state_prefix,
            end_key=f"{state_prefix}{end_key}" if end_key else None,
            limit=limit
        )

    cpdef object arrow_aggregate_state(self, str state_name, list group_by_cols,
                                     dict aggregations):
        """
        Perform Arrow-native aggregation on state data.

        Args:
            state_name: Name of the state to aggregate
            group_by_cols: Columns to group by
            aggregations: Aggregation functions to apply

        Returns:
            Arrow table with aggregated results
        """
        if not self._arrow_store:
            return None

        return self._arrow_store.arrow_aggregate(group_by_cols, aggregations)

    cpdef object arrow_join_states(self, str left_state, str right_state,
                                 list join_keys, str join_type = "inner"):
        """
        Join two state tables using Arrow-native joins.

        This enables complex analytical operations across different state collections.
        """
        if not self._arrow_store:
            return None

        # Get both state tables
        left_table = self.arrow_scan_state(left_state)
        right_table = self.arrow_scan_state(right_state)

        if left_table is None or right_table is None:
            return None

        return self._arrow_store.arrow_join(left_table, join_keys, join_type)

    # Utility methods

    cpdef object get_backend_stats(self):
        """Get comprehensive backend statistics."""
        stats = {
            'backend_type': 'tonbo_arrow',
            'initialized': self._initialized,
            'current_namespace': self.get_current_namespace(),
            'arrow_integration': self._arrow_store is not None,
        }

        if self._underlying_store and hasattr(self._underlying_store, 'get_stats'):
            underlying_stats = self._underlying_store.get_stats()
            stats.update(underlying_stats)

        return stats


# State primitive implementations

@cython.final
cdef class TonboValueState:
    """
    ValueState implementation using Tonbo storage.

    Stores single values with TTL support and efficient point lookups.
    """
    # Note: cdef attributes are declared in .pxd file, not here

    def __cinit__(self, TonboStateBackend backend, str state_name):
        """Initialize ValueState."""
        self._backend = backend
        self._state_name = state_name
        self._current_key = None

    cpdef void set_current_key(self, str namespace, str key):
        """Set the current key context for state operations."""
        self._current_key = f"{namespace}:{key}"

    cpdef object get(self):
        """Get the current value."""
        if not self._current_key:
            return None

        state_key = f"{self._backend.get_current_namespace()}:{self._state_name}:{self._current_key}"
        return self._backend._underlying_store.get(state_key)

    cpdef void update(self, object value):
        """Update the current value."""
        if not self._current_key:
            raise RuntimeError("Current key not set")

        state_key = f"{self._backend.get_current_namespace()}:{self._state_name}:{self._current_key}"
        self._backend._underlying_store.set(state_key, value)

    cpdef void clear(self):
        """Clear the current value."""
        if not self._current_key:
            return

        state_key = f"{self._backend.get_current_namespace()}:{self._state_name}:{self._current_key}"
        self._backend._underlying_store.delete(state_key)


@cython.final
cdef class TonboListState:
    """
    ListState implementation using Tonbo storage.

    Stores lists of values with efficient append and iteration operations.
    """
    # Note: cdef attributes are declared in .pxd file, not here

    def __cinit__(self, TonboStateBackend backend, str state_name):
        """Initialize ListState."""
        self._backend = backend
        self._state_name = state_name
        self._current_key = None

    cpdef void set_current_key(self, str namespace, str key):
        """Set the current key context for state operations."""
        self._current_key = f"{namespace}:{key}"

    cpdef object get(self):
        """Get the current list."""
        if not self._current_key:
            return []

        state_key = f"{self._backend.get_current_namespace()}:{self._state_name}:{self._current_key}"
        value = self._backend._underlying_store.get(state_key)
        return value if value is not None else []

    cpdef void add(self, object value):
        """Add a value to the list."""
        if not self._current_key:
            raise RuntimeError("Current key not set")

        current_list = self.get()
        current_list.append(value)

        state_key = f"{self._backend.get_current_namespace()}:{self._state_name}:{self._current_key}"
        self._backend._underlying_store.set(state_key, current_list)

    cpdef void update(self, object values):
        """Update the entire list."""
        if not self._current_key:
            raise RuntimeError("Current key not set")

        state_key = f"{self._backend.get_current_namespace()}:{self._state_name}:{self._current_key}"
        self._backend._underlying_store.set(state_key, values)

    cpdef void clear(self):
        """Clear the list."""
        if not self._current_key:
            return

        state_key = f"{self._backend.get_current_namespace()}:{self._state_name}:{self._current_key}"
        self._backend._underlying_store.delete(state_key)


@cython.final
cdef class TonboMapState:
    """
    MapState implementation using Tonbo storage.

    Stores key-value maps with efficient lookup and iteration operations.
    """
    # Note: cdef attributes are declared in .pxd file, not here

    def __cinit__(self, TonboStateBackend backend, str state_name):
        """Initialize MapState."""
        self._backend = backend
        self._state_name = state_name
        self._current_key = None

    cpdef void set_current_key(self, str namespace, str key):
        """Set the current key context for state operations."""
        self._current_key = f"{namespace}:{key}"

    cpdef object get(self, str map_key):
        """Get a value from the map."""
        if not self._current_key:
            return None

        state_key = f"{self._backend.get_current_namespace()}:{self._state_name}:{self._current_key}:{map_key}"
        return self._backend._underlying_store.get(state_key)

    cpdef bint contains(self, str map_key) except -1:
        """Check if map contains the key."""
        if not self._current_key:
            return False

        state_key = f"{self._backend.get_current_namespace()}:{self._state_name}:{self._current_key}:{map_key}"
        return self._backend._underlying_store.exists(state_key)

    cpdef void put(self, str map_key, object value):
        """Put a value in the map."""
        if not self._current_key:
            raise RuntimeError("Current key not set")

        state_key = f"{self._backend.get_current_namespace()}:{self._state_name}:{self._current_key}:{map_key}"
        self._backend._underlying_store.set(state_key, value)

    cpdef bint remove(self, str map_key) except -1:
        """Remove a key from the map."""
        if not self._current_key:
            return False

        state_key = f"{self._backend.get_current_namespace()}:{self._state_name}:{self._current_key}:{map_key}"
        return self._backend._underlying_store.delete(state_key)

    cpdef object entries(self):
        """Get all map entries."""
        if not self._current_key:
            return []

        # Scan for all keys with this prefix
        prefix = f"{self._backend.get_current_namespace()}:{self._state_name}:{self._current_key}:"

        items = self._backend._underlying_store.scan(
            start_key=prefix,
            prefix=prefix
        )

        # Convert to list and strip prefix from keys
        result = []
        prefix_len = len(prefix)
        for key, value in items:
            map_key = key[prefix_len:]
            result.append((map_key, value))

        return result

    cpdef object keys(self):
        """Get all map keys."""
        entries = self.entries()
        return [key for key, value in entries]

    cpdef object values(self):
        """Get all map values."""
        entries = self.entries()
        return [value for key, value in entries]

    cpdef bint is_empty(self) except -1:
        """Check if map is empty."""
        return len(self.keys()) == 0

    cpdef void clear(self):
        """Clear the entire map."""
        if not self._current_key:
            return

        # Remove all entries with this prefix
        entries = self.entries()
        prefix = f"{self._backend.get_current_namespace()}:{self._state_name}:{self._current_key}:"

        for map_key, _ in entries:
            state_key = f"{prefix}{map_key}"
            self._backend._underlying_store.delete(state_key)


@cython.final
cdef class TonboReducingState:
    """
    ReducingState implementation using Tonbo storage.

    Applies a reduce function to combine multiple values into a single result.
    """
    # Note: cdef attributes are declared in .pxd file, not here

    def __cinit__(self, TonboStateBackend backend, str state_name, object reduce_function):
        """Initialize ReducingState."""
        self._backend = backend
        self._state_name = state_name
        self._current_key = None
        self._reduce_function = reduce_function

    cpdef void set_current_key(self, str namespace, str key):
        """Set the current key context for state operations."""
        self._current_key = f"{namespace}:{key}"

    cpdef object get(self):
        """Get the current reduced value."""
        if not self._current_key:
            return None

        state_key = f"{self._backend.get_current_namespace()}:{self._state_name}:{self._current_key}"
        return self._backend._underlying_store.get(state_key)

    cpdef void add(self, object value):
        """Add a value and reduce with existing state."""
        if not self._current_key:
            raise RuntimeError("Current key not set")

        current_value = self.get()

        if current_value is None:
            new_value = value
        else:
            new_value = self._reduce_function(current_value, value)

        state_key = f"{self._backend.get_current_namespace()}:{self._state_name}:{self._current_key}"
        self._backend._underlying_store.set(state_key, new_value)

    cpdef void clear(self):
        """Clear the state."""
        if not self._current_key:
            return

        state_key = f"{self._backend.get_current_namespace()}:{self._state_name}:{self._current_key}"
        self._backend._underlying_store.delete(state_key)


@cython.final
cdef class TonboAggregatingState:
    """
    AggregatingState implementation using Tonbo storage.

    Uses separate add and result extraction functions for flexible aggregation.
    """
    # Note: cdef attributes are declared in .pxd file, not here

    def __cinit__(self, TonboStateBackend backend, str state_name,
                  object add_function, object get_result_function):
        """Initialize AggregatingState."""
        self._backend = backend
        self._state_name = state_name
        self._current_key = None
        self._add_function = add_function
        self._get_result_function = get_result_function

    cpdef void set_current_key(self, str namespace, str key):
        """Set the current key context for state operations."""
        self._current_key = f"{namespace}:{key}"

    cpdef object get(self):
        """Get the current aggregated result."""
        if not self._current_key:
            return None

        accumulator = self._get_accumulator()
        if accumulator is not None:
            return self._get_result_function(accumulator)
        return None

    cpdef void add(self, object value):
        """Add a value to the accumulator."""
        if not self._current_key:
            raise RuntimeError("Current key not set")

        accumulator = self._get_accumulator()
        if accumulator is None:
            accumulator = value  # First value becomes initial accumulator
        else:
            accumulator = self._add_function(accumulator, value)

        self._set_accumulator(accumulator)

    cpdef void clear(self):
        """Clear the accumulator."""
        if not self._current_key:
            return

        self._delete_accumulator()

    cdef object _get_accumulator(self):
        """Get the raw accumulator value."""
        if not self._current_key:
            return None

        state_key = f"{self._backend.get_current_namespace()}:{self._state_name}:{self._current_key}:accumulator"
        return self._backend._underlying_store.get(state_key)

    cdef void _set_accumulator(self, object accumulator):
        """Set the raw accumulator value."""
        if not self._current_key:
            return

        state_key = f"{self._backend.get_current_namespace()}:{self._state_name}:{self._current_key}:accumulator"
        self._backend._underlying_store.set(state_key, accumulator)

    cdef void _delete_accumulator(self):
        """Delete the accumulator."""
        if not self._current_key:
            return

        state_key = f"{self._backend.get_current_namespace()}:{self._state_name}:{self._current_key}:accumulator"
        self._backend._underlying_store.delete(state_key)
