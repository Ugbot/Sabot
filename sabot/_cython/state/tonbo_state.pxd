# -*- coding: utf-8 -*-
"""
Header file for Cython Tonbo State Backend
"""

from libc.stdint cimport int32_t

cdef class TonboStateBackend:
    cdef:
        object _underlying_store
        object _arrow_store
        object _current_namespace
        bint _initialized

    cpdef void initialize(self, object store_backend)  # Changed from StoreBackend to object for flexibility
    cpdef void set_current_namespace(self, str namespace)
    cpdef str get_current_namespace(self)
    cpdef object get_value_state(self, str state_name)
    cpdef object get_list_state(self, str state_name)
    cpdef object get_map_state(self, str state_name)
    cpdef object get_reducing_state(self, str state_name, object reduce_function)
    cpdef object get_aggregating_state(self, str state_name, object add_function, object get_result_function)
    cpdef object arrow_scan_state(self, str state_name, str start_key = *, str end_key = *, int32_t limit = *)
    cpdef object arrow_aggregate_state(self, str state_name, list group_by_cols, dict aggregations)
    cpdef object arrow_join_states(self, str left_state, str right_state, list join_keys, str join_type = *)
    cpdef object get_backend_stats(self)

cdef class TonboValueState:
    cdef:
        TonboStateBackend _backend
        str _state_name
        str _current_key

    cpdef void set_current_key(self, str namespace, str key)
    cpdef object get(self)
    cpdef void update(self, object value)
    cpdef void clear(self)

cdef class TonboListState:
    cdef:
        TonboStateBackend _backend
        str _state_name
        str _current_key

    cpdef void set_current_key(self, str namespace, str key)
    cpdef object get(self)
    cpdef void add(self, object value)
    cpdef void update(self, object values)
    cpdef void clear(self)

cdef class TonboMapState:
    cdef:
        TonboStateBackend _backend
        str _state_name
        str _current_key

    cpdef void set_current_key(self, str namespace, str key)
    cpdef object get(self, str map_key)
    cpdef bint contains(self, str map_key) except -1
    cpdef void put(self, str map_key, object value)
    cpdef bint remove(self, str map_key) except -1
    cpdef object entries(self)
    cpdef object keys(self)
    cpdef object values(self)
    cpdef bint is_empty(self) except -1
    cpdef void clear(self)

cdef class TonboReducingState:
    cdef:
        TonboStateBackend _backend
        str _state_name
        str _current_key
        object _reduce_function

    cpdef void set_current_key(self, str namespace, str key)
    cpdef object get(self)
    cpdef void add(self, object value)
    cpdef void clear(self)

cdef class TonboAggregatingState:
    cdef:
        TonboStateBackend _backend
        str _state_name
        str _current_key
        object _add_function
        object _get_result_function

    cpdef void set_current_key(self, str namespace, str key)
    cpdef object get(self)
    cpdef void add(self, object value)
    cpdef void clear(self)

    # C method declarations
    cdef object _get_accumulator(self)
    cdef void _set_accumulator(self, object accumulator)
    cdef void _delete_accumulator(self)
