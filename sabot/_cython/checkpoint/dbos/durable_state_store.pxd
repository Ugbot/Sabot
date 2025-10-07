# -*- coding: utf-8 -*-
"""
Header file for DBOS-Compatible Durable State Store
"""

from libc.stdint cimport int64_t, int32_t

cdef class DBOSDurableStateStore:
    cdef:
        object underlying_store
        object dbos_transaction_manager
        object workflow_state_cache
        bint durability_enabled

    cpdef void initialize(self, object store_backend, object dbos_transaction_manager=?)
    cpdef void put_value(self, str key, object value)
    cpdef object get_value(self, str key)
    cpdef bint delete_value(self, str key) except -1
    cpdef bint exists_value(self, str key) except -1
    cpdef void save_workflow_checkpoint(self, str workflow_id, int64_t checkpoint_id, object checkpoint_data)
    cpdef object load_workflow_checkpoint(self, str workflow_id, int64_t checkpoint_id)
    cpdef object list_workflow_checkpoints(self, str workflow_id)
    cpdef void save_operator_state(self, str workflow_id, int32_t operator_id,
                                 int64_t checkpoint_id, object operator_state)
    cpdef object load_operator_state(self, str workflow_id, int32_t operator_id,
                                   int64_t checkpoint_id)
    cpdef void cleanup_old_checkpoints(self, str workflow_id, int32_t keep_count)
    cpdef object get_storage_stats(self)

    # Private cdef methods
    cdef void _put_value_dbos_transaction(self, str key, object value)
    cdef object _get_value_dbos_transaction(self, str key)
    cdef bint _delete_value_dbos_transaction(self, str key) except -1
    cdef bint _exists_value_dbos_transaction(self, str key) except -1
    cdef void _put_value_direct(self, str key, object value)
    cdef object _get_value_direct(self, str key)
    cdef bint _delete_value_direct(self, str key) except -1
    cdef bint _exists_value_direct(self, str key) except -1
    cdef int64_t _get_timestamp_ns(self)
