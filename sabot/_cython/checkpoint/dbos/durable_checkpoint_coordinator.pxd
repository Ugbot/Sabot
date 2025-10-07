# -*- coding: utf-8 -*-
"""
Header file for DBOS-Enhanced Durable Checkpoint Coordinator
"""

from libc.stdint cimport int64_t, int32_t
from ..coordinator cimport CheckpointCoordinator

cdef class DurableCheckpointCoordinator:
    cdef:
        CheckpointCoordinator checkpoint_coordinator
        object dbos_controller
        object workflow_registry
        object durable_state_store
        bint durability_enabled

    cpdef void enable_durability(self, object dbos_controller, object durable_state_store)
    cpdef object start_workflow(self, str workflow_id, str workflow_type, dict workflow_config)
    cpdef void register_workflow_operator(self, str workflow_id, int32_t operator_id,
                                        str operator_name, object operator_instance)
    cpdef int64_t trigger_workflow_checkpoint(self, str workflow_id) except -1
    cpdef bint acknowledge_workflow_checkpoint(self, str workflow_id,
                                             int32_t operator_id,
                                             int64_t checkpoint_id) except -1
    cpdef object recover_workflow(self, str workflow_id)
    cpdef void pause_workflow(self, str workflow_id, str reason=?)
    cpdef void resume_workflow(self, str workflow_id)
    cpdef void stop_workflow(self, str workflow_id, str reason=?)
    cpdef object get_workflow_stats(self, str workflow_id)
    cpdef object list_active_workflows(self)
    cpdef object get_system_health(self)

    # Private cdef methods
    cdef void _setup_dbos_callbacks(self)
    cdef void _persist_workflow_state(self, str workflow_id, object workflow_state)
    cdef object _load_workflow_state(self, str workflow_id)
    cdef int64_t _get_timestamp_ns(self)
