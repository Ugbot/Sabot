# -*- coding: utf-8 -*-
"""
Header file for DBOS-Enhanced Durable Checkpoint Coordinator
"""

from libc.stdint cimport int64_t, int32_t

cdef class DurableCheckpointCoordinator:
    cdef:
        object checkpoint_coordinator
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
