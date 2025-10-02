# -*- coding: utf-8 -*-
"""
Checkpoint Coordinator Cython Header

Header declarations for CheckpointCoordinator.
"""

from libc.stdint cimport int64_t, int32_t

from .barrier_tracker cimport BarrierTracker


# Forward declare checkpoint structs
cdef struct CheckpointMetadata:
    int64_t checkpoint_id
    int64_t start_timestamp
    int64_t completion_timestamp
    int32_t operator_count
    int32_t completed_count
    bint is_completed
    bint has_failed
    int64_t failure_timestamp


cdef struct OperatorCheckpointState:
    int32_t operator_id
    int64_t checkpoint_id
    bint is_snapshot_complete
    bint has_acknowledged
    int64_t snapshot_timestamp
    int64_t ack_timestamp


cdef class CheckpointCoordinator:
    cdef:
        int64_t current_checkpoint_id
        CheckpointMetadata* active_checkpoints
        int32_t max_concurrent_checkpoints
        int32_t active_checkpoint_count
        OperatorCheckpointState* operator_states
        int32_t max_operators
        int32_t registered_operator_count
        dict operator_registry
        BarrierTracker barrier_tracker
        object state_backend
        object tonbo_backend
        bytes checkpoint_prefix

    # Internal methods
    cdef void _initialize_arrays(self)
    cdef int64_t _generate_checkpoint_id(self) nogil
    cdef CheckpointMetadata* _allocate_checkpoint_metadata(self, int64_t checkpoint_id)
    cdef void _inject_barriers(self, int64_t checkpoint_id)
    cdef int64_t _get_timestamp_ns(self) nogil
    cdef CheckpointMetadata* _find_checkpoint(self, int64_t checkpoint_id) nogil
    cdef OperatorCheckpointState* _find_operator_state(self, int32_t operator_id,
                                                        int64_t checkpoint_id) nogil
    cdef OperatorCheckpointState* _allocate_operator_checkpoint_state(self,
                                                                       int32_t operator_id,
                                                                       int64_t checkpoint_id)
    cdef void _cleanup_checkpoint_operator_states(self, int64_t checkpoint_id)

    # Initialization
    cpdef void set_barrier_tracker(self, BarrierTracker tracker)
    cpdef void set_storage_backends(self, object state_backend, object tonbo_backend)

    # Operator management
    cpdef void register_operator(self, int32_t operator_id, str operator_name,
                                object operator_instance)

    # Checkpoint lifecycle
    cpdef int64_t trigger_checkpoint(self) except -1
    cpdef bint acknowledge_checkpoint(self, int32_t operator_id, int64_t checkpoint_id) except -1
    cpdef bint is_checkpoint_complete(self, int64_t checkpoint_id) nogil
    cpdef bint has_checkpoint_failed(self, int64_t checkpoint_id) nogil
    cpdef void fail_checkpoint(self, int64_t checkpoint_id, str reason)

    # Statistics and monitoring
    cpdef object get_checkpoint_stats(self, int64_t checkpoint_id)
    cpdef object get_active_checkpoints(self)
    cpdef void cleanup_completed_checkpoints(self)
    cpdef object get_coordinator_stats(self)

    # Persistence
    cpdef void persist_state(self)
    cpdef void restore_state(self)
