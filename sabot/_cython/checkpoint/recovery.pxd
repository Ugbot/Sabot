# -*- coding: utf-8 -*-
"""
Recovery Manager Cython Header

Header declarations for RecoveryManager.
"""

from libc.stdint cimport int64_t

from .storage cimport CheckpointStorage
from .coordinator cimport CheckpointCoordinator


cdef class RecoveryManager:
    cdef:
        CheckpointStorage storage
        CheckpointCoordinator coordinator
        object application_state_manager
        bint recovery_in_progress
        int64_t recovering_checkpoint_id

    # Internal methods
    cdef void _validate_checkpoint(self, int64_t checkpoint_id)
    cdef void _restore_system_state(self, int64_t checkpoint_id)
    cdef void _restore_application_state(self, int64_t checkpoint_id)
    cdef void _validate_restored_state(self)
    cdef void _complete_recovery(self)
    cdef void _apply_incremental_checkpoint(self, int64_t checkpoint_id)
    cdef void _start_partial_recovery(self, int64_t checkpoint_id, object skip_components)
    cdef void _validate_checkpoint_partial(self, int64_t checkpoint_id, object skip_components)
    cdef void _restore_system_state_partial(self, int64_t checkpoint_id, object skip_components)
    cdef void _restore_application_state_partial(self, int64_t checkpoint_id, object skip_components)

    # Recovery control
    cpdef void set_application_state_manager(self, object manager)
    cpdef bint needs_recovery(self)
    cpdef int64_t select_recovery_checkpoint(self)
    cpdef void start_recovery(self, int64_t checkpoint_id)
    cpdef void cancel_recovery(self)
    cpdef bint is_recovery_in_progress(self)
    cpdef int64_t get_recovering_checkpoint_id(self)
    cpdef object get_recovery_stats(self)

    # Recovery strategies
    cpdef object get_recovery_strategies(self)
