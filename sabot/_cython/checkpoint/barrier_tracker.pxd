# -*- coding: utf-8 -*-
"""
Barrier Tracker Cython Header

Header declarations for BarrierTracker.
"""

from libc.stdint cimport int64_t, int32_t

# Forward declare BarrierState struct
cdef struct BarrierState:
    int64_t checkpoint_id
    int32_t total_inputs
    int32_t received_count
    bint is_complete
    int64_t timestamp


cdef class BarrierTracker:
    cdef:
        int32_t num_channels
        BarrierState* barrier_states  # Array of barrier states
        int32_t max_concurrent_checkpoints
        int32_t active_barrier_count
        object state_backend
        bytes barrier_prefix

    # Internal methods
    cdef BarrierState* _find_barrier_state(self, int64_t checkpoint_id) nogil
    cdef BarrierState* _allocate_barrier_state(self, int64_t checkpoint_id,
                                                int32_t total_inputs) nogil
    cdef int64_t _get_timestamp_ns(self) nogil

    # Core barrier operations
    cpdef bint register_barrier(self, int32_t channel, int64_t checkpoint_id,
                               int32_t total_inputs) except -1
    cdef bint is_barrier_aligned(self, int64_t checkpoint_id) nogil
    cpdef void reset_barrier(self, int64_t checkpoint_id)
    cpdef object get_barrier_stats(self, int64_t checkpoint_id)
    cpdef object get_active_barriers(self)
    cdef int32_t get_active_barrier_count(self) nogil
    cdef bint has_capacity(self) nogil
    cpdef void clear_all_barriers(self)

    # Persistence
    cpdef void persist_state(self)
    cpdef void restore_state(self)
