# -*- coding: utf-8 -*-
"""
Checkpoint Storage Cython Header

Header declarations for CheckpointStorage.
"""

from libc.stdint cimport int64_t, int32_t


cdef class CheckpointStorage:
    cdef:
        object tonbo_backend
        object rocksdb_backend
        bytes checkpoint_prefix
        int32_t max_checkpoints

    # Internal methods
    cdef void _update_checkpoint_manifest(self, int64_t checkpoint_id)
    cdef void _remove_from_checkpoint_manifest(self, int64_t checkpoint_id)

    # Core storage operations
    cpdef void store_checkpoint(self, int64_t checkpoint_id, object application_data,
                               object system_metadata)
    cpdef object load_checkpoint(self, int64_t checkpoint_id)
    cpdef object list_available_checkpoints(self)
    cpdef void delete_checkpoint(self, int64_t checkpoint_id)
    cpdef void cleanup_old_checkpoints(self)

    # Checkpoint information
    cpdef int64_t get_latest_checkpoint_id(self)
    cpdef bint checkpoint_exists(self, int64_t checkpoint_id)
    cpdef object get_checkpoint_info(self, int64_t checkpoint_id)
    cpdef object get_storage_stats(self)

    # Incremental checkpoints
    cpdef void store_incremental_checkpoint(self, int64_t checkpoint_id,
                                          int64_t base_checkpoint_id,
                                          object changes_since_base)
    cpdef object load_incremental_checkpoint(self, int64_t checkpoint_id)
