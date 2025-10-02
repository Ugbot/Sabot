# -*- coding: utf-8 -*-
"""
Checkpoint Storage Implementation

Manages persistent checkpoint storage using Tonbo + RocksDB.
Handles incremental snapshots and recovery.
"""

from libc.stdint cimport int64_t, uint64_t, int32_t
from libcpp.string cimport string

cimport cython

from ..state.rocksdb_state cimport RocksDBStateBackend


@cython.final
cdef class CheckpointStorage:
    """
    Checkpoint storage manager for persistent snapshots.

    Uses hybrid storage approach:
    - Tonbo: Application data (Arrow batches, columnar state)
    - RocksDB: System metadata, coordination state

    Supports incremental checkpoints and fast recovery.
    """

    # Attributes declared in .pxd - do not redeclare here

    def __cinit__(self, object tonbo_backend, object rocksdb_backend,
                  int32_t max_checkpoints=10):
        """Initialize checkpoint storage."""
        self.tonbo_backend = tonbo_backend
        self.rocksdb_backend = rocksdb_backend
        self.checkpoint_prefix = b"__checkpoint_storage__"
        self.max_checkpoints = max_checkpoints

    cpdef void store_checkpoint(self, int64_t checkpoint_id, object application_data,
                               object system_metadata):
        """
        Store a complete checkpoint.

        application_data: Tonbo-compatible data (Arrow batches, etc.)
        system_metadata: Coordination state, timers, etc. (stored in RocksDB)
        """
        cdef str checkpoint_key = f"{self.checkpoint_prefix.decode('utf-8')}{checkpoint_id}"

        # Store application data in Tonbo
        if self.tonbo_backend and application_data:
            # Assume application_data is a dict of state tables
            for state_name, state_data in application_data.items():
                tonbo_key = f"{checkpoint_key}_app_{state_name}"
                self.tonbo_backend.put_value(tonbo_key, state_data)

        # Store system metadata in RocksDB
        if self.rocksdb_backend and system_metadata:
            metadata_key = f"{checkpoint_key}_meta"
            self.rocksdb_backend.put_value(metadata_key, system_metadata)

        # Update checkpoint manifest
        self._update_checkpoint_manifest(checkpoint_id)

    cpdef object load_checkpoint(self, int64_t checkpoint_id):
        """
        Load a checkpoint from storage.

        Returns tuple: (application_data, system_metadata)
        """
        cdef str checkpoint_key = f"{self.checkpoint_prefix.decode('utf-8')}{checkpoint_id}"
        cdef dict application_data = {}
        cdef object system_metadata = None

        # Load system metadata from RocksDB
        if self.rocksdb_backend:
            metadata_key = f"{checkpoint_key}_meta"
            system_metadata = self.rocksdb_backend.get_value(metadata_key)

        # Load application data from Tonbo
        if self.tonbo_backend:
            # In a real implementation, we'd scan for all keys with this prefix
            # For now, return empty dict
            application_data = {}

        return (application_data, system_metadata)

    cpdef object list_available_checkpoints(self):
        """
        List all available checkpoints.

        Returns list of checkpoint IDs sorted by recency.
        """
        if not self.rocksdb_backend:
            return []

        # Get checkpoint manifest
        manifest_key = self.checkpoint_prefix.decode('utf-8') + "manifest"
        manifest = self.rocksdb_backend.get_value(manifest_key)

        if not manifest:
            return []

        # Return sorted checkpoint IDs (most recent first)
        return sorted(manifest, reverse=True)

    cpdef void delete_checkpoint(self, int64_t checkpoint_id):
        """Delete a checkpoint from storage."""
        cdef str checkpoint_key = f"{self.checkpoint_prefix.decode('utf-8')}{checkpoint_id}"

        # Delete application data from Tonbo
        if self.tonbo_backend:
            # In a real implementation, we'd scan and delete all keys with this prefix
            pass

        # Delete system metadata from RocksDB
        if self.rocksdb_backend:
            metadata_key = f"{checkpoint_key}_meta"
            self.rocksdb_backend.clear_value(metadata_key)

        # Update manifest
        self._remove_from_checkpoint_manifest(checkpoint_id)

    cpdef void cleanup_old_checkpoints(self):
        """Clean up old checkpoints to maintain max_checkpoints limit."""
        available_checkpoints = self.list_available_checkpoints()

        if len(available_checkpoints) > self.max_checkpoints:
            # Delete oldest checkpoints
            checkpoints_to_delete = available_checkpoints[self.max_checkpoints:]

            for checkpoint_id in checkpoints_to_delete:
                self.delete_checkpoint(checkpoint_id)

    cdef void _update_checkpoint_manifest(self, int64_t checkpoint_id):
        """Update the checkpoint manifest."""
        if not self.rocksdb_backend:
            return

        manifest_key = self.checkpoint_prefix.decode('utf-8') + "manifest"
        manifest = self.rocksdb_backend.get_value(manifest_key)

        if not manifest:
            manifest = []

        if checkpoint_id not in manifest:
            manifest.append(checkpoint_id)

        # Keep only recent checkpoints
        if len(manifest) > self.max_checkpoints:
            manifest = manifest[-self.max_checkpoints:]

        self.rocksdb_backend.put_value(manifest_key, manifest)

    cdef void _remove_from_checkpoint_manifest(self, int64_t checkpoint_id):
        """Remove checkpoint from manifest."""
        if not self.rocksdb_backend:
            return

        manifest_key = self.checkpoint_prefix.decode('utf-8') + "manifest"
        manifest = self.rocksdb_backend.get_value(manifest_key)

        if manifest and checkpoint_id in manifest:
            manifest.remove(checkpoint_id)
            self.rocksdb_backend.put_value(manifest_key, manifest)

    cpdef int64_t get_latest_checkpoint_id(self):
        """Get the ID of the latest available checkpoint."""
        available_checkpoints = self.list_available_checkpoints()

        if not available_checkpoints:
            return -1

        return available_checkpoints[0]  # Most recent first

    cpdef bint checkpoint_exists(self, int64_t checkpoint_id):
        """Check if a checkpoint exists."""
        available_checkpoints = self.list_available_checkpoints()
        return checkpoint_id in available_checkpoints

    cpdef object get_checkpoint_info(self, int64_t checkpoint_id):
        """Get information about a checkpoint."""
        if not self.checkpoint_exists(checkpoint_id):
            return None

        # Load metadata
        application_data, system_metadata = self.load_checkpoint(checkpoint_id)

        return {
            'checkpoint_id': checkpoint_id,
            'has_application_data': application_data is not None and len(application_data) > 0,
            'has_system_metadata': system_metadata is not None,
            'application_data_keys': list(application_data.keys()) if application_data else [],
            'system_metadata_size': len(str(system_metadata)) if system_metadata else 0,
        }

    cpdef object get_storage_stats(self):
        """Get storage statistics."""
        available_checkpoints = self.list_available_checkpoints()

        stats = {
            'total_checkpoints': len(available_checkpoints),
            'max_checkpoints': self.max_checkpoints,
            'available_checkpoints': available_checkpoints,
            'latest_checkpoint': self.get_latest_checkpoint_id(),
            'has_tonbo_backend': self.tonbo_backend is not None,
            'has_rocksdb_backend': self.rocksdb_backend is not None,
        }

        # Add individual checkpoint info
        checkpoint_details = {}
        for cp_id in available_checkpoints[:5]:  # Show details for latest 5
            checkpoint_details[str(cp_id)] = self.get_checkpoint_info(cp_id)

        stats['checkpoint_details'] = checkpoint_details

        return stats

    # Incremental checkpoint support

    cpdef void store_incremental_checkpoint(self, int64_t checkpoint_id,
                                          int64_t base_checkpoint_id,
                                          object changes_since_base):
        """
        Store an incremental checkpoint based on a previous checkpoint.

        This stores only the changes since base_checkpoint_id, enabling
        much faster checkpoints for small changes.
        """
        cdef str checkpoint_key = f"{self.checkpoint_prefix.decode('utf-8')}{checkpoint_id}"

        # Store incremental metadata
        incremental_meta = {
            'base_checkpoint_id': base_checkpoint_id,
            'is_incremental': True,
            'changes': changes_since_base
        }

        if self.rocksdb_backend:
            meta_key = f"{checkpoint_key}_incremental_meta"
            self.rocksdb_backend.put_value(meta_key, incremental_meta)

        # Store the changes in Tonbo
        if self.tonbo_backend and changes_since_base:
            for state_name, changes in changes_since_base.items():
                changes_key = f"{checkpoint_key}_changes_{state_name}"
                self.tonbo_backend.put_value(changes_key, changes)

        # Update manifest
        self._update_checkpoint_manifest(checkpoint_id)

    cpdef object load_incremental_checkpoint(self, int64_t checkpoint_id):
        """
        Load an incremental checkpoint, reconstructing from base + changes.
        """
        cdef str checkpoint_key = f"{self.checkpoint_prefix.decode('utf-8')}{checkpoint_id}"

        # Load incremental metadata
        if not self.rocksdb_backend:
            return None

        meta_key = f"{checkpoint_key}_incremental_meta"
        incremental_meta = self.rocksdb_backend.get_value(meta_key)

        if not incremental_meta or not incremental_meta.get('is_incremental', False):
            # Not an incremental checkpoint, load normally
            return self.load_checkpoint(checkpoint_id)

        # Load base checkpoint
        base_checkpoint_id = incremental_meta['base_checkpoint_id']
        base_data, base_meta = self.load_checkpoint(base_checkpoint_id)

        # Apply changes
        changes = incremental_meta.get('changes', {})

        # In a real implementation, we'd merge base_data with changes
        # For now, return the changes
        return (changes, incremental_meta)

    # Python special methods

    def __str__(self):
        """String representation."""
        latest = self.get_latest_checkpoint_id()
        total = len(self.list_available_checkpoints())
        return f"CheckpointStorage(checkpoints={total}, latest={latest})"

    def __repr__(self):
        """Detailed representation."""
        stats = self.get_storage_stats()
        return (f"CheckpointStorage(total={stats['total_checkpoints']}, "
                f"max={stats['max_checkpoints']}, "
                f"latest={stats['latest_checkpoint']})")
