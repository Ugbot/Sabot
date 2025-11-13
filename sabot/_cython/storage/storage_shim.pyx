# -*- coding: utf-8 -*-
"""Cython wrapper for Sabot Storage Shim Interface

This module provides Python bindings for the C++ storage shim layer.
All MarbleDB-specific code is isolated in the C++ shim, so this layer
is clean and backend-agnostic.
"""

import logging
from typing import Optional, List, Dict, Tuple, Callable
from libc.stdint cimport uint64_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.memory cimport unique_ptr, shared_ptr
from libcpp.utility cimport move
from libcpp cimport bool as cbool

from .storage_shim cimport (
    Status, StatusCode, StorageConfig,
    StateBackend, StoreBackend,
    CreateStateBackend, CreateStoreBackend,
)

logger = logging.getLogger(__name__)

# Arrow C++ declarations - declare directly here like marbledb_store.pyx does
cdef extern from "arrow/api.h" namespace "arrow":
    cdef cppclass Schema:
        pass
    cdef cppclass RecordBatch:
        pass
    cdef cppclass Table:
        pass

# Arrow imports
cimport pyarrow.lib as pa_lib
# Import C types for casting if needed
from pyarrow.includes.libarrow cimport (
    CRecordBatch as PCRecordBatch,
    CSchema as PCSchema,
    CTable as PCTable,
)

cdef class SabotStateBackend:
    """Python wrapper for StateBackend interface."""
    
    cdef unique_ptr[StateBackend] _backend
    cdef bint _is_open
    
    def __cinit__(self, str backend_type="marbledb"):
        """Initialize state backend."""
        self._backend = CreateStateBackend(backend_type.encode('utf-8'))
        if self._backend.get() == NULL:
            raise RuntimeError(f"Failed to create {backend_type} state backend")
        self._is_open = False
    
    def __init__(self, str backend_type="marbledb"):
        """Python __init__ - already initialized in __cinit__."""
        pass
    
    def open(self, str db_path):
        """Open the backend."""
        if self._is_open:
            return
        
        cdef StorageConfig config
        config.path = db_path.encode('utf-8')
        config.memtable_size_mb = 64
        config.enable_bloom_filter = True
        config.enable_sparse_index = True
        config.index_granularity = 8192
        
        cdef Status status = self._backend.get().Open(config)
        if not status.ok():
            raise RuntimeError(f"Failed to open backend: {status.message.decode('utf-8')}")
        
        self._is_open = True
        logger.info(f"State backend opened at {db_path}")
    
    def close(self):
        """Close the backend."""
        if not self._is_open:
            return
        
        cdef Status status = self._backend.get().Close()
        if not status.ok():
            logger.warning(f"Close warning: {status.message.decode('utf-8')}")
        
        self._is_open = False
        logger.info("State backend closed")
    
    def flush(self):
        """Flush pending writes."""
        if not self._is_open:
            raise RuntimeError("Backend not open")
        
        cdef Status status = self._backend.get().Flush()
        if not status.ok():
            raise RuntimeError(f"Flush failed: {status.message.decode('utf-8')}")
    
    def put(self, str key, bytes value):
        """Put key-value pair."""
        if not self._is_open:
            raise RuntimeError("Backend not open")
        
        cdef string key_str = key.encode('utf-8')
        cdef string value_str = value
        
        cdef Status status = self._backend.get().Put(key_str, value_str)
        if not status.ok():
            raise RuntimeError(f"Put failed: {status.message.decode('utf-8')}")
    
    def get(self, str key) -> Optional[bytes]:
        """Get value for key."""
        if not self._is_open:
            raise RuntimeError("Backend not open")
        
        cdef string key_str = key.encode('utf-8')
        cdef string value_str
        
        cdef Status status = self._backend.get().Get(key_str, &value_str)
        if status.IsNotFound():
            return None
        if not status.ok():
            raise RuntimeError(f"Get failed: {status.message.decode('utf-8')}")
        
        return value_str
    
    def delete(self, str key):
        """Delete key."""
        if not self._is_open:
            raise RuntimeError("Backend not open")
        
        cdef string key_str = key.encode('utf-8')
        cdef Status status = self._backend.get().Delete(key_str)
        if not status.ok():
            raise RuntimeError(f"Delete failed: {status.message.decode('utf-8')}")
    
    def exists(self, str key) -> bool:
        """Check if key exists."""
        if not self._is_open:
            raise RuntimeError("Backend not open")
        
        cdef string key_str = key.encode('utf-8')
        cdef cbool exists_val
        
        cdef Status status = self._backend.get().Exists(key_str, &exists_val)
        if not status.ok():
            raise RuntimeError(f"Exists failed: {status.message.decode('utf-8')}")
        
        return exists_val
    
    def multi_get(self, list keys) -> Dict[str, Optional[bytes]]:
        """Multi-get values for multiple keys."""
        if not self._is_open:
            raise RuntimeError("Backend not open")
        
        cdef vector[string] key_vec
        cdef vector[string] value_vec
        
        for key in keys:
            key_vec.push_back(str(key).encode('utf-8'))
        
        cdef Status status = self._backend.get().MultiGet(key_vec, &value_vec)
        if not status.ok():
            raise RuntimeError(f"MultiGet failed: {status.message.decode('utf-8')}")
        
        result = {}
        for i in range(len(keys)):
            key = keys[i]
            if i < value_vec.size() and not value_vec[i].empty():
                result[key] = value_vec[i]
            else:
                result[key] = None
        
        return result
    
    def delete_range(self, str start_key, str end_key):
        """Delete range of keys."""
        if not self._is_open:
            raise RuntimeError("Backend not open")
        
        cdef string start_str = start_key.encode('utf-8')
        cdef string end_str = end_key.encode('utf-8')
        
        cdef Status status = self._backend.get().DeleteRange(start_str, end_str)
        if not status.ok():
            raise RuntimeError(f"DeleteRange failed: {status.message.decode('utf-8')}")
    
    def scan(self, str start_key="", str end_key=""):
        """Scan range - returns iterator."""
        if not self._is_open:
            raise RuntimeError("Backend not open")
        
        cdef string start_str = start_key.encode('utf-8')
        cdef string end_str = end_key.encode('utf-8')
        
        # Collect results
        results = []
        
        # Define callback
        def callback(key: str, value: bytes) -> bool:
            results.append((key, value))
            return True  # Continue
        
        # TODO: Implement proper callback mechanism
        # For now, use a workaround
        raise NotImplementedError("Scan callback not yet implemented in Cython wrapper")
    
    def checkpoint(self) -> str:
        """Create checkpoint."""
        if not self._is_open:
            raise RuntimeError("Backend not open")
        
        cdef string checkpoint_path
        cdef Status status = self._backend.get().CreateCheckpoint(&checkpoint_path)
        if not status.ok():
            raise RuntimeError(f"Checkpoint failed: {status.message.decode('utf-8')}")
        
        return checkpoint_path.decode('utf-8')
    
    def restore(self, str checkpoint_path):
        """Restore from checkpoint."""
        if not self._is_open:
            raise RuntimeError("Backend not open")
        
        cdef string path_str = checkpoint_path.encode('utf-8')
        cdef Status status = self._backend.get().RestoreFromCheckpoint(path_str)
        if not status.ok():
            raise RuntimeError(f"Restore failed: {status.message.decode('utf-8')}")

cdef class SabotStoreBackend:
    """Python wrapper for StoreBackend interface."""
    
    cdef unique_ptr[StoreBackend] _backend
    cdef bint _is_open
    
    def __cinit__(self, str backend_type="marbledb"):
        """Initialize store backend."""
        self._backend = CreateStoreBackend(backend_type.encode('utf-8'))
        if self._backend.get() == NULL:
            raise RuntimeError(f"Failed to create {backend_type} store backend")
        self._is_open = False
    
    def __init__(self, str backend_type="marbledb"):
        """Python __init__ - already initialized in __cinit__."""
        pass
    
    def open(self, str db_path):
        """Open the backend."""
        if self._is_open:
            return
        
        cdef StorageConfig config
        config.path = db_path.encode('utf-8')
        config.memtable_size_mb = 64
        config.enable_bloom_filter = True
        config.enable_sparse_index = True
        config.index_granularity = 8192
        
        cdef Status status = self._backend.get().Open(config)
        if not status.ok():
            raise RuntimeError(f"Failed to open backend: {status.message.decode('utf-8')}")
        
        self._is_open = True
        logger.info(f"Store backend opened at {db_path}")
    
    def close(self):
        """Close the backend."""
        if not self._is_open:
            return
        
        cdef Status status = self._backend.get().Close()
        if not status.ok():
            logger.warning(f"Close warning: {status.message.decode('utf-8')}")
        
        self._is_open = False
        logger.info("Store backend closed")
    
    def flush(self):
        """Flush pending writes."""
        if not self._is_open:
            raise RuntimeError("Backend not open")
        
        cdef Status status = self._backend.get().Flush()
        if not status.ok():
            raise RuntimeError(f"Flush failed: {status.message.decode('utf-8')}")
    
    def create_table(self, str table_name, object schema):
        """Create table with schema."""
        if not self._is_open:
            raise RuntimeError("Backend not open")
        
        cdef string table_str = table_name.encode('utf-8')
        cdef pa_lib.Schema schema_obj = <pa_lib.Schema>schema
        
        # PyArrow's sp_schema returns shared_ptr<CSchema>, but CSchema is a typedef for arrow::Schema
        # Cython knows this, so we can directly assign to shared_ptr[Schema] without casting
        cdef shared_ptr[Schema] cpp_schema = schema_obj.sp_schema
        
        cdef Status status = self._backend.get().CreateTable(table_str, cpp_schema)
        if not status.ok():
            raise RuntimeError(f"CreateTable failed: {status.message.decode('utf-8')}")
    
    def list_tables(self):
        """List all tables."""
        if not self._is_open:
            raise RuntimeError("Backend not open")
        
        cdef vector[string] table_names
        cdef Status status = self._backend.get().ListTables(&table_names)
        if not status.ok():
            raise RuntimeError(f"ListTables failed: {status.message.decode('utf-8')}")
        
        return [name.decode('utf-8') for name in table_names]
    
    def insert_batch(self, str table_name, object batch):
        """Insert Arrow RecordBatch."""
        if not self._is_open:
            raise RuntimeError("Backend not open")
        
        cdef string table_str = table_name.encode('utf-8')
        cdef pa_lib.RecordBatch batch_obj = <pa_lib.RecordBatch>batch
        
        # PyArrow's sp_batch returns shared_ptr<CRecordBatch>, but CRecordBatch is a typedef for arrow::RecordBatch
        # Cython knows this, so we can directly assign to shared_ptr[RecordBatch] without casting
        cdef shared_ptr[RecordBatch] cpp_batch = batch_obj.sp_batch
        
        cdef Status status = self._backend.get().InsertBatch(table_str, cpp_batch)
        if not status.ok():
            raise RuntimeError(f"InsertBatch failed: {status.message.decode('utf-8')}")
    
    def scan_table(self, str table_name) -> object:
        """Scan table - returns PyArrow Table.
        
        CURRENT STATUS: Stub implementation due to Cython shared_ptr conversion challenge.
        
        WORKAROUND: Use sabot._cython.stores.marbledb_store.MarbleDBStoreBackend directly
        which has working scan_table() implementation.
        
        The C++ shim works correctly - this is purely a Cython type system issue.
        
        PROPER FIX (Future):
        Implement Arrow C Data Interface in C++ helper:
          1. Add ExportTableToC(table_name, ArrowSchema*, ArrowArray*)
          2. Use Arrow C structures for zero-copy conversion
          3. Import to PyArrow using Schema._import_from_c()
        
        This is the standard Arrow approach for C++/Python interop.
        """
        if not self._is_open:
            raise RuntimeError("Backend not open")
        
        import pyarrow as pa
        
        # Stub: return empty table
        # For working reads, use sabot._cython.stores.marbledb_store directly
        return pa.Table.from_batches([], schema=pa.schema([]))
    
    def delete_range(self, str table_name, str start_key, str end_key):
        """Delete range of keys."""
        if not self._is_open:
            raise RuntimeError("Backend not open")
        
        cdef string table_str = table_name.encode('utf-8')
        cdef string start_str = start_key.encode('utf-8')
        cdef string end_str = end_key.encode('utf-8')
        
        cdef Status status = self._backend.get().DeleteRange(table_str, start_str, end_str)
        if not status.ok():
            raise RuntimeError(f"DeleteRange failed: {status.message.decode('utf-8')}")

