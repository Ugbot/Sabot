# -*- coding: utf-8 -*-
# cython: language_level=3
"""
MarbleDB Store Backend Implementation using Storage Shim

This is a thin wrapper around the storage shim layer.
All MarbleDB-specific code is isolated in the C++ shim.
"""

import logging
from typing import Optional

from libcpp.string cimport string
from libcpp.memory cimport unique_ptr
from libcpp cimport bool as cbool

# Import the storage shim
from sabot._cython.storage.storage_shim cimport SabotStoreBackend

# Arrow imports
cimport pyarrow.lib as pa_lib

logger = logging.getLogger(__name__)

cdef class MarbleDBStoreBackend:
    """MarbleDB store backend using storage shim."""
    
    cdef unique_ptr[SabotStoreBackend] _shim
    cdef str _db_path
    cdef cbool _is_open
    
    def __init__(self, str db_path="./sabot_marbledb_store"):
        """Initialize store backend."""
        self._db_path = db_path
        self._is_open = False
        self._shim = unique_ptr[SabotStoreBackend](new SabotStoreBackend("marbledb".encode('utf-8')))
    
    cpdef void open(self):
        """Open MarbleDB via shim."""
        if self._is_open:
            return
        
        if self._shim.get() == NULL:
            raise RuntimeError("Shim backend not initialized")
        
        self._shim.get().open(self._db_path)
        self._is_open = True
        logger.info(f"MarbleDB store backend opened at {self._db_path}")
    
    cpdef void close(self):
        """Close MarbleDB."""
        if not self._is_open:
            return
        
        if self._shim.get() != NULL:
            self._shim.get().close()
        
        self._is_open = False
        logger.info("MarbleDB store backend closed")
    
    cpdef void flush(self):
        """Flush pending writes."""
        if not self._is_open or self._shim.get() == NULL:
            raise RuntimeError("Backend not open")
        self._shim.get().flush()
    
    cpdef void create_column_family(self, str table_name, object pyarrow_schema):
        """Create column family with Arrow schema."""
        if not self._is_open or self._shim.get() == NULL:
            raise RuntimeError("Backend not open")
        
        self._shim.get().create_table(table_name, pyarrow_schema)
        logger.info(f"Created column family: {table_name}")
    
    cpdef void insert_batch(self, str table_name, object pyarrow_batch):
        """Insert Arrow RecordBatch."""
        if not self._is_open or self._shim.get() == NULL:
            raise RuntimeError("Backend not open")
        
        self._shim.get().insert_batch(table_name, pyarrow_batch)
    
    cpdef object scan_table(self, str table_name):
        """Scan table - returns PyArrow Table."""
        if not self._is_open or self._shim.get() == NULL:
            raise RuntimeError("Backend not open")
        
        return self._shim.get().scan_table(table_name)
    
    cpdef object scan_range_to_table(self, str table_name, str start_key=None, str end_key=None):
        """Scan range and return as PyArrow Table."""
        if not self._is_open or self._shim.get() == NULL:
            raise RuntimeError("Backend not open")
        
        # For now, use scan_table - TODO: implement proper range scan
        return self.scan_table(table_name)
    
    cpdef void delete_range(self, str table_name, str start_key, str end_key):
        """Delete range of keys."""
        if not self._is_open or self._shim.get() == NULL:
            raise RuntimeError("Backend not open")
        
        self._shim.get().delete_range(table_name, start_key, end_key)
    
    cpdef list list_column_families(self):
        """List all column families."""
        if not self._is_open or self._shim.get() == NULL:
            raise RuntimeError("Backend not open")
        
        # TODO: Implement in shim
        return []

