# -*- coding: utf-8 -*-
# cython: language_level=3
"""
MarbleDB State Backend Implementation using Storage Shim

This is a thin wrapper around the storage shim layer.
All MarbleDB-specific code is isolated in the C++ shim.
"""

import time
import logging
import pickle
from typing import Optional

from .state_backend cimport StateBackend

from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.memory cimport unique_ptr
from libcpp cimport bool as cbool

# Import the storage shim
from sabot._cython.storage.storage_shim cimport (
    SabotStateBackend,
    Status, StatusCode
)

logger = logging.getLogger(__name__)

cdef class MarbleDBStateBackend(StateBackend):
    """
    MarbleDB-based state backend implementation using storage shim.
    
    Uses the C++ storage shim layer for clean separation from MarbleDB.
    Provides Flink-compatible state management with <200ns point lookups.
    """
    
    cdef unique_ptr[SabotStateBackend] _shim
    cdef str _db_path
    
    def __init__(self, str db_path="./sabot_marbledb_state"):
        """Initialize the MarbleDB state backend."""
        self._db_path = db_path
        self._shim = unique_ptr[SabotStateBackend](new SabotStateBackend("marbledb".encode('utf-8')))
    
    cpdef void open(self):
        """Open MarbleDB via shim."""
        if self._shim.get() == NULL:
            raise RuntimeError("Shim backend not initialized")
        
        self._shim.get().open(self._db_path)
        logger.info(f"MarbleDB state backend opened at {self._db_path}")
    
    cpdef void close(self):
        """Close MarbleDB."""
        if self._shim.get() != NULL:
            self._shim.get().close()
            logger.info("MarbleDB state backend closed")
    
    cpdef void flush(self):
        """Flush pending writes."""
        if self._shim.get() == NULL:
            raise RuntimeError("Backend not open")
        self._shim.get().flush()
    
    def _make_key(self, str state_name):
        """Create a scoped key for the current key context."""
        namespace = self.current_namespace.decode('utf-8') if self.current_namespace.size() > 0 else ""
        key = self.current_key.decode('utf-8') if self.current_key.size() > 0 else ""
        return f"{namespace}|{key}|{state_name}"
    
    # ValueState operations
    
    cpdef void put_value(self, str state_name, object value):
        """Put value to ValueState."""
        if self._shim.get() == NULL:
            raise RuntimeError("MarbleDB backend not open")
        
        cdef double start_time = time.perf_counter() * 1000
        
        try:
            key_str = self._make_key(state_name)
            serialized_value = pickle.dumps(value)
            
            self._shim.get().put(key_str, serialized_value)
            
            duration = time.perf_counter() * 1000 - start_time
            self._record_operation("put_value", duration)
            
        except Exception as e:
            logger.error(f"Put value failed: {e}")
            raise
    
    cpdef object get_value(self, str state_name, object default_value=None):
        """Get value from ValueState."""
        if self._shim.get() == NULL:
            raise RuntimeError("MarbleDB backend not open")
        
        cdef double start_time = time.perf_counter() * 1000
        
        try:
            key_str = self._make_key(state_name)
            value_bytes = self._shim.get().get(key_str)
            
            if value_bytes is None:
                self._record_operation("get_value", time.perf_counter() * 1000 - start_time)
                return default_value
            
            value = pickle.loads(value_bytes)
            
            duration = time.perf_counter() * 1000 - start_time
            self._record_operation("get_value", duration)
            
            return value
            
        except Exception as e:
            logger.error(f"Get value failed: {e}")
            return default_value
    
    cpdef void clear_value(self, str state_name):
        """Clear value from ValueState."""
        if self._shim.get() == NULL:
            raise RuntimeError("MarbleDB backend not open")
        
        try:
            key_str = self._make_key(state_name)
            self._shim.get().delete(key_str)
        except Exception as e:
            logger.error(f"Clear value failed: {e}")
            raise
    
    # ListState operations
    
    cpdef void add_to_list(self, str state_name, object value):
        """Add value to ListState."""
        if self._shim.get() == NULL:
            raise RuntimeError("MarbleDB backend not open")
        
        try:
            current_list = self.get_list(state_name)
            if current_list is None:
                current_list = []
            current_list.append(value)
            self.put_value(state_name, current_list)
        except Exception as e:
            logger.error(f"Add to list failed: {e}")
            raise
    
    cpdef object get_list(self, str state_name):
        """Get list from ListState."""
        return self.get_value(state_name, [])
    
    cpdef void clear_list(self, str state_name):
        """Clear list from ListState."""
        self.clear_value(state_name)
    
    # MapState operations
    
    cpdef void put_to_map(self, str state_name, object map_key, object value):
        """Put value to MapState."""
        if self._shim.get() == NULL:
            raise RuntimeError("MarbleDB backend not open")
        
        try:
            current_map = self.get_map(state_name)
            if current_map is None:
                current_map = {}
            current_map[map_key] = value
            self.put_value(state_name, current_map)
        except Exception as e:
            logger.error(f"Put to map failed: {e}")
            raise
    
    cpdef object get_from_map(self, str state_name, object map_key, object default_value=None):
        """Get value from MapState."""
        current_map = self.get_map(state_name)
        if current_map is None:
            return default_value
        return current_map.get(map_key, default_value)
    
    cpdef void remove_from_map(self, str state_name, object map_key):
        """Remove key from MapState."""
        if self._shim.get() == NULL:
            raise RuntimeError("MarbleDB backend not open")
        
        try:
            current_map = self.get_map(state_name)
            if current_map is not None and map_key in current_map:
                del current_map[map_key]
                self.put_value(state_name, current_map)
        except Exception as e:
            logger.error(f"Remove from map failed: {e}")
            raise
    
    cpdef dict get_map(self, str state_name):
        """Get entire map from MapState."""
        return self.get_value(state_name, {})
    
    cpdef void clear_map(self, str state_name):
        """Clear map from MapState."""
        self.clear_value(state_name)
    
    # Raw operations (for direct key-value access)
    
    cpdef void put_raw(self, str key, bytes value):
        """Put raw bytes."""
        if self._shim.get() == NULL:
            raise RuntimeError("MarbleDB backend not open")
        self._shim.get().put(key, value)
    
    cpdef bytes get_raw(self, str key):
        """Get raw bytes."""
        if self._shim.get() == NULL:
            raise RuntimeError("MarbleDB backend not open")
        result = self._shim.get().get(key)
        return result if result is not None else b""
    
    cpdef void delete_raw(self, str key):
        """Delete raw key."""
        if self._shim.get() == NULL:
            raise RuntimeError("MarbleDB backend not open")
        self._shim.get().delete(key)
    
    cpdef bint exists_raw(self, str key):
        """Check if key exists."""
        if self._shim.get() == NULL:
            raise RuntimeError("MarbleDB backend not open")
        return self._shim.get().exists(key)
    
    cpdef dict multi_get_raw(self, list keys):
        """Multi-get raw values."""
        if self._shim.get() == NULL:
            raise RuntimeError("MarbleDB backend not open")
        return self._shim.get().multi_get(keys)
    
    cpdef void delete_range_raw(self, str start_key, str end_key):
        """Delete range of keys."""
        if self._shim.get() == NULL:
            raise RuntimeError("MarbleDB backend not open")
        self._shim.get().delete_range(start_key, end_key)
    
    cpdef object scan_range(self, str start_key=None, str end_key=None):
        """Scan range - returns iterator."""
        if self._shim.get() == NULL:
            raise RuntimeError("MarbleDB backend not open")
        
        # For now, return a simple iterator
        # TODO: Implement proper iterator using shim's scan callback
        raise NotImplementedError("Scan range iterator not yet implemented")
    
    cpdef str checkpoint(self):
        """Create checkpoint."""
        if self._shim.get() == NULL:
            raise RuntimeError("MarbleDB backend not open")
        return self._shim.get().checkpoint()
    
    cpdef void restore(self, str checkpoint_path):
        """Restore from checkpoint."""
        if self._shim.get() == NULL:
            raise RuntimeError("MarbleDB backend not open")
        self._shim.get().restore(checkpoint_path)

