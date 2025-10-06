# -*- coding: utf-8 -*-
"""
High-Performance Tonbo Cython Wrapper using FFI

Provides C-level access to Tonbo's embedded Arrow database via Rust FFI.
Implements zero-copy operations and direct memory access for streaming workloads.
"""

from libc.stdint cimport int64_t, int32_t, uint8_t, uintptr_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, strlen
from cpython.bytes cimport PyBytes_FromStringAndSize

cimport cython
from sabot._cython.tonbo_ffi cimport (
    TonboDb, TonboIter,
    tonbo_db_open, tonbo_db_get, tonbo_db_insert,
    tonbo_db_delete, tonbo_db_close, tonbo_free_bytes,
    tonbo_db_scan, tonbo_iter_next, tonbo_iter_free
)

# Import Python standard library
from typing import Optional, Iterator, Any, Tuple
import logging
import time


@cython.final
cdef class FastTonboBackend:
    """
    High-performance Tonbo backend using Cython FFI for zero-copy operations.

    This provides direct access to Tonbo's LSM tree and Arrow integration
    with minimal Python overhead for maximum streaming performance.
    """

    def __cinit__(self, str db_path):
        """Initialize the fast Tonbo backend."""
        self._db = NULL
        self._db_path_bytes = db_path.encode('utf-8')
        self._initialized = False
        self._logger = logging.getLogger(__name__)

    def initialize(self):
        """Initialize the Tonbo database connection (synchronous via FFI)."""
        if self._initialized:
            return

        cdef const char* path_ptr = <const char*>self._db_path_bytes

        self._db = tonbo_db_open(path_ptr)

        if self._db == NULL:
            raise RuntimeError(f"Failed to open Tonbo database at {self._db_path_bytes.decode('utf-8')}")

        self._initialized = True
        self._logger.info(f"FastTonboBackend initialized at {self._db_path_bytes.decode('utf-8')}")

    def close(self):
        """Close the Tonbo database connection."""
        if self._db != NULL:
            tonbo_db_close(self._db)
            self._db = NULL
            self._initialized = False
            self._logger.info("FastTonboBackend closed")

    def __dealloc__(self):
        """Cleanup on object destruction."""
        if self._db != NULL:
            tonbo_db_close(self._db)
            self._db = NULL

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def fast_get(self, str key) -> Optional[bytes]:
        """
        High-performance get operation with minimal Python overhead.

        Returns raw bytes value directly from Tonbo for zero-copy processing.
        """
        if not self._initialized or self._db == NULL:
            return None

        cdef:
            bytes key_bytes = key.encode('utf-8')
            const char* key_ptr = <const char*>key_bytes
            uintptr_t key_len = len(key_bytes)
            uint8_t* value_ptr = NULL
            uintptr_t value_len = 0
            int result
            bytes value_bytes

        try:
            result = tonbo_db_get(
                self._db,
                key_ptr,
                key_len,
                &value_ptr,
                &value_len
            )

            if result == 0:
                # Success - copy bytes to Python object
                value_bytes = PyBytes_FromStringAndSize(<char*>value_ptr, value_len)
                tonbo_free_bytes(value_ptr, value_len)
                return value_bytes
            elif result == -1:
                # Key not found
                return None
            else:
                # Error
                self._logger.error(f"Tonbo get error for key {key}: result={result}")
                return None

        except Exception as e:
            self._logger.error(f"Tonbo get exception for key {key}: {e}")
            if value_ptr != NULL:
                tonbo_free_bytes(value_ptr, value_len)
            return None

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def fast_insert(self, str key, bytes value):
        """
        High-performance insert operation.

        Directly inserts bytes without Python object overhead.
        """
        if not self._initialized or self._db == NULL:
            raise RuntimeError("Database not initialized")

        cdef:
            bytes key_bytes = key.encode('utf-8')
            const char* key_ptr = <const char*>key_bytes
            uintptr_t key_len = len(key_bytes)
            const uint8_t* value_ptr = <const uint8_t*>value
            uintptr_t value_len = len(value)
            int result

        result = tonbo_db_insert(
            self._db,
            key_ptr,
            key_len,
            value_ptr,
            value_len
        )

        if result != 0:
            raise RuntimeError(f"Tonbo insert failed for key {key}: result={result}")

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def fast_delete(self, str key) -> bint:
        """
        High-performance delete operation.

        Returns True if key was deleted, False otherwise.
        """
        if not self._initialized or self._db == NULL:
            return False

        cdef:
            bytes key_bytes = key.encode('utf-8')
            const char* key_ptr = <const char*>key_bytes
            uintptr_t key_len = len(key_bytes)
            int result

        result = tonbo_db_delete(self._db, key_ptr, key_len)

        if result == 0:
            return True
        elif result == -1:
            # Key not found
            return False
        else:
            # Error
            self._logger.error(f"Tonbo delete error for key {key}: result={result}")
            return False

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def fast_exists(self, str key) -> bint:
        """
        High-performance existence check.
        """
        result = self.fast_get(key)
        return result is not None

    def get_stats(self) -> dict:
        """Get backend performance statistics."""
        return {
            'backend_type': 'tonbo_ffi',
            'initialized': self._initialized,
            'db_path': self._db_path_bytes.decode('utf-8'),
            'performance_mode': 'zero_copy_ffi',
            'features': ['fast_get', 'fast_insert', 'fast_delete', 'direct_ffi']
        }


@cython.final
cdef class TonboCythonWrapper:
    """
    Convenience wrapper for Cython Tonbo operations.

    Provides a high-level interface to the fast Tonbo backend
    with automatic serialization/deserialization.
    """

    def __cinit__(self, str db_path):
        """Initialize the wrapper."""
        self._backend = FastTonboBackend(db_path)
        self._serializer = None

    def initialize(self, serializer=None):
        """Initialize with optional custom serializer."""
        from sabot._cython.tonbo_wrapper import _DefaultSerializer
        self._serializer = serializer or _DefaultSerializer()
        self._backend.initialize()

    def close(self):
        """Close the backend."""
        self._backend.close()

    def get(self, key: Any) -> Any:
        """Get a value with automatic deserialization."""
        if not self._serializer:
            raise RuntimeError("Serializer not initialized")

        key_str = str(key)
        value_bytes = self._backend.fast_get(key_str)

        if value_bytes:
            return self._serializer.deserialize(value_bytes)
        return None

    def put(self, key: Any, value: Any):
        """Put a value with automatic serialization."""
        if not self._serializer:
            raise RuntimeError("Serializer not initialized")

        key_str = str(key)
        value_bytes = self._serializer.serialize(value)
        self._backend.fast_insert(key_str, value_bytes)

    def delete(self, key: Any) -> bool:
        """Delete a value."""
        key_str = str(key)
        return self._backend.fast_delete(key_str)

    def exists(self, key: Any) -> bool:
        """Check if a key exists."""
        key_str = str(key)
        return self._backend.fast_exists(key_str)


# Default serializer (defined at module level for import)
class _DefaultSerializer:
    """Default pickle-based serializer."""

    def serialize(self, obj: Any) -> bytes:
        import pickle
        return pickle.dumps(obj)

    def deserialize(self, data: bytes) -> Any:
        import pickle
        return pickle.loads(data)
