# cython: language_level=3
"""Cython base store backend interface for high-performance storage operations."""

import asyncio
from libc.stdint cimport uint64_t, int64_t
from libc.string cimport memcpy, memset
from libcpp.string cimport string as cpp_string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from cpython.ref cimport PyObject, Py_INCREF, Py_DECREF
from cpython.bytes cimport PyBytes_FromStringAndSize
from cython.operator cimport dereference as deref, preincrement as inc

from typing import Any, Dict, List, Optional, Union, Iterator


cdef class BackendConfig:
    """Store backend configuration."""
    # Attributes declared in .pxd file

    def __cinit__(self, backend_type: str = "memory", path: str = "", max_size: int = -1,
                  ttl_seconds: float = -1.0, compression: str = "", options: dict = None):
        self.backend_type = backend_type.encode('utf-8')
        self.path = path.encode('utf-8') if path else cpp_string()
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.compression = compression.encode('utf-8') if compression else cpp_string()
        self.options = options or {}

    cpdef str get_backend_type(self):
        return self.backend_type.decode('utf-8')

    cpdef str get_path(self):
        return self.path.decode('utf-8') if self.path.size() > 0 else ""

    cpdef int64_t get_max_size(self):
        return self.max_size

    cpdef double get_ttl_seconds(self):
        return self.ttl_seconds

    cpdef str get_compression(self):
        return self.compression.decode('utf-8') if self.compression.size() > 0 else ""

    cpdef dict get_options(self):
        return self.options


# Backwards compatibility alias
FastStoreConfig = BackendConfig


cdef class StoreBackend:
    """Abstract base class for storage backends."""
    # Attributes declared in .pxd file

    def __cinit__(self, BackendConfig config):
        self._config = config
        self._lock = asyncio.Lock()

    cdef BackendConfig get_config(self):
        return self._config

    async def start(self) -> None:
        """Initialize the backend."""
        pass

    async def stop(self) -> None:
        """Clean up resources."""
        pass

    async def get(self, key) -> Optional[Any]:
        """Get a value by key."""
        return None

    async def set(self, key, value) -> None:
        """Set a value by key."""
        pass

    async def delete(self, key) -> bool:
        """Delete a value by key. Returns True if deleted."""
        return False

    async def exists(self, key) -> bool:
        """Check if a key exists."""
        return False

    async def keys(self, prefix: Optional[str] = None) -> List[Any]:
        """Get all keys, optionally filtered by prefix."""
        return []

    async def values(self) -> List[Any]:
        """Get all values."""
        return []

    async def items(self, prefix: Optional[str] = None) -> List[tuple]:
        """Get all key-value pairs, optionally filtered by prefix."""
        return []

    async def clear(self) -> None:
        """Clear all data."""
        pass

    async def size(self) -> int:
        """Get number of items stored."""
        return 0

    async def scan(
        self,
        start_key=None,
        end_key=None,
        prefix: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Iterator[tuple]:
        """Scan/range query over keys."""
        return iter([])

    async def batch_set(self, items: Dict[Any, Any]) -> None:
        """Set multiple key-value pairs in a batch."""
        pass

    async def batch_delete(self, keys: List[Any]) -> int:
        """Delete multiple keys in a batch. Returns number deleted."""
        return 0

    async def get_stats(self) -> Dict[str, Any]:
        """Get backend statistics."""
        return {}

    async def backup(self, path) -> None:
        """Create a backup at the given path."""
        pass

    async def restore(self, path) -> None:
        """Restore from backup at the given path."""
        pass

    # Transaction support (optional)
    async def begin_transaction(self):
        """Begin a transaction (optional)."""
        raise NotImplementedError("Transactions not supported by this backend")

    async def commit_transaction(self, transaction) -> None:
        """Commit a transaction (optional)."""
        raise NotImplementedError("Transactions not supported by this backend")

    async def rollback_transaction(self, transaction) -> None:
        """Rollback a transaction (optional)."""
        raise NotImplementedError("Transactions not supported by this backend")


# Backwards compatibility alias
FastStoreBackend = StoreBackend


cdef class StoreTransaction:
    """Base transaction class for atomic operations."""
    # Attributes declared in .pxd file

    def __cinit__(self, StoreBackend backend):
        self._backend = backend
        self._operations = []

    async def get(self, key):
        """Get within transaction."""
        # Default implementation - backends can override
        return await self._backend.get(key)

    async def set(self, key, value) -> None:
        """Set within transaction."""
        self._operations.append(('set', key, value))

    async def delete(self, key) -> bool:
        """Delete within transaction."""
        self._operations.append(('delete', key, None))
        return True

    cpdef list get_operations(self):
        """Get all operations in this transaction."""
        return self._operations[:]


# Backwards compatibility alias
FastStoreTransaction = StoreTransaction


# Factory functions
def create_backend(backend_spec, **kwargs):
    """Factory function to create a backend instance."""
    cdef:
        BackendConfig config
        str backend_type
        str path = ""

    # Convert backend_spec to str if needed
    backend_spec = str(backend_spec)

    # Parse backend spec like "memory://" or "redis://host:port"
    if "://" in backend_spec:
        parts = backend_spec.split("://", 1)
        backend_type = parts[0]
        if len(parts) > 1:
            path = parts[1]
    else:
        backend_type = backend_spec

    # Create config
    config = BackendConfig(
        backend_type=backend_type,
        path=path,
        max_size=kwargs.get('max_size', -1),
        ttl_seconds=kwargs.get('ttl_seconds', -1.0),
        compression=kwargs.get('compression', ''),
        options=kwargs.get('options', {})
    )

    # Create backend instance
    if backend_type == "memory":
        return MemoryBackend(config)
    else:
        raise ValueError(f"Unknown backend type: {backend_type}. Only 'memory' backend is implemented in stores_base.pyx")


# Backwards compatibility alias
create_fast_backend = create_backend


# Import and register implementations
cdef class MemoryBackend(StoreBackend):
    """In-memory storage backend."""

    cdef:
        unordered_map[cpp_string, PyObject*] _data
        uint64_t _size

    def __cinit__(self, BackendConfig config):
        super().__init__(config)
        self._size = 0

    async def start(self) -> None:
        """Initialize the memory backend."""
        pass

    async def stop(self) -> None:
        """Clean up the memory backend."""
        async with self._lock:
            self._data.clear()
            self._size = 0

    async def get(self, key) -> Optional[Any]:
        """Get a value by key."""
        cdef cpp_string c_key
        cdef unordered_map[cpp_string, PyObject*].iterator it

        async with self._lock:
            c_key = str(key).encode('utf-8')
            it = self._data.find(c_key)
            if it != self._data.end():
                # Dereference and return Python object
                return <object>deref(it).second
            return None

    async def set(self, key, value) -> None:
        """Set a value by key."""
        cdef cpp_string c_key
        cdef PyObject* py_value
        cdef unordered_map[cpp_string, PyObject*].iterator it

        async with self._lock:
            c_key = str(key).encode('utf-8')
            py_value = <PyObject*>value

            # Increment reference count
            Py_INCREF(<object>py_value)

            # Check if key already exists
            it = self._data.find(c_key)
            if it != self._data.end():
                # Decrement old value's refcount
                Py_DECREF(<object>deref(it).second)
            else:
                self._size += 1

            self._data[c_key] = py_value

    async def delete(self, key) -> bool:
        """Delete a value by key."""
        cdef cpp_string c_key
        cdef unordered_map[cpp_string, PyObject*].iterator it

        async with self._lock:
            c_key = str(key).encode('utf-8')
            it = self._data.find(c_key)
            if it != self._data.end():
                Py_DECREF(<object>deref(it).second)
                self._data.erase(it)
                self._size -= 1
                return True
            return False

    async def exists(self, key) -> bool:
        """Check if a key exists."""
        cdef cpp_string c_key

        async with self._lock:
            c_key = str(key).encode('utf-8')
            return self._data.find(c_key) != self._data.end()

    async def size(self) -> int:
        """Get number of items stored."""
        async with self._lock:
            return self._size

    async def clear(self) -> None:
        """Clear all data."""
        cdef unordered_map[cpp_string, PyObject*].iterator it

        async with self._lock:
            # Decrement all reference counts
            it = self._data.begin()
            while it != self._data.end():
                Py_DECREF(<object>deref(it).second)
                inc(it)

            self._data.clear()
            self._size = 0

    async def keys(self, prefix: Optional[str] = None) -> List[Any]:
        """Get all keys, optionally filtered by prefix."""
        cdef list result
        cdef unordered_map[cpp_string, PyObject*].iterator it
        cdef str key_str

        async with self._lock:
            result = []
            it = self._data.begin()

            while it != self._data.end():
                key_str = deref(it).first.decode('utf-8')
                if prefix is None or key_str.startswith(prefix):
                    result.append(key_str)
                inc(it)

            return result

    async def get_stats(self) -> Dict[str, Any]:
        """Get backend statistics."""
        async with self._lock:
            return {
                'backend_type': 'memory',
                'size': self._size,
                'memory_usage_bytes': self._size * 256,  # Rough estimate
                'max_size': self._config.get_max_size(),
                'ttl_seconds': self._config.get_ttl_seconds(),
            }


# Backwards compatibility aliases
FastMemoryBackend = MemoryBackend
UltraFastMemoryBackend = MemoryBackend  # Also support the old "ultra" name
