# cython: language_level=3
"""Cython-optimized memory store backend for high-performance in-memory operations."""

from libc.stdint cimport uint64_t, int64_t
from libc.string cimport memcpy, memset
from libcpp.string cimport string as cpp_string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from cpython.ref cimport PyObject, Py_INCREF, Py_DECREF
from cpython.bytes cimport PyBytes_FromStringAndSize
from cython.operator cimport dereference as deref, preincrement as inc

from typing import Any, Dict, List, Optional

from .stores_base cimport StoreBackend, BackendConfig, StoreTransaction, FastStoreBackend, FastStoreConfig, FastStoreTransaction


cdef class MemoryTransaction(StoreTransaction):
    """Memory transaction with change tracking."""

    cdef:
        unordered_map[cpp_string, PyObject*] _changes
        vector[cpp_string] _deletions

    def __cinit__(self, StoreBackend backend):
        super().__init__(backend)

    async def get(self, key):
        """Get within transaction."""
        cdef cpp_string c_key = str(key).encode('utf-8')

        # Check if deleted in this transaction
        for i in range(self._deletions.size()):
            if self._deletions[i] == c_key:
                return None

        # Check if changed in this transaction
        cdef unordered_map[cpp_string, PyObject*].iterator it = self._changes.find(c_key)
        if it != self._changes.end():
            return <object>deref(it).second

        # Fall back to backend
        return await self._backend.get(key)

    async def set(self, key, value) -> None:
        """Set within transaction."""
        cdef cpp_string c_key = str(key).encode('utf-8')
        cdef PyObject* py_value = <PyObject*>value

        Py_INCREF(<object>py_value)

        # Remove from deletions if present
        cdef vector[cpp_string].iterator del_it
        cdef size_t i = 0
        while i < self._deletions.size():
            if self._deletions[i] == c_key:
                self._deletions.erase(self._deletions.begin() + i)
                break
            i += 1

        # Check if already changed
        cdef unordered_map[cpp_string, PyObject*].iterator change_it = self._changes.find(c_key)
        if change_it != self._changes.end():
            Py_DECREF(<object>deref(change_it).second)

        self._changes[c_key] = py_value
        self._operations.append(('set', key, value))

    async def delete(self, key) -> bool:
        """Delete within transaction."""
        cdef cpp_string c_key = str(key).encode('utf-8')

        # Remove from changes if present
        cdef unordered_map[cpp_string, PyObject*].iterator it = self._changes.find(c_key)
        if it != self._changes.end():
            Py_DECREF(<object>deref(it).second)
            self._changes.erase(it)

        # Add to deletions
        self._deletions.push_back(c_key)
        self._operations.append(('delete', key, None))

        return True


cdef class OptimizedMemoryBackend(StoreBackend):
    """Optimized Cython memory backend with zero-copy operations."""

    cdef:
        unordered_map[cpp_string, PyObject*] _data
        uint64_t _size
        uint64_t _hits
        uint64_t _misses

    def __cinit__(self, BackendConfig config):
        super().__init__(config)
        self._size = 0
        self._hits = 0
        self._misses = 0

    async def start(self) -> None:
        """Initialize the memory backend."""
        pass

    async def stop(self) -> None:
        """Clean up the memory backend."""
        async with self._lock:
            self._data.clear()
            self._size = 0
            self._hits = 0
            self._misses = 0

    async def get(self, key) -> Optional[Any]:
        """Ultra-fast get operation."""
        cdef cpp_string c_key
        cdef unordered_map[cpp_string, PyObject*].iterator it

        async with self._lock:
            c_key = str(key).encode('utf-8')
            it = self._data.find(c_key)

            if it != self._data.end():
                self._hits += 1
                # Zero-copy return - just return the Python object reference
                return <object>deref(it).second
            else:
                self._misses += 1
                return None

    async def set(self, key, value) -> None:
        """Ultra-fast set operation."""
        cdef cpp_string c_key
        cdef PyObject* py_value
        cdef unordered_map[cpp_string, PyObject*].iterator it

        async with self._lock:
            c_key = str(key).encode('utf-8')
            py_value = <PyObject*>value

            # Increment reference count for zero-copy storage
            Py_INCREF(<object>py_value)

            # Check if key already exists
            it = self._data.find(c_key)
            if it != self._data.end():
                # Decrement old value's refcount
                Py_DECREF(<object>deref(it).second)
            else:
                self._size += 1

            # Store with zero-copy
            self._data[c_key] = py_value

    async def delete(self, key) -> bool:
        """Ultra-fast delete operation."""
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
        """Ultra-fast exists check."""
        cdef cpp_string c_key

        async with self._lock:
            c_key = str(key).encode('utf-8')
            return self._data.find(c_key) != self._data.end()

    async def keys(self, prefix: Optional[str] = None) -> List[Any]:
        """Get all keys with optional prefix filtering."""
        cdef list result
        cdef cpp_string c_prefix
        cdef unordered_map[cpp_string, PyObject*].iterator it
        cdef str key_str

        async with self._lock:
            result = []
            c_prefix = prefix.encode('utf-8') if prefix else cpp_string()
            it = self._data.begin()

            while it != self._data.end():
                key_str = deref(it).first.decode('utf-8')
                if prefix is None or key_str.startswith(prefix):
                    result.append(key_str)
                inc(it)

            return result

    async def values(self) -> List[Any]:
        """Get all values."""
        cdef list result
        cdef unordered_map[cpp_string, PyObject*].iterator it

        async with self._lock:
            result = []
            it = self._data.begin()

            while it != self._data.end():
                result.append(<object>deref(it).second)
                inc(it)

            return result

    async def items(self, prefix: Optional[str] = None) -> List[tuple]:
        """Get all key-value pairs with optional prefix filtering."""
        cdef list result
        cdef cpp_string c_prefix
        cdef unordered_map[cpp_string, PyObject*].iterator it
        cdef str key_str

        async with self._lock:
            result = []
            c_prefix = prefix.encode('utf-8') if prefix else cpp_string()
            it = self._data.begin()

            while it != self._data.end():
                key_str = deref(it).first.decode('utf-8')
                if prefix is None or key_str.startswith(prefix):
                    result.append((key_str, <object>deref(it).second))
                inc(it)

            return result

    async def clear(self) -> None:
        """Clear all data with proper cleanup."""
        cdef unordered_map[cpp_string, PyObject*].iterator it

        async with self._lock:
            # Properly decrement all reference counts
            it = self._data.begin()
            while it != self._data.end():
                Py_DECREF(<object>deref(it).second)
                inc(it)

            self._data.clear()
            self._size = 0
            self._hits = 0
            self._misses = 0

    async def size(self) -> int:
        """Get number of items stored."""
        async with self._lock:
            return self._size

    async def batch_set(self, items: Dict[Any, Any]) -> None:
        """Batch set multiple items for high throughput."""
        cdef cpp_string c_key
        cdef PyObject* py_value
        cdef unordered_map[cpp_string, PyObject*].iterator it

        async with self._lock:
            for key, value in items.items():
                c_key = str(key).encode('utf-8')
                py_value = <PyObject*>value

                Py_INCREF(<object>py_value)

                it = self._data.find(c_key)
                if it != self._data.end():
                    Py_DECREF(<object>deref(it).second)
                else:
                    self._size += 1

                self._data[c_key] = py_value

    async def batch_delete(self, keys: List[Any]) -> int:
        """Batch delete multiple keys."""
        cdef int deleted
        cdef cpp_string c_key
        cdef unordered_map[cpp_string, PyObject*].iterator it

        async with self._lock:
            deleted = 0
            for key in keys:
                c_key = str(key).encode('utf-8')
                it = self._data.find(c_key)

                if it != self._data.end():
                    Py_DECREF(<object>deref(it).second)
                    self._data.erase(it)
                    self._size -= 1
                    deleted += 1

            return deleted

    async def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive backend statistics."""
        cdef double hit_rate

        async with self._lock:
            hit_rate = 0.0
            if self._hits + self._misses > 0:
                hit_rate = float(self._hits) / float(self._hits + self._misses)

            return {
                'backend_type': 'ultra_fast_memory',
                'size': self._size,
                'hits': self._hits,
                'misses': self._misses,
                'hit_rate': hit_rate,
                'memory_usage_bytes': self._size * 256,  # Rough estimate per entry
                'max_size': self._config.get_max_size(),
                'ttl_seconds': self._config.get_ttl_seconds(),
            }

    async def begin_transaction(self):
        """Begin a transaction."""
        return MemoryTransaction(self)

    async def commit_transaction(self, transaction: MemoryTransaction) -> None:
        """Commit a transaction."""
        cdef unordered_map[cpp_string, PyObject*].iterator change_it
        cdef unordered_map[cpp_string, PyObject*].iterator txn_it
        cdef cpp_string c_key
        cdef size_t i

        async with self._lock:
            # Apply changes - iterate through transaction's changes map
            txn_it = transaction._changes.begin()
            while txn_it != transaction._changes.end():
                c_key = deref(txn_it).first
                change_it = self._data.find(c_key)
                if change_it != self._data.end():
                    Py_DECREF(<object>deref(change_it).second)
                else:
                    self._size += 1
                self._data[c_key] = deref(txn_it).second
                inc(txn_it)

            # Apply deletions
            for i in range(transaction._deletions.size()):
                c_key = transaction._deletions[i]
                change_it = self._data.find(c_key)
                if change_it != self._data.end():
                    Py_DECREF(<object>deref(change_it).second)
                    self._data.erase(change_it)
                    self._size -= 1

    async def rollback_transaction(self, transaction: MemoryTransaction) -> None:
        """Rollback a transaction (no-op since changes aren't applied until commit)."""
        # Clean up transaction's reference counts
        cdef unordered_map[cpp_string, PyObject*].iterator it

        it = transaction._changes.begin()
        while it != transaction._changes.end():
            Py_DECREF(<object>deref(it).second)
            inc(it)
        transaction._changes.clear()
        transaction._deletions.clear()


# Backwards compatibility aliases
UltraFastMemoryBackend = OptimizedMemoryBackend
FastMemoryBackend = OptimizedMemoryBackend
FastMemoryTransaction = MemoryTransaction
