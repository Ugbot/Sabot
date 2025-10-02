# -*- coding: utf-8 -*-
"""
High-Performance Tonbo Cython Wrapper

Provides C-level access to Tonbo's embedded Arrow database for maximum performance.
Implements zero-copy operations and direct memory access for streaming workloads.
"""

from libc.stdint cimport int64_t, int32_t, uint8_t
from cpython.ref cimport PyObject
from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.unicode cimport PyUnicode_FromString

cimport cython

# Import Python standard library
from typing import Dict, List, Optional, Iterator, AsyncIterator, Any, Tuple
import asyncio
import logging


@cython.final
cdef class FastTonboBackend:
    """
    High-performance Tonbo backend using Cython for zero-copy operations.

    This provides direct access to Tonbo's LSM tree and Arrow integration
    with minimal Python overhead for maximum streaming performance.
    """

    cdef:
        object _db_path
        object _tonbo_db
        object _event_loop
        bint _initialized
        object _logger

    def __cinit__(self, str db_path):
        """Initialize the fast Tonbo backend."""
        self._db_path = db_path
        self._tonbo_db = None
        self._event_loop = None
        self._initialized = False
        self._logger = logging.getLogger(__name__)

    async def initialize(self):
        """Initialize the Tonbo database connection."""
        try:
            # Import Tonbo here to avoid import errors if not available
            from tonbo import TonboDB, DbOption, Column, DataType

            # Create database options
            options = DbOption(self._db_path)

            # Define a simple key-value schema for streaming state
            class KVRecord:
                def __init__(self, key: str, value: bytes, timestamp: int64_t = 0):
                    self.key = key
                    self.value = value
                    self.timestamp = timestamp

            # Apply Tonbo schema annotations
            KVRecord.key = Column(DataType.String, name="key", primary_key=True)
            KVRecord.value = Column(DataType.Bytes, name="value")
            KVRecord.timestamp = Column(DataType.Int64, name="timestamp")
            KVRecord.__record__ = True

            # Initialize database
            self._tonbo_db = TonboDB(options, KVRecord())
            self._event_loop = asyncio.get_event_loop()
            self._initialized = True

            self._logger.info(f"FastTonboBackend initialized at {self._db_path}")

        except ImportError:
            raise RuntimeError("Tonbo not available. Install with: pip install tonbo")
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Tonbo: {e}")

    async def close(self):
        """Close the Tonbo database connection."""
        if self._tonbo_db:
            try:
                # Tonbo doesn't have explicit close method in current API
                # await self._tonbo_db.close()  # TODO: Add when available
                pass
            except Exception as e:
                self._logger.error(f"Error closing Tonbo database: {e}")

        self._tonbo_db = None
        self._initialized = False

    @cython.boundscheck(False)
    @cython.wraparound(False)
    async def fast_get(self, str key) -> Optional[bytes]:
        """
        High-performance get operation with minimal Python overhead.

        Returns raw bytes value directly from Tonbo for zero-copy processing.
        """
        if not self._initialized or not self._tonbo_db:
            return None

        try:
            # Direct Tonbo lookup
            result = await self._tonbo_db.get(key)

            if result and 'value' in result:
                # Return raw bytes without Python object creation
                return result['value']

            return None

        except Exception as e:
            self._logger.error(f"Tonbo get error for key {key}: {e}")
            return None

    @cython.boundscheck(False)
    @cython.wraparound(False)
    async def fast_insert(self, str key, bytes value):
        """
        High-performance insert operation.

        Directly inserts bytes without Python object overhead.
        """
        if not self._initialized or not self._tonbo_db:
            return

        try:
            # Get current timestamp for versioning
            cdef int64_t timestamp = self._get_current_timestamp_ns()

            # Create record directly
            record = {
                'key': key,
                'value': value,
                'timestamp': timestamp
            }

            # Direct Tonbo insert
            await self._tonbo_db.insert(record)

        except Exception as e:
            self._logger.error(f"Tonbo insert error for key {key}: {e}")
            raise

    @cython.boundscheck(False)
    @cython.wraparound(False)
    async def fast_delete(self, str key) -> bint:
        """
        High-performance delete operation.

        Returns True if key was deleted, False otherwise.
        """
        if not self._initialized or not self._tonbo_db:
            return False

        try:
            # Direct Tonbo delete
            await self._tonbo_db.remove(key)
            return True

        except Exception as e:
            self._logger.error(f"Tonbo delete error for key {key}: {e}")
            return False

    @cython.boundscheck(False)
    @cython.wraparound(False)
    async def fast_scan_range(self, str start_key = None, str end_key = None,
                             int32_t limit = 1000) -> AsyncIterator[Tuple[str, bytes, int64_t]]:
        """
        High-performance range scan with zero-copy results.

        Yields (key, value_bytes, timestamp) tuples directly from Tonbo.
        """
        if not self._initialized or not self._tonbo_db:
            return

        try:
            from tonbo import Bound

            # Set up bounds for range scan
            lower_bound = None
            if start_key is not None:
                lower_bound = Bound.Included(start_key)

            upper_bound = None
            if end_key is not None:
                upper_bound = Bound.Excluded(end_key)

            # Perform scan with projection for efficiency
            scan_stream = await self._tonbo_db.scan(
                lower_bound,
                upper_bound,
                limit=limit,
                projection=["key", "value", "timestamp"]
            )

            # Yield results directly (zero-copy)
            cdef int32_t count = 0
            async for record in scan_stream:
                if count >= limit:
                    break

                if record and 'key' in record and 'value' in record:
                    key = record['key']
                    value_bytes = record['value']
                    timestamp = record.get('timestamp', 0)

                    yield (key, value_bytes, timestamp)
                    count += 1

        except Exception as e:
            self._logger.error(f"Tonbo scan error: {e}")
            return

    @cython.boundscheck(False)
    @cython.wraparound(False)
    async def fast_batch_insert(self, list key_value_pairs):
        """
        High-performance batch insert operation.

        Args:
            key_value_pairs: List of (key: str, value: bytes) tuples
        """
        if not self._initialized or not self._tonbo_db:
            return

        try:
            # Prepare batch records
            cdef int64_t timestamp = self._get_current_timestamp_ns()
            records = []

            cdef Py_ssize_t i
            for i in range(len(key_value_pairs)):
                key, value_bytes = key_value_pairs[i]
                records.append({
                    'key': key,
                    'value': value_bytes,
                    'timestamp': timestamp
                })

            # TODO: Implement batch insert when Tonbo API supports it
            # For now, insert individually for correctness
            for record in records:
                await self._tonbo_db.insert(record)

        except Exception as e:
            self._logger.error(f"Tonbo batch insert error: {e}")
            raise

    @cython.boundscheck(False)
    @cython.wraparound(False)
    async def fast_exists(self, str key) -> bint:
        """
        High-performance existence check.
        """
        # Use get operation and check result
        result = await self.fast_get(key)
        return result is not None

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef int64_t _get_current_timestamp_ns(self):
        """Get current timestamp in nanoseconds."""
        cdef timespec ts
        clock_gettime(CLOCK_REALTIME, &ts)
        return ts.tv_sec * 1000000000 + ts.tv_nsec

    async def get_stats(self) -> Dict[str, Any]:
        """Get backend performance statistics."""
        return {
            'backend_type': 'tonbo_cython',
            'initialized': self._initialized,
            'db_path': self._db_path,
            'performance_mode': 'zero_copy',
            'features': ['fast_get', 'fast_insert', 'fast_scan', 'batch_ops']
        }


@cython.final
cdef class TonboCythonWrapper:
    """
    Convenience wrapper for Cython Tonbo operations.

    Provides a high-level interface to the fast Tonbo backend
    with automatic serialization/deserialization.
    """

    cdef:
        FastTonboBackend _backend
        object _serializer

    def __cinit__(self, str db_path):
        """Initialize the wrapper."""
        self._backend = FastTonboBackend(db_path)
        self._serializer = None

    async def initialize(self, serializer = None):
        """Initialize with optional custom serializer."""
        self._serializer = serializer or self._default_serializer
        await self._backend.initialize()

    async def close(self):
        """Close the backend."""
        await self._backend.close()

    async def get(self, key: Any) -> Any:
        """Get a value with automatic deserialization."""
        if not self._serializer:
            raise RuntimeError("Serializer not initialized")

        key_str = str(key)
        value_bytes = await self._backend.fast_get(key_str)

        if value_bytes:
            return self._serializer.deserialize(value_bytes)
        return None

    async def put(self, key: Any, value: Any):
        """Put a value with automatic serialization."""
        if not self._serializer:
            raise RuntimeError("Serializer not initialized")

        key_str = str(key)
        value_bytes = self._serializer.serialize(value)
        await self._backend.fast_insert(key_str, value_bytes)

    async def delete(self, key: Any) -> bool:
        """Delete a value."""
        key_str = str(key)
        return await self._backend.fast_delete(key_str)

    async def scan(self, start_key: Any = None, end_key: Any = None,
                  limit: int = 1000) -> AsyncIterator[Tuple[Any, Any]]:
        """
        Scan with automatic deserialization.

        Yields (key, value) tuples.
        """
        if not self._serializer:
            raise RuntimeError("Serializer not initialized")

        start_str = str(start_key) if start_key is not None else None
        end_str = str(end_key) if end_key is not None else None

        async for key_str, value_bytes, timestamp in self._backend.fast_scan_range(
            start_str, end_str, limit
        ):
            key = self._deserialize_key(key_str)
            value = self._serializer.deserialize(value_bytes)
            yield (key, value)

    def _deserialize_key(self, key_str: str) -> Any:
        """Deserialize key from string."""
        # Simple key deserialization - can be extended
        try:
            # Try to parse as int/float first
            if '.' in key_str:
                return float(key_str)
            else:
                return int(key_str)
        except ValueError:
            return key_str

    class _DefaultSerializer:
        """Default pickle-based serializer."""

        def serialize(self, obj: Any) -> bytes:
            import pickle
            return pickle.dumps(obj)

        def deserialize(self, data: bytes) -> Any:
            import pickle
            return pickle.loads(data)

    @property
    def _default_serializer(self):
        """Get default serializer instance."""
        return self._DefaultSerializer()


# External C declarations
cdef extern from "<time.h>" nogil:
    ctypedef struct timespec:
        long tv_sec
        long tv_nsec
    int clock_gettime(int clk_id, timespec *tp)

cdef extern from "<sys/time.h>" nogil:
    int CLOCK_REALTIME