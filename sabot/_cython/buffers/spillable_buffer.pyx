# -*- coding: utf-8 -*-
# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
"""
SpillableBuffer - Memory-managed buffer that spills Arrow batches to MarbleDB.

Provides transparent bigger-than-memory support for streaming operators.
When memory threshold is exceeded, batches are spilled to MarbleDB using
the Arrow batch API (InsertBatch/iter_batches).

Usage:
    buffer = SpillableBuffer(
        buffer_id="groupby_1",
        memory_threshold_bytes=256 * 1024 * 1024,  # 256MB
        db_path="/tmp/sabot_spill"
    )

    # Append batches (spills automatically when threshold exceeded)
    for batch in input_stream:
        buffer.append(batch)

    # Iterate (transparently reads from memory + disk)
    for batch in buffer:
        process(batch)

    buffer.close()  # Cleanup spill files
"""

import logging
import os
import shutil
import threading
import uuid
from typing import Iterator, Optional

from libc.stdint cimport int64_t

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_MEMORY_THRESHOLD = 256 * 1024 * 1024  # 256MB
DEFAULT_SPILL_PATH = "/tmp/sabot_spill"


cdef class SpillableBuffer:
    """Memory-managed buffer that spills Arrow batches to MarbleDB.

    Two access patterns supported:
    1. Append-only: batches appended sequentially (for aggregations, windows)
    2. Iteration: yields all batches (memory first, then spilled)

    Memory tracking uses Arrow batch.nbytes for accurate accounting.
    Spill uses MarbleDB's InsertBatch() for zero-copy Arrow storage.
    Read-back uses iter_batches() for streaming access.
    """

    def __cinit__(self):
        """Initialize C-level attributes."""
        self._memory_used_bytes = 0
        self._memory_threshold_bytes = DEFAULT_MEMORY_THRESHOLD
        self._in_memory_batches = []
        self._in_memory_count = 0
        self._has_spilled = False
        self._spill_table_name = ""
        self._spill_store = None
        self._spilled_batch_count = 0
        self._schema = None
        self._buffer_id = ""
        self._db_path = DEFAULT_SPILL_PATH
        self._lock = None
        self._closed = False

    def __init__(
        self,
        str buffer_id = "",
        int64_t memory_threshold_bytes = DEFAULT_MEMORY_THRESHOLD,
        str db_path = DEFAULT_SPILL_PATH,
        object schema = None
    ):
        """Initialize SpillableBuffer.

        Args:
            buffer_id: Unique identifier for this buffer (used in spill table name)
            memory_threshold_bytes: Memory threshold before spilling (default 256MB)
            db_path: Path for MarbleDB spill storage
            schema: Optional PyArrow schema (inferred from first batch if not provided)
        """
        self._buffer_id = buffer_id or f"spill_{uuid.uuid4().hex[:8]}"
        self._memory_threshold_bytes = memory_threshold_bytes
        self._db_path = db_path
        self._schema = schema
        self._lock = threading.Lock()

        logger.debug(
            f"SpillableBuffer created: id={self._buffer_id}, "
            f"threshold={self._memory_threshold_bytes / (1024*1024):.1f}MB"
        )

    cpdef void append(self, object batch):
        """Append batch with memory tracking and automatic spilling.

        Args:
            batch: PyArrow RecordBatch to append

        The batch is added to in-memory storage unless:
        1. Memory threshold would be exceeded -> trigger spill first
        2. Already spilling -> write directly to MarbleDB
        """
        if self._closed:
            raise RuntimeError("SpillableBuffer is closed")

        if batch is None:
            return

        cdef int64_t batch_size

        # Infer schema from first batch
        if self._schema is None:
            self._schema = batch.schema

        # Get batch memory size
        batch_size = batch.nbytes if hasattr(batch, 'nbytes') else 0

        with self._lock:
            # Check if spill needed BEFORE adding
            if not self._has_spilled and \
               self._memory_used_bytes + batch_size > self._memory_threshold_bytes:
                logger.info(
                    f"SpillableBuffer {self._buffer_id}: triggering spill at "
                    f"{self._memory_used_bytes / (1024*1024):.1f}MB "
                    f"(threshold: {self._memory_threshold_bytes / (1024*1024):.1f}MB)"
                )
                self._trigger_spill()

            # Add to appropriate location
            if self._has_spilled:
                # Already spilling - go directly to disk
                self._spill_batch(batch)
            else:
                # Still in memory
                self._in_memory_batches.append(batch)
                self._memory_used_bytes += batch_size
                self._in_memory_count += 1

    cdef void _trigger_spill(self):
        """Spill all in-memory batches to MarbleDB."""
        if self._has_spilled:
            return  # Already spilling

        # Initialize MarbleDB table for this buffer
        self._init_spill_store()

        # Spill all current in-memory batches
        cdef object batch
        for batch in self._in_memory_batches:
            self._spill_batch(batch)

        # Clear in-memory storage
        self._in_memory_batches = []
        self._memory_used_bytes = 0
        self._has_spilled = True

        logger.info(
            f"SpillableBuffer {self._buffer_id}: spilled {self._spilled_batch_count} batches"
        )

    cdef void _init_spill_store(self):
        """Initialize MarbleDB storage for spillable buffer."""
        # Generate unique table name
        self._spill_table_name = f"spill_{self._buffer_id}_{uuid.uuid4().hex[:8]}"

        # Create spill directory if needed
        spill_dir = os.path.join(self._db_path, self._buffer_id)
        os.makedirs(spill_dir, exist_ok=True)

        # Use MarbleDB Python bindings (more reliable than Cython wrapper)
        try:
            import marbledb
            self._spill_store = marbledb.open_database(spill_dir)

            # Create table with schema
            if self._schema is not None:
                self._spill_store.create_table(self._spill_table_name, self._schema)

            logger.debug(
                f"SpillableBuffer {self._buffer_id}: initialized spill store at {spill_dir}"
            )

        except ImportError:
            # Fallback to Cython wrapper if marbledb module not available
            from sabot._cython.stores.marbledb_store import MarbleDBStoreBackend
            self._spill_store = MarbleDBStoreBackend(spill_dir, {})
            self._spill_store.open()

            if self._schema is not None:
                self._spill_store.create_column_family(self._spill_table_name, self._schema)

        except Exception as e:
            logger.error(f"Failed to initialize spill store: {e}")
            raise

    cdef void _spill_batch(self, object batch):
        """Write single batch to MarbleDB."""
        if self._spill_store is None:
            raise RuntimeError("Spill store not initialized")

        try:
            # Use appropriate method based on store type
            if hasattr(self._spill_store, 'insert_batch'):
                # MarbleDB Python bindings
                self._spill_store.insert_batch(self._spill_table_name, batch)
            else:
                # Cython wrapper
                self._spill_store.insert_batch(self._spill_table_name, batch)

            self._spilled_batch_count += 1

        except Exception as e:
            logger.error(f"Failed to spill batch: {e}")
            raise

    def __iter__(self):
        """Unified iterator: memory batches first, then spilled.

        Yields:
            PyArrow RecordBatch objects in order:
            1. All in-memory batches (fast, no I/O)
            2. All spilled batches via MarbleDB (streaming, zero-copy)
        """
        if self._closed:
            raise RuntimeError("SpillableBuffer is closed")

        # Phase 1: Yield in-memory batches
        for batch in self._in_memory_batches:
            yield batch

        # Phase 2: Yield spilled batches (lazy streaming from MarbleDB)
        if self._has_spilled and self._spill_store is not None:
            try:
                # Use scan_table which returns a QueryResult
                result = self._spill_store.scan_table(self._spill_table_name)

                # Handle different return types - prefer to_table() which filters None batches
                if hasattr(result, 'to_table'):
                    # PyQueryResult from marbledb Python bindings
                    table = result.to_table()
                    if table is not None:
                        for batch in table.to_batches():
                            yield batch
                elif hasattr(result, 'to_batches'):
                    # If it's already a Table, convert to batches
                    for batch in result.to_batches():
                        if batch is not None:
                            yield batch
                elif hasattr(result, '__iter__'):
                    # If it's iterable, filter out None batches
                    for batch in result:
                        if batch is not None:
                            yield batch

            except Exception as e:
                logger.error(f"Failed to read spilled batches: {e}")
                raise

    cpdef void clear(self):
        """Clear all data (memory and spilled)."""
        with self._lock:
            self._in_memory_batches = []
            self._memory_used_bytes = 0
            self._in_memory_count = 0

            if self._has_spilled and self._spill_store is not None:
                # Close and cleanup spill store
                try:
                    if hasattr(self._spill_store, 'close'):
                        self._spill_store.close()
                except Exception as e:
                    logger.warning(f"Error closing spill store: {e}")

                # Remove spill directory
                spill_dir = os.path.join(self._db_path, self._buffer_id)
                try:
                    if os.path.exists(spill_dir):
                        shutil.rmtree(spill_dir)
                except Exception as e:
                    logger.warning(f"Error removing spill directory: {e}")

                self._spill_store = None
                self._has_spilled = False
                self._spilled_batch_count = 0

    cpdef int64_t memory_used(self):
        """Get current in-memory usage in bytes."""
        return self._memory_used_bytes

    cpdef int64_t total_batches(self):
        """Get total number of batches (memory + spilled)."""
        return self._in_memory_count + self._spilled_batch_count

    cpdef bint is_spilled(self):
        """Check if buffer has spilled to disk."""
        return self._has_spilled

    cpdef void flush(self):
        """Flush any pending writes to MarbleDB."""
        if self._has_spilled and self._spill_store is not None:
            try:
                if hasattr(self._spill_store, 'flush'):
                    self._spill_store.flush()
            except Exception as e:
                logger.warning(f"Error flushing spill store: {e}")

    cpdef void close(self):
        """Close buffer and cleanup spill files."""
        if self._closed:
            return

        self._closed = True

        with self._lock:
            # Clear memory
            self._in_memory_batches = []
            self._memory_used_bytes = 0

            # Close and cleanup spill store
            if self._spill_store is not None:
                try:
                    if hasattr(self._spill_store, 'close'):
                        self._spill_store.close()
                except Exception as e:
                    logger.warning(f"Error closing spill store: {e}")

                # Remove spill directory
                spill_dir = os.path.join(self._db_path, self._buffer_id)
                try:
                    if os.path.exists(spill_dir):
                        shutil.rmtree(spill_dir)
                        logger.debug(f"SpillableBuffer {self._buffer_id}: cleaned up {spill_dir}")
                except Exception as e:
                    logger.warning(f"Error removing spill directory: {e}")

                self._spill_store = None

        logger.debug(f"SpillableBuffer {self._buffer_id}: closed")

    def __dealloc__(self):
        """Cleanup on deallocation."""
        if not self._closed:
            self.close()

    def __len__(self):
        """Return total number of batches."""
        return self.total_batches()

    def __repr__(self):
        return (
            f"SpillableBuffer("
            f"id={self._buffer_id}, "
            f"memory={self._memory_used_bytes / (1024*1024):.1f}MB, "
            f"threshold={self._memory_threshold_bytes / (1024*1024):.1f}MB, "
            f"batches={self.total_batches()}, "
            f"spilled={self._has_spilled})"
        )
