# cython: language_level=3, boundscheck=False, wraparound=False
"""
SpillableWindowBuffer - Bigger-than-memory window state.

Replaces the in-memory WindowBuffer with a spillable version that:
1. Tracks memory usage per window
2. Spills individual windows to MarbleDB when threshold exceeded
3. Reads back from MarbleDB during aggregation

This enables window operations on 100GB-1TB datasets with bounded memory.
"""

import cython
from typing import Dict, List, Optional, Iterator
from libc.stdint cimport int64_t, uint64_t
from libcpp cimport bool as cbool
import logging
import os
import uuid
import time

logger = logging.getLogger(__name__)

# Import Arrow
try:
    from sabot import cyarrow as pa
    ARROW_AVAILABLE = True
except ImportError:
    try:
        import pyarrow as pa
        ARROW_AVAILABLE = True
    except ImportError:
        ARROW_AVAILABLE = False
        pa = None

# Import MarbleDB
try:
    import marbledb
    MARBLEDB_AVAILABLE = True
except ImportError:
    MARBLEDB_AVAILABLE = False
    marbledb = None

# Default configuration
DEFAULT_WINDOW_MEMORY_THRESHOLD = 64 * 1024 * 1024  # 64MB per window
DEFAULT_TOTAL_MEMORY_THRESHOLD = 256 * 1024 * 1024   # 256MB total


cdef class WindowState:
    """State for a single window - in-memory or spilled."""

    cdef public:
        uint64_t window_id
        double start_time
        double end_time
        uint64_t record_count
        double last_update
        bint is_complete

    cdef:
        # Batch storage
        list _batches           # In-memory batches
        int64_t _memory_bytes   # Memory usage estimate

        # Spill state
        bint _is_spilled
        str _spill_table_name
        object _marbledb        # MarbleDB handle for spilled data

    def __init__(self, uint64_t window_id, double start_time, double end_time):
        self.window_id = window_id
        self.start_time = start_time
        self.end_time = end_time
        self.record_count = 0
        self.last_update = time.time()
        self.is_complete = False

        self._batches = []
        self._memory_bytes = 0
        self._is_spilled = False
        self._spill_table_name = ""
        self._marbledb = None

    cpdef void add_batch(self, object batch):
        """Add a batch to this window."""
        if batch is None or batch.num_rows == 0:
            return

        self._batches.append(batch)
        self._memory_bytes += batch.nbytes
        self.record_count += batch.num_rows
        self.last_update = time.time()

    cpdef int64_t memory_bytes(self):
        """Get memory usage in bytes."""
        return self._memory_bytes

    cpdef bint is_spilled(self):
        """Check if window has been spilled to disk."""
        return self._is_spilled

    cpdef void spill_to_marbledb(self, object db, str table_name):
        """Spill in-memory batches to MarbleDB."""
        if not self._batches or self._is_spilled:
            return

        self._marbledb = db
        self._spill_table_name = table_name

        # Write all batches to MarbleDB
        cdef object batch
        for batch in self._batches:
            if batch is not None and batch.num_rows > 0:
                db.insert_batch(table_name, batch)

        # Clear in-memory batches
        self._batches = []
        self._memory_bytes = 0
        self._is_spilled = True

        logger.debug(f"Window {self.window_id}: spilled to MarbleDB table {table_name}")

    cpdef list get_batches(self):
        """Get all batches for this window (reads from MarbleDB if spilled)."""
        if not self._is_spilled:
            return list(self._batches)

        # Read back from MarbleDB
        if self._marbledb is None:
            return []

        try:
            result = self._marbledb.scan_table(self._spill_table_name)
            table = result.to_table()
            return table.to_batches()
        except Exception as e:
            logger.error(f"Failed to read spilled window {self.window_id}: {e}")
            return []

    cpdef object get_table(self):
        """Get all data as a single Arrow Table."""
        batches = self.get_batches()
        if not batches:
            return None
        return pa.Table.from_batches(batches)


cdef class SpillableWindowBuffer:
    """
    Window buffer that spills to MarbleDB when memory threshold exceeded.

    Memory management:
    - Each window tracks its own memory usage
    - When total memory exceeds threshold, oldest/largest windows spill
    - On aggregation, reads back from MarbleDB as needed
    """

    cdef:
        dict _windows                    # window_id -> WindowState
        list _window_order               # Order for eviction (oldest first)
        uint64_t _next_window_id

        # Memory management
        int64_t _total_memory_bytes
        int64_t _per_window_threshold
        int64_t _total_threshold

        # MarbleDB for spill
        object _marbledb
        str _db_path
        str _buffer_id
        bint _db_initialized
        bint _closed

    def __init__(self,
                 int64_t per_window_threshold=DEFAULT_WINDOW_MEMORY_THRESHOLD,
                 int64_t total_threshold=DEFAULT_TOTAL_MEMORY_THRESHOLD,
                 str db_path="/tmp/sabot_window_spill"):
        self._windows = {}
        self._window_order = []
        self._next_window_id = 0

        self._total_memory_bytes = 0
        self._per_window_threshold = per_window_threshold
        self._total_threshold = total_threshold

        self._marbledb = None
        self._db_path = db_path
        self._buffer_id = f"window_{uuid.uuid4().hex[:8]}"
        self._db_initialized = False
        self._closed = False

        logger.debug(
            f"SpillableWindowBuffer created: per_window={per_window_threshold/(1024*1024):.1f}MB, "
            f"total={total_threshold/(1024*1024):.1f}MB"
        )

    cpdef WindowState create_window(self, double start_time, double end_time):
        """Create a new window."""
        cdef uint64_t window_id = self._next_window_id
        self._next_window_id += 1

        cdef WindowState window = WindowState(window_id, start_time, end_time)
        self._windows[window_id] = window
        self._window_order.append(window_id)

        return window

    cpdef WindowState get_window(self, uint64_t window_id):
        """Get window by ID."""
        return self._windows.get(window_id)

    cpdef void add_batch_to_window(self, uint64_t window_id, object batch):
        """Add batch to window, triggering spill if needed."""
        cdef WindowState window = self._windows.get(window_id)
        if window is None:
            return

        # Add batch
        window.add_batch(batch)

        # Update total memory
        cdef int64_t batch_bytes = batch.nbytes if batch is not None else 0
        self._total_memory_bytes += batch_bytes

        # Check if we need to spill this window
        if window.memory_bytes() >= self._per_window_threshold:
            self._spill_window(window)

        # Check if total memory exceeded
        if self._total_memory_bytes >= self._total_threshold:
            self._spill_oldest_windows()

    cdef void _init_marbledb(self):
        """Initialize MarbleDB for spilling."""
        if self._db_initialized or not MARBLEDB_AVAILABLE:
            return

        spill_dir = os.path.join(self._db_path, self._buffer_id)
        os.makedirs(spill_dir, exist_ok=True)

        self._marbledb = marbledb.open_database(spill_dir)
        self._db_initialized = True

        logger.debug(f"SpillableWindowBuffer: initialized MarbleDB at {spill_dir}")

    cdef void _spill_window(self, WindowState window):
        """Spill a single window to MarbleDB."""
        if window.is_spilled():
            return

        if not MARBLEDB_AVAILABLE:
            logger.warning("MarbleDB not available, cannot spill window")
            return

        # Initialize MarbleDB if needed
        if not self._db_initialized:
            self._init_marbledb()

        # Create unique table for this window
        table_name = f"window_{window.window_id}"

        # Update memory tracking before spill
        cdef int64_t freed_bytes = window.memory_bytes()

        # Spill the window
        window.spill_to_marbledb(self._marbledb, table_name)

        # Update total memory
        self._total_memory_bytes -= freed_bytes

        logger.debug(
            f"SpillableWindowBuffer: spilled window {window.window_id}, "
            f"freed {freed_bytes/(1024*1024):.1f}MB"
        )

    cdef void _spill_oldest_windows(self):
        """Spill oldest windows until under threshold."""
        cdef uint64_t window_id
        cdef WindowState window

        for window_id in self._window_order:
            if self._total_memory_bytes < self._total_threshold:
                break

            window = self._windows.get(window_id)
            if window is not None and not window.is_spilled():
                self._spill_window(window)

    cpdef list get_expired_windows(self, double current_time):
        """Get windows that have expired."""
        cdef list expired = []
        cdef uint64_t window_id
        cdef WindowState window

        for window_id in self._window_order:
            window = self._windows.get(window_id)
            if window is not None and window.end_time < current_time:
                expired.append(window_id)

        return expired

    cpdef void mark_complete(self, uint64_t window_id):
        """Mark a window as complete."""
        cdef WindowState window = self._windows.get(window_id)
        if window is not None:
            window.is_complete = True

    cpdef object aggregate_window(self, uint64_t window_id, dict aggregations):
        """
        Aggregate a window's data and return result batch.

        Reads from MarbleDB if window was spilled.
        """
        cdef WindowState window = self._windows.get(window_id)
        if window is None:
            return None

        # Get all data (reads from MarbleDB if spilled)
        table = window.get_table()
        if table is None or table.num_rows == 0:
            return None

        # Apply aggregations using Arrow group_by
        # For window aggregation, typically no grouping key
        agg_list = []
        for output_name, (column, func) in aggregations.items():
            agg_list.append((column, func))

        try:
            result = table.group_by([]).aggregate(agg_list)
            return result.to_batches()[0] if result.num_rows > 0 else None
        except Exception as e:
            logger.error(f"Error aggregating window {window_id}: {e}")
            return None

    cpdef void remove_window(self, uint64_t window_id):
        """Remove a window and free resources."""
        cdef WindowState window = self._windows.pop(window_id, None)
        if window is None:
            return

        # Update memory if not spilled
        if not window.is_spilled():
            self._total_memory_bytes -= window.memory_bytes()

        # Remove from order
        try:
            self._window_order.remove(window_id)
        except ValueError:
            pass

        # Note: MarbleDB table cleanup happens on close()

    cpdef uint64_t size(self):
        """Get number of active windows."""
        return len(self._windows)

    cpdef int64_t total_memory_bytes(self):
        """Get total in-memory bytes."""
        return self._total_memory_bytes

    cpdef dict get_stats(self):
        """Get buffer statistics."""
        cdef int spilled_count = 0
        cdef WindowState window

        for window in self._windows.values():
            if window.is_spilled():
                spilled_count += 1

        return {
            'active_windows': len(self._windows),
            'spilled_windows': spilled_count,
            'memory_bytes': self._total_memory_bytes,
            'memory_mb': self._total_memory_bytes / (1024 * 1024),
            'db_initialized': self._db_initialized,
        }

    cpdef void close(self):
        """Close buffer and cleanup resources."""
        if self._closed:
            return

        self._closed = True

        # Close MarbleDB
        if self._marbledb is not None:
            try:
                self._marbledb.close()
            except:
                pass

            # Cleanup spill directory
            import shutil
            spill_dir = os.path.join(self._db_path, self._buffer_id)
            try:
                if os.path.exists(spill_dir):
                    shutil.rmtree(spill_dir)
            except:
                pass

        # Clear windows
        self._windows = {}
        self._window_order = []
        self._total_memory_bytes = 0

        logger.debug(f"SpillableWindowBuffer {self._buffer_id}: closed")

    def __dealloc__(self):
        if not self._closed:
            self.close()


# ============================================================================
# Spillable Window Processor
# ============================================================================

cdef class SpillableTumblingWindowProcessor:
    """
    Tumbling window processor with bigger-than-memory support.

    Uses SpillableWindowBuffer to handle windows that exceed memory.
    """

    cdef:
        SpillableWindowBuffer _buffer
        double _window_size_seconds
        double _current_window_start
        uint64_t _current_window_id
        dict _aggregations
        bint _initialized
        str _timestamp_field

    def __init__(self,
                 double window_size_seconds,
                 dict aggregations=None,
                 int64_t per_window_threshold=DEFAULT_WINDOW_MEMORY_THRESHOLD,
                 int64_t total_threshold=DEFAULT_TOTAL_MEMORY_THRESHOLD,
                 str timestamp_field="timestamp"):
        self._buffer = SpillableWindowBuffer(
            per_window_threshold=per_window_threshold,
            total_threshold=total_threshold
        )
        self._window_size_seconds = window_size_seconds
        self._current_window_start = 0.0
        self._current_window_id = 0
        self._aggregations = aggregations or {}
        self._initialized = False
        self._timestamp_field = timestamp_field

    cpdef object process_batch(self, object batch):
        """
        Process a batch through tumbling windows.

        Returns completed window results, if any.
        """
        if batch is None or batch.num_rows == 0:
            return None

        cdef double current_time = time.time()
        cdef WindowState window
        cdef list completed = []

        # Initialize first window
        if not self._initialized:
            self._current_window_start = current_time
            window = self._buffer.create_window(
                self._current_window_start,
                self._current_window_start + self._window_size_seconds
            )
            self._current_window_id = window.window_id
            self._initialized = True

        # Check if current window is complete
        window = self._buffer.get_window(self._current_window_id)
        while window is not None and current_time >= window.end_time:
            # Mark complete and aggregate
            window.is_complete = True
            result = self._buffer.aggregate_window(
                self._current_window_id,
                self._aggregations
            )
            if result is not None:
                completed.append(result)

            # Remove old window
            self._buffer.remove_window(self._current_window_id)

            # Start new window
            self._current_window_start = window.end_time
            window = self._buffer.create_window(
                self._current_window_start,
                self._current_window_start + self._window_size_seconds
            )
            self._current_window_id = window.window_id

        # Add batch to current window
        self._buffer.add_batch_to_window(self._current_window_id, batch)

        # Return completed windows (if any)
        if completed:
            return pa.Table.from_batches(completed)
        return None

    def __iter__(self):
        """Iterate - returns self for use in stream."""
        return self

    cpdef object flush(self):
        """Flush current window (for end of stream)."""
        if not self._initialized:
            return None

        result = self._buffer.aggregate_window(
            self._current_window_id,
            self._aggregations
        )

        return result

    cpdef dict get_stats(self):
        """Get processor statistics."""
        return {
            'window_size_seconds': self._window_size_seconds,
            'buffer': self._buffer.get_stats(),
        }

    cpdef void close(self):
        """Close processor and cleanup."""
        self._buffer.close()


# Factory functions
def create_spillable_window_buffer(**kwargs):
    """Create a SpillableWindowBuffer."""
    return SpillableWindowBuffer(**kwargs)


def create_spillable_tumbling_window(window_size_seconds, aggregations=None, **kwargs):
    """Create a spillable tumbling window processor."""
    return SpillableTumblingWindowProcessor(
        window_size_seconds,
        aggregations,
        **kwargs
    )
