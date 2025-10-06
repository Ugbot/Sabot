# cython: language_level=3
"""Cython-optimized windowing system for Arrow-based stream processing."""

import asyncio
from libc.stdint cimport uint64_t, int64_t
from libc.stddef cimport size_t
from libc.string cimport memcpy, memset
from libcpp.string cimport string as cpp_string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.deque cimport deque
from libcpp.memory cimport shared_ptr
from libcpp cimport bool as cbool
from cython.operator cimport dereference as deref, preincrement as inc

import time
from typing import Any, Dict, List, Optional, Callable, Iterator
from enum import Enum

# Arrow C++ forward declarations - minimal to avoid import conflicts
cdef extern from "arrow/api.h" namespace "arrow" nogil:
    cdef cppclass CRecordBatch "arrow::RecordBatch":
        int64_t num_rows()
        const shared_ptr[CSchema]& schema()

    cdef cppclass CSchema "arrow::Schema":
        pass

    cdef cppclass CTable "arrow::Table":
        pass

# Window types enum
class WindowType(Enum):
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    HOPPING = "hopping"
    SESSION = "session"


# Pure C++ struct for window metadata - no Python objects
cdef extern from * nogil:
    """
    struct WindowMetadata {
        uint64_t window_id;
        double start_time;
        double end_time;
        uint64_t record_count;
        double last_update;
        bool is_complete;

        WindowMetadata() : window_id(0), start_time(0.0), end_time(0.0),
                          record_count(0), last_update(0.0), is_complete(false) {}
    };
    """
    cdef struct WindowMetadata:
        uint64_t window_id
        double start_time
        double end_time
        uint64_t record_count
        double last_update
        cbool is_complete


cdef class WindowConfig:
    """Configuration for window operations."""

    cdef:
        object window_type  # WindowType enum
        double size_seconds
        double slide_seconds
        double hop_seconds
        double timeout_seconds
        cpp_string key_field
        cpp_string timestamp_field
        bint emit_empty_windows

    def __cinit__(
        self,
        object window_type,  # WindowType enum
        double size_seconds,
        double slide_seconds = -1.0,
        double hop_seconds = -1.0,
        double timeout_seconds = -1.0,
        str key_field = "key",
        str timestamp_field = "timestamp",
        bint emit_empty_windows = False
    ):
        self.window_type = window_type
        self.size_seconds = size_seconds
        self.slide_seconds = slide_seconds if slide_seconds > 0 else size_seconds
        self.hop_seconds = hop_seconds if hop_seconds > 0 else size_seconds
        self.timeout_seconds = timeout_seconds
        self.key_field = key_field.encode('utf-8')
        self.timestamp_field = timestamp_field.encode('utf-8')
        self.emit_empty_windows = emit_empty_windows


# C++ struct for batch storage - inline definition
cdef extern from * nogil:
    """
    #include <vector>
    #include <memory>

    struct WindowRecordBatches {
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        std::shared_ptr<arrow::Schema> schema;
        uint64_t total_rows;

        WindowRecordBatches() : total_rows(0) {}

        void add_batch(const std::shared_ptr<arrow::RecordBatch>& batch) {
            if (batches.empty() && batch) {
                schema = batch->schema();
            }
            if (batch) {
                batches.push_back(batch);
                total_rows += batch->num_rows();
            }
        }

        void clear() {
            batches.clear();
            total_rows = 0;
        }
    };
    """
    cdef cppclass WindowRecordBatches:
        vector[shared_ptr[CRecordBatch]] batches
        shared_ptr[CSchema] schema
        uint64_t total_rows

        WindowRecordBatches()
        void add_batch(const shared_ptr[CRecordBatch]& batch)
        void clear()


cdef class WindowBuffer:
    """Pure C++ window buffer - metadata in struct map, batches separate."""

    cdef:
        unordered_map[uint64_t, WindowMetadata] _metadata
        unordered_map[uint64_t, WindowRecordBatches] _batches  # Store by value
        deque[uint64_t] _window_order
        uint64_t _max_windows
        uint64_t _next_id

    def __cinit__(self, uint64_t max_windows=10000):
        self._max_windows = max_windows
        self._next_id = 0

    cdef WindowMetadata* create_window(self, double start_time, double end_time):
        """Create new window - returns pointer to metadata struct in map."""
        cdef WindowMetadata meta
        cdef uint64_t wid = self._next_id
        self._next_id += 1

        # Initialize metadata struct
        meta.window_id = wid
        meta.start_time = start_time
        meta.end_time = end_time
        meta.record_count = 0
        meta.last_update = time.time()
        meta.is_complete = False

        # Store in maps (stored by value, C++ will call constructor)
        self._metadata[wid] = meta
        self._batches[wid] = WindowRecordBatches()
        self._window_order.push_back(wid)

        # Evict old windows if needed
        self._evict_old_windows()

        return &self._metadata[wid]

    cdef void _evict_old_windows(self):
        """Evict windows beyond max_windows limit."""
        cdef uint64_t old_id

        while self._window_order.size() > self._max_windows:
            old_id = self._window_order.front()
            self._window_order.pop_front()

            # Erase from maps (C++ will call destructor automatically)
            self._batches.erase(old_id)
            self._metadata.erase(old_id)

    cdef void add_batch_to_window(self, uint64_t window_id, const shared_ptr[CRecordBatch]& batch):
        """Add RecordBatch to window."""
        cdef unordered_map[uint64_t, WindowRecordBatches].iterator it
        cdef WindowMetadata* meta

        it = self._batches.find(window_id)
        if it != self._batches.end():
            # Add batch to storage (access map value directly)
            deref(it).second.add_batch(batch)

            # Update metadata
            meta = &self._metadata[window_id]
            meta.record_count += batch.get().num_rows()
            meta.last_update = time.time()

    cdef WindowRecordBatches* get_window_batches(self, uint64_t window_id):
        """Get batch storage for window (return pointer to map value)."""
        cdef unordered_map[uint64_t, WindowRecordBatches].iterator it
        it = self._batches.find(window_id)
        if it != self._batches.end():
            return &deref(it).second
        return NULL

    cdef WindowMetadata* get_window_metadata(self, uint64_t window_id):
        """Get metadata for window."""
        cdef unordered_map[uint64_t, WindowMetadata].iterator it
        it = self._metadata.find(window_id)
        if it != self._metadata.end():
            return &deref(it).second
        return NULL

    cdef vector[uint64_t] get_expired_windows(self, double current_time):
        """Get windows that have expired."""
        cdef vector[uint64_t] expired
        cdef unordered_map[uint64_t, WindowMetadata].iterator it
        cdef WindowMetadata* meta

        it = self._metadata.begin()
        while it != self._metadata.end():
            meta = &deref(it).second
            if meta.end_time < current_time:
                expired.push_back(deref(it).first)
            inc(it)
        return expired

    cdef void remove_window(self, uint64_t window_id):
        """Remove a window."""
        cdef deque[uint64_t].iterator order_it

        # Erase from maps (C++ will call destructors)
        self._batches.erase(window_id)
        self._metadata.erase(window_id)

        # Remove from order deque
        order_it = self._window_order.begin()
        while order_it != self._window_order.end():
            if deref(order_it) == window_id:
                self._window_order.erase(order_it)
                break
            inc(order_it)

    cdef uint64_t size(self):
        """Get number of active windows."""
        return self._metadata.size()

    def __dealloc__(self):
        """Clean up all windows - C++ maps will call destructors automatically."""
        pass


cdef class BaseWindowProcessor:
    """Base class for window processors with Arrow compute integration."""

    cdef:
        WindowConfig config
        WindowBuffer buffer
        object aggregations
        bint is_active

    def __cinit__(self, WindowConfig config, dict aggregations = None):
        self.config = config
        self.buffer = WindowBuffer()
        self.aggregations = aggregations or {}
        self.is_active = True

    async def process_record(self, object record_batch):
        """Process a batch of records through the window."""
        # This will be implemented by subclasses
        pass

    async def emit_windows(self):
        """Emit completed windows."""
        # This will be implemented by subclasses
        pass

    async def get_window_stats(self):
        """Get window statistics."""
        return {
            'active_windows': self.buffer.size(),
            'config': {
                'window_type': self.config.window_type.value,
                'size_seconds': self.config.size_seconds,
                'slide_seconds': self.config.slide_seconds,
                'hop_seconds': self.config.hop_seconds,
            }
        }


cdef class TumblingWindowProcessor(BaseWindowProcessor):
    """Tumbling window processor - fixed-size, non-overlapping windows."""

    cdef:
        double _current_window_start
        uint64_t _current_window_id

    def __cinit__(self, WindowConfig config, dict aggregations = None):
        super().__init__(config, aggregations)
        self._current_window_start = 0.0
        self._current_window_id = 0

    async def process_record(self, object record_batch):
        """Process records through tumbling windows - async wrapper for compatibility."""
        # Convert Python RecordBatch to C++ shared_ptr
        cdef shared_ptr[CRecordBatch] batch_ptr

        # For now, handle Python pyarrow.RecordBatch
        # In production, would use pyarrow C API to get shared_ptr directly
        if hasattr(record_batch, '_export_to_c'):
            # Use Arrow C Data Interface if available
            self._process_batch_internal(record_batch)
        else:
            # Fallback: just store as is for now
            self._process_batch_internal(record_batch)

    cdef void _process_batch_internal(self, object py_batch):
        """Internal batch processing - pure C++ path."""
        cdef double current_time = time.time()
        cdef WindowMetadata* window_meta
        cdef WindowMetadata* new_meta

        # Initialize first window if needed
        if self._current_window_id == 0:
            self._current_window_start = current_time
            new_meta = self.buffer.create_window(
                self._current_window_start,
                self._current_window_start + self.config.size_seconds
            )
            self._current_window_id = new_meta.window_id

        # Get current window metadata
        window_meta = self.buffer.get_window_metadata(self._current_window_id)

        # Check if we need to start a new window
        if window_meta != NULL and current_time >= window_meta.end_time:
            # Mark current window as complete
            window_meta.is_complete = True

            # Start new window
            self._current_window_start = current_time
            new_meta = self.buffer.create_window(
                self._current_window_start,
                self._current_window_start + self.config.size_seconds
            )
            self._current_window_id = new_meta.window_id

        # Add batch to current window
        # Note: In full implementation, would convert py_batch to shared_ptr[CRecordBatch]
        # For now, just update metadata
        if window_meta != NULL:
            window_meta.record_count += 1  # Placeholder


cdef class SlidingWindowProcessor(BaseWindowProcessor):
    """Sliding window processor - fixed-size, overlapping windows."""

    cdef:
        double _last_window_start
        vector[uint64_t] _active_window_ids

    def __cinit__(self, WindowConfig config, dict aggregations = None):
        super().__init__(config, aggregations)
        self._last_window_start = 0.0

    async def process_record(self, object record_batch):
        """Process records through sliding windows."""
        cdef double current_time = time.time()
        cdef WindowMetadata* window_meta
        cdef vector[uint64_t] expired
        cdef uint64_t window_id
        cdef size_t i

        # Initialize if needed
        if self._last_window_start == 0.0:
            self._last_window_start = current_time

        # Create new windows as needed
        while self._last_window_start <= current_time:
            window_meta = self.buffer.create_window(
                self._last_window_start,
                self._last_window_start + self.config.size_seconds
            )
            self._active_window_ids.push_back(window_meta.window_id)
            self._last_window_start += self.config.slide_seconds

        # Add batch to all active windows
        for i in range(self._active_window_ids.size()):
            window_id = self._active_window_ids[i]
            # In full implementation: buffer.add_batch_to_window(window_id, batch)
            window_meta = self.buffer.get_window_metadata(window_id)
            if window_meta != NULL:
                window_meta.record_count += 1  # Placeholder

        # Clean up expired windows
        cdef vector[uint64_t].iterator it
        expired = self.buffer.get_expired_windows(current_time)
        for i in range(expired.size()):
            window_id = expired[i]
            # Remove from active list
            it = self._active_window_ids.begin()
            while it != self._active_window_ids.end():
                if deref(it) == window_id:
                    self._active_window_ids.erase(it)
                    break
                inc(it)
            self.buffer.remove_window(window_id)


cdef class HoppingWindowProcessor(BaseWindowProcessor):
    """Hopping window processor - fixed-size windows with custom hop."""

    cdef:
        double last_window_start
        vector[uint64_t] _active_window_ids  # Store window IDs instead of pointers

    def __cinit__(self, WindowConfig config, dict aggregations = None):
        super().__init__(config, aggregations)
        self.last_window_start = 0.0

    async def process_record(self, object record_batch):
        """Process records through hopping windows."""
        cdef double current_time = time.time()
        cdef WindowMetadata* window_meta
        cdef uint64_t window_id

        if self.last_window_start == 0.0:
            self.last_window_start = current_time

        # Create windows with hop-based spacing
        while self.last_window_start <= current_time:
            window_meta = self.buffer.create_window(
                self.last_window_start,
                self.last_window_start + self.config.size_seconds
            )
            if window_meta != NULL:
                self._active_window_ids.push_back(window_meta.window_id)
            self.last_window_start += self.config.hop_seconds

        # Add batch to all active windows
        cdef size_t i
        cdef object py_batch
        cdef shared_ptr[CRecordBatch] arrow_batch

        for i in range(self._active_window_ids.size()):
            window_id = self._active_window_ids[i]
            window_meta = self.buffer.get_window_metadata(window_id)
            if window_meta != NULL:
                # TODO: Proper Arrow conversion - placeholder for now
                # py_batch = record_batch
                # arrow_batch = pyarrow_unwrap_batch(py_batch)
                # self.buffer.add_batch_to_window(window_id, arrow_batch)
                window_meta.record_count += 1
                window_meta.last_update = current_time


cdef class SessionWindowProcessor(BaseWindowProcessor):
    """Session window processor - variable-size windows based on activity gaps."""

    cdef:
        unordered_map[cpp_string, uint64_t] _session_to_window_id  # Map session key to window ID
        double session_timeout

    def __cinit__(self, WindowConfig config, dict aggregations = None):
        super().__init__(config, aggregations)
        self.session_timeout = config.timeout_seconds

    async def process_record(self, object record_batch):
        """Process records through session windows."""
        cdef list records = []
        cdef double current_time = time.time()
        cdef cpp_string session_key
        cdef uint64_t window_id
        cdef WindowMetadata* window_meta = NULL
        cdef unordered_map[cpp_string, uint64_t].iterator it
        cdef str key
        cdef dict record_dict

        if hasattr(record_batch, 'to_pylist'):
            records = record_batch.to_pylist()
        elif isinstance(record_batch, list):
            records = record_batch
        else:
            records = [record_batch]

        for record in records:
            if isinstance(record, dict):
                record_dict = <dict>record
                key = record_dict.get(self.config.key_field.decode('utf-8'), 'default')
                session_key = str(key).encode('utf-8')

                # Get or create session window
                it = self._session_to_window_id.find(session_key)

                if it != self._session_to_window_id.end():
                    window_id = deref(it).second
                    window_meta = self.buffer.get_window_metadata(window_id)

                    # Check if session has timed out
                    if window_meta != NULL and current_time - window_meta.last_update > self.session_timeout:
                        # Close old session and start new one
                        window_meta.is_complete = True
                        window_meta = self.buffer.create_window(current_time, current_time + self.session_timeout)
                        if window_meta != NULL:
                            self._session_to_window_id[session_key] = window_meta.window_id
                else:
                    # Create new session window
                    window_meta = self.buffer.create_window(current_time, current_time + self.session_timeout)
                    if window_meta != NULL:
                        self._session_to_window_id[session_key] = window_meta.window_id

                # Update window metadata
                if window_meta != NULL:
                    window_meta.record_count += 1
                    window_meta.last_update = current_time
                    window_meta.end_time = current_time + self.session_timeout  # Extend session

    async def emit_windows(self):
        """Emit completed session windows."""
        cdef double current_time = time.time()
        cdef vector[cpp_string] to_remove
        cdef unordered_map[cpp_string, uint64_t].iterator it
        cdef WindowMetadata* window_meta
        cdef uint64_t window_id
        cdef size_t i

        it = self._session_to_window_id.begin()
        while it != self._session_to_window_id.end():
            window_id = deref(it).second
            window_meta = self.buffer.get_window_metadata(window_id)

            if window_meta != NULL:
                if window_meta.is_complete or (current_time - window_meta.last_update > self.session_timeout):
                    # Emit this session window
                    window_meta.is_complete = True
                    # In a real implementation, you'd yield the window results here

                if window_meta.is_complete:
                    to_remove.push_back(deref(it).first)

            inc(it)

        # Clean up completed sessions
        for i in range(to_remove.size()):
            self._session_to_window_id.erase(to_remove[i])


# Factory function for creating window processors
def create_window_processor(str window_type, double size_seconds, **kwargs):
    """Create a window processor instance."""
    cdef WindowConfig config

    if window_type == "tumbling":
        config = WindowConfig(WindowType.TUMBLING, size_seconds, **kwargs)
        return TumblingWindowProcessor(config, kwargs.get('aggregations'))
    elif window_type == "sliding":
        slide_seconds = kwargs.get('slide_seconds', size_seconds / 2.0)
        config = WindowConfig(WindowType.SLIDING, size_seconds, slide_seconds=slide_seconds, **kwargs)
        return SlidingWindowProcessor(config, kwargs.get('aggregations'))
    elif window_type == "hopping":
        hop_seconds = kwargs.get('hop_seconds', size_seconds / 2.0)
        config = WindowConfig(WindowType.HOPPING, size_seconds, hop_seconds=hop_seconds, **kwargs)
        return HoppingWindowProcessor(config, kwargs.get('aggregations'))
    elif window_type == "session":
        timeout_seconds = kwargs.get('timeout_seconds', 30.0)
        config = WindowConfig(WindowType.SESSION, size_seconds, timeout_seconds=timeout_seconds, **kwargs)
        return SessionWindowProcessor(config, kwargs.get('aggregations'))
    else:
        raise ValueError(f"Unknown window type: {window_type}")


# Window manager for coordinating multiple windows
cdef class WindowManager:
    """Manager for multiple window processors."""

    cdef:
        dict _processors  # Use Python dict for Python objects
        object lock

    def __cinit__(self):
        self._processors = {}
        self.lock = asyncio.Lock()

    async def create_window(self, str name, str window_type, double size_seconds, **kwargs):
        """Create a new window processor."""
        cdef BaseWindowProcessor processor

        async with self.lock:
            processor = create_window_processor(window_type, size_seconds, **kwargs)
            self._processors[name] = processor
            return processor

    async def process_record(self, str window_name, object record_batch):
        """Process a record batch through a specific window."""
        async with self.lock:
            if window_name in self._processors:
                await self._processors[window_name].process_record(record_batch)

    async def get_window_stats(self, str window_name = None):
        """Get statistics for windows."""
        cdef dict all_stats

        async with self.lock:
            if window_name:
                if window_name in self._processors:
                    return await self._processors[window_name].get_window_stats()
            else:
                # Return stats for all windows
                all_stats = {}
                for name, processor in self._processors.items():
                    all_stats[name] = await processor.get_window_stats()
                return all_stats

    async def emit_all_windows(self):
        """Emit all completed windows."""
        async with self.lock:
            for processor in self._processors.values():
                await processor.emit_windows()


# Global window manager instance
cdef WindowManager _global_window_manager = WindowManager()

cpdef WindowManager get_window_manager():
    """Get the global window manager instance."""
    return _global_window_manager
