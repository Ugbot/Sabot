# cython: language_level=3
"""Cython-optimized windowing system for Arrow-based stream processing."""

import asyncio
from libc.stdint cimport uint64_t, int64_t
from libc.string cimport memcpy, memset
from libcpp.string cimport string as cpp_string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.deque cimport deque
from cpython.ref cimport PyObject
from cython.operator cimport dereference as deref, preincrement as inc

import time
from typing import Any, Dict, List, Optional, Callable, Iterator
from enum import Enum

# Arrow imports
cdef extern from "arrow/api.h" namespace "arrow":
    cdef cppclass CStatus:
        bint ok()
        CStatus& operator=(CStatus&)

    cdef cppclass CDataType:
        pass

    cdef cppclass CField:
        pass

    cdef cppclass CSchema:
        pass

    cdef cppclass CArray:
        pass

    cdef cppclass CRecordBatch:
        pass

    cdef cppclass CTable:
        pass

    cdef cppclass CBuffer:
        pass

    cdef cppclass CMemoryPool:
        pass

cdef extern from "arrow/compute/api.h" namespace "arrow::compute":
    cdef cppclass CFunctionOptions:
        pass

    cdef cppclass CExpression:
        pass

    cdef CStatus CallFunction "arrow::compute::CallFunction"(
        const c_string& func_name,
        const vector[CDatum]& args,
        const CFunctionOptions& options,
        CDatum* out
    )

cdef extern from "arrow/python/pyarrow.h" namespace "arrow::py":
    cdef object wrap_array "arrow::py::wrap_array"(const shared_ptr[CArray]& arr)
    cdef object wrap_table "arrow::py::wrap_table"(const shared_ptr[CTable]& table)
    cdef object wrap_record_batch "arrow::py::wrap_record_batch"(const shared_ptr[CRecordBatch]& batch)
    cdef object wrap_schema "arrow::py::wrap_schema"(const shared_ptr[CSchema]& schema)

    cdef CStatus unwrap_array "arrow::py::unwrap_array"(object obj, shared_ptr[CArray]* out)
    cdef CStatus unwrap_table "arrow::py::unwrap_table"(object obj, shared_ptr[CTable]* out)
    cdef CStatus unwrap_record_batch "arrow::py::unwrap_record_batch"(object obj, shared_ptr[CRecordBatch]* out)
    cdef CStatus unwrap_schema "arrow::py::unwrap_schema"(object obj, shared_ptr[CSchema]* out)

cdef extern from "<memory>" namespace "std":
    cdef cppclass shared_ptr[T]:
        T* get()
        bint operator bool()

cdef extern from "<vector>" namespace "std":
    cdef cppclass vector[T]:
        pass

cdef extern from "<string>" namespace "std":
    cdef cppclass c_string:
        pass

cdef extern from "<chrono>" namespace "std::chrono":
    cdef cppclass system_clock:
        pass

cdef extern from "arrow/type.h" namespace "arrow":
    cdef cppclass CDatum:
        pass

# Window types enum
class WindowType(Enum):
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    HOPPING = "hopping"
    SESSION = "session"


cdef class WindowConfig:
    """Configuration for window operations."""

    cdef:
        WindowType window_type
        double size_seconds
        double slide_seconds
        double hop_seconds
        double timeout_seconds
        cpp_string key_field
        cpp_string timestamp_field
        bint emit_empty_windows

    def __cinit__(
        self,
        WindowType window_type,
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


cdef class WindowState:
    """State for a single window instance."""

    cdef:
        uint64_t window_id
        double start_time
        double end_time
        unordered_map[cpp_string, PyObject*] data
        uint64_t record_count
        double last_update
        bint is_complete

    def __cinit__(self, uint64_t window_id, double start_time, double end_time):
        self.window_id = window_id
        self.start_time = start_time
        self.end_time = end_time
        self.record_count = 0
        self.last_update = time.time()
        self.is_complete = False

    cdef void add_record(self, cpp_string key, PyObject* value):
        """Add a record to this window."""
        Py_INCREF(value)
        cdef unordered_map[cpp_string, PyObject*].iterator it = self.data.find(key)
        if it != self.data.end():
            Py_DECREF(deref(it).second)
        self.data[key] = value
        self.record_count += 1
        self.last_update = time.time()

    cdef object get_records(self):
        """Get all records in this window as a dict."""
        cdef dict result = {}
        cdef unordered_map[cpp_string, PyObject*].iterator it = self.data.begin()
        cdef str key_str
        while it != self.data.end():
            key_str = deref(it).first.decode('utf-8')
            result[key_str] = <object>deref(it).second
            inc(it)
        return result

    cdef uint64_t size(self):
        """Get number of records in this window."""
        return self.record_count

    cdef void clear(self):
        """Clear all data from this window."""
        cdef unordered_map[cpp_string, PyObject*].iterator it = self.data.begin()
        while it != self.data.end():
            Py_DECREF(deref(it).second)
            inc(it)
        self.data.clear()
        self.record_count = 0

    def __dealloc__(self):
        """Clean up when window is destroyed."""
        self.clear()


cdef class WindowBuffer:
    """Buffer for managing multiple windows efficiently."""

    cdef:
        unordered_map[uint64_t, WindowState*] windows
        deque[uint64_t] window_order
        uint64_t max_windows
        uint64_t next_window_id

    def __cinit__(self, uint64_t max_windows = 10000):
        self.max_windows = max_windows
        self.next_window_id = 0

    cdef WindowState* create_window(self, double start_time, double end_time):
        """Create a new window."""
        cdef uint64_t window_id = self.next_window_id
        cdef uint64_t old_id
        cdef WindowState* window
        cdef unordered_map[uint64_t, WindowState*].iterator it

        self.next_window_id += 1

        window = new WindowState(window_id, start_time, end_time)
        self.windows[window_id] = window
        self.window_order.push_back(window_id)

        # Evict old windows if we exceed max
        while self.window_order.size() > self.max_windows:
            old_id = self.window_order.front()
            self.window_order.pop_front()
            it = self.windows.find(old_id)
            if it != self.windows.end():
                deref(it).second.clear()
                del deref(it).second
                self.windows.erase(it)

        return window

    cdef WindowState* get_window(self, uint64_t window_id):
        """Get a window by ID."""
        cdef unordered_map[uint64_t, WindowState*].iterator it = self.windows.find(window_id)
        if it != self.windows.end():
            return deref(it).second
        return NULL

    cdef vector[uint64_t] get_expired_windows(self, double current_time):
        """Get windows that have expired."""
        cdef vector[uint64_t] expired
        cdef unordered_map[uint64_t, WindowState*].iterator it = self.windows.begin()
        while it != self.windows.end():
            cdef WindowState* window = deref(it).second
            if window.end_time < current_time:
                expired.push_back(deref(it).first)
            inc(it)
        return expired

    cdef void remove_window(self, uint64_t window_id):
        """Remove a window."""
        cdef unordered_map[uint64_t, WindowState*].iterator it = self.windows.find(window_id)
        if it != self.windows.end():
            deref(it).second.clear()
            del deref(it).second
            self.windows.erase(it)

            # Remove from order deque
            cdef deque[uint64_t].iterator order_it = self.window_order.begin()
            while order_it != self.window_order.end():
                if deref(order_it) == window_id:
                    self.window_order.erase(order_it)
                    break
                inc(order_it)

    cdef uint64_t size(self):
        """Get number of active windows."""
        return self.windows.size()

    def __dealloc__(self):
        """Clean up all windows."""
        cdef unordered_map[uint64_t, WindowState*].iterator it = self.windows.begin()
        while it != self.windows.end():
            deref(it).second.clear()
            del deref(it).second
            inc(it)
        self.windows.clear()


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

    cdef object _compute_aggregations(self, object records):
        """Compute aggregations using Arrow."""
        if not self.aggregations or not records:
            return records

        try:
            import pyarrow as pa
            import pyarrow.compute as pc

            # Convert records to Arrow table
            if isinstance(records, dict):
                table = pa.Table.from_pydict(records)
            elif hasattr(records, 'to_arrow'):
                table = records.to_arrow()
            else:
                return records

            # Apply aggregations
            result = {}
            for col_name, agg_func in self.aggregations.items():
                if col_name in table.column_names:
                    column = table.column(col_name)
                    if agg_func == 'sum':
                        result[col_name] = pc.sum(column).as_py()
                    elif agg_func == 'mean':
                        result[col_name] = pc.mean(column).as_py()
                    elif agg_func == 'count':
                        result[col_name] = len(column)
                    elif agg_func == 'min':
                        result[col_name] = pc.min(column).as_py()
                    elif agg_func == 'max':
                        result[col_name] = pc.max(column).as_py()

            return result

        except ImportError:
            # Fallback if Arrow not available
            return records


cdef class TumblingWindowProcessor(BaseWindowProcessor):
    """Tumbling window processor - fixed-size, non-overlapping windows."""

    cdef:
        double current_window_start
        WindowState* current_window

    def __cinit__(self, WindowConfig config, dict aggregations = None):
        super().__init__(config, aggregations)
        self.current_window_start = 0.0
        self.current_window = NULL

    async def process_record(self, object record_batch):
        """Process records through tumbling windows."""
        cdef double current_time = time.time()

        # Initialize first window if needed
        if self.current_window_start == 0.0:
            self.current_window_start = current_time
            self.current_window = self.buffer.create_window(
                self.current_window_start,
                self.current_window_start + self.config.size_seconds
            )

        # Check if we need to start a new window
        if current_time >= self.current_window.end_time:
            # Mark current window as complete
            if self.current_window != NULL:
                self.current_window.is_complete = True

            # Start new window
            self.current_window_start = current_time
            self.current_window = self.buffer.create_window(
                self.current_window_start,
                self.current_window_start + self.config.size_seconds
            )

        # Add records to current window
        await self._add_records_to_window(record_batch, self.current_window)

    cdef async def _add_records_to_window(self, object record_batch, WindowState* window):
        """Add records to a specific window."""
        if window == NULL:
            return

        # Extract records (assuming list of dicts or Arrow batch)
        records = []
        if hasattr(record_batch, 'to_pylist'):
            # Arrow RecordBatch
            records = record_batch.to_pylist()
        elif isinstance(record_batch, list):
            records = record_batch
        else:
            records = [record_batch]

        # Add each record to window
        for record in records:
            if isinstance(record, dict):
                # Use key field for grouping
                key = record.get(self.config.key_field.decode('utf-8'), 'default')
                window.add_record(str(key).encode('utf-8'), <PyObject*>record)


cdef class SlidingWindowProcessor(BaseWindowProcessor):
    """Sliding window processor - fixed-size, overlapping windows."""

    cdef:
        double last_window_start
        unordered_map[uint64_t, WindowState*] active_windows

    def __cinit__(self, WindowConfig config, dict aggregations = None):
        super().__init__(config, aggregations)
        self.last_window_start = 0.0

    async def process_record(self, object record_batch):
        """Process records through sliding windows."""
        cdef double current_time = time.time()

        # Initialize if needed
        if self.last_window_start == 0.0:
            self.last_window_start = current_time

        # Create new windows as needed
        while self.last_window_start <= current_time:
            cdef WindowState* window = self.buffer.create_window(
                self.last_window_start,
                self.last_window_start + self.config.size_seconds
            )
            self.active_windows[window.window_id] = window
            self.last_window_start += self.config.slide_seconds

        # Add records to all active windows
        cdef unordered_map[uint64_t, WindowState*].iterator it = self.active_windows.begin()
        while it != self.active_windows.end():
            await self._add_records_to_window(record_batch, deref(it).second)
            inc(it)

        # Clean up expired windows
        cdef vector[uint64_t] expired = self.buffer.get_expired_windows(current_time)
        for i in range(expired.size()):
            cdef uint64_t window_id = expired[i]
            self.active_windows.erase(window_id)
            self.buffer.remove_window(window_id)

    cdef async def _add_records_to_window(self, object record_batch, WindowState* window):
        """Add records to a specific window."""
        if window == NULL:
            return

        # Similar to tumbling window implementation
        records = []
        if hasattr(record_batch, 'to_pylist'):
            records = record_batch.to_pylist()
        elif isinstance(record_batch, list):
            records = record_batch
        else:
            records = [record_batch]

        for record in records:
            if isinstance(record, dict):
                key = record.get(self.config.key_field.decode('utf-8'), 'default')
                window.add_record(str(key).encode('utf-8'), <PyObject*>record)


cdef class HoppingWindowProcessor(BaseWindowProcessor):
    """Hopping window processor - fixed-size windows with custom hop."""

    cdef:
        double last_window_start

    def __cinit__(self, WindowConfig config, dict aggregations = None):
        super().__init__(config, aggregations)
        self.last_window_start = 0.0

    async def process_record(self, object record_batch):
        """Process records through hopping windows."""
        cdef double current_time = time.time()

        if self.last_window_start == 0.0:
            self.last_window_start = current_time

        # Create windows with hop-based spacing
        while self.last_window_start <= current_time:
            cdef WindowState* window = self.buffer.create_window(
                self.last_window_start,
                self.last_window_start + self.config.size_seconds
            )
            self.last_window_start += self.config.hop_seconds

            # Add records to this window
            await self._add_records_to_window(record_batch, window)


cdef class SessionWindowProcessor(BaseWindowProcessor):
    """Session window processor - variable-size windows based on activity gaps."""

    cdef:
        unordered_map[cpp_string, WindowState*] session_windows
        double session_timeout

    def __cinit__(self, WindowConfig config, dict aggregations = None):
        super().__init__(config, aggregations)
        self.session_timeout = config.timeout_seconds

    async def process_record(self, object record_batch):
        """Process records through session windows."""
        records = []
        if hasattr(record_batch, 'to_pylist'):
            records = record_batch.to_pylist()
        elif isinstance(record_batch, list):
            records = record_batch
        else:
            records = [record_batch]

        cdef double current_time = time.time()

        for record in records:
            if isinstance(record, dict):
                key = record.get(self.config.key_field.decode('utf-8'), 'default')
                cdef cpp_string session_key = str(key).encode('utf-8')

                # Get or create session window
                cdef WindowState* window = NULL
                cdef unordered_map[cpp_string, WindowState*].iterator it = self.session_windows.find(session_key)

                if it != self.session_windows.end():
                    window = deref(it).second
                    # Check if session has timed out
                    if current_time - window.last_update > self.session_timeout:
                        # Close old session and start new one
                        window.is_complete = True
                        window = self.buffer.create_window(current_time, current_time + self.session_timeout)
                        self.session_windows[session_key] = window
                else:
                    # Create new session window
                    window = self.buffer.create_window(current_time, current_time + self.session_timeout)
                    self.session_windows[session_key] = window

                # Add record to session
                window.add_record(session_key, <PyObject*>record)
                window.end_time = current_time + self.session_timeout  # Extend session

    async def emit_windows(self):
        """Emit completed session windows."""
        cdef double current_time = time.time()
        cdef vector[cpp_string] to_remove

        cdef unordered_map[cpp_string, WindowState*].iterator it = self.session_windows.begin()
        while it != self.session_windows.end():
            cdef WindowState* window = deref(it).second
            if window.is_complete or (current_time - window.last_update > self.session_timeout):
                # Emit this session window
                window.is_complete = True
                # In a real implementation, you'd yield the window results here

            if window.is_complete:
                to_remove.push_back(deref(it).first)

            inc(it)

        # Clean up completed sessions
        for i in range(to_remove.size()):
            self.session_windows.erase(to_remove[i])


# Factory function for creating window processors
cpdef object create_window_processor(str window_type, double size_seconds, **kwargs):
    """Create a window processor instance."""
    cdef WindowType wt
    cdef WindowConfig config

    if window_type == "tumbling":
        wt = WindowType.TUMBLING
        config = WindowConfig(wt, size_seconds, **kwargs)
        return TumblingWindowProcessor(config, kwargs.get('aggregations'))
    elif window_type == "sliding":
        wt = WindowType.SLIDING
        slide_seconds = kwargs.get('slide_seconds', size_seconds / 2.0)
        config = WindowConfig(wt, size_seconds, slide_seconds=slide_seconds, **kwargs)
        return SlidingWindowProcessor(config, kwargs.get('aggregations'))
    elif window_type == "hopping":
        wt = WindowType.HOPPING
        hop_seconds = kwargs.get('hop_seconds', size_seconds / 2.0)
        config = WindowConfig(wt, size_seconds, hop_seconds=hop_seconds, **kwargs)
        return HoppingWindowProcessor(config, kwargs.get('aggregations'))
    elif window_type == "session":
        wt = WindowType.SESSION
        timeout_seconds = kwargs.get('timeout_seconds', 30.0)
        config = WindowConfig(wt, size_seconds, timeout_seconds=timeout_seconds, **kwargs)
        return SessionWindowProcessor(config, kwargs.get('aggregations'))
    else:
        raise ValueError(f"Unknown window type: {window_type}")


# Window manager for coordinating multiple windows
cdef class WindowManager:
    """Manager for multiple window processors."""

    cdef:
        unordered_map[cpp_string, BaseWindowProcessor] processors
        object lock

    def __cinit__(self):
        self.lock = asyncio.Lock()

    async def create_window(self, str name, str window_type, double size_seconds, **kwargs):
        """Create a new window processor."""
        async with self.lock:
            cdef cpp_string c_name = name.encode('utf-8')
            cdef BaseWindowProcessor processor = create_window_processor(window_type, size_seconds, **kwargs)
            self.processors[c_name] = processor
            return processor

    async def process_record(self, str window_name, object record_batch):
        """Process a record batch through a specific window."""
        async with self.lock:
            cdef cpp_string c_name = window_name.encode('utf-8')
            cdef unordered_map[cpp_string, BaseWindowProcessor].iterator it = self.processors.find(c_name)
            if it != self.processors.end():
                await deref(it).second.process_record(record_batch)

    async def get_window_stats(self, str window_name = None):
        """Get statistics for windows."""
        async with self.lock:
            if window_name:
                cdef cpp_string c_name = window_name.encode('utf-8')
                cdef unordered_map[cpp_string, BaseWindowProcessor].iterator it = self.processors.find(c_name)
                if it != self.processors.end():
                    return await deref(it).second.get_window_stats()
            else:
                # Return stats for all windows
                cdef dict all_stats = {}
                cdef unordered_map[cpp_string, BaseWindowProcessor].iterator it = self.processors.begin()
                while it != self.processors.end():
                    cdef str name = deref(it).first.decode('utf-8')
                    all_stats[name] = await deref(it).second.get_window_stats()
                    inc(it)
                return all_stats

    async def emit_all_windows(self):
        """Emit all completed windows."""
        async with self.lock:
            cdef unordered_map[cpp_string, BaseWindowProcessor].iterator it = self.processors.begin()
            while it != self.processors.end():
                await deref(it).second.emit_windows()
                inc(it)


# Global window manager instance
cdef WindowManager _global_window_manager = WindowManager()

cpdef WindowManager get_window_manager():
    """Get the global window manager instance."""
    return _global_window_manager
