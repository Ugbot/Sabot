# cython: language_level=3
"""Cython-optimized join operations for Arrow-based stream processing."""

import asyncio
from libc.stdint cimport uint64_t, int64_t
from libc.string cimport memcpy, memset
from libcpp.string cimport string as cpp_string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.utility cimport pair
from cpython.ref cimport PyObject, Py_INCREF, Py_DECREF
from cython.operator cimport dereference as deref, preincrement as inc

# Type aliases - use void* instead of PyObject* for Cython compatibility
# Cast to <object><PyObject*> when reading, <void*><PyObject*> when writing
ctypedef pair[void*, double] WindowEntry
ctypedef vector[WindowEntry] WindowEntries
ctypedef pair[void*, uint64_t] VersionedEntry
ctypedef vector[VersionedEntry] VersionedEntries

import time
from typing import Any, Dict, List, Optional, Callable, AsyncIterator
from enum import Enum

# Arrow imports - commented out as not used, causes template compilation issues
# All Arrow operations use Python-level pyarrow API instead of C++ bindings
# cdef extern from "arrow/api.h" namespace "arrow":
#     cdef cppclass CStatus, CDataType, CField, CSchema, CArray, CRecordBatch, CTable, etc...
# cdef extern from "arrow/compute/api.h" namespace "arrow::compute":
#     cdef cppclass CFunctionOptions, CExpression, CHashJoinOptions, etc...

# Arrow C++ bindings commented out - these are not used in the current implementation
# All Arrow operations use Python-level pyarrow API instead
# cdef extern from "arrow/python/pyarrow.h" namespace "arrow::py":
#     cdef object wrap_array "arrow::py::wrap_array"(...)
#     etc...

# cdef extern from "<memory>" namespace "std":
#     cdef cppclass shared_ptr[T]:
#         T* get()
#         bint operator bool()

# vector is already imported from libcpp.vector above

cdef extern from "<string>" namespace "std":
    cdef cppclass c_string:
        pass

cdef extern from "<chrono>" namespace "std::chrono":
    cdef cppclass system_clock:
        pass

cdef extern from "arrow/type.h" namespace "arrow":
    cdef cppclass CDatum:
        pass


# C++ enum for high-performance join type checks (integer comparisons)
cdef enum CJoinType:
    INNER = 0
    LEFT_OUTER = 1
    RIGHT_OUTER = 2
    FULL_OUTER = 3


class JoinType(Enum):
    """Join types supported by Sabot (Flink-compatible) - Python API."""
    INNER = "inner"
    LEFT_OUTER = "left_outer"
    RIGHT_OUTER = "right_outer"
    FULL_OUTER = "full_outer"


class JoinMode(Enum):
    """Join execution modes (Flink-style)."""
    BATCH = "batch"           # Traditional batch joins
    STREAMING = "streaming"   # Streaming joins
    INTERVAL = "interval"     # Time-interval based joins
    TEMPORAL = "temporal"     # Temporal table joins
    WINDOW = "window"         # Window-based joins
    LOOKUP = "lookup"         # External system lookups


class JoinStrategy(Enum):
    """Join execution strategies."""
    HASH = "hash"
    MERGE = "merge"
    NESTED_LOOP = "nested_loop"


# Helper function to convert Python JoinType to C++ CJoinType
cdef inline CJoinType _to_c_join_type(object py_join_type):
    """Convert Python JoinType enum to C++ CJoinType for performance."""
    if py_join_type == JoinType.INNER:
        return INNER
    elif py_join_type == JoinType.LEFT_OUTER:
        return LEFT_OUTER
    elif py_join_type == JoinType.RIGHT_OUTER:
        return RIGHT_OUTER
    elif py_join_type == JoinType.FULL_OUTER:
        return FULL_OUTER
    else:
        return INNER  # Default fallback


# Helper function to convert C++ CJoinType back to string for stats/logging
cdef inline str _c_join_type_to_str(CJoinType c_join_type):
    """Convert C++ CJoinType to string for stats and logging."""
    if c_join_type == INNER:
        return "inner"
    elif c_join_type == LEFT_OUTER:
        return "left_outer"
    elif c_join_type == RIGHT_OUTER:
        return "right_outer"
    elif c_join_type == FULL_OUTER:
        return "full_outer"
    else:
        return "inner"  # Default fallback


cdef class JoinCondition:
    """Represents a join condition."""

    cdef:
        cpp_string _left_field
        cpp_string _right_field
        cpp_string _left_alias
        cpp_string _right_alias

    def __cinit__(
        self,
        str left_field,
        str right_field,
        str left_alias = "",
        str right_alias = ""
    ):
        self._left_field = left_field.encode('utf-8')
        self._right_field = right_field.encode('utf-8')
        self._left_alias = left_alias.encode('utf-8') if left_alias else cpp_string()
        self._right_alias = right_alias.encode('utf-8') if right_alias else cpp_string()

    cpdef str get_left_field(self):
        return self._left_field.decode('utf-8')

    cpdef str get_right_field(self):
        return self._right_field.decode('utf-8')


cdef class StreamTableJoinProcessor:
    """Cython-optimized stream-table join processor."""

    cdef:
        CJoinType _join_type  # C++ enum for performance
        list _conditions  # List of JoinCondition objects (Python-managed for safety)
        unordered_map[cpp_string, void*] _table_data
        object _lock
        double _last_cleanup

    def __cinit__(self, object join_type, list conditions):
        self._join_type = _to_c_join_type(join_type)
        self._lock = asyncio.Lock()
        self._last_cleanup = time.time()
        self._conditions = []

        # Convert conditions - store as Python list
        for condition in conditions:
            if isinstance(condition, dict):
                join_cond = JoinCondition(
                    condition['left_field'],
                    condition['right_field'],
                    condition.get('left_alias', ''),
                    condition.get('right_alias', '')
                )
                self._conditions.append(join_cond)
            elif isinstance(condition, JoinCondition):
                self._conditions.append(condition)

    async def load_table_data(self, table_data: Dict[str, Any]) -> None:
        """Load table data for joins."""
        cdef unordered_map[cpp_string, void*].iterator it
        cdef cpp_string c_key
        async with self._lock:
            # Clear existing data
            it = self._table_data.begin()
            while it != self._table_data.end():
                Py_DECREF(<object>deref(it).second)
                inc(it)
            self._table_data.clear()

            # Load new data
            for key, value in table_data.items():
                c_key = str(key).encode('utf-8')
                Py_INCREF(value)
                self._table_data[c_key] = <void*><PyObject*>value

    async def process_stream_record(self, object stream_record) -> List[dict]:
        """Process a stream record and return joined results."""
        cdef cpp_string c_join_key
        cdef unordered_map[cpp_string, void*].iterator it
        async with self._lock:
            results = []

            # Extract join key from stream record
            if not isinstance(stream_record, dict):
                return results

            join_key = self._extract_join_key(stream_record)
            if not join_key:
                # Handle non-inner joins (left join returns original record)
                if self._join_type in (JoinType.LEFT_OUTER, JoinType.FULL_OUTER):
                    results.append(stream_record)
                return results

            c_join_key = join_key.encode('utf-8')
            it = self._table_data.find(c_join_key)

            if it != self._table_data.end():
                # Found matching table record
                table_record = <object><PyObject*>deref(it).second
                joined_record = self._merge_records(stream_record, table_record)
                results.append(joined_record)
            else:
                # No match found
                if self._join_type in (JoinType.LEFT_OUTER, JoinType.FULL_OUTER):
                    results.append(stream_record)

            return results

    cdef str _extract_join_key(self, dict record):
        """Extract join key from record."""
        if not self._conditions == []:
            condition = self._conditions[0]  # Use first condition for key
            field_name = condition.get_right_field()  # Stream record uses right side
            return str(record.get(field_name, ''))

        return ''

    cdef dict _merge_records(self, dict stream_record, dict table_record):
        """Merge stream and table records."""
        merged = dict(stream_record)

        # Add table fields with prefix to avoid conflicts
        for key, value in table_record.items():
            if key not in merged:
                merged[f"table_{key}"] = value
            else:
                # Handle conflicts by keeping stream value and adding table value with suffix
                merged[f"table_{key}"] = value

        return merged

    async def get_stats(self) -> Dict[str, Any]:
        """Get join processor statistics."""
        async with self._lock:
            return {
                'join_type': _c_join_type_to_str(self._join_type),
                'table_size': self._table_data.size(),
                'conditions_count': len(self._conditions),
                'last_cleanup': self._last_cleanup,
            }


cdef class StreamStreamJoinProcessor:
    """Cython-optimized stream-stream join processor with windowing."""

    cdef:
        CJoinType _join_type  # C++ enum for performance
        list _conditions  # List of JoinCondition objects (Python-managed for safety)
        unordered_map[cpp_string, WindowEntries] _left_window
        unordered_map[cpp_string, WindowEntries] _right_window
        double _window_size_seconds
        object _lock
        uint64_t _max_window_size

    def __cinit__(self, object join_type, list conditions, double window_size_seconds = 300.0, uint64_t max_window_size = 10000):
        self._join_type = _to_c_join_type(join_type)
        self._window_size_seconds = window_size_seconds
        self._lock = asyncio.Lock()
        self._conditions = []
        self._max_window_size = max_window_size

        # Convert conditions - store pointers in C++ vector
        cdef JoinCondition join_cond
        for condition in conditions:
            if isinstance(condition, dict):
                join_cond = JoinCondition(
                    condition['left_field'],
                    condition['right_field'],
                    condition.get('left_alias', ''),
                    condition.get('right_alias', '')
                )
                self._conditions.append(join_cond)
            elif isinstance(condition, JoinCondition):
                self._conditions.append(condition)


    async def process_left_stream(self, object record) -> List[dict]:
        """Process a record from the left stream."""
        return await self._process_stream_record(record, True)

    async def process_right_stream(self, object record) -> List[dict]:
        """Process a record from the right stream."""
        return await self._process_stream_record(record, False)

    async def _process_stream_record(self, object record, bint is_left) -> List[dict]:
        """Process a stream record and find joins within the window."""
        cdef cpp_string c_join_key
        cdef WindowEntries *target_window
        cdef WindowEntries *other_window
        cdef size_t i

        async with self._lock:
            results = []

            if not isinstance(record, dict):
                return results

            current_time = time.time()
            join_key = self._extract_join_key(record, is_left)

            if not join_key:
                return results

            c_join_key = join_key.encode('utf-8')

            if is_left:
                target_window = &self._left_window[c_join_key]
                other_window = &self._right_window[c_join_key]
            else:
                target_window = &self._right_window[c_join_key]
                other_window = &self._left_window[c_join_key]

            # Add current record to window
            Py_INCREF(record)
            target_window.push_back(WindowEntry(<void*><PyObject*>record, current_time))

            # Clean old records from target window
            self._cleanup_window(target_window, current_time)

            # Find matches in the other window
            for i in range(other_window.size()):
                other_record = <object>(deref(other_window)[i].first)
                other_time = deref(other_window)[i].second

                # Check if within window
                if abs(current_time - other_time) <= self._window_size_seconds:
                    if is_left:
                        joined = self._merge_records(record, other_record, True)
                    else:
                        joined = self._merge_records(other_record, record, False)

                    results.append(joined)

            # Apply join semantics
            if not results and self._join_type in (JoinType.LEFT_OUTER, JoinType.FULL_OUTER) and is_left:
                results.append(record)
            elif not results and self._join_type in (JoinType.RIGHT_OUTER, JoinType.FULL_OUTER) and not is_left:
                results.append(record)

            return results

    cdef str _extract_join_key(self, dict record, bint is_left):
        """Extract join key from record."""
        if not self._conditions == []:
            condition = self._conditions[0]
            if is_left:
                field_name = condition.get_left_field()
            else:
                field_name = condition.get_right_field()
            return str(record.get(field_name, ''))

        return ''

    cdef dict _merge_records(self, dict left_record, dict right_record, bint left_is_left):
        """Merge two records with appropriate prefixes."""
        merged = {}

        # Add left record fields
        for key, value in left_record.items():
            if left_is_left:
                merged[f"left_{key}"] = value
            else:
                merged[f"right_{key}"] = value

        # Add right record fields
        for key, value in right_record.items():
            if not left_is_left:
                merged[f"left_{key}"] = value
            else:
                merged[f"right_{key}"] = value

        return merged

    cdef void _cleanup_window(self, WindowEntries *window, double current_time):
        """Clean old records from a window."""
        cdef size_t i = 0
        while i < window.size():
            record_time = deref(window)[i].second
            if current_time - record_time > self._window_size_seconds:
                # Remove old record
                Py_DECREF(<object>deref(window)[i].first)
                window.erase(window.begin() + i)
            else:
                i += 1

        # Limit window size
        while window.size() > self._max_window_size:
            Py_DECREF(<object>deref(window)[0].first)
            window.erase(window.begin())

    async def get_stats(self) -> Dict[str, Any]:
        """Get join processor statistics."""
        cdef size_t left_total
        cdef size_t right_total
        cdef unordered_map[cpp_string, WindowEntries].iterator it

        async with self._lock:
            left_total = 0

            it = self._left_window.begin()
            while it != self._left_window.end():
                left_total += deref(it).second.size()
                inc(it)

            right_total = 0
            it = self._right_window.begin()
            while it != self._right_window.end():
                right_total += deref(it).second.size()
                inc(it)

            return {
                'join_type': _c_join_type_to_str(self._join_type),
                'window_size_seconds': self._window_size_seconds,
                'left_window_size': left_total,
                'right_window_size': right_total,
                'unique_keys': self._left_window.size(),
                'max_window_size': self._max_window_size,
            }


cdef class IntervalJoinProcessor:
    """Cython-optimized interval join processor (Flink-style)."""

    cdef:
        CJoinType _join_type  # C++ enum for performance
        list _conditions  # List of JoinCondition objects (Python-managed for safety)
        unordered_map[cpp_string, WindowEntries] _left_buffer
        unordered_map[cpp_string, WindowEntries] _right_buffer
        double _left_lower_bound
        double _left_upper_bound
        double _right_lower_bound
        double _right_upper_bound
        object _lock
        uint64_t _max_buffer_size

    def __cinit__(
        self,
        object join_type,
        list conditions,
        double left_lower_bound,
        double left_upper_bound,
        double right_lower_bound,
        double right_upper_bound,
        uint64_t max_buffer_size = 10000
    ):
        self._join_type = _to_c_join_type(join_type)
        self._left_lower_bound = left_lower_bound
        self._left_upper_bound = left_upper_bound
        self._right_lower_bound = right_lower_bound
        self._right_upper_bound = right_upper_bound
        self._max_buffer_size = max_buffer_size
        self._lock = asyncio.Lock()
        self._conditions = []

        # Convert conditions - store pointers in C++ vector
        cdef JoinCondition join_cond
        for condition in conditions:
            if isinstance(condition, dict):
                join_cond = JoinCondition(
                    condition['left_field'],
                    condition['right_field'],
                    condition.get('left_alias', ''),
                    condition.get('right_alias', '')
                )
                self._conditions.append(join_cond)
            elif isinstance(condition, JoinCondition):
                self._conditions.append(condition)


    async def process_left_record(self, object record) -> List[dict]:
        """Process a record from the left stream."""
        return await self._process_record(record, True)

    async def process_right_record(self, object record) -> List[dict]:
        """Process a record from the right stream."""
        return await self._process_record(record, False)

    async def _process_record(self, object record, bint is_left) -> List[dict]:
        """Process a record and find interval joins."""
        cdef cpp_string c_join_key
        cdef WindowEntries *source_buffer
        cdef WindowEntries *target_buffer
        cdef double lower_bound, upper_bound
        cdef size_t i

        async with self._lock:
            results = []

            if not isinstance(record, dict):
                return results

            current_time = time.time()
            join_key = self._extract_join_key(record, is_left)

            if not join_key:
                return results

            c_join_key = join_key.encode('utf-8')

            if is_left:
                source_buffer = &self._left_buffer[c_join_key]
                target_buffer = &self._right_buffer[c_join_key]
                lower_bound = self._left_lower_bound
                upper_bound = self._left_upper_bound
            else:
                source_buffer = &self._right_buffer[c_join_key]
                target_buffer = &self._left_buffer[c_join_key]
                lower_bound = self._right_lower_bound
                upper_bound = self._right_upper_bound

            # Add current record to source buffer
            Py_INCREF(record)
            source_buffer.push_back(WindowEntry(<void*><PyObject*>record, current_time))

            # Clean old records
            self._cleanup_buffer(source_buffer, current_time, abs(lower_bound))
            self._cleanup_buffer(target_buffer, current_time, abs(upper_bound))

            # Find matches within the interval
            for i in range(target_buffer.size()):
                target_record = <object>deref(target_buffer)[i].first
                target_time = deref(target_buffer)[i].second

                time_diff = current_time - target_time

                # Check if within interval bounds
                if lower_bound <= time_diff <= upper_bound:
                    if is_left:
                        joined = self._merge_interval_records(record, target_record, True)
                    else:
                        joined = self._merge_interval_records(target_record, record, False)
                    results.append(joined)

            return results

    cdef void _cleanup_buffer(self, WindowEntries *buffer, double current_time, double max_age):
        """Clean old records from buffer."""
        cdef size_t i = 0
        while i < buffer.size():
            record_time = deref(buffer)[i].second
            if current_time - record_time > max_age:
                Py_DECREF(<object>deref(buffer)[i].first)
                buffer.erase(buffer.begin() + i)
            else:
                i += 1

        # Limit buffer size
        while buffer.size() > self._max_buffer_size:
            Py_DECREF(<object>deref(buffer)[0].first)
            buffer.erase(buffer.begin())

    cdef str _extract_join_key(self, dict record, bint is_left):
        """Extract join key from record."""
        if not self._conditions == []:
            condition = self._conditions[0]
            if is_left:
                field_name = condition.get_left_field()
            else:
                field_name = condition.get_right_field()
            return str(record.get(field_name, ''))

        return ''

    cdef dict _merge_interval_records(self, dict left_record, dict right_record, bint left_is_left):
        """Merge interval join records."""
        merged = {}

        # Add records with stream prefixes
        for key, value in left_record.items():
            merged[f"left_{key}"] = value

        for key, value in right_record.items():
            merged[f"right_{key}"] = value

        return merged

    async def get_stats(self) -> Dict[str, Any]:
        """Get interval join processor statistics."""
        cdef size_t left_total
        cdef size_t right_total
        cdef unordered_map[cpp_string, WindowEntries].iterator it

        async with self._lock:
            left_total = 0

            it = self._left_buffer.begin()
            while it != self._left_buffer.end():
                left_total += deref(it).second.size()
                inc(it)

            right_total = 0
            it = self._right_buffer.begin()
            while it != self._right_buffer.end():
                right_total += deref(it).second.size()
                inc(it)

            return {
                'join_type': _c_join_type_to_str(self._join_type),
                'join_mode': 'interval',
                'left_buffer_size': left_total,
                'right_buffer_size': right_total,
                'left_lower_bound': self._left_lower_bound,
                'left_upper_bound': self._left_upper_bound,
                'right_lower_bound': self._right_lower_bound,
                'right_upper_bound': self._right_upper_bound,
            }


cdef class TemporalJoinProcessor:
    """Cython-optimized temporal table join processor (Flink-style)."""

    cdef:
        CJoinType _join_type  # C++ enum for performance
        list _conditions  # List of JoinCondition objects (Python-managed for safety)
        unordered_map[cpp_string, VersionedEntries] _temporal_table
        object _lock
        uint64_t _current_version

    def __cinit__(self, object join_type, list conditions):
        self._join_type = _to_c_join_type(join_type)
        self._current_version = 0
        self._lock = asyncio.Lock()
        self._conditions = []

        # Convert conditions
        for condition in conditions:
            if isinstance(condition, dict):
                join_cond = JoinCondition(
                    condition['left_field'],
                    condition['right_field'],
                    condition.get('left_alias', ''),
                    condition.get('right_alias', '')
                )
                self._conditions.append(join_cond)
            elif isinstance(condition, JoinCondition):
                self._conditions.append(condition)


    async def update_temporal_table(self, table_data: Dict[str, Any], version: uint64_t) -> None:
        """Update the temporal table with new version."""
        cdef unordered_map[cpp_string, VersionedEntries].iterator table_it
        cdef VersionedEntries *versions
        cdef size_t i
        cdef cpp_string c_key
        cdef VersionedEntries ver_entries

        async with self._lock:
            self._current_version = version

            # Clear previous version
            table_it = self._temporal_table.begin()
            while table_it != self._temporal_table.end():
                versions = &deref(table_it).second
                for i in range(versions.size()):
                    Py_DECREF(<object>deref(versions)[i].first)
                versions.clear()
                inc(table_it)

            self._temporal_table.clear()

            # Add new version data
            for key, value in table_data.items():
                c_key = str(key).encode('utf-8')
                ver_entries = VersionedEntries()
                Py_INCREF(value)
                ver_entries.push_back(VersionedEntry(<void*><PyObject*>value, version))
                self._temporal_table[c_key] = ver_entries

    async def process_stream_record(self, object record, version: uint64_t = 0) -> List[dict]:
        """Process a stream record with temporal join."""
        cdef cpp_string c_join_key
        cdef unordered_map[cpp_string, VersionedEntries].iterator it
        cdef VersionedEntries *versions
        cdef void* matched_record
        cdef uint64_t matched_version
        cdef size_t i

        async with self._lock:
            results = []

            if not isinstance(record, dict):
                return results

            join_key = self._extract_join_key(record, True)

            if not join_key:
                # Handle non-inner joins
                if self._join_type in (JoinType.LEFT_OUTER, JoinType.FULL_OUTER):
                    results.append(record)
                return results

            c_join_key = join_key.encode('utf-8')
            it = self._temporal_table.find(c_join_key)

            if it != self._temporal_table.end():
                # Find the appropriate version
                versions = &deref(it).second
                matched_record = NULL
                matched_version = 0

                # Find the latest version <= requested version
                for i in range(versions.size()):
                    ver = deref(versions)[i].second
                    if ver <= version and ver > matched_version:
                        matched_record = deref(versions)[i].first
                        matched_version = ver

                if matched_record != NULL:
                    table_record = <object>matched_record
                    joined = self._merge_temporal_records(record, table_record)
                    results.append(joined)
                elif self._join_type in (JoinType.LEFT_OUTER, JoinType.FULL_OUTER):
                    results.append(record)
            elif self._join_type in (JoinType.LEFT_OUTER, JoinType.FULL_OUTER):
                results.append(record)

            return results

    cdef str _extract_join_key(self, dict record, bint is_left):
        """Extract join key from record."""
        if not self._conditions == []:
            condition = self._conditions[0]
            if is_left:
                field_name = condition.get_left_field()
            else:
                field_name = condition.get_right_field()
            return str(record.get(field_name, ''))

        return ''

    cdef dict _merge_temporal_records(self, dict stream_record, dict table_record):
        """Merge stream and temporal table records."""
        merged = dict(stream_record)

        # Add table fields
        for key, value in table_record.items():
            if key not in merged:
                merged[f"temporal_{key}"] = value

        return merged

    async def get_stats(self) -> Dict[str, Any]:
        """Get temporal join processor statistics."""
        cdef size_t total_versions
        cdef unordered_map[cpp_string, VersionedEntries].iterator it

        async with self._lock:
            total_versions = 0
            it = self._temporal_table.begin()
            while it != self._temporal_table.end():
                total_versions += deref(it).second.size()
                inc(it)

            return {
                'join_type': _c_join_type_to_str(self._join_type),
                'join_mode': 'temporal',
                'current_version': self._current_version,
                'unique_keys': self._temporal_table.size(),
                'total_versions': total_versions,
            }


cdef class LookupJoinProcessor:
    """Cython-optimized lookup join processor (Flink-style)."""

    cdef:
        CJoinType _join_type  # C++ enum for performance
        list _conditions  # List of JoinCondition objects (Python-managed for safety)
        object _lookup_func
        object _lock
        unordered_map[cpp_string, void*] _cache
        uint64_t _cache_size
        uint64_t _cache_hits
        uint64_t _cache_misses

    def __cinit__(self, object join_type, list conditions, object lookup_func, uint64_t cache_size = 1000):
        self._join_type = _to_c_join_type(join_type)
        self._lookup_func = lookup_func
        self._cache_size = cache_size
        self._lock = asyncio.Lock()
        self._conditions = []
        self._cache_hits = 0
        self._cache_misses = 0

        # Convert conditions
        for condition in conditions:
            if isinstance(condition, dict):
                join_cond = JoinCondition(
                    condition['left_field'],
                    condition['right_field'],
                    condition.get('left_alias', ''),
                    condition.get('right_alias', '')
                )
                self._conditions.append(join_cond)
            elif isinstance(condition, JoinCondition):
                self._conditions.append(condition)


    async def process_stream_record(self, object record) -> List[dict]:
        """Process a stream record with lookup join."""
        cdef cpp_string c_join_key
        cdef unordered_map[cpp_string, void*].iterator cache_it
        cdef object lookup_result
        cdef unordered_map[cpp_string, void*].iterator first_it

        async with self._lock:
            results = []

            if not isinstance(record, dict):
                return results

            join_key = self._extract_join_key(record)

            if not join_key:
                # Handle non-inner joins
                if self._join_type in (JoinType.LEFT_OUTER, JoinType.FULL_OUTER):
                    results.append(record)
                return results

            # Check cache first
            c_join_key = join_key.encode('utf-8')
            cache_it = self._cache.find(c_join_key)

            lookup_result = None
            if cache_it != self._cache.end():
                self._cache_hits += 1
                lookup_result = <object>deref(cache_it).second
            else:
                self._cache_misses += 1
                # Perform external lookup
                try:
                    lookup_result = await self._lookup_func(join_key)
                    if lookup_result is not None:
                        # Cache the result
                        Py_INCREF(lookup_result)
                        self._cache[c_join_key] = <void*><PyObject*>lookup_result

                        # Evict old cache entries if needed
                        while self._cache.size() > self._cache_size:
                            # Simple LRU eviction - remove first entry
                            first_it = self._cache.begin()
                            Py_DECREF(<object>deref(first_it).second)
                            self._cache.erase(first_it)
                except Exception as e:
                    # Lookup failed
                    pass

            if lookup_result is not None:
                joined = self._merge_lookup_records(record, lookup_result)
                results.append(joined)
            elif self._join_type in (JoinType.LEFT_OUTER, JoinType.FULL_OUTER):
                results.append(record)

            return results

    cdef str _extract_join_key(self, dict record):
        """Extract join key from record."""
        if not self._conditions == []:
            condition = self._conditions[0]
            field_name = condition.get_left_field()
            return str(record.get(field_name, ''))

        return ''

    cdef dict _merge_lookup_records(self, dict stream_record, dict lookup_record):
        """Merge stream and lookup records."""
        merged = dict(stream_record)

        # Add lookup fields with prefix
        for key, value in lookup_record.items():
            if key not in merged:
                merged[f"lookup_{key}"] = value

        return merged

    async def get_stats(self) -> Dict[str, Any]:
        """Get lookup join processor statistics."""
        cdef double hit_rate

        async with self._lock:
            hit_rate = 0.0
            if self._cache_hits + self._cache_misses > 0:
                hit_rate = float(self._cache_hits) / float(self._cache_hits + self._cache_misses)

            return {
                'join_type': _c_join_type_to_str(self._join_type),
                'join_mode': 'lookup',
                'cache_size': self._cache.size(),
                'max_cache_size': self._cache_size,
                'cache_hits': self._cache_hits,
                'cache_misses': self._cache_misses,
                'cache_hit_rate': hit_rate,
            }


cdef class WindowJoinProcessor:
    """Cython-optimized window join processor (Flink-style)."""

    cdef:
        CJoinType _join_type  # C++ enum for performance
        list _conditions  # List of JoinCondition objects (Python-managed for safety)
        unordered_map[cpp_string, WindowEntries] _left_windows
        unordered_map[cpp_string, WindowEntries] _right_windows
        double _window_size
        double _window_slide
        object _lock
        uint64_t _max_window_size

    def __cinit__(
        self,
        object join_type,
        list conditions,
        double window_size,
        double window_slide = 0.0,
        uint64_t max_window_size = 10000
    ):
        self._join_type = _to_c_join_type(join_type)
        self._window_size = window_size
        self._window_slide = window_slide if window_slide > 0 else window_size
        self._max_window_size = max_window_size
        self._lock = asyncio.Lock()
        self._conditions = []

        # Convert conditions
        for condition in conditions:
            if isinstance(condition, dict):
                join_cond = JoinCondition(
                    condition['left_field'],
                    condition['right_field'],
                    condition.get('left_alias', ''),
                    condition.get('right_alias', '')
                )
                self._conditions.append(join_cond)
            elif isinstance(condition, JoinCondition):
                self._conditions.append(condition)


    async def process_left_record(self, object record) -> List[dict]:
        """Process a record from the left stream."""
        return await self._process_window_record(record, True)

    async def process_right_record(self, object record) -> List[dict]:
        """Process a record from the right stream."""
        return await self._process_window_record(record, False)

    async def _process_window_record(self, object record, bint is_left) -> List[dict]:
        """Process a record through window join."""
        cdef cpp_string c_join_key
        cdef WindowEntries *source_windows
        cdef WindowEntries *target_windows
        cdef size_t i

        async with self._lock:
            results = []

            if not isinstance(record, dict):
                return results

            current_time = time.time()
            join_key = self._extract_join_key(record, is_left)

            if not join_key:
                return results

            c_join_key = join_key.encode('utf-8')

            if is_left:
                source_windows = &self._left_windows[c_join_key]
                target_windows = &self._right_windows[c_join_key]
            else:
                source_windows = &self._right_windows[c_join_key]
                target_windows = &self._left_windows[c_join_key]

            # Add record to source window
            Py_INCREF(record)
            source_windows.push_back(WindowEntry(<void*><PyObject*>record, current_time))

            # Clean old records from both windows
            self._cleanup_window(source_windows, current_time)
            self._cleanup_window(target_windows, current_time)

            # Join with records in target window
            for i in range(target_windows.size()):
                target_record = <object>deref(target_windows)[i].first
                target_time = deref(target_windows)[i].second

                # Check if within same window
                if self._same_window(current_time, target_time):
                    if is_left:
                        joined = self._merge_window_records(record, target_record, True)
                    else:
                        joined = self._merge_window_records(target_record, record, False)
                    results.append(joined)

            return results

    cdef void _cleanup_window(self, WindowEntries *window, double current_time):
        """Clean old records from window."""
        cdef size_t i = 0
        while i < window.size():
            record_time = deref(window)[i].second
            if current_time - record_time > self._window_size:
                Py_DECREF(<object>deref(window)[i].first)
                window.erase(window.begin() + i)
            else:
                i += 1

        # Limit window size
        while window.size() > self._max_window_size:
            Py_DECREF(<object>deref(window)[0].first)
            window.erase(window.begin())

    cdef bint _same_window(self, double time1, double time2):
        """Check if two timestamps fall in the same window."""
        # For tumbling windows
        if self._window_slide >= self._window_size:
            window_start1 = int(time1 / self._window_size) * self._window_size
            window_start2 = int(time2 / self._window_size) * self._window_size
            return window_start1 == window_start2
        else:
            # For sliding windows - check overlap
            return abs(time1 - time2) < self._window_size

    cdef str _extract_join_key(self, dict record, bint is_left):
        """Extract join key from record."""
        if not self._conditions == []:
            condition = self._conditions[0]
            if is_left:
                field_name = condition.get_left_field()
            else:
                field_name = condition.get_right_field()
            return str(record.get(field_name, ''))

        return ''

    cdef dict _merge_window_records(self, dict left_record, dict right_record, bint left_is_left):
        """Merge window join records."""
        merged = {}

        # Add records with window prefixes
        for key, value in left_record.items():
            merged[f"left_{key}"] = value

        for key, value in right_record.items():
            merged[f"right_{key}"] = value

        return merged

    async def get_stats(self) -> Dict[str, Any]:
        """Get window join processor statistics."""
        cdef size_t left_total
        cdef size_t right_total
        cdef unordered_map[cpp_string, WindowEntries].iterator it

        async with self._lock:
            left_total = 0

            it = self._left_windows.begin()
            while it != self._left_windows.end():
                left_total += deref(it).second.size()
                inc(it)

            right_total = 0
            it = self._right_windows.begin()
            while it != self._right_windows.end():
                right_total += deref(it).second.size()
                inc(it)

            return {
                'join_type': _c_join_type_to_str(self._join_type),
                'join_mode': 'window',
                'window_size': self._window_size,
                'window_slide': self._window_slide,
                'left_windows_size': left_total,
                'right_windows_size': right_total,
                'unique_keys': self._left_windows.size(),
            }


cdef class TableTableJoinProcessor:
    """Cython-optimized table-table join processor."""

    cdef:
        CJoinType _join_type  # C++ enum for performance
        list _conditions  # List of JoinCondition objects (Python-managed for safety)
        unordered_map[cpp_string, PyObject*] _left_table
        unordered_map[cpp_string, PyObject*] _right_table
        object _lock

    def __cinit__(self, object join_type, list conditions):
        self._join_type = _to_c_join_type(join_type)
        self._lock = asyncio.Lock()
        self._conditions = []

        # Convert conditions
        for condition in conditions:
            if isinstance(condition, dict):
                join_cond = JoinCondition(
                    condition['left_field'],
                    condition['right_field'],
                    condition.get('left_alias', ''),
                    condition.get('right_alias', '')
                )
                self._conditions.append(join_cond)
            elif isinstance(condition, JoinCondition):
                self._conditions.append(condition)


    async def load_left_table(self, table_data: Dict[str, Any]) -> None:
        """Load left table data."""
        cdef unordered_map[cpp_string, PyObject*].iterator it
        cdef cpp_string c_key

        async with self._lock:
            # Clear existing data
            it = self._left_table.begin()
            while it != self._left_table.end():
                Py_DECREF(<object><PyObject*>deref(it).second)
                inc(it)
            self._left_table.clear()

            # Load new data
            for key, value in table_data.items():
                c_key = str(key).encode('utf-8')
                Py_INCREF(value)
                self._left_table[c_key] = <PyObject*>value

    async def load_right_table(self, table_data: Dict[str, Any]) -> None:
        """Load right table data."""
        cdef unordered_map[cpp_string, PyObject*].iterator it
        cdef cpp_string c_key

        async with self._lock:
            # Clear existing data
            it = self._right_table.begin()
            while it != self._right_table.end():
                Py_DECREF(<object><PyObject*>deref(it).second)
                inc(it)
            self._right_table.clear()

            # Load new data
            for key, value in table_data.items():
                c_key = str(key).encode('utf-8')
                Py_INCREF(value)
                self._right_table[c_key] = <PyObject*>value

    async def execute_join(self) -> List[dict]:
        """Execute the table-table join."""
        cdef unordered_map[cpp_string, PyObject*].iterator left_it
        cdef unordered_map[cpp_string, PyObject*].iterator right_it
        cdef cpp_string c_left_key
        cdef cpp_string c_right_key

        async with self._lock:
            results = []

            # Simple nested loop join (can be optimized with hash join)
            left_it = self._left_table.begin()
            while left_it != self._left_table.end():
                left_record = <object>deref(left_it).second
                left_key = self._extract_join_key(left_record, True)

                if left_key:
                    c_left_key = left_key.encode('utf-8')
                    right_it = self._right_table.find(c_left_key)

                    if right_it != self._right_table.end():
                        # Found match
                        right_record = <object>deref(right_it).second
                        joined = self._merge_table_records(left_record, right_record)
                        results.append(joined)
                    elif self._join_type in (JoinType.LEFT_OUTER, JoinType.FULL_OUTER):
                        # Left join - include left record even without match
                        results.append(left_record)

                inc(left_it)

            # Handle right outer joins
            if self._join_type in (JoinType.RIGHT_OUTER, JoinType.FULL_OUTER):
                right_it = self._right_table.begin()
                while right_it != self._right_table.end():
                    right_record = <object>deref(right_it).second
                    right_key = self._extract_join_key(right_record, False)

                    if right_key:
                        c_right_key = right_key.encode('utf-8')
                        left_it = self._left_table.find(c_right_key)

                        if left_it == self._left_table.end():
                            # No match found, include right record
                            results.append(right_record)

                    inc(right_it)

            return results

    cdef str _extract_join_key(self, dict record, bint is_left):
        """Extract join key from record."""
        if not self._conditions == []:
            condition = self._conditions[0]
            if is_left:
                field_name = condition.get_left_field()
            else:
                field_name = condition.get_right_field()
            return str(record.get(field_name, ''))

        return ''

    cdef dict _merge_table_records(self, dict left_record, dict right_record):
        """Merge two table records."""
        merged = dict(left_record)

        # Add right table fields with prefix to avoid conflicts
        for key, value in right_record.items():
            if key not in merged:
                merged[key] = value
            else:
                # Handle conflicts
                merged[f"right_{key}"] = value

        return merged

    async def get_stats(self) -> Dict[str, Any]:
        """Get join processor statistics."""
        async with self._lock:
            return {
                'join_type': _c_join_type_to_str(self._join_type),
                'left_table_size': self._left_table.size(),
                'right_table_size': self._right_table.size(),
                'conditions_count': len(self._conditions),
            }


# Factory functions
def create_stream_table_join(
    str join_type,
    list conditions,
    **kwargs
):
    """Create a stream-table join processor."""
    jt = JoinType(join_type)
    return StreamTableJoinProcessor(jt, conditions)


def create_stream_stream_join(
    str join_type,
    list conditions,
    **kwargs
):
    """Create a stream-stream join processor."""
    jt = JoinType(join_type)
    window_size = kwargs.get('window_size_seconds', 300.0)
    max_window_size = kwargs.get('max_window_size', 10000)
    return StreamStreamJoinProcessor(jt, conditions, window_size, max_window_size)


def create_interval_join(
    str join_type,
    list conditions,
    double left_lower_bound,
    double left_upper_bound,
    double right_lower_bound,
    double right_upper_bound,
    **kwargs
):
    """Create an interval join processor (Flink-style)."""
    jt = JoinType(join_type)
    max_buffer_size = kwargs.get('max_buffer_size', 10000)
    return IntervalJoinProcessor(
        jt, conditions, left_lower_bound, left_upper_bound,
        right_lower_bound, right_upper_bound, max_buffer_size
    )


def create_temporal_join(
    str join_type,
    list conditions,
    **kwargs
):
    """Create a temporal table join processor (Flink-style)."""
    jt = JoinType(join_type)
    return TemporalJoinProcessor(jt, conditions)


def create_lookup_join(
    str join_type,
    list conditions,
    object lookup_func,
    **kwargs
):
    """Create a lookup join processor (Flink-style)."""
    jt = JoinType(join_type)
    cache_size = kwargs.get('cache_size', 1000)
    return LookupJoinProcessor(jt, conditions, lookup_func, cache_size)


def create_window_join(
    str join_type,
    list conditions,
    double window_size,
    **kwargs
):
    """Create a window join processor (Flink-style)."""
    jt = JoinType(join_type)
    window_slide = kwargs.get('window_slide', 0.0)
    max_window_size = kwargs.get('max_window_size', 10000)
    return WindowJoinProcessor(jt, conditions, window_size, window_slide, max_window_size)


cdef class ArrowTableJoinProcessor:
    """Arrow-native table-table join processor using pyarrow.compute."""

    cdef:
        CJoinType _join_type  # C++ enum for performance
        list _conditions  # List of JoinCondition objects (Python-managed for safety)
        object _left_table
        object _right_table
        object _lock

    def __cinit__(self, object join_type, list conditions):
        self._join_type = _to_c_join_type(join_type)
        self._lock = asyncio.Lock()
        self._conditions = []
        self._left_table = None
        self._right_table = None

        # Convert conditions
        for condition in conditions:
            if isinstance(condition, dict):
                join_cond = JoinCondition(
                    condition['left_field'],
                    condition['right_field'],
                    condition.get('left_alias', ''),
                    condition.get('right_alias', '')
                )
                self._conditions.append(join_cond)
            elif isinstance(condition, JoinCondition):
                self._conditions.append(condition)


    async def load_left_table(self, table_data) -> None:
        """Load left table (Arrow Table or convertible data)."""
        async with self._lock:
            if not hasattr(table_data, 'join'):
                # Convert to Arrow Table if needed
                if hasattr(table_data, 'to_arrow'):
                    table_data = table_data.to_arrow()
                elif hasattr(table_data, 'to_pandas'):
                    import pandas as pd
                    if isinstance(table_data, pd.DataFrame):
                        import pyarrow as pa
                        table_data = pa.Table.from_pandas(table_data)

            self._left_table = table_data

    async def load_right_table(self, table_data) -> None:
        """Load right table (Arrow Table or convertible data)."""
        async with self._lock:
            if not hasattr(table_data, 'join'):
                # Convert to Arrow Table if needed
                if hasattr(table_data, 'to_arrow'):
                    table_data = table_data.to_arrow()
                elif hasattr(table_data, 'to_pandas'):
                    import pandas as pd
                    if isinstance(table_data, pd.DataFrame):
                        import pyarrow as pa
                        table_data = pa.Table.from_pandas(table_data)

            self._right_table = table_data

    async def execute_join(self):
        """Execute Arrow-native table join."""
        async with self._lock:
            if self._left_table is None or self._right_table is None:
                raise ValueError("Both tables must be loaded before joining")

            if not self._conditions == []:
                condition = self._conditions[0]
                left_key = condition.get_left_field()
                right_key = condition.get_right_field()

                # Use Arrow's native join
                try:
                    import pyarrow as pa
                    result = self._left_table.join(
                        self._right_table,
                        keys=[left_key],
                        right_keys=[right_key],
                        join_type=_c_join_type_to_str(self._join_type),
                        use_threads=True
                    )
                    return result
                except Exception as e:
                    # Fallback to pandas-style join if Arrow join fails
                    left_df = self._left_table.to_pandas()
                    right_df = self._right_table.to_pandas()

                    how_map = {
                        JoinType.INNER.value: 'inner',
                        JoinType.LEFT_OUTER.value: 'left',
                        JoinType.RIGHT_OUTER.value: 'right',
                        JoinType.FULL_OUTER.value: 'outer'
                    }

                    joined_df = left_df.merge(
                        right_df,
                        left_on=left_key,
                        right_on=right_key,
                        how=how_map.get(_c_join_type_to_str(self._join_type), 'inner'),
                        suffixes=('', '_right')
                    )

                    import pyarrow as pa
                    return pa.Table.from_pandas(joined_df)
            else:
                raise ValueError("Join conditions are required for table joins")

    async def get_stats(self) -> Dict[str, Any]:
        """Get join processor statistics."""
        async with self._lock:
            return {
                'join_type': _c_join_type_to_str(self._join_type),
                'join_mode': 'arrow_native',
                'left_table_loaded': self._left_table is not None,
                'right_table_loaded': self._right_table is not None,
                'conditions_count': len(self._conditions),
            }


cdef class ArrowDatasetJoinProcessor:
    """Arrow-native dataset join processor using pyarrow.dataset."""

    cdef:
        CJoinType _join_type  # C++ enum for performance
        list _conditions  # List of JoinCondition objects (Python-managed for safety)
        object _left_dataset
        object _right_dataset
        object _lock

    def __cinit__(self, object join_type, list conditions):
        self._join_type = _to_c_join_type(join_type)
        self._lock = asyncio.Lock()
        self._conditions = []
        self._left_dataset = None
        self._right_dataset = None

        # Convert conditions
        for condition in conditions:
            if isinstance(condition, dict):
                join_cond = JoinCondition(
                    condition['left_field'],
                    condition['right_field'],
                    condition.get('left_alias', ''),
                    condition.get('right_alias', '')
                )
                self._conditions.append(join_cond)
            elif isinstance(condition, JoinCondition):
                self._conditions.append(condition)


    async def load_left_dataset(self, dataset) -> None:
        """Load left dataset."""
        async with self._lock:
            import pyarrow.dataset as ds
            if not isinstance(dataset, ds.Dataset):
                # Convert to dataset if needed
                if isinstance(dataset, str):
                    # Path to files
                    dataset = ds.dataset(dataset)
                elif hasattr(dataset, 'to_arrow'):
                    # Convert table to dataset
                    dataset = ds.dataset(dataset.to_arrow())

            self._left_dataset = dataset

    async def load_right_dataset(self, dataset) -> None:
        """Load right dataset."""
        async with self._lock:
            import pyarrow.dataset as ds
            if not isinstance(dataset, ds.Dataset):
                # Convert to dataset if needed
                if isinstance(dataset, str):
                    # Path to files
                    dataset = ds.dataset(dataset)
                elif hasattr(dataset, 'to_arrow'):
                    # Convert table to dataset
                    dataset = ds.dataset(dataset.to_arrow())

            self._right_dataset = dataset

    async def execute_join(self):
        """Execute Arrow dataset join."""
        async with self._lock:
            if self._left_dataset is None or self._right_dataset is None:
                raise ValueError("Both datasets must be loaded before joining")

            if not self._conditions == []:
                condition = self._conditions[0]
                left_key = condition.get_left_field()
                right_key = condition.get_right_field()

                try:
                    # Use Arrow dataset join
                    result = self._left_dataset.join(
                        self._right_dataset,
                        keys=[left_key],
                        right_keys=[right_key],
                        join_type=_c_join_type_to_str(self._join_type),
                        use_threads=True
                    )
                    return result
                except Exception as e:
                    # Fallback: convert to tables and join
                    left_table = self._left_dataset.to_table()
                    right_table = self._right_dataset.to_table()

                    result = left_table.join(
                        right_table,
                        keys=[left_key],
                        right_keys=[right_key],
                        join_type=_c_join_type_to_str(self._join_type),
                        use_threads=True
                    )
                    return result
            else:
                raise ValueError("Join conditions are required for dataset joins")

    async def get_stats(self) -> Dict[str, Any]:
        """Get dataset join processor statistics."""
        async with self._lock:
            return {
                'join_type': _c_join_type_to_str(self._join_type),
                'join_mode': 'arrow_dataset',
                'left_dataset_loaded': self._left_dataset is not None,
                'right_dataset_loaded': self._right_dataset is not None,
                'conditions_count': len(self._conditions),
            }


cdef class ArrowAsofJoinProcessor:
    """Arrow-native as-of join processor using pyarrow.dataset.join_asof."""

    cdef:
        CJoinType _join_type  # C++ enum for performance
        str _left_key
        str _right_key
        str _left_by_key
        str _right_by_key
        object _left_dataset
        object _right_dataset
        object _lock
        double _tolerance

    def __cinit__(
        self,
        str left_key,
        str right_key,
        str left_by_key = None,
        str right_by_key = None,
        double tolerance = 0.0
    ):
        self._join_type = INNER  # As-of joins are typically inner
        self._left_key = left_key
        self._right_key = right_key
        self._left_by_key = left_by_key
        self._right_by_key = right_by_key
        self._tolerance = tolerance
        self._lock = asyncio.Lock()
        self._left_dataset = None
        self._right_dataset = None

    async def load_left_dataset(self, dataset) -> None:
        """Load left dataset for as-of join."""
        async with self._lock:
            import pyarrow.dataset as ds
            if not isinstance(dataset, ds.Dataset):
                # Convert to dataset if needed
                if isinstance(dataset, str):
                    dataset = ds.dataset(dataset)
                elif hasattr(dataset, 'to_arrow'):
                    dataset = ds.dataset(dataset.to_arrow())

            self._left_dataset = dataset

    async def load_right_dataset(self, dataset) -> None:
        """Load right dataset for as-of join."""
        async with self._lock:
            import pyarrow.dataset as ds
            if not isinstance(dataset, ds.Dataset):
                # Convert to dataset if needed
                if isinstance(dataset, str):
                    dataset = ds.dataset(dataset)
                elif hasattr(dataset, 'to_arrow'):
                    dataset = ds.dataset(dataset.to_arrow())

            self._right_dataset = dataset

    async def execute_join(self):
        """Execute Arrow as-of join."""
        async with self._lock:
            if self._left_dataset is None or self._right_dataset is None:
                raise ValueError("Both datasets must be loaded before as-of joining")

            try:
                # Use Arrow dataset as-of join
                result = self._left_dataset.join_asof(
                    self._right_dataset,
                    on=self._left_key,
                    right_on=self._right_key,
                    by=self._left_by_key if self._left_by_key else None,
                    right_by=self._right_by_key if self._right_by_key else None,
                    tolerance=self._tolerance if self._tolerance > 0 else None,
                    direction='backward'  # Default: match most recent prior
                )
                return result
            except Exception as e:
                # Fallback: convert to pandas and use pandas merge_asof
                left_df = self._left_dataset.to_table().to_pandas().sort_values(self._left_key)
                right_df = self._right_dataset.to_table().to_pandas().sort_values(self._right_key)

                import pandas as pd
                merged_df = pd.merge_asof(
                    left_df,
                    right_df,
                    on=self._left_key,
                    right_on=self._right_key,
                    by=self._left_by_key if self._left_by_key else None,
                    right_by=self._right_by_key if self._right_by_key else None,
                    tolerance=pd.Timedelta(seconds=self._tolerance) if self._tolerance > 0 else None,
                    direction='backward'
                )

                import pyarrow as pa
                return pa.Table.from_pandas(merged_df)

    async def get_stats(self) -> Dict[str, Any]:
        """Get as-of join processor statistics."""
        async with self._lock:
            return {
                'join_type': 'asof',
                'join_mode': 'arrow_asof',
                'left_key': self._left_key,
                'right_key': self._right_key,
                'left_by_key': self._left_by_key,
                'right_by_key': self._right_by_key,
                'tolerance': self._tolerance,
                'left_dataset_loaded': self._left_dataset is not None,
                'right_dataset_loaded': self._right_dataset is not None,
            }


def create_table_table_join(
    str join_type,
    list conditions,
    **kwargs
):
    """Create a table-table join processor."""
    jt = JoinType(join_type)
    return TableTableJoinProcessor(jt, conditions)


def create_arrow_table_join(
    str join_type,
    list conditions,
    **kwargs
):
    """Create an Arrow-native table join processor."""
    jt = JoinType(join_type)
    return ArrowTableJoinProcessor(jt, conditions)


def create_arrow_dataset_join(
    str join_type,
    list conditions,
    **kwargs
):
    """Create an Arrow-native dataset join processor."""
    jt = JoinType(join_type)
    return ArrowDatasetJoinProcessor(jt, conditions)


cpdef ArrowAsofJoinProcessor create_arrow_asof_join(
    str left_key,
    str right_key,
    str left_by_key = None,
    str right_by_key = None,
    double tolerance = 0.0
):
    """Create an Arrow-native as-of join processor."""
    return ArrowAsofJoinProcessor(left_key, right_key, left_by_key, right_by_key, tolerance)
