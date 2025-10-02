# cython: language_level=3
"""Cython-optimized join operations for Arrow-based stream processing."""

import asyncio
from libc.stdint cimport uint64_t, int64_t
from libc.string cimport memcpy, memset
from libcpp.string cimport string as cpp_string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.utility cimport pair
from cpython.ref cimport PyObject
from cython.operator cimport dereference as deref

# Type aliases - use void* instead of PyObject* for Cython compatibility
# Cast to <object><PyObject*> when reading, <void*><PyObject*> when writing
ctypedef pair[void*, double] WindowEntry
ctypedef vector[WindowEntry] WindowEntries
ctypedef pair[void*, uint64_t] VersionedEntry
ctypedef vector[VersionedEntry] VersionedEntries

import time
from typing import Any, Dict, List, Optional, Callable, AsyncIterator
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

    cdef cppclass CHashJoinOptions:
        pass

    cdef CStatus HashJoin "arrow::compute::HashJoin"(
        const vector[CDatum]& left_keys,
        const vector[CDatum]& right_keys,
        CHashJoinOptions options,
        vector[pair[uint64_t, uint64_t]]* result_ids
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


class JoinType(Enum):
    """Join types supported by Sabot (Flink-compatible)."""
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
        JoinType _join_type
        vector[JoinCondition*] _conditions
        unordered_map[cpp_string, void*] _table_data
        object _lock
        double _last_cleanup

    def __cinit__(self, JoinType join_type, list conditions):
        self._join_type = join_type
        self._lock = asyncio.Lock()
        self._last_cleanup = time.time()

        # Convert conditions
        for condition in conditions:
            if isinstance(condition, dict):
                join_cond = new JoinCondition(
                    condition['left_field'],
                    condition['right_field'],
                    condition.get('left_alias', ''),
                    condition.get('right_alias', '')
                )
                self._conditions.push_back(join_cond)
            elif isinstance(condition, JoinCondition):
                self._conditions.push_back(<JoinCondition*>condition)

    async def load_table_data(self, table_data: Dict[str, Any]) -> None:
        """Load table data for joins."""
        cdef unordered_map[cpp_string, void*].iterator it
        cdef cpp_string c_key
        async with self._lock:
            # Clear existing data
            it = self._table_data.begin()
            while it != self._table_data.end():
                Py_DECREF(<PyObject*>deref(it).second)
                inc(it)
            self._table_data.clear()

            # Load new data
            for key, value in table_data.items():
                c_key = str(key).encode('utf-8')
                Py_INCREF(<PyObject*>value)
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
        if not self._conditions.empty():
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
                'join_type': self._join_type.value,
                'table_size': self._table_data.size(),
                'conditions_count': self._conditions.size(),
                'last_cleanup': self._last_cleanup,
            }


cdef class StreamStreamJoinProcessor:
    """Cython-optimized stream-stream join processor with windowing."""

    cdef:
        JoinType _join_type
        vector[JoinCondition*] _conditions
        unordered_map[cpp_string, WindowEntries] _left_window
        unordered_map[cpp_string, WindowEntries] _right_window
        double _window_size_seconds
        object _lock
        uint64_t _max_window_size

    def __cinit__(self, JoinType join_type, list conditions, double window_size_seconds = 300.0, uint64_t max_window_size = 10000):
        self._join_type = join_type
        self._window_size_seconds = window_size_seconds
        self._lock = asyncio.Lock()
        self._max_window_size = max_window_size

        # Convert conditions
        for condition in conditions:
            if isinstance(condition, dict):
                join_cond = new JoinCondition(
                    condition['left_field'],
                    condition['right_field'],
                    condition.get('left_alias', ''),
                    condition.get('right_alias', '')
                )
                self._conditions.push_back(join_cond)

    async def process_left_stream(self, object record) -> List[dict]:
        """Process a record from the left stream."""
        return await self._process_stream_record(record, True)

    async def process_right_stream(self, object record) -> List[dict]:
        """Process a record from the right stream."""
        return await self._process_stream_record(record, False)

    async def _process_stream_record(self, object record, bint is_left) -> List[dict]:
        """Process a stream record and find joins within the window."""
        async with self._lock:
            results = []

            if not isinstance(record, dict):
                return results

            current_time = time.time()
            join_key = self._extract_join_key(record, is_left)

            if not join_key:
                return results

            cdef cpp_string c_join_key = join_key.encode('utf-8')
            cdef WindowEntries *target_window
            cdef WindowEntries *other_window

            if is_left:
                target_window = &self._left_window[c_join_key]
                other_window = &self._right_window[c_join_key]
            else:
                target_window = &self._right_window[c_join_key]
                other_window = &self._left_window[c_join_key]

            # Add current record to window
            Py_INCREF(<PyObject*>record)
            target_window.push_back(pair[PyObject*, double](<PyObject*>record, current_time))

            # Clean old records from target window
            self._cleanup_window(target_window, current_time)

            # Find matches in the other window
            cdef size_t i
            for i in range(other_window.size()):
                other_record = <object>other_window[i].first
                other_time = other_window[i].second

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
        if not self._conditions.empty():
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
            record_time = window[i].second
            if current_time - record_time > self._window_size_seconds:
                # Remove old record
                Py_DECREF(window[i].first)
                window.erase(window.begin() + i)
            else:
                i += 1

        # Limit window size
        while window.size() > self._max_window_size:
            Py_DECREF(window[0].first)
            window.erase(window.begin())

    async def get_stats(self) -> Dict[str, Any]:
        """Get join processor statistics."""
        async with self._lock:
            cdef size_t left_total = 0
            cdef unordered_map[cpp_string, WindowEntries].iterator it

            it = self._left_window.begin()
            while it != self._left_window.end():
                left_total += deref(it).second.size()
                inc(it)

            cdef size_t right_total = 0
            it = self._right_window.begin()
            while it != self._right_window.end():
                right_total += deref(it).second.size()
                inc(it)

            return {
                'join_type': self._join_type.value,
                'window_size_seconds': self._window_size_seconds,
                'left_window_size': left_total,
                'right_window_size': right_total,
                'unique_keys': self._left_window.size(),
                'max_window_size': self._max_window_size,
            }


cdef class IntervalJoinProcessor:
    """Cython-optimized interval join processor (Flink-style)."""

    cdef:
        JoinType _join_type
        vector[JoinCondition*] _conditions
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
        JoinType join_type,
        list conditions,
        double left_lower_bound,
        double left_upper_bound,
        double right_lower_bound,
        double right_upper_bound,
        uint64_t max_buffer_size = 10000
    ):
        self._join_type = join_type
        self._left_lower_bound = left_lower_bound
        self._left_upper_bound = left_upper_bound
        self._right_lower_bound = right_lower_bound
        self._right_upper_bound = right_upper_bound
        self._max_buffer_size = max_buffer_size
        self._lock = asyncio.Lock()

        # Convert conditions
        for condition in conditions:
            if isinstance(condition, dict):
                join_cond = new JoinCondition(
                    condition['left_field'],
                    condition['right_field'],
                    condition.get('left_alias', ''),
                    condition.get('right_alias', '')
                )
                self._conditions.push_back(join_cond)

    async def process_left_record(self, object record) -> List[dict]:
        """Process a record from the left stream."""
        return await self._process_record(record, True)

    async def process_right_record(self, object record) -> List[dict]:
        """Process a record from the right stream."""
        return await self._process_record(record, False)

    async def _process_record(self, object record, bint is_left) -> List[dict]:
        """Process a record and find interval joins."""
        async with self._lock:
            results = []

            if not isinstance(record, dict):
                return results

            current_time = time.time()
            join_key = self._extract_join_key(record, is_left)

            if not join_key:
                return results

            cdef cpp_string c_join_key = join_key.encode('utf-8')
            cdef WindowEntries *source_buffer
            cdef WindowEntries *target_buffer
            cdef double lower_bound, upper_bound

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
            Py_INCREF(<PyObject*>record)
            source_buffer.push_back(pair[PyObject*, double](<PyObject*>record, current_time))

            # Clean old records
            self._cleanup_buffer(source_buffer, current_time, abs(lower_bound))
            self._cleanup_buffer(target_buffer, current_time, abs(upper_bound))

            # Find matches within the interval
            cdef size_t i
            for i in range(target_buffer.size()):
                target_record = <object>target_buffer[i].first
                target_time = target_buffer[i].second

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
            record_time = buffer[i].second
            if current_time - record_time > max_age:
                Py_DECREF(buffer[i].first)
                buffer.erase(buffer.begin() + i)
            else:
                i += 1

        # Limit buffer size
        while buffer.size() > self._max_buffer_size:
            Py_DECREF(buffer[0].first)
            buffer.erase(buffer.begin())

    cdef str _extract_join_key(self, dict record, bint is_left):
        """Extract join key from record."""
        if not self._conditions.empty():
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
        async with self._lock:
            cdef size_t left_total = 0
            cdef unordered_map[cpp_string, WindowEntries].iterator it

            it = self._left_buffer.begin()
            while it != self._left_buffer.end():
                left_total += deref(it).second.size()
                inc(it)

            cdef size_t right_total = 0
            it = self._right_buffer.begin()
            while it != self._right_buffer.end():
                right_total += deref(it).second.size()
                inc(it)

            return {
                'join_type': self._join_type.value,
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
        JoinType _join_type
        vector[JoinCondition*] _conditions
        unordered_map[cpp_string, VersionedEntries] _temporal_table
        object _lock
        uint64_t _current_version

    def __cinit__(self, JoinType join_type, list conditions):
        self._join_type = join_type
        self._current_version = 0
        self._lock = asyncio.Lock()

        # Convert conditions
        for condition in conditions:
            if isinstance(condition, dict):
                join_cond = new JoinCondition(
                    condition['left_field'],
                    condition['right_field'],
                    condition.get('left_alias', ''),
                    condition.get('right_alias', '')
                )
                self._conditions.push_back(join_cond)

    async def update_temporal_table(self, table_data: Dict[str, Any], version: uint64_t) -> None:
        """Update the temporal table with new version."""
        async with self._lock:
            self._current_version = version

            # Clear previous version
            cdef unordered_map[cpp_string, VersionedEntries].iterator table_it
            table_it = self._temporal_table.begin()
            while table_it != self._temporal_table.end():
                cdef VersionedEntries *versions = &deref(table_it).second
                cdef size_t i
                for i in range(versions.size()):
                    Py_DECREF(versions[i].first)
                versions.clear()
                inc(table_it)

            self._temporal_table.clear()

            # Add new version data
            for key, value in table_data.items():
                cdef cpp_string c_key = str(key).encode('utf-8')
                cdef VersionedEntries versions
                Py_INCREF(<PyObject*>value)
                versions.push_back(pair[PyObject*, uint64_t](<PyObject*>value, version))
                self._temporal_table[c_key] = versions

    async def process_stream_record(self, object record, version: uint64_t = 0) -> List[dict]:
        """Process a stream record with temporal join."""
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

            cdef cpp_string c_join_key = join_key.encode('utf-8')
            cdef unordered_map[cpp_string, VersionedEntries].iterator it
            it = self._temporal_table.find(c_join_key)

            if it != self._temporal_table.end():
                # Find the appropriate version
                cdef vector[pair[PyObject*, uint64_t]] *versions = &deref(it).second
                cdef PyObject* matched_record = NULL
                cdef uint64_t matched_version = 0

                # Find the latest version <= requested version
                cdef size_t i
                for i in range(versions.size()):
                    ver = versions[i].second
                    if ver <= version and ver > matched_version:
                        matched_record = versions[i].first
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
        if not self._conditions.empty():
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
        async with self._lock:
            cdef size_t total_versions = 0
            cdef unordered_map[cpp_string, VersionedEntries].iterator it
            it = self._temporal_table.begin()
            while it != self._temporal_table.end():
                total_versions += deref(it).second.size()
                inc(it)

            return {
                'join_type': self._join_type.value,
                'join_mode': 'temporal',
                'current_version': self._current_version,
                'unique_keys': self._temporal_table.size(),
                'total_versions': total_versions,
            }


cdef class LookupJoinProcessor:
    """Cython-optimized lookup join processor (Flink-style)."""

    cdef:
        JoinType _join_type
        vector[JoinCondition*] _conditions
        object _lookup_func
        object _lock
        unordered_map[cpp_string, void*] _cache
        uint64_t _cache_size
        uint64_t _cache_hits
        uint64_t _cache_misses

    def __cinit__(self, JoinType join_type, list conditions, object lookup_func, uint64_t cache_size = 1000):
        self._join_type = join_type
        self._lookup_func = lookup_func
        self._cache_size = cache_size
        self._lock = asyncio.Lock()
        self._cache_hits = 0
        self._cache_misses = 0

        # Convert conditions
        for condition in conditions:
            if isinstance(condition, dict):
                join_cond = new JoinCondition(
                    condition['left_field'],
                    condition['right_field'],
                    condition.get('left_alias', ''),
                    condition.get('right_alias', '')
                )
                self._conditions.push_back(join_cond)

    async def process_stream_record(self, object record) -> List[dict]:
        """Process a stream record with lookup join."""
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
            cdef cpp_string c_join_key = join_key.encode('utf-8')
            cdef unordered_map[cpp_string, void*].iterator cache_it = self._cache.find(c_join_key)

            cdef object lookup_result = None
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
                        Py_INCREF(<PyObject*>lookup_result)
                        self._cache[c_join_key] = <PyObject*>lookup_result

                        # Evict old cache entries if needed
                        while self._cache.size() > self._cache_size:
                            # Simple LRU eviction - remove first entry
                            cdef unordered_map[cpp_string, void*].iterator first_it = self._cache.begin()
                            Py_DECREF(deref(first_it).second)
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
        if not self._conditions.empty():
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
        async with self._lock:
            cdef double hit_rate = 0.0
            if self._cache_hits + self._cache_misses > 0:
                hit_rate = float(self._cache_hits) / float(self._cache_hits + self._cache_misses)

            return {
                'join_type': self._join_type.value,
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
        JoinType _join_type
        vector[JoinCondition*] _conditions
        unordered_map[cpp_string, WindowEntries] _left_windows
        unordered_map[cpp_string, WindowEntries] _right_windows
        double _window_size
        double _window_slide
        object _lock
        uint64_t _max_window_size

    def __cinit__(
        self,
        JoinType join_type,
        list conditions,
        double window_size,
        double window_slide = 0.0,
        uint64_t max_window_size = 10000
    ):
        self._join_type = join_type
        self._window_size = window_size
        self._window_slide = window_slide if window_slide > 0 else window_size
        self._max_window_size = max_window_size
        self._lock = asyncio.Lock()

        # Convert conditions
        for condition in conditions:
            if isinstance(condition, dict):
                join_cond = new JoinCondition(
                    condition['left_field'],
                    condition['right_field'],
                    condition.get('left_alias', ''),
                    condition.get('right_alias', '')
                )
                self._conditions.push_back(join_cond)

    async def process_left_record(self, object record) -> List[dict]:
        """Process a record from the left stream."""
        return await self._process_window_record(record, True)

    async def process_right_record(self, object record) -> List[dict]:
        """Process a record from the right stream."""
        return await self._process_window_record(record, False)

    async def _process_window_record(self, object record, bint is_left) -> List[dict]:
        """Process a record through window join."""
        async with self._lock:
            results = []

            if not isinstance(record, dict):
                return results

            current_time = time.time()
            join_key = self._extract_join_key(record, is_left)

            if not join_key:
                return results

            cdef cpp_string c_join_key = join_key.encode('utf-8')
            cdef vector[pair[PyObject*, double]] *source_windows
            cdef vector[pair[PyObject*, double]] *target_windows

            if is_left:
                source_windows = &self._left_windows[c_join_key]
                target_windows = &self._right_windows[c_join_key]
            else:
                source_windows = &self._right_windows[c_join_key]
                target_windows = &self._left_windows[c_join_key]

            # Add record to source window
            Py_INCREF(<PyObject*>record)
            source_windows.push_back(pair[PyObject*, double](<PyObject*>record, current_time))

            # Clean old records from both windows
            self._cleanup_window(source_windows, current_time)
            self._cleanup_window(target_windows, current_time)

            # Join with records in target window
            cdef size_t i
            for i in range(target_windows.size()):
                target_record = <object>target_windows[i].first
                target_time = target_windows[i].second

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
            record_time = window[i].second
            if current_time - record_time > self._window_size:
                Py_DECREF(window[i].first)
                window.erase(window.begin() + i)
            else:
                i += 1

        # Limit window size
        while window.size() > self._max_window_size:
            Py_DECREF(window[0].first)
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
        if not self._conditions.empty():
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
        async with self._lock:
            cdef size_t left_total = 0
            cdef unordered_map[cpp_string, WindowEntries].iterator it

            it = self._left_windows.begin()
            while it != self._left_windows.end():
                left_total += deref(it).second.size()
                inc(it)

            cdef size_t right_total = 0
            it = self._right_windows.begin()
            while it != self._right_windows.end():
                right_total += deref(it).second.size()
                inc(it)

            return {
                'join_type': self._join_type.value,
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
        JoinType _join_type
        vector[JoinCondition*] _conditions
        unordered_map[cpp_string, PyObject*] _left_table
        unordered_map[cpp_string, PyObject*] _right_table
        object _lock

    def __cinit__(self, JoinType join_type, list conditions):
        self._join_type = join_type
        self._lock = asyncio.Lock()

        # Convert conditions
        for condition in conditions:
            if isinstance(condition, dict):
                join_cond = new JoinCondition(
                    condition['left_field'],
                    condition['right_field'],
                    condition.get('left_alias', ''),
                    condition.get('right_alias', '')
                )
                self._conditions.push_back(join_cond)

    async def load_left_table(self, table_data: Dict[str, Any]) -> None:
        """Load left table data."""
        async with self._lock:
            # Clear existing data
            cdef unordered_map[cpp_string, PyObject*].iterator it = self._left_table.begin()
            while it != self._left_table.end():
                Py_DECREF(deref(it).second)
                inc(it)
            self._left_table.clear()

            # Load new data
            for key, value in table_data.items():
                cdef cpp_string c_key = str(key).encode('utf-8')
                Py_INCREF(<PyObject*>value)
                self._left_table[c_key] = <PyObject*>value

    async def load_right_table(self, table_data: Dict[str, Any]) -> None:
        """Load right table data."""
        async with self._lock:
            # Clear existing data
            cdef unordered_map[cpp_string, PyObject*].iterator it = self._right_table.begin()
            while it != self._right_table.end():
                Py_DECREF(deref(it).second)
                inc(it)
            self._right_table.clear()

            # Load new data
            for key, value in table_data.items():
                cdef cpp_string c_key = str(key).encode('utf-8')
                Py_INCREF(<PyObject*>value)
                self._right_table[c_key] = <PyObject*>value

    async def execute_join(self) -> List[dict]:
        """Execute the table-table join."""
        async with self._lock:
            results = []

            # Simple nested loop join (can be optimized with hash join)
            cdef unordered_map[cpp_string, PyObject*].iterator left_it = self._left_table.begin()
            while left_it != self._left_table.end():
                left_record = <object>deref(left_it).second
                left_key = self._extract_join_key(left_record, True)

                if left_key:
                    cdef cpp_string c_left_key = left_key.encode('utf-8')
                    cdef unordered_map[cpp_string, PyObject*].iterator right_it = self._right_table.find(c_left_key)

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
                cdef unordered_map[cpp_string, PyObject*].iterator right_it = self._right_table.begin()
                while right_it != self._right_table.end():
                    right_record = <object>deref(right_it).second
                    right_key = self._extract_join_key(right_record, False)

                    if right_key:
                        cdef cpp_string c_right_key = right_key.encode('utf-8')
                        cdef unordered_map[cpp_string, PyObject*].iterator left_it = self._left_table.find(c_right_key)

                        if left_it == self._left_table.end():
                            # No match found, include right record
                            results.append(right_record)

                    inc(right_it)

            return results

    cdef str _extract_join_key(self, dict record, bint is_left):
        """Extract join key from record."""
        if not self._conditions.empty():
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
                'join_type': self._join_type.value,
                'left_table_size': self._left_table.size(),
                'right_table_size': self._right_table.size(),
                'conditions_count': self._conditions.size(),
            }


# Factory functions
cpdef StreamTableJoinProcessor create_stream_table_join(
    str join_type,
    list conditions,
    **kwargs
):
    """Create a stream-table join processor."""
    jt = JoinType(join_type)
    return StreamTableJoinProcessor(jt, conditions)


cpdef StreamStreamJoinProcessor create_stream_stream_join(
    str join_type,
    list conditions,
    **kwargs
):
    """Create a stream-stream join processor."""
    jt = JoinType(join_type)
    window_size = kwargs.get('window_size_seconds', 300.0)
    max_window_size = kwargs.get('max_window_size', 10000)
    return StreamStreamJoinProcessor(jt, window_size, max_window_size)


cpdef IntervalJoinProcessor create_interval_join(
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


cpdef TemporalJoinProcessor create_temporal_join(
    str join_type,
    list conditions,
    **kwargs
):
    """Create a temporal table join processor (Flink-style)."""
    jt = JoinType(join_type)
    return TemporalJoinProcessor(jt, conditions)


cpdef LookupJoinProcessor create_lookup_join(
    str join_type,
    list conditions,
    object lookup_func,
    **kwargs
):
    """Create a lookup join processor (Flink-style)."""
    jt = JoinType(join_type)
    cache_size = kwargs.get('cache_size', 1000)
    return LookupJoinProcessor(jt, conditions, lookup_func, cache_size)


cpdef WindowJoinProcessor create_window_join(
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
        JoinType _join_type
        vector[JoinCondition*] _conditions
        object _left_table
        object _right_table
        object _lock

    def __cinit__(self, JoinType join_type, list conditions):
        self._join_type = join_type
        self._lock = asyncio.Lock()
        self._left_table = None
        self._right_table = None

        # Convert conditions
        for condition in conditions:
            if isinstance(condition, dict):
                join_cond = new JoinCondition(
                    condition['left_field'],
                    condition['right_field'],
                    condition.get('left_alias', ''),
                    condition.get('right_alias', '')
                )
                self._conditions.push_back(join_cond)

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

            if not self._conditions.empty():
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
                        join_type=self._join_type.value,
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
                        how=how_map.get(self._join_type.value, 'inner'),
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
                'join_type': self._join_type.value,
                'join_mode': 'arrow_native',
                'left_table_loaded': self._left_table is not None,
                'right_table_loaded': self._right_table is not None,
                'conditions_count': self._conditions.size(),
            }


cdef class ArrowDatasetJoinProcessor:
    """Arrow-native dataset join processor using pyarrow.dataset."""

    cdef:
        JoinType _join_type
        vector[JoinCondition*] _conditions
        object _left_dataset
        object _right_dataset
        object _lock

    def __cinit__(self, JoinType join_type, list conditions):
        self._join_type = join_type
        self._lock = asyncio.Lock()
        self._left_dataset = None
        self._right_dataset = None

        # Convert conditions
        for condition in conditions:
            if isinstance(condition, dict):
                join_cond = new JoinCondition(
                    condition['left_field'],
                    condition['right_field'],
                    condition.get('left_alias', ''),
                    condition.get('right_alias', '')
                )
                self._conditions.push_back(join_cond)

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

            if not self._conditions.empty():
                condition = self._conditions[0]
                left_key = condition.get_left_field()
                right_key = condition.get_right_field()

                try:
                    # Use Arrow dataset join
                    result = self._left_dataset.join(
                        self._right_dataset,
                        keys=[left_key],
                        right_keys=[right_key],
                        join_type=self._join_type.value,
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
                        join_type=self._join_type.value,
                        use_threads=True
                    )
                    return result
            else:
                raise ValueError("Join conditions are required for dataset joins")

    async def get_stats(self) -> Dict[str, Any]:
        """Get dataset join processor statistics."""
        async with self._lock:
            return {
                'join_type': self._join_type.value,
                'join_mode': 'arrow_dataset',
                'left_dataset_loaded': self._left_dataset is not None,
                'right_dataset_loaded': self._right_dataset is not None,
                'conditions_count': self._conditions.size(),
            }


cdef class ArrowAsofJoinProcessor:
    """Arrow-native as-of join processor using pyarrow.dataset.join_asof."""

    cdef:
        JoinType _join_type
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
        self._join_type = JoinType.INNER  # As-of joins are typically inner
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


cpdef TableTableJoinProcessor create_table_table_join(
    str join_type,
    list conditions,
    **kwargs
):
    """Create a table-table join processor."""
    jt = JoinType(join_type)
    return TableTableJoinProcessor(jt, conditions)


cpdef ArrowTableJoinProcessor create_arrow_table_join(
    str join_type,
    list conditions,
    **kwargs
):
    """Create an Arrow-native table join processor."""
    jt = JoinType(join_type)
    return ArrowTableJoinProcessor(jt, conditions)


cpdef ArrowDatasetJoinProcessor create_arrow_dataset_join(
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
