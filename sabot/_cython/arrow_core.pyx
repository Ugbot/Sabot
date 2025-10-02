# cython: language_level=3
"""Core Arrow operations for Sabot - Cython implementation."""

import time
from typing import Any, Dict, List, Optional, Union, Callable
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, memset
from cpython.ref cimport PyObject
from cpython.bytes cimport PyBytes_FromStringAndSize

# Arrow C++ imports
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

cdef extern from "arrow/type.h" namespace "arrow":
    cdef cppclass CDatum:
        pass

# Import Python modules dynamically
cdef object asyncio_module = None
cdef object logging_module = None
cdef object pa = None
cdef object pc = None

def _import_modules():
    """Import required modules at runtime."""
    global asyncio_module, logging_module, pa, pc
    if asyncio_module is None:
        import asyncio
        import logging
        import pyarrow as pa
        import pyarrow.compute as pc
        asyncio_module = asyncio
        logging_module = logging


cdef class FastArrowRecordBatch:
    """Cython-optimized Arrow RecordBatch wrapper."""

    cdef:
        shared_ptr[CRecordBatch] c_batch
        object py_batch
        dict column_cache
        bint _is_initialized

    def __cinit__(self):
        self.column_cache = {}
        self._is_initialized = False

    def __init__(self, batch=None):
        """Initialize with optional Arrow RecordBatch."""
        _import_modules()

        if batch is not None:
            self._set_batch(batch)
        else:
            # Create empty batch
            self.py_batch = pa.RecordBatch.from_arrays([], names=[])
            self._is_initialized = True

    cdef void _set_batch(self, object batch):
        """Set the underlying Arrow batch."""
        if not hasattr(batch, 'num_rows'):
            # Convert if needed
            if hasattr(batch, 'to_pylist'):
                # It's a table, convert to batch
                batch = pa.RecordBatch.from_struct_array(batch.to_struct_array())

        self.py_batch = batch

        # Try to unwrap to C++ batch for direct access
        cdef shared_ptr[CRecordBatch] c_batch_ptr
        cdef CStatus status = unwrap_record_batch(batch, &c_batch_ptr)
        if status.ok():
            self.c_batch = c_batch_ptr
        else:
            self.c_batch = shared_ptr[CRecordBatch]()

        self._is_initialized = True

    cpdef FastArrowRecordBatch filter(self, object condition):
        """Filter batch using Arrow compute."""
        if not self._is_initialized:
            raise RuntimeError("Batch not initialized")

        try:
            # Use Arrow compute for filtering
            filtered = pc.filter(self.py_batch, condition)
            result = FastArrowRecordBatch()
            result._set_batch(filtered)
            return result
        except Exception as e:
            logging_module.warning(f"Arrow filter failed: {e}")
            return self._filter_python(condition)

    cdef FastArrowRecordBatch _filter_python(self, object condition):
        """Fallback Python filtering."""
        # This would implement basic filtering in Python
        result = FastArrowRecordBatch()
        result._set_batch(self.py_batch)  # Placeholder
        return result

    cpdef FastArrowRecordBatch select(self, list columns):
        """Select specific columns."""
        if not self._is_initialized:
            raise RuntimeError("Batch not initialized")

        try:
            selected = self.py_batch.select(columns)
            result = FastArrowRecordBatch()
            result._set_batch(selected)
            return result
        except Exception as e:
            logging_module.error(f"Column selection failed: {e}")
            raise

    cpdef FastArrowRecordBatch sort(self, list sort_keys, bint ascending=True):
        """Sort batch by specified keys."""
        if not self._is_initialized:
            raise RuntimeError("Batch not initialized")

        try:
            sorted_batch = pc.sort_indices(
                self.py_batch,
                sort_keys=sort_keys,
                options=pc.SortOptions(ascending=ascending)
            )
            # Take the sorted batch
            result = FastArrowRecordBatch()
            result._set_batch(pc.take(self.py_batch, sorted_batch))
            return result
        except Exception as e:
            logging_module.error(f"Sorting failed: {e}")
            raise

    cpdef dict aggregate(self, dict aggregations):
        """Aggregate batch data."""
        if not self._is_initialized:
            raise RuntimeError("Batch not initialized")

        try:
            result = {}
            for col_name, agg_func in aggregations.items():
                if col_name in self.py_batch.column_names:
                    column = self.py_batch.column(col_name)
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
                    else:
                        result[col_name] = None
                else:
                    result[col_name] = None
            return result
        except Exception as e:
            logging_module.error(f"Aggregation failed: {e}")
            return {}

    cpdef object to_pandas(self):
        """Convert to pandas DataFrame."""
        if not self._is_initialized:
            return None

        try:
            return self.py_batch.to_pandas()
        except Exception as e:
            logging_module.error(f"Conversion to pandas failed: {e}")
            raise

    cpdef object to_pylist(self):
        """Convert to Python list of dicts."""
        if not self._is_initialized:
            return []

        try:
            return self.py_batch.to_pylist()
        except Exception as e:
            logging_module.error(f"Conversion to pylist failed: {e}")
            raise

    cpdef int num_rows(self):
        """Get number of rows."""
        if not self._is_initialized:
            return 0
        return len(self.py_batch)

    cpdef int num_columns(self):
        """Get number of columns."""
        if not self._is_initialized:
            return 0
        return self.py_batch.num_columns

    cpdef list column_names(self):
        """Get column names."""
        if not self._is_initialized:
            return []
        return self.py_batch.column_names

    cpdef object schema(self):
        """Get Arrow schema."""
        if not self._is_initialized:
            return None
        return self.py_batch.schema


cdef class FastArrowTable:
    """Cython-optimized Arrow Table wrapper with advanced operations."""

    cdef:
        shared_ptr[CTable] c_table
        object py_table
        dict column_cache
        bint _is_initialized

    def __cinit__(self):
        self.column_cache = {}
        self._is_initialized = False

    def __init__(self, table=None):
        """Initialize with optional Arrow table."""
        _import_modules()

        if table is not None:
            self._set_table(table)
        else:
            # Create empty table
            self.py_table = pa.table({})
            self._is_initialized = True

    cdef void _set_table(self, object table):
        """Set the underlying Arrow table."""
        if not hasattr(table, '__arrow_c_array__'):
            # Convert pandas or other formats to Arrow
            if hasattr(table, 'to_arrow'):
                table = table.to_arrow()
            elif hasattr(pa, 'Table') and hasattr(pa.Table, 'from_pandas'):
                try:
                    import pandas as pd
                    if isinstance(table, pd.DataFrame):
                        table = pa.Table.from_pandas(table)
                except ImportError:
                    pass

        self.py_table = table

        # Try to unwrap to C++ table for direct access
        cdef shared_ptr[CTable] c_table_ptr
        cdef CStatus status = unwrap_table(table, &c_table_ptr)
        if status.ok():
            self.c_table = c_table_ptr
        else:
            self.c_table = shared_ptr[CTable]()

        self._is_initialized = True

    cpdef FastArrowTable filter(self, object condition):
        """Filter table using Arrow compute."""
        if not self._is_initialized:
            raise RuntimeError("Table not initialized")

        try:
            filtered = pc.filter(self.py_table, condition)
            result = FastArrowTable()
            result._set_table(filtered)
            return result
        except Exception as e:
            logging_module.warning(f"Arrow filter failed: {e}")
            return self

    cpdef FastArrowTable select(self, list columns):
        """Select specific columns."""
        if not self._is_initialized:
            raise RuntimeError("Table not initialized")

        try:
            selected = self.py_table.select(columns)
            result = FastArrowTable()
            result._set_table(selected)
            return result
        except Exception as e:
            logging_module.error(f"Column selection failed: {e}")
            raise

    cpdef FastArrowTable group_by(self, list keys):
        """Group by operation."""
        if not self._is_initialized:
            raise RuntimeError("Table not initialized")

        try:
            grouped = self.py_table.group_by(keys)
            result = FastArrowTable()
            result._set_table(grouped)
            return result
        except Exception as e:
            logging_module.error(f"Group by failed: {e}")
            raise

    cpdef FastArrowTable agg(self, dict aggregations):
        """Aggregate grouped data."""
        if not self._is_initialized:
            raise RuntimeError("Table not initialized")

        try:
            # Assume table is already grouped
            aggregated = self.py_table.aggregate(aggregations)
            result = FastArrowTable()
            result._set_table(aggregated)
            return result
        except Exception as e:
            logging_module.error(f"Aggregation failed: {e}")
            raise

    cpdef FastArrowTable join(self, FastArrowTable other, list on=None, str how="left"):
        """Join with another table."""
        if not self._is_initialized or not other._is_initialized:
            raise RuntimeError("Tables not initialized")

        try:
            joined = pc.join(self.py_table, other.py_table,
                           keys=on or [], join_type=how)
            result = FastArrowTable()
            result._set_table(joined)
            return result
        except Exception as e:
            logging_module.error(f"Join failed: {e}")
            raise

    cpdef FastArrowTable sort(self, list sort_keys, bint ascending=True):
        """Sort table by specified keys."""
        if not self._is_initialized:
            raise RuntimeError("Table not initialized")

        try:
            sorted_indices = pc.sort_indices(
                self.py_table,
                sort_keys=sort_keys,
                options=pc.SortOptions(ascending=ascending)
            )
            sorted_table = pc.take(self.py_table, sorted_indices)
            result = FastArrowTable()
            result._set_table(sorted_table)
            return result
        except Exception as e:
            logging_module.error(f"Sorting failed: {e}")
            raise

    cpdef object to_pandas(self):
        """Convert to pandas DataFrame."""
        if not self._is_initialized:
            return None

        try:
            return self.py_table.to_pandas()
        except Exception as e:
            logging_module.error(f"Conversion to pandas failed: {e}")
            raise

    cpdef bytes to_arrow_ipc(self):
        """Serialize to Arrow IPC format."""
        if not self._is_initialized:
            raise RuntimeError("Table not initialized")

        try:
            sink = pa.BufferOutputStream()
            with pa.ipc.new_file(sink, self.py_table.schema) as writer:
                writer.write(self.py_table)
            return sink.getvalue().to_pybytes()
        except Exception as e:
            logging_module.error(f"IPC serialization failed: {e}")
            raise

    cpdef int num_rows(self):
        """Get number of rows."""
        if not self._is_initialized:
            return 0
        return len(self.py_table)

    cpdef int num_columns(self):
        """Get number of columns."""
        if not self._is_initialized:
            return 0
        return self.py_table.num_columns

    cpdef list column_names(self):
        """Get column names."""
        if not self._is_initialized:
            return []
        return self.py_table.column_names

    cpdef object schema(self):
        """Get Arrow schema."""
        if not self._is_initialized:
            return None
        return self.py_table.schema

    cpdef dict get_stats(self):
        """Get table statistics."""
        if not self._is_initialized:
            return {}

        return {
            'num_rows': self.num_rows(),
            'num_columns': self.num_columns(),
            'column_names': self.column_names(),
            'memory_usage': self._estimate_memory_usage(),
            'schema': str(self.schema()) if self.schema() else None
        }

    cdef size_t _estimate_memory_usage(self):
        """Estimate memory usage of the table."""
        if not self._is_initialized:
            return 0

        try:
            return self.py_table.nbytes
        except AttributeError:
            return self.num_rows() * self.num_columns() * 8


cdef class FastArrowOperation:
    """Fast Arrow operation for pipeline chaining."""

    cdef:
        str _name
        object _operation_func
        dict _params
        double _execution_time
        size_t _rows_processed
        bint _is_lazy

    def __cinit__(self, str name, object operation_func, dict params=None, bint is_lazy=True):
        self._name = name
        self._operation_func = operation_func
        self._params = params or {}
        self._execution_time = 0.0
        self._rows_processed = 0
        self._is_lazy = is_lazy

    cpdef str get_name(self):
        """Get operation name."""
        return self._name

    cpdef object execute(self, object data):
        """Execute the operation on data."""
        cdef double start_time = time.time()

        try:
            result = self._operation_func(data, **self._params)
            self._execution_time = time.time() - start_time

            # Track rows processed
            if hasattr(result, 'num_rows'):
                self._rows_processed = result.num_rows
            elif hasattr(data, 'num_rows'):
                self._rows_processed = data.num_rows

            return result
        except Exception as e:
            logging_module.error(f"Operation {self._name} failed: {e}")
            raise

    cpdef dict get_stats(self):
        """Get operation statistics."""
        return {
            'name': self._name,
            'execution_time': self._execution_time,
            'rows_processed': self._rows_processed,
            'is_lazy': self._is_lazy,
            'params': self._params
        }


cdef class FastArrowPipeline:
    """Fast Arrow pipeline for operation chaining."""

    cdef:
        list _operations
        object _current_data
        dict _pipeline_stats
        str _name
        bint _enable_caching
        dict _cache

    def __cinit__(self, str name="arrow_pipeline", bint enable_caching=False):
        self._operations = []
        self._current_data = None
        self._pipeline_stats = {}
        self._name = name
        self._enable_caching = enable_caching
        self._cache = {}

    cpdef FastArrowPipeline add_operation(self, FastArrowOperation operation):
        """Add an operation to the pipeline."""
        self._operations.append(operation)
        return self

    def filter(self, object condition):
        """Add filter operation."""
        # Create operation without closure
        operation = FastArrowOperation("filter", None, {"condition": condition, "op_type": "filter"})
        return self.add_operation(operation)

    def select(self, list columns):
        """Add select operation."""
        operation = FastArrowOperation("select", None, {"columns": columns, "op_type": "select"})
        return self.add_operation(operation)

    def group_by(self, list keys):
        """Add group_by operation."""
        operation = FastArrowOperation("group_by", None, {"keys": keys, "op_type": "group_by"})
        return self.add_operation(operation)

    def agg(self, dict aggregations):
        """Add aggregation operation."""
        operation = FastArrowOperation("agg", None, {"aggregations": aggregations, "op_type": "agg"})
        return self.add_operation(operation)

    def join(self, object other, list on=None, str how="left"):
        """Add join operation."""
        operation = FastArrowOperation("join", None, {"other": other, "on": on, "how": how, "op_type": "join"})
        return self.add_operation(operation)

    def sort(self, list by, bint ascending=True):
        """Add sort operation."""
        operation = FastArrowOperation("sort", None, {"by": by, "ascending": ascending, "op_type": "sort"})
        return self.add_operation(operation)

    def limit(self, size_t n):
        """Add limit operation."""
        operation = FastArrowOperation("limit", None, {"n": n, "op_type": "limit"})
        return self.add_operation(operation)

    cpdef FastArrowPipeline transform(self, object func, str name="transform"):
        """Add custom transform operation."""
        operation = FastArrowOperation(name, func, {"custom": True})
        return self.add_operation(operation)

    cpdef object execute(self, object initial_data=None):
        """Execute the entire pipeline."""
        cdef object data = initial_data if initial_data is not None else self._current_data

        if data is None:
            raise ValueError("No data provided for pipeline execution")

        cdef double pipeline_start = time.time()

        # Execute each operation in sequence
        for operation in self._operations:
            data = operation.execute(data)

        cdef double pipeline_time = time.time() - pipeline_start

        # Update pipeline stats
        self._pipeline_stats = {
            'execution_time': pipeline_time,
            'operations_executed': len(self._operations),
            'final_rows': data.num_rows if hasattr(data, 'num_rows') else 0
        }

        self._current_data = data
        return data

    async def execute_async(self, object initial_data=None):
        """Execute pipeline asynchronously."""
        # For now, just wrap sync execution
        loop = asyncio_module.get_event_loop()
        result = await loop.run_in_executor(None, self.execute, initial_data)
        return result

    cpdef void clear_pipeline(self):
        """Clear all operations from pipeline."""
        self._operations.clear()
        self._pipeline_stats.clear()

    cpdef list get_operations(self):
        """Get list of operations in pipeline."""
        return self._operations[:]

    cpdef dict get_pipeline_stats(self):
        """Get pipeline execution statistics."""
        stats = dict(self._pipeline_stats)
        stats['operations'] = [op.get_stats() for op in self._operations]
        return stats

    cpdef object get_current_data(self):
        """Get current pipeline data."""
        return self._current_data

    cpdef str show_pipeline(self):
        """Get string representation of pipeline."""
        lines = [f"Arrow Pipeline: {self._name}"]
        lines.append("=" * 40)

        for i, operation in enumerate(self._operations, 1):
            lines.append(f"{i}. {operation.get_name()}")

        if self._pipeline_stats:
            lines.append("")
            lines.append("Last Execution Stats:")
            for key, value in self._pipeline_stats.items():
                lines.append(f"  {key}: {value}")

        return "\n".join(lines)


# Global instances
cdef FastArrowPipeline _default_pipeline = FastArrowPipeline("default")

# Convenience functions
cpdef FastArrowRecordBatch create_record_batch(object data=None):
    """Create a FastArrowRecordBatch from data."""
    return FastArrowRecordBatch(data)

cpdef FastArrowTable create_table(object data=None):
    """Create a FastArrowTable from data."""
    return FastArrowTable(data)

cpdef FastArrowPipeline create_pipeline(str name="arrow_pipeline"):
    """Create a new Arrow pipeline."""
    return FastArrowPipeline(name)

cpdef object execute_pipeline(FastArrowPipeline pipeline, object data):
    """Execute an Arrow pipeline."""
    return pipeline.execute(data)

cpdef dict get_arrow_stats():
    """Get Arrow processing statistics."""
    return {
        'default_pipeline': _default_pipeline.get_pipeline_stats()
    }
