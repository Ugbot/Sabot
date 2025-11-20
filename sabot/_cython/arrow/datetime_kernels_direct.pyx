# cython: language_level=3
# distutils: language = c++
# distutils: include_dirs = /Users/bengamble/Sabot/vendor/arrow/cpp/build/install/include
"""
Sabot DateTime Kernels - Direct C++ bindings to vendored Arrow compute kernels.

These kernels call the vendored Arrow C++ library directly (NO system PyArrow dependency).
"""

# Import Python datetime C API (required by Arrow's Python headers)
from cpython.datetime cimport import_datetime
import_datetime()

from libc.stdint cimport int64_t, uint64_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.memory cimport shared_ptr, make_shared
from libcpp cimport bool as cbool

# Force inclusion of Python datetime.h at compile time (before Arrow headers)
cdef extern from "datetime.h":
    pass

cimport pyarrow.lib as ca
from pyarrow.lib cimport (
    CArray,
    CDataType,
    CRecordBatch,
    CSchema,
    CStatus,
    CResult,
    CTable,
    pyarrow_wrap_array,
    pyarrow_wrap_table,
    pyarrow_wrap_batch,
    pyarrow_wrap_schema,
    pyarrow_wrap_data_type,
    GetResultValue,
)

# Import Arrow compute registry
cdef extern from "arrow/compute/api.h" namespace "arrow::compute" nogil:
    cdef cppclass CFunctionRegistry "arrow::compute::FunctionRegistry":
        @staticmethod
        CFunctionRegistry* GetFunctionRegistry()

    cdef cppclass CFunction "arrow::compute::Function":
        pass

    cdef cppclass CExecContext "arrow::compute::ExecContext":
        CExecContext()
        CFunctionRegistry* func_registry()

    cdef cppclass CDatum "arrow::compute::Datum":
        CDatum()
        CDatum(shared_ptr[CArray])
        shared_ptr[CArray] make_array()

    cdef CResult[CDatum] CallFunction(const string& func_name,
                                     const vector[CDatum]& args,
                                     CExecContext* ctx) except +

# Import Arrow types
cdef extern from "arrow/api.h" namespace "arrow" nogil:
    cdef cppclass CTimestampType "arrow::TimestampType"(CDataType):
        pass

    cdef shared_ptr[CDataType] timestamp(TimeUnit unit)
    cdef shared_ptr[CDataType] utf8()

    cdef enum TimeUnit "arrow::TimeUnit::type":
        TimeUnit_NANO "arrow::TimeUnit::NANO"

# Import custom Sabot kernels header
cdef extern from "arrow/compute/kernels/scalar_temporal_sabot.h" namespace "arrow::compute::internal" nogil:
    void RegisterSabotTemporalFunctions(CFunctionRegistry* registry)


def register_sabot_kernels():
    """Register Sabot datetime kernels with Arrow compute registry."""
    cdef CFunctionRegistry* registry = CFunctionRegistry.GetFunctionRegistry()
    with nogil:
        RegisterSabotTemporalFunctions(registry)


# Auto-register kernels on module import
register_sabot_kernels()


def parse_datetime(array, format_string: str):
    """
    Parse datetime strings using cpp-datetime format codes.

    This calls vendored Arrow C++ directly (no PyArrow dependency).

    Args:
        array: PyArrow Array of strings
        format_string: cpp-datetime format (e.g., "yyyy-MM-dd HH:mm:ss")

    Returns:
        PyArrow Array of timestamps (nanosecond precision)
    """
    import pyarrow as pa

    if not isinstance(array, pa.Array):
        array = pa.array(array)

    cdef:
        shared_ptr[CArray] c_array = ca.pyarrow_unwrap_array(array)
        CDatum input_datum = CDatum(c_array)
        vector[CDatum] args
        CExecContext ctx
        CResult[CDatum] result
        CDatum result_datum
        shared_ptr[CArray] result_array

    args.push_back(input_datum)

    # Call the C++ kernel directly
    with nogil:
        result = CallFunction(b"sabot_parse_datetime", args, &ctx)
        result_datum = GetResultValue(result)
        result_array = result_datum.make_array()

    return pyarrow_wrap_array(result_array)


def format_datetime(array, format_string: str):
    """
    Format timestamps using cpp-datetime format codes.

    This calls vendored Arrow C++ directly (no PyArrow dependency).

    Args:
        array: PyArrow Array of timestamps
        format_string: cpp-datetime format (e.g., "yyyy-MM-dd")

    Returns:
        PyArrow Array of formatted strings
    """
    import pyarrow as pa

    if not isinstance(array, pa.Array):
        array = pa.array(array)

    cdef:
        shared_ptr[CArray] c_array = ca.pyarrow_unwrap_array(array)
        CDatum input_datum = CDatum(c_array)
        vector[CDatum] args
        CExecContext ctx
        CResult[CDatum] result
        CDatum result_datum
        shared_ptr[CArray] result_array

    args.push_back(input_datum)

    # Call the C++ kernel directly
    with nogil:
        result = CallFunction(b"sabot_format_datetime", args, &ctx)
        result_datum = GetResultValue(result)
        result_array = result_datum.make_array()

    return pyarrow_wrap_array(result_array)


def add_days_simd(array, int days):
    """
    Add days to timestamps using SIMD-optimized arithmetic.

    Uses AVX2 when available (4-8x speedup).

    Args:
        array: PyArrow Array of timestamps
        days: Number of days to add (can be negative)

    Returns:
        PyArrow Array of timestamps
    """
    import pyarrow as pa

    if not isinstance(array, pa.Array):
        array = pa.array(array)

    cdef:
        shared_ptr[CArray] c_array = ca.pyarrow_unwrap_array(array)
        CDatum input_datum = CDatum(c_array)
        vector[CDatum] args
        CExecContext ctx
        CResult[CDatum] result
        CDatum result_datum
        shared_ptr[CArray] result_array

    args.push_back(input_datum)

    # Call the C++ kernel directly
    with nogil:
        result = CallFunction(b"sabot_add_days_simd", args, &ctx)
        result_datum = GetResultValue(result)
        result_array = result_datum.make_array()

    return pyarrow_wrap_array(result_array)


# Convenience aliases
def to_datetime(values, format: str = "yyyy-MM-dd HH:mm:ss"):
    """Convert values to timestamps."""
    import pyarrow as pa
    if not isinstance(values, pa.Array):
        values = pa.array(values)
    return parse_datetime(values, format)


def date_add(array, int days):
    """Add days to timestamps (convenience wrapper)."""
    return add_days_simd(array, days)


__all__ = [
    'register_sabot_kernels',
    'parse_datetime',
    'format_datetime',
    'add_days_simd',
    'to_datetime',
    'date_add',
]
