# cython: language_level=3
"""
Cython declarations for Arrow C++ compute kernels.

Provides zero-copy access to Arrow's SIMD-accelerated compute functions.
"""

from libcpp.memory cimport shared_ptr
from libcpp cimport bool as cbool

# Arrow C++ types
cdef extern from "arrow/api.h" namespace "arrow":
    cdef cppclass CArray "arrow::Array":
        int64_t length()

    cdef cppclass CTable "arrow::Table":
        int64_t num_rows()
        int64_t num_columns()

    cdef cppclass CRecordBatch "arrow::RecordBatch":
        int64_t num_rows()
        int64_t num_columns()

    cdef cppclass CChunkedArray "arrow::ChunkedArray":
        pass


# Arrow compute kernels
cdef extern from "arrow/compute/api.h" namespace "arrow::compute":

    cdef cppclass Datum:
        Datum()
        Datum(shared_ptr[CArray])
        Datum(shared_ptr[CTable])
        Datum(shared_ptr[CRecordBatch])
        shared_ptr[CArray] array()
        shared_ptr[CTable] table()
        shared_ptr[CRecordBatch] record_batch()

    cdef cppclass TakeOptions:
        TakeOptions()
        @staticmethod
        TakeOptions Defaults()
        cbool boundscheck

    cdef cppclass FilterOptions:
        FilterOptions()
        @staticmethod
        FilterOptions Defaults()

    cdef cppclass CastOptions:
        CastOptions()
        @staticmethod
        CastOptions Safe()
        @staticmethod
        CastOptions Unsafe()

    # Compute functions (return Result<Datum>)
    cdef cppclass CResult "arrow::Result"[T]:
        cbool ok()
        T& ValueOrDie()
        T ValueUnsafe()

    # Take kernel (select by indices)
    CResult[Datum] Take(const Datum& values, const Datum& indices, const TakeOptions& options)

    # Filter kernel (select by boolean mask)
    CResult[Datum] Filter(const Datum& values, const Datum& filter, const FilterOptions& options)

    # Unique kernel (get unique values)
    CResult[Datum] Unique(const Datum& values)

    # Hash kernels
    CResult[Datum] DictionaryEncode(const Datum& values)
