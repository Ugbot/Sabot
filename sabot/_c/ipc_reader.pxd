# cython: language_level=3
"""
Arrow IPC Reader - C++ Declarations

Streaming RecordBatch reading from Arrow IPC files using vendored Arrow C++.
"""

from libcpp.memory cimport shared_ptr
from libcpp.string cimport string
from libcpp cimport bool as cbool
from libc.stdint cimport int64_t

# Import Arrow C++ types from vendored Arrow
from pyarrow.includes.libarrow cimport (
    CRecordBatch,
    CSchema,
    CRandomAccessFile,
    CResult,
    CStatus,
)

# Forward declarations for Arrow IPC types
cdef extern from "arrow/ipc/reader.h" namespace "arrow::ipc" nogil:
    # IPC Read Options
    cdef cppclass CIpcReadOptions "arrow::ipc::IpcReadOptions":
        @staticmethod
        CIpcReadOptions Defaults()

        cbool use_threads
        int64_t max_recursion_depth

    # RecordBatch File Reader (for Arrow IPC files)
    cdef cppclass CRecordBatchFileReader "arrow::ipc::RecordBatchFileReader":
        @staticmethod
        CResult[shared_ptr[CRecordBatchFileReader]] Open(
            shared_ptr[CRandomAccessFile] file,
            const CIpcReadOptions& options
        )

        @staticmethod
        CResult[shared_ptr[CRecordBatchFileReader]] Open(
            shared_ptr[CRandomAccessFile] file
        )

        int num_record_batches()
        CResult[shared_ptr[CRecordBatch]] ReadRecordBatch(int i)
        shared_ptr[CSchema] schema()


# Forward declarations for Arrow IO
cdef extern from "arrow/io/api.h" namespace "arrow::io" nogil:
    cdef cppclass CReadableFile "arrow::io::ReadableFile":
        @staticmethod
        CResult[shared_ptr[CRandomAccessFile]] Open(const string& path)


# Python-accessible Cython class
cdef class ArrowIPCReader:
    cdef shared_ptr[CRecordBatchFileReader] reader
    cdef shared_ptr[CRandomAccessFile] file
    cdef readonly int num_batches
    cdef readonly object schema
