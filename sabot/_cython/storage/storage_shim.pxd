# -*- coding: utf-8 -*-
"""Cython declarations for Sabot Storage Shim Interface"""

from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.memory cimport unique_ptr, shared_ptr
from libcpp cimport bool as cbool

cdef extern from "sabot/storage/interface.h" namespace "sabot::storage":
    cdef enum StatusCode:
        OK
        NotFound
        InvalidArgument
        IOError
        NotSupported
        InternalError
    
    cdef cppclass Status:
        StatusCode code
        string message
        Status()
        Status(StatusCode, const string&)
        cbool ok()
        cbool IsNotFound()
        @staticmethod
        Status OK()
        @staticmethod
        Status NotFound(const string&)
        @staticmethod
        Status InvalidArgument(const string&)
        @staticmethod
        Status IOError(const string&)
        @staticmethod
        Status NotSupported(const string&)
        @staticmethod
        Status InternalError(const string&)
    
    cdef struct StorageConfig:
        string path
        size_t memtable_size_mb
        cbool enable_bloom_filter
        cbool enable_sparse_index
        size_t index_granularity
        StorageConfig()
        StorageConfig(const string&)
    
    cdef cppclass StateBackend:
        Status Open(const StorageConfig&)
        Status Close()
        Status Flush()
        Status Put(const string&, const string&)
        Status Get(const string&, string*)
        Status Delete(const string&)
        Status Exists(const string&, cbool*)
        Status MultiGet(const vector[string]&, vector[string]*)
        Status DeleteRange(const string&, const string&)
        Status CreateCheckpoint(string*)
        Status RestoreFromCheckpoint(const string&)
    
    # Arrow C++ type declarations - use arrow/api.h namespace types directly
    # These match what MarbleDB expects: shared_ptr<arrow::Schema>, shared_ptr<arrow::RecordBatch>, shared_ptr<arrow::Table>
    cdef extern from "arrow/api.h" namespace "arrow":
        cdef cppclass Schema:
            pass
        cdef cppclass RecordBatch:
            pass
        cdef cppclass Table:
            pass
    
    cdef cppclass StoreBackend:
        Status Open(const StorageConfig&)
        Status Close()
        Status Flush()
        Status CreateTable(const string&, const shared_ptr[Schema]&)
        Status ListTables(vector[string]*)
        Status InsertBatch(const string&, const shared_ptr[RecordBatch]&)
        Status ScanTable(const string&, shared_ptr[Table]*)
        Status ScanTableBatches(const string&, vector[shared_ptr[RecordBatch]]*)
        Status GetBatchCount(const string&, size_t*)
        Status GetBatchAt(const string&, size_t, shared_ptr[RecordBatch]*)
        Status ClearBatchCache(const string&)
        Status ScanRange(const string&, const string&, const string&, shared_ptr[Table]*)
        Status DeleteRange(const string&, const string&, const string&)
    
    unique_ptr[StateBackend] CreateStateBackend(const string&)
    unique_ptr[StoreBackend] CreateStoreBackend(const string&)

