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
    
    # Forward declare Arrow types
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
        Status ScanRange(const string&, const string&, const string&, shared_ptr[Table]*)
        Status DeleteRange(const string&, const string&, const string&)
    
    unique_ptr[StateBackend] CreateStateBackend(const string&)
    unique_ptr[StoreBackend] CreateStoreBackend(const string&)

