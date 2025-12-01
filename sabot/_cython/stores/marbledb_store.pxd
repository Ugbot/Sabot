# -*- coding: utf-8 -*-
"""Cython declarations for MarbleDBStoreBackend.

Full C++ type declarations matching what's used in the .pyx file.
"""

from libc.stdint cimport uint64_t, int64_t
from libcpp.memory cimport unique_ptr, shared_ptr
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp cimport bool as cbool

# MarbleDB C++ declarations from marble/status.h
cdef extern from "marble/status.h" namespace "marble":
    cdef cppclass Status:
        Status()
        cbool ok()
        string ToString()
        cbool IsNotFound()
        @staticmethod
        Status OK()
        @staticmethod
        Status NotFound(const string& msg)
        @staticmethod
        Status InvalidArgument(const string& msg)
        @staticmethod
        Status IOError(const string& msg)
        @staticmethod
        Status NotImplemented(const string& msg)

# MarbleDB C++ declarations from marble/db.h
cdef extern from "marble/db.h" namespace "marble":
    cdef cppclass DBOptions:
        DBOptions()
        string db_path
        size_t memtable_size_threshold
        cbool enable_bloom_filter
        cbool enable_sparse_index
        size_t index_granularity
        cbool enable_hot_key_cache
        size_t hot_key_cache_size_mb

    cdef cppclass ReadOptions:
        ReadOptions()
        cbool verify_checksums
        cbool fill_cache
        cbool reverse_order

    cdef cppclass WriteOptions:
        WriteOptions()
        cbool sync

    # Note: Key is an abstract base class in MarbleDB.
    # Concrete implementations (StringKey, Int64Key) would need separate bindings.
    cdef cppclass Key:
        pass

    cdef cppclass KeyRange:
        KeyRange()
        shared_ptr[Key] start_key
        shared_ptr[Key] end_key
        cbool include_start
        cbool include_end

    cdef cppclass Record:
        pass

    cdef cppclass ColumnFamilyHandle:
        pass

    cdef cppclass ColumnFamilyOptions:
        ColumnFamilyOptions()
        shared_ptr[Schema] schema
        int primary_key_index
        size_t target_file_size
        cbool enable_bloom_filter
        size_t bloom_filter_bits_per_key
        cbool enable_sparse_index
        size_t index_granularity

    cdef struct ColumnFamilyDescriptor:
        ColumnFamilyDescriptor()  # Default constructor
        string name
        ColumnFamilyOptions options

    cdef cppclass MarbleDB:
        Status CreateColumnFamily(const ColumnFamilyDescriptor& descriptor, ColumnFamilyHandle** handle)
        Status InsertBatch(const string& table_name, const shared_ptr[RecordBatch]& batch)
        Status ScanTable(const string& table_name, unique_ptr[QueryResult]* result)
        Status NewIterator(const string& table_name, const ReadOptions& options, const KeyRange& range, unique_ptr[Iterator]* iterator)
        Status DeleteRange(const WriteOptions& options, const Key& begin_key, const Key& end_key)
        Status DeleteRange(const WriteOptions& options, ColumnFamilyHandle* cf, const Key& begin_key, const Key& end_key)
        vector[string] ListColumnFamilies()
        Status Flush()

    cdef cppclass Iterator:
        cbool Valid()
        void Next()
        void Prev()
        shared_ptr[Key] key()
        shared_ptr[Record] value()
        void Seek(const Key& target)
        void SeekToFirst()
        void SeekToLast()
        Status status()

# MarbleDB API functions from marble/api.h
cdef extern from "marble/api.h" namespace "marble":
    Status OpenDatabase(const string& path, unique_ptr[MarbleDB]* db)
    void CloseDatabase(unique_ptr[MarbleDB]* db)

    cdef cppclass QueryResult:
        cbool HasNext()
        Status Next(shared_ptr[RecordBatch]* batch)
        shared_ptr[Schema] schema()
        int64_t num_rows()

# Arrow C++ declarations
cdef extern from "arrow/api.h" namespace "arrow":
    cdef cppclass Schema:
        pass

    cdef cppclass RecordBatch:
        pass

    cdef cppclass Table:
        pass

# Extension type declaration
cdef class MarbleDBStoreBackend:
    cdef unique_ptr[MarbleDB] _db
    cdef str _db_path
    cdef cbool _is_open
    cdef dict _config
    cdef unordered_map[string, ColumnFamilyHandle*] _column_families

    cpdef void open(self)
    cpdef void close(self)
    cpdef void create_column_family(self, str table_name, object pyarrow_schema)
    cpdef void insert_batch(self, str table_name, object pyarrow_batch)
    cpdef object scan_table(self, str table_name)
    cpdef object scan_range_to_table(self, str table_name, str start_key=*, str end_key=*)
    cpdef void delete_range(self, str table_name, str start_key, str end_key)
    cpdef list list_column_families(self)
    cpdef void flush(self)
