# -*- coding: utf-8 -*-
"""Cython declarations for MarbleDBStateBackend."""

from libc.stdint cimport uint64_t
from libcpp.memory cimport unique_ptr
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.utility cimport pair
from libcpp cimport bool as cbool

from .state_backend cimport StateBackend

# Full LSMTree declaration (needed for method calls)
cdef extern from "marble/lsm_storage.h" namespace "marble":
    cdef cppclass Status:
        cbool ok()
        string ToString()
        @staticmethod
        Status OK()
        @staticmethod
        Status NotFound(const string& msg)
        @staticmethod
        Status InvalidArgument(const string& msg)
        @staticmethod
        Status IOError(const string& msg)
    
    cdef cppclass LSMTreeConfig:
        LSMTreeConfig()
        size_t memtable_max_size_bytes
        size_t memtable_max_entries
        size_t sstable_max_size_bytes
        cbool enable_bloom_filters
        cbool enable_mmap_flush
        string data_directory
        string wal_directory
        string temp_directory

    cdef cppclass LSMTree:
        Status Init(const LSMTreeConfig& config)
        Status Shutdown()
        Status Put(uint64_t key, const string& value)
        Status Get(uint64_t key, string* value)
        Status MultiGet(const vector[uint64_t]& keys,
                       vector[string]* values,
                       vector[Status]* statuses)
        Status Delete(uint64_t key)
        Status DeleteBatch(const vector[uint64_t]& keys)
        Status DeleteRange(uint64_t start_key, uint64_t end_key, size_t* deleted_count)
        Status Scan(uint64_t start_key, uint64_t end_key,
                   vector[pair[uint64_t, string]]* results)
        Status Flush()

    unique_ptr[LSMTree] CreateLSMTree()

cdef class MarbleDBStateBackend(StateBackend):
    cdef unique_ptr[LSMTree] _lsm
    cdef str _db_path
    cdef cbool _is_open
    
    cpdef void open(self)
    cpdef void close(self)
    cpdef void flush(self)
    cpdef void put_value(self, str state_name, object value)
    cpdef object get_value(self, str state_name, object default_value=*)
    cpdef void clear_value(self, str state_name)
    cpdef void add_to_list(self, str state_name, object value)
    cdef list _get_list_internal(self, str list_key)
    cpdef object get_list(self, str state_name)
    cpdef void clear_list(self, str state_name)
    cpdef void put_to_map(self, str state_name, object map_key, object value)
    cdef dict _get_map_internal(self, str map_state_key)
    cpdef object get_from_map(self, str state_name, object map_key, object default_value=*)
    cpdef void remove_from_map(self, str state_name, object map_key)
    cpdef dict get_map(self, str state_name)
    cpdef void clear_map(self, str state_name)
    cpdef void put_raw(self, str key, bytes value)
    cpdef bytes get_raw(self, str key)
    cpdef void delete_raw(self, str key)
    cpdef bint exists_raw(self, str key)
    cpdef dict multi_get_raw(self, list keys)
    cpdef void delete_range_raw(self, str start_key, str end_key)
    cpdef void insert_batch_raw(self, list keys, list values)
    cpdef void delete_batch_raw(self, list keys)
    cpdef object scan_range(self, str start_key=*, str end_key=*)
    cpdef str checkpoint(self)
    cpdef void restore(self, str checkpoint_path)

