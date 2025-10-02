# -*- coding: utf-8 -*-
"""
RocksDB State Backend Header

Cython implementation of RocksDB state backend for Flink-compatible state management.
Provides efficient KV storage with TTL and batch operations.
"""

from libc.stdint cimport int64_t, uint64_t, int32_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map

from .state_backend cimport StateBackend, StateDescriptor, KeyScope


cdef extern from "rocksdb/c.h":
    # RocksDB types
    ctypedef struct rocksdb_t:
        pass
    ctypedef struct rocksdb_options_t:
        pass
    ctypedef struct rocksdb_writeoptions_t:
        pass
    ctypedef struct rocksdb_readoptions_t:
        pass
    ctypedef struct rocksdb_iterator_t:
        pass
    ctypedef struct rocksdb_checkpoint_t:
        pass

    # Core operations
    rocksdb_t* rocksdb_open(rocksdb_options_t* options, const char* name, char** errptr) nogil
    void rocksdb_close(rocksdb_t* db) nogil
    void rocksdb_put(rocksdb_t* db, rocksdb_writeoptions_t* options,
                     const char* key, size_t keylen,
                     const char* val, size_t vallen, char** errptr) nogil
    char* rocksdb_get(rocksdb_t* db, rocksdb_readoptions_t* options,
                      const char* key, size_t keylen,
                      size_t* vallen, char** errptr) nogil
    void rocksdb_delete(rocksdb_t* db, rocksdb_writeoptions_t* options,
                        const char* key, size_t keylen, char** errptr) nogil

    # Iterator operations (for scans and timers)
    rocksdb_iterator_t* rocksdb_create_iterator(rocksdb_t* db, rocksdb_readoptions_t* options) nogil
    void rocksdb_iter_seek(rocksdb_iterator_t* iter, const char* k, size_t klen) nogil
    void rocksdb_iter_next(rocksdb_iterator_t* iter) nogil
    unsigned char rocksdb_iter_valid(rocksdb_iterator_t* iter) nogil
    const char* rocksdb_iter_key(rocksdb_iterator_t* iter, size_t* klen) nogil
    const char* rocksdb_iter_value(rocksdb_iterator_t* iter, size_t* vlen) nogil
    void rocksdb_iter_destroy(rocksdb_iterator_t* iter) nogil

    # Memory management
    void rocksdb_free(void* ptr) nogil


cdef class RocksDBStateBackend(StateBackend):
    """
    RocksDB-based state backend for Flink-compatible state management.

    Provides efficient persistent storage for all state types:
    - ValueState, ListState, MapState
    - ReducingState, AggregatingState
    - User timers (event-time, processing-time)
    - System operational state

    Performance targets:
    - Get/Put: <1ms (SSD), <100μs (NVMe)
    - Batch operations: 10x faster than individual
    - Iterator scans: <1ms for 1000 entries
    """

    cdef:
        rocksdb_t* db
        rocksdb_options_t* options
        rocksdb_writeoptions_t* write_opts
        rocksdb_readoptions_t* read_opts
        string db_path
        bint is_open
        object list_states  # Python dict[str, list]
        object map_states  # Python dict[str, dict]
        object _db  # Python RocksDB instance
        str _db_path
        bint _is_open
        object _list_states
        object _map_states
        object _fallback_conn
        object _fallback_lock

    # Core state operations (implementing StateBackend interface)
    cpdef void put_value(self, str state_name, object value)
    cpdef object get_value(self, str state_name, object default_value=*)
    cpdef void clear_value(self, str state_name)

    cpdef void add_to_list(self, str state_name, object value)
    cpdef object get_list(self, str state_name)
    cpdef void clear_list(self, str state_name)
    cpdef void remove_from_list(self, str state_name, object value)
    cpdef bint list_contains(self, str state_name, object value)

    cpdef void put_to_map(self, str state_name, object key, object value)
    cpdef object get_from_map(self, str state_name, object key, object default_value=*)
    cpdef void remove_from_map(self, str state_name, object key)
    cpdef bint map_contains(self, str state_name, object key)
    cpdef object get_map_keys(self, str state_name)
    cpdef object get_map_values(self, str state_name)
    cpdef object get_map_entries(self, str state_name)
    cpdef void clear_map(self, str state_name)

    cpdef void add_to_reducing(self, str state_name, object value)
    cpdef object get_reducing(self, str state_name, object default_value=*)
    cpdef void clear_reducing(self, str state_name)

    cpdef void add_to_aggregating(self, str state_name, object input_value)
    cpdef object get_aggregating(self, str state_name, object default_value=*)
    cpdef void clear_aggregating(self, str state_name)

    # Bulk operations
    cpdef void clear_all_states(self)
    cpdef object snapshot_all_states(self)
    cpdef void restore_all_states(self, object snapshot)

    # RocksDB-specific operations (Python implementation, no C methods)

    # Lifecycle
    cpdef void open(self)
    cpdef void close(self)
    cpdef void flush(self)
