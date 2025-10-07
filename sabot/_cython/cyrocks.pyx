# -*- coding: utf-8 -*-
"""
CyRocks: Minimal Cython Binding for RocksDB C++

Direct C++ binding to vendored RocksDB library. Implements only the essential
operations needed by Sabot's state backend.
"""

from libcpp.string cimport string
from libcpp cimport bool as cpp_bool
from libc.stdint cimport uint64_t
from cpython.bytes cimport PyBytes_AsString, PyBytes_Size

# External C++ declarations for RocksDB types
cdef extern from "rocksdb/status.h" namespace "rocksdb":
    cdef cppclass Status:
        Status() nogil
        cpp_bool ok() nogil
        cpp_bool IsNotFound() nogil
        string ToString() nogil

cdef extern from "rocksdb/slice.h" namespace "rocksdb":
    cdef cppclass Slice:
        Slice() nogil
        Slice(const char* data, size_t size) nogil
        const char* data() nogil
        size_t size() nogil

cdef extern from "rocksdb/options.h" namespace "rocksdb":
    cdef cppclass Options:
        Options() nogil
        cpp_bool create_if_missing
        uint64_t write_buffer_size
        int max_write_buffer_number
        uint64_t target_file_size_base
        int max_background_compactions
        int max_background_flushes

cdef extern from "rocksdb/write_batch.h" namespace "rocksdb":
    cdef cppclass WriteBatch:
        WriteBatch() nogil
        void Put(const Slice& key, const Slice& value) nogil
        void Clear() nogil

cdef extern from "rocksdb/db.h" namespace "rocksdb":
    cdef cppclass DB:
        Status Put(const WriteOptions&, const Slice&, const Slice&) nogil
        Status Get(const ReadOptions&, const Slice&, string*) nogil
        Status Delete(const WriteOptions&, const Slice&) nogil
        Status Close() nogil

    cdef cppclass WriteOptions:
        WriteOptions() nogil
        cpp_bool sync

    cdef cppclass ReadOptions:
        ReadOptions() nogil

    cdef cppclass FlushOptions:
        FlushOptions() nogil
        cpp_bool wait

    Status DB_Open "rocksdb::DB::Open"(const Options&, const string&, DB**) nogil


# Exception handling
cdef class RocksDBError(Exception):
    """RocksDB operation error."""
    pass


cdef inline void check_status(const Status& st) except *:
    """Check RocksDB status and raise exception if not OK."""
    if not st.ok():
        if st.IsNotFound():
            return  # Not found is not an error for get operations
        error_msg = st.ToString().decode('utf-8')
        raise RocksDBError(error_msg)


# Cython wrapper classes
cdef class CyOptions:
    """
    RocksDB Options wrapper.

    Minimal wrapper for configuring RocksDB database instances.
    """
    cdef Options* opts_ptr

    def __cinit__(self):
        self.opts_ptr = new Options()
        if self.opts_ptr == NULL:
            raise MemoryError("Failed to allocate Options")

    def __dealloc__(self):
        if self.opts_ptr != NULL:
            del self.opts_ptr

    @property
    def create_if_missing(self):
        return self.opts_ptr.create_if_missing

    @create_if_missing.setter
    def create_if_missing(self, cpp_bool value):
        self.opts_ptr.create_if_missing = value

    @property
    def write_buffer_size(self):
        return self.opts_ptr.write_buffer_size

    @write_buffer_size.setter
    def write_buffer_size(self, uint64_t value):
        self.opts_ptr.write_buffer_size = value

    @property
    def max_write_buffer_number(self):
        return self.opts_ptr.max_write_buffer_number

    @max_write_buffer_number.setter
    def max_write_buffer_number(self, int value):
        self.opts_ptr.max_write_buffer_number = value

    @property
    def target_file_size_base(self):
        return self.opts_ptr.target_file_size_base

    @target_file_size_base.setter
    def target_file_size_base(self, uint64_t value):
        self.opts_ptr.target_file_size_base = value

    @property
    def max_background_compactions(self):
        return self.opts_ptr.max_background_compactions

    @max_background_compactions.setter
    def max_background_compactions(self, int value):
        self.opts_ptr.max_background_compactions = value

    @property
    def max_background_flushes(self):
        return self.opts_ptr.max_background_flushes

    @max_background_flushes.setter
    def max_background_flushes(self, int value):
        self.opts_ptr.max_background_flushes = value


cdef class CyDB:
    """
    RocksDB Database wrapper.

    Minimal wrapper providing essential KV operations: get, put, delete, close, flush.
    """
    cdef DB* db_ptr
    cdef WriteOptions write_opts
    cdef ReadOptions read_opts
    cdef FlushOptions flush_opts
    cdef cpp_bool is_open

    def __cinit__(self, str db_path, CyOptions options):
        cdef Status st
        cdef string path_str = db_path.encode('utf-8')

        self.db_ptr = NULL
        self.is_open = False

        # Open database
        with nogil:
            st = DB_Open(options.opts_ptr[0], path_str, &self.db_ptr)

        check_status(st)
        self.is_open = True

        # Initialize options
        self.write_opts.sync = False
        self.flush_opts.wait = True

    def __dealloc__(self):
        if self.db_ptr != NULL and self.is_open:
            self.close()

    cpdef void put(self, bytes key, bytes value) except *:
        """
        Put a key-value pair.

        Args:
            key: Key as bytes
            value: Value as bytes
        """
        if not self.is_open:
            raise RocksDBError("Database is not open")

        cdef const char* key_data = PyBytes_AsString(key)
        cdef size_t key_size = PyBytes_Size(key)
        cdef const char* value_data = PyBytes_AsString(value)
        cdef size_t value_size = PyBytes_Size(value)

        cdef Slice key_slice = Slice(key_data, key_size)
        cdef Slice value_slice = Slice(value_data, value_size)
        cdef Status st

        with nogil:
            st = self.db_ptr.Put(self.write_opts, key_slice, value_slice)

        check_status(st)

    cpdef bytes get(self, bytes key):
        """
        Get a value by key.

        Args:
            key: Key as bytes

        Returns:
            Value as bytes, or None if not found
        """
        if not self.is_open:
            raise RocksDBError("Database is not open")

        cdef const char* key_data = PyBytes_AsString(key)
        cdef size_t key_size = PyBytes_Size(key)
        cdef Slice key_slice = Slice(key_data, key_size)
        cdef string value_str
        cdef Status st

        with nogil:
            st = self.db_ptr.Get(self.read_opts, key_slice, &value_str)

        if st.IsNotFound():
            return None

        check_status(st)

        # Convert C++ string to Python bytes
        return value_str.c_str()[:value_str.size()]

    cpdef void delete(self, bytes key) except *:
        """
        Delete a key.

        Args:
            key: Key as bytes
        """
        if not self.is_open:
            raise RocksDBError("Database is not open")

        cdef const char* key_data = PyBytes_AsString(key)
        cdef size_t key_size = PyBytes_Size(key)
        cdef Slice key_slice = Slice(key_data, key_size)
        cdef Status st

        with nogil:
            st = self.db_ptr.Delete(self.write_opts, key_slice)

        check_status(st)

    cpdef void close(self) except *:
        """Close the database."""
        cdef Status st

        if self.db_ptr != NULL and self.is_open:
            with nogil:
                st = self.db_ptr.Close()
            check_status(st)
            del self.db_ptr
            self.db_ptr = NULL
            self.is_open = False

    cpdef void flush(self) except *:
        """Flush writes to disk (no-op for now, RocksDB auto-flushes)."""
        # RocksDB handles flushing automatically
        # This is here for API compatibility
        pass
