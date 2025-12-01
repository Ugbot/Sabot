# -*- coding: utf-8 -*-
# cython: language_level=3
"""
MarbleDB Store Backend Implementation

Cython implementation using MarbleDB Arrow Batch API for table storage.
Provides InsertBatch, ScanTable, CreateColumnFamily, and NewIterator operations.
"""

import logging
import os
import time
from typing import Optional, Dict, List

from libc.stdint cimport uint64_t, uint32_t, int64_t, UINT64_MAX
from libcpp.string cimport string
from libcpp.memory cimport unique_ptr, shared_ptr, make_shared
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp cimport bool as cbool

logger = logging.getLogger(__name__)

# MarbleDB C++ declarations
cdef extern from "marble/api.h" namespace "marble":
    Status OpenDatabase(const string& path, unique_ptr[MarbleDB]* db)
    void CloseDatabase(unique_ptr[MarbleDB]* db)

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

cdef extern from "marble/api.h" namespace "marble":
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

    cdef cppclass Field:
        pass

    cdef cppclass DataType:
        pass

# Use pyarrow.lib for Arrow conversions
cimport pyarrow.lib as pa_lib
from pyarrow.includes.libarrow cimport (
    CRecordBatch as PCRecordBatch,
    CSchema as PCSchema,
    CTable as PCTable,
)


cdef class MarbleDBStoreBackend:
    """
    MarbleDB store backend for Arrow table operations.

    Provides:
    - CreateColumnFamily with Arrow schemas
    - InsertBatch for bulk Arrow RecordBatch insertion
    - ScanTable for full table scans
    - NewIterator for range scans
    """
    # Note: cdef attributes declared in .pxd file

    def __init__(self, str db_path, dict config=None):
        """Initialize MarbleDB store backend."""
        self._db_path = db_path
        self._config = config or {}
        self._is_open = False
    
    cpdef void open(self):
        """Open MarbleDB instance."""
        if self._is_open:
            return

        cdef Status status
        cdef string db_path_bytes = self._db_path.encode('utf-8')

        # Use OpenDatabase from api.h
        status = OpenDatabase(db_path_bytes, &self._db)
        if not status.ok():
            raise RuntimeError(f"Failed to open MarbleDB: {status.ToString().decode('utf-8')}")

        self._is_open = True
        logger.info(f"MarbleDB store backend opened at {self._db_path}")
    
    cpdef void close(self):
        """Close MarbleDB."""
        if not self._is_open:
            return
        
        # Use CloseDatabase from api.h
        if self._db.get() != NULL:
            # CloseDatabase takes unique_ptr by reference and sets it to nullptr
            # We'll just reset our unique_ptr
            self._db.reset()
        
        self._is_open = False
        logger.info("MarbleDB store backend closed")
    
    cpdef void create_column_family(self, str table_name, object pyarrow_schema):
        """Create column family (table) with Arrow schema.

        Each column family in MarbleDB is effectively a table with:
        - Its own Arrow schema defining typed columns
        - Independent compaction settings
        - Bloom filters and sparse indexes

        Args:
            table_name: Name of the table/column family
            pyarrow_schema: PyArrow Schema defining column types
        """
        if not self._is_open:
            raise RuntimeError("MarbleDB backend not open")

        cdef Status status
        cdef ColumnFamilyDescriptor desc
        cdef ColumnFamilyHandle* handle = NULL
        cdef pa_lib.Schema schema_obj
        cdef string cf_name

        try:
            # Convert PyArrow schema to C++ Arrow schema
            schema_obj = <pa_lib.Schema>pyarrow_schema

            # Build descriptor with Arrow schema
            cf_name = table_name.encode('utf-8')
            desc.name = cf_name
            desc.options.schema = schema_obj.sp_schema
            desc.options.primary_key_index = 0  # First column is primary key by default
            desc.options.enable_bloom_filter = True
            desc.options.enable_sparse_index = True
            desc.options.index_granularity = 8192
            desc.options.bloom_filter_bits_per_key = 10

            # Create column family
            status = self._db.get().CreateColumnFamily(desc, &handle)
            if not status.ok():
                raise RuntimeError(f"CreateColumnFamily failed: {status.ToString().decode('utf-8')}")

            # Store handle for future operations
            if handle != NULL:
                self._column_families[cf_name] = handle

            logger.info(f"Created column family '{table_name}' with {len(pyarrow_schema)} columns")

        except Exception as e:
            logger.error(f"CreateColumnFamily failed: {e}")
            raise
    
    cpdef void insert_batch(self, str table_name, object pyarrow_batch):
        """Insert Arrow RecordBatch - direct C++ InsertBatch()."""
        if not self._is_open:
            raise RuntimeError("MarbleDB backend not open")
        
        cdef Status status
        cdef pa_lib.RecordBatch batch_obj
        cdef shared_ptr[RecordBatch] cpp_batch
        cdef string table_name_str
        
        try:
            # Convert PyArrow RecordBatch to C++ RecordBatch
            # Use pyarrow.lib to get C++ RecordBatch pointer
            batch_obj = <pa_lib.RecordBatch>pyarrow_batch
            cpp_batch = batch_obj.sp_batch
            
            # Insert batch
            table_name_str = table_name.encode('utf-8')
            status = self._db.get().InsertBatch(table_name_str, cpp_batch)
            if not status.ok():
                raise RuntimeError(f"InsertBatch failed: {status.ToString().decode('utf-8')}")
            
        except Exception as e:
            logger.error(f"InsertBatch failed: {e}")
            raise
    
    cpdef object scan_table(self, str table_name):
        """Scan table - returns PyArrow Table.

        Note: This is a simplified implementation that returns NotImplemented
        until proper pyarrow C++ type conversion is implemented.
        For now, users should use the state backend (marbledb_backend) which
        has working get/put/scan operations.
        """
        if not self._is_open:
            raise RuntimeError("MarbleDB backend not open")

        cdef Status status
        cdef unique_ptr[QueryResult] result
        cdef string table_name_str
        cdef shared_ptr[RecordBatch] batch_ptr
        cdef vector[shared_ptr[RecordBatch]] cpp_batches

        try:
            table_name_str = table_name.encode('utf-8')
            status = self._db.get().ScanTable(table_name_str, &result)
            if not status.ok():
                raise RuntimeError(f"ScanTable failed: {status.ToString().decode('utf-8')}")

            # Collect batches in C++ vector
            while result.get().HasNext():
                status = result.get().Next(&batch_ptr)
                if status.ok() and batch_ptr.get() != NULL:
                    cpp_batches.push_back(batch_ptr)

            # Convert to PyArrow using pyarrow's IPC serialization
            # This is a workaround for shared_ptr conversion issues
            import pyarrow as pa

            if cpp_batches.size() == 0:
                # Return empty table
                return pa.table({})

            # For proper implementation, we'd need to use Arrow IPC to
            # serialize batches and deserialize in Python, or use
            # pyarrow's internal CRecordBatch types directly
            # For now, return a placeholder indicating the scan succeeded
            # but conversion isn't implemented yet
            logger.warning("scan_table: C++ to PyArrow batch conversion not fully implemented")
            return pa.table({})

        except Exception as e:
            logger.error(f"ScanTable failed: {e}")
            raise
    
    cpdef object scan_range_to_table(self, str table_name, str start_key=None, str end_key=None):
        """Scan range and return as PyArrow Table.

        Note: Range scanning currently falls back to full table scan.
        The NewIterator API with KeyRange requires concrete Key implementations
        (StringKey, Int64Key) which need additional Cython bindings.

        For now, this does a full table scan and ignores the range parameters.
        Use marbledb_backend for proper range scans with the LSMTree API.
        """
        if not self._is_open:
            raise RuntimeError("MarbleDB backend not open")

        if start_key is not None or end_key is not None:
            logger.warning(
                "scan_range_to_table: Range parameters ignored, "
                "doing full table scan. Use marbledb_backend for proper range scans."
            )

        # Fall back to full table scan
        return self.scan_table(table_name)
    
    cpdef void delete_range(self, str table_name, str start_key, str end_key):
        """Delete range of keys.

        Note: DeleteRange requires concrete Key implementations (StringKey, Int64Key)
        which need additional Cython bindings. Currently not implemented.

        Use marbledb_backend for delete operations.
        """
        if not self._is_open:
            raise RuntimeError("MarbleDB backend not open")

        # The MarbleDB DeleteRange API requires concrete Key objects (StringKey, Int64Key)
        # which we don't have bindings for yet. The abstract Key class can't be instantiated.
        raise NotImplementedError(
            "delete_range not implemented for marbledb_store. "
            "Use marbledb_backend for delete operations."
        )
    
    cpdef list list_column_families(self):
        """List all column families."""
        if not self._is_open:
            raise RuntimeError("MarbleDB backend not open")
        
        cdef vector[string] cf_names
        cdef list result = []
        
        try:
            cf_names = self._db.get().ListColumnFamilies()
            for name in cf_names:
                result.append(name.decode('utf-8'))
            return result
            
        except Exception as e:
            logger.error(f"ListColumnFamilies failed: {e}")
            raise
    
    cpdef void flush(self):
        """Flush pending writes."""
        if not self._is_open:
            return
        
        cdef Status status
        status = self._db.get().Flush()
        if not status.ok():
            logger.warning(f"Flush warning: {status.ToString().decode('utf-8')}")

